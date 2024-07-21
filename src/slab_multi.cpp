#include <iostream>
#include <cassert>
#include <fstream>

#include "slab_multi.h"
#include "lru.h"
#include "mc.h"

slab_multi::application::application(
    size_t appid,
    size_t min_mem_pct,
    size_t target_mem)
    : appid{appid}, min_mem_pct{min_mem_pct}, target_mem{target_mem},
      min_mem{size_t(target_mem * (min_mem_pct / 100.))}, credit_bytes{},
      bytes_in_use{}, accesses{}, hits{}, shadow_q_hits{}, survivor_items{},
      survivor_bytes{}, evicted_items{}, evicted_bytes{}
{
}

slab_multi::application::~application() {}

slab_multi::slab_multi(stats stat)
    : Policy{stat}, last_dump{}, apps{}, slabs{}, slab_for_key{}, slab_count{}, mem_in_use{}
{
  if (stat.memcachier_classes)
    slab_count = 15;
  else
    slab_count = slabs_init(stat.gfactor);
  // 根据slab class数量初始化若干个LRU
  slabs.resize(slab_count);

  if (stat.memcachier_classes)
  {
    std::cerr << "Initialized with memcachier slab classes" << std::endl;
  }
  else
  {
    std::cerr << "Initialized with " << slab_count
              << " slab classes" << std::endl;
  }
}

slab_multi::~slab_multi()
{
}

// 添加一个新应用
void slab_multi::add_app(size_t appid, size_t min_mem_pct, size_t target_memory)
{
  assert(apps.find(appid) == apps.end());
  apps.emplace(appid, application{appid, min_mem_pct, target_memory});
}

void slab_multi::dump_app_stats(double time)
{
  // slab and its lru classes don't do a good job of tracking per-app
  // memory use. Because of the separation between them, it is much
  // easier just to sum it all up on demand.
  // 先重置
  for (auto &p : apps)
  {
    application &app = p.second;
    app.bytes_in_use = 0;
  }
  // 遍历所有slab，对所有应用的内存使用情况进行统计
  for (const LRU &klass : slabs)
  {
    std::unordered_map<int32_t, size_t> per_app_use =
        klass.get_per_app_bytes_in_use();
    for (auto &pr : per_app_use)
    {
      auto appit = apps.find(pr.first);
      assert(appit != apps.end());
      application &app = appit->second;

      app.bytes_in_use += pr.second;
    }
  }

  // 遍历每个应用，输出统计信息
  for (auto &p : apps)
  {
    application &app = p.second;
    app.dump_stats(time);
  }
}

size_t slab_multi::process_request(const Request *r, bool warmup)
{
  assert(r->size() > 0);
  // 特殊处理过大的请求
  if (r->size() > int32_t(MAX_SIZE))
  {
    std::cerr << "Can't process large Request of size " << r->size()
              << std::endl;
    return 1;
  }

  // 每小时dump一次统计信息
  if (!warmup && ((last_dump == 0.) || (r->time - last_dump > 3600.)))
  {
    if (last_dump == 0.)
      application::dump_stats_header();
    dump_app_stats(r->time);
    if (last_dump == 0.)
      last_dump = r->time;
    last_dump += 3600.0;
  }

  // 如果是新的应用，添加到apps中
  if (stat.apps->empty())
    stat.apps->insert(r->appid);
  assert(stat.apps->count(r->appid) == 1);

  // 查找appid对应的应用信息
  auto appit = apps.find(r->appid);
  assert(appit != apps.end());
  application &app = appit->second;

  if (!warmup)
  {
    ++stat.accesses;
    ++app.accesses;
  }

  uint64_t class_size = 0;
  uint64_t klass = 0;
  // 获取当前请求大小对应的slab class
  std::tie(class_size, klass) = get_slab_class(r->size());
  if (klass == PROC_MISS)
  {
    // We need to count these as misses in case different size to class
    // mappings don't all cover the same range of object sizes. If some
    // slab class configuration doesn't handle some subset of the acceses
    // it must penalized. In practice, memcachier and memcached's policies
    // cover the same range, so this shouldn't get invoked anyway.
    return PROC_MISS;
  }
  assert(klass < slabs.size());

  // See if slab assignment already exists for this key.
  // Check if change in size (if any) requires reclassification
  // to a different slab. If so, remove from current slab.
  // 获取当前请求之前所在的slab class
  auto csit = slab_for_key.find(r->kid);

  // 如果之前已经存在，且之前的slab class不是当前的slab class，需要从之前的slab class中删除
  if (csit != slab_for_key.end() && csit->second != klass)
  {
    LRU &sclass = slabs.at(csit->second);
    sclass.remove(r);
    slab_for_key.erase(r->kid);
  }

  // 当前请求要插入的slab class
  LRU &slab_class = slabs.at(klass);

  // Round up the Request size so the per-class LRU holds the right
  // size.
  Request copy{*r};
  copy.key_sz = 0;
  copy.val_sz = class_size;
  copy.frag_sz = class_size - r->size(); // 当前请求在当前slab中所造成的碎片大小

  // If the LRU has used all of its allocated space up, try to expand it.
  // 若当前还有空闲空间，且当前slab class剩余空间不足以容纳当前请求，扩容
  while (mem_in_use < stat.global_mem &&
         slab_class.would_cause_eviction(copy))
  {
    slab_class.expand(SLABSIZE);
    mem_in_use += SLABSIZE;
  }

  // 新的slab class处理该请求，可能会造成驱逐，outcome为slab class的剩余空间增加量(使用空间的减少量)
  size_t outcome = slab_class.process_request(&copy, warmup);

  // Trace the move of the key into its new slab class.
  // 更新请求到slab class的映射
  slab_for_key.insert(std::pair<uint32_t, uint32_t>(r->kid, klass));

  if (outcome == PROC_MISS)
  {
    // Count compulsory misses.
    return PROC_MISS;
  }
  else if (!warmup)
  {
    ++stat.hits;
    ++app.hits;
  }

  return 1;
}

// 获取所有slab class的总缓存数据量
size_t slab_multi::get_bytes_cached() const
{
  size_t b = 0;
  for (const auto &s : slabs)
    b += s.get_bytes_cached();
  return b;
}

std::pair<uint64_t, uint64_t> slab_multi::get_slab_class(uint32_t size)
{
  uint64_t class_size = 64;
  uint64_t klass = 0;

  if (stat.memcachier_classes)
  {
    while (true)
    {
      if (size < class_size)
        return {class_size, klass};
      class_size <<= 1;
      ++klass;
      if (klass == slab_count)
      {
        return {PROC_MISS, PROC_MISS};
      }
    }
  }
  else
  {
    std::tie(class_size, klass) = slabs_clsid(size);
    // Object too big for chosen size classes.
    if (size > 0 && klass == 0)
      return {PROC_MISS, PROC_MISS};
    --klass;
    return {class_size, klass};
  }
}
