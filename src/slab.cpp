#include <iostream>
#include <cassert>
#include <fstream>

#include "slab.h"
#include "lru.h"
#include "mc.h"

// slab缓存，每个slab class使用一个LRU
slab::slab(stats stat)
    : Policy{stat}, slabs{}, slab_for_key{}, slab_count{}, mem_in_use{}
{
  // 64B, 128B, 256B, 512B, 1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB
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

slab::~slab()
{
}

size_t slab::process_request(const Request *r, bool warmup)
{
  assert(r->size() > 0);

  if (!warmup)
    ++stat.accesses;

  uint64_t class_size = 0;
  uint64_t klass = 0;
  // 为当前请求找到合适的slab class，返回class_size和class序号
  std::tie(class_size, klass) = get_slab_class(r->size());

  if (klass == PROC_MISS) // 没有合适的slab能容纳当前请求
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
  // 查看当前请求是否已经在某个slab class中
  auto csit = slab_for_key.find(r->kid);

  // 若当前请求已经在某个slab class中，且该slab class不是当前请求最适合的slab class
  if (csit != slab_for_key.end() && csit->second != klass)
  { // 从原slab class中移除
    LRU &sclass = slabs.at(csit->second);
    sclass.remove(r);
    slab_for_key.erase(r->kid);
  }

  // 新的slab class
  LRU &slab_class = slabs.at(klass);

  // Round up the Request size so the per-class LRU holds the right
  // size.
  Request copy{*r};
  copy.key_sz = 0;
  copy.val_sz = class_size;
  // 计算当前请求在slab class中的碎片大小
  copy.frag_sz = class_size - r->size();

  // If the LRU has used all of its allocated space up, try to expand it.
  // 若当前还有空闲空间，且当前slab class空间不够，扩容
  while (mem_in_use < stat.global_mem &&
         slab_class.would_cause_eviction(*r))
  {
    // 扩容1MB
    slab_class.expand(SLABSIZE);
    mem_in_use += SLABSIZE;
  }

  // 新的slab class处理该请求，可能会造成驱逐，outcome为slab class的剩余空间增加量(使用空间的减少量)
  size_t outcome = slab_class.process_request(&copy, warmup);

  // 更新请求到slab class的映射
  // Trace the move of the key into its new slab class.
  slab_for_key.insert(std::pair<uint32_t, uint32_t>(r->kid, klass));

  if (outcome == PROC_MISS)
  {
    // Count compulsory misses.
    return PROC_MISS;
  }

  if (!warmup)
    ++stat.hits;

  return 1;
}

// 获取所有slab class的总缓存数据量
size_t slab::get_bytes_cached() const
{
  size_t b = 0;
  // 遍历所有slab class，统计缓存的总数据大小
  for (const auto &s : slabs)
    b += s.get_bytes_cached();
  return b;
}

std::pair<uint64_t, uint64_t> slab::get_slab_class(uint32_t size)
{
  uint64_t class_size = 64;
  uint64_t klass = 0;

  if (stat.memcachier_classes) // memcachier的slab class，2的幂次递增
  {
    while (true)
    {
      // 若当前slab class能容纳size，则返回class_size和class序号
      if (size < class_size)
        return {class_size, klass};
      class_size <<= 1;
      ++klass;
      if (klass == slab_count) // 最大的slab也无法容纳
      {
        return {PROC_MISS, PROC_MISS};
      }
    }
  }
  else // 自定义的slab class，遍历选择
  {
    std::tie(class_size, klass) = slabs_clsid(size);
    // Object too big for chosen size classes.
    if (size > 0 && klass == 0)
      return {PROC_MISS, PROC_MISS};
    --klass;
    return {class_size, klass};
  }
}
