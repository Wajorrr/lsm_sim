#include <cassert>
#include <tuple>
#include <string>

#include "shadowlru.h"
#include "shadowslab.h"
#include "mc.h"
#include "common.h"

shadowslab::shadowslab(stats stat)
    : Policy{stat}, slabs{}, slabids{}, slab_for_key{}, next_slabid{0}, size_curve{}, slab_count{}
{

  if (stat.memcachier_classes)
    slab_count = 15;
  else
    slab_count = slabs_init(stat.gfactor);
  slabs.resize(slab_count);
  slabids.resize(slab_count);

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

shadowslab::~shadowslab()
{
}

size_t shadowslab::process_request(const Request *r, bool warmup)
{
  assert(r->size() > 0);

  if (!warmup)
  {
    ++stat.accesses;
  }
  uint64_t class_size = 0;
  uint64_t klass = 0;
  // 为当前请求找到合适的slab class，返回class_size和class序号
  std::tie(class_size, klass) = get_slab_class(r->size());

  // 没有合适的slab能容纳当前请求
  if (klass == PROC_MISS)
  {
    // We need to count these as misses in case different size to class
    // mappings don't all cover the same range of object sizes. If some
    // slab class configuration doesn't handle some subset of the acceses
    // it must penalized. In practice, memcachier and memcached's policies
    // cover the same range, so this shouldn't get invoked anyway.
    if (!warmup)
      size_curve.miss();
    return PROC_MISS;
  }

  // See if slab assignment already exists for this key.
  // Check if change in size (if any) requires reclassification
  // to a different slab. If so, remove from current slab.
  // 查看当前请求是否已经在某个slab class中
  auto csit = slab_for_key.find(r->kid);

  // 若当前请求已经在某个slab class中，且该slab class不是当前请求最适合的slab class
  if (csit != slab_for_key.end() && csit->second != klass)
  {
    shadowlru &sclass = slabs.at(csit->second);
    sclass.remove(r);
    slab_for_key.erase(r->kid);
  }

  // 新的slab class
  shadowlru &slab_class = slabs.at(klass);

  // Round up the Request size so the per-class LRU holds the right
  // size.
  Request copy{*r};
  copy.key_sz = 0;
  copy.val_sz = class_size;
  // 计算当前请求在slab class中的碎片大小
  copy.frag_sz = class_size - r->size();

  // 获取当前请求在slab class中的重用距离
  size_t size_distance = slab_class.process_request(&copy, warmup);
  if (size_distance == PROC_MISS)
  {
    // Count compulsory misses.
    if (!warmup)
      size_curve.miss();
    return PROC_MISS;
  }

  // Proc didn't miss, re-validate key/slab pair.
  // 更新请求到slab class的映射
  slab_for_key.insert(std::pair<uint32_t, uint32_t>(r->kid, klass));

  // Determine if we need to 'grow' the slab class by giving it more slabs.
  // 当前slab class中缓存的数据对应多少个slab
  size_t max_slabid_index = slab_class.get_bytes_cached() / SLABSIZE;
  // 当前slab class的slab列表
  std::vector<uint64_t> &class_ids = slabids.at(klass);
  // 若当前slab class的slab数量不够，扩容1个slab
  while (class_ids.size() < max_slabid_index + 1)
    class_ids.emplace_back(next_slabid++);

  // Figure out where in the space of slabids this access hit.
  // 当前重用距离对应第几个slab
  size_t slabid_index = size_distance / SLABSIZE;
  // 当前slab class中的对应slab
  size_t slabid = class_ids.at(slabid_index);

  // 记录全局重用距离
  size_t approx_global_size_distance =
      (slabid * SLABSIZE) + (size_distance % SLABSIZE);
  if (!warmup)
  {
    size_curve.hit(approx_global_size_distance);
    ++stat.hits;
  }

  return 0;
}

size_t shadowslab::get_bytes_cached() const
{
  return 0;
}

std::pair<uint64_t, uint64_t> shadowslab::get_slab_class(uint32_t size)
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

void shadowslab::log_curves()
{
  std::string app_ids = "";

  // 所有app id组成的字符串
  for (auto &a : *stat.apps)
    app_ids += std::to_string(a);

  std::string filename_suffix{"-app" + app_ids + (stat.memcachier_classes ? "-memcachier" : "-memcached")};

  // 保存重用距离cdf
  size_curve.dump_cdf("shadowslab-size-curve" + filename_suffix + ".data");
  dump_util("shadowslab-util" + filename_suffix + ".data");
}

// Given a slab id, searches over the slabids vectors and finds the vector
// position (slru) and the inner vector position (slab distance in lru) for
// the slab id.
std::pair<int32_t, int64_t> shadowslab::get_next_slab(uint32_t next)
{
  // Search for next slab.
  // 遍历slab class
  for (auto ito = slabids.begin(); ito != slabids.end(); ++ito)
  {
    // 遍历slab class中的slab
    for (auto iti = ito->begin(); iti != ito->end(); ++iti)
    {
      // 若当前slab id和给定的id相同
      if (*iti == next)
      {
        // 返回slab class id和当前slab在slab class中的位置
        int32_t slab_class = ito - slabids.begin();
        int64_t slab_dist_in_class = iti - ito->begin();
        return {slab_class, slab_dist_in_class};
      }
    }
  }
  return {-1, -1};
}

// Locates slabs in the order they were allocated and reports the
// fragmentation for each slab in that order. Intended to be used to
// graph utilization as a function of overall cache size.
void shadowslab::dump_util(const std::string &filename)
{
  std::ofstream out{filename};

  // Gather frag vectors for all slab classes.
  std::vector<std::vector<size_t>> frag_vectors{};
  // 遍历所有slab class，获取每个slab class中请求的碎片大小
  for (const auto &slab : slabs)
    frag_vectors.emplace_back(slab.get_class_frags(SLABSIZE));

  // Determine total number of slab ids.
  size_t num_slabs = 0;
  // 遍历每个slab class,统计总共的slab数量
  for (const auto &class_ids : slabids)
    num_slabs += class_ids.size();

  out << "cache_size utilization" << std::endl;

  // 总碎片大小
  size_t total_wasted = 0;
  // 遍历所有slab id
  for (size_t next_slab = 0; next_slab < num_slabs; ++next_slab)
  {
    // Get next slab class and slab distance.
    int64_t slab_class = 0;
    int64_t slab_dist = 0;
    // 找到当前slab id对应的slab class和其在slab class的slab列表中的位置
    std::tie(slab_class, slab_dist) = get_next_slab(next_slab);

    // Get the frag value for slab.
    // 当前slab class中请求的碎片大小列表
    const std::vector<size_t> &fs = frag_vectors.at(slab_class);
    // 根据当前slab在slab class中的位置，获取当前slab的碎片大小
    // 累加到总碎片大小中
    total_wasted += fs.at(slab_dist);
    // 当前累计的slab的总大小
    size_t cache_size = (1 + next_slab) * SLABSIZE;
    // 计算当前的总空间利用率
    double ratio = 1 - ((double)total_wasted / cache_size);

    // 输出当前slab的总大小和总空间利用率
    out << cache_size << " " << ratio << std::endl;
  }
}
