#include <iostream>
#include <fstream>
#include <cassert>
#include <unordered_set>
#include <algorithm>

#include "lsm.h"

lsm::lsm(stats stat)
    : Policy{stat}, cleaner{cleaning_policy::OLDEST_ITEM}, map{}, head{nullptr}, segments{}, free_segments{}
{

  srand(0);

  if (stat.global_mem % stat.segment_size != 0)
  {
    std::cerr << "WARNING: global_mem not a multiple of segment_size" << std::endl;
  }
  // 初始化段列表
  segments.resize(stat.global_mem / stat.segment_size);
  free_segments = segments.size();
  std::cerr << "global_mem " << stat.global_mem
            << " segment_size " << stat.segment_size
            << " segment_count " << segments.size() << std::endl;

  // Check for enough segments to sustain cleaning width and head segment.
  assert(segments.size() > 2 * stat.cleaning_width);

  // Sets up head.
  rollover(0.);
}

lsm::~lsm() {}

size_t lsm::process_request(const Request *r, bool warmup)
{
  assert(r->size() > 0);

  if (!warmup)
    ++stat.accesses;

  // 哈希表查找请求对应的lru链表节点指针
  auto it = map.find(r->kid);
  // 找到了
  if (it != map.end())
  {
    if (!warmup)
      ++stat.hits;

    auto list_it = it->second;
    // 当前请求所属的段
    segment *old_segment = list_it->seg;
    int32_t old_Request_size = list_it->req.size();
    // 如果请求大小没有改变，将请求移到原段lru链表头部，然后直接返回
    if (old_Request_size == r->size())
    {
      // Promote this item to the front.
      old_segment->queue.erase(list_it);
      old_segment->queue.emplace_front(old_segment, *r);
      map[r->kid] = old_segment->queue.begin();

      ++old_segment->access_count;

      return 1;
    }
    else
    { // 请求大小有改变，重新插入
      //  If the size changed, then we have to put it in head. Just
      //  falling through to the code below handles that. We still
      //  count this record as a single hit, even though some miss
      //  would have had to have happened to shootdown the old stale
      //  sized value. This is to be fair to other policies like gLRU
      //  that don't detect these size changes.
    }
  }
  else // 未命中，插入
  {
    // If miss in hash table, then count a miss. This get will count
    // as a second access that hits below.
  }

  // 若当前开放段(头部段)的空间不够，选一个新空闲段为开放段
  if (head->filled_bytes + r->size() > stat.segment_size)
    rollover(r->time);
  // assert(head->filled_bytes + r->size() <= stat.segment_size);

  // Add the new Request.
  // 将请求插入到当前开放段的lru链表头部
  head->queue.emplace_front(head, *r);
  // 更新请求到lru链表节点的映射
  map[r->kid] = head->queue.begin();
  // 更新开放段当前已使用的空间大小
  head->filled_bytes += r->size();

  ++head->access_count;

  return PROC_MISS;
}

size_t lsm::get_bytes_cached() const
{
  return 0;
}

void lsm::dump_util(const std::string &filename) {}

void lsm::rollover(double timestamp)
{
  // 标记当前开放段是否更新成功
  bool rolled = false;
  for (auto &segment : segments)
  {
    // 已经被实例化了，跳过
    if (segment)
      continue;

    // 使用emplace方法实例化新段
    // 这是C++标准库容器的一个方法，用于就地构造元素，避免不必要的拷贝或移动操作
    segment.emplace();
    // 空闲段-1
    --free_segments;
    // 将head指针指向新实例化的段
    head = &segment.value();
    // 段的创建时间戳
    head->low_timestamp = timestamp;
    rolled = true;
    break;
  }
  assert(rolled);

  // 若空闲段数量小于阈值，GC
  // 保持cleaning_width个空闲段，每次GC时选取cleaning_width个段进行清理
  // 数据回收到cleaning_width-1个段
  if (free_segments < stat.cleaning_width)
    clean();
}

// 输出段的实例化情况
void lsm::dump_usage()
{
  std::cerr << "[";
  for (auto &segment : segments)
    std::cerr << (bool(segment) ? "X" : "_");
  std::cerr << "]" << std::endl;
}

double lsm::get_running_hit_rate()
{
  return double(stat.hits) / stat.accesses;
}

// 选取GC的受害者段
auto lsm::choose_cleaning_sources() -> std::vector<segment *>
{
  switch (cleaner)
  {
  case cleaning_policy::OLDEST_ITEM: // 选取创建时间最旧的cleaning_width个段
    return choose_cleaning_sources_oldest_item();
  case cleaning_policy::ROUND_ROBIN: // 在段列表中顺序选取cleaning_width个段
    return choose_cleaning_sources_round_robin();
  case cleaning_policy::RUMBLE: // 选取访问次数最少的cleaning_width个段
    return choose_cleaning_sources_rumble();
  case cleaning_policy::RANDOM:
  default:
    return choose_cleaning_sources_random(); // 随机选取cleaning_width个段
  }
}

auto lsm::choose_cleaning_sources_random() -> std::vector<segment *>
{
  std::vector<segment *> srcs{};

  for (size_t i = 0; i < stat.cleaning_width; ++i)
  {
    // 随机选取一个段，若为未实例化的段、开放段、已选取的段，则继续随机选取，最多遍历1000000次
    for (size_t j = 0; j < 1000000; ++j)
    {
      // 随机选取一个段
      int r = rand() % segments.size();
      auto &segment = segments.at(r);

      // Don't pick free segments.
      if (!segment)
        continue;

      // Don't pick the head segment.
      if (&segment.value() == head)
        continue;

      // Don't pick any segment we've already picked!
      bool already_picked = false;
      for (auto &already_in : srcs)
        already_picked |= (already_in == &segment.value());
      if (already_picked)
        continue;

      srcs.emplace_back(&segment.value());
      break;
    }
    // Check for enough in use segments during cleaning; if not then a bug!
    assert(srcs.size() == i + 1);
  }

  return srcs;
}

auto lsm::choose_cleaning_sources_oldest_item() -> std::vector<segment *>
{
  std::vector<segment *> srcs{};

  // 遍历所有段，将所有段置入一个vector，跳过未实例化的段和当前开放段
  for (auto &segment : segments)
  {
    // Don't pick free segments.
    if (!segment)
      continue;

    // Don't pick the head segment.
    if (&segment.value() == head)
      continue;

    srcs.emplace_back(&segment.value());
  }

  // 按创建时间从旧到新排序
  std::sort(srcs.begin(), srcs.end(),
            [](const segment *left, const segment *right)
            {
              return left->low_timestamp < right->low_timestamp;
            });
  // std::cout << "Sorted list" << std::endl;
  // for (segment* src : srcs)
  // std::cout << src->low_timestamp << std::endl;

  // 保留创建时间最旧的cleaning_width个段作为GC段
  srcs.resize(stat.cleaning_width);

  // std::cout << "Selected list" << std::endl;
  // for (segment* src : srcs)
  // std::cout << src->low_timestamp << std::endl;
  // std::cout << "==============" << std::endl;

  // 重置段的访问计数？
  for (auto &segment : segments)
  {
    // Don't pick free segments.
    if (!segment)
      continue;
    segment->access_count = 0;
  }

  // Check for enough in use segments during cleaning; if not then a bug!
  assert(srcs.size() == stat.cleaning_width);

  return srcs;
}

auto lsm::choose_cleaning_sources_rumble() -> std::vector<segment *>
{
  std::vector<segment *> srcs{};

  for (auto &segment : segments)
  {
    // Don't pick free segments.
    if (!segment)
      continue;

    // Don't pick the head segment.
    if (&segment.value() == head)
      continue;

    srcs.emplace_back(&segment.value());
  }

  // 按段的访问次数从小到大排序
  std::sort(srcs.begin(), srcs.end(),
            [](const segment *left, const segment *right)
            {
              return left->access_count < right->access_count;
            });
  // std::cout << "Sorted list" << std::endl;
  // for (segment* src : srcs)
  // std::cout << src->access_count << std::endl;
  srcs.resize(stat.cleaning_width);
  // std::cout << "Selected list" << std::endl;
  // for (segment* src : srcs)
  // std::cout << src->access_count << std::endl;
  // std::cout << "==============" << std::endl;

  // 重置段的访问计数
  for (auto &segment : segments)
  {
    // Don't pick free segments.
    if (!segment)
      continue;
    segment->access_count = 0;
  }

  // Check for enough in use segments during cleaning; if not then a bug!
  assert(srcs.size() == stat.cleaning_width);

  return srcs;
}

auto lsm::choose_cleaning_sources_round_robin() -> std::vector<segment *>
{
  static size_t next = 0;

  std::vector<segment *> srcs{};

  // 直接在segments中顺序选择cleaning_width个段
  while (srcs.size() < stat.cleaning_width)
  {
    auto &segment = segments.at(next);

    if (!segment)
      continue;

    // Don't pick the head segment.
    if (&segment.value() == head)
      continue;

    srcs.emplace_back(&segment.value());

    ++next;
    next %= segments.size();
  }

  return srcs;
}

// 从空闲段中选取一个段作为GC的数据迁移段
auto lsm::choose_cleaning_destination() -> segment *
{
  for (auto &segment : segments)
  {
    // Don't pick a used segment. (This also guarantees we don't pick
    // the same free segment more than once.)
    if (segment)
      continue;

    // Construct, clean empty segment here.
    segment.emplace();
    --free_segments;

    return &segment.value();
  }

  assert(false);
}

void lsm::dump_cleaning_plan(std::vector<segment *> srcs,
                             std::vector<segment *> dsts)
{
  std::cerr << "Cleaning Plan [";
  for (auto &segment : segments)
  {
    if (segment)
    {
      for (auto &src : srcs)
      {
        if (src == &segment.value())
        {
          std::cerr << "S";
          goto next;
        }
      }
      for (auto &dst : dsts)
      {
        if (dst == &segment.value())
        {
          std::cerr << "D";
          goto next;
        }
      }
      std::cerr << "X";
      goto next;
    }
    std::cerr << "-";
  next:
    continue;
  }
  std::cerr << "]" << std::endl;
}

void lsm::clean()
{
  /*
  const char* spinner = "|/-\\";
  static uint8_t last = 0;
  std::cerr << spinner[last++ & 0x03] << '\r';
  */

  // 根据GC策略选取cleaning_width个GC的受害者段
  std::vector<segment *> src_segments = choose_cleaning_sources();

  // One iterator for each segment to be cleaned. We use these to keep a
  // finger into each queue and merge them into a sorted order in the
  // destination segments.
  // 获取每个受害者段的lru链表头部迭代器
  std::vector<lru_queue::iterator> its{};
  for (auto *segment : src_segments)
    its.emplace_back(segment->queue.begin());

  size_t dst_index = 0;
  // 从空闲段中选取一个段容纳GC的数据
  segment *dst = choose_cleaning_destination();
  // 容纳GC数据的段列表
  std::vector<segment *> dst_segments{};
  dst_segments.push_back(dst);

  while (true)
  {
    item *item = nullptr;
    size_t it_to_incr = 0;
    // 遍历要GC的段
    for (size_t i = 0; i < src_segments.size(); ++i)
    {
      // 获取当前段的迭代器
      auto &it = its.at(i);
      // 若当前段的迭代器已经遍历完，跳过
      if (it == src_segments.at(i)->queue.end())
        continue;
      // 选取每个GC段的迭代器 当前指向的请求中 请求时间最晚的那个请求
      if (!item || it->req.time > item->req.time)
      {
        item = &*it;
        it_to_incr = i;
      }
    }

    // All done with all its.
    // 已经没有需要迁移的数据了
    if (!item)
    {
      for (size_t i = 0; i < src_segments.size(); ++i)
        assert(its.at(i) == src_segments.at(i)->queue.end());

      // 最后一个段中的碎片空间大小
      stat.cleaned_ext_frag_bytes += (stat.segment_size - dst->filled_bytes);
      // GC之后迁移数据所生成的新段数
      ++stat.cleaned_generated_segs;

      break;
    }

    // 迁移当前请求
    // Check to see if this version is still needed.
    auto it = map.find(item->req.kid);
    // If not in the hash table, just drop it.
    if (it == map.end())
    { // 若当前请求不在哈希表中(被删除？)，直接跳过
      // 当前段的迭代器指向下一个请求
      ++its.at(it_to_incr);
      continue;
    }
    // If hash table pointer refers to a different version, drop this one.
    if (&*it->second != item)
    { // 若当前请求在哈希表中查询到的指针指向的请求版本不是当前请求(被更新，无效化了)，跳过
      // 当前段的迭代器指向下一个请求
      ++its.at(it_to_incr);
      continue;
    }

    // 当前请求所属的段需要和当前GC的段相同(不能是被更新写入到另外段的无效请求)
    assert(src_segments.at(it_to_incr) == item->seg);

    // Check to see if there is room for the item in the dst.
    // 若当前容纳迁移数据的段空间不足，继续选取新的空闲段
    if (dst->filled_bytes + item->req.size() > stat.segment_size)
    {
      // 记录当前段的碎片空间大小
      stat.cleaned_ext_frag_bytes += (stat.segment_size - dst->filled_bytes);
      // GC之后迁移数据所生成的新段数+1
      ++stat.cleaned_generated_segs;

      // 当前GC过程生成的新段数
      ++dst_index;
      // Break out of relocation if we are out of free space our dst segs.
      // 若当前GC过程生成的新段数等于cleaning_width-1，结束GC，剩余数据驱逐
      if (dst_index == stat.cleaning_width - 1)
        break;

      // Rollover to new dst.
      // 选取一个新的空闲段作为容纳迁移数据的段
      dst = choose_cleaning_destination();
      // 放入容纳迁移数据的段列表
      dst_segments.push_back(dst);
    }
    assert(dst->filled_bytes + item->req.size() <= stat.segment_size);
    // 当前段的迭代器指向下一个请求
    ++its.at(it_to_incr);

    // Relocate *to the back* to retain timestamp sort order.
    // 将当前请求迁移到容纳迁移数据的段的lru链表尾部
    dst->queue.emplace_back(dst, item->req);
    // 更新哈希表中请求到lru链表节点的映射
    map[item->req.kid] = (--dst->queue.end());
    dst->filled_bytes += item->req.size();
    // 段的创建时间，以GC迁移的最后一个请求的时间戳为准
    dst->low_timestamp = item->req.time;
  }

  // Clear items that are going to get thrown on the floor from the hashtable.
  // 已经迁移了cleaning_width-1个段的数据，剩余数据驱逐
  for (size_t i = 0; i < src_segments.size(); ++i)
  { // 遍历每个GC段，递增其迭代器直至遍历完所有请求
    auto &it = its.at(i);
    while (it != src_segments.at(i)->queue.end())
    {
      // Need to do a double check here. The item we are evicting may
      // still exist in the hash table but it may point to a newer version
      // of this object. In that case, skip the erase from the hash table.
      auto hash_it = map.find(it->req.kid);
      if (hash_it != map.end())
      {
        item *from_list = &*it;              // 当前段中的请求指针
        item *from_hash = &*hash_it->second; // 当前请求在哈希表中映射到的请求指针

        // 若两个指针相同，说明当前段中当前请求有效，进行驱逐
        if (from_list == from_hash)
        {
          map.erase(it->req.kid);
          ++stat.evicted_items;
          stat.evicted_bytes += from_list->req.size();
        }
      }

      ++it;
    }
  }

  const bool debug = false;
  if (debug)
  {
    // Sanity check - none of the items left in the hash table should point
    // into a src_segment.
    // 遍历哈希表中所有项，检查是否还存在指向被GC段的请求
    for (auto &entry : map)
    {
      item &item = *entry.second;
      for (segment *src : src_segments)
        assert(item.seg != src);
    }

    // dump_cleaning_plan(src_segments, dst_segments);
  }

  // Reset each src segment as free for reuse.
  // 重置每个GC段，使其成为空闲段
  for (auto *src : src_segments) // 遍历每个GC段指针
  {
    for (auto &segment : segments) // 遍历段容器中的每个位置
    {
      if (src == &*segment)
      {
        segment = nullopt;
        ++free_segments;
      }
    }
  }

  if (debug)
  {
    // Sanity check - none of the segments should contain more data than their
    // rated capacity.
    size_t stored_in_whole_cache = 0; // 所有段中存储的请求总大小(有效+无效)
    // 遍历每个段，检查其中容纳的请求总大小是否超过了段的容量
    for (auto &segment : segments)
    {
      if (!segment)
        continue;

      size_t bytes = 0;
      for (const auto &item : segment->queue)
      {
        // 这里没有查哈希表，因此包括无效的请求
        // 无效+有效的请求总大小不应超过段的容量
        assert(item.seg == &segment.value());
        bytes += item.req.size();
      }
      assert(bytes <= stat.segment_size);
      assert(bytes == segment->filled_bytes);
      stored_in_whole_cache += bytes;
    }

    // Sanity check - the sum of all of the Requests active in the hash table
    // should not be greater than the combined space in all of the in use
    // segments.
    // 统计哈希表中映射到的活跃请求的总大小
    size_t reachable_from_map = 0; // 活跃请求总大小
    std::unordered_set<int32_t> seen{};
    // 遍历哈希表中映射到的活跃请求
    for (auto &entry : map)
    {
      item &item = *entry.second;
      // HT key had better only point to objects with the same kid.
      // 不应出现错误映射(id->请求 不对应)
      if (entry.first != item.req.kid)
      {
        std::cerr << "Mismatch! map entry "
                  << entry.first << " != " << item.req.kid << std::endl;
        item.req.dump();
      }
      assert(entry.first == item.req.kid);
      // Better not see the same kid twice among the objects in the HT.
      // 哈希表中不应出现重复的请求映射
      assert(seen.find(item.req.kid) == seen.end());
      reachable_from_map += item.req.size();
      seen.insert(item.req.kid);
    }
    // 活跃请求总大小不应超过存储的所有请求总大小
    if (reachable_from_map > stored_in_whole_cache)
    {
      std::cerr << "reachable_from_map: " << reachable_from_map << std::endl
                << "stored_in_whole_cache: " << stored_in_whole_cache
                << std::endl;
    }
    assert(reachable_from_map <= stored_in_whole_cache);
  }
}
