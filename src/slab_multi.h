#ifndef SLABMULTI_H
#define SLABMULTI_H

#include "policy.h"
#include "lru.h"

#include <vector>
#include <memory>
#include <unordered_map>

// slab缓存，并支持多应用负载场景
class slab_multi : public Policy
{
public:
  static constexpr uint8_t MIN_CHUNK = 48;        // Memcached min chunk bytes
  static constexpr double DEF_GFACT = 1.25;       // Memcached growth factor
  static constexpr uint32_t PAGE_SIZE = 1000000;  // Memcached page bytes
  static constexpr uint16_t MAX_CLASSES = 256;    // Memcached max no of slabs
  static constexpr size_t MAX_SIZE = 1024 * 1024; // Largest KV pair allowed

  class application
  {
  public:
    application(size_t appid, size_t min_mem, size_t target_mem);
    ~application();

    // 最大内存限制
    size_t bytes_limit() const
    {
      return target_mem + credit_bytes;
    }

    bool try_steal_from(application &other, size_t bytes)
    {
      // 其他应用的内存总量不足
      if (other.bytes_limit() < bytes)
        return false;

      if (&other == this)
        return false;

      // 如果其他应用内存减少后小于最小内存，偷取失败
      const size_t would_become = other.bytes_limit() - bytes;
      if (would_become < min_mem)
        return false;

      // 偷取成功
      other.credit_bytes -= bytes;
      credit_bytes += bytes;

      return true;
    }

    static void dump_stats_header()
    {
      std::cout << "time "
                << "app "
                << "subPolicy "
                << "target_mem "
                << "credit_bytes "
                << "share "
                << "min_mem "
                << "min_mem_pct "
                << "steal_size "
                << "bytes_in_use "
                << "need "
                << "hits "
                << "accesses "
                << "shadow_q_hits "
                << "survivor_items "
                << "survivor_bytes "
                << "evicted_items "
                << "evicted_bytes "
                << "hit_rate "
                << std::endl;
    }

    void dump_stats(double time)
    {
      std::cout << int64_t(time) << " "
                << appid << " "
                << "multislab "
                << target_mem << " "
                << credit_bytes << " "
                << target_mem + credit_bytes << " "
                << min_mem << " "
                << min_mem_pct << " "
                << 0 << " "
                << bytes_in_use << " "
                << need() << " "
                << hits << " "
                << accesses << " "
                << shadow_q_hits << " "
                << survivor_items << " "
                << survivor_bytes << " "
                << evicted_items << " "
                << evicted_bytes << " "
                << double(hits) / accesses << " "
                << std::endl;
    }

    double need()
    {
      return double(target_mem + credit_bytes) / bytes_in_use;
    }

    const size_t appid;       // 应用id
    const size_t min_mem_pct; // 最小内存百分比
    const size_t target_mem;  // 目标内存
    const size_t min_mem;     // 最小内存

    ssize_t credit_bytes;

    size_t bytes_in_use;

    size_t accesses;
    size_t hits;
    size_t shadow_q_hits;
    size_t survivor_items;
    size_t survivor_bytes;
    size_t evicted_items;
    size_t evicted_bytes;
  };

  slab_multi(stats stat);
  ~slab_multi();

  void add_app(size_t appid, size_t min_mem_pct, size_t target_memory);
  void dump_app_stats(double time);

  size_t process_request(const Request *r, bool warmup);
  size_t get_bytes_cached() const;

private:
  std::pair<uint64_t, uint64_t> get_slab_class(uint32_t size);

  static constexpr size_t SLABSIZE = 1024 * 1024;

  double last_dump;
  std::unordered_map<size_t, application> apps;

  std::vector<LRU> slabs;

  // Simple mapping of existing keys to their respective slab.
  std::unordered_map<uint32_t, uint32_t> slab_for_key;

  uint32_t slab_count;

  uint64_t mem_in_use;
};

#endif
