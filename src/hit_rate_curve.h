#include <cstddef>
#include <numeric>
#include <vector>
#include <cinttypes>
#include <iostream>
#include <exception>
#include <fstream>

#ifndef HIT_RATE_CURVE_H
#define HIT_RATE_CURVE_H

class hit_rate_curve
{
public:
  static constexpr size_t MAX_DISTANCE = 1lu * 1024 * 1024 * 1024;
  hit_rate_curve()
      : distances{}, too_big_hit{}, misses{}
  {
  }

  // 记录一次访问的重用距离
  void hit(size_t distance)
  {
    // 重用距离过大
    if (distance >= MAX_DISTANCE)
    {
      ++too_big_hit;
      return;
    }

    // 容器扩容
    if (distances.size() < distance + 1)
      distances.resize(distance + 1, 0);

    // 记录重用距离出现次数
    ++(distances.at(distance));
  }

  void miss()
  {
    ++misses;
  }

  // 输出每个重用距离的出现次数
  void dump() const
  {
    for (size_t i = 0; i < distances.size(); ++i)
      std::cout << distances[i] << " ";
  }

  // 输出每个重用距离及其出现次数
  void dump_readable() const
  {
    for (size_t i = 0; i < distances.size(); ++i)
      std::cout << "Distance " << i << " Count " << distances[i] << std::endl;
  }

  // 输出每个重用距离的累积分布
  void dump_cdf(const std::string &filename) const
  {
    std::ofstream out{filename};

    out << "distance cumfrac" << std::endl;
    if (distances.size() == 0)
      return;

    size_t total = std::accumulate(distances.begin(), distances.end(), 0) +
                   too_big_hit +
                   misses;

    size_t accum = 0; // 累计数量
    for (size_t i = 0; i < distances.size(); ++i)
    {
      size_t delta = distances[i];
      accum += delta;
      if (delta)
        out << i << " " << float(accum) / total << std::endl; // 当前累计百分比
    }
  }

  // 合并两个重用距离统计vector
  void merge(const hit_rate_curve &other)
  {
    if (distances.size() < other.distances.size())
      distances.resize(other.distances.size());

    for (size_t i = 0; i < other.distances.size(); ++i)
      distances[i] += other.distances[i];
  }

  // For each distance, a count of the hits at that distance.
  // Units on distance depends on how the caller interprets it.
  // For example, some curves may represent hit rank while others
  // may represent number of bytes into shadow queue of the hit.
  std::vector<size_t> distances;

  // The distances vector holds a size_t for each distance. If the highest
  // seen distance is too large, the distances vector can grow to
  // many gigabytes. too_big_hit tracks hits that with huge distances.
  size_t too_big_hit;

  size_t misses;
};

#endif
