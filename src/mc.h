#ifndef MC_H
#define MC_H

// static uint16_t slab_count;

// 根据factor：slab大小增长比例，来初始化slab
uint16_t slabs_init(const double factor);
// 根据给定的size，返回合适的slab class的大小和id
std::pair<uint32_t, uint32_t> slabs_clsid(const size_t size);

#endif
