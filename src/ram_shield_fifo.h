#ifndef RAM_SHIELD_FIFO_H
#define RAM_SHIELD_FIFO_H

#include <vector>
#include <iostream>
#include <unordered_map>
#include <list>
#include <cassert>
#include "policy.h"
#include "ram_shield.h"

// 相较于原始的ram_shield，对块队列采用FIFO策略GC，而不是直接实时根据每个块的有效空间阈值进行GC
// 当块队列满时，按FIFO策略选取块进行GC和有效对象的移动
class RamShield_fifo : public RamShield
{
	virtual void evict_item(RamShield::RItem &victimItem, bool warmup);

public:
	RamShield_fifo(stats stat, size_t block_size);
	~RamShield_fifo();
	size_t proc(const Request *r, bool warmup);
};

#endif
