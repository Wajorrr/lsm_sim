#include <iostream>
#include <math.h>
#include <fstream>
#include "ram_shield.h"

size_t blockSize = 1048576;
double allocation_threshold = 1;

RamShield::RamShield(stats stat, size_t block_size) : FlashCache(stat),
													  flash{},
													  allObjects{},
													  maxBlocks{},
													  numBlocks{}
{
	maxBlocks = FLASH_SIZE / block_size; // 块数量
}

RamShield::~RamShield() {}

size_t RamShield::proc(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}
	counter++;

	double currTime = r->time;

	assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
	auto searchRKId = allObjects.find(r->kid);
	if (searchRKId != allObjects.end()) // 请求对象存在
	{
		/*
		 * The object exists in system. If the sizes of the
		 * current Request and previous Request differ then the previous
		 * Request is removed. Otherwise, one
		 * needs to update the hitrate and its place in the globalLru.
		 * If it is in the cache, one needs also to update the
		 * 'flashiness' value and its place in the dram MFU and dram LRU
		 */
		RamShield::RItem &item = searchRKId->second;
		if (r->size() == item.size) // 请求大小与对象大小相同
		{
			// HIT
			if (!warmup)
			{
				stat.hits++;
			}
			// 若对象有效，在全局lru队列中重新插入到头部
			if (!item.isGhost)
			{
				assert(item.globalLruIt != globalLru.end());
				globalLru.erase(item.globalLruIt);
			}
			globalLru.emplace_front(item.kId);
			item.globalLruIt = globalLru.begin();

			if (item.isInDram) // 在DRAM中，更新对象的flashiness
			{
				if (!warmup)
				{
					stat.hits_dram++;
				}

				// Update Flashiness
				std::pair<uint32_t, double> p = *(item.dramLocation);
				// flashiness+1
				p.second += 1;
				dramIt tmp = item.dramLocation;
				// 重新插回dram flashiness队列
				dramAdd(p, tmp, item);
				// 删除原项
				dram.erase(tmp);
			}
			else // 在Flash中
			{
				if (!warmup)
				{
					stat.hits_flash++;
				}
				if (item.isGhost) // 若为无效对象
				{
					item.isGhost = false; // 重新标记为有效对象
					item.flashIt->size += item.size;
					flashSize += item.size;
					// 若总空间大小超过阈值，驱逐全局lru末尾对象
					while (dramSize + flashSize > DRAM_SIZE + FLASH_SIZE * stat.threshold)
					{
						// 全局lru队列中最后一个对象
						uint32_t globalLruKid = globalLru.back();
						RamShield::RItem &victimItem = allObjects[globalLruKid];
						assert(victimItem.size > 0);
						// 驱逐对象
						evict_item(victimItem, warmup);
					}
					assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
				}
			}
			item.lastAccessInTrace = counter; // 更新逻辑时间戳
			item.last_accessed = currTime;	  // 更新实际时间戳

			return 1;
		}
		else // 请求大小与对象大小不同，更新(先删除后重插入)
		{
			// UPDATE
			if (!item.isInDram) // 若在Flash中，从所属块中删除对象
				item.flashIt->items.remove(item.kId);

			if (!item.isGhost) // 若对象有效，对对象进行驱逐(dram或flash中)，之后再插入新对象
				evict_item(item, warmup);

			// 若在flash中，还需要从删除相应的对象索引(flash中的驱逐不会直接删除对象)
			if (!item.isInDram)
			{
				assert(allObjects.find(item.kId) != allObjects.end());
				allObjects.erase(item.kId);
			}
		}
	}
	// MISS

	/*
	 * The Request doesn't exist in the system. We always insert new Requests
	 * to the DRAM.
	 */
	// 插入新对象/更新重插入对象
	RamShield::RItem newItem(r, counter);
	assert(((unsigned int)newItem.size) <= DRAM_SIZE);
	assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
	while (true)
	{
		// 若DRAM空间足够，且总空间大小不超过阈值，直接插入到DRAM中
		if ((newItem.size + dramSize <= DRAM_SIZE) && (dramSize + flashSize + newItem.size <= DRAM_SIZE + FLASH_SIZE * stat.threshold))
		{
			// 将对象插入到DRAM中
			add_item(newItem);
			return PROC_MISS;
		}

		// Not enough space in DRAM, check flash
		assert(numBlocks <= maxBlocks);
		// 若无法直接插入dram,首先检查总空间大小是否超过阈值，若超过，驱逐全局lru末尾对象
		if ((dramSize + flashSize + newItem.size) > DRAM_SIZE + FLASH_SIZE * stat.threshold)
		{
			uint32_t globalLruKid = globalLru.back();
			RamShield::RItem &victimItem = allObjects[globalLruKid];
			assert(victimItem.size > 0);
			// 驱逐对象
			evict_item(victimItem, warmup);
			assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
		}
		else if (numBlocks < maxBlocks) // dram空间不足但总空间大小未超过阈值，且flash块数量未达到最大值
		{
			// 分配新块，将dram中flashiness最大的对象迁移到新块中，直到新块满且空间利用率高于阈值
			allocate_flash_block(warmup);
			assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
		}
		else
		{
			assert(0);
		}
		assert(numBlocks <= maxBlocks);
	}
	assert(0);
	return PROC_MISS;
}

void RamShield::add_item(RItem &newItem)
{
	// 插入新对象到DRAM中
	dramAddFirst(newItem);
	globalLru.emplace_front(newItem.kId);
	newItem.globalLruIt = globalLru.begin();
	allObjects[newItem.kId] = newItem;
	dramSize += newItem.size;
}

// 驱逐指定对象
void RamShield::evict_item(RamShield::RItem &victimItem, bool warmup /*uint32_t &victimKid*/)
{
	assert(!victimItem.isGhost);
	// 从全局lru队列中删除
	globalLru.erase(victimItem.globalLruIt);
	victimItem.globalLruIt = globalLru.end();
	if (victimItem.isInDram) // 若在DRAM中
	{
		// 从DRAM中删除
		dram.erase(victimItem.dramLocation);
		dramSize -= victimItem.size;
		assert(allObjects.find(victimItem.kId) != allObjects.end());
		allObjects.erase(victimItem.kId);
	}
	else // 若在Flash中
	{
		// 标记为ghost，无效对象
		victimItem.isGhost = true;
		// 所属的block
		blockIt curr_block = victimItem.flashIt;
		// 更新block大小和flash大小
		curr_block->size -= victimItem.size;
		flashSize -= victimItem.size;
		// 若块的空间利用率低于阈值，对块进行GC
		if ((curr_block->size / (double)stat.block_size) < stat.threshold)
		{
			assert(dramSize <= DRAM_SIZE);
			// 对块进行GC,有效对象迁移到DRAM中
			evict_block(curr_block);
			// 重新分配新块，将dram中flashiness最大的对象迁移到新块中，直到新块满且空间利用率高于阈值
			allocate_flash_block(warmup);
		}
	}
}

void RamShield::evict_block(blockIt victim_block)
{
	// 遍历GC块中的对象id
	for (keyIt it = victim_block->items.begin(); it != victim_block->items.end(); it++)
	{
		assert(allObjects.find(*it) != allObjects.end());
		// 获取对象
		RamShield::RItem &victim_item = allObjects[*it];

		if (victim_item.isGhost) // 若对象是无效对象，直接删除
		{
			allObjects.erase(*it);
		}
		else // 否则GC到DRAM中
		{
			victim_item.isInDram = true;
			victim_item.flashIt = flash.end();
			// 初始化flashiness
			std::pair<uint32_t, double> p(victim_item.kId, INITIAL_CREDIT);
			dramAddFirst(victim_item);
			dramSize += victim_item.size;
		}
	}
	// 从flash中删除块
	flash.erase(victim_block);
	flashSize -= victim_block->size;
	numBlocks--;
	assert(numBlocks == flash.size());
}

void RamShield::allocate_flash_block(bool warmup)
{
	assert(flashSize <= FLASH_SIZE);

	flash.emplace_front(); // 在flash头部插入新块
	RamShield::Block &curr_block = flash.front();

	// dram flashiness队列中末尾对象(flashiness最大的对象)
	auto mfu_it = --dram.end();
	// 从dram flashiness队列中选取对象插入到新块，直到新块满且空间利用率高于阈值
	while (mfu_it != dram.begin())
	{
		assert(!dram.empty());
		uint32_t mfuKid = mfu_it->first;
		mfu_it--; // 前一个对象
		RamShield::RItem &mfuItem = allObjects[mfuKid];
		assert(mfuItem.size > 0);

		// 若新块空间不足
		if (curr_block.size + mfuItem.size > stat.block_size)
		{
			// 若新块空间利用率已经高于阈值，结束遍历
			if (curr_block.size / (double)stat.block_size > allocation_threshold)
				break;
			else // 否则，继续遍历，继续添加对象到新块
				continue;
		}

		// 从DRAM中删除对象
		dram.erase(mfuItem.dramLocation);
		mfuItem.isInDram = false;
		mfuItem.dramLocation = dram.end();
		dramSize -= mfuItem.size;

		// 添加到flash的新块中
		mfuItem.flashIt = flash.begin();
		curr_block.items.emplace_front(mfuKid);
		curr_block.size += mfuItem.size;

		assert(mfuItem.size > 0);
		assert(numBlocks <= maxBlocks);
	};

	assert(curr_block.size <= stat.block_size);
	// 更新块数量和flash大小
	numBlocks++;
	flashSize += curr_block.size;
	assert(numBlocks <= maxBlocks);
	assert(flashSize <= FLASH_SIZE);

	if (!warmup)
	{
		stat.writes_flash++;
		stat.flash_bytes_written += stat.block_size;
	}
}

void RamShield::dump_stats(void)
{
	Policy::dump_stats();
}
