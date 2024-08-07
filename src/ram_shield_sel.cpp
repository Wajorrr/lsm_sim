#include <iostream>
#include <math.h>
#include <fstream>
#include "ram_shield_sel.h"

RamShield_sel::RamShield_sel(stats stat, size_t block_size) : RamShield(stat, block_size)
{
}

RamShield_sel::~RamShield_sel() {}

size_t RamShield_sel::proc(const Request *r, bool warmup)
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
		if (r->size() == item.size) // 若请求大小与对象大小相同
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

			if (item.isInDram) // 若对象在dram中，更新flashiness
			{
				if (!warmup)
				{
					stat.hits_dram++;
				}

				// Update Flashiness
				std::pair<uint32_t, double> p = *(item.dramLocation);
				p.second += 1;
				dramIt tmp = item.dramLocation;
				dramAdd(p, tmp, item);
				dram.erase(tmp);
			}
			else // 对象在flash中
			{
				if (!warmup)
				{
					stat.hits_flash++;
				}
				if (item.isGhost) // 若为无效对象，重新标记为有效
				{
					item.isGhost = false;
					item.flashIt->size += item.size;
					flashSize += item.size;
					// 若总空间超过阈值，进行驱逐
					while (dramSize + flashSize > DRAM_SIZE + FLASH_SIZE * stat.threshold)
					{
						uint32_t globalLruKid = globalLru.back();
						RamShield::RItem &victimItem = allObjects[globalLruKid];
						assert(victimItem.size > 0);
						evict_item(victimItem, warmup);
					}
					assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
				}
			}
			item.lastAccessInTrace = counter;
			item.last_accessed = currTime;

			return 1;
		}
		else // 若请求大小与对象大小不同，更新
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
		if (newItem.size + dramSize <= DRAM_SIZE && (dramSize + flashSize + newItem.size <= DRAM_SIZE + FLASH_SIZE * stat.threshold))
		{
			add_item(newItem);
			assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
			return PROC_MISS;
		}

		// Not enough space in DRAM
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
		else if (numBlocks == maxBlocks) // dram空间不足但总空间大小未超过阈值，且flash块数量达到最大值
		{
			blockIt victim_block = flash.begin();
			// 遍历flash块，找到有效空间最少的块，进行GC
			for (blockIt curr_block = flash.begin(); curr_block != flash.end(); curr_block++)
			{
				if (curr_block->size < victim_block->size)
					victim_block = curr_block;
			}
			assert(victim_block != flash.end());
			evict_block(victim_block);
			assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
		}
		else if (numBlocks < maxBlocks) // dram空间不足但总空间大小未超过阈值，且flash块数量未达到最大值
		{
			// 分配新块，将dram中flashiness最大的对象迁移到新块中，直到新块满且空间利用率高于阈值
			allocate_flash_block(warmup);
			assert(dramSize + flashSize <= DRAM_SIZE + FLASH_SIZE * stat.threshold);
			assert(numBlocks <= maxBlocks);
			assert(dramSize <= DRAM_SIZE);
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

void RamShield_sel::evict_item(RamShield::RItem &victimItem, bool warmup)
{
	assert(!victimItem.isGhost);
	globalLru.erase(victimItem.globalLruIt);
	victimItem.globalLruIt = globalLru.end();
	if (victimItem.isInDram) // 若对象在DRAM中，从DRAM中删除
	{
		dram.erase(victimItem.dramLocation);
		dramSize -= victimItem.size;
		assert(allObjects.find(victimItem.kId) != allObjects.end());
		allObjects.erase(victimItem.kId);
	}
	else // 若对象在flash中，标记为无效(相较于lru,不直接根据块空间利用率进行实时gc和回收)
	{
		victimItem.isGhost = true;
		blockIt curr_block = victimItem.flashIt;
		curr_block->size -= victimItem.size;
		flashSize -= victimItem.size;
	}
}
