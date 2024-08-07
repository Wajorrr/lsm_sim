#include <iostream>
#include <math.h>
#include <fstream>
#include "flash_cache_lruk_clock.h"

size_t FC_K_LRU_CLK = 8;
size_t DRAM_SIZE_FC_KLRU_CLK = 51209600;
size_t FC_KLRU_QUEUE_SIZE_CLK = DRAM_SIZE_FC_KLRU_CLK / FC_K_LRU_CLK;
size_t FLASH_SIZE_FC_KLRU_CLK = 51209600;
size_t CLOCK_MAX_VALUE_KLRU = 7;
size_t CLOCK_JUMP = 2;
int MIN_QUEUE_TO_MOVE_TO_FLASH = 6;

FlashCacheLrukClk::FlashCacheLrukClk(stats stat) : Policy(stat),
												   dram(FC_K_LRU_CLK),
												   dramLru(0),
												   clockLru(),
												   kLruSizes(FC_K_LRU_CLK, 0),
												   flash(),
												   allObjects(),
												   allFlashObjects(),
												   dramSize(0),
												   flashSize(0),
												   clockIt(),
												   firstEviction(false)
{
}

FlashCacheLrukClk::~FlashCacheLrukClk() {}

size_t FlashCacheLrukClk::get_bytes_cached() const
{
	return dramSize + flashSize;
}

size_t FlashCacheLrukClk::process_request(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}

	bool updateWrites = true;
	uint32_t mfuKid = 0;

	auto searchRKId = allObjects.find(r->kid);
	if (searchRKId != allObjects.end()) // 若请求对象存在
	{
		// Object found

		FlashCacheLrukClk::Item &item = searchRKId->second;
		FlashCacheLrukClk::dramIt &clockItemIt = item.ClockIt;

		if (r->size() == item.size) // 请求大小与对象大小相同
		{
			if (!warmup)
			{
				stat.hits++;
			}

			// reseting the clock
			// 增加clock值，若超过最大值，则置为最大值
			if (item.clockJumpStatus + CLOCK_JUMP > CLOCK_MAX_VALUE_KLRU)
			{
				item.clockJumpStatus = CLOCK_MAX_VALUE_KLRU;
			}
			else
			{
				item.clockJumpStatus += CLOCK_JUMP;
			}
			clockItemIt->second = item.clockJumpStatus;

			if (item.isInDram) // 请求对象在DRAM中
			{
				if (!warmup)
				{
					stat.hits_dram++;
				}
				// 对象所在队列号
				size_t qN = item.queueNumber;
				//--------dramLRU ---------------
				// 从原队列中删除
				if (item.dramLruIt != dramLru.end())
				{
					dramLru.erase(item.dramLruIt);
				}
				dramLru.emplace_front(item.kId);
				dram[qN].erase(item.dramLocation);
				kLruSizes[qN] -= item.size;
				dramSize -= item.size;

				// 若不是最后一个队列，提升到下一个队列
				if ((qN + 1) != FC_K_LRU_CLK)
				{
					qN++;
				}
				else
				{
					updateWrites = false;
				}

				//--------dramLRU ---------------
				// 插入到lru头部
				item.dramLruIt = dramLru.begin();
				std::vector<uint32_t> objects{r->kid};
				// 提升到下一个队列
				dramAddandReorder(objects, r->size(), qN, updateWrites, warmup);
			}
			else // 请求对象在flash中
			{
				if (!warmup)
				{
					stat.hits_flash++;
					auto searchflashKId = allFlashObjects.find(r->kid);
					if (searchflashKId != allFlashObjects.end())
					{
						FlashCacheLrukClk::RecItem &recitem = searchflashKId->second;
						recitem.hitTimesAfterInserted++; // 记录命中次数
					}
				}
			}

			return 1;
		}
		else // 对象大小有变化，更新
		{	 // Size updated

			// 若当前对象为clock指针指向的对象，则将clock指针指向下一个对象
			if (clockItemIt == clockIt)
			{
				// changing the clockIt since this item is being deleted
				clockIt++;
				if (clockIt == clockLru.end())
				{
					clockIt = clockLru.begin();
				}
			}
			// 删除对象，重新插入
			deleteItem(r->kid);
		}
	}

	/*
	 * The Request doesn't exist in the system or was updated. We always insert
	 * new Requests to the DRAM at the beginning of the last queue.
	 *
	 * 2. While (object not inserted to the DRAM)
	 *	2.1  if (item.size() + dramSize <= DRAM_SIZE_FC_KLRU) -
	 *		insert item to the dram and return
	 * 	2.2 if possible to move from DRAM to flash -
	 *		by Dram LRUK
	 *	2.4 remove item by clock method. back to 2
	 */

	// 插入新对象
	FlashCacheLrukClk::Item newItem;
	newItem.kId = r->kid;
	newItem.size = r->size();
	newItem.isInDram = true;

	assert(((unsigned int)newItem.size) <= FC_KLRU_QUEUE_SIZE_CLK);

	while (true)
	{
		// 记录是否至少已经进行了一次驱逐
		if (!firstEviction)
		{
			firstEviction = true;
		}
		bool isDeleted = false;
		bool nomfuitem = false;
		dramIt tmpIt, startIt = clockIt;

		// 若DRAM空间足够，插入到DRAM中
		if (newItem.size + dramSize <= DRAM_SIZE_FC_KLRU_CLK)
		{
			// If we have place in the dram insert the new item to the beginning of the last queue.

			allObjects[newItem.kId] = newItem;

			std::vector<uint32_t> objects{r->kid};
			// newitem为true
			dramAdd(objects, r->size(), 0, true, warmup, true);

			return PROC_MISS;
		}
		// If we don't have enough space in the dram, we can move MRU items to the flash
		// or to delete LRU items

		// dram中没有足够空间，需要驱逐对象
		// 从后往前遍历队列，找到第一个MRU对象
		for (int i = FC_K_LRU_CLK - 1; i >= MIN_QUEUE_TO_MOVE_TO_FLASH; i--)
		{ // Find the MRU item
			if (kLruSizes[i] > 0)
			{
				mfuKid = (dram[i]).front();
				nomfuitem = false;
				break;
			}
			else
			{
				nomfuitem = true;
			}
		}

		// 若找到MRU对象
		if (!nomfuitem)
		{ // If we have mfu item we need to move it to flash
			FlashCacheLrukClk::Item &mfuItem = allObjects[mfuKid];
			size_t qN = mfuItem.queueNumber;

			assert(mfuItem.size > 0);

			// 若flash空间足够，将MRU对象插入到flash
			if (flashSize + mfuItem.size <= FLASH_SIZE_FC_KLRU_CLK)
			{
				// If we have enough space in the flash, we will insert the MRU item
				// to the flash
				if (!warmup)
				{
					// 当前请求对象是否是对flash中对象的更新
					// ？？？没搞懂这里在干嘛
					auto searchnewflashKId = allFlashObjects.find(r->kid);
					if (searchnewflashKId == allFlashObjects.end()) // 若不在flash中
					{
						FlashCacheLrukClk::RecItem newrecitem;
						newrecitem.kId = r->kid;
						newrecitem.queueWhenInserted = qN;
						newrecitem.size = mfuItem.size;
						allFlashObjects[newrecitem.kId] = newrecitem;
					}
					else // 若在flash中
					{
						FlashCacheLrukClk::RecItem &newrecitem = searchnewflashKId->second;
						newrecitem.rewrites++;
					}
				}
				//-----------dramLru----------------
				// 从dramLru中删除mru对象
				if (mfuItem.dramLruIt != dramLru.end())
				{
					dramLru.erase(mfuItem.dramLruIt);
				}

				mfuItem.isInDram = false;
				dram[qN].erase(mfuItem.dramLocation);
				mfuItem.dramLocation = (dram[0]).end();
				dramSize -= mfuItem.size;
				kLruSizes[qN] -= mfuItem.size;

				// 插入flash
				flash.emplace_front(mfuKid);
				mfuItem.flashIt = flash.begin();
				flashSize += mfuItem.size;
				//--------dramLRU ---------------
				mfuItem.dramLruIt = dramLru.end();

				if (!warmup)
				{
					stat.writes_flash++;
					stat.flash_bytes_written += mfuItem.size;
				}
			}
			else // flash空间不足，需要删除对象，从全局clock队列中删除
			{
				// If we don't have space in the flash, we will delete the first
				// item with clock 0 and make room for the new item

				// 移动clock指针，删除clock值为0的对象
				while (clockIt != clockLru.end())
				{
					// assert(clockIt->second <= CLOCK_MAX_VALUE_KLRU);
					if (clockIt->second == 0)
					{
						// This item need to be delete
						tmpIt = clockIt;
						tmpIt++;
						deleteItem(clockIt->first);
						isDeleted = true;
						break;
					}
					else
					{
						clockIt->second--;
					}
					clockIt++;
				}
				// 若没有删除对象，从头开始遍历到start位置
				if (!isDeleted)
				{
					// No item was deleted moving to items on the other
					// half of the watch
					clockIt = clockLru.begin();
					// assert(clockIt->second <= CLOCK_MAX_VALUE_KLRU);

					while (clockIt != startIt)
					{
						if (clockIt->second == 0)
						{
							tmpIt = clockIt;
							tmpIt++;
							deleteItem(clockIt->first);
							isDeleted = true;
							break;
						}
						else
						{
							clockIt->second--;
						}
						clockIt++;
					}
				}
				// 若遍历完一圈仍没有删除对象，直接删除当前对象
				if (!isDeleted)
				{
					// After a full cycle no item was deleted
					// Delete the item you started with
					assert(clockLru.size() > 0);
					assert(clockIt != clockLru.end());
					tmpIt = clockIt;
					tmpIt++;
					deleteItem(clockIt->first);
				}

				// reseting the clockIt again
				clockIt = (tmpIt == clockLru.end()) ? clockLru.begin() : tmpIt;
			}
		}
		else // 若lru-k队列中没有对象，则从dram lru中直接删除对象
		{	 // We don't have mfu item so we need to delete item from dramLru
			//--------dramLRU ---------------
			deleteItem(dramLru.back());
		}
	}
	assert(false);
	return PROC_MISS;
}

// 将对象插入到对应队列中
void FlashCacheLrukClk::dramAdd(std::vector<uint32_t> &objects,
								size_t sum __attribute__((unused)),
								size_t k,
								bool updateWrites __attribute__((unused)),
								bool warmup __attribute__((unused)),
								bool NewItem)
{

	for (const uint32_t &elem : objects)
	{
		FlashCacheLrukClk::Item &item = allObjects[elem];
		std::pair<uint32_t, size_t> it;
		it.first = item.kId;
		it.second = CLOCK_START_VAL;

		// 插入到指定队列中
		dram[k].emplace_front(elem);
		item.dramLocation = dram[k].begin();
		item.queueNumber = k;
		dramSize += item.size;
		kLruSizes[k] += item.size;
		if (k != 0)
		{
			assert(kLruSizes[k] <= FC_KLRU_QUEUE_SIZE_CLK);
		}

		// 若为新对象，插入到clocklru和dramlru中
		if (NewItem)
		{ // New Item, need to insert to clocklru and update clock
			if (clockLru.size() == 0)
			{
				clockLru.emplace_front(it);
				clockIt = clockLru.begin();
				item.ClockIt = clockLru.begin();
			}
			else
			{
				// 插入到clock指针的位置
				clockLru.insert(clockIt, it);
				dramIt Clkit = clockIt;
				Clkit--;
				item.ClockIt = Clkit;
			}

			//--------dramLRU ---------------
			// 插入到dramLru头部
			dramLru.emplace_front(item.kId);
			item.dramLruIt = dramLru.begin();
		}
	}
}

void FlashCacheLrukClk::dramAddandReorder(std::vector<uint32_t> &objects,
										  size_t sum,
										  size_t k,
										  bool updateWrites,
										  bool warmup)
{

	assert(k < FC_K_LRU_CLK);

	std::vector<uint32_t> newObjects;

	size_t newSum = 0;

	if (k != 0) // 若不为第一个队列，驱逐之后插入到前一个队列
	{
		while (sum + kLruSizes[k] > FC_KLRU_QUEUE_SIZE_CLK)
		{
			assert(kLruSizes[k] > 0);
			assert(dram[k].size() > 0);
			uint32_t elem = dram[k].back();
			FlashCacheLrukClk::Item &item = allObjects[elem];
			dram[k].pop_back();
			kLruSizes[k] -= item.size;
			dramSize -= item.size;
			// saving the extracted items in order to put them in lower queue
			newSum += item.size;
			newObjects.emplace_back(elem);
		}
	}
	else // 若为第一个队列，直接驱逐，只是在lru-k中驱逐了，没有从dram中驱逐
	{
		while (sum + dramSize > DRAM_SIZE_FC_KLRU_CLK)
		{
			// shouldnt get to here
			assert(0);
			assert(kLruSizes[k] > 0);
			assert(dram[k].size() > 0);
			uint32_t elem = dram[k].back();
			FlashCacheLrukClk::Item &item = allObjects[elem];
			dram[k].pop_back();
			kLruSizes[k] -= item.size;
			dramSize -= item.size;
		}
	}

	if (!updateWrites)
	{
		assert(newObjects.size() == 0);
		assert(newSum == 0);
	}

	// 当前对象插入到当前队列
	dramAdd(objects, sum, k, updateWrites, warmup, false);

	// 驱逐对象插入到前一个队列
	if (k > 0 && newObjects.size() > 0)
	{
		dramAddandReorder(newObjects, newSum, k - 1, true, warmup);
	}
}

// 删除对象
void FlashCacheLrukClk::deleteItem(uint32_t keyId)
{

	auto searchRKId = allObjects.find(keyId);
	assert(searchRKId != allObjects.end());

	FlashCacheLrukClk::Item &item = searchRKId->second;
	// 若删除的对象为clock指针指向的对象，则将clock指针指向下一个对象
	if (item.ClockIt == clockIt)
	{
		clockIt++;
		if (clockIt == clockLru.end())
		{
			clockIt = clockLru.begin();
		}
	}
	clockLru.erase(item.ClockIt);

	if (item.isInDram) // 若对象在DRAM中
	{
		// 从对应队列中删除
		size_t dGqN = item.queueNumber;
		dram[dGqN].erase(item.dramLocation);
		kLruSizes[dGqN] -= item.size;
		dramSize -= item.size;
		//--------dramLRU ---------------
		// 从dramLru中删除
		if (item.dramLruIt != dramLru.end())
		{
			dramLru.erase(item.dramLruIt);
		}
	}
	else // 若对象在flash中
	{
		flash.erase(item.flashIt);
		flashSize -= item.size;
	}

	allObjects.erase(keyId);
}

void FlashCacheLrukClk::dump_stats(void)
{
	assert(stat.apps->size() == 1);
	uint32_t appId = 0;
	for (const auto &app : *stat.apps)
	{
		appId = app;
	}
	std::string filename{stat.policy + "-app" + std::to_string(appId) + "-flash_mem" + std::to_string(FLASH_SIZE_FC_KLRU_CLK) + "-dram_mem" + std::to_string(DRAM_SIZE_FC_KLRU_CLK) + "-K" + std::to_string(FC_K_LRU_CLK)};
	std::ofstream out{filename};

	out << "dram size " << DRAM_SIZE_FC_KLRU_CLK << std::endl;
	out << "flash size " << FLASH_SIZE_FC_KLRU_CLK << std::endl;
	out << "#accesses " << stat.accesses << std::endl;
	out << "#global hits " << stat.hits << std::endl;
	out << "#dram hits " << stat.hits_dram << std::endl;
	out << "#flash hits " << stat.hits_flash << std::endl;
	out << "hit rate " << double(stat.hits) / stat.accesses << std::endl;
	out << "#writes to flash " << stat.writes_flash << std::endl;
	out << "credit limit " << stat.credit_limit << std::endl;
	out << "#bytes written to flash " << stat.flash_bytes_written << std::endl;
	out << std::endl
		<< std::endl;

	out << "Amount of lru queues: " << FC_K_LRU_CLK << std::endl;
	for (size_t i = 0; i < FC_K_LRU_CLK; i++)
	{
		out << "dram[" << i << "] has " << dram[i].size() << " items" << std::endl;
		out << "dram[" << i << "] size written " << kLruSizes[i] << std::endl;
	}
	out << "Total dram filled size " << dramSize << std::endl;

	for (auto it = allFlashObjects.begin(); it != allFlashObjects.end(); it++)
	{
		out << it->second.kId << "," << it->second.queueWhenInserted << "," << it->second.hitTimesAfterInserted << "," << it->second.size << "," << it->second.rewrites << std::endl;
	}
}
