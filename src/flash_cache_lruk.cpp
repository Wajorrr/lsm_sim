#include <iostream>
#include <math.h>
#include <fstream>
#include "flash_cache_lruk.h"

size_t FC_K_LRU = 8;
size_t DRAM_SIZE_FC_KLRU = 51209600;					  // dram总空间
size_t FC_KLRU_QUEUE_SIZE = DRAM_SIZE_FC_KLRU / FC_K_LRU; // 每个lru队列的大小
size_t FLASH_SIZE_FC_KLRU = 51209600;
double K_FC_KLRU = 1;
size_t L_FC_KLRU = 1;
double P_FC_KLRU = 0.3;

// #define COMPARE_TIME
// #define RELATIVE

FlashCacheLruk::FlashCacheLruk(stats stat) : Policy(stat),
											 dram(FC_K_LRU),
											 kLruSizes(FC_K_LRU, 0),
											 flash(),
											 globalLru(),
											 allObjects(),
											 credits(0),
											 lastCreditUpdate(0),
											 dramSize(0),
											 flashSize(0),
											 counter(0)
{
}

FlashCacheLruk::~FlashCacheLruk() {}

size_t FlashCacheLruk::get_bytes_cached() const
{
	return dramSize + flashSize;
}

size_t FlashCacheLruk::process_request(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}
	counter++;

	double currTime = r->time;
	// 更新flash的写入配额
	updateCredits(currTime);
	bool updateWrites = true;
	uint32_t mfuKid = 0;

	// #ifdef COMPARE_TIME
	//	updateDramFlashiness(currTime);
	// #else
	//	updateDramFlashiness();
	// #endif

	auto searchRKId = allObjects.find(r->kid);
	if (searchRKId != allObjects.end()) // 请求对象存在
	{
		/*
		 * The object exists in flashCache system. If the sizes of the
		 * current Request and previous Request differ then the previous
		 * Request is removed from the flashcache system. Otherwise, one
		 * needs to update the hitrate and its place in the globalLru.
		 * If it is in the cache, one needs as well to update the
		 * 'flashiness' value and its place in the dram MFU and dram LRU
		 */
		FlashCacheLruk::Item &item = searchRKId->second;
		if (r->size() == item.size) // 请求大小与对象大小相同
		{
			if (!warmup)
			{
				stat.hits++;
			}

			// 提升到全局LRU队列头部
			globalLru.erase(item.globalLruIt);
			globalLru.emplace_front(item.kId);
			item.globalLruIt = globalLru.begin();

			if (item.isInDram) // 若对象在DRAM中
			{

				if (!warmup)
				{
					stat.hits_dram++;
				}

				// 对象所在的队列号
				size_t qN = item.queueNumber;
				//				std::pair<uint32_t, double> p = *(item.dramLocation);
				// #ifdef COMPARE_TIME
				//				p.second += hitCredit(item, currTime);
				// #else
				//				p.second += hitCredit(item);
				// #endif

				// 从原队列中删除
				dram[qN].erase(item.dramLocation);
				kLruSizes[qN] -= item.size;
				dramSize -= item.size;

				// 若不是最后一个队列，提升到下一个队列
				if ((qN + 1) != FC_K_LRU)
				{
					qN++;
				}
				else
				{
					updateWrites = false;
				}

				std::vector<uint32_t> objects{r->kid};
				// 插入到下一个队列
				dramAddandReorder(objects, r->size(), qN, updateWrites, warmup);
			}
			else // 若对象在flash中
			{
				if (!warmup)
				{
					stat.hits_flash++;
				}
			}
			item.lastAccessInTrace = counter;
			item.last_accessed = currTime;
			lastCreditUpdate = r->time;
			return 1;
		}
		else // 对象大小有变化，更新，删除对象再重插入
		{	 // Size changed

			globalLru.erase(item.globalLruIt);

			if (item.isInDram) // 对象在dram中
			{
				// 在指定队列中删除对象
				size_t qN = item.queueNumber;
				dram[qN].erase(item.dramLocation);
				kLruSizes[qN] -= item.size;
				dramSize -= item.size;
			}
			else // 对象在flash中
			{	 // in flash
				flash.erase(item.flashIt);
				flashSize -= item.size;
			}

			allObjects.erase(item.kId);
		}
	}
	/*
	 * The Request doesn't exist in the system. We always insert new Requests
	 * to the DRAM at the beginning of the last queue.
	 *
	 * 2. While (object not inserted to the DRAM)
	 *	2.1  if (item.size() + dramSize <= DRAM_SIZE_FC_KLRU) -
	 *		insert item to the dram and return
	 *	2.2 if (not enough credits) - remove the least recently used item
	 *		in the dram until there is a place. return to 2
	 * 	2.3 if (possible to move from DRAM to flash) -
	 *		move items from DRAM to flash. back to 2.
	 *	2.4 remove from global lru. back to 2
	 */

	// 插入新对象
	FlashCacheLruk::Item newItem;
	newItem.kId = r->kid;
	newItem.size = r->size();
	newItem.isInDram = true;
	newItem.last_accessed = r->time;
	newItem.lastAccessInTrace = counter;

	assert(((unsigned int)newItem.size) <= DRAM_SIZE_FC_KLRU);

	while (true)
	{
		// 若dram中有足够空间，直接插入dram
		if (newItem.size + dramSize <= DRAM_SIZE_FC_KLRU)
		{
			// If we have place in the dram insert the new item to the beginning of the last queue.

			globalLru.emplace_front(newItem.kId);
			newItem.globalLruIt = globalLru.begin();
			allObjects[newItem.kId] = newItem;
			lastCreditUpdate = r->time;

			std::vector<uint32_t> objects{r->kid};
			// 插入到第一个队列
			dramAdd(objects, r->size(), 0, true, warmup);

			return PROC_MISS;
		}
		// If we don't have enough space in the dram, we can move MRU items to the flash
		// or to delete LRU items

		// dram中没有足够空间，需要驱逐对象
		// 从后往前遍历队列，找到第一个MRU对象
		for (int i = FC_K_LRU - 1; i >= 0; i--)
		{ // Find the MRU item
			if (kLruSizes[i] > 0)
			{
				mfuKid = ((dram[i]).front()).first;
				break;
			}
		}

		FlashCacheLruk::Item &mfuItem = allObjects[mfuKid];
		size_t qN = mfuItem.queueNumber;

		assert(mfuItem.size > 0);
		//		if (credits < (double) mfuItem.size) {
		//			// If we can't write into the flash we need to make room in the dram
		//			if (!warmup) {stat.credit_limit++;}
		//
		//			while (newItem.size + dramSize > DRAM_SIZE_FC_KLRU ) {
		//				// -------------
		//				// Need to extract the last items from the last queue until
		//				// there will be enough space. Then to insert the new item at the
		//				// beginning of the last queue
		//				// ------------
		//
		//				uint32_t lruKid = ((dram[0]).back()).first;
		//				FlashCacheLruk::Item& lruItem = allObjects[lruKid];
		//
		//				assert(lruItem.size > 0);
		//				dram[0].erase(lruItem.dramLocation);
		//				globalLru.erase(lruItem.globalLruIt);
		//				kLruSizes[0] -= lruItem.size;
		//				dramSize -= lruItem.size;
		//				allObjects.erase(lruKid);
		//			}
		//			continue;
		//		} else {
		// We can write items to the flash

		// 若flash空间足够，将MRU对象插入到flash
		if (flashSize + mfuItem.size <= FLASH_SIZE_FC_KLRU)
		{
			// If we have enough space in the flash, we will insert the MRU item
			// to the flash

			mfuItem.isInDram = false;
			dram[qN].erase(mfuItem.dramLocation);
			mfuItem.dramLocation = ((dram[0]).end());

			flash.emplace_front(mfuKid);
			dramSize -= mfuItem.size;
			mfuItem.flashIt = flash.begin();

			credits -= mfuItem.size;
			kLruSizes[qN] -= mfuItem.size;
			flashSize += mfuItem.size;
			if (!warmup)
			{
				stat.writes_flash++;
				stat.flash_bytes_written += mfuItem.size;
			}
		}
		else // 若flash空间不足，驱逐全局LRU末尾对象
		{
			// If we don't have space in the flash, we will delete the GLRU item
			// and make room for the new item
			// 全局LRU队列末尾对象
			uint32_t globalLruKid = globalLru.back();
			FlashCacheLruk::Item &globalLruItem = allObjects[globalLruKid];
			globalLru.erase(globalLruItem.globalLruIt);
			if (globalLruItem.isInDram) // 若在dram中
			{
				// 从对应队列中删除
				size_t dGqN = globalLruItem.queueNumber;
				dram[dGqN].erase(globalLruItem.dramLocation);
				kLruSizes[dGqN] -= globalLruItem.size;
				dramSize -= globalLruItem.size;
			}
			else // 若在flash中
			{
				flash.erase(globalLruItem.flashIt);
				flashSize -= globalLruItem.size;
			}
			allObjects.erase(globalLruKid);
		}
		//		}
	}
	assert(false);
	return PROC_MISS;
}

void FlashCacheLruk::updateCredits(const double &currTime)
{
	double elapsed_secs = currTime - lastCreditUpdate;
	credits += elapsed_secs * LRUK_FLASH_RATE;
}

// void FlashCacheLruk::updateDramFlashiness(const double& currTime) {
//	double mul;
// #ifdef COMPARE_TIME
//	assert(currTime >= 0);
//	double elapsed_secs = currTime - lastCreditUpdate;
//        mul = exp(-elapsed_secs / K_FC_KLRU);
// #else
//	assert(currTime == -1);
//	mul = exp(-1 / K_FC_KLRU);
// #endif
//	for(dramIt it = dram.begin(); it != dram.end(); it++) {
//                 it->second = it->second * mul;
//	}
// }

double FlashCacheLruk::hitCredit(const Item &item, const double &currTime) const
{
	double diff;
#ifdef COMPARE_TIME
	diff = currTime - item.last_accessed;
#else
	assert(currTime == -1);
	assert(item.lastAccessInTrace < counter);
	diff = counter - item.lastAccessInTrace;
#endif
	assert(diff != 0);
	double mul = exp(-diff / K_FC_KLRU);
	return ((1 - mul) * (L_FC_KLRU / diff));
}

// dram中插入指定队列，不进行驱逐(要提前保证空间足够)
void FlashCacheLruk::dramAdd(std::vector<uint32_t> &objects,
							 size_t sum __attribute__((unused)),
							 size_t k,
							 bool updateWrites __attribute__((unused)),
							 bool warmup __attribute__((unused)))
{

	for (const uint32_t &elem : objects)
	{
		FlashCacheLruk::Item &item = allObjects[elem];
		std::pair<uint32_t, double> it;
		it.first = elem;
		it.second = 0;
		dram[k].emplace_front(it);
		item.dramLocation = dram[k].begin();
		item.queueNumber = k;
		dramSize += item.size;
		kLruSizes[k] += item.size;
		if (k != 0)
		{
			assert(kLruSizes[k] <= FC_KLRU_QUEUE_SIZE);
		}
	}
}

// 在dram的lru-k中，将对象插入到指定队列
void FlashCacheLruk::dramAddandReorder(std::vector<uint32_t> &objects,
									   size_t sum,
									   size_t k,
									   bool updateWrites,
									   bool warmup)
{

	assert(k < FC_K_LRU);

	std::vector<uint32_t> newObjects;

	size_t newSum = 0;

	if (k != 0) // 若不是第一个队列，驱逐对象降级
	{
		// 驱逐直到空间足够
		while (sum + kLruSizes[k] > FC_KLRU_QUEUE_SIZE)
		{
			assert(kLruSizes[k] > 0);
			assert(dram[k].size() > 0);
			// 队列的末尾对象
			uint32_t elem = (dram[k].back()).first;
			FlashCacheLruk::Item &item = allObjects[elem];
			dram[k].pop_back();
			kLruSizes[k] -= item.size;
			dramSize -= item.size;
			// saving the extracted items in order to put them in lower queue
			newSum += item.size;
			newObjects.emplace_back(elem);
		}
	}
	else // 若为第一个队列，直接驱逐
	{
		while (sum + dramSize > DRAM_SIZE_FC_KLRU)
		{
			// shouldnt get to here
			assert(0);
			assert(kLruSizes[k] > 0);
			assert(dram[k].size() > 0);
			// 队列的末尾对象
			uint32_t elem = (dram[k].back()).first;
			FlashCacheLruk::Item &item = allObjects[elem];
			dram[k].pop_back();
			kLruSizes[k] -= item.size;
			dramSize -= item.size;
			// in k=0 we don't need to save the extracted items since we are dumping them forever
			globalLru.erase(item.globalLruIt);
		}
	}

	if (!updateWrites)
	{
		assert(newObjects.size() == 0);
		assert(newSum == 0);
	}

	//		for (const uint32_t& elem : objects) {
	//			FlashCacheLruk::Item& item = allObjects[elem];
	//			std::pair<uint32_t, double> it;
	//			it.first = elem;
	//			it.second=0;
	//			dram[k].emplace_front(it);
	//			item.dramLocation = dram[k].begin();
	//			item.queueNumber = k;
	//			kLruSizes[k] += item.size;
	//			dramSize += item.size;
	//			if (k > 0)
	//				{assert(kLruSizes[k] <= FC_KLRU_QUEUE_SIZE);}
	//		}

	// 将要插入的对象插入到当前队列
	dramAdd(objects, sum, k, updateWrites, warmup);

	// 将驱逐的对象插入到前一级队列
	if (k > 0 && newObjects.size() > 0)
	{
		dramAddandReorder(newObjects, newSum, k - 1, true, warmup);
	}
}

void FlashCacheLruk::dump_stats(void)
{
	assert(stat.apps->size() == 1);
	uint32_t appId = 0;
	for (const auto &app : *stat.apps)
	{
		appId = app;
	}
	std::string filename{stat.policy
#ifdef RELATIVE
						 + "-relative" + std::to_string(P_FC_KLRU)
#endif
#ifdef COMPARE_TIME
						 + "-time"
#else
						 + "-place"
#endif
						 + "-app" + std::to_string(appId) + "-flash_mem" + std::to_string(FLASH_SIZE_FC_KLRU) + "-dram_mem" + std::to_string(DRAM_SIZE_FC_KLRU) + "-K" + std::to_string(K_FC_KLRU)};
	std::ofstream out{filename};
	out << "dram size " << DRAM_SIZE_FC_KLRU << std::endl;
	out << "flash size " << FLASH_SIZE_FC_KLRU << std::endl;
	out << "initial credit " << LRUK_INITIAL_CREDIT << std::endl;
	out << "#credits per sec " << LRUK_FLASH_RATE << std::endl;
#ifdef COMPARE_TIME
	out << "Time" << std::endl;
#else
	out << "Place" << std::endl;
#endif

#ifdef RELATIVE
	out << "P_FC " << P_FC_KLRU << std::endl;
#endif
	out << "K " << K_FC_KLRU << std::endl;
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
	out << "hwadsfasfsafasfsdaffasgasfdsdafasfgsfg" << std::endl;
	out << "key,rate" << std::endl;
	out << "Amount of lru queues: " << FC_K_LRU << std::endl;
	for (size_t i = 0; i < FC_K_LRU; i++)
	{
		out << "dram[" << i << "] has " << dram[i].size() << " items" << std::endl;
		out << "dram[" << i << "] size written " << kLruSizes[i] << std::endl;
	}
	out << "Total dram filled size " << dramSize << std::endl;
	// for (dramIt it = dram.begin(); it != dram.end(); it++) {
	//	out << it->first << "," << it->second << std::endl;
	// }
}
