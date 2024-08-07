#include <iostream>
#include <math.h>
#include <fstream>
#include "flash_cache.h"

size_t DRAM_SIZE = 51209600;
size_t FLASH_SIZE = 51209600;
double K = 1;
size_t L_FC = 1;
double P_FC = 0.3;

// #define COMPARE_TIME
// #define RELATIVE

FlashCache::FlashCache(stats stat) : Policy(stat),
									 dram(),
									 dramLru(),
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

FlashCache::~FlashCache() {}

size_t FlashCache::get_bytes_cached() const
{
	return dramSize + flashSize;
}

size_t FlashCache::process_request(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}
	counter++;

	double currTime = r->time;
	updateCredits(currTime);

#ifdef COMPARE_TIME
	updateDramFlashiness(currTime);
#else
	updateDramFlashiness();
#endif

	auto searchRKId = allObjects.find(r->kid);
	if (searchRKId != allObjects.end())
	{
		/*
		 * The object exists in flashCache system. If the sizes of the
		 * current Request and previous Request differ then the previous
		 * Request is removed from the flashcache system. Otherwise, one
		 * needs to update the hitrate and its place in the globalLru.
		 * If it is in the cache, one needs as well to update the
		 * 'flashiness' value and its place in the dram MFU and dram LRU
		 */
		FlashCache::Item &item = searchRKId->second;
		// 对象大小没改变
		if (r->size() == item.size)
		{
			if (!warmup)
			{
				stat.hits++;
			}
			// 将对象移动到全局lru列表头部
			globalLru.erase(item.globalLruIt);
			globalLru.emplace_front(item.kId);
			item.globalLruIt = globalLru.begin();
			// 如果对象在DRAM中，更新到dram lru头部，并更新flashiness
			if (item.isInDram)
			{
				if (!warmup)
				{
					stat.hits_dram++;
				}
				dramLru.erase(item.dramLruIt);
				dramLru.emplace_front(item.kId);
				item.dramLruIt = dramLru.begin();
				// <对象id, flashiness>
				std::pair<uint32_t, double> p = *(item.dramLocation);
#ifdef COMPARE_TIME
				p.second += hitCredit(item, currTime);
#else
				// 更新flashiness
				p.second += hitCredit(item);
#endif
				// 在dram中的lru指针
				dramIt tmp = item.dramLocation;
				// 更新在flashiness列表中的位置
				dramAdd(p, tmp, item);
				dram.erase(tmp);
			}
			else // 对象在flash中
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
		else // 对象大小改变了，从原来的所有索引中删除，然后重新插入
		{
			globalLru.erase(item.globalLruIt);
			if (item.isInDram)
			{
				dram.erase(item.dramLocation);
				dramLru.erase(item.dramLruIt);
				dramSize -= item.size;
			}
			else
			{
				flash.erase(item.flashIt);
				flashSize -= item.size;
			}
			allObjects.erase(item.kId);
		}
	}
	/*
	 * The Request doesn't exist in the system. We always insert new Requests
	 * to the DRAM.
	 * 2. While (object not inserted to the DRAM)
	 *	2.1  if (item.size() + dramSize <= DRAM_SIZE) -
	 *		insert item to the dram and return
	 *	2.2 if (not enough credits) - remove the least recently used item
	 *		in the dram until there is a place. return to 2
	 * 	2.3 if (possible to move from DRAM to flash) -
	 *		move items from DRAM to flash. back to 2.
	 *	2.4 remove from global lru. back to 2
	 */
	// 创建新对象，首先插入到DRAM中
	FlashCache::Item newItem;
	newItem.kId = r->kid;
	newItem.size = r->size();
	newItem.isInDram = true;
	newItem.last_accessed = r->time;
	newItem.lastAccessInTrace = counter;
	assert(((unsigned int)newItem.size) <= DRAM_SIZE);
	// 循环：
	// dram中有空间，则直接插入;空间不足，则首先判断是否能将flashiness最大的对象插入flash
	// 若已没有能插入flash的对象，则驱逐DRAM中的对象
	// 将flashiness最大的对象插入flash时，若flash空间不足，则驱逐全局lru队列末尾的对象
	while (true)
	{
		// 若DRAM中有足够空间，直接插入
		if (newItem.size + dramSize <= DRAM_SIZE)
		{
#ifdef RELATIVE
			// 插入到flashiness队列指定头部位置
			dramAddFirst(newItem);
#else
			// 创建一个<对象id, flashiness>
			std::pair<uint32_t, double> p(newItem.kId, INITIAL_CREDIT);
			// 根据flashiness插入到flashiness队列中
			dramAdd(p, dram.begin(), newItem);
#endif
			// 插入dram LRU头部
			dramLru.emplace_front(newItem.kId);
			newItem.dramLruIt = dramLru.begin();
			// 插入全局lru头部
			globalLru.emplace_front(newItem.kId);
			newItem.globalLruIt = globalLru.begin();
			// 全局索引
			allObjects[newItem.kId] = newItem;
			lastCreditUpdate = r->time;
			dramSize += newItem.size;
			return PROC_MISS;
		}
		// DRAM中没有足够空间，需要驱逐
		// 先获取flashiness最大的对象，判断是否可以准入flash
		uint32_t mfuKid = dram.back().first;
		FlashCache::Item &mfuItem = allObjects[mfuKid];
		assert(mfuItem.size > 0);
		// 如果credits不够，不插入flash，从dram中直接驱逐
		if (credits < (double)mfuItem.size)
		{
			if (!warmup)
			{
				stat.credit_limit++;
			}
			// 驱逐直到有足够空间
			while (newItem.size + dramSize > DRAM_SIZE)
			{
				// 获取dram lru队列最后一个对象
				uint32_t lruKid = dramLru.back();
				FlashCache::Item &lruItem = allObjects[lruKid];
				assert(lruItem.size > 0);
				// 从flashiness队列、dram lru队列、全局lru队列中删除
				dram.erase(lruItem.dramLocation);
				dramLru.erase(lruItem.dramLruIt);
				globalLru.erase(lruItem.globalLruIt);
				// 更新dram大小和索引
				dramSize -= lruItem.size;
				allObjects.erase(lruKid);
			}
			continue;
		}
		else // credits足够，则将当前flashiness最大的对象插入flash
		{
			// 若flash中有足够空间，则直接插入
			if (flashSize + mfuItem.size <= FLASH_SIZE)
			{
				// 更新标记
				mfuItem.isInDram = false;
				// 从DRAM中删除
				dram.erase(mfuItem.dramLocation);
				dramLru.erase(mfuItem.dramLruIt);
				// 插入flash队列
				flash.emplace_front(mfuKid);
				// 更新对象的dram位置索引
				mfuItem.dramLocation = dram.end();
				mfuItem.dramLruIt = dramLru.end();
				// 更新对象的flash位置索引
				mfuItem.flashIt = flash.begin();
				// 更新credit、dram大小、flash大小
				credits -= mfuItem.size;
				dramSize -= mfuItem.size;
				flashSize += mfuItem.size;
				if (!warmup)
				{
					stat.writes_flash++;
					stat.flash_bytes_written += mfuItem.size;
				}
			}
			else // flash空间不足，对全局lru队列末尾的对象进行驱逐
			{
				// 获取全局lru队列最后一个对象
				uint32_t globalLruKid = globalLru.back();
				FlashCache::Item &globalLruItem = allObjects[globalLruKid];
				globalLru.erase(globalLruItem.globalLruIt);
				// 如果对象在DRAM中，从DRAM中删除
				if (globalLruItem.isInDram)
				{
					dram.erase(globalLruItem.dramLocation);
					dramLru.erase(globalLruItem.dramLruIt);
					dramSize -= globalLruItem.size;
				}
				else // 如果对象在flash中，从flash中删除
				{
					flash.erase(globalLruItem.flashIt);
					flashSize -= globalLruItem.size;
				}
				allObjects.erase(globalLruKid);
			}
		}
	}
	assert(false);
	return PROC_MISS;
}

// 传入的参数为实际时间戳，以控制写入flash的速率
void FlashCache::updateCredits(const double &currTime)
{
	// 实际时间戳过了多久
	double elapsed_secs = currTime - lastCreditUpdate;
	// 根据实际更新时间差，更新credits
	credits += elapsed_secs * FLASH_RATE;
}

// 根据实际时间戳更新所有dram中对象的flashiness
void FlashCache::updateDramFlashiness(const double &currTime)
{
	double mul;
#ifdef COMPARE_TIME
	assert(currTime >= 0);
	// 实际时间戳过了多久
	double elapsed_secs = currTime - lastCreditUpdate;
	// 时间差越大，mul越小，从1逐渐趋近于0
	mul = exp(-elapsed_secs / K);
#else
	assert(currTime == -1);
	mul = exp(-1 / K);
#endif
	// 遍历flashiness队列，更新flashiness
	for (dramIt it = dram.begin(); it != dram.end(); it++)
	{
		it->second = it->second * mul;
	}
}

// 根据访问时间间隔计算对象的flashiness增加量
double FlashCache::hitCredit(const Item &item, const double &currTime) const
{
	double diff;
#ifdef COMPARE_TIME
	// 距离上一次访问的实际时间间隔
	diff = currTime - item.last_accessed;
#else
	assert(currTime == -1);
	assert(item.lastAccessInTrace < counter);
	// 距离上一次访问的逻辑时间间隔
	diff = counter - item.lastAccessInTrace;
#endif
	assert(diff != 0);
	// 距离上一次访问的时间间隔越大，mul越小
	double mul = exp(-diff / K);
	// 时间间隔越大，访问频率越小，越适合插入flash
	// flashiness增加量越大
	// (1/diff)*e^{-diff}
	return ((1 - mul) * (L_FC / diff));
}

void FlashCache::dramAdd(const std::pair<uint32_t, double> &p,
						 dramIt beginPlace,
						 Item &item)
{
	// 优先队列，按照flashiness升序排序
	for (dramIt it = beginPlace; it != dram.end(); it++)
	{
		if (p.second < it->second)
		{
			dram.insert(it, p);
			// 更新位置索引
			item.dramLocation = --it;
			return;
		}
	}
	// 如果p的flashiness最大，插入到队尾
	dram.emplace_back(p);
	dramIt tmp = dram.end();
	assert(dram.size() > 0);
	tmp--;
	item.dramLocation = tmp;
}

void FlashCache::dramAddFirst(Item &item)
{
	if (dram.size() == 0)
	{
		std::pair<uint32_t, double> p(item.kId, INITIAL_CREDIT);
		dram.emplace_front(p);
		item.dramLocation = dram.begin();
		return;
	}
	dramIt it = dram.begin();
	// 使用了 std::advance 函数，该函数用于将迭代器移动指定的步数
	// 正数表示向后移动，负数表示向前移动
	// std::distance(dram.begin(), dram.end())。这个距离表示 dram 容器中元素的数量
	std::advance(it, ceil(std::distance(dram.begin(), dram.end()) * P_FC));
	std::pair<uint32_t, double> p;
	if (it == dram.end()) // 插入队尾
	{
		assert(dram.size() > 0);
		it--; // 指向最后一个对象的位置
		// flashiness赋值为当前插入位置的flashiness
		p = std::make_pair(item.kId, it->second);
		dram.emplace_back(p); // 插入到队尾
		it++;				  // 指向新插入的对象
		assert(it != dram.end());
		item.dramLocation = it;
		return;
	}
	// 插入到指定位置
	p = std::make_pair(item.kId, it->second);
	dram.insert(it, p);
	it--;
	item.dramLocation = it;
}

void FlashCache::dump_stats(void)
{
	assert(stat.apps->size() == 1);
	uint32_t appId = 0;
	for (const auto &app : *stat.apps)
	{
		appId = app;
	}
	std::string filename{stat.policy
#ifdef RELATIVE
						 + "-relative" + std::to_string(P_FC)
#endif
#ifdef COMPARE_TIME
						 + "-time"
#else
						 + "-place"
#endif
						 + "-app" + std::to_string(appId) + "-flash_mem" + std::to_string(FLASH_SIZE) + "-dram_mem" + std::to_string(DRAM_SIZE) + "-K" + std::to_string(K)};
	std::ofstream out{filename};
	out << "dram size " << DRAM_SIZE << std::endl;
	out << "flash size " << FLASH_SIZE << std::endl;
	out << "initial credit " << INITIAL_CREDIT << std::endl;
	out << "#credits per sec " << FLASH_RATE << std::endl;
#ifdef COMPARE_TIME
	out << "Time" << std::endl;
#else
	out << "Place" << std::endl;
#endif

#ifdef RELATIVE
	out << "P_FC " << P_FC << std::endl;
#endif
	out << "K " << K << std::endl;
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
	out << "key,rate" << std::endl;
	for (dramIt it = dram.begin(); it != dram.end(); it++)
	{
		out << it->first << "," << it->second << std::endl;
	}
}
