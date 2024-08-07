#ifndef FLASH_CACHE_H
#define FLASH_CACHE_H

#include <vector>
#include <iostream>
#include <unordered_map>
#include <list>
#include <cassert>
#include "policy.h"

/*
 * These parameters define how many bytes the DRAM
 * and flash can hold. These parametrs can be changed
 */
extern size_t DRAM_SIZE;
extern size_t FLASH_SIZE;
const size_t FLASH_RATE = 1024 * 1024;
const size_t INITIAL_CREDIT = 1;
extern double K;
extern size_t L_FC;
extern double P_FC;

// 维护一个全局lru队列，一个dram lru队列，一个dram flashiness队列，一个flash队列
// 从dram中驱逐时，若能够将flashiness值最大的对象移动到flash中，则优先进行移动
// 否则从dram lru队列中进行驱逐，直到dram空间足够
// flash中的对象不进行移动，但会失效：1、被更新时 2、在全局lru中被驱逐时
// flash空间不足时，从全局lru队列中进行驱逐(也会驱逐dram中的对象)，直到flash空间足够
class FlashCache : public Policy
{
protected:
	typedef std::list<std::pair<uint32_t, double>>::iterator dramIt;
	typedef std::list<uint32_t>::iterator keyIt;

	struct Item
	{
		uint32_t kId;
		int32_t size;
		double last_accessed;	  // 实际时间戳
		size_t lastAccessInTrace; // 逻辑时间戳
		bool isInDram;
		dramIt dramLocation; // 对象在dram flashiness队列中的位置
		keyIt dramLruIt;	 // 对象在dram lru队列中的位置
		keyIt flashIt;		 // 对象在flash队列中的位置
		keyIt globalLruIt;	 // 对象在全局lru队列中的位置

		Item() : kId(0), size(0), last_accessed(0), lastAccessInTrace(0),
				 isInDram(true), dramLocation(), dramLruIt(), flashIt(), globalLruIt() {}
	};

	std::list<std::pair<uint32_t, double>> dram; // dram flashiness队列
	std::list<uint32_t> dramLru;				 // dram lru队列
	std::list<uint32_t> flash;					 // flash队列
	std::list<uint32_t> globalLru;				 // 全局lru队列

	std::unordered_map<uint32_t, Item> allObjects; // 全局哈希索引
	/*
	 * One can move objects from the DRAM to the flash only if he has enough
	 * credits. Number of current credits should be higher then the object
	 * size. Each delta T (FLASH_RATE * delta T) are added.
	 */
	double credits;

	/*
	 * The last time the credits where updates
	 */
	double lastCreditUpdate;

	size_t dramSize;  // dram中对象总大小
	size_t flashSize; // flash中对象总大小

	size_t counter;

	void updateCredits(const double &currTime);
	void updateDramFlashiness(const double &currTime = -1);
	double hitCredit(const Item &item, const double &currTime = -1) const;
	void dramAdd(const std::pair<uint32_t, double> &p,
				 dramIt beginPlace,
				 Item &item);
	void dramAddFirst(Item &item);
	friend class VictimCache;

public:
	FlashCache(stats stat);
	~FlashCache();
	size_t process_request(const Request *r, bool warmup);
	size_t get_bytes_cached() const;
	void dump_stats(void);
};

#endif
