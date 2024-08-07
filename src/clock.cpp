#include <iostream>
#include "clock.h"

size_t CLOCK_MAX_VALUE = 15;

Clock::Clock(stats stat) : Policy(stat),
						   clockLru(),
						   allObjects(),
						   lruSize(0),
						   noZeros(0),
						   firstEviction(false),
						   clockIt()
{
}

Clock::~Clock() {}

size_t Clock::get_bytes_cached() const { return lruSize; }

size_t Clock::process_request(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}

	auto searchRKId = allObjects.find(r->kid);
	if (searchRKId != allObjects.end()) // 请求对象存在
	{
		Clock::ClockItem &item = searchRKId->second;
		ClockLru::iterator &clockItemIt = item.clockLruIt;

		if (r->size() == item.size) // 请求大小与对象大小相同
		{
			if (!warmup)
			{
				stat.hits++;
			}
			clockItemIt->second = CLOCK_MAX_VALUE; // 重置对象的clock值
			return 1;
		}
		else // 对象大小有变化，更新
		{
			// 若当前clock指向的对象是当前对象，则将clock指向下一个对象
			if (clockItemIt == clockIt)
			{
				clockIt++;
				if (clockIt == clockLru.end())
				{
					clockIt = clockLru.begin();
				}
			}
			// 删除当前对象
			clockLru.erase(clockItemIt);
			lruSize -= item.size;
			allObjects.erase(item.kId);
		}
	}
	// 插入新对象
	Clock::ClockItem newItem;
	newItem.kId = r->kid;
	newItem.size = r->size();

	assert((size_t)newItem.size <= stat.global_mem);

	// 缓存空间不足，驱逐直到空间足够
	while (lruSize + newItem.size > stat.global_mem)
	{
		// 标记为已经进行了至少一次驱逐
		if (!firstEviction)
		{
			firstEviction = true;
		}
		bool isDeleted = false;
		ClockLru::iterator tmpIt, startIt = clockIt;

		// 移动clock指针进行遍历
		while (clockIt != clockLru.end())
		{
			assert(clockIt->second <= CLOCK_MAX_VALUE);
			// 若当前对象的clock值为0，则删除
			if (clockIt->second == 0)
			{
				tmpIt = clockIt;
				tmpIt++;
				deleteItem(clockIt->first);
				// 标记为进行了删除
				isDeleted = true;
				break;
			}
			else // 否则，将clock值减1
			{
				clockIt->second--;
			}
			// 移动clock指针
			clockIt++;
		}
		// 若没有进行删除，继续遍历剩余对象
		if (!isDeleted)
		{
			// 从头开始遍历到start位置
			clockIt = clockLru.begin();
			assert(clockIt->second <= CLOCK_MAX_VALUE);
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
		// 已经遍历完一圈了，仍然没有进行删除，则直接删除当前对象
		if (!isDeleted)
		{
			if (!warmup)
			{
				noZeros++;
			}
			assert(clockLru.size() > 0);
			assert(clockIt != clockLru.end());
			tmpIt = clockIt;
			tmpIt++;
			deleteItem(clockIt->first);
		}
		// 删除之后，将clock指针指向下一个对象
		clockIt = (tmpIt == clockLru.end()) ? clockLru.begin() : tmpIt;
	}

	// 插入新对象
	std::pair<uint32_t, size_t> p;
	// 若已经进行了至少一次驱逐，则将新对象的clock值设为CLOCK_MAX_VALUE
	p = firstEviction
			? std::make_pair(newItem.kId, CLOCK_MAX_VALUE)
			: std::make_pair(newItem.kId, (size_t)0);
	if (clockLru.size() == 0)
	{
		clockLru.emplace_front(p);
		clockIt = clockLru.begin();
		newItem.clockLruIt = clockLru.begin();
	}
	else // 插入到clock指针的位置，将clock指针指向新对象
	{
		clockLru.insert(clockIt, p);
		ClockLru::iterator it = clockIt;
		it--;
		newItem.clockLruIt = it;
	}
	allObjects[newItem.kId] = newItem;
	lruSize += newItem.size;
	return PROC_MISS;
}

// 删除一个对象
void Clock::deleteItem(uint32_t keyId)
{
	auto searchRKId = allObjects.find(keyId);
	assert(searchRKId != allObjects.end());
	Clock::ClockItem &item = searchRKId->second;
	clockLru.erase(item.clockLruIt);
	lruSize -= item.size;
	allObjects.erase(keyId);
}

void Clock::dump_stats(void)
{
	assert(stat.apps->size() == 1);
	uint32_t appId = 0;
	for (const auto &app : *stat.apps)
	{
		appId = app;
	}
	std::string filename{stat.policy + "-app" + std::to_string(appId) + "-globalMemory" + std::to_string(stat.global_mem) + "-clockMaxValue" + std::to_string(CLOCK_MAX_VALUE)};
	std::ofstream out{filename};
	out << "global_mem " << stat.global_mem << std::endl;
	out << "clock max value " << CLOCK_MAX_VALUE << std::endl;
	out << "#accesses " << stat.accesses << std::endl;
	out << "#global hits " << stat.hits << std::endl;
	out << "hit rate " << double(stat.hits) / stat.accesses << std::endl;
	out << "noZeros " << noZeros << std::endl;
}
