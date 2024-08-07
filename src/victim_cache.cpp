#include "victim_cache.h"

static bool file_defined = false;
static double lastRequest = 0;

VictimCache::VictimCache(stats stat) : Policy(stat),
									   dram(),
									   flash(),
									   allObjects(),
									   dramSize(0),
									   flashSize(0),
									   missed_bytes(0),
									   out()
{
}

VictimCache::~VictimCache() {}

size_t VictimCache::get_bytes_cached() const { return dramSize + flashSize; }

size_t VictimCache::process_request(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}
	lastRequest = r->time;

	auto searchRKId = allObjects.find(r->kid);
	bool hitFlash = false;
	if (searchRKId != allObjects.end()) // 找到了
	{
		FlashCache::Item &item = searchRKId->second;
		if (item.isInDram) // 在dram中
		{
			dram.erase(item.dramLruIt);
			dramSize -= item.size;
		}
		else // 在flash中
		{
			flash.erase(item.flashIt);
			flashSize -= item.size;
			hitFlash = true;
		}
		if (r->size() == item.size) // 请求的大小和item的大小相同，命中
		{
			if (!warmup)
			{
				stat.hits++;
				if (hitFlash)
				{
					stat.hits_flash++;
				}
				else
				{
					stat.hits_dram++;
				}
			}
			// 将item重新插入到dram lru头部
			insertToDram(item, warmup);
			return 1;
		}
		else // 请求的大小与item的大小不同，即有更新，则先删除
		{
			allObjects.erase(item.kId);
		}
	}

	// 未命中或有更新，将新item插入到dram中
	FlashCache::Item newItem;
	newItem.kId = r->kid;
	newItem.size = r->size();
	newItem.isInDram = true;
	assert(((unsigned int)newItem.size) <= DRAM_SIZE);
	insertToDram(newItem, warmup);
	allObjects[newItem.kId] = newItem;
	if (!warmup)
	{
		missed_bytes += newItem.size;
	}
	return PROC_MISS;
}

void VictimCache::insertToDram(FlashCache::Item &item, bool warmup)
{
	// 若dram容量不足，则将dram中的最久未使用的item移动到flash中
	while (item.size + dramSize > DRAM_SIZE)
	{
		// dram队尾的item
		uint32_t lruDramItemKey = dram.back();
		FlashCache::Item &lruDramItem = allObjects[lruDramItemKey];
		// 从dram中移除
		dram.erase(lruDramItem.dramLruIt);
		dramSize -= lruDramItem.size;
		// 若flash容量不够，则进行驱逐
		while (lruDramItem.size + flashSize > FLASH_SIZE)
		{
			// flash队尾的item
			uint32_t flashDramItemKey = flash.back();
			FlashCache::Item &lruFlashItem = allObjects[flashDramItemKey];
			flash.erase(lruFlashItem.flashIt);
			flashSize -= lruFlashItem.size;
			allObjects.erase(lruFlashItem.kId);
		}
		// 将lruDramItem移动到flash中
		flash.emplace_front(lruDramItemKey);
		lruDramItem.flashIt = flash.begin();
		lruDramItem.isInDram = false;
		flashSize += lruDramItem.size;
		if (!warmup)
		{
			stat.writes_flash++;
			stat.flash_bytes_written += lruDramItem.size;
		}
	}
	// 将item插入到dram中
	dram.emplace_front(item.kId);
	item.dramLruIt = dram.begin();
	item.isInDram = true;
	dramSize += item.size;
}

void VictimCache::dump_stats(void)
{
	std::string appids{};
	if (all_apps)
	{
		appids = "-all,";
	}
	else
	{
		for (const auto &app : *stat.apps)
			appids += std::to_string(app) + ",";
	}
	appids = appids.substr(0, appids.length() - 1);
	std::string filename{stat.policy + "-app" + appids + "-flash_mem" + std::to_string(FLASH_SIZE) + "-dram_mem" + std::to_string(DRAM_SIZE)};
	if (!file_defined)
	{
		out.open(filename);
		file_defined = true;
	}
	out << "Last Request was at :" << lastRequest << std::endl;
	out << "dram size " << DRAM_SIZE << std::endl;
	out << "flash size " << FLASH_SIZE << std::endl;
	out << "#accesses " << stat.accesses << std::endl;
	out << "#global hits " << stat.hits << std::endl;
	out << "#dram hits " << stat.hits_dram << std::endl;
	out << "#flash hits " << stat.hits_flash << std::endl;
	out << "hit rate " << double(stat.hits) / stat.accesses << std::endl;
	out << "#writes to flash " << stat.writes_flash << std::endl;
	out << "#bytes written to flash " << stat.flash_bytes_written << std::endl;
	out << "#missed bytes written to dram " << missed_bytes << std::endl;
}
