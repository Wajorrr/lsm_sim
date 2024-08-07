#include <cassert>
#include "lruk.h"

size_t K_LRU = 8;
size_t KLRU_QUEUE_SIZE = 1024;

Lruk::Lruk(stats stat)
	: Policy(stat), kLruSizes(K_LRU, 0), kLru(K_LRU), allObjects(), kLruHits(K_LRU, 0), kLruNumWrites(K_LRU, 0)
{
	assert(K_LRU >= 1);
}

Lruk::~Lruk() {}

size_t Lruk::get_bytes_cached() const
{
	size_t bytesCached = 0;
	for (size_t sizeQueue : kLruSizes)
	{
		bytesCached += sizeQueue;
	}
	return bytesCached;
}

size_t Lruk::process_request(const Request *r, bool warmup)
{
	if (!warmup)
	{
		stat.accesses++;
	}

	bool updateWrites = true;
	auto searchRKId = allObjects.find(r->kid);
	if (searchRKId != allObjects.end()) // 请求对象存在
	{
		Lruk::LKItem &item = searchRKId->second;
		// 对象所在的队列号
		size_t qN = item.queueNumber;
		// 从对应队列中删除对象
		kLru[qN].erase(item.iter);
		kLruSizes[qN] -= item.size;
		if (r->size() == item.size) // 请求大小与对象大小相同，则进行提升
		{
			if (!warmup)
			{
				stat.hits++;
				kLruHits[qN]++;
			}
			if ((qN + 1) != K_LRU) // 若不是最后一个队列，提升到下一个队列
			{
				qN++;
			}
			else // 若是最后一个队列，提升到当前队列头部
			{
				updateWrites = false;
			}
			std::vector<uint32_t> objects{r->kid};
			// 插入到下一个队列
			insert(objects, r->size(), qN, updateWrites, warmup);
			return 1;
		}
		else // 对象大小有变化，更新，则删除对象再重插入
		{
			allObjects.erase(item.kId);
		}
	}
	// 插入新对象
	Lruk::LKItem newItem;
	newItem.kId = r->kid;
	newItem.size = r->size();
	newItem.queueNumber = 0;
	allObjects[r->kid] = newItem;
	std::vector<uint32_t> objects{r->kid};
	// 插入到第一级队列
	insert(objects, r->size(), 0, true, warmup);
	return PROC_MISS;
}

// 将对象插入到对应队列中
void Lruk::insert(std::vector<uint32_t> &objects,
				  size_t sum,
				  size_t k,
				  bool updateWrites,
				  bool warmup)
{

	assert(k < K_LRU);

	std::vector<uint32_t> newObjects;
	size_t newSum = 0;
	// 驱逐直到空间足够
	while (sum + kLruSizes[k] > KLRU_QUEUE_SIZE)
	{
		assert(kLruSizes[k] > 0);
		assert(kLru[k].size() > 0);
		// 驱逐队列中最后一个对象
		uint32_t elem = kLru[k].back();
		kLru[k].pop_back();
		Lruk::LKItem &item = allObjects[elem];
		kLruSizes[k] -= item.size;
		if (k > 0) // 若不是第一个队列，则在驱逐后插入前一级队列
		{
			newSum += item.size;
			newObjects.emplace_back(elem);
		}
		else
		{
			allObjects.erase(elem);
		}
	}
	if (!updateWrites)
	{
		assert(newObjects.size() == 0);
		assert(newSum == 0);
	}

	// 将对象插入到当前队列
	for (const uint32_t &elem : objects)
	{
		Lruk::LKItem &item = allObjects[elem];
		kLru[k].emplace_front(elem);
		item.iter = kLru[k].begin();
		item.queueNumber = k;
		kLruSizes[k] += item.size;
		if (!warmup && updateWrites)
		{
			kLruNumWrites[k] += item.size;
		}
		assert(kLruSizes[k] <= KLRU_QUEUE_SIZE);
	}

	// 将驱逐的对象插入到前一级队列
	if (k > 0 && newObjects.size() > 0)
	{
		insert(newObjects, newSum, k - 1, true, warmup);
	}
}

void Lruk::dump_stats(void)
{
	assert(stat.apps->size() == 1);
	uint32_t appId = 0;
	for (const auto &app : *stat.apps)
	{
		appId = app;
	}
	std::string filename{stat.policy + "-app" + std::to_string(appId) + "-K" + std::to_string(K_LRU) + "-QSize" + std::to_string(KLRU_QUEUE_SIZE)};
	std::ofstream out{filename};
	out << "K_LRU (number of queues) " << K_LRU << std::endl;
	out << "queue size " << KLRU_QUEUE_SIZE << std::endl;
	out << "#accesses " << stat.accesses << std::endl;
	out << "#global hits " << stat.hits << std::endl;
	out << "hit rate " << double(stat.hits) / stat.accesses << std::endl;
	for (size_t i = 0; i < K_LRU; i++)
	{
		out << "queue " << i << " hits: " << kLruHits[i] << std::endl;
		out << "queue " << i << " hit rate: " << double(kLruHits[i]) / stat.accesses << std::endl;
		out << "queue " << i << " bytes written: " << kLruNumWrites[i] << std::endl;
	}
}
