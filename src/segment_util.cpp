#include <algorithm>
#include <math.h>
#include "segment_util.h"

SegmentUtil::SegmentUtil(stats stat) : Policy(stat),
									   objects(),
									   allObjects(),
									   dataSize(0),
									   bytesCached(0),
									   numHash(0),
									   numInserted(0)
{
	assert(exp2(bits_for_page) == number_of_pages);
	assert(segment_util % page_size == 0);
}

SegmentUtil::~SegmentUtil() {}

size_t SegmentUtil::process_request(const Request *r, bool woormup __attribute__((unused)))
{
	// 若总空间已经超过上限，将当前已缓存的数据按照大小排序，插入页中，并输出统计信息
	if (r->size() + dataSize > top_data_bound)
	{
		// 对象从大到小排序
		std::sort(objects.begin(), objects.end(), compareSizes);
		// 初始化页数组
		std::vector<size_t> pageSizes(number_of_pages, 0);
		// 遍历对象，依次插入到页中
		for (size_t i = 0; i < objects.size(); i++)
		{
			SegmentUtil::SUItem &item = objects[i];
			uint128_t lastHash = item.kId;
			// 多次尝试插入，尽可能减少哈希冲突造成的插入失败
			for (size_t j = 0; j < num_hash_functions; j++)
			{
				// 生成哈希值
				lastHash = MurmurHash3_x64_128((void *)&lastHash, sizeof(lastHash), 0);
				// 对哈希值取模，计算所属页号
				uint32_t pageId = lastHash & ((1lu << bits_for_page) - 1);
				assert(pageId < number_of_pages);
				// no space in the page
				if (pageSizes[pageId] == page_size)
				{
					continue;
				}
				assert(page_size >= pageSizes[pageId]);
				// 剩余空间大小
				size_t head = page_size - pageSizes[pageId];
				// 剩余空间足够
				if (item.size <= head)
				{
					item.inserted = true;
					pageSizes[pageId] += item.size;
					bytesCached += item.size;
					numHash += (j + 1);
					numInserted++;
					break;
				}
				// 剩余空间不够
				assert(item.size > head);
				// 计算还需要的整页数
				size_t numPages = (item.size - head) / page_size;
				assert(item.size >= (head + numPages * page_size));
				// 是否还有不足一页的数据需要额外占用一页
				size_t tail = item.size - (head + numPages * page_size);
				// no continuous memory
				// 若连续页数不足，跳过
				if (pageId + numPages >= number_of_pages)
				{
					continue;
				}
				if (tail > 0 && (pageId + numPages + 1 >= number_of_pages))
				{
					continue;
				}

				// 连续页数足够
				bool isEmpty = true;
				// 检查连续页中是否已经有数据
				for (size_t i = 1; i <= numPages; i++)
				{
					if (pageSizes[pageId + i] != 0)
					{
						isEmpty = false;
					}
				}
				// 若已经有数据，跳过
				if (!isEmpty)
				{
					continue;
				}
				// 若最后一页不够容纳剩余数据，跳过
				if (pageSizes[pageId + numPages + 1] + tail > page_size)
				{
					continue;
				}
				// 插入数据
				item.inserted = true;
				bytesCached += item.size;
				numHash += (j + 1);
				numInserted++;
				pageSizes[pageId] += head;
				// 完全使用的页
				for (size_t i = 1; i <= numPages; i++)
				{
					pageSizes[pageId + i] += page_size;
				}
				// 最后一页
				pageSizes[pageId + numPages + 1] += tail;
				break;
			}
		}
		dump_stats();
		exit(0);
	}
	else
	{
		// 下一次输出统计信息的剩余数据量
		static int32_t next_dump = 10 * 1024 * 1024;
		// 若当前请求的数据不在对象列表中，将其插入
		if (allObjects.find(r->kid) == allObjects.end())
		{
			SegmentUtil::SUItem item(r->kid, (size_t)r->size(), r->time, r->key_sz, r->val_sz);
			objects.emplace_back(item);
			allObjects[r->kid] = true;
			dataSize += r->size();
			if (next_dump < r->size())
			{
				std::cerr << "Progress: " << dataSize / 1024 / 1024 << " MB" << std::endl;
				next_dump += 10 * 1024 * 1024;
			}
			next_dump -= r->size();
		}
	}
	return 0;
}

bool SegmentUtil::compareSizes(const SegmentUtil::SUItem &item1,
							   const SegmentUtil::SUItem &item2)
{
	return item1.size > item2.size;
}

size_t SegmentUtil::get_bytes_cached() const { return bytesCached; }

void SegmentUtil::dump_stats(void)
{
	assert(stat.apps->size() == 1);
	uint32_t appId = 0;
	for (const auto &app : *stat.apps)
	{
		appId = app;
	}
	std::string filename{stat.policy + "-app" + std::to_string(appId) + "-segment_size" +
						 std::to_string(segment_util) + "-hash" + std::to_string(num_hash_functions) + "-page" + std::to_string(page_size)};
	std::ofstream out{filename};
	out << "segment size: " << segment_util << std::endl;
	out << "page size: " << page_size << std::endl;
	out << "#hash functions: " << num_hash_functions << std::endl;
	out << "avg hash function used: " << (double)numHash / ((double)numInserted) << std::endl;
	out << "bits per page: " << bits_for_page << std::endl;
	out << "total bytes cached: " << bytesCached << std::endl;
	out << "util: " << ((double)bytesCached) / segment_util << std::endl;
	// out << "key,size,inserted" << std::endl;
	//  for (auto& item : objects) {
	//	out << item.kId << "," << item.size << "," << item.inserted << std::endl;
	// }
}

// 对 64 位无符号整数进行循环左移操作
// 参数包括一个 64 位无符号整数 x 和一个 8 位有符号整数 r，表示左移的位数
inline static uint64_t rotl64(uint64_t x, int8_t r)
{
	return (x << r) | (x >> (64 - r));
}

// 用于对 64 位无符号整数进行混淆操作
// 通常用于哈希函数的后处理步骤，以确保输出的哈希值具有良好的随机性和分布性
inline static uint64_t fmix64(uint64_t k)
{
	// 将 k 右移 33 位的结果与 k 进行按位异或操作
	// 这一步骤有助于将高位的信息混合到低位
	k ^= k >> 33;
	// 将 k 乘以一个大质数常量
	// 这一步骤有助于进一步打乱比特位的分布
	k *= 0xff51afd7ed558ccdllu;
	// 进一步打乱比特位
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53llu;
	// 确保最终结果具有良好的随机性
	k ^= k >> 33;

	return k;
}

inline static uint64_t getblock64(const uint64_t *p, int i)
{
	return p[i];
}

// MurmurHash3 算法
// 用于生成 128 位的哈希值。该函数接受三个参数：key（要哈希的数据）、len（数据长度）和 seed（种子值）
uint128_t SegmentUtil::MurmurHash3_x64_128(const void *key,
										   const int len,
										   const uint32_t seed)
{
	// 将输入的 key 转换为字节数组 data
	const uint8_t *data = (const uint8_t *)key;
	// 计算数据块的数量 nblocks
	const int nblocks = len / 16;
	// 初始化两个 64 位的哈希值 h1 和 h2
	uint64_t h1 = seed;
	uint64_t h2 = seed;

	// 定义两个常量 c1 和 c2，它们是用于混合数据的魔数
	uint64_t c1 = 0x87c37b91114253d5llu;
	uint64_t c2 = 0x4cf5ad432745937fllu;

	//----------
	// body

	const uint64_t *blocks = (const uint64_t *)(data);
	// 处理每个 128 位的数据块
	for (int i = 0; i < nblocks; i++)
	{
		// 对于每个块，提取两个 64 位的子块 k1 和 k2
		// 并对它们进行一系列的混合操作，包括乘法、旋转和异或操作
		uint64_t k1 = getblock64(blocks, i * 2 + 0);
		uint64_t k2 = getblock64(blocks, i * 2 + 1);

		// 混合后的结果更新到哈希值 h1 和 h2 中
		k1 *= c1;
		k1 = rotl64(k1, 31);
		k1 *= c2;
		h1 ^= k1;

		h1 = rotl64(h1, 27);
		h1 += h2;
		h1 = h1 * 5 + 0x52dce729;

		k2 *= c2;
		k2 = rotl64(k2, 33);
		k2 *= c1;
		h2 ^= k2;

		h2 = rotl64(h2, 31);
		h2 += h1;
		h2 = h2 * 5 + 0x38495ab5;
	}

	//----------
	// tail

	// 处理剩余的字节（即“尾部”）
	// 根据剩余字节的数量，逐字节地将它们混合到 k1 和 k2 中，并进行类似的混合操作
	const uint8_t *tail = (const uint8_t *)(data + nblocks * 16);

	uint64_t k1 = 0;
	uint64_t k2 = 0;

	switch (len & 15)
	{
	case 15:
		k2 ^= (uint64_t)(tail[14]) << 48;
	case 14:
		k2 ^= (uint64_t)(tail[13]) << 40;
	case 13:
		k2 ^= (uint64_t)(tail[12]) << 32;
	case 12:
		k2 ^= (uint64_t)(tail[11]) << 24;
	case 11:
		k2 ^= (uint64_t)(tail[10]) << 16;
	case 10:
		k2 ^= (uint64_t)(tail[9]) << 8;
	case 9:
		k2 ^= (uint64_t)(tail[8]) << 0;
		k2 *= c2;
		k2 = rotl64(k2, 33);
		k2 *= c1;
		h2 ^= k2;

	case 8:
		k1 ^= (uint64_t)(tail[7]) << 56;
	case 7:
		k1 ^= (uint64_t)(tail[6]) << 48;
	case 6:
		k1 ^= (uint64_t)(tail[5]) << 40;
	case 5:
		k1 ^= (uint64_t)(tail[4]) << 32;
	case 4:
		k1 ^= (uint64_t)(tail[3]) << 24;
	case 3:
		k1 ^= (uint64_t)(tail[2]) << 16;
	case 2:
		k1 ^= (uint64_t)(tail[1]) << 8;
	case 1:
		k1 ^= (uint64_t)(tail[0]) << 0;
		k1 *= c1;
		k1 = rotl64(k1, 31);
		k1 *= c2;
		h1 ^= k1;
	};

	//----------
	// finalization

	// 进行哈希值的最终混合和归一化
	// 将数据长度异或到 h1 和 h2 中，然后通过 fmix64 函数对它们进行进一步的混合
	h1 ^= len;
	h2 ^= len;

	h1 += h2;
	h2 += h1;

	h1 = fmix64(h1);
	h2 = fmix64(h2);

	h1 += h2;
	h2 += h1;

	// 最终的 128 位哈希值由 h1 和 h2 组合而成
	return ((uint128_t)h2 << 64) | h1;
}
