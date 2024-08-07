#ifndef SEGMENT_UTIL_H
#define SEGMENT_UTIL_H

#include <vector>
#include <unordered_map>
#include <cassert>
#include <iostream>
#include <math.h>
#include "policy.h"

static const size_t top_data_bound = 1024u * 1024u * 1024u;		// 总空间上限
static const size_t segment_util = 512 * 1024u * 1024u;			// 段利用大小
static const size_t page_size = 1024;							// 页大小
static const size_t number_of_pages = segment_util / page_size; // 段中页数
static const size_t num_hash_functions = 8;
static const size_t bits_for_page = log2(number_of_pages);

typedef __uint128_t uint128_t;

class SegmentUtil : public Policy
{
private:
	struct SUItem // 请求对象
	{
		uint32_t kId;
		size_t size;
		double time;
		bool inserted;
		int32_t keySize;
		int32_t valSize;

		SUItem() : kId(0), size(0), time(0), inserted(false),
				   keySize(0), valSize(0) {}
		SUItem(const uint32_t &rkId,
			   const size_t &rsize,
			   const double &rtime,
			   const int32_t &rkeySize,
			   const int32_t &rvalSize) : kId(rkId), size(rsize), time(rtime), inserted(false),
										  keySize(rkeySize), valSize(rvalSize) {}
	};

	std::vector<SUItem> objects;				   // 当前累计的对象列表
	std::unordered_map<uint32_t, bool> allObjects; // 标识对象是否存在

	size_t dataSize;	// 当前累计的对象总大小
	size_t bytesCached; // 实际缓存的数据大小
	size_t numHash;		// 哈希次数
	size_t numInserted; // 插入成功的对象数量

	static bool compareSizes(const SegmentUtil::SUItem &item1,
							 const SegmentUtil::SUItem &item2);

	inline static uint128_t MurmurHash3_x64_128(
		const void *key,
		const int len,
		const uint32_t seed);

public:
	SegmentUtil(stats stat);
	~SegmentUtil();
	size_t process_request(const Request *r, bool warmup);
	size_t get_bytes_cached() const;
	void dump_stats(void);
};

#endif
