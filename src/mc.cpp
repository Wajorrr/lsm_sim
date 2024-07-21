#include <iostream>
#include <cinttypes>
#include <cstring>
#include <utility>
#include <stdio.h>
#include "mc.h"

static const int MAX_NUMBER_OF_SLAB_CLASSES = 64;
static const size_t POWER_SMALLEST = 1;
static const size_t POWER_LARGEST = 256;
static const size_t CHUNK_ALIGN_BYTES = 8;
static const size_t chunk_size = 48;
static const size_t item_size_max = 1024 * 1024;

typedef uint32_t rel_time_t;

typedef struct _stritem
{
    /* Protected by LRU locks */
    struct _stritem *next;
    struct _stritem *prev;
    /* Rest are protected by an item lock */
    struct _stritem *h_next; /* hash chain next */
    rel_time_t time;         /* least recent access */
    rel_time_t exptime;      /* expire time */
    int nbytes;              /* size of data */
    unsigned short refcount;
    uint8_t nsuffix;     /* length of flags-and-length string */
    uint8_t it_flags;    /* ITEM_* above */
    uint8_t slabs_clsid; /* which slab class we're in */
    uint8_t nkey;        /* key length, w/terminating null and padding */
    /* this odd type prevents type-punning issues when we do
     * the little shuffle to save space when not using CAS. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
    union
    {
        uint64_t cas;
        char end;
    } data[];
#pragma GCC diagnostic pop
    /* if it_flags & ITEM_CAS we have 8 bytes CAS */
    /* then null-terminated key */
    /* then " flags length\r\n" (no terminating null) */
    /* then data with terminating \r\n (no terminating null; it's binary!) */
} item;

typedef struct
{
    unsigned int size;    /* sizes of items */
    unsigned int perslab; /* how many items per slab */

    void *slots;          /* list of item ptrs */
    unsigned int sl_curr; /* total free items in list */

    unsigned int slabs; /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    size_t requested; /* The number of requested bytes */
} slabclass_t;

static slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];
static int power_largest;

/**
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 *
 *
 * NOTE: Modified to return the max number of slabs
 *       (for sizing arrays elsewhere).
 */
uint16_t slabs_init(const double factor)
{
    int i = POWER_SMALLEST - 1;

    // stutsman: original memcached code boost class size by
    // at least sizeof(item) but since our simulator doesn't
    // account for metadata this probably doesn't make sense?
    unsigned int size = sizeof(item) + chunk_size;
    // unsigned int size = chunk_size;

    memset(slabclass, 0, sizeof(slabclass));

    while (++i < MAX_NUMBER_OF_SLAB_CLASSES - 1 &&
           size <= item_size_max / factor)
    {
        /* Make sure items are always n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES)
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);

        std::cout << "slab class " << i << " size " << size << std::endl;
        slabclass[i].size = size;                                 // 当前slab类中每个slab的大小
        slabclass[i].perslab = item_size_max / slabclass[i].size; // 当前slab类中有多少个slab
        size *= factor;
    }

    power_largest = i;
    slabclass[power_largest].size = item_size_max; // 最大的slab class
    slabclass[power_largest].perslab = 1;          // 最大的slab class中只有一个slab

    std::cout << "slab class " << i << " size " << item_size_max << std::endl;

    return power_largest;
}

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 *
 * NOTE: modified to return class id and class_size.
 */
std::pair<uint32_t, uint32_t> slabs_clsid(const size_t size)
{
    int res = POWER_SMALLEST;
    if (size == 0)
        return {0, 0};
    while (size > slabclass[res].size) // 从小到大遍历，找到合适的slab class
    {
        ++res;
        if (res == power_largest) /* won't fit in the biggest slab */
            return {0, 0};
    }
    return {slabclass[res].size, res}; // 返回slab class的大小和slab class的id
}
