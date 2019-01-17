#ifndef JOIN_RESULT_H
#define JOIN_RESULT_H

#include <cstdint>
#include <cstring>
#include "structs.h"

struct key_tuple {
    uint64_t keyR;
    uint64_t keyS;
};

struct bucket_info {
    // Table of pairs {idR, idS} (results of R |><| S)
    bucket_info *next;
};

struct Result {
    size_t capacity;            // Capacity of each & every bucket_info
    size_t size;                // Size of current bucket_info (all the other bucket_infos and memory behind are full)
    bucket_info *head;          // Current bucket_info

    void add_result(uint64_t, uint64_t);

    void RadixHashJoin(relation &, relation &);

    void join_buckets(relation_info *small, relation_info *big, size_t begSmall, size_t begBig, size_t bucketNo,
                      bool orderFlag);

    Result();

    ~Result();
};


#endif //JOIN_RESULT_H
