#include "Result.h"
#include "auxFun.h"

/* This is the number of the n less significant bits */
#define HASH_LSB 8

#define BUCKET_SIZE (128 * 1024)

/* Initialize result list. */
Result::Result() {
    capacity = (BUCKET_SIZE - sizeof(bucket_info)) / sizeof(tuple);
    size = capacity;
    head = nullptr;
}

/* Add result to result list. */
void Result::add_result(uint64_t key1, uint64_t key2) {

    /*if page is full, get more space */
    if (size == capacity) {
        auto *temp = (bucket_info *) malloc(BUCKET_SIZE);
        temp->next = head;

        head = temp;
        size = 0;
    }
    auto *page = (key_tuple *) &head[1];
    page[size].keyR = key1;
    page[size].keyS = key2;
    size++;
}

/* hash small
 * make chain array
 * Parse through big bucket, join with small
 * Add resulting RowIDs to list
 * always put key of array R first and then key of array S
 */
void join_buckets(Result *list, relation_info *small, relation_info *big, size_t begSmall, size_t begBig,
                  size_t bucketNo, bool orderFlag) {
    size_t hashValue = next_prime(small->histogram[bucketNo]);

    auto *bucket = new int64_t[hashValue];
    for (size_t i = 0; i < hashValue; i++)
        bucket[i] = -1;

    auto *chain = new int64_t[small->histogram[bucketNo]];
    size_t endSmall = begSmall + small->histogram[bucketNo];

    for (size_t i = begSmall; i < endSmall; i++) {
        uint64_t position = small->tups.tuples[i].payload % hashValue;
        chain[i - begSmall] = bucket[position];
        bucket[position] = i - begSmall;
    }
    size_t endBig = begBig + big->histogram[bucketNo];

    for (size_t i = begBig; i < endBig; i++) {
        uint64_t bigHash = big->tups.tuples[i].payload % hashValue;
        int64_t position = bucket[bigHash];
        while (position != -1) {
            if (big->tups.tuples[i].payload == small->tups.tuples[position + begSmall].payload) {
                if (orderFlag)
                    list->add_result(big->tups.tuples[i].key, small->tups.tuples[position + begSmall].key);
                else
                    list->add_result(small->tups.tuples[position + begSmall].key, big->tups.tuples[i].key);
            }
            position = chain[position];
        }
    }
    delete[] bucket;
    delete[] chain;
}

/* pass the two relations through the first hash
 * for every backet, join them
 * return the results
 */
void Result::RadixHashJoin(relation &relR, relation &relS) {
    size_t twoInLSB = pow2(HASH_LSB);    // 2^HASH_LSB

    relation_info relRhashed;
    relation_info relShashed;
    relRhashed.hash_relation(relR, twoInLSB);
    relShashed.hash_relation(relS, twoInLSB);

    size_t begR = 0, begS = 0;
    for (size_t i = 0; i < twoInLSB; i++) {
        if (relRhashed.histogram[i] != 0 && relShashed.histogram[i] != 0) {
            if (relRhashed.histogram[i] >= relShashed.histogram[i])
                join_buckets(this, &relShashed, &relRhashed, begS, begR, i, true);
            else
                join_buckets(this, &relRhashed, &relShashed, begR, begS, i, false);
        }
        begR += relRhashed.histogram[i];
        begS += relShashed.histogram[i];
    }
}

/* Free result list's contents. */
Result::~Result() {
    while (head != nullptr) {
        bucket_info *temp = head;
        head = head->next;
        free(temp);
    }
}
