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

bool Result::isEmpty() {
    return head == nullptr;
}

/* Add result to result list. */
void Result::add_result(uint64_t key1, uint64_t key2) {

    /*if page is full, get more space */
    if (size == capacity) {
        auto temp = (bucket_info *) malloc(BUCKET_SIZE);
        temp->next = head;

        head = temp;
        size = 0;
    }
    auto page = (key_tuple *) &head[1];
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
void Result::join_buckets(relation_info *small, relation_info *big, size_t begSmall, size_t begBig, size_t smallSize,
                          size_t bigSize, bool orderFlag) {
    size_t hashValue = next_prime(smallSize);

    auto bucket = new int64_t[hashValue];
    for (size_t i = 0; i < hashValue; i++)
        bucket[i] = -1;

    auto chain = new int64_t[smallSize];
    size_t endSmall = begSmall + smallSize;

    for (size_t i = begSmall; i < endSmall; i++) {
        uint64_t position = small->tuples.tuples[i].payload % hashValue;
        chain[i - begSmall] = bucket[position];
        bucket[position] = i - begSmall;
    }
    size_t endBig = begBig + bigSize;

    for (size_t i = begBig; i < endBig; i++) {
        uint64_t bigHash = big->tuples.tuples[i].payload % hashValue;
        int64_t position = bucket[bigHash];
        while (position != -1) {
            if (big->tuples.tuples[i].payload == small->tuples.tuples[position + begSmall].payload) {
                if (orderFlag)
                    add_result(big->tuples.tuples[i].key, small->tuples.tuples[position + begSmall].key);
                else
                    add_result(small->tuples.tuples[position + begSmall].key, big->tuples.tuples[i].key);
            }
            position = chain[position];
        }
    }
    delete[] bucket;
    delete[] chain;
}

void Result::addAll(bucket_info *node, size_t size) {
    auto page = (key_tuple *) &node[1];
    auto s = page + size;
    for (key_tuple *kt = page; kt < s; kt++) {
        add_result(kt->keyR, kt->keyS);
    }
}

/* pass the two relations through the first hash
 * for every backet, join them
 * return the results
 */
void Result::multiRadixHashJoin(JobScheduler &js, relation &relR, relation &relS) {
    size_t twoInLSB = pow2(HASH_LSB);    // 2^HASH_LSB

    relation_info relRhashed;
    relation_info relShashed;
    relRhashed.hash_relation(js, relR, twoInLSB);
    relShashed.hash_relation(js, relS, twoInLSB);

    auto res = new Result[twoInLSB];

    size_t begR = 0, begS = 0;
    for (size_t i = 0; i < twoInLSB; i++) {
        if (relRhashed.histogram[i] != 0 && relShashed.histogram[i] != 0) {
            js.schedule(new JoinJob(res[i], &relShashed, &relRhashed, begS, begR, relShashed.histogram[i], relRhashed.histogram[i]));
        }
        begR += relRhashed.histogram[i];
        begS += relShashed.histogram[i];
    }

    js.barrier();

    for (size_t i = 0; i < twoInLSB; i++) {
        if (!res[i].isEmpty()) {
            bucket_info *node = res[i].head;
            addAll(node, res[i].size);
            node = node->next;
            while (node != nullptr) {
                addAll(node, res[i].capacity);
                node = node->next;
            }
        }
    }

    delete[] res;
}

/* Free result list's contents. */
Result::~Result() {
    while (head != nullptr) {
        bucket_info *temp = head;
        head = head->next;
        free(temp);
    }
}
