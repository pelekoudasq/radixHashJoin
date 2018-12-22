#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>

#include "structs.h"
#include "auxFun.h"
#include "Result.h"

/* This is the number of the n less significant bits */
#define HASH_LSB 8

/* First parsing through table
 * Find hash position and increase histogram value by one
 * From histogram to summarised histogram
 * Second parsing through table, new relation table
 * Find hash position
 * Copy that tuple to the new table in the right position
 * Increase position for current hash result value
 */

void relation_info::hash_relation(relation &rel, size_t twoInLSB) {
    histogram = new size_t[twoInLSB]();
    for (size_t i = 0; i < rel.num_tuples; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        histogram[position]++;
    }

    auto *sumHistogram = new size_t[twoInLSB];
    sumHistogram[0] = 0;
    for (size_t i = 1; i < twoInLSB; i++)
        sumHistogram[i] = sumHistogram[i - 1] + histogram[i - 1];

    tups.num_tuples = rel.num_tuples;
    tups.tuples = new tuple[tups.num_tuples];
    for (size_t i = 0; i < rel.num_tuples; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        memcpy(tups.tuples + sumHistogram[position], &rel.tuples[i], sizeof(tuple));
        sumHistogram[position]++;
    }
    delete[] sumHistogram;
}

relation_info::~relation_info() {
    delete[] histogram;
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
Result *RadixHashJoin(relation &relR, relation &relS) {
    size_t twoInLSB = pow2(HASH_LSB);    // 2^HASH_LSB

    relation_info relRhashed;
    relation_info relShashed;
    relRhashed.hash_relation(relR, twoInLSB);
    relShashed.hash_relation(relS, twoInLSB);
    auto *list = new Result;
    list->init_list();

    size_t begR = 0, begS = 0;
    for (size_t i = 0; i < twoInLSB; i++) {
        if (relRhashed.histogram[i] != 0 && relShashed.histogram[i] != 0) {
            if (relRhashed.histogram[i] >= relShashed.histogram[i])
                join_buckets(list, &relShashed, &relRhashed, begS, begR, i, true);
            else
                join_buckets(list, &relRhashed, &relShashed, begR, begS, i, false);
        }
        begR += relRhashed.histogram[i];
        begS += relShashed.histogram[i];
    }

    return list;
}

relation::~relation() {
    delete[] tuples;
}
