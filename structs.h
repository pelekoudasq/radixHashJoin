#ifndef _STRUCTS_
#define _STRUCTS_

#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "JobScheduler.h"

struct relList {
    uint64_t num_tuples;
    uint64_t num_columns;
    uint64_t **values;
    uint64_t *col_min;
    uint64_t *col_max;
    uint64_t *distinct;

    explicit relList(char *filename);

    void destroy();
};

struct tuple {
    uint64_t key;
    uint64_t payload;
};

struct relation {
    tuple *tuples;
    uint64_t num_tuples;

    void create_relation(uint64_t join_table, relList &rel, uint64_t column_number,
                         std::unordered_map<uint64_t, std::unordered_set<uint64_t> > &filtered,
                         std::vector<uint64_t> &inter);

    void foo(const relList &rel, size_t column_number, const std::unordered_set<uint64_t> &uniqueValues);

    ~relation();
};

struct relation_info {
    relation tuples;
    size_t *histogram;

    void hash_relation(JobScheduler &js, relation &rel, size_t twoInLSB);

    ~relation_info();
};

#endif
