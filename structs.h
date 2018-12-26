#ifndef _STRUCTS_
#define _STRUCTS_

#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct relList {
    uint64_t num_tuples;
    uint64_t num_columns;
    uint64_t *value;

    explicit relList(char *filename);

    ~relList();
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

    void foo(const relList &rel, size_t offset, const std::unordered_set<uint64_t> &uniqueValues);

    ~relation();
};

struct relation_info {
    relation tups;
    size_t *histogram;

    void hash_relation(relation &rel, size_t twoInLSB);

    ~relation_info();
};

#endif
