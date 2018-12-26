#ifndef JOIN_QUERY_H
#define JOIN_QUERY_H

#include <cstdint>
#include <vector>
#include "structs.h"

struct join_info {
    join_info(uint64_t table1, uint64_t column1, uint64_t table2, uint64_t column2);

    uint64_t table1;
    uint64_t column1;
    uint64_t table2;
    uint64_t column2;
};

struct filter_info {
    filter_info(uint64_t table, uint64_t column, int op, uint64_t number);

    uint64_t table;
    uint64_t column;
    int op;
    uint64_t number;
};

struct proj_info {
    proj_info(uint64_t table, uint64_t column);

    uint64_t table;
    uint64_t column;
    uint64_t sum;
};

struct Query {
    std::vector<uint64_t> table;
    std::vector<join_info> join;
    std::vector<filter_info> filter;
    std::vector<proj_info> proj;
    bool filtered_out;

    bool read_relations();
    void read_predicates();
    void read_projections();
    void execute(std::vector<relList> &relations);
    void print();
};

#endif //JOIN_QUERY_H
