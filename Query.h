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
    std::vector<relList_stats> stats;
    bool filtered_out;

    explicit Query(int ch);

    bool read_relations(int ch);

    void read_predicates();

    void read_projections();

    void execute(JobScheduler &js, std::vector<relList> &relations);

    bool run_filters(std::vector<relList> &relations,
                     std::unordered_map<uint64_t, std::unordered_set<uint64_t> > &filtered);

    void run_joins(JobScheduler &js, std::vector<relList> &relations,
                   std::unordered_map<uint64_t, std::unordered_set<uint64_t> > &filtered);

    void print() const;
};

#endif //JOIN_QUERY_H
