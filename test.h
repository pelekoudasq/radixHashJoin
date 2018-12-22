#ifndef JOIN_TEST_H
#define JOIN_TEST_H

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include "Query.h"
#include "Result.h"

void parse_table(join_info &join, std::vector<relList> &relations, uint64_t table_number,
                 std::unordered_map<uint64_t, std::unordered_set<uint64_t> > &filtered,
                 std::vector<std::vector<uint64_t> > &intermediate,
                 size_t tableSize);

bool run_filters(Query &query, std::vector<relList> &relations,
                 std::unordered_map<uint64_t, std::unordered_set<uint64_t> > &filtered);

std::vector<std::vector<uint64_t> > update_intermediate(std::vector<std::vector<uint64_t> > &intermediate,
                                                        Result *results, join_info &join, size_t tableSize);

#endif //JOIN_TEST_H
