#ifndef _AUXFUN_
#define _AUXFUN_

#include <vector>
#include <unordered_set>

#include "../query_handler/query.h"
#include "../structs.h"

int32_t next_prime(int32_t x);
int32_t pow2(int32_t exp);

void print_disq(query_info& query, std::vector<relList>& relations, bool **disqualified);
void print_disq2(query_info& query, std::vector<relList>& relations, std::unordered_set<uint64_t>* disqualified);
void print_relations(std::vector<relList>& relations);
void print_batches(std::vector<std::vector<query_info>>& batches);

#endif
