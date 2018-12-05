#ifndef _AUXFUN_
#define _AUXFUN_

#include <vector>
#include <unordered_set>

#include "../query_handler/query.h"
#include "../structs.h"

int64_t next_prime(int64_t x);
int64_t pow2(int64_t exp);

void print_disq(query_info& query, std::vector<relList>& relations, bool **disqualified);
void print_disq2(query_info& query, std::vector<relList>& relations, std::unordered_set<uint64_t>* disqualified);
void print_relations(std::vector<relList>& relations);
void print_batches(std::vector<std::vector<query_info>>& batches);

#endif
