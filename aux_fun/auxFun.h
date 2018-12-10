#ifndef _AUXFUN_
#define _AUXFUN_

#include <vector>
#include <unordered_set>

#include "../query_handler/query.h"
#include "../structs.h"

int64_t next_prime(int64_t x);
int64_t pow2(int64_t exp);

void print_batches(std::vector<std::vector<query_info>>& batches);

#endif
