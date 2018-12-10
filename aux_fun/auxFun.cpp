#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "../structs.h"
#include "auxFun.h"


/* Get next prime for hashing */
int64_t next_prime(int64_t x) {
    if (x < 2) return 2;
    if (x == 3) return 5;
    int64_t retValue = (x % 2) ? x + 2 : x + 1; // Next odd number
    while (1) {
        if (retValue % 3 != 0) {
            int isPrime = 1;
            for (int64_t i = 5; i*i <= retValue; i += 6) {
                if (retValue % i == 0 || retValue % (i+2) == 0) {
                    isPrime = 0;
                    break;
                }
            }
            if (isPrime)
                return retValue;
        }
        retValue += 2;
    }
}

int64_t pow2(int64_t exp){
    return 1 << exp;
}

relation *tableRelation(int64_t *table, int64_t columnNumber, uint64_t numOfRows, int64_t numOfColumns) {
    relation *R = (relation*)malloc(sizeof(relation));
    R->num_tuples = numOfRows;
    R->tuples = (tuple*)malloc(numOfRows*sizeof(tuple));
    for (size_t i = 0; i < numOfRows; i++){
        R->tuples[i].key = i + 1;
        R->tuples[i].payload = table[i*numOfColumns+columnNumber-1];
    }
    return R;
}

relation *randomRel(int x) {
    relation *R = (relation*)malloc(sizeof(relation));
    R->num_tuples = x;
    R->tuples = (tuple*)malloc(x*sizeof(tuple));
    if(R->tuples == NULL){
        fprintf(stderr, ">>>>>>>>>>>>>>>>>>> Fail\n");
    }
    for (int64_t i = 0; i < x; i++){
        R->tuples[i].key = i + 1;
        R->tuples[i].payload = rand() % 25;
    }
    return R;
}

using namespace std;

void print_batches(vector<vector<query_info>>& batches) {
	for (auto&& queries : batches) {
		for (auto&& query : queries) {
			print_query(query);
		}
		printf("F\n");
	}
}
