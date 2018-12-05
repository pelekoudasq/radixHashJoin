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
    for (int64_t i = 0; i < numOfRows; i++){
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

// void print_disq(query_info& query, vector<relList>& relations, bool **disqualified) {
// 	for (auto&& table_number : query.table) {
// 		for (int j = 0; j < relations[table_number].num_tuples; ++j) {
// 			if (disqualified[i] == NULL)
// 				printf("0 ");
// 			else
// 				printf("%d ", disqualified[i][j]);
// 		}
// 		printf("\n");
// 	}
// }
//
// void print_disq2(query_info& query, vector<relList>& relations, unordered_set<uint64_t>* disqualified) {
// 	for (auto&& table_number : query.table) {
// 		for (int j = 0; j < relations[table_number].num_tuples; ++j) {
// 			printf("%d ", disqualified[i].find(j));
// 		}
// 		printf("\n");
// 	}
// }

void print_relations(vector<relList>& relations) {
	for (auto&& relation : relations){
		//printf("%ld, %ld\n", relation.num_tuples, relation.num_columns);
		for (size_t j = 0; j < relation.num_tuples; ++j){
			for (size_t k = 0; k < relation.num_columns; ++k)
				printf("%ld|", relation.value[k*relation.num_tuples+j]);
			printf("\n");
		}
	}
}

void print_batches(vector<vector<query_info>>& batches) {
	for (auto&& queries : batches) {
		for (auto&& query : queries) {
			print_query(query);
		}
		printf("F\n");
	}
}
