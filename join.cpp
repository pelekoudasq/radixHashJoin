#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>

#include "structs.h"
#include "result_list/resultList.h"
#include "aux_fun/auxFun.h"

/* This is the number of the n less significant bits */
#define HASH_LSB 8
//#define H1(X) (X & 0xFF)
//#define H2(A, B) ((A) / (B))

int64_t twoInLSB;	// 2^HASH_LSB

/* First parsing through table
 * Find hash position and increase histogram value by one
 * From histogram to summarised histogram
 * Second parsing through table, new relation table
 * Find hash position
 * Copy that tuple to the new table in the right position
 * Increase position for current hash result value
 */

void hash_relation(relation_info* newRel, relation *rel){

	newRel->histogram = (int64_t*)calloc(twoInLSB, sizeof(int64_t));
	for (int64_t i = 0; i < rel->num_tuples; i++){
		int64_t position = rel->tuples[i].payload & (twoInLSB-1);
		newRel->histogram[position]++;
	}

	int64_t *sumHistogram = (int64_t*)malloc(twoInLSB * sizeof(int64_t));
	sumHistogram[0] = 0;
	for (int i = 1; i < twoInLSB; i++)
		sumHistogram[i] = sumHistogram[i-1] + newRel->histogram[i-1];

	newRel->tups.num_tuples = rel->num_tuples;
	newRel->tups.tuples = (tuple*)malloc((rel->num_tuples)*sizeof(tuple));
	for (int64_t i = 0; i < rel->num_tuples; i++){
		int64_t position = rel->tuples[i].payload & (twoInLSB-1);
		memcpy(newRel->tups.tuples+sumHistogram[position], &rel->tuples[i], sizeof(tuple));
		sumHistogram[position]++;
	}
	free(sumHistogram);
}

/* hash small
 * make chain array
 * Parse through big bucket, join with small
 * Add resulting RowIDs to list
 * always put key of array R first and then key of array S
 */

void join_buckets(result* list, relation_info* small, relation_info* big, int begSmall, int begBig, int bucketNo, int orderFlag) {
	int hashValue = next_prime(small->histogram[bucketNo]);

	int64_t* bucket = (int64_t*)malloc(hashValue * sizeof(int64_t));
	for (int i = 0; i < hashValue; i++)
		bucket[i] = -1;

	int64_t* chain = (int64_t*)malloc(small->histogram[bucketNo] * sizeof(int64_t));
	int endSmall = begSmall + small->histogram[bucketNo];

	for (int64_t i = begSmall; i < endSmall; i++){
		int64_t position = (small->tups.tuples[i].payload) % hashValue;
		int previousValue = bucket[position];
		bucket[position] = i-begSmall;
		chain[i-begSmall] = previousValue;
	}
	int endBig = begBig + big->histogram[bucketNo];

	for (int64_t i = begBig; i < endBig; i++) {
		int bigHash = (big->tups.tuples[i].payload) % hashValue;
		int64_t position = bucket[bigHash];
		while (position != -1) {
			if (big->tups.tuples[i].payload == small->tups.tuples[position+begSmall].payload) {
				if (orderFlag)
					add_result(list, big->tups.tuples[i].key, small->tups.tuples[position+begSmall].key);
				else
					add_result(list, small->tups.tuples[position+begSmall].key, big->tups.tuples[i].key);
			}
			position = chain[position];
		}
	}
	free(bucket);
	free(chain);
}

/* pass the two relations through the first hash
 * for every backet, join them
 * return the results
 */

result* RadixHashJoin(relation *relR, relation *relS) {
	twoInLSB = pow2(HASH_LSB);

	relation_info relRhashed;
	relation_info relShashed;
	hash_relation(&relRhashed, relR);
	hash_relation(&relShashed, relS);

	result* list = (result*)malloc(sizeof(result));
	init_list(list);

	int begR = 0, begS = 0;
	for (int i = 0; i < twoInLSB; i++){
		if (relRhashed.histogram[i] != 0 && relShashed.histogram[i] != 0) {
			if (relRhashed.histogram[i] >= relShashed.histogram[i])
				join_buckets(list, &relShashed, &relRhashed, begS, begR, i, 1);
			else
				join_buckets(list, &relRhashed, &relShashed, begR, begS, i, 0);
		}
		begR += relRhashed.histogram[i];
		begS += relShashed.histogram[i];
	}

	free(relRhashed.tups.tuples);
	free(relShashed.tups.tuples);
	free(relRhashed.histogram);
	free(relShashed.histogram);

	return list;
}
