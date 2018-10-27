#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "auxFun.h"

//this is the number of the n less significant bits
#define HASH_LSB 8
#define H1(X) (X & 0xFF)
#define H2(A, B) ((A) / (B))

int32_t twoInLSB;

void hashRelation(relInfo* newRel, relation *rel){
	newRel->histogram = calloc(twoInLSB, sizeof(int32_t));
	//first run through table
	for(int32_t i = 0; i < rel->num_tuples; i++){
		//find hash position and increase histogram value by one
		int32_t position = rel->tuples[i].payload & (twoInLSB-1);
		newRel->histogram[position]++;
	}

	//from histogram to summarised histogram
	int32_t *sumHistogram = malloc(twoInLSB * sizeof(int32_t));
	sumHistogram[0] = 0;
	for (int i = 1; i < twoInLSB; i++)
		sumHistogram[i] = sumHistogram[i-1] + newRel->histogram[i-1];

	//second run through table, new relation table
	newRel->tups.num_tuples = rel->num_tuples;
	newRel->tups.tuples = malloc((rel->num_tuples)*sizeof(tuple));

	for(int32_t i = 0; i < rel->num_tuples; i++){
		//find hash position
		int32_t position = (rel->tuples[i].payload)&(twoInLSB-1);
		//copy that tuple to the new table in the right position
		memcpy(newRel->tups.tuples+sumHistogram[position], &rel->tuples[i], sizeof(tuple));
		//increase position for current hash result value
		sumHistogram[position]++;
	}
	free(sumHistogram);
}

void getBucket(relInfo relR, relInfo relS, int begR, int begS, int bucketNo){
	//compare buckets of R and S
	// if (relR.histogram[bucketNo] > relS.histogram[bucketNo]){

	// }else{
		
	// }
}

result* RadixHashJoin(relation *relR, relation *relS){

	twoInLSB = pow2(HASH_LSB);
	
	relInfo relRhashed;
	relInfo relShashed;
	hashRelation(&relRhashed, relR);
	hashRelation(&relShashed, relS);

	int begR = 0, begS = 0;
	for (int i = 0; i < twoInLSB; i++){
		getBucket(relRhashed, relShashed, begR, begS, i);

		begR += relRhashed.histogram[i];
		begS += relShashed.histogram[i];
	}

	printf("%d, %d\n", relRhashed.tups.num_tuples, relShashed.tups.num_tuples);

	free(relRhashed.tups.tuples);
	free(relShashed.tups.tuples);
	free(relRhashed.histogram);
	free(relShashed.histogram);
	return NULL;
}
