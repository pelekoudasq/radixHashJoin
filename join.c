#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"

//this is the number of the n less significant bits
#define HASH_LSB 3

int32_t ipow(int32_t base, int32_t exp){

	int32_t result = 1;
	while (exp){
		if (exp & 1)
			result *= base;
		exp >>= 1;
		base *= base;
	}
	return result;
}

relation *hashRelation(relation *rel){

	int32_t twoInLSB = ipow(2,HASH_LSB);
	int32_t histogram[twoInLSB];
	for(int32_t i = 0; i < twoInLSB; i++)
		histogram[i] = 0;

	//first run through table
	for(int32_t i = 0; i < rel->num_tuples; i++){

		//get tuple from original table
		tuple tempTuple;
		memcpy(&tempTuple, rel->tuples+i, sizeof(tuple));
		//find hash position and increase histogram value by one
		int32_t position = (tempTuple.payload)&(twoInLSB-1);
		histogram[position]++;
	}

	//from histogram to summarised histogram
	int32_t sumHistogram[twoInLSB];
	sumHistogram[0] = 0;
	for (int32_t i = 1; i < twoInLSB; i++)
		sumHistogram[i] = sumHistogram[i-1] + histogram[i-1];

	//second run through table, new relation table
	relation *newRelation = malloc(sizeof(relation));
	newRelation->num_tuples = rel->num_tuples;
	newRelation->tuples = malloc((rel->num_tuples)*sizeof(tuple));

	for(int32_t i = 0; i < rel->num_tuples; i++){
		
		//get tuple from original table
		tuple tempTuple;
		memcpy(&tempTuple, rel->tuples+i, sizeof(tuple));
		//find hash position
		int32_t position = (tempTuple.payload)&(twoInLSB-1);
		//copy that tuple to the new table in the right position
		memcpy(newRelation->tuples+sumHistogram[position], &tempTuple, sizeof(tuple));
		//increase position for current hash result value
		sumHistogram[position]++;
	}
	return newRelation;
}

result* RadixHashJoin(relation *relR, relation *relS){
	
	relation *relRhashed = hashRelation(relR);
	relation *relShashed = hashRelation(relS);


	printf("%p, %p\n", relRhashed, relShashed);

	free(relRhashed->tuples);
	free(relRhashed);
	free(relShashed->tuples);
	free(relShashed);
	return NULL;
}
