#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "auxFun.h"

//this is the number of the n less significant bits
#define HASH_LSB 8

int32_t twoInLSB;

relInfo hashRelation(relation *rel){

		
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

	relInfo newRel;
	newRel.tups = newRelation;
	newRel.histogram = histogram;

	return newRel;
}

getBucket( relInfo relR, relInfo relS. int begR, int begS){
	
	//compare buckets of R and S
	if (relRhashed.histogram[i] > relShashed.histogram[i]){

	}
}

result* RadixHashJoin(relation *relR, relation *relS){

	twoInLSB = ipow(2,HASH_LSB);
	
	relInfo relRhashed = hashRelation(relR);
	relInfo relShashed = hashRelation(relS);

	int begR = 0, begS = 0;
	for (int i = 0; i < twoInLSB; i++){
		getBucket(relRhashed, relShashed, begR, begS);

		begR += relRhashed.histogram[i];
		begS += relShashed.histogram[i];
	}

	printf("%d, %d\n", relRhashed.tups->num_tuples, relShashed.tups->num_tuples);

	free(relRhashed.tups->tuples);
	free(relRhashed.tups);
	free(relShashed.tups->tuples);
	free(relShashed.tups);
	return NULL;
}
