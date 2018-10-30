#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "join.h"


relation *tableRelation(int32_t *table, int32_t columnNumber, uint32_t numOfRows, int32_t numOfColumns) {
	
	relation *R = malloc(sizeof(relation));
	R->num_tuples = numOfRows;
	R->tuples = malloc(numOfRows*sizeof(tuple));
	for (int32_t i = 0; i < numOfRows; i++){

		tuple tempTuple;
		tempTuple.key = i + 1;
		tempTuple.payload = table[i*numOfColumns+columnNumber-1];
		memcpy(R->tuples+i, &tempTuple, sizeof(tuple));
	}
	return R;
}

int main(){
	
	//test table 1
	int32_t x[10][2] = {
		{1, 5}, 
		{2, 54}, 
		{3, 29},
		{4, 64}, 
		{5, 65},
		{6, 5}, 
		{7, 76}, 
		{8, 94}, 
		{9, 23}, 
		{10, 43}
	};

	//test table 2
	int32_t y[10][3] = {
		{1, 50, 34}, 
		{2, 51, 43}, 
		{3, 54, 87},
		{4, 43, 81}, 
		{5, 67, 94},
		{6, 35, 56}, 
		{7, 65, 77}, 
		{8, 29, 39}, 
		{9, 87, 29}, 
		{10, 5, 59}
	};

	//create test relations
	relation *relX = tableRelation(x[0], 2, 10, 2);
	relation *relY = tableRelation(y[0], 3, 10, 3);

	result* list = RadixHashJoin(relX, relY);

	// HERE PRINT THE LIST
	
	// Free test relations
	free(relX->tuples);
	free(relX);
	free(relY->tuples);
	free(relY);
	free(list);

	return 0;
}