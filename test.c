#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "join.h"


relation *tableRelation(int32_t *table, int32_t columnNumber, uint32_t numOfRows, int32_t numOfColumns){
	printf("in function tableRelation\n");

	relation *R = malloc(sizeof(relation));
	R->num_tuples = numOfRows;
	R->tuples = malloc(numOfRows*sizeof(tuple));
	int32_t i;
	for (i = 0; i < numOfRows; i++){

		tuple *tempTuple = malloc(sizeof(tuple));
		tempTuple->key = i + 1;
		tempTuple->payload = table[i*numOfColumns+columnNumber-1];
		memcpy(R->tuples+(i*sizeof(tuple)), tempTuple, sizeof(tuple));
		//printf("%p, %p\n", R->tuples+(i*sizeof(tuple))+sizeof(tuple), R->tuples+(numOfRows*sizeof(tuple)));
		//printf("%d\n", table[i*numOfColumns+columnNumber-1]);
		free(tempTuple);
	}
	return R;
}

int main(){
	
	int32_t x[10][2] = {
		{1, 5}, 
		{2, 54}, 
		{3, 29},
		{4 ,64}, 
		{5, 65},
		{6, 5}, 
		{7, 76}, 
		{8, 94}, 
		{9, 23}, 
		{10, 43}
	};

	int32_t y[10][3] = {
		{1, 50, 34}, 
		{2, 51, 43}, 
		{3, 54, 87},
		{4 ,43, 81}, 
		{5, 67, 94},
		{6, 35, 56}, 
		{7, 65, 77}, 
		{8, 29, 39}, 
		{9, 87, 29}, 
		{10, 5, 59}
	};

	relation *relX = tableRelation(x[0], 2, 10, 2);
	

	/*int32_t test;
	memcpy(&test, (relX->tuples)+sizeof(int32_t)*5, sizeof(int32_t));
	printf("%d\n", test);*/

	//tuple test/* = malloc(sizeof(tuple))*/;
	//memcpy(&test, (relX->tuples)+sizeof(tuple)*9, sizeof(tuple));
	//printf("%d\n", test.payload);
	
	relation *relY = tableRelation(y[0], 3, 10, 3);

	/*memcpy(&test, (relX->tuples)+sizeof(tuple)*9, sizeof(tuple));
	printf("%d\n", test.payload);*/

	result *res = RadixHashJoin(relX, relY);
	printf("res: %p\n", res);
	free(relX->tuples);
	free(relX);

	return 0;
}