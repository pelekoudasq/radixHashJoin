#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "auxFun.h"
#include "join.h"
#include "resultList.h"


relation *tableRelation(int32_t *table, int32_t columnNumber, uint32_t numOfRows, int32_t numOfColumns) {

	relation *R = malloc(sizeof(relation));
	R->num_tuples = numOfRows;
	R->tuples = malloc(numOfRows*sizeof(tuple));
	for (int32_t i = 0; i < numOfRows; i++){
		R->tuples[i].key = i + 1;
		R->tuples[i].payload = table[i*numOfColumns+columnNumber-1];
	}
	return R;
}

relation *randomRel(int x) {

	relation *R = malloc(sizeof(relation));
	R->num_tuples = x;
	R->tuples = malloc(x*sizeof(tuple));
	if(R->tuples == NULL){
		fprintf(stderr, ">>>>>>>>>>>>>>>>>>> Fail\n");
	}
	for (int32_t i = 0; i < x; i++){
		R->tuples[i].key = i + 1;
		R->tuples[i].payload = rand() % 25;
	}
	return R;
}

int main(){
	srand(101);
	//test table 1
	/*int32_t x[10][2] = {
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
	*/

	relation* relX = randomRel(8);
	relation* relY = randomRel(8);

	result *list = RadixHashJoin(relX, relY);

	print_list(list);
	
	// Free test relations
	free(relX->tuples);
	free(relX);
	free(relY->tuples);
	free(relY);
	empty_list(list);
	free(list);

	return 0;
}

