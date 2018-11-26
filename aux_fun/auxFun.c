#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "../structs.h"
#include "auxFun.h"


/* Get next prime for hashing */
int32_t next_prime(int32_t x) {
    if (x < 2) return 2;
    if (x == 3) return 5;
    int32_t retValue = (x % 2) ? x + 2 : x + 1; // Next odd number
    while (1) {
        if (retValue % 3 != 0) {
            int isPrime = 1;
            for (int32_t i = 5; i*i <= retValue; i += 6) {
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

int32_t pow2(int32_t exp){
    return 1 << exp;
}

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
