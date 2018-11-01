#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "auxFun.h"

int32_t nextPrime(int32_t x) {
    if (x < 2) return 2;
    int32_t retValue = (x % 2) ? x + 2 : x + 1; // next odd number
    while(1) {
        int isPrime = 1;
        for(int32_t i = 3; i*i <= retValue; i += 2){
            if(retValue % i == 0){
                isPrime = 0;
                break;
            }
        }
        if (isPrime)
            return retValue;
        retValue += 2;
    }
}

int32_t pow2(int32_t exp){
    return 1 << exp;
}

void empty_list(result* list) {
    while (list->head != NULL) {
        bucket* temp = list->head;
        list->head = temp->next;
        free(temp);
    }
}

void print_list(result* list) {
    bucket* temp = list->head;
    if (temp == NULL) return;
    for (int32_t i = 0; i < list->size; i++) {
        printf("YES %d, %d\n", temp->page[i].key, temp->page[i].payload);
    }
    temp = temp->next;
    while (temp != NULL) {
        for (int32_t i = 0; i < list->capacity; i++) {
            printf("YES %d, %d\n", temp->page[i].key, temp->page[i].payload);
        }
        temp = temp->next;
    }
}
