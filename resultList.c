#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "resultList.h"

#ifdef _debug_
int countBuckets;
int countResults;
#endif

void init_list(result* list) {
    bucketInfo* temp = malloc(1024*1024);
    temp->capacity = (1024*1024 - sizeof(bucketInfo)) / sizeof(tuple);
    temp->size = 0;
    temp->next = NULL;

    fprintf(stderr,"%d\n",temp->capacity);

    list->head = temp;
    list->tail = temp;
}

void empty_list(result* list) {
    #ifdef _debug_
    fprintf(stderr,"%d , %d\n", countBuckets, countResults);
    #endif
    while (list->head != NULL) {
        bucketInfo* temp = list->head;
        list->head = temp->next;
        free(temp);
    }
}

void addResult(result* list, int32_t key1, int32_t key2) {
    //R->key, S->payload
    bucketInfo* bucket = list->tail;
    if (bucket->size == bucket->capacity) {
        bucket = malloc(1024*1024);
        bucket->capacity = (1024*1024 - sizeof(bucketInfo)) / sizeof(tuple);
        bucket->size = 0;
        bucket->next = NULL;

        list->tail->next = bucket;
        list->tail = bucket;

        #ifdef _debug_
        countBuckets++;
        #endif
    }
    #ifdef _debug_
    countResults++;
    #endif

    key_tuple* page = (key_tuple*)&(bucket[1]);
    page[bucket->size].keyR = key1;
    page[bucket->size].keyS = key2;
    bucket->size++;
}

void print_list(result* list) {
    bucketInfo* temp = list->head;
    key_tuple* page = (key_tuple*)&temp[1];

    while (temp != NULL) {
        page = (key_tuple*)&temp[1];
        for (int32_t i = 0; i < temp->size; i++) {
            printf("YES %d, %d\n", page[i].keyR, page[i].keyS);
        }
        temp = temp->next;
    }
}