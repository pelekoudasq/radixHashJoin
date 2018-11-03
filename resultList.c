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
    list->capacity = (1024*1024 - sizeof(bucketInfo)) / sizeof(tuple);
    list->size = list->capacity;
    list->head = NULL;
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
    if (list->size == list->capacity) {
        bucketInfo* temp = malloc(1024*1024);
        temp->next = list->head;

        list->head = temp;
        list->size = 0;
        #ifdef _debug_
        countBuckets++;
        #endif
    }
    #ifdef _debug_
    countResults++;
    #endif
    tuple* page = (tuple*)&(list->head[1]);
    page[list->size].key = key1;
    page[list->size].payload = key2;
    list->size++;
}

void print_list(result* list) {
    bucketInfo* temp = list->head;
    tuple* page = (tuple*)&temp[1];

    if (temp == NULL) return;
    for (int32_t i = 0; i < list->size; i++) {
        printf("YES %d, %d\n", page[i].key, page[i].payload);
        //printf("%d %d\n", (relX->tuples[temp->page[i].key-1]).payload, (relY->tuples[temp->page[i].payload-1]).payload);
    }
    temp = temp->next;
    while (temp != NULL) {
        page = (tuple*)&temp[1];
        for (int32_t i = 0; i < list->capacity; i++) {
            printf("YES %d, %d\n", page[i].key, page[i].payload);
        }
        temp = temp->next;
    }
}