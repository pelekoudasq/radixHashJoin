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
    list->capacity = (1024*1024 - sizeof(bucket_Info)) / sizeof(tuple);
    list->size = list->capacity;
    list->head = NULL;
}

void empty_list(result* list) {
    #ifdef _debug_
    fprintf(stderr,"%d , %d\n", countBuckets, countResults);
    #endif
    while (list->head != NULL) {
        bucket_Info* temp = list->head;
        list->head = temp->next;
        free(temp);
    }
}

void add_result(result* list, int32_t key1, int32_t key2) {
    if (list->size == list->capacity) {
        bucket_Info* temp = malloc(1024*1024);
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
    key_tuple* page = (key_tuple*)&(list->head[1]);
    page[list->size].keyR = key1;
    page[list->size].keyS = key2;
    list->size++;
}

void print_list(result* list) {
    bucket_Info* temp = list->head;
    key_tuple* page = (key_tuple*)&temp[1];

    if (temp == NULL) return;
    for (int32_t i=0; i<list->size; i++) {
        printf("Pairs of IDs are: %d, %d\n", page[i].keyR, page[i].keyS);
    }
    temp = temp->next;
    while (temp != NULL) {
        page = (key_tuple*)&temp[1];
        for (int32_t i=0; i<list->capacity; i++) {
            printf("Pairs of IDs are: %d, %d\n", page[i].keyR, page[i].keyS);
        }
        temp = temp->next;
    }
}