#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "../structs.h"
#include "resultList.h"

#ifdef _debug_
int countBuckets;
int countResults;
#endif

/* Initialize result list. */
void init_list(result* list) {
    list->capacity = (1024*1024 - sizeof(bucket_info)) / sizeof(tuple);
    list->size = list->capacity;
    list->head = NULL;
}

/* Free result list's contents. */
void empty_list(result* list) {
    #ifdef _debug_
    fprintf(stderr,"%d , %d\n", countBuckets, countResults);
    #endif
    while (list->head != NULL) {
        bucket_info* temp = list->head;
        list->head = temp->next;
        free(temp);
    }
}

/* Add result to result list. */
void add_result(result* list, int32_t key1, int32_t key2) {

    /*if page is full, get more space */
    if (list->size == list->capacity) {
        bucket_info* temp = (bucket_info*)malloc(1024*1024);
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

/* Print result list (for checking results). */
void print_list(result* list) {
    bucket_info* temp = list->head;
    key_tuple* page = (key_tuple*)&temp[1];

    if (temp == NULL) return;
    /* print current bucket */
    for (int32_t i=0; i<list->size; i++) {
        printf("Pairs of IDs are: %d, %d\n", page[i].keyR, page[i].keyS);
    }
    temp = temp->next;
    /* print all other buckets */
    while (temp != NULL) {
        page = (key_tuple*)&temp[1];
        for (int32_t i=0; i<list->capacity; i++) {
            printf("Pairs of IDs are: %d, %d\n", page[i].keyR, page[i].keyS);
        }
        temp = temp->next;
    }
}
