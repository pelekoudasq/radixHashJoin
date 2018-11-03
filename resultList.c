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

void empty_list(result* list) {
    #ifdef _debug_
    fprintf(stderr,"%d , %d\n", countBuckets, countResults);
    #endif
    while (list->head != NULL) {
        bucket* temp = list->head;
        list->head = temp->next;
        free(temp->page);
        free(temp);
    }
}

void addResult(result* list, int32_t key1, int32_t key2) {
    if (list->size == list->capacity) {
        bucket* temp = malloc(sizeof(bucket));
        temp->next = list->head;  
        temp->page = malloc(1024*1024);

        list->head = temp;
        list->size = 0;
        #ifdef _debug_
        countBuckets++;
        #endif
    }
    #ifdef _debug_
    countResults++;
    #endif
    list->head->page[list->size].key = key1;
    list->head->page[list->size].payload = key2;
    list->size++;
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