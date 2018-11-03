#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "structs.h"
#include "resultList.h"

void empty_list(result* list) {
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
    }
     
    list->head->page[list->size].key = key1;
    list->head->page[list->size].payload = key2;
    list->size++;
}

void print_list(result* list, relation* relX, relation* relY) {
    bucket* temp = list->head;
    if (temp == NULL) return;
    for (int32_t i = 0; i < list->size; i++) {
        printf("YES %d, %d\n", temp->page[i].key, temp->page[i].payload);
        printf("%d %d\n", (relX->tuples[temp->page[i].key-1]).payload, (relY->tuples[temp->page[i].payload-1]).payload);
    }
    temp = temp->next;
    while (temp != NULL) {
        for (int32_t i = 0; i < list->capacity; i++) {
            printf("YES %d, %d\n", temp->page[i].key, temp->page[i].payload);
        }
        temp = temp->next;
    }
}