#include "Result.h"
#include <cstdlib>

#include "structs.h"

#define BUCKET_SIZE (1024 * 1024)

/* Initialize result list. */
void Result::init_list() {
    capacity = (BUCKET_SIZE - sizeof(bucket_info)) / sizeof(tuple);
    size = capacity;
    head = nullptr;
}

/* Free result list's contents. */
void Result::empty_list() {
    while (head != nullptr) {
        bucket_info *temp = head;
        head = head->next;
        free(temp);
    }
}

/* Add result to result list. */
void Result::add_result(uint64_t key1, uint64_t key2) {

    /*if page is full, get more space */
    if (size == capacity) {
        auto *temp = (bucket_info *) malloc(BUCKET_SIZE);
        temp->next = head;

        head = temp;
        size = 0;
    }
    auto *page = (key_tuple *) &head[1];
    page[size].keyR = key1;
    page[size].keyS = key2;
    size++;
}

Result::~Result() {
    empty_list();
}
