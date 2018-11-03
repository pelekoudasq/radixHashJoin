#ifndef _RESULT_LIST_
#define _RESULT_LIST_

typedef struct bucket {
	tuple *page;			// Table of pairs {idR, idS} (results of R |><| S)
	struct bucket *next;
} bucket;

typedef struct {
	int32_t capacity;		// Capacity of each & every bucket
	int32_t size;			// Size of current bucket (all the other buckets and memory behind are full)
	bucket* head;			// Current bucket
} result;

void empty_list(result*);
void addResult(result*, int32_t, int32_t);
void print_list(result*, relation*, relation*);

#endif
