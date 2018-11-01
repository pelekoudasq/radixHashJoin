#ifndef _STRUCTS_
#define _STRUCTS_


typedef struct {
	int32_t key;
	int32_t payload;
} tuple;

typedef struct {
	tuple *tuples;
	uint32_t num_tuples;
} relation;

typedef struct {
	relation tups;
	int32_t *histogram;
} relInfo;

typedef struct bucket {
	tuple *page;			// Table of pairs {idR, idS} (results of R |><| S)
	struct bucket *next;
} bucket;

typedef struct {
	int32_t capacity;		// Capacity of each & every bucket
	int32_t size;			// Size of current bucket (all the other buckets and memory behind are full)
	bucket* head;			// Current bucket
} result;

#endif