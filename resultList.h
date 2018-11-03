#ifndef _RESULT_LIST_
#define _RESULT_LIST_

typedef struct bucketInfo {
	tuple *page;			// Table of pairs {idR, idS} (results of R |><| S)
	struct bucketInfo *next;
} bucketInfo;

typedef struct {
	int32_t capacity;		// Capacity of each & every bucketInfo
	int32_t size;			// Size of current bucketInfo (all the other bucketInfos and memory behind are full)
	bucketInfo* head;			// Current bucketInfo
} result;

void init_list(result*);
void empty_list(result*);
void addResult(result*, int32_t, int32_t);
void print_list(result*);

#endif
