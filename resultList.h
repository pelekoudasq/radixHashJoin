#ifndef _RESULT_LIST_
#define _RESULT_LIST_

typedef struct {
	int32_t keyR;
	int32_t keyS;
} key_tuple;

typedef struct bucket_Info {
	key_tuple *page;			// Table of pairs {idR, idS} (results of R |><| S)
	struct bucket_Info *next;
} bucket_Info;

typedef struct {
	int32_t capacity;			// Capacity of each & every bucket_Info
	int32_t size;				// Size of current bucket_Info (all the other bucket_Infos and memory behind are full)
	bucket_Info* head;			// Current bucket_Info
} result;

void init_list(result*);
void empty_list(result*);
void add_result(result*, int32_t, int32_t);
void print_list(result*);

#endif
