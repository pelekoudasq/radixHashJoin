#ifndef _RESULT_LIST_
#define _RESULT_LIST_

struct key_tuple{
	int32_t keyR;
	int32_t keyS;
};

struct bucket_info {
	key_tuple *page;			// Table of pairs {idR, idS} (results of R |><| S)
	bucket_info *next;
};

struct result{
	int32_t capacity;			// Capacity of each & every bucket_info
	int32_t size;				// Size of current bucket_info (all the other bucket_infos and memory behind are full)
	bucket_info* head;			// Current bucket_info
};

void init_list(result*);
void empty_list(result*);
void add_result(result*, int32_t, int32_t);
void print_list(result*);

#endif
