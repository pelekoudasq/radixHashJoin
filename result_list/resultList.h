#ifndef _RESULT_LIST_
#define _RESULT_LIST_

struct key_tuple{
	int64_t keyR;
	int64_t keyS;
};

struct bucket_info {
	key_tuple *page;			// Table of pairs {idR, idS} (results of R |><| S)
	bucket_info *next;
};

struct result{
	int64_t capacity;			// Capacity of each & every bucket_info
	int64_t size;				// Size of current bucket_info (all the other bucket_infos and memory behind are full)
	bucket_info* head;			// Current bucket_info
};

void init_list(result*);
void empty_list(result*);
void add_result(result*, int64_t, int64_t);
void print_list(result*);

#endif
