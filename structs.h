#ifndef _STRUCTS_
#define _STRUCTS_


typedef struct tuple{
	int32_t key;
	int32_t payload;
}tuple;

typedef struct relation{
	tuple *tuples;
	uint32_t num_tuples;
}relation;

typedef struct relInfo{
	relation tups;
	int32_t *histogram;
}relInfo;

typedef struct result{
	void *page;
	struct result *next;
}result;

#endif