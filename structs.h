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
} relation_info;

#endif
