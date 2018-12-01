#ifndef _STRUCTS_
#define _STRUCTS_

struct relList{
	uint64_t num_tuples;
	uint64_t num_columns;
	uint64_t *value;
};

struct tuple{
	int32_t key;
	int32_t payload;
};

struct relation{
	tuple *tuples;
	uint32_t num_tuples;
};

struct relation_info{
	relation tups;
	int32_t *histogram;
};

#endif
