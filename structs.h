#ifndef _STRUCTS_
#define _STRUCTS_

struct relList{
	uint64_t num_tuples;
	uint64_t num_columns;
	uint64_t *value;
};

struct tuple{
	int64_t key;
	int64_t payload;
};

struct relation{
	tuple *tuples;
	uint64_t num_tuples;
};

struct relation_info{
	relation tups;
	int64_t *histogram;
};

#endif
