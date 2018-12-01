#ifndef _QUERY_
#define _QUERY_

#include <vector>

struct join_info{
	uint64_t table1;
	uint64_t column1;
	uint64_t table2;
	uint64_t column2;
};

struct filter_info{
	uint64_t table;
	uint64_t column;
	char op;
	uint64_t number;
};

struct proj_info{
	uint64_t table;
	uint64_t column;
};

struct query_info{
	std::vector<uint64_t> table;
	std::vector<join_info> join;
	std::vector<filter_info> filter;
	std::vector<proj_info> proj;
};

struct query_node {
	query_node* next;
	query_info* query;
};

struct query_list{
	query_node* head;
	query_node* tail;
	size_t size;
};

struct batch_node {
	batch_node* next;
	query_list* queries;
};

struct batch_list{
	batch_node* head;
	batch_node* tail;
	size_t size;
};

uint64_t read_number(int ch, int *);
int read_relations(query_info *);
void read_predicates(query_info *);
void read_projections(query_info *);
void free_query(query_info *);
void print_query(query_info&);
void query_push(query_list *, query_info *);
void batch_push(batch_list *, query_list *);

#endif
