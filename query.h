#ifndef _QUERY_
#define _QUERY_

typedef struct {
	uint64_t table1;
	uint64_t column1;
	uint64_t table2;
	uint64_t column2;
} join_info;

typedef struct {
	uint64_t table;
	uint64_t column;
	char op;
	uint64_t number;
} filter_info;

typedef struct {
	uint64_t table;
	uint64_t column;
} proj_info;

typedef struct {
	uint64_t* table;
	join_info* join;
	filter_info* filter;
	proj_info* proj;
	size_t table_size;
	size_t join_size;
	size_t filter_size;
	size_t proj_size;
} query_info;

typedef struct query_node {
	struct query_node* next;
	query_info* query;
} query_node;

typedef struct {
	query_node* head;
	query_node* tail;
	size_t size;
} query_list;

typedef struct batch_node {
	struct batch_node* next;
	query_list* queries;
} batch_node;

typedef struct {
	batch_node* head;
	batch_node* tail;
	size_t size;
} batch_list;

uint64_t read_number(int ch, int *);
int read_relations(query_info *);
void read_predicates(query_info *);
void read_projections(query_info *);
void free_query(query_info *);
void print_query(query_info *);
void query_push(query_list *, query_info *);
void batch_push(batch_list *, query_list *);

#endif
