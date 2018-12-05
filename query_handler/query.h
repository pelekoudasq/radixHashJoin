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

struct table_ids {
	uint64_t tableNo;						  // Which table it is
	std::vector<uint64_t> rowids; // The row ids of the table after filter or join
};

uint64_t read_number(int ch, int *);
bool read_relations(query_info&);
void read_predicates(query_info&);
void read_projections(query_info&);
void print_query(query_info&);
void fill_intermediate(std::vector<table_ids>&);

#endif
