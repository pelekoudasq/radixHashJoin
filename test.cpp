#include <cctype>
#include <fcntl.h>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unordered_set>

#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "structs.h"
#include "aux_fun/auxFun.h"
#include "join.h"
#include "result_list/resultList.h"
#include "file_list/fileList.h"
#include "query_handler/query.h"

using namespace std;

bool **run_filters(query_info& query, vector<relList>& relations) {
	vector<filter_info>& filter = query.filter;
	bool **disqualified = (bool**)calloc(sizeof(bool*), query.table.size());
	for (auto&& f : filter) {
		uint64_t table_number = query.table[f.table];
		uint64_t column_number = f.column;
		if (disqualified[f.table] == NULL)
			disqualified[f.table] = new bool[relations[table_number].num_tuples](); // sets all to false
		size_t offset = column_number*relations[table_number].num_tuples;
		for(size_t j = 0; j < relations[table_number].num_tuples; j++){
			uint64_t value = relations[table_number].value[offset+j];
			if (disqualified[f.table][j]) {
				if (f.op == '>')
					disqualified[f.table][j] = !(value > f.number);
				else if (f.op == '<')
					disqualified[f.table][j] = !(value < f.number);
				else if (f.op == '=')
					disqualified[f.table][j] = !(value == f.number);
			}
		}
	}
	return disqualified;
}

unordered_set<uint64_t>* run_filters2(query_info& query, vector<relList>& relations) {
	vector<filter_info>& filter = query.filter;
	unordered_set<uint64_t>* disqualified = new unordered_set<uint64_t>[query.table.size()];
	for (auto&& f : filter) {
		uint64_t table_number = query.table[f.table];
		uint64_t column_number = f.column;
		size_t offset = column_number*relations[table_number].num_tuples;
		for(size_t j = 0; j < relations[table_number].num_tuples; j++){
			uint64_t value = relations[table_number].value[offset+j];
			if (f.op == '>') {
				if ( !(value > f.number) )
					disqualified[f.table].insert(j);
			}
			else if (f.op == '<') {
				if ( !(value < f.number) )
					disqualified[f.table].insert(j);
			}
			else if (f.op == '=') {
				if ( !(value == f.number) )
					disqualified[f.table].insert(j);
			}
		}
	}
	return disqualified;
}

void parse_table(join_info& join, relList *relations, uint64_t table_number) {

	uint64_t column1_number = join.column1;
	uint64_t column2_number = join.column2;
	for(size_t i = 0; i < relations[table_number].num_tuples; i++){
		uint64_t value1 = relations[table_number].value[column1_number*relations[table_number].num_tuples+i];
		uint64_t value2 = relations[table_number].value[column2_number*relations[table_number].num_tuples+i];
		if (value1 == value2){
			//add i (rowid) to intermediate results for table
		}
	}
}

void run_joins(query_info* query, relList *relations) {
	vector<join_info>& join = query->join;

	for (auto&& j : join) {
		if ( j.table1 == j.table2 ){
			uint64_t table_number = query->table[j.table1];
			parse_table(j, relations, table_number);
		} else {
			uint64_t table1_number = query->table[j.table1];
			uint64_t column1_number = j.column1;
			uint64_t table2_number = query->table[j.table2];
			uint64_t column2_number = j.column2;
			//get rowids for these tables from intermediate results, if they exist
			//create relations to send to RadixHashJoin
			//send relations to RadxHashJoin
			//get results to intermediate
		}
	}
}

// 	3 0 1|0.2=1.0&0.1=2.0&0.2>3499|1.2 0.1
//	{ , , }
//	     -NULL

/*
0  2  1
__ __ __ __
|| ||
|| ||
|| ||

__________________________________________
struct struct struct
__________________________________________
| tableName = 11;
| vector row_ids;
|
|

rowid0 0.0	rowid1 1.0 ---> |><|
1      10   1			 10 			0-1,1,8						1-6
2      20   2			 10 			1-1,2						2-5
3      30   3			 23 			2-4,4						5-13
4      40   4			 40 			3-6,5
5      50   5			 60
6      60   6			 100
*/

void execute(query_info& query, vector<relList>& relations) {
	bool **disqualified = run_filters(query, relations);
	unordered_set<uint64_t>* disqualified2 = run_filters2(query, relations);
	//uint64_t **intermediate = new uint64_t*[query.table.size()]; //all null
	vector<table_ids> intermediate;
	vector<intermediate> intermediate_set;
	fill_intermediate(intermediate_set);		// Goto querry.cpp for more

	//run_joins(intermediate);

	for (size_t i = 0; i < query.table.size(); i++) {
		if (disqualified[i] != NULL)
			delete[] disqualified[i];
	}
	free(disqualified);
	delete[] disqualified2;
}

int main(int argc, char const *argv[]){
	char *lineptr = NULL;
	size_t n = 0;
	ssize_t lineSize;

	//get every filepath
	vector<relList> relations;
	while ( (lineSize = getline(&lineptr, &n, stdin)) != -1 && strcmp(lineptr, "Done\n") != 0 ){
		lineptr[lineSize-1] = '\0';
		//open file and get contents
		relList relation;
		int fileDesc = open(lineptr, O_RDONLY);
		read(fileDesc, &relation.num_tuples, sizeof(uint64_t));
		read(fileDesc, &relation.num_columns, sizeof(uint64_t));
		relation.value = (uint64_t*)mmap(NULL, relation.num_tuples*relation.num_columns*sizeof(uint64_t), PROT_READ, MAP_PRIVATE, fileDesc, 0);
		relation.value += 2;			//file offset
		close(fileDesc);
		relations.push_back(relation);
	}
	if (lineptr != NULL)
		free(lineptr);

	//print_relations(relations);

	//parse batch
	vector<vector<query_info>> batches;
	while (!feof(stdin)) {
		vector<query_info> queries;
		while (true) {
			query_info query;
			if (read_relations(query)){
				getchar();							//get new line
				break;
			}
			read_predicates(query);
			read_projections(query);
			queries.push_back(query);
		}
		if (!queries.empty()) {
			batches.push_back(queries);
		}
	}
	//for every batch, execute queries
	for (auto&& queries : batches) {
		for (auto&& query : queries) {
			execute(query, relations);
		}
	}
	print_batches(batches);

	for (auto&& relation : relations){
		munmap(relation.value, relation.num_tuples*relation.num_columns*sizeof(uint64_t));
	}

	return 0;
}
