#include <ctype.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

bool **run_filters(query_info& query, relList *relations) {
	std::vector<filter_info>& filter = query.filter;
	bool **filter_results = (bool**)calloc(sizeof(bool*), query.table.size());
	for (auto&& f : filter) {
		uint64_t table_number = query.table[f.table];
		uint64_t column_number = f.column;
		if (filter_results[f.table] == NULL)
			filter_results[f.table] = new bool[relations[table_number].num_tuples];
		for(size_t j = 0; j < relations[table_number].num_tuples; j++){
			uint64_t value = relations[table_number].value[column_number*relations[table_number].num_tuples+j];
			if (f.op == '>')
				filter_results[f.table][j] = (value > f.number);
			else if (f.op == '<')
				filter_results[f.table][j] = (value < f.number);
			else if (f.op == '=')
				filter_results[f.table][j] = (value == f.number);
		}
	}
	return filter_results;
}

void parse_table(join_info *join, relList *relations, uint64_t table_number) {

	uint64_t column1_number = join->column1;
	uint64_t column2_number = join->column2;
	for(size_t i = 0; i < relations[table_number].num_tuples; i++){
		uint64_t value1 = relations[table_number].value[column1_number*relations[table_number].num_tuples+i];
		uint64_t value2 = relations[table_number].value[column2_number*relations[table_number].num_tuples+i];
		if (value1 == value2){
			//add i (rowid) to intermediate results for table
		}
	}
}

void run_joins(query_info* query, relList *relations) {
	std::vector<join_info>& join = query->join;
	for (auto&& j : join) {
		if ( j.table1 == j.table2 ){
			uint64_t table_number = query->table[j.table1];
			parse_table(&j, relations, table_number);
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

void execute(query_info& query, relList *relations) {
	bool **filter_results = run_filters(query, relations);
	/*for (int i = 0; i < query->table_size; ++i) {
		uint64_t table_number = query->table[i];
		for (int j = 0; j <  relations[table_number].num_tuples; ++j) {
			if (filter_results[i] == NULL)
				printf("1 ");
			else
				printf("%d ", filter_results[i][j]);
		}
		printf("\n");
	}*/
	//run_joins();
	for (size_t i = 0; i < query.table.size(); i++) {
		if (filter_results[i] != NULL)
			delete[] filter_results[i];
	}
	free(filter_results);
	print_query(query);
}

int main(int argc, char const *argv[]){

	char *lineptr = NULL;
	size_t n = 0;
	ssize_t lineSize;
	fileList *list = NULL;
	int listSize = 0;

	//get every filepath, push it to the list
	while ( (lineSize = getline(&lineptr, &n, stdin)) != -1 && strcmp(lineptr, "Done\n") != 0 ){
		lineptr[lineSize-1] = '\0';
		char *filepath = new char[lineSize];
		strcpy(filepath, lineptr);
		list = push_file(list, filepath);
		listSize++;
	}

	if (lineptr != NULL)
		free(lineptr);

	relList *relations = new relList[listSize];

	//for every filepath, open file and get contents
	for (int i = listSize-1; i >= 0; i--){

		char *filepath;
		list = pop_file(list, &filepath);
		int fileDesc = open(filepath, O_RDONLY);
		delete[] filepath;
		read(fileDesc, &relations[i].num_tuples, sizeof(uint64_t));
		read(fileDesc, &relations[i].num_columns, sizeof(uint64_t));
		relations[i].value = (uint64_t*)mmap(NULL, relations[i].num_tuples*relations[i].num_columns*sizeof(uint64_t), PROT_READ, MAP_PRIVATE, fileDesc, 0);
		relations[i].value += 2;			//file offset
		close(fileDesc);
	}

	// for (int i = 0; i < listSize; ++i){
	// 	//printf("%ld, %ld\n", relations[i].num_tuples, relations[i].num_columns);
	// 	for (int j = 0; j < relations[i].num_tuples; ++j){
	// 		for (int k = 0; k < relations[i].num_columns; ++k)
	// 		{
	// 			printf("%ld|", relations[i].value[k*relations[i].num_tuples+j]);
	// 		}
	// 		printf("\n");

	// 	}
	// }

	//parse batch
	std::vector<std::vector<query_info>> batches;
	while (!feof(stdin)) {
		printf("NEW Batch\n");
		std::vector<query_info> queries;
		while (1) {
			query_info query;
			int returnValue = read_relations(&query);
			//check return value for end of file or end of batch
			if (returnValue == EOF) {
				getchar();
				break;
			} else if (returnValue){
				getchar();							//get \n and continue to next batch
				break;
			}
			read_predicates(&query);
			read_projections(&query);
			queries.push_back(query);
		}
		if (!queries.empty())
			batches.push_back(queries);
	}

	//for every batch, execute queries
	for (auto&& queries : batches) {
		for (auto&& query : queries) {
			execute(query, relations);
		}
		printf("F\n");
	}

	for (int i = 0; i < listSize; ++i){
		munmap(relations[i].value, relations[i].num_tuples*relations[i].num_columns*sizeof(uint64_t));
	}
	delete[] relations;

	return 0;
}
