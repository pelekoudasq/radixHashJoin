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


int **run_filters(query_info* query, relList *relations) {
	
	filter_info* filter = query->filter;
	int **filter_results = calloc(sizeof(int*), query->table_size);
	for (size_t i = 0; i < query->filter_size; i++){
		uint64_t table_number = query->table[filter->table];
		uint64_t column_number = filter->column;
		if (filter_results[filter->table] == NULL)
			filter_results[filter->table] = malloc(sizeof(int)*relations[table_number].num_tuples);
		for(size_t j = 0; j < relations[table_number].num_tuples; j++){
			uint64_t value = relations[table_number].value[column_number*relations[table_number].num_tuples+j];
			if (filter->op == '>')
				filter_results[filter->table][j] = (value > filter->number);
			else if (filter->op == '<')
				filter_results[filter->table][j] = (value < filter->number);
			else if (filter->op == '=')
				filter_results[filter->table][j] = (value = filter->number);
		}
		filter++;
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

	join_info *join = query->join;
	for (size_t i = 0; i < query->join_size; i++) {
		// if self join
		if ( join->table1 == join->table2 ){
			uint64_t table_number = query->table[join->table1];
			parse_table(join, relations, table_number);
		} else {
			uint64_t table1_number = query->table[join->table1];
			uint64_t column1_number = join->column1;
			uint64_t table2_number = query->table[join->table2];
			uint64_t column2_number = join->column2;
			//get rowids for these tables from intermediate results, if they exist
			//create relations to send to RadixHashJoin
			//send relations to RadxHashJoin
			//get results to intermediate
		}
		join++;
	}
}

void execute(query_info* query, relList *relations) {
	int **filter_results = run_filters(query, relations);
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
		char *filepath = malloc(lineSize);
		strcpy(filepath, lineptr);
		list = push_file(list, filepath);
		listSize++;
	}

	if( lineptr != NULL )
		free(lineptr);

	relList *relations = malloc(sizeof(relList)*listSize);

	for (int i = listSize-1; i >= 0; i--){

		char *filepath;
		list = pop_file(list, &filepath);
		int fileDesc = open(filepath, O_RDONLY);
		free(filepath);
		read(fileDesc, &relations[i].num_tuples, sizeof(uint64_t));
		read(fileDesc, &relations[i].num_columns, sizeof(uint64_t));
		relations[i].value = mmap(NULL, relations[i].num_tuples*relations[i].num_columns*sizeof(uint64_t), PROT_READ, MAP_PRIVATE, fileDesc, 0);
		relations[i].value += 2;
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
	batch_list batches;
	batches.size = 0;
	batches.head = NULL;
	while (!feof(stdin)) {
		printf("NEW Batch\n");
		query_list* queries = malloc(sizeof(query_list));
		queries->size = 0;
		queries->head = NULL;
		while (1) {
			query_info* query = malloc(sizeof(query_info));
			int returnValue = read_relations(query);
			if (returnValue == EOF) {
				getchar();
				free(query);
				free(queries);
				queries = NULL;
				break;
			} else if (returnValue){
				getchar();
				break;
			}
			read_predicates(query);
			read_projections(query);
			query_push(queries, query);
		}
		batch_push(&batches, queries);
	}

	batch_node* batch = batches.head;
	while (batch != NULL) {
		query_node* query = batch->queries->head;
		while (query != NULL) {
			execute(query->query, relations);
			//free_query(query->query);
			query = query->next;
		}
		printf("F\n");
		batch = batch->next;
	}

	for (int i = 0; i < listSize; ++i){
		munmap(relations[i].value, relations[i].num_tuples*relations[i].num_columns*sizeof(uint64_t));
	}
	free(relations);

	return 0;
}
