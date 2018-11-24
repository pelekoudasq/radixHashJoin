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
#include "auxFun.h"
#include "join.h"
#include "resultList.h"



typedef struct fileList{
	char *filepath;
	struct fileList *next;
}fileList;

fileList *push_file(fileList *head, char* filepath){
	
	fileList *newNode = malloc(sizeof(fileList));
	newNode->filepath = filepath;
	newNode->next = head;
	return newNode;
}

fileList *pop_file(fileList *head, char **filepath){

	fileList *temp = head->next;
	*filepath = head->filepath;
	free(head);
	return temp;
}

typedef struct {
	uint64_t num_tuples;
	uint64_t num_columns;
	uint64_t *value;
}relList;

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

uint64_t read_number(int ch, int* delim){
	uint64_t num = 0;
	while( isdigit(ch) ){
		num = num*10 + ch - '0';
		ch = getchar();
	}
	*delim = ch;
	return num;
}

int read_relations(query_info* query){
	uint64_t *table = malloc(sizeof(uint64_t)*4);
	size_t table_size = 0;
	//push_table
	int ch = getchar();
	if (ch == 'F' ) return 1;
	if (ch == EOF) return EOF;
	table[table_size] = read_number(ch, &ch);
	table_size++;
	while(ch != '|'){
		//push_table
		ch = getchar();
		table[table_size] = read_number(ch, &ch);
		table_size++;
	}
	query->table = table;
	query->table_size = table_size;
	return 0;
}

void read_predicates(query_info* query){
	join_info *join = malloc(sizeof(join_info)*4);
	filter_info *filter = malloc(sizeof(filter_info)*4);
	size_t join_size = 0;
	size_t filter_size = 0;
	int ch = '\0';
	while(ch != '|'){										//for every predicate
		int op;
		ch = getchar();
		uint64_t table1 = read_number(ch, &op); 			//read number(table1), ignore->(.) that follows it
		uint64_t column1 = read_number(getchar(), &op);		//read number(column1), save operation that follows it
		uint64_t unknown = read_number(getchar(), &ch);		//read next number after operation, save delimiter
		if(ch == '.'){										//if delimighter is (.) we have join
			uint64_t column2 = read_number(getchar(), &ch); //read number(column2), save what follows( & or | )
			join[join_size].table1 = table1;
			join[join_size].table2 = unknown;
			join[join_size].column1 = column1;
			join[join_size].column2 = column2;
			join_size++;
		} else {
			filter[filter_size].table = table1;
			filter[filter_size].column = column1;
			filter[filter_size].op = op;
			filter[filter_size].number = unknown;
			filter_size++;
		}
	}
	query->join = join;
	query->filter = filter;
	query->join_size = join_size;
	query->filter_size = filter_size;
}


void read_projections(query_info* query){
	query->proj = malloc(sizeof(proj_info)*4);
	query->proj_size = 0;
	int ch = '\0';
	while(ch != '\n'){										//for every predicate
		uint64_t table = read_number(getchar(), &ch);		//read number(table1), ignore->(.) that follows it
		uint64_t column = read_number(getchar(), &ch);		//read number(column1), save operation that follows it
		query->proj[query->proj_size].table = table;
		query->proj[query->proj_size].column = column;
		query->proj_size++;
	}
}

void free_query(query_info* query) {
	free(query->table);
	free(query->join);
	free(query->filter);
	free(query->proj);
}
// void query_init(query_info* query) {
// 	query->filter_size = 0;
// 	query->join_size = 0;
// 	query->table_size = 0;
// 	query->proj_size = 0;
// }

void print_query(query_info* query) {
	if (query->table_size)
		printf("%ld", query->table[0]);
	for (int i = 1; i < query->table_size; i++)
		printf(" %ld", query->table[i]);
	putchar('|');
	if (query->join_size)
		printf("%ld.%ld=%ld.%ld", query->join[0].table1, query->join[0].column1, query->join[0].table2, query->join[0].column2);
	for (int i = 1; i < query->join_size; i++)
		printf("&%ld.%ld=%ld.%ld", query->join[i].table1, query->join[i].column1, query->join[i].table2, query->join[i].column2);
	if (query->filter_size) {
		if (query->join_size) putchar('&');
		printf("%ld.%ld%c%ld", query->filter[0].table, query->filter[0].column, query->filter[0].op, query->filter[0].number);
	}
	for (int i = 1; i < query->filter_size; i++)
		printf(" %ld.%ld%c%ld", query->filter[i].table, query->filter[i].column, query->filter[i].op, query->filter[i].number);
	putchar('|');
	if (query->proj_size)
		printf("%ld.%ld", query->proj[0].table, query->proj[0].column);
	for (int i = 1; i < query->proj_size; i++)
		printf(" %ld.%ld", query->proj[i].table, query->proj[i].column);
	putchar('\n');
}

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
	//run_queries();
	print_query(query);
}

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

void query_push(query_list* list, query_info* query) {
	query_node* temp = malloc(sizeof(query_node));
	temp->next = NULL;
	temp->query = query;
	if (list->head == NULL) list->head = temp;
	else list->tail->next = temp;
	list->tail = temp;
	list->size++;
}

void batch_push(batch_list* list, query_list* queries) {
	if (queries == NULL)
		return;
	batch_node* temp = malloc(sizeof(batch_node));
	temp->next = NULL;
	temp->queries = queries;
	if (list->head == NULL) list->head = temp;
	else list->tail->next = temp;
	list->tail = temp;
	list->size++;
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
