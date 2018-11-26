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

#include "query.h"

uint64_t read_number(int ch, int* delim){
	uint64_t num = 0;
	while( isdigit(ch) ){
		num = num*10 + ch - '0';
		ch = getchar();
	}
	*delim = ch;
	return num;
}

int read_relations(query_info *query){
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

void read_predicates(query_info *query){
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

void read_projections(query_info *query){
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

void free_query(query_info *query) {
	free(query->table);
	free(query->join);
	free(query->filter);
	free(query->proj);
}

void print_query(query_info *query) {
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

void query_push(query_list *list, query_info *query) {
	query_node* temp = malloc(sizeof(query_node));
	temp->next = NULL;
	temp->query = query;
	if (list->head == NULL) list->head = temp;
	else list->tail->next = temp;
	list->tail = temp;
	list->size++;
}

void batch_push(batch_list *list, query_list *queries) {
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
