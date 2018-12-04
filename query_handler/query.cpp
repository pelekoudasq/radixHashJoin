#include <cctype>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "query.h"

/* Get the whole number from string input.
	Return number as uint64_t and next character as delim */
uint64_t read_number(int ch, int* delim){
	uint64_t num = 0;
	while( isdigit(ch) ){
		num = num*10 + ch - '0';
		ch = getchar();
	}
	*delim = ch;
	return num;
}

/* Gets the tables needed for the query.
 * Also, checks for end of input and returns accordingly.
 * Takes the query struct as argument to update.
 * Returns true on batch end.
 */
bool read_relations(query_info& query){
	int ch = getchar();
	//check if end of batch or end of file
	if (ch == 'F' || ch == EOF) return true;

	std::vector<uint64_t>& table = query.table;
	//push_table
	table.push_back(read_number(ch, &ch));

	while(ch != '|'){
		//push_table
		ch = getchar();
		table.push_back(read_number(ch, &ch));
	}
	return false;
}

/* Reads predicates for query. Stores filters and joins separetely.
	Takes the query struct as argument to update.*/
void read_predicates(query_info& query){
	std::vector<join_info>& join = query.join;
	std::vector<filter_info>& filter = query.filter;
	int ch = '\0';
	while(ch != '|'){										//for every predicate
		int op;
		ch = getchar();
		uint64_t table1 = read_number(ch, &op); 			//read number(table1), ignore->(.) that follows it
		uint64_t column1 = read_number(getchar(), &op);		//read number(column1), save operation that follows it
		uint64_t unknown = read_number(getchar(), &ch);		//read next number after operation, save delimiter
		if(ch == '.'){										//if delimiter is (.) we have join
			uint64_t column2 = read_number(getchar(), &ch); //read number(column2), save what follows( & or | )
			join_info temp;
			temp.table1 = table1;
			temp.table2 = unknown;
			temp.column1 = column1;
			temp.column2 = column2;
			join.push_back(temp);
		} else {
			filter_info temp;
			temp.table = table1;
			temp.column = column1;
			temp.op = op;
			temp.number = unknown;
			filter.push_back(temp);
		}
	}
}

/* Gets projections required.
	Takes the query struct as argument to update. */
void read_projections(query_info& query){
	std::vector<proj_info>& proj = query.proj;
	int ch = '\0';
	while(ch != '\n'){										//while not end of line
		uint64_t table = read_number(getchar(), &ch);		//read number(table), ignore->(.) that follows it
		uint64_t column = read_number(getchar(), &ch);		//read number(column), keep ch to check if \n
		proj_info temp;
		temp.table = table;
		temp.column = column;
		proj.push_back(temp);
	}
}

/* Fills the intermediate table with possibly lots of other intermediates */
void fill_intermediate(vector<table_ids>& intermediate_set) {
	0	2	1		3
	x	x	x		x
	x	x	x		x
	x	x	x		x
	x	x	x		x
	/*
	intermediate_set[0] --> 1ος intermediate που φτιάχνουμε και του βάζουμε πράμα μέχρι να
	μην χρησμοποιηθεί στήλη πίνακα που έχει μέσα, οπότε τότε βάζουμε ένα νέο intermediate
	στο intermediate_set κλπ.
	*/
}

/* Prints query struct to check if input is read correctly. Assumes joins are given before filters. */
void print_query(query_info& query) {
	//tables
	if (query.table.size())
		printf("%ld", query.table[0]);
	for (int i = 1; i < query.table.size(); i++)
		printf(" %ld", query.table[i]);
	putchar('|');
	//joins
	if (query.join.size())
		printf("%ld.%ld=%ld.%ld", query.join[0].table1, query.join[0].column1, query.join[0].table2, query.join[0].column2);
	for (int i = 1; i < query.join.size(); i++)
		printf("&%ld.%ld=%ld.%ld", query.join[i].table1, query.join[i].column1, query.join[i].table2, query.join[i].column2);
	//filters
	if (query.filter.size()) {
		if (query.join.size()) putchar('&');
		printf("%ld.%ld%c%ld", query.filter[0].table, query.filter[0].column, query.filter[0].op, query.filter[0].number);
	}
	for (int i = 1; i < query.filter.size(); i++)
		printf(" %ld.%ld%c%ld", query.filter[i].table, query.filter[i].column, query.filter[i].op, query.filter[i].number);
	putchar('|');
	//projections
	if (query.proj.size())
		printf("%ld.%ld", query.proj[0].table, query.proj[0].column);
	for (int i = 1; i < query.proj.size(); i++)
		printf(" %ld.%ld", query.proj[i].table, query.proj[i].column);
	putchar('\n');
}
