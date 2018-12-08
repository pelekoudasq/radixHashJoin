#include <cctype>
#include <fcntl.h>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unordered_set>
#include <unordered_map>

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

using std::vector;
using std::unordered_map;
using std::unordered_set;
using std::pair;

unordered_map< uint64_t, unordered_set<uint64_t> >* run_filters(query_info& query, vector<relList>& relations) {
	vector<filter_info>& filter = query.filter;
	unordered_map< uint64_t, unordered_set<uint64_t> >* filtered = new unordered_map< uint64_t, unordered_set<uint64_t> >();
	for (uint64_t i = 0; i < query.table.size(); i++) {
		uint64_t table_number = query.table[i];
		auto& f = (*filtered)[i];
		for(uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
			f.insert(j);
		}
	}
	for (auto&& f : filter) {
		uint64_t table_number = query.table[f.table];
		uint64_t column_number = f.column;
		size_t offset = column_number*relations[table_number].num_tuples;
		for(uint64_t j = 0; j < relations[table_number].num_tuples; j++){
			uint64_t value = relations[table_number].value[offset+j];
			if (f.op == '>') {
				if ( !(value > f.number) )
					(*filtered)[f.table].erase(j);
			}
			else if (f.op == '<') {
				if ( !(value < f.number) )
					(*filtered)[f.table].erase(j);
			}
			else if (f.op == '=') {
				if ( !(value == f.number) )
					(*filtered)[f.table].erase(j);
			}
		}
	}
	return filtered;
}

void parse_table(join_info& join, vector<relList>& relations, uint64_t table_number, 
	unordered_map< uint64_t, unordered_set<uint64_t> >& filtered, vector< vector<uint64_t> >& intermediate, size_t tableSize) {

	uint64_t column1_number = join.column1;
	uint64_t column2_number = join.column2;
	size_t offset = relations[table_number].num_tuples;

	if ( intermediate[join.table1].empty() ) {
		unordered_map< uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join.table1);
		for (auto&& rowid : table->second){
			uint64_t value1 = relations[table_number].value[column1_number*offset+rowid];
			uint64_t value2 = relations[table_number].value[column2_number*offset+rowid];
			if (value1 == value2){
				printf("CHANGES\n");
				intermediate[join.table1].push_back(rowid);
			}
		}
	} else {
		vector<uint64_t> oldTable (intermediate[join.table1]);
		vector<uint64_t>::iterator i = oldTable.end();
		//size_t position = oldTable.size();
		while ( i != oldTable.begin() ){
			uint64_t value1 = relations[table_number].value[column1_number*offset+(*i)];
			uint64_t value2 = relations[table_number].value[column2_number*offset+(*i)];
			if (value1 == value2){
				//erase joined values from all tables
				for (size_t k = 0; k < tableSize; k++)	{
					if ( !intermediate[k].empty() ){
						intermediate[k].erase(i);
					}
				}
			}
			i--;
		}
	}
}

relation *create_relation(uint64_t join_table, vector<relList>& relations, uint64_t table_number, uint64_t column_number,
	unordered_map< uint64_t, unordered_set<uint64_t> >& filtered, vector< vector<uint64_t> >& intermediate) {

	relation *R = (relation*)malloc(sizeof(relation));
	size_t offset = column_number*relations[table_number].num_tuples;

	if ( intermediate[join_table].empty() ){
		unordered_map< uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join_table);
		R->num_tuples = table->second.size();
		R->tuples = (tuple*)malloc(R->num_tuples*sizeof(tuple));
		size_t i = 0;
		for (auto&& rowid : table->second){
			R->tuples[i].key = rowid;
			R->tuples[i].payload = relations[table_number].value[offset+rowid];
			i++;
		}
	} else {
		R->num_tuples = intermediate[join_table].size();
		R->tuples = (tuple*)malloc((R->num_tuples)*sizeof(tuple));
		for (size_t i = 0; i < R->num_tuples; i++) {
			size_t rowid = intermediate[join_table].at(i);
			R->tuples[i].key = rowid;
			R->tuples[i].payload = relations[table_number].value[offset+rowid];
		}
	}

	return R;
}

void update_intermediate(vector< vector<uint64_t> >& intermediate, result *results, join_info& join){
	return;
}

void run_joins(query_info& query, vector<relList>& relations, unordered_map< uint64_t, unordered_set<uint64_t> >& filtered) {

	vector<join_info>& joins = query.join;
	vector< vector<uint64_t> > intermediate(query.table.size());
	printf("join\n");
	for (auto&& join : joins) {
		if ( join.table1 == join.table2 ){
			uint64_t table_number = query.table[join.table1];
			parse_table(join, relations, table_number, filtered, intermediate, query.table.size());
		} else {
			uint64_t table1_number = query.table[join.table1];
			uint64_t column1_number = join.column1;
			uint64_t table2_number = query.table[join.table2];
			uint64_t column2_number = join.column2;
			//get rowids for these tables from intermediate or filtered
			//create relations to send to RadixHashJoin
			relation *relR = create_relation(join.table1, relations, table1_number, column1_number, filtered, intermediate);
			relation *relS = create_relation(join.table2, relations, table2_number, column2_number, filtered, intermediate);
			//send relations to RadxHashJoin
			result* results = RadixHashJoin(relR, relS);
			//get results to intermediate
			update_intermediate(intermediate, results, join);
		}
		for (size_t i = 0; i < query.table.size(); ++i) {
			if (filtered[i].empty())
				printf("'0' ");
			else
				printf("%ld ", filtered[i].size());
		}
		printf("\n");
		for (size_t i = 0; i < query.table.size(); ++i) {
			if (intermediate[i].empty())
				printf("'0' ");
			else
				printf("%ld ", intermediate[i].size());
		}
		printf("\n");
	}
}

void execute(query_info& query, vector<relList>& relations) {

	unordered_map< uint64_t, unordered_set<uint64_t> >* filtered = run_filters(query, relations); // size = query.table.size()

	run_joins(query, relations, *filtered);
	// for (pair<uint64_t, unordered_set<uint64_t>> table : *filtered) {
	// 	printf("---Table %d---\n", table. );
	// 	for (auto&& rowid : table.second) {
	// 		printf("%d, ", rowid);
	// 	}
	// 	printf("\n");
	// }
	delete filtered;
	// Run joins.

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
		if (read(fileDesc, &relation.num_tuples, sizeof(uint64_t)) < 0){
			return -1;
		}
		if (read(fileDesc, &relation.num_columns, sizeof(uint64_t)) < 0){
			return -1;
		}
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
