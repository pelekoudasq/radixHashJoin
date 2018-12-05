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

using namespace std;

// bool **run_filters(query_info& query, vector<relList>& relations) {
// 	vector<filter_info>& filter = query.filter;
// 	bool **disqualified = (bool**)calloc(sizeof(bool*), query.table.size());
// 	for (auto&& f : filter) {
// 		uint64_t table_number = query.table[f.table];
// 		uint64_t column_number = f.column;
// 		if (disqualified[f.table] == NULL)
// 			disqualified[f.table] = new bool[relations[table_number].num_tuples](); // sets all to false
// 		size_t offset = column_number*relations[table_number].num_tuples;
// 		for(size_t j = 0; j < relations[table_number].num_tuples; j++){
// 			uint64_t value = relations[table_number].value[offset+j];
// 			if (disqualified[f.table][j]) {
// 				if (f.op == '>')
// 					disqualified[f.table][j] = !(value > f.number);
// 				else if (f.op == '<')
// 					disqualified[f.table][j] = !(value < f.number);
// 				else if (f.op == '=')
// 					disqualified[f.table][j] = !(value == f.number);
// 			}
// 		}
// 	}
// 	return disqualified;
// }

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

// unordered_map< uint64_t, vector<uint64_t> >* run_filters2(query_info& query, vector<relList>& relations) {
// 	vector<filter_info>& filter = query.filter;
// 	unordered_set<uint64_t>* disqualified = new unordered_set<uint64_t>[query.table.size()];
// 	for (auto&& f : filter) {
// 		uint64_t table_number = query.table[f.table];
// 		uint64_t column_number = f.column;
// 		size_t offset = column_number*relations[table_number].num_tuples;
// 		for(size_t j = 0; j < relations[table_number].num_tuples; j++){
// 			uint64_t value = relations[table_number].value[offset+j];
// 			if (f.op == '>') {
// 				if ( !(value > f.number) )
// 					disqualified[f.table].insert(j);
// 			}
// 			else if (f.op == '<') {
// 				if ( !(value < f.number) )
// 					disqualified[f.table].insert(j);
// 			}
// 			else if (f.op == '=') {
// 				if ( !(value == f.number) )
// 					disqualified[f.table].insert(j);
// 			}
// 		}
// 	}
// 	unordered_map< uint64_t, vector<uint64_t> >* intermediates = new unordered_map< uint64_t, vector<uint64_t> >;
// 	// Create an intermediate for every filter result.
// 	for (size_t i = 0; i < query.table.size(); i++) {
// 		if (!disqualified2[i].empty()) {
// 			auto& intermediate = intermediates[i];
// 			for(size_t j = 0; j < relations[query.table[i]].num_tuples; j++) {
// 				if(disqualified2[i].find(j) == disqualified2[i].end()) {
// 					//add to intermediate
// 					intermediate.push_back(j);
// 				}
// 			}
// 		}
// 	}
//
// 	delete[] disqualified2;
// 	return intermediates;
// }

void parse_table(join_info& join, vector<relList>& relations, uint64_t table_number) {

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

void run_joins(query_info& query, vector<relList>& relations, unordered_map<uint64_t, vector<uint64_t>>& intermediates) {

	vector<join_info>& join = query.join;

	for (auto&& j : join) {
		if ( j.table1 == j.table2 ){
			uint64_t table_number = query.table[j.table1];
			parse_table(j, relations, table_number);
		} else {
			uint64_t table1_number = query.table[j.table1];
			uint64_t column1_number = j.column1;
			uint64_t table2_number = query.table[j.table2];
			uint64_t column2_number = j.column2;
			//get rowids for these tables from intermediate results, if they exist
			//create relations to send to RadixHashJoin
			//send relations to RadxHashJoin
			//get results to intermediate
		}
	}
}

void execute(query_info& query, vector<relList>& relations) {

	unordered_map< uint64_t, unordered_set<uint64_t> >* filtered = run_filters(query, relations); // size = query.table.size()

	// for (pair<uint64_t, unordered_set<uint64_t>> table : *filtered) {
	// 	printf("---Table %d---\n", table.first);
	// 	for (auto&& rowid : table.second) {
	// 		printf("%d, ", rowid);
	// 	}
	// 	printf("\n");
	// }
	delete filtered;
	// Run joins.
	// run_joins(query, relations, intermediates);

	/*for (size_t i = 0; i < query.table.size(); i++) {
		if (disqualified2[i].size() != 0)
			delete[] disqualified2[i];
	}*/
	//free(disqualified2);

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
