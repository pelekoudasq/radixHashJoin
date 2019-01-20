#include "Query.h"
#include "intermediate.h"
#include <iostream>
#include <cmath>

using namespace std;

/* Get the whole number from string input.
	Return number as uint64_t and next character as delim */
uint64_t read_number(int ch, int *delim) {
    uint64_t num = 0;
    while (isdigit(ch)) {
        num = num * 10 + ch - '0';
        ch = getchar();
    }
    *delim = ch;
    return num;
}

/* Gets the tables needed for the query.
 * Also, checks for end of input and returns accordingly.
 * Returns true on batch end.
 */
bool Query::read_relations(int ch) {
    //push_table
    table.push_back(read_number(ch, &ch));

    while (ch != '|') {
        //push_table
        ch = getchar();
        table.push_back(read_number(ch, &ch));
    }
    return false;
}


/* Reads predicates for query. Stores filters and joins separetely.*/
void Query::read_predicates() {
    int ch = '\0';
    while (ch != '|') {                                     //for every predicate
        int op;
        ch = getchar();
        uint64_t table1 = read_number(ch, &op);             //read number(table1), ignore->(.) that follows it
        uint64_t column1 = read_number(getchar(), &op);     //read number(column1), save operation that follows it
        uint64_t unknown = read_number(getchar(), &ch);     //read next number after operation, save delimiter
        if (ch == '.') {                                    //if delimiter is (.) we have join
            uint64_t column2 = read_number(getchar(), &ch); //read number(column2), save what follows( & or | )
            join.emplace_back(table1, column1, unknown, column2);
        } else {
            filter.emplace_back(table1, column1, op, unknown);
        }
    }
}

/* Gets projections required. */
void Query::read_projections() {
    int ch = '\0';
    while (ch != '\n') {                                      //while not end of line
        uint64_t table = read_number(getchar(), &ch);         //read number(table), ignore->(.) that follows it
        uint64_t column = read_number(getchar(), &ch);        //read number(column), keep ch to check if \n
        proj.emplace_back(table, column);
    }
}

/* Get sums for projections part */
uint64_t column_proj(const vector<relList> &relations, uint64_t table_number, uint64_t column_number,
                     const vector<uint64_t> &vec) {
    const relList &rel = relations[table_number];
    uint64_t sum = 0;
    for (auto &&val : vec) {
        sum += rel.values[column_number][val];
    }
    return sum;
}

/* Run filters first.
 * For all tables in filters, get all rows and then erase
 * all those that do not match the filter.
 * If a filter returned no results, return true, else false.
 */
bool Query::run_filters(vector<relList> &relations, unordered_map<uint64_t, unordered_set<uint64_t> > &filtered) {
    for (uint64_t i = 0; i < table.size(); i++) {
        uint64_t table_number = table[i];
        filtered[i].reserve(relations[table_number].num_tuples);
        for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
            filtered[i].insert(j);
        }
        stats[i].fill(relations[table_number]);
    }

    for (auto &&f : filter) {
        uint64_t table_number = table[f.table];
        uint64_t column_number = f.column;
        if (f.op == '>') {
            if (f.number > stats[f.table].max[column_number]) {
                return true;
            }
            for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
                uint64_t value = relations[table_number].values[column_number][j];
                if (value <= f.number) {
                    filtered[f.table].erase(j);
                }
            }
            if (filtered[f.table].empty()) {
                return true;
            }

            if (stats[f.table].distinct[column_number] != 1) {
                stats[f.table].distinct[column_number] =
                        (stats[f.table].distinct[column_number] * (stats[f.table].max[column_number] - f.number + 1)) /
                        (stats[f.table].max[column_number] - stats[f.table].low[column_number]);
            }
            stats[f.table].low[column_number] = f.number + 1;
        } else if (f.op == '<') {
            if (f.number < stats[f.table].low[column_number]) {
                return true;
            }
            for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
                uint64_t value = relations[table_number].values[column_number][j];
                if (value >= f.number) {
                    filtered[f.table].erase(j);
                }
            }
            if (filtered[f.table].empty()) {
                return true;
            }

            if (stats[f.table].distinct[column_number] != 1)
                stats[f.table].distinct[column_number] =
                        (stats[f.table].distinct[column_number] * (f.number - 1 - stats[f.table].low[column_number])) /
                        (stats[f.table].max[column_number] - stats[f.table].low[column_number]);
            stats[f.table].max[column_number] = f.number - 1;
        } else if (f.op == '=') {
            for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
                uint64_t value = relations[table_number].values[column_number][j];
                if (value != f.number) {
                    filtered[f.table].erase(j);
                }
            }
            if (filtered[f.table].empty()) {
                return true;
            }
            stats[f.table].low[column_number] = f.number;
            stats[f.table].max[column_number] = f.number;
            stats[f.table].distinct[column_number] = 1;
        }
        for (size_t i = 0; i < relations[table_number].num_columns; ++i) {
            if (i != column_number) {
                stats[f.table].distinct[i] = (size_t) ((double) stats[f.table].distinct[i] *
                                           (1 - pow(1 - (double) filtered[f.table].size() / (double) stats[f.table].size,
                                                   (double) stats[f.table].size / (double) stats[f.table].distinct[i])));
            }
        }
        stats[f.table].size = filtered[f.table].size();
    }

    return false;
}

/* If self-join, just parse table.
 * Else, make tuples into relation for join and RadixHashJoin.
 * If join is empty, stop query and sum projections.
 */
void Query::run_joins(JobScheduler &js, vector<relList> &relations,
                      unordered_map<uint64_t, unordered_set<uint64_t> > &filtered) {
    vector<vector<uint64_t> > intermediate(table.size());
    for (auto &&j : join) {
        if (j.table1 == j.table2) {
            uint64_t table_number = table[j.table1];
            parse_table(j, relations[table_number], filtered, intermediate);
        } else {
            uint64_t table1_number = table[j.table1];
            uint64_t column1_number = j.column1;
            uint64_t table2_number = table[j.table2];
            uint64_t column2_number = j.column2;
            //get rowids for these tables from intermediate or filtered
            //create relations to send to RadixHashJoin
            relation relR;
            relation relS;
            relR.create_relation(j.table1, relations[table1_number], column1_number, filtered,
                                 intermediate[j.table1]);
            relS.create_relation(j.table2, relations[table2_number], column2_number, filtered,
                                 intermediate[j.table2]);
            //send relations to RadxHashJoin
            Result results;
            results.multiRadixHashJoin(js, relR, relS);
            //if we have no results in this join there are no results
            if (results.isEmpty()) {
                filtered_out = true;
                break;
            }
            //get results to intermediate
            update_intermediate(intermediate, results, j);
        }
    }
    js.barrier();

    for (auto &p : proj) {
        p.sum = column_proj(relations, table[p.table], p.column, intermediate[p.table]);
    }
}

/* Run filters, then joins */
void Query::execute(JobScheduler &js, vector<relList> &relations) {
    unordered_map<uint64_t, unordered_set<uint64_t> > filtered;
    filtered_out = run_filters(relations, filtered);
    if (filtered_out) {
        return;
    }
    run_joins(js, relations, filtered);
}

/* Print projections when join has results. */
void printVAL(vector<proj_info>::const_iterator &it) {
    cout << it->sum;
    it++;
}

/* Print projections when a join/filter has returned no results. */
void printNULL(vector<proj_info>::const_iterator &it) {
    cout << "NULL";
    it++;
}

/* Print out projections according to filters */
void Query::print() const {
    auto print = filtered_out ? printNULL : printVAL;
    auto it = proj.begin();
    print(it);
    while (it != proj.end()) {
        cout << ' ';
        print(it);
    }
    cout << endl;
}

Query::Query(int ch) {
    read_relations(ch);
    read_predicates();
    read_projections();
    stats.resize(table.size());
}

/* Constructors for info structs. */

proj_info::proj_info(uint64_t table, uint64_t column) :
        table(table), column(column), sum(0) {}

filter_info::filter_info(uint64_t table, uint64_t column, int op, uint64_t number) :
        table(table), column(column), op(op), number(number) {}

join_info::join_info(uint64_t table1, uint64_t column1, uint64_t table2, uint64_t column2) :
        table1(table1), column1(column1), table2(table2), column2(column2) {}
