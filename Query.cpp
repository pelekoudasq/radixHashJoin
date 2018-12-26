#include "Query.h"
#include "test.h"
#include <iostream>

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
 * Takes the query struct as argument to update.
 * Returns true on batch end.
 */
bool Query::read_relations() {
    int ch = getchar();
    //check if end of batch or end of file
    if (ch == 'F' || ch == EOF) return true;

    //push_table
    table.push_back(read_number(ch, &ch));

    while (ch != '|') {
        //push_table
        ch = getchar();
        table.push_back(read_number(ch, &ch));
    }
    return false;
}


/* Reads predicates for query. Stores filters and joins separetely.
	Takes the query struct as argument to update.*/
void Query::read_predicates() {
    int ch = '\0';
    while (ch != '|') {                                        //for every predicate
        int op;
        ch = getchar();
        uint64_t table1 = read_number(ch, &op);            //read number(table1), ignore->(.) that follows it
        uint64_t column1 = read_number(getchar(), &op);        //read number(column1), save operation that follows it
        uint64_t unknown = read_number(getchar(), &ch);        //read next number after operation, save delimiter
        if (ch == '.') {                                        //if delimiter is (.) we have join
            uint64_t column2 = read_number(getchar(), &ch); //read number(column2), save what follows( & or | )
            join.emplace_back(table1, column1, unknown, column2);
        } else {
            filter.emplace_back(table1, column1, op, unknown);
        }
    }
}

/* Gets projections required.
	Takes the query struct as argument to update. */
void Query::read_projections() {
    int ch = '\0';
    while (ch != '\n') {                                      //while not end of line
        uint64_t table = read_number(getchar(), &ch);         //read number(table), ignore->(.) that follows it
        uint64_t column = read_number(getchar(), &ch);        //read number(column), keep ch to check if \n
        proj.emplace_back(table, column);
    }
}

uint64_t column_proj(const vector<relList> &relations, uint64_t table_number, uint64_t column_number,
                     const vector<uint64_t> &vec) {
    const relList &rel = relations[table_number];
    uint64_t sum = 0;
    size_t offset = column_number * rel.num_tuples;
    for (auto &&val : vec) {
        sum += rel.value[offset + val];
    }
    return sum;
}

void printVAL(const Query &query) {
    const vector<proj_info> &projections = query.proj;
    auto it = projections.begin();
    std::cout << it->sum;
    it++;
    while (it != projections.end()) {
        std::cout << ' ' << it->sum;
        it++;
    }
    std::cout << std::endl;
}

void printNULL(const Query &query) {
    const vector<proj_info> &projections = query.proj;
    auto it = projections.begin();
    std::cout << "NULL";
    it++;
    while (it != projections.end()) {
        std::cout << ' ' << "NULL";
        it++;
    }
    std::cout << std::endl;
}

void run_joins(Query &query, vector<relList> &relations, unordered_map<uint64_t, unordered_set<uint64_t> > &filtered) {
    vector<join_info> &joins = query.join;
    vector<vector<uint64_t> > intermediate(query.table.size());
    for (auto &&join : joins) {
        if (join.table1 == join.table2) {
            uint64_t table_number = query.table[join.table1];
            parse_table(join, relations, table_number, filtered, intermediate);
        } else {
            uint64_t table1_number = query.table[join.table1];
            uint64_t column1_number = join.column1;
            uint64_t table2_number = query.table[join.table2];
            uint64_t column2_number = join.column2;
            //get rowids for these tables from intermediate or filtered
            //create relations to send to RadixHashJoin
            relation relR;
            relation relS;
            relR.create_relation(join.table1, relations[table1_number], column1_number, filtered,
                                 intermediate[join.table1]);
            relS.create_relation(join.table2, relations[table2_number], column2_number, filtered,
                                 intermediate[join.table2]);
            //send relations to RadxHashJoin
            Result results;
            results.RadixHashJoin(relR, relS);
            //if we have no results in this join, then empty the intermediate, no results
            if (results.head == nullptr) {
                break;
            }
            //get results to intermediate
            update_intermediate(intermediate, results, join);
            //free process' structures
        }
    }

    for (auto &p : query.proj) {
        p.sum = column_proj(relations, query.table[p.table], p.column, intermediate[p.table]);
    }
}

void Query::execute(vector<relList> &relations) {
    unordered_map<uint64_t, unordered_set<uint64_t> > filtered;
    filtered_out = run_filters(*this, relations, filtered);
    if (!filtered_out) {
        run_joins(*this, relations, filtered);
    }
}

void Query::print() {
    if (filtered_out) {
        printNULL(*this);
    } else {
        printVAL(*this);
    }
}

proj_info::proj_info(uint64_t table, uint64_t column) :
        table(table), column(column), sum(0) {}

filter_info::filter_info(uint64_t table, uint64_t column, int op, uint64_t number) :
        table(table), column(column), op(op), number(number) {}

join_info::join_info(uint64_t table1, uint64_t column1, uint64_t table2, uint64_t column2) :
        table1(table1), column1(column1), table2(table2), column2(column2) {}
