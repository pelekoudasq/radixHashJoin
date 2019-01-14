#include <iostream>
#include "test.h"

using std::vector;
using std::unordered_map;
using std::unordered_set;

/* Run filters first.
 * For all tables in filters, get all rows and then erase
 * all those that do not match the filter.
 * If a filter returned no results, return true, else false.
 */
bool run_filters(Query &query, vector<relList> &relations,
                 unordered_map<uint64_t, unordered_set<uint64_t> > &filtered) {
    vector<filter_info> &filter = query.filter;
    for (uint64_t i = 0; i < query.table.size(); i++) {
        uint64_t table_number = query.table[i];
        auto &f = filtered[i];
        for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
            f.insert(j);
        }
    }
    for (auto &&f : filter) {
        uint64_t table_number = query.table[f.table];
        uint64_t column_number = f.column;
        for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
            uint64_t value = relations[table_number].values[column_number][j];
            if (f.op == '>') {
                if (value <= f.number)
                    filtered[f.table].erase(j);
            } else if (f.op == '<') {
                if (value >= f.number)
                    filtered[f.table].erase(j);
            } else if (f.op == '=') {
                if (value != f.number)
                    filtered[f.table].erase(j);
            }
        }
    }

    for (size_t i = 0; i < query.table.size(); i++) {
        if (filtered[i].empty()) {
            return true;
        }
    }
    return false;
}

/* If the table has not been joined before, just parse original data and add matching rows to intermediate.
 * Else, get data from intermediate and match those, enter matching rows to intermediate.
 */
void parse_table(join_info &join, relList &relation, unordered_map<uint64_t,
  unordered_set<uint64_t> > &filtered, vector<vector<uint64_t> > &intermediate) {

    uint64_t column1_number = join.column1;
    uint64_t column2_number = join.column2;

    if (intermediate[join.table1].empty()) {
        unordered_map<uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join.table1);
        for (auto &&rowid : table->second) {
            uint64_t value1 = relation.values[column1_number][rowid];
            uint64_t value2 = relation.values[column2_number][rowid];
            if (value1 == value2) {
                intermediate[join.table1].push_back(rowid);
            }
        }
    } else {
        vector<uint64_t> oldTable(intermediate[join.table1]);
        auto i = oldTable.end();
        //size_t position = oldTable.size();
        while (i != oldTable.begin()) {
            uint64_t value1 = relation.values[column1_number][(*i)];
            uint64_t value2 = relation.values[column2_number][(*i)];
            if (value1 != value2) {
                //erase joined values from all tables
                for (auto &k : intermediate) {
                    if (!k.empty()) {
                        k.erase(i);
                    }
                }
            }
            i--;
        }
    }
}

/* for case 2: one of them is empty.
 * Create new updated intermediate and only pass data that match the new joined data.
 * Update all other full intermediates and create intermediate for the one that was empty.
 * table1 is the full table of the intermediate
 * table2 is the empty table of the intermediate_upd
 */
void change_intermediate(const vector<vector<uint64_t> > &intermediate, vector<vector<uint64_t> > &intermediate_upd,
                        uint64_t rowid1, uint64_t rowid2, const vector<uint64_t> &table1, vector<uint64_t> &table2) {

   size_t isize = intermediate.size();
   size_t size = table1.size();
   for (size_t element = 0; element < size; element++) {
       if (table1[element] == rowid1) {
           for (size_t i = 0; i < isize; ++i) {
               if (!intermediate[i].empty())
                   intermediate_upd[i].push_back(intermediate[i][element]);
           }
           table2.push_back(rowid2);
       }
   }
}

/* for case 3: both are full (both have been joined before)
 * Create new updated intermediate and only pass data that match the new joined data.
 * Update all other full intermediates.
 */
void change_both_intermediate(const vector<vector<uint64_t> > &intermediate, const vector<uint64_t> &table1,
                              const vector<uint64_t> &table2,
                              vector<vector<uint64_t> > &intermediate_upd, uint64_t rowid1, uint64_t rowid2) {

    size_t isize = intermediate.size();
    size_t size = table1.size();
    for (size_t element = 0; element < size; element++) {
        if (table1[element] == rowid1 && table2[element] == rowid2) {
            for (size_t i = 0; i < isize; ++i) {
                if (!intermediate[i].empty()) {
                    intermediate_upd[i].push_back(intermediate[i][element]);
                }
            }
        }
    }
}

/* Update for case 1:  both intermediates are empty(no previous join for either).
 * Reserve the size of result for each intermediate and push back, one by one.
 */
void getVector(vector<vector<uint64_t>> &intermediate, const join_info &join, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    vector<uint64_t> &table1 = intermediate[join.table1];
    vector<uint64_t> &table2 = intermediate[join.table2];
    table1.reserve(size);
    table2.reserve(size);
    auto s = page + size;
    for (key_tuple *kt = page; kt < s; kt++) {
        table1.push_back(kt->keyR);
        table2.push_back(kt->keyS);
    }
}

/* Update for case 2: one of them is empty.
 * Call change_intermediate, according to which intermediate is empty.
 */
void getVector2(const vector<vector<uint64_t>> &intermediate, const join_info &join,
                vector<vector<uint64_t>> &intermediate_upd, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    auto s = page + size;
    if (intermediate[join.table1].empty()) {
        const vector<uint64_t> &table1 = intermediate[join.table2];
        vector<uint64_t> &table2 = intermediate_upd[join.table1];
        for (key_tuple *kt = page; kt < s; kt++) {
          change_intermediate(intermediate, intermediate_upd, kt->keyS, kt->keyR, table1, table2);
        }
    } else {
        const vector<uint64_t> &table1 = intermediate[join.table1];
        vector<uint64_t> &table2 = intermediate_upd[join.table2];
        for (key_tuple *kt = page; kt < s; kt++) {
          change_intermediate(intermediate, intermediate_upd, kt->keyR, kt->keyS, table1, table2);
        }
    }
}

/* Update for case 3: both are full( both have been joined before).
 * For each result, update intermediate
 */
void getVector3(const vector<vector<uint64_t>> &intermediate, const join_info &join,
                vector<vector<uint64_t>> &intermediate_upd, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    auto s = page + size;
    for (key_tuple *kt = page; kt < s; kt++) {
        change_both_intermediate(intermediate, intermediate[join.table1], intermediate[join.table2],
                                 intermediate_upd, kt->keyR, kt->keyS);
    }
}

/* Update intermediate after a successful(non-empty) join between 2 different tables.
 * Different cases for if:
 * 1) both intermediates are empty(no previous join for either)
 * 2) one of them is empty
 * 3) both are full( both have been joined before)
 */
void update_intermediate(vector<vector<uint64_t> > &intermediate, const Result &results, join_info &join) {
    vector<vector<uint64_t> > intermediate_upd(intermediate.size());
    for (size_t i = 0; i < intermediate.size(); i++) {
      intermediate_upd[i].reserve(intermediate[i].size());
    }
    bucket_info *node = results.head;

    if (intermediate[join.table1].empty() && intermediate[join.table2].empty()) {
        //intermediate results for both tables are empty
        //push back all results to their intermediate vectors
        getVector(intermediate_upd, join, node, results.size);
        node = node->next;
        while (node != nullptr) {
            getVector(intermediate_upd, join, node, results.capacity);
            node = node->next;
        }
    } else if (intermediate[join.table1].empty() || intermediate[join.table2].empty()) {
        //one of them is empty, as we parse the results, we search the intermediate of the
        //table that already exists and we create the new intermediate
        getVector2(intermediate, join, intermediate_upd, node, results.size);
        node = node->next;
        while (node != nullptr) {
            getVector2(intermediate, join, intermediate_upd, node, results.capacity);
            node = node->next;
        }
    } else {
        //both are not empty, we have to parse them both to find the matching pairs
        //in the intermediate and the results from the join
        getVector3(intermediate, join, intermediate_upd, node, results.size);
        node = node->next;
        while (node != nullptr) {
            getVector3(intermediate, join, intermediate_upd, node, results.capacity);
            node = node->next;
        }
    }

    intermediate = intermediate_upd;
}
