#include "join.h"
#include <unordered_map>
#include <set>
#include <iostream>
#include "test.h"

using std::vector;
using std::set;
using std::unordered_map;
using std::unordered_set;
using std::pair;

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
        size_t offset = column_number * relations[table_number].num_tuples;
        for (uint64_t j = 0; j < relations[table_number].num_tuples; j++) {
            uint64_t value = relations[table_number].value[offset + j];
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

void parse_table(join_info &join, vector<relList> &relations, uint64_t table_number,
                 unordered_map<uint64_t, unordered_set<uint64_t> > &filtered, vector<vector<uint64_t> > &intermediate,
                 size_t tableSize) {

    uint64_t column1_number = join.column1;
    uint64_t column2_number = join.column2;
    size_t offset = relations[table_number].num_tuples;

    if (intermediate[join.table1].empty()) {
        unordered_map<uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join.table1);
        for (auto &&rowid : table->second) {
            uint64_t value1 = relations[table_number].value[column1_number * offset + rowid];
            uint64_t value2 = relations[table_number].value[column2_number * offset + rowid];
            if (value1 == value2) {
                printf("CHANGES\n");
                intermediate[join.table1].push_back(rowid);
            }
        }
    } else {
        vector<uint64_t> oldTable(intermediate[join.table1]);
        auto i = oldTable.end();
        //size_t position = oldTable.size();
        while (i != oldTable.begin()) {
            uint64_t value1 = relations[table_number].value[column1_number * offset + (*i)];
            uint64_t value2 = relations[table_number].value[column2_number * offset + (*i)];
            if (value1 != value2) {
                //erase joined values from all tables
                for (size_t k = 0; k < tableSize; k++) {
                    if (!intermediate[k].empty()) {
                        intermediate[k].erase(i);
                    }
                }
            }
            i--;
        }
    }
}

void relation::foo(const relList &rel, size_t offset, const unordered_set<uint64_t> &uniqueValues) {
    num_tuples = uniqueValues.size();
    tuples = new tuple[num_tuples];
    size_t i = 0;
    for (auto &&rowid : uniqueValues) {
        tuples[i].key = rowid;
        tuples[i].payload = rel.value[offset + rowid];
        i++;
    }
}

void relation::create_relation(uint64_t join_table, relList &rel, uint64_t column_number,
                               unordered_map<uint64_t, unordered_set<uint64_t> > &filtered,
                               vector<uint64_t> &inter) {
    size_t offset = column_number * rel.num_tuples;
    if (inter.empty()) {
        unordered_map<uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join_table);
        const unordered_set<uint64_t> &uniqueValues = table->second;
        foo(rel, offset, uniqueValues);
    } else {
        unordered_set<uint64_t> uniqueValues;
        for (auto &&val : inter)
            uniqueValues.insert(val);
        foo(rel, offset, uniqueValues);
    }
}

void change_intermediate(const vector<vector<uint64_t> > &intermediate, vector<vector<uint64_t> > &intermediate_upd,
                         uint64_t rowid1, uint64_t rowid2, size_t table_existing, size_t table_n_existing,
                         size_t tableSize) {

    for (size_t element = 0; element < intermediate[table_existing].size(); element++) {
        if (intermediate[table_existing].at(element) == rowid1) {
            for (size_t i = 0; i < tableSize; ++i) {
                if (!intermediate[i].empty())
                    intermediate_upd[i].push_back(intermediate[i].at(element));
            }
            intermediate_upd[table_n_existing].push_back(rowid2);
        }
    }
}

void change_both_intermediate(const vector<vector<uint64_t> > &intermediate,
                              vector<vector<uint64_t> > &intermediate_upd, uint64_t rowid1, uint64_t rowid2,
                              size_t table1, size_t table2, size_t tableSize) {

    for (size_t element = 0; element < intermediate[table1].size(); element++) {
        if (intermediate[table1].at(element) == rowid1 && intermediate[table2].at(element) == rowid2) {
            for (size_t i = 0; i < tableSize; ++i) {
                if (!intermediate[i].empty())
                    intermediate_upd[i].push_back(intermediate[i].at(element));
            }
        }
    }
}

void getVector(vector<vector<uint64_t>> &intermediate_upd, const join_info &join, const bucket_info *temp,
               size_t size) {
    auto *page = (key_tuple *) &temp[1];
    intermediate_upd[join.table1].reserve(size);
    intermediate_upd[join.table2].reserve(size);
    for (size_t i = 0; i < size; i++) {
        intermediate_upd[join.table1].push_back(page[i].keyR);
        intermediate_upd[join.table2].push_back(page[i].keyS);
    }
}

void getVector2(const vector<vector<uint64_t>> &intermediate, const join_info &join, size_t tableSize,
                vector<vector<uint64_t>> &intermediate_upd, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    if (intermediate[join.table2].empty()) {
        for (size_t i = 0; i < size; i++) {
            change_both_intermediate(intermediate, intermediate_upd, page[i].keyR, page[i].keyS, join.table1,
                                     join.table2, tableSize);
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            change_both_intermediate(intermediate, intermediate_upd, page[i].keyS, page[i].keyR, join.table2,
                                     join.table1, tableSize);
        }
    }
}

void getVector3(const vector<vector<uint64_t>> &intermediate, const join_info &join, size_t tableSize,
                vector<vector<uint64_t>> &intermediate_upd, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    if (intermediate[join.table2].empty()) {
        for (size_t i = 0; i < size; i++) {
            change_intermediate(intermediate, intermediate_upd, page[i].keyR, page[i].keyS, join.table1,
                                join.table2, tableSize);
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            change_intermediate(intermediate, intermediate_upd, page[i].keyS, page[i].keyR, join.table2,
                                join.table1, tableSize);
        }
    }
}

vector<vector<uint64_t> > update_intermediate(vector<vector<uint64_t> > &intermediate, Result *results, join_info &join,
                                              size_t tableSize) {
    vector<vector<uint64_t> > intermediate_upd(tableSize);

    bucket_info *temp = results->head;

    if (intermediate[join.table1].empty() && intermediate[join.table2].empty()) {
        //if intermediate results for both tables are empty
        //push back all results to their intermediate vectors
        getVector(intermediate_upd, join, temp, results->size);
        temp = temp->next;
        while (temp != nullptr) {
            getVector(intermediate_upd, join, temp, results->capacity);
            temp = temp->next;
        }
    } else if (!intermediate[join.table1].empty() && !intermediate[join.table2].empty()) {
        //if both are not empty, then we have to parse them both to find the matching pairs
        //in the intermediate and the results from the join
        getVector2(intermediate, join, tableSize, intermediate_upd, temp, results->size);
        temp = temp->next;
        while (temp != nullptr) {
            getVector2(intermediate, join, tableSize, intermediate_upd, temp, results->capacity);
            temp = temp->next;
        }
    } else {
        //else one of them is empty, so as we parse the results, we search the intermediate of the
        //table that already exists and we create the new intermediate
        getVector3(intermediate, join, tableSize, intermediate_upd, temp, results->size);
        temp = temp->next;
        while (temp != nullptr) {
            getVector3(intermediate, join, tableSize, intermediate_upd, temp, results->capacity);
            temp = temp->next;
        }
    }

    return intermediate_upd;
}
