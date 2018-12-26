#include <iostream>
#include "test.h"

using std::vector;
using std::unordered_map;
using std::unordered_set;

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
                 unordered_map<uint64_t, unordered_set<uint64_t> > &filtered, vector<vector<uint64_t> > &intermediate) {

    uint64_t column1_number = join.column1;
    uint64_t column2_number = join.column2;
    size_t offset = relations[table_number].num_tuples;

    if (intermediate[join.table1].empty()) {
        printf("DO I NEVER DO THIS?\n");
        unordered_map<uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join.table1);
        for (auto &&rowid : table->second) {
            uint64_t value1 = relations[table_number].value[column1_number * offset + rowid];
            uint64_t value2 = relations[table_number].value[column2_number * offset + rowid];
            if (value1 == value2) {
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

void change_intermediate(const vector<vector<uint64_t> > &intermediate, vector<vector<uint64_t> > &intermediate_upd,
                         uint64_t rowid1, uint64_t rowid2, size_t table_existing, size_t table_n_existing) {

    vector<uint64_t> &table_n = intermediate_upd[table_n_existing];
    for (size_t element = 0; element < intermediate[table_existing].size(); element++) {
        if (intermediate[table_existing][element] == rowid1) {
            for (size_t i = 0; i < intermediate.size(); ++i) {
                if (!intermediate[i].empty())
                    intermediate_upd[i].push_back(intermediate[i][element]);
            }
            table_n.push_back(rowid2);
        }
    }
}

void change_both_intermediate(const vector<vector<uint64_t> > &intermediate, const vector<uint64_t> &table1,
                              const vector<uint64_t> &table2,
                              vector<vector<uint64_t> > &intermediate_upd, uint64_t rowid1, uint64_t rowid2) {

    for (size_t element = 0; element < table1.size(); element++) {
        if (table1[element] == rowid1 && table2[element] == rowid2) {
            for (size_t i = 0; i < intermediate.size(); ++i) {
                if (!intermediate[i].empty()) {
                    intermediate_upd[i].push_back(intermediate[i][element]);
                }
            }
        }
    }
}

void getVector(vector<vector<uint64_t>> &intermediate, const join_info &join, const bucket_info *temp, size_t size) {
    auto *page = (key_tuple *) &temp[1];
    vector<uint64_t> &table1 = intermediate[join.table1];
    vector<uint64_t> &table2 = intermediate[join.table2];
    table1.reserve(size);
    table2.reserve(size);
    for (key_tuple *kt = page; kt < page + size; kt++) {
        table1.push_back(kt->keyR);
        table2.push_back(kt->keyS);
    }
}

void getVector2(const vector<vector<uint64_t>> &intermediate, const join_info &join,
                vector<vector<uint64_t>> &intermediate_upd, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    if (intermediate[join.table2].empty()) {
        for (key_tuple *kt = page; kt < page + size; kt++) {
            change_intermediate(intermediate, intermediate_upd, kt->keyR, kt->keyS, join.table1, join.table2);
        }
    } else {
        for (key_tuple *kt = page; kt < page + size; kt++) {
            change_intermediate(intermediate, intermediate_upd, kt->keyS, kt->keyR, join.table2, join.table1);
        }
    }
}

void getVector3(const vector<vector<uint64_t>> &intermediate, const join_info &join,
                vector<vector<uint64_t>> &intermediate_upd, const bucket_info *temp, size_t size) {
    auto page = (key_tuple *) &temp[1];
    if (intermediate[join.table2].empty()) {
        for (key_tuple *kt = page; kt < page + size; kt++) {
            change_both_intermediate(intermediate, intermediate[join.table1], intermediate[join.table2],
                                     intermediate_upd, kt->keyR, kt->keyS);
        }
    } else {
        for (key_tuple *kt = page; kt < page + size; kt++) {
            change_both_intermediate(intermediate, intermediate[join.table2], intermediate[join.table1],
                                     intermediate_upd, kt->keyS, kt->keyR);
        }
    }
}

void update_intermediate(vector<vector<uint64_t> > &intermediate, const Result &results, join_info &join) {
    vector<vector<uint64_t> > intermediate_upd(intermediate.size());

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
