#include <cstdio>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "structs.h"

using std::unordered_map;
using std::unordered_set;
using std::vector;


/* Get relations and data from line read from user. */
relList::relList(char *filename) {
    int fd = open(filename, O_RDONLY);
    if (read(fd, this, 2 * sizeof(uint64_t)) != 2 * sizeof(uint64_t)) return;
//    read(fd, &num_tuples, sizeof(uint64_t));
//    read(fd, &num_columns, sizeof(uint64_t));
    value = (uint64_t *) mmap(nullptr, num_tuples * num_columns * sizeof(uint64_t), PROT_READ, MAP_PRIVATE, fd, 0);
    value += 2;            //file offset
    close(fd);
}

relList::~relList() {
    munmap(value, num_tuples * num_columns * sizeof(uint64_t));
}

/* First parsing through table
 * Find hash position and increase histogram value by one
 * From histogram to summarised histogram
 * Second parsing through table, new relation table
 * Find hash position
 * Copy that tuple to the new table in the right position
 * Increase position for current hash result value
 */

void relation_info::hash_relation(relation &rel, size_t twoInLSB) {
    histogram = new size_t[twoInLSB]();
    for (size_t i = 0; i < rel.num_tuples; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        histogram[position]++;
    }

    auto *sumHistogram = new size_t[twoInLSB];
    sumHistogram[0] = 0;
    for (size_t i = 1; i < twoInLSB; i++)
        sumHistogram[i] = sumHistogram[i - 1] + histogram[i - 1];

    tups.num_tuples = rel.num_tuples;
    tups.tuples = new tuple[tups.num_tuples];
    for (size_t i = 0; i < rel.num_tuples; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        memcpy(tups.tuples + sumHistogram[position], &rel.tuples[i], sizeof(tuple));
        sumHistogram[position]++;
    }
    delete[] sumHistogram;
}

relation_info::~relation_info() {
    delete[] histogram;
}

relation::~relation() {
    delete[] tuples;
}

/* Return certain column of a relation in the form
 * of table of tuples for using RadixHashJoin
 */
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

/* Get data either from intermediate or filters and call foo for right format
 */
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
