#include <cstdio>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cassert>
#include "structs.h"
#include "JobScheduler.h"

using std::unordered_map;
using std::unordered_set;
using std::vector;

/* Get relations and data from line read from user. */
relList::relList(char *filename) {
    int fd = open(filename, O_RDONLY);
    assert(fd != -1);

    struct stat s{};
    fstat(fd, &s);
    auto st_size = (size_t) s.st_size;

    auto mapped = (uint64_t *) mmap(nullptr, st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    assert(mapped != MAP_FAILED);

    num_tuples = mapped[0];
    num_columns = mapped[1];
    assert(st_size == (num_tuples * num_columns + 2) * sizeof(uint64_t));

    mapped += 2;
    values = new uint64_t *[num_columns];
    col_max = new uint64_t[num_columns];
    col_min = new uint64_t[num_columns];
    distinct = new uint64_t[num_columns];
    for (size_t i = 0; i < num_columns; ++i) {
        values[i] = mapped;
        mapped += num_tuples;
        uint64_t min = values[i][0];
        uint64_t max = values[i][0];
        for (size_t j = 1; j < num_tuples; j++) {
            if (min > values[i][j]) {
                min = values[i][j];
            } else if (max < values[i][j]) {
                max = values[i][j];
            }
        }
        col_max[i] = max;
        col_min[i] = min;

        vector<bool> dist(max - min + 1, false);
        for (size_t j = 0; j < num_tuples; j++) {
            dist[values[i][j] - min] = true;
        }
        for (size_t j = 0; j < max - min; j++) {
            if (dist[j] == true) {
                distinct[i]++;
            }
        }
    }
    close(fd);
}

void relList::destroy() {
    int rc = munmap(values[0] - 2, (num_tuples * num_columns + 2) * sizeof(uint64_t));
    assert(rc == 0);
    delete[] values;
    delete[] distinct;
    delete[] col_min;
    delete[] col_max;
}

size_t *singleHistogram(relation &rel, size_t twoInLSB) {
    auto histogram = new size_t[twoInLSB]();
    for (size_t i = 0; i < rel.num_tuples; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        histogram[position]++;
    }
    return histogram;
}

tuple *singlePartition(relation &rel, size_t twoInLSB, const size_t *histogram) {
    auto sumHistogram = new size_t[twoInLSB];
    sumHistogram[0] = 0;
    for (size_t i = 1; i < twoInLSB; i++)
        sumHistogram[i] = sumHistogram[i - 1] + histogram[i - 1];

    auto tups = new tuple[rel.num_tuples];
    for (size_t i = 0; i < rel.num_tuples; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        memcpy(tups + sumHistogram[position], &rel.tuples[i], sizeof(tuple));
        sumHistogram[position]++;
    }
    delete[] sumHistogram;
    return tups;
}

size_t **multiHistogram(relation &rel, size_t twoInLSB, size_t *start, size_t *end) {
    JobScheduler js;
    js.init(NUM_OF_THREADS);
    auto hists = new size_t *[NUM_OF_THREADS];

    for (size_t i = 0; i < NUM_OF_THREADS; ++i) {
        hists[i] = new size_t[twoInLSB]();
        Job *job = new HistogramJob(hists[i], rel, twoInLSB, start[i], end[i]);
        js.schedule(job);
    }
    js.barrier();
    js.stop();
    js.destroy();

    return hists;
}

size_t **multiPartition(relation &rel, size_t twoInLSB, size_t **sumHists, size_t **hists, size_t *start, size_t *end) {
    JobScheduler js;
    js.init(NUM_OF_THREADS);
    auto tups = new size_t *[NUM_OF_THREADS];

    for (size_t i = 0; i < NUM_OF_THREADS; ++i) {
        sumHists[i] = new size_t[twoInLSB]();
        tups[i] = new size_t[end[i] - start[i]];
        Job *job = new PartitionJob(tups[i], rel, twoInLSB, start[i], end[i], sumHists[i], hists[i]);
        js.schedule(job);
    }
    js.barrier();
    js.stop();
    js.destroy();

    return tups;
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
    // partition R table to NUM_OF_THREADS tables (r)
    size_t start[NUM_OF_THREADS];
    size_t end[NUM_OF_THREADS];

    uint64_t div = rel.num_tuples / NUM_OF_THREADS;
    uint64_t mod = rel.num_tuples % NUM_OF_THREADS;

    start[0] = 0;
    end[0] = div;
    for (size_t i = 1; i < NUM_OF_THREADS; ++i) {
        start[i] = end[i - 1];
        end[i] = start[i] + div;
        if (mod > 0) {
            end[i]++;
            mod--;
        }
    }

    // histograms of r tables
//    clock_t x = clock();
    auto hists = multiHistogram(rel, twoInLSB, start, end);

    // total histogram for RadixHashJoin
    histogram = new size_t[twoInLSB]();
    for (size_t i = 0; i < NUM_OF_THREADS; ++i) {
        for (size_t j = 0; j < twoInLSB; ++j) {
            histogram[j] += hists[i][j];
        }
    }
//    printf("%fs\n", (clock() - x) / (double) CLOCKS_PER_SEC);

    // sumHistograms (p)
    auto sumHists = new size_t *[NUM_OF_THREADS];

    // r' tables of R indexes
    auto tups = multiPartition(rel, twoInLSB, sumHists, hists, start, end);

    // R' table
    tuples.num_tuples = rel.num_tuples;
    tuples.tuples = new tuple[tuples.num_tuples];
    size_t position = 0;
    for (size_t j = 0; j < twoInLSB; j++) {
        for (size_t i = 0; i < NUM_OF_THREADS; ++i) {
            for (size_t element = sumHists[i][j]; element < sumHists[i][j] + hists[i][j]; element++) {
                auto x = tups[i][element];
                memcpy(&tuples.tuples[position], &rel.tuples[x], sizeof(tuple));
                position++;
            }
        }
    }

    for (size_t i = 0; i < NUM_OF_THREADS; ++i) {
        delete[] hists[i];
        delete[] sumHists[i];
        delete[] tups[i];
    }
    delete[] hists;
    delete[] sumHists;
    delete[] tups;
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
void relation::foo(const relList &rel, size_t column_number, const unordered_set<uint64_t> &uniqueValues) {
    num_tuples = uniqueValues.size();
    tuples = new tuple[num_tuples];
    size_t i = 0;
    for (auto &&rowid : uniqueValues) {
        tuples[i].key = rowid;
        tuples[i].payload = rel.values[column_number][rowid];
        i++;
    }
}

/* Get data either from intermediate or filters and call foo for right format
 */
void relation::create_relation(uint64_t join_table, relList &rel, uint64_t column_number,
                               unordered_map<uint64_t, unordered_set<uint64_t> > &filtered,
                               vector<uint64_t> &inter) {
    if (inter.empty()) {
        unordered_map<uint64_t, unordered_set<uint64_t> >::const_iterator table = filtered.find(join_table);
        const unordered_set<uint64_t> &uniqueValues = table->second;
        foo(rel, column_number, uniqueValues);
    } else {
        unordered_set<uint64_t> uniqueValues;
        for (auto &&val : inter)
            uniqueValues.insert(val);
        foo(rel, column_number, uniqueValues);
    }
}
