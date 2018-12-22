#include <vector>
#include <cstdlib>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include "structs.h"
#include "Query.h"
#include "test.h"

using std::vector;

int main() {
    char *lineptr = nullptr;
    size_t n = 0;
    ssize_t lineSize;

    //get every filepath
    vector<relList> relations;
    while ((lineSize = getline(&lineptr, &n, stdin)) != -1 && strcmp(lineptr, "Done\n") != 0) {
        lineptr[lineSize - 1] = '\0';
        //open file and get contents
        relList relation;
        int fileDesc = open(lineptr, O_RDONLY);
        if (read(fileDesc, &relation.num_tuples, sizeof(uint64_t)) < 0) {
            return -1;
        }
        if (read(fileDesc, &relation.num_columns, sizeof(uint64_t)) < 0) {
            return -1;
        }
        relation.value = (uint64_t *) mmap(nullptr, relation.num_tuples * relation.num_columns * sizeof(uint64_t),
                                           PROT_READ, MAP_PRIVATE, fileDesc, 0);
        relation.value += 2;            //file offset
        close(fileDesc);
        relations.push_back(relation);
    }
    if (lineptr != nullptr)
        free(lineptr);

    //print_relations(relations);

    //parse batch
    vector<vector<Query>> batches;
    while (!feof(stdin)) {
        vector<Query> queries;
        while (true) {
            Query query;
            if (query.read_relations()) {
                getchar();                            //get new line
                break;
            }
            query.read_predicates();
            query.read_projections();
            queries.push_back(query);
        }
        if (!queries.empty()) {
            batches.push_back(queries);
        }
    }
    //for every batch, execute queries
    for (auto &&queries : batches) {
        for (auto &&query : queries) {
            query.execute(relations);
        }
        for (auto &&query : queries) {
            query.print();
        }
    }

    for (auto &&relation : relations) {
        munmap(relation.value, relation.num_tuples * relation.num_columns * sizeof(uint64_t));
    }

    return 0;
}
