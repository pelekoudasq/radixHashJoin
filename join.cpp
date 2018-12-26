#include <vector>
#include <cstdlib>
#include <cstdio>
#include "structs.h"
#include "Query.h"

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
        relations.emplace_back(lineptr);
    }
    if (lineptr != nullptr)
        free(lineptr);

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

    return 0;
}
