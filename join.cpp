#include <vector>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include "structs.h"
#include "Query.h"
#include "MainScheduler.h"

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
        int ch = getchar();
        //check if end of batch or end of file
        while (ch != 'F' && ch != EOF) {
            queries.emplace_back(ch);
            ch = getchar();                     //F or first char of next query
        }
        getchar();                            //get new line
        if (!queries.empty()) {
            batches.push_back(queries);
        }
    }
    //for every batch, execute queries
    MainScheduler js;
    js.init(NUM_OF_THREADS);
    for (auto &&queries : batches) {
        for (auto &&query : queries) {
            js.schedule(new QueryJob(query, relations));
        }
    }
    js.stop();
    js.destroy();
    for (auto &&queries : batches) {
        for (auto &&query : queries) {
            query.print();
        }
    }

    for (auto &&rel : relations) {
        rel.destroy();
    }

    return 0;
}
