#include <vector>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include "structs.h"
#include "Query.h"

using std::vector;


/* Job for executing queries */
class QueryJob : public Job {
    Query &query;
    vector<relList> &relations;

    JobScheduler js;
public:
    QueryJob(Query &query, vector<relList> &relations) : query(query), relations(relations) {
        js.init(NUM_OF_THREADS);
    }

    ~QueryJob() override {
        js.stop();
        js.destroy();
    }

    int run() override {
        query.execute(js, relations);
        return 0;
    }
};

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
            ch = getchar();
        }
        getchar();                            //get new line
        if (!queries.empty()) {
            batches.push_back(queries);
        }
    }
    //for every batch, execute queries
    JobScheduler js;
    js.init(NUM_OF_THREADS);
    for (auto &&queries : batches) {
        for (auto &&query : queries) {
            js.schedule(new QueryJob(query, relations));
        }
    }
    js.barrier();
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
