#include <vector>
#include <cstdlib>
#include <cstdio>
#include "structs.h"
#include "Query.h"
#include "JobScheduler.h"

using std::vector;

class Job2 : public Job {
    Query &query;
    vector<relList> &relations;
public:
    Job2(Query &query, vector<relList> &relations) : query(query), relations(relations) {}

    int run() override {
        query.execute(relations);
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
        relations.emplace_back();
        relations.back().init(lineptr);
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
    JobScheduler js;
    for (auto &&queries : batches) {
        js.init(1);
        for (auto &&query : queries) {
            js.schedule(new Job2(query, relations));
//            query.execute(relations);
        }
        js.barrier();
        js.stop();
        js.destroy();
        for (auto &&query : queries) {
            query.print();
        }
    }

    for (auto &&rel : relations) {
        rel.destroy();
    }

    return 0;
}
