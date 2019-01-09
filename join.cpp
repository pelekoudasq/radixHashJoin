#include <vector>
#include <cstdlib>
#include <cstdio>
#include "structs.h"
#include "Query.h"
#include "JobScheduler.h"

using std::vector;

extern clock_t t1;
extern clock_t t2;
extern clock_t t3;
extern clock_t time1;
extern clock_t time2;
extern clock_t time3;
extern clock_t timer;
extern size_t count1;
extern size_t count2;
extern size_t count3;

int main(void) {
  JobScheduler js;
  js.init(10);
  js.schedule(new Job1());
  js.schedule(new Job1());
  js.schedule(new Job1());
  js.schedule(new Job1());
  js.barrier();
  js.stop();
  js.destroy();
}

int main1(void) {
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
    printf("%ld / %ld / %ld\n", time1, count1, t1);
    printf("%ld / %ld / %ld\n", time2, count2, t2);
    printf("%ld / %ld / %ld\n", time3, count3, t3);
    printf("%ld\n", timer);
    return 0;
}
