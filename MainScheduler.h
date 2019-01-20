#ifndef JOIN_MAINSCHEDULER_H
#define JOIN_MAINSCHEDULER_H

#include <pthread.h>
#include <queue>
#include "Query.h"

#define NUM_OF_THREADS 8

/* Job for executing queries */
class QueryJob : public Job {
    Query &query;
    std::vector<relList> &relations;
    JobScheduler *js;
public:
    QueryJob(Query &query, std::vector<relList> &relations);

    void init(void *js) override;

    int run() override;
};

/**
 * Class JobScheduler
 */
class MainScheduler : public JobScheduler {
public:
    bool init(size_t num_of_threads) override;
};

#endif //JOIN_MAINSCHEDULER_H
