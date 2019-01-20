#include "MainScheduler.h"

/*
 * Start routine
 */
void *mainThreadWork(void *arg) {
    auto ms = (MainScheduler*) arg;
    JobScheduler js;
    js.init(NUM_OF_THREADS);
    ms->threadWork(&js);
    js.stop();
    js.destroy();
    return nullptr;
}

bool MainScheduler::init(size_t num_of_threads) {
    return JobScheduler::init(num_of_threads, mainThreadWork);
}

QueryJob::QueryJob(Query &query, std::vector<relList> &relations) : query(query), relations(relations) {
}

int QueryJob::run() {
    query.execute(*js, relations);
    return 0;
}

void QueryJob::init(void *js) {
    this->js = (JobScheduler *) js;
}
