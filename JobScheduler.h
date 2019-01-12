#ifndef JOIN_JOBSCHEDULER_H
#define JOIN_JOBSCHEDULER_H

#include <pthread.h>
#include <queue>

/**
 * Abstract Class Job
 */
class Job {
public:
    Job() = default;

    virtual ~Job() = default;

/**
 * This method should be implemented by subclasses.
 */
    virtual int run() = 0;
};

class Job1 : public Job {
public:
   int run() override;
};

/**
 * Class JobScheduler
 */
class JobScheduler {
    bool done;
    size_t num_of_threads;
    pthread_t* threads;
    pthread_mutex_t queueLock;
    pthread_cond_t cond_nonempty;
    std::queue<Job*> q;
public:
    void threadWork();

    JobScheduler() = default;

    ~JobScheduler() = default;

/**
 * Initializes the JobScheduler with the number of open threads.
 * Returns true if everything done right false else.
 */
    bool init(size_t num_of_threads);

/**
 * Free all resources that the are allocated by JobScheduler
 * Returns true if everything done right false else.
 */
    bool destroy();

/**
 * Waits Until executed all jobs in the queue.
 */
    void barrier();

/**
 * Add a job in the queue and returns a JobId
 */
    int schedule(Job *job);

/**
 * Waits until all threads finish their job, and after that close all threads.
 */
    void stop();
};

#endif //JOIN_JOBSCHEDULER_H
