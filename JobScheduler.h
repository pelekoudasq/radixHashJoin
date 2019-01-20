#ifndef JOIN_JOBSCHEDULER_H
#define JOIN_JOBSCHEDULER_H

#include <pthread.h>
#include <queue>

struct relation;
struct relation_info;
struct Result;

#define NUM_OF_THREADS 8

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

class HistogramJob : public Job {
    size_t *histogram;
    relation &rel;
    size_t twoInLSB;
    size_t start;
    size_t end;

    int run() override;

public:
    HistogramJob(size_t *histogram, relation &rel, size_t twoInLSB, size_t start, size_t end);
};

class PartitionJob : public Job {
    size_t *tuples;
    relation &rel;
    size_t twoInLSB;
    size_t start;
    size_t end;
    size_t *sumHistogram;
    size_t *histogram;

    int run() override;

public:
    PartitionJob(size_t *tuples, relation &rel, size_t twoInLSB, size_t start, size_t end, size_t *sumHistogram,
                 size_t *histogram);
};

class JoinJob : public Job {
    Result &result;
    relation_info *relShashed;
    relation_info *relRhashed;
    size_t begS;
    size_t begR;
    size_t histS;
    size_t histR;

    int run() override;

public:
    JoinJob(Result &result, relation_info *relShashed, relation_info *relRhashed, size_t begS, size_t begR,
            size_t histS, size_t histR);
};

/**
 * Class JobScheduler
 */
class JobScheduler {
    bool bar;
    bool done;
    size_t num_of_threads;
    pthread_t *threads;
    pthread_mutex_t queueLock;
    pthread_cond_t cond_nonempty;
    pthread_cond_t cond_empty;
    pthread_barrier_t pbar;
    pthread_barrier_t pbar2;
    std::queue<Job *> q;

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
 * Waits until executed all jobs in the queue.
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
