#ifndef JOIN_JOBSCHEDULER_H
#define JOIN_JOBSCHEDULER_H

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
    virtual int Run() = 0;
};

/**
 * Class JobScheduler
 */
class JobScheduler {
    int num_of_threads;
public:
    JobScheduler() = default;

    ~JobScheduler() = default;

/**
 * Initializes the JobScheduler with the number of open threads.
 * Returns true if everything done right false else.
 */
    bool init(int num_of_threads);

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
