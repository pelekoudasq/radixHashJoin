#include "JobScheduler.h"
#include "Result.h"
#include <iostream>

using std::cout;
using std::cerr;
using std::endl;

/* Start routine */
void *threadWork(void *arg) {
    auto js = (JobScheduler *) arg;
    js->threadWork();
    return nullptr;
}

/*
 * Getting job from queue and running it until:
 * 1) Queue is empty
 * 2) Barrier is called (bar)
 * 3) Work is done
 */
void JobScheduler::threadWork() {
    while (true) {
        if (pthread_mutex_lock(&queueLock) != 0) {
            cerr << "lock" << endl;
            exit(EXIT_FAILURE);
        }

        while (q.empty() && !done && !bar) {
            pthread_cond_wait(&cond_nonempty, &queueLock);
        }

        Job *job;
        if (q.empty()) {
            job = nullptr;
        } else {
            job = q.front();
            q.pop();
        }
        if (q.empty()) {
            // if queue has emptied, signal barrier
            pthread_cond_signal(&cond_empty);
        }

        if (pthread_mutex_unlock(&queueLock) != 0) {
            cerr << "unlock" << endl;
            exit(EXIT_FAILURE);
        }

        if (job != nullptr) {
            job->run();
            delete job;
        } else {
            if (done)
                return;
        }
        if (bar) {
            // wait until all have reached this point and bar is false again
            pthread_barrier_wait(&pbar);
            pthread_barrier_wait(&pbar2);
        }
    }
}

/* Initialize JobScheduler info */
bool JobScheduler::init(size_t num_of_threads) {
    bar = false;
    done = false;

    pthread_mutex_init(&queueLock, nullptr);
    pthread_cond_init(&cond_nonempty, nullptr);
    pthread_cond_init(&cond_empty, nullptr);
    pthread_barrier_init(&pbar, nullptr, (unsigned) num_of_threads + 1);
    pthread_barrier_init(&pbar2, nullptr, (unsigned) num_of_threads + 1);

    this->num_of_threads = num_of_threads;
    threads = new pthread_t[num_of_threads];
    for (size_t i = 0; i < num_of_threads; i++) {
        pthread_create(threads + i, nullptr, ::threadWork, this);
    }
    return true;
}

/* Destroy JobScheduler info */
bool JobScheduler::destroy() {
    pthread_mutex_destroy(&queueLock);
    pthread_cond_destroy(&cond_nonempty);
    pthread_cond_destroy(&cond_empty);
    pthread_barrier_destroy(&pbar);
    pthread_barrier_destroy(&pbar2);

    delete[] threads;
    return true;
}

/* If barrier is called, wait until queue is empty and enable barrier.
 * Wake threads to finish any remaining jobs.
 * Wait until all jobs are done, then disable barrier.
 */
void JobScheduler::barrier() {
    if (pthread_mutex_lock(&queueLock) != 0) {
        cerr << "lock" << endl;
        exit(EXIT_FAILURE);
    }

    while (!q.empty()) {
        pthread_cond_wait(&cond_empty, &queueLock);
    }
    bar = true;
    if (pthread_mutex_unlock(&queueLock) != 0) {
        cerr << "unlock" << endl;
        exit(EXIT_FAILURE);
    }

    pthread_cond_broadcast(&cond_nonempty);
    pthread_barrier_wait(&pbar);
    bar = false;
    pthread_barrier_wait(&pbar2);     // so threadWork doesnt get stuck in wait
}

/* Appoint job in JobScheduler's queue*/
int JobScheduler::schedule(Job *job) {
    if (pthread_mutex_lock(&queueLock) != 0) {
        cerr << "lock" << endl;
        exit(EXIT_FAILURE);
    }
    q.push(job);
    pthread_cond_signal(&cond_nonempty);
    if (pthread_mutex_unlock(&queueLock) != 0) {
        cerr << "unlock" << endl;
        exit(EXIT_FAILURE);
    }
    return 0;
}

/* Enable done and wait until all threads finish their jobs */
void JobScheduler::stop() {
    done = true;
    pthread_cond_broadcast(&cond_nonempty);
    for (size_t i = 0; i < num_of_threads; i++) {
        pthread_join(threads[i], nullptr);
    }
}

/* Job to calculate small histograms*/
int HistogramJob::run() {
    for (size_t i = start; i < end; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        histogram[position]++;
    }
    return 0;
}

/* Initialize values */
HistogramJob::HistogramJob(size_t *histogram, relation &rel, size_t twoInLSB, size_t start, size_t end)
        : histogram(histogram), rel(rel), twoInLSB(twoInLSB), start(start), end(end) {}

/* Job to calculate small sumHistograms and r' by indexes*/
int PartitionJob::run() {
    sumHistogram[0] = 0;
    auto sumHistogramB = new size_t[twoInLSB];
    sumHistogramB[0] = 0;
    for (size_t i = 1; i < twoInLSB; i++) {
        sumHistogram[i] = sumHistogram[i - 1] + histogram[i - 1];
        sumHistogramB[i] = sumHistogram[i];
    }
    for (size_t i = start; i < end; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        tuples[sumHistogramB[position]] = i;
        sumHistogramB[position]++;
    }
    delete[] sumHistogramB;
    return 0;
}

/* Initialize values */
PartitionJob::PartitionJob(size_t *tuples, relation &rel, size_t twoInLSB, size_t start, size_t end,
                           size_t *sumHistogram, size_t *histogram)
        : tuples(tuples), rel(rel), twoInLSB(twoInLSB), start(start), end(end), sumHistogram(sumHistogram),
          histogram(histogram) {}

/* Job to join relation by buckets */
int JoinJob::run() {
    if (histR >= histS)
        result.join_buckets(relShashed, relRhashed, begS, begR, histS, histR, true);
    else
        result.join_buckets(relRhashed, relShashed, begR, begS, histR, histS, false);
    return 0;
}

/* Initialize values */
JoinJob::JoinJob(Result &result, relation_info *relShashed, relation_info *relRhashed, size_t begS, size_t begR,
                 size_t histS, size_t histR) : result(result), relShashed(relShashed), relRhashed(relRhashed),
                                               begS(begS), begR(begR), histS(histS), histR(histR) {}
