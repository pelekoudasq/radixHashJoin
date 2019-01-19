#include "JobScheduler.h"
#include <iostream>

using std::cout;
using std::cerr;
using std::endl;

void *threadWork(void *arg) {
    auto js = (JobScheduler *) arg;
    js->threadWork();
    return nullptr;
}

void JobScheduler::threadWork() {
    while (true) {
        if (pthread_mutex_lock(&queueLock) != 0) {
            cerr << "lock" << endl;
            exit(EXIT_FAILURE);
        }

        while (q.empty() && !done) {
            pthread_cond_wait(&cond_nonempty, &queueLock);
        }

        if (q.empty()) {
            if (pthread_mutex_unlock(&queueLock) != 0) {
                cerr << "unlock" << endl;
                exit(EXIT_FAILURE);
            }
            return;
        }

        Job *job = q.front();
        q.pop();

        if (pthread_mutex_unlock(&queueLock) != 0) {
            cerr << "unlock" << endl;
            exit(EXIT_FAILURE);
        }

        job->run();
        delete job;
    }
}

bool JobScheduler::init(size_t num_of_threads) {
    done = false;
    queueLock = PTHREAD_MUTEX_INITIALIZER;
    cond_nonempty = PTHREAD_COND_INITIALIZER;
    this->num_of_threads = num_of_threads;
    threads = new pthread_t[num_of_threads];
    for (size_t i = 0; i < num_of_threads; i++) {
        pthread_create(threads + i, nullptr, ::threadWork, this);
    }
    return true;
}

bool JobScheduler::destroy() {
    delete[] threads;
    return true;
}

void JobScheduler::barrier() {

}

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

void JobScheduler::stop() {
    barrier();
    done = true;
    pthread_cond_broadcast(&cond_nonempty);
    for (size_t i = 0; i < num_of_threads; i++) {
        pthread_join(threads[i], nullptr);
    }
}

int HistogramJob::run() {
    for (size_t i = start; i < end; i++) {
        size_t position = rel.tuples[i].payload & (twoInLSB - 1);
        histogram[position]++;
    }
    return 0;
}

HistogramJob::HistogramJob(size_t *histogram, relation &rel, size_t twoInLSB, size_t start, size_t end)
        : histogram(histogram), rel(rel), twoInLSB(twoInLSB), start(start), end(end) {}

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

PartitionJob::PartitionJob(size_t *tuples, relation &rel, size_t twoInLSB, size_t start, size_t end,
                           size_t *sumHistogram, size_t *histogram)
        : tuples(tuples), rel(rel), twoInLSB(twoInLSB), start(start), end(end), sumHistogram(sumHistogram),
          histogram(histogram) {}

int JoinJob::run() {
    if (histR >= histS)
        result.join_buckets(relShashed, relRhashed, begS, begR, histS, histR, true);
    else
        result.join_buckets(relRhashed, relShashed, begR, begS, histR, histS, false);
    return 0;
}

JoinJob::JoinJob(Result &result, relation_info *relShashed, relation_info *relRhashed, size_t begS, size_t begR,
                 size_t histS, size_t histR) : result(result), relShashed(relShashed), relRhashed(relRhashed),
                                               begS(begS), begR(begR), histS(histS), histR(histR) {}
