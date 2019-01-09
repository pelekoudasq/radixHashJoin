#include "JobScheduler.h"
#include <iostream>

using namespace std;

int Job1::run(void) {
  cout << "hello" << endl;
  return 0;
}

void *threadWork(void *arg) {
  JobScheduler* js = (JobScheduler*) arg;
  js->threadWork();
  return nullptr;
}

void JobScheduler::threadWork() {
  if (pthread_mutex_lock(&queueLock) < 0) {
			cerr << "lock" << endl;
			exit(EXIT_FAILURE);
		}

		while (q.empty() || !done){
			pthread_cond_wait(&cond_nonempty, &queueLock);
		}

    if (q.empty()) {
  		if (pthread_mutex_unlock(&queueLock) < 0){
  			cerr << "unlock" << endl;
  			exit(EXIT_FAILURE);
  		}
      return;
    }

		Job* job = q.back();
    q.pop_back();
		if (pthread_mutex_unlock(&queueLock) < 0){
			cerr << "unlock" << endl;
			exit(EXIT_FAILURE);
		}

    job->run();
}

bool JobScheduler::init(size_t num_of_threads) {
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
    if (pthread_mutex_lock(&queueLock) < 0){
			cerr << "lock" << endl;
			exit(EXIT_FAILURE);
		}
    q.push_back(job);
    pthread_cond_signal(&cond_nonempty);
		if (pthread_mutex_unlock(&queueLock) < 0){
			cerr << "unlock" << endl;
			exit(EXIT_FAILURE);
		}
    return 0;
}

void JobScheduler::stop() {
    done = true;
    for (size_t i = 0; i < num_of_threads; i++) {
        pthread_join(threads[i], nullptr);
    }
    done = false;
}
