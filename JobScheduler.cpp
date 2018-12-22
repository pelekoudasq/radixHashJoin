#include "JobScheduler.h"

bool JobScheduler::init(int num_of_threads) {
    this->num_of_threads = num_of_threads;
    return false;
}

bool JobScheduler::destroy() {
    return false;
}

void JobScheduler::barrier() {

}

int JobScheduler::schedule(Job *job) {
    return 0;
}

void JobScheduler::stop() {

}
