//
// Created by liavf on 12/05/2025.
//

#ifndef OS_EX3_BARRIER_H
#define OS_EX3_BARRIER_H

#include <mutex>
#include <condition_variable>
#include <pthread.h>

class Barrier {
public:
    explicit Barrier(int numThreads);
    ~Barrier() = default;
    void barrier();

private:
    std::mutex mutex;
    std::condition_variable cv;
    int count;
    int generation;
    const int numThreads;
};

#endif //OS_EX3_BARRIER_H
