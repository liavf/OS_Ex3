#include "MapReduceFramework.h"
#include "pthread.h"
#include <iostream>
#include <algorithm>
#include <atomic>
#include <numeric>
#include "Barrier.h"


struct JobContext {
    JobContext(const MapReduceClient &client,
               const InputVec &inputVec, OutputVec &outputVec,
               int multiThreadLevel);

    ~JobContext();

    int numOfThreads;
    pthread_t *threads;
    stage_t stage = UNDEFINED_STAGE;
    int denominator;
    const MapReduceClient *client;
    const InputVec *inputVec;
    std::vector<IntermediateVec> intermediateVecs;
    std::vector<IntermediateVec> shuffledVecs;
    OutputVec *outputVec;
    std::atomic<int> atomic_counter_map;
    std::atomic<int> atomic_progress_counter;
    Barrier barrier;
    bool isWaitingForJob = false;
    pthread_mutex_t reduce_mutex;
    pthread_mutex_t output_mutex;
    pthread_mutex_t stage_mutex;
    pthread_mutex_t error_mutex;
};

void switchToShuffle(JobContext *jobContext);

void switchToReduce(JobContext *jobContext);

const K2 *getMinKey(JobContext *jobContext);

IntermediateVec getNewKeyVector(JobContext *jobContext, const K2 *minKey);

void threadMap(JobContext *context);

void threadReduce(JobContext *context);

void safeExitOnError(std::string string);

JobContext::JobContext(const MapReduceClient &client,
                       const InputVec &inputVec, OutputVec &outputVec,
                       int multiThreadLevel) : barrier(multiThreadLevel) {

    numOfThreads = multiThreadLevel;
    this->client = &client;
    this->inputVec = &inputVec;
    this->outputVec = &outputVec;
    denominator = inputVec.size();
    atomic_counter_map = 0;
    int res1 = pthread_mutex_init(&output_mutex, nullptr);
    int res2 = pthread_mutex_init(&reduce_mutex, nullptr);
    int res3 = pthread_mutex_init(&stage_mutex, nullptr);
    int res4 = pthread_mutex_init(&error_mutex, nullptr);
    if (res1 || res2 || res3 || res4) {
        std::cout << "system error: initializing mutex failed\n" << std::endl;
        exit(1);
    }
    pthread_t *threads = new pthread_t[multiThreadLevel];
    this->threads = threads;
};

JobContext::~JobContext() {
    delete[] threads;
}

bool compareByKey(const IntermediatePair &a, const IntermediatePair &b) {
    return *(a.first) < *(b.first);
}

void threadMap(JobContext *context) {
    IntermediateVec newIntermediateVec;
    while (true) {
        long unsigned int nextInputIndex = context->atomic_counter_map++;
        if (nextInputIndex >= context->inputVec->size()) break;

        context->client->map(context->inputVec->at
                (nextInputIndex).first, context->inputVec->at
                (nextInputIndex).second, &newIntermediateVec);
        context->atomic_progress_counter++;
    }
    std::sort(newIntermediateVec.begin(), newIntermediateVec.end(), compareByKey);
    int res = pthread_mutex_lock(&context->output_mutex);
    context->intermediateVecs.push_back(newIntermediateVec);
    if (res) safeExitOnError("system error: locking mutex failed\n");
    res = pthread_mutex_unlock(&context->output_mutex);
    if (res) safeExitOnError("system error: unlocking mutex failed\n");
}

void shuffle(JobContext *jobContext) {
    switchToShuffle(jobContext);
    while (true) {
        // Find minimal key
        const K2 *minKey = getMinKey(jobContext);
        // If minKey is nullptr, all vectors are empty, so we break
        if (minKey == nullptr) break;

        IntermediateVec newKeyVector = getNewKeyVector(jobContext, minKey);
        // Add the new vector to shuffledVecs
        jobContext->shuffledVecs.push_back(newKeyVector);
    }

    switchToReduce(jobContext);

}

const K2 *getMinKey(JobContext *jobContext) {
    const K2 *minKey = nullptr;
    for (const auto &vec: jobContext->intermediateVecs) {
        if (!vec.empty() && (!minKey ||  *minKey < *(vec.back().first))) {
            minKey = vec.back().first;
        }
    }
    return minKey;
}

IntermediateVec getNewKeyVector(JobContext *jobContext, const K2 *minKey) {
    IntermediateVec newKeyVector;

    // Collect all elements with the minimal key and remove them from original vectors
    for (auto &vec: jobContext->intermediateVecs) {
      while (!vec.empty() && (!(*(vec.back().first) < *minKey || *minKey < *(vec.back().first)))) {
                newKeyVector.push_back(vec.back());
                jobContext->atomic_progress_counter++;
                vec.pop_back();
      }
    }
    return newKeyVector;
}

void switchToShuffle(JobContext *jobContext) {
    int res = pthread_mutex_lock(&jobContext->stage_mutex);
    jobContext->stage = SHUFFLE_STAGE;
    if (res) safeExitOnError("system error: locking mutex failed\n");
    auto &vec = jobContext->intermediateVecs;
    jobContext->denominator = std::accumulate(vec.begin(), vec.end(), 0,
                                              [](int total, const std::vector<std::pair<K2 *, V2 *>> &inner_vec) {
                                                  return total + inner_vec.size();
                                              });
    jobContext->atomic_progress_counter = 0;
    res = pthread_mutex_unlock(&jobContext->stage_mutex);
    if (res) safeExitOnError("system error: unlocking mutex failed\n");
}

void switchToReduce(JobContext *jobContext) {
    int res = pthread_mutex_lock(&jobContext->stage_mutex);
    jobContext->stage = REDUCE_STAGE;
    if (res) safeExitOnError("system error: locking mutex failed\n");
    jobContext->atomic_progress_counter = 0;
    res = pthread_mutex_unlock(&jobContext->stage_mutex);
    if (res) safeExitOnError("system error: unlocking mutex failed\n");
}

void threadReduce(JobContext *context) {
    while (true) {
        int res = pthread_mutex_lock(&context->reduce_mutex);
        if (res) safeExitOnError("system error: locking mutex failed\n");
        if (context->shuffledVecs.empty()) {
            res = pthread_mutex_unlock(&context->reduce_mutex);
            if (res) safeExitOnError("system error: unlocking mutex failed\n");
            break;
        }
        auto vector = context->shuffledVecs.back();
        context->shuffledVecs.pop_back();
        res = pthread_mutex_unlock(&context->reduce_mutex);
        if (res) safeExitOnError("system error: unlocking mutex failed\n");
        int vecSize = vector.size();
        context->client->reduce(&vector, context);
        context->atomic_progress_counter += vecSize;
    }
}

void *threadEntryPoint(void *arg) {
    JobContext *context = static_cast<JobContext *>(arg);
    threadMap(context);
//  debugMap ();

    context->barrier.barrier();
    context->barrier.barrier();

    threadReduce(context);

    return nullptr;
}

void *mainThreadEntryPoint(void *arg) {
    JobContext *context = static_cast<JobContext *>(arg);
    threadMap(context);
//  debugMap ();

    context->barrier.barrier();
    shuffle(context);
    context->barrier.barrier();

    threadReduce(context);

    return nullptr;
}

void emit2(K2 *key, V2 *value, void *context) {
    IntermediateVec *intervec = static_cast<IntermediateVec *>(context);
    IntermediatePair newPair = {key, value};
    intervec->push_back(newPair);
}

void emit3(K3 *key, V3 *value, void *context) {

    JobContext *jobContext = static_cast<JobContext *>(context);
    OutputPair pair = {key, value};
    int res = pthread_mutex_lock(&jobContext->output_mutex);
    jobContext->outputVec->push_back(pair);
    if (res) safeExitOnError("system error: locking mutex failed\n");
    res = pthread_mutex_unlock(&jobContext->output_mutex);
    if (res) safeExitOnError("system error: unlocking mutex failed\n");
}

JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {
    JobContext *context = new JobContext(client, inputVec, outputVec,
                                         multiThreadLevel);
    context->stage = MAP_STAGE;
    context->atomic_progress_counter = 0;
    context->atomic_counter_map = 0;
    int res = pthread_create(&context->threads[0], nullptr,
                             &mainThreadEntryPoint,
                             context);
    for (int i = 1; i < multiThreadLevel; i++) {
        if (res) safeExitOnError("system error: creating thread failed\n");
        res = pthread_create(&context->threads[i], nullptr, &threadEntryPoint,
                             context);
        if (res) {
            std::cout << "system error: creating thread failed\n";
            exit(1);
        }
    }
    return context;
}

void waitForJob(JobHandle job) {
    JobContext *context = static_cast<JobContext *>(job);
    if (context->isWaitingForJob) return;
    context->isWaitingForJob = true;
    for (int i = 0; i < context->numOfThreads; i++) {
        int res = pthread_join(context->threads[i], nullptr);
        if (res) safeExitOnError("system error: joining thread failed\n");
    }
}

void getJobState(JobHandle job, JobState *state) {
    JobContext *jobContext = static_cast<JobContext *>(job);
    int res = pthread_mutex_lock(&jobContext->stage_mutex);
    state->stage = jobContext->stage;
    if (res) safeExitOnError("system error: locking mutex failed\n");
    float fraction =
            (float) (jobContext->atomic_progress_counter) / jobContext->denominator;
    state->percentage = fraction * 100;
    res = pthread_mutex_unlock(&jobContext->stage_mutex);
    if (res) safeExitOnError("system error: locking mutex failed\n");
}

void closeJobHandle(JobHandle job) {
    JobContext *jobContext = static_cast<JobContext *>(job);
    //waiting if we're not done:
    waitForJob(job);
    int res1 = pthread_mutex_destroy(&jobContext->stage_mutex);
    int res2 = pthread_mutex_destroy(&jobContext->reduce_mutex);
    int res3 = pthread_mutex_destroy(&jobContext->output_mutex);
    int res4 = pthread_mutex_destroy(&jobContext->error_mutex);
    if (res1 || res2 || res3 || res4) {
        std::cout << "system error: destroying mutex failed\n" << std::endl;
        exit(1);
    }
    delete jobContext;
}

void safeExitOnError(std::string string) {
//    pthread_mutex_lock(&context->error_mutex);
//    pthread_mutex_lock(&context->stage_mutex);
//
    std::cout << string << std::endl;
//
//    //cancel other threads:
//    for(int i = 0; i < context->numOfThreads; i++){
//        if (!pthread_equal(context->threads[i], pthread_self())){
//            pthread_cancel(context->threads[i]);
//        }
//    }
//    //join other threads:
//    for(int i = 0; i < context->numOfThreads; i++){
//        if (!pthread_equal(context->threads[i], pthread_self())){
//            pthread_join(context->threads[i], nullptr);
//        }
//    }
//
//    // unlock error mutex
//    pthread_mutex_unlock(&context->error_mutex);
//    pthread_mutex_unlock(&context->stage_mutex);
//
//    //todo unlock all mutexes
//
//    delete context;
    exit(1);
}