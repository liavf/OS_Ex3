//
// Created by liavf on 12/05/2025.
//

#include "MapReduceFramework.h"
#include "Barrier.h"
#include <iostream>
#include <atomic>
#include <thread>
#include <vector>
#include <cstdio>
#include <thread>
#include "pthread.h"

struct JobContext {
    JobContext(const MapReduceClient &client,
               const InputVec &inputVec, OutputVec &outputVec,
               int multiThreadLevel);
    ~JobContext();

    const MapReduceClient* client;
    const InputVec* input_vec;
    OutputVec* output_vec;
    int thread_num;
    int total_progress;

    std::vector<IntermediateVec> inter_vec;
    std::vector<IntermediateVec> shuff_int_vec;

    thread_t* all_threads;
//    pthread_t* all_threads;
    stage_t stage = UNDEFINED_STAGE;

    std::atomic<int> atomic_input_count;
    std::atomic<int> atomic_progress;
    std::atomic<int> atomic_inter_count;
    std::atomic<int> atomic_key_count;
    Barrier barrier;

    std::mutex inter_mutex;
    std::mutex output_mutex;
    std::mutex reduce_mutex;
    std::mutex stage_mutex;

    bool is_waiting;
};

JobContext::JobContext(const MapReduceClient &client,
                       const InputVec &inputVec, OutputVec &outputVec,
                       int multiThreadLevel) : barrier(multiThreadLevel) {

    this->client = &client;
    this->inputVec = &inputVec;
    this->outputVec = &outputVec;
    thread_num = multiThreadLevel;
    total_progress = inputVec.size();

    atomic_input_count = 0;
    atomic_inter_count = 0;
    atomic_progress = 0;
    atomic_key_count = 0;
//    pthread_t* threads = new pthread_t[thread_num];
    thread_t* threads = new thread_t[thread_num];
    this->all_threads = threads;

    is_waiting = false;
};

JobContext::~JobContext() {
    delete[] threads;
}

void thread_map(JobContext *jobContext);
void thread_shuffle(JobContext *jobContext);
void thread_reduce(JobContext *jobContext);

void move_to_shuffle(JobContext *jobContext);
void move_to_reduce(JobContext *jobContext);


void *main_thread_func(void *arg) {
    JobContext *context = static_cast<JobContext *>(arg);
    thread_map(context);
    context->barrier.barrier();
    thread_shuffle(context);
    context->barrier.barrier();
    thread_reduce(context);
}

void *thread_func(void *arg) {
    JobContext *context = static_cast<JobContext *>(arg);
    thread_map(context);
    thread_reduce(context);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    JobContext* context = new JobContext(client, inputVec, outputVec,
                                         multiThreadLevel);
    context->stage = MAP_STAGE;
    // create main thread
    std::thread* new_thread = new thread(&main_thread_func, context);
    if (!new_thread) {
        printf("system error: thread creation failed");
        exit(1);
    }
    else {
        context->all_threads[0] = new_thread;
    }
//    if (!pthread_create(&context->all_threads[0], nullptr, &main_thread_func, context)) {
//        printf("system error: thread creation failed");
//        exit(1);
//    }
    //create rest of threads - function without shuffle
    for (int i = 1; i < context->thread_num; i++) {
        std::thread new_thread* = new thread(&thread_func, context);
        if (!new_thread) {
            printf("system error: thread creation failed");
            exit(1);
        }
        else {
            context->all_threads[i] = new_thread;
        }
//        if (!pthread_create(&context->all_threads[i], nullptr, &thread_func, context)) {
//            printf("system error: thread creation failed");
//            exit(1);
//        }
    }
    return context;
}


void emit2 (K2* key, V2* value, void* context) {
    JobContext* job_context = static_cast<JobContext *>(context);
    IntermediatePair pair_to_add = {key, value};
//    job_context->inter_mutex.lock();
//    job_context->atomic_progress++;
//    job_context->atomic_inter_count++;
    job_context->inter_vec->push_back(pair_to_add);
//    job_context->inter_mutex.unlock();
}

void emit3 (K3* key, V3* value, void* context) {
    JobContext *job_context = static_cast<JobContext *>(context);
    OutputPair output_pair = {key, value};
    job_context->output_mutex.lock();
    job_context->atomic_progress++;
    jobContext->outputVec->push_back(output_pair);
    job_context->output_mutex.unlock();
}

void waitForJob(JobHandle job) {
    JobContext *context = static_cast<JobContext *>(job);
    if (context->is_waiting) {
        return;
    }
    else {
        context->is_waiting = true;
    }
    num_to_join = context->thread_num;
    for (int i = 0; i < num_to_join; i++) {
        int trial = context->all_threads[i].join();
        if (!trial) {
            printf("system error: failed joining threads");
            exit(1);
        }
//        if (!pthread_join(context->all_threads[i], nullptr)) {
//            printf("system error: failed joining threads");
//            exit(1);
//        }
    }
}

void getJobState(JobHandle job, JobState* state) {
    JobContext *job_context = static_cast<JobContext *>(job);

    job_context->stage_mutex.lock();
    state->stage = job_context->stage;
    int num_completed = job_context->atomic_progress;
    int num_to_complete = job_context->total_progress;
    float progress = (num_completed / num_to_complete) * 100;
    state->percentage = progress;
    job_context->stage_mutex.unlock();
}

void closeJobHandle(JobHandle job) {
    JobContext *job_context = static_cast<JobContext *>(job);
    waitForJob(job);
    delete job_context;
}

bool compare_func(const IntermediatePair &a, const IntermediatePair &b) {
    return *(a.first) < *(b.first);
}

void thread_map(JobContext *jobContext) {
    //each thread has its own vector
    IntermediateVec new_vec;
    int input_length = jobContext->input_vec->size();
    while (jobContext->atomic_input_count < input_length) {
        //TODO: check if this works, maybe the ++ needs to be in same line
        int curr_index = jobContext->atomic_input_count;
        auto curr_pair = jobContext->input_vec[curr_index];
        auto key_index = curr_pair.first;
        auto val_index = curr_pair.second;
        jobContext->client->map(key_index, val_index, &new_vec);
        jobContext->atomic_input_count++;
    }
    //sort for later
    std::sort(new_vec.begin(), new_vec.end(), compare_func);
    jobContext->inter_mutex->lock();
    jobContext->inter_vec.push_back(new_vec);
    jobContext->atomic_inter_count += new_vec.size();
    jobContext->inter_mutex->unlock();
}

void move_to_shuffle(JobContext *jobContext) {
    jobContext->stage_mutex.lock();
    jobContext->stage = SHUFFLE_STAGE;
    jobContext->total_progress = jobContext->atomic_inter_count;
    jobContext->atomic_progress = 0;
    jobContext->stage_mutex.unlock();
}

void thread_shuffle(JobContext *jobContext) {
    move_to_shuffle(jobContext);

    while (true) {
        //find minimal key from all vectors
        K2* min_key = nullptr;
//        K2* min_key = jobContext->inter_vec[0].back.first;
        for (int i = 1; i < jobContext->thread_num; i++) {
            if ((!jobContext->inter_vec[i].empty()) && (!min_key)) {
                min_key = jobContext->inter_vec[i].back.first;
            }
            if ((!jobContext->inter_vec[i].empty()) && (jobContext->inter_vec[i].back.first < min_key)) {
                min_key = jobContext->inter_vec[i].back.first;
            }
        }
        if (!min_key) {
            break;
        }

        //create a vector for each key and pop all elements from thread vectors
        IntermediateVec key_vec;
        for (int i = 1; i < jobContext->thread_num; i++) {
            while ((!jobContext->inter_vec[i].empty()) && (jobContext->inter_vec[i].back.first <= min_key)) {
                key_vec.push_back(jobContext->inter_vec[i].back);
                jobContext->inter_vec[i].pop_back();
                jobContext->atomic_progress++;
            }
        }

        //push key vector into shuffled vector
        jobContext->shuff_int_vec.push_back(key_vec);
        jobContext->atomic_key_count++;
    }
}

void move_to_reduce(JobContext *jobContext) {
    jobContext->stage_mutex.lock();
    jobContext->stage = REDUCE_STAGE;
    jobContext->total_progress = jobContext->atomic_key_count;
    jobContext->atomic_progress = 0;
    jobContext->stage_mutex.unlock();
}

void thread_reduce(JobContext *jobContext) {
    move_to_reduce(jobContext);
    while (true) {
        jobContext->reduce_mutex.lock();
        if (jobContext->shuff_int_vec.empty()) {
            jobContext->reduce_mutex.unlock();
            break;
        }
        IntermediateVec curr_vec = jobContext->shuff_int_vec.back;
        jobContext->shuff_int_vec.pop_back();
        jobContext->reduce_mutex.unlock();
        jobContext->client->reduce(curr_vec, jobContext);
        jobContext->atomic_progress++;
    }
}


