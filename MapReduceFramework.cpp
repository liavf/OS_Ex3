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

struct Emit2Context {
    IntermediateVec* inter_vec; // thread level
    std::atomic<int>* atomic_inter_count;  // global counter
};

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

//    thread_t* all_threads;
    std::vector<std::thread> all_threads;
//    pthread_t* all_threads;
    stage_t stage = UNDEFINED_STAGE;

    std::atomic<int> atomic_input_count;
    std::atomic<int> atomic_progress;
    std::atomic<int> atomic_inter_count;
//    std::atomic<int> atomic_key_count;
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
    this->input_vec = &inputVec;
    this->output_vec = &outputVec;
    thread_num = multiThreadLevel;
    total_progress = inputVec.size();

    atomic_input_count = 0;
    atomic_inter_count = 0;
    atomic_progress = 0;
//    atomic_key_count = 0;
//    pthread_t* threads = new pthread_t[thread_num];
//    this->all_threads = threads;
    is_waiting = false;
};

JobContext::~JobContext() {
//    delete[] threads; // NEVER ALLOCATED TO HEAP
}

void thread_map(JobContext *jobContext);
void thread_shuffle(JobContext *jobContext);
void thread_reduce(JobContext *jobContext);

void move_to_next_phase(JobContext *jobContext, stage_t next_stage);
//void move_to_shuffle(JobContext *jobContext);
//void move_to_reduce(JobContext *jobContext);


void *main_thread_func(void *arg) {
    JobContext *context = static_cast<JobContext *>(arg);
    thread_map(context);
    context->barrier.barrier();
    thread_shuffle(context);
    context->barrier.barrier();
    thread_reduce(context);
    return nullptr;
}

void *thread_func(void *arg) {
    JobContext *context = static_cast<JobContext *>(arg);
    thread_map(context);
    context->barrier.barrier();
    context->barrier.barrier(); // barrier for shuffle
    thread_reduce(context);

    return nullptr;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    JobContext* context;
    // allocate job context
    try {
        context = new JobContext(client, inputVec, outputVec,
                                             multiThreadLevel);
    } catch (const std::bad_alloc& e) {
        std::cerr << "system error: memory allocation failed\n";
        exit(1);
    }
    context->stage = MAP_STAGE;
    // create all threads
    try {
        context->all_threads.emplace_back(main_thread_func, context);
        for (int i = 1; i < multiThreadLevel; ++i) {
            context->all_threads.emplace_back(thread_func, context);
        }
    } catch (const std::system_error& e) {
        std::cerr << "system error: thread creation failed\n";
        exit(1);
    }
//    std::thread* new_thread = new thread(&main_thread_func, context);
//    if (!new_thread) {
//        printf("system error: thread creation failed");
//        exit(1);
//    }
//    else {
//        context->all_threads[0] = new_thread;
//    }
//    if (!pthread_create(&context->all_threads[0], nullptr, &main_thread_func, context)) {
//        printf("system error: thread creation failed");
//        exit(1);
//    }
    //create rest of threads - function without shuffle
//    for (int i = 1; i < context->thread_num; i++) {
//        std::thread* new_thread = new thread(&thread_func, context);
//        if (!new_thread) {
//            printf("system error: thread creation failed");
//            exit(1);
//        }
//        else {
//            context->all_threads[i] = new_thread;
//        }
//        if (!pthread_create(&context->all_threads[i], nullptr, &thread_func, context)) {
//            printf("system error: thread creation failed");
//            exit(1);
//        }
    return context;
}


void emit2 (K2* key, V2* value, void* context) {
//    JobContext *job_context = static_cast<JobContext *>(context);
//    IntermediatePair pair_to_add = {key, value};
//    job_context->atomic_progress++;
////    job_context->atomic_inter_count++;
//    job_context->inter_vec.push_back(pair_to_add);
    auto* ctx = static_cast<Emit2Context*>(context);
    ctx->inter_vec->push_back({key, value});
    ctx->atomic_inter_count->fetch_add(1);
}

void emit3 (K3* key, V3* value, void* context) {
    JobContext *job_context = static_cast<JobContext *>(context);
    OutputPair output_pair = {key, value};
    job_context->output_mutex.lock();
    try
    {
//        std::cout << "[EMIT3] Output: Key = " << key << ", Value = " << value << std::endl;
        job_context->output_vec->push_back(output_pair);
        job_context->atomic_progress++;
    }
    catch (...) {
        std::cerr << "system error: failed to push to output vector\n";
        exit(1);
    }
    job_context->output_mutex.unlock();
}

void waitForJob(JobHandle job) {
    JobContext *context = static_cast<JobContext *>(job);
    // protection so only one job will join all threads
    static std::mutex wait_mutex;
    std::unique_lock<std::mutex> lock(wait_mutex);

    if (context->is_waiting) {
        return;
    }
    else {
        context->is_waiting = true;
    }
    lock.unlock();

    for (std::thread& t : context->all_threads) {
        if (t.joinable()) {
            try {
                t.join();
            } catch (const std::system_error& e) {
                std::cerr << "system error: failed joining thread: " << e.what() << std::endl;
                exit(1);
            }
        }
    }
//    num_to_join = context->thread_num;
//    for (int i = 0; i < num_to_join; i++) {
//        int trial = context->all_threads[i].join();
//        if (!trial) {
//            printf("system error: failed joining threads");
//            exit(1);
//        }
//        if (!pthread_join(context->all_threads[i], nullptr)) {
//            printf("system error: failed joining threads");
//            exit(1);
//        }
}

void getJobState(JobHandle job, JobState* state) {
    JobContext *job_context = static_cast<JobContext *>(job);

//    job_context->stage_mutex.lock();
    std::unique_lock<std::mutex> lock(job_context->stage_mutex);

    state->stage = job_context->stage;
    int num_completed = job_context->atomic_progress;
    int num_to_complete = job_context->total_progress;
    float progress = 0.0f;
    if (num_to_complete > 0) { // avoid division by zero
        progress = (static_cast<float>(num_completed) / num_to_complete) * 100;
    }

    state->percentage = progress;
//    job_context->stage_mutex.unlock();
}

void closeJobHandle(JobHandle job) {
    //////  ASSUMPTION - ONLY ONE JOB CALLS IT, NO NEED FOR MUTEX////////
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
    Emit2Context emit_ctx;
    emit_ctx.inter_vec = &new_vec;
    emit_ctx.atomic_inter_count = &jobContext->atomic_inter_count;

    int input_length = jobContext->input_vec->size();
//    while (jobContext->atomic_input_count < input_length) {
//    DELETED BECAUSE IT CAN CAUSE RACE CONDITION - IF MULTIPLE THREADS CHECK IT
    while (true) {
        int curr_index = jobContext->atomic_input_count.fetch_add(1);
        if (curr_index >= input_length) {
            break;  // done
        }
//        auto curr_pair = jobContext->input_vec[curr_index];
        auto& curr_pair = (*jobContext->input_vec)[curr_index];
        const K1* key = curr_pair.first;
        const V1* val = curr_pair.second;
        jobContext->client->map(key, val, &emit_ctx);
    }
    //sort for later
    std::sort(new_vec.begin(), new_vec.end(), compare_func);
    std::unique_lock<std::mutex> lock(jobContext->inter_mutex);
    jobContext->inter_vec.push_back(new_vec);
//    jobContext->atomic_inter_count += new_vec.size(); // already in emit2
}

bool keys_equal(K2* a, K2* b) {
    return !(*a < *b) && !(*b < *a);
}

void thread_shuffle(JobContext *jobContext) {
//    move_to_shuffle(jobContext);
    move_to_next_phase(jobContext, SHUFFLE_STAGE);

    while (true) {
        //find minimal key from all vectors
        K2* max_key = nullptr;
        // Find the max key from all non-empty vectors
        for (int i = 0; i < jobContext->thread_num; ++i) {
            if (!jobContext->inter_vec[i].empty()) {
                std::cout << "[SHUFFLE] Thread " << i << " BACK key = " << (jobContext->inter_vec[i].back().first) << std::endl;
                K2* candidate = jobContext->inter_vec[i].back().first;
                if (!max_key || *max_key < *candidate) {
                    max_key = candidate;
                }
            }
        }
        if (!max_key) {
            break; // all vectors are empty
        }

        //create a vector for each key and pop all elements from thread vectors
        IntermediateVec key_vec;
        for (int i = 0; i < jobContext->thread_num; i++) {
            auto& vec = jobContext->inter_vec[i];
            while (!vec.empty() && keys_equal(vec.back().first, max_key)) {
//                std::cout << "[SHUFFLE] Popping key = " << (void*)vec.back().first
//                          << " from thread " << i << std::endl;
                key_vec.push_back(vec.back());
                vec.pop_back();
                jobContext->atomic_progress++;
            }
        }

        //push key vector into shuffled vector
        jobContext->shuff_int_vec.push_back(key_vec);
//        jobContext->atomic_key_count++;
    }
}

void thread_reduce(JobContext *jobContext) {
//    move_to_reduce(jobContext);
    move_to_next_phase(jobContext, REDUCE_STAGE);
    while (true) {
        IntermediateVec curr_vec;
        jobContext->reduce_mutex.lock();
        if (jobContext->shuff_int_vec.empty()) {
            jobContext->reduce_mutex.unlock();
            break;
        }
        curr_vec = jobContext->shuff_int_vec.back();
        jobContext->shuff_int_vec.pop_back();
        jobContext->reduce_mutex.unlock();
        // DEBUG//
        if (!curr_vec.empty()) {
//            std::cout << "[REDUCE] Reducing Key at address = " << curr_vec.back().first
//                      << ", Num Pairs = " << curr_vec.size() << std::endl;
        }
        jobContext->client->reduce(&curr_vec, jobContext);
//        jobContext->atomic_progress++;
//        jobContext->atomic_progress += curr_vec.size();
    }
}

void move_to_next_phase(JobContext *jobContext, stage_t next_stage) {
    jobContext->stage_mutex.lock();
    jobContext->stage = next_stage;
    jobContext->total_progress = jobContext->atomic_inter_count;
    jobContext->atomic_progress = 0;
    jobContext->stage_mutex.unlock();
}

//void move_to_shuffle(JobContext *jobContext) {
//    jobContext->stage_mutex.lock();
//    jobContext->stage = SHUFFLE_STAGE;
//    jobContext->total_progress = jobContext->atomic_inter_count;
//    jobContext->atomic_progress = 0;
//    jobContext->stage_mutex.unlock();
//}
//
//void move_to_reduce(JobContext *jobContext) {
//    jobContext->stage_mutex.lock();
//    jobContext->stage = REDUCE_STAGE;
//    jobContext->total_progress = jobContext->atomic_key_count;
//    jobContext->atomic_progress = 0;
//    jobContext->stage_mutex.unlock();
//}


