//
// Created by user on 21/06/2024.
//
#include "JobContext.h"

JobContext::JobContext(const MapReduceClient& client, const InputVec& inputVec,
                       OutputVec& outputVec, int multiThreadLevel)
        : client(client), inputVec(inputVec), outputVec(outputVec), multiThreadLevel(multiThreadLevel),
        stage(UNDEFINED_STAGE), percentage(0), jobFinished(false) {
    pthread_mutex_init(&jobMutex, nullptr);
    pthread_cond_init(&jobCond, nullptr);
}

JobContext::~JobContext()
{
    pthread_mutex_destroy(&jobMutex);
    pthread_cond_destroy(&jobCond);
}

void JobContext::waitForJob() {
    pthread_mutex_lock(&jobMutex);
    while (!jobFinished) {
        pthread_cond_wait(&jobCond, &jobMutex);
    }
    pthread_mutex_unlock(&jobMutex);

    for (pthread_t thread : threads) {
        pthread_join(thread, nullptr);
    }
}

void JobContext::runThread()
{
    client.map(nullptr, nullptr, nullptr);

}

void JobContext::getJobState(JobState* state)
{
    pthread_mutex_lock(&jobMutex);
    state->stage = stage;
    pthread_mutex_unlock(&jobMutex);
}

void JobContext::addThread(pthread_t thread)
{
    threads.push_back(thread);
}

