//
// Created by user on 21/06/2024.
//

#ifndef MAPREDUCEFRAMEWORK_JOBCONTEXT_H
#define MAPREDUCEFRAMEWORK_JOBCONTEXT_H


class JobContext
{
public:
    JobContext(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel);
    ~JobContext();

    void runThread();
    void waitForJob();
    void getJobState(JobState* state);
    void addThread(pthread_t thread);

private:
    void setJobState(stage_t stage, float percentage, bool finished = false);

    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;
    int multiThreadLevel;
    std::vector<pthread_t> threads;
    pthread_mutex_t jobMutex;
    pthread_cond_t jobCond;
    stage_t stage;
    float percentage;
    bool jobFinished;
};


#endif //MAPREDUCEFRAMEWORK_JOBCONTEXT_H
