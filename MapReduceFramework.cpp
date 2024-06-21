#include <MapReduceFramework.h>

pthread_t[] threads;
stage_t stage = stage_t.UNDEFINED_STAGE;
float percentage = 0;

void emit2 (K2* key, V2* value, void* context){

}
void emit3 (K3* key, V3* value, void* context){

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
  for (int i = 0; i<multiThreadLevel; i++) {
    // create threads
    pthread_t thread;
    pthread_create(thread, null, runThread, null);
    threads.push_back(thread);
  }
}

void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){
  job.state = state->stage;
  job.percentage = state->percentage;
}
void closeJobHandle(JobHandle job){

}

void runThread() {

}
	