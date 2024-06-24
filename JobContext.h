//
// Created by user on 21/06/2024.
//

#ifndef MAPREDUCEFRAMEWORK_JOBCONTEXT_H
#define MAPREDUCEFRAMEWORK_JOBCONTEXT_H
#include "MapReduceFramework.h"
#include <pthread.h>
#include <iostream>
#include <memory>
#include <atomic>
#include <vector>
#include <utility>
#include "Barrier.h"
#include <algorithm>

using namespace std;

struct ThreadContext;

class JobContext
{
 public:
  JobContext (const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel);
  ~JobContext ();
  void operator= (const JobContext &other);

  void waitForJob ();
  JobState getJobState ();
  void addThread (int id);
  InputVec getInputVec ();
  OutputVec getOutputVec ();
  Barrier* getBarrier ();
  long unsigned int getInputLength ();
  void setJobState (JobState state);
  const MapReduceClient &getClient () const;

  pthread_mutex_t jobMutex;
  pthread_cond_t jobCond;

  //std::vector<<std::vector<std::pair<K2*, V2*>>>> intermediateVectors;
  //std::vector<<std::vector<std::pair<K2*, V2*>>>> shuffledVectors;




 private:
  void setJobState (stage_t stage, float percentage, bool finished = false);

  const MapReduceClient &client;
  const InputVec &inputVec;
  OutputVec &outputVec;
  int multiThreadLevel;
  long unsigned int inputLength;
  std::vector <pthread_t> threads;
  std::vector<ThreadContext *> threadContexts;

  JobState state;
  bool jobFinished;
  std::atomic<long unsigned int> atomic_length;
  Barrier *barrier;

};

struct ThreadContext {
    int threadId;
    std::unique_ptr<std::vector<std::pair<K2*, V2*>>> intermediateVector;
    std::atomic<long unsigned int>& atomic_length;
    JobContext* jobContext;


    ThreadContext(int id, std::atomic<long unsigned int>& atomic_length, JobContext* jobContext)
        : threadId(id), intermediateVector(new std::vector<std::pair<K2*,
                                           V2*>>()), atomic_length
                                           (atomic_length),
                                           jobContext(jobContext)
          {}

};

#endif //MAPREDUCEFRAMEWORK_JOBCONTEXT_H
