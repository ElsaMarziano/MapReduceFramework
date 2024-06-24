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
#include <set>


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
  long unsigned int getInputLength ();
  void setJobState (JobState state);
  const MapReduceClient &getClient () const;

  pthread_mutex_t jobMutex;
  pthread_cond_t jobCond;
  std::set<K2*> getUniqueKeySet();
  std::vector<std::vector<std::pair<K2*, V2*>>> getIntermediateVectors();
  std::vector<std::vector<std::pair<K2*, V2*>>> getShuffledVectors();
    void setShuffledVectors(std::vector<std::vector<std::pair<K2*, V2*>>> shuffledVectors);
    void insertToUniqueKeySet(std::vector<K2*> unique_key_set);
    void insertToIntermediateVectors(std::vector<std::pair<K2*, V2*>> intermediate_vector);





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
  std::set<K2*> uniqueKeySet;
  std::vector<std::vector<std::pair<K2*, V2*>>> intermediateVectors;
  std::vector<std::vector<std::pair<K2*, V2*>>> shuffledVectors;

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
