//
// Created by user on 21/06/2024.
//

#ifndef MAPREDUCEFRAMEWORK_JOBCONTEXT_H
#define MAPREDUCEFRAMEWORK_JOBCONTEXT_H
#include "MapReduceFramework.h"
#include "Barrier.h"

#include <pthread.h>
#include <iostream>
#include <memory>
#include <atomic>
#include <vector>
#include <utility>
#include <set>
#include <algorithm>
#include <semaphore.h>

using namespace std;

struct ThreadContext;

struct K2Comparator {
    bool operator()(const K2* key1, const K2* key2) const {
		return (*key1)<(*key2);
    }
};


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
  std::set<K2*, K2Comparator> getUniqueKeySet();
  std::vector<IntermediateVec> getIntermediateVectors();
  std::vector<IntermediateVec> getShuffledVectors();
  sem_t* getShuffleSemaphore();
    void insertToShuffledVectors(IntermediateVec     vectors);
    void insertToUniqueKeySet(K2* unique_key);
    void insertToIntermediateVectors(IntermediateVec intermediate_vector);
    void insertToOutputVec(K3* key, V3* value);
    void setJobFinished();
    int getMultiThreadLevel();




 private:
  void setJobState (stage_t stage, float percentage, bool finished = false);
  const MapReduceClient &client;
  const InputVec &inputVec;
  OutputVec &outputVec;
  bool isWaitingForJob;
  int multiThreadLevel;
  long unsigned int inputLength;
  std::vector <pthread_t> threads;
  std::vector<ThreadContext *> threadContexts;

  JobState state;
  bool jobFinished;
  std::atomic<long unsigned int> atomic;
  std::set<K2*, K2Comparator> uniqueKeySet;
  std::vector<std::vector<std::pair<K2*, V2*>>> intermediateVectors;
  std::vector<std::vector<std::pair<K2*, V2*>>> shuffledVectors;
  Barrier *barrier;
  sem_t shuffleSemaphore;  // Semaphore to control shuffle synchronization

};

struct ThreadContext {
    int threadId;
    std::unique_ptr<std::vector<std::pair<K2*, V2*>>> intermediateVector;
    std::atomic<long unsigned int>& atomic;
    JobContext* jobContext;

    ThreadContext(int id, std::atomic<long unsigned int>& atomic, JobContext* jobContext)
        : threadId(id), intermediateVector(new std::vector<std::pair<K2*,
                                           V2*>>()), atomic
                                           (atomic),
                                           jobContext(jobContext)
          {}

};

#endif //MAPREDUCEFRAMEWORK_JOBCONTEXT_H
