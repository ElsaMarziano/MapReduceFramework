#include <pthread.h>
#include "MapReduceFramework.h"
#include "JobContext.h"
#include <iostream>
#include <vector>
#include <string>
#include <map>

using namespace std;

std::map<JobHandle, JobContext *> jobs;

void systemError (string text)
{
  std::cout << "system error: " << text << std::endl;
  exit (1);
}

// Context is whatever we want it to be - we get it from map, and we're the
// ones to call map.
// Should probably contain the input, output and intermediate pointers
void emit2 (K2 *key, V2 *value, void *context)
{
//  Add key and value to the intermediate vector of the calling thread

  auto *castContext = static_cast<ThreadContext *>(context);
  pthread_mutex_lock (&castContext->jobContext->jobMutex);
  castContext->intermediateVector->push_back (std::make_pair (key,
                                                              value));
  pthread_mutex_unlock (&castContext->jobContext->jobMutex);

}

void emit3 (K3 *key, V3 *value, void *context)
{
    auto* castContext = static_cast<ThreadContext*>(context);
    castContext->jobContext->insertToOutputVec (key, value);

}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobContext *job = new JobContext (client, inputVec, outputVec,
                                    multiThreadLevel);
  JobHandle jobHandle = new JobHandle ();
  jobs[jobHandle] = job;
  return jobHandle;
}

void waitForJob (JobHandle job)
{
  if (jobs.find (job) == jobs.end ())
  {
    systemError ("Job not found");
  }
  jobs[job]->waitForJob ();
}

void getJobState (JobHandle job, JobState *state)
{
  auto it = jobs.find (job);
  if (it == jobs.end ())
  {
    systemError ("Job not found");
  }
  *state = it->second->getJobState ();
}

void closeJobHandle(JobHandle job)
{
  auto it = jobs.find(job);
  if (it == jobs.end())
  {
    systemError("Job not found");
  }
  waitForJob(job);
  delete it->second;
  jobs.erase(it);
}


	