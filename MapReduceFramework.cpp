#include <pthread.h>
#include <MapReduceFramework.h>
#include <iostream>
#include <vector>
#include <string>


pthread_t[] threads;
pthread_mutex_t waitMutex;
JobState state = new JobState ({UNDEFINED_STAGE, 0});
bool joined = false;
int inputLength = 0;


// Context is whatever we want it to be - we get it from map, and we're the
// ones to call map.
// Should probably contain the input, output and intermediate pointers
void emit2 (K2 *key, V2 *value, void *context)
{

}
void emit3 (K3 *key, V3 *value, void *context)
{

}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  JobContext job = new JobContext (client, inputVec, outputVec, multiThreadLevel);
  pthread_mutex_init (&waitMutex, nullptr);
  inputLength = inputVec.size ();
  for (int i = 0; i < multiThreadLevel; i++)
  {
    // create threads
    pthread_t thread;
    pthread_create (thread, null, runThread, null);
    JobContext.addThread (thread); //TODO check the implementation in JobContext
  }
}

void waitForJob (JobHandle job)
{
  pthread_mutex_lock (&waitMutex);
  // Check if the job has already been joined
  if (!job->joined)
  {
    job->joined = true;  // Mark it as joined
    pthread_mutex_unlock (&job->waitMutex);  // Unlock before joining the
    // thread
    // TODO Check this is okay and doesn't result in endless loop
    for (pthread_t thread: job->threads)
    {
      pthread_join (thread, nullptr);
    }
  }
  else
  {
    pthread_mutex_unlock (&job->waitMutex);  // Just unlock if already joined
  }
}

void getJobState (JobHandle job, JobState *state)
{
  state = job.state; // change to use function from the job class
}

void closeJobHandle (JobHandle job)
{
//  Free everything, delete all threads
  pthread_mutex_destroy (&waitMutex);
  for(pthread_t thread: threads) {
    pthread_cancel(thread);
  }
}

void runThread () // Parameters: maybe pid?
{
//  Update the state when running

//  While there still are vectors in the input, take a vector and run the map
// function on it.
// Add some kind of key to make sure he's the only one taking a vector from
// the array

// If there are no more vectors,sort the arrays of vector he mapped

//Once this is done, wait for the other threads to finish. Then if this is
//thread 0, shuffle all of the arrays

// Once done waiting for the shuffles, take a vector from the shuffled
// vectors and run reduce on it until there are no more vectors.

}

void systemError(string text) {
  std::cout << "system error: "<< text << std::endl;
  exit(1);
}
	