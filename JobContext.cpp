#include "JobContext.h"
#include <pthread.h>
#include <iostream>
#include <algorithm>

using namespace std;

void jobSystemError (string text)
{
  std::cout << "system error: " << text << std::endl;
  exit (1);
}

void *runThread (void *context)
{
  auto *castContext = static_cast<ThreadContext *>(context);
  auto &jobContext = castContext->jobContext;

  /**
   *------------------------------ MAP PHASE -------------------------------
   */
  long unsigned int old_value = castContext->atomic_length.fetch_add (1);

  while (old_value < jobContext->getInputLength ())
  {
//    If this is the first iteration, set the job state to 0 - we're
//    entering map stage
    if (old_value == 0)
    {
      jobContext->setJobState ({MAP_STAGE, 0});
    }
//    Map over the input we got
    jobContext->getClient ().map (jobContext->getInputVec ()[old_value].first,
                                  jobContext->getInputVec ()[old_value].second,
                                  context);
//    Update state
    float result = static_cast<float>(100.0f
                                      * static_cast<float>(castContext->atomic_length.load ())
                                      /
                                      jobContext->getInputLength ());
    if (castContext->atomic_length.load ()
        >= jobContext->getInputLength () - 1)
    {
      jobContext->setJobState ({MAP_STAGE, 100.0f});
      break;
    }
    else if (old_value < jobContext->getInputLength () - 1)
    {
      old_value = castContext->atomic_length.fetch_add (1);
      jobContext->setJobState ({MAP_STAGE, result});
    }
  }

  /**
   * Sorting
   */
  std::sort (castContext->intermediateVector->begin (),
             castContext->intermediateVector->end (),
             [](const std::pair<K2 *, V2 *> &a, const std::pair<K2 *, V2 *> &b)
             {
               return *a.first < *b.first;
             });

    printf("before barrier %d\n", castContext->threadId);
    jobContext->getBarrier()->barrier();
    printf("after barrier %d\n", castContext->threadId);
  // Additional synchronization logic here if needed
  //jobContext->intermediaryVectors.push_back (castContext->intermediateVector);

  /**
   * ------------------------------- SHUFFLE PHASE -------------------------------
   */
  if(castContext->threadId == 0)
  {
    jobContext->setJobState ({SHUFFLE_STAGE, 0});
//    for(auto &vector: jobContext->intermediaryVectors)
//    {
//      for(auto &pair: *vector)
//      {
//        jobContext->shuffledVectors[pair.first->hash() % jobContext->multiThreadLevel].push_back(pair);
//      }
//    }

  /**
   * ------------------------------- REDUCE PHASE -------------------------------
   */
      long unsigned int old_value = castContext->atomic_length.fetch_add (1);
      castContext->atomic_length.store(0);

      while (old_value < jobContext->getInputLength ())
      {
//    If this is the first iteration, set the job state to 0 - we're
//    entering map stage
          if (old_value == 0)
          {
              jobContext->setJobState ({REDUCE_STAGE, 0});
          }
//    Reduce over the intermediate we got
          jobContext->getClient ().reduce(castContext->shuffledVector[old_value].first,
                                         castContext->shuffledVector[old_value].second,
                                         context);
//    Update state
          float result = static_cast<float>(100.0f
                                            * static_cast<float>(castContext->atomic_length.load ())
                                            /
                                            jobContext->getInputLength ());
          if (castContext->atomic_length.load ()
              >= jobContext->getShuffledVectors().size() - 1)
          {
              jobContext->setJobState ({REDUCE_STAGE, 100.0f});
              break;
          }
          else if (old_value < jobContext->getShuffledVectors().size() - 1)
          {
              old_value = castContext->atomic_length.fetch_add (1);
              jobContext->setJobState ({REDUCE_STAGE, result});
          }
      }

  }

  return nullptr;
}

JobContext::JobContext (const MapReduceClient &client, const InputVec &inputVec,
                        OutputVec &outputVec, int multiThreadLevel)
    : client (client), inputVec (inputVec), outputVec (outputVec),
      multiThreadLevel (multiThreadLevel), state ({UNDEFINED_STAGE, 0}),
      jobFinished (false), atomic_length (0)
{
  pthread_mutex_init (&jobMutex, nullptr
  );
  pthread_cond_init (&jobCond, nullptr
  );
  inputLength = inputVec.size ();

  barrier = new Barrier(multiThreadLevel);

//  TODO Save this in bits somehow
  for (int i = 0; i < multiThreadLevel; i++)
  {
    addThread (i);
  }

}

JobContext::~JobContext ()
{
  for (auto context: threadContexts)
  {
    context->intermediateVector->clear ();
    delete context;
  }
  for (auto it = threads.begin (); it != threads.end ();)
  {
    pthread_cancel (*it);  // Cancel the thread
    // Erase the thread from the vector and advance the iterator
    it = threads.erase (it);
  }
  pthread_mutex_destroy (&jobMutex);
  pthread_cond_destroy (&jobCond);
//  TODO add sem_destroy
}

//void JobContext::operator= (const JobContext &other)
//{
//  client = other.client;
//  inputVec = other.inputVec;
//  outputVec = other.outputVec;
//  multiThreadLevel = other.multiThreadLevel;
//  jobFinished = other.jobFinished;
//  atomic_length = other.atomic_length;
//  inputLength = other.inputLength;
//  state = other.state;
//  threads = other.threads;
//  threadContexts = other.threadContexts;
//  jobMutex = other.jobMutex;
//  jobCond = other.jobCond;
//}

void JobContext::waitForJob ()
{
  pthread_mutex_lock (&jobMutex);
  while (!jobFinished)
  {
    pthread_cond_wait (&jobCond, &jobMutex);
  }
  pthread_mutex_unlock (&jobMutex);

  for (pthread_t thread: threads)
  {
    pthread_join (thread, nullptr);
  }
}

JobState JobContext::getJobState ()
{
  if (state.stage == MAP_STAGE)
  {
    float result = static_cast<float>(100.0f
                                      * static_cast<float>(atomic_length) /
                                      inputLength);
    if(result > 100) result = 100.0f;
    state.percentage = result;
  }
  return state;
}

void JobContext::addThread (int id)
{
  pthread_t thread;
  auto *context = new ThreadContext (id, atomic_length, this);

  pthread_attr_t attr;
  pthread_attr_init (&attr);
  if(pthread_create (&thread, &attr, runThread, static_cast<void *>
  (context)) != 0)  {
    jobSystemError("Could not create thread");
  };
  pthread_attr_destroy (&attr);
  threadContexts.push_back (context);
  threads.push_back (thread);
}

InputVec JobContext::getInputVec ()
{
  return inputVec;
}

OutputVec JobContext::getOutputVec ()
{
  return outputVec;
}

long unsigned int JobContext::getInputLength ()
{
  return inputLength;
}

const MapReduceClient &JobContext::getClient () const
{
  return client;
}

Barrier *JobContext::getBarrier ()
{
  return barrier;
}

std::vector<std::vector<std::pair<K2 *, V2 *>>> JobContext::getShuffledVectors ()
{
  return std::vector<std::vector<std::pair<K2 *, V2 *>>>();
}

void JobContext::setJobState (JobState state)
{
  pthread_mutex_lock (&jobMutex);
  this->state = state;
  pthread_cond_broadcast (&jobCond);
  pthread_mutex_unlock (&jobMutex);
}




