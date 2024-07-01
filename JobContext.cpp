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
  long unsigned int old_value = castContext->atomic.fetch_add (1);

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
                                      * static_cast<float>(castContext->atomic.load ())
                                      /
                                      jobContext->getInputLength ());
    if (castContext->atomic.load ()
        >= jobContext->getInputLength () - 1)
    {
      jobContext->setJobState ({MAP_STAGE, 100.0f});
      break;
    }
    else if (old_value < jobContext->getInputLength () - 1)
    {
      old_value = castContext->atomic.fetch_add (1);
      jobContext->setJobState ({MAP_STAGE, result});
    }
  }

  /**
   * Sorting
   */
  if (!castContext->intermediateVector->empty ())
  {
//     Sorting the keys
    std::sort (castContext->intermediateVector->begin (),
               castContext->intermediateVector->end (),
               [] (const IntermediatePair &a, const IntermediatePair &b)
               {
                   return *a.first < *b.first;
               });
    jobContext->insertToIntermediateVectors (*(castContext->intermediateVector));

    for (size_t i = 0; i < castContext->intermediateVector->size (); i++)
    {
        jobContext->insertToUniqueKeySet ((*castContext->intermediateVector)[i].first);

    }
  }
  jobContext->getBarrier ()->barrier (); // Wait for everyone

  /**
   * ------------------------------- SHUFFLE PHASE -------------------------------
   */

  if (castContext->threadId == 0)
  {
    castContext->atomic.exchange (0);
    jobContext->setJobState ({SHUFFLE_STAGE, 0});
    auto uniqueKeySet = jobContext->getUniqueKeySet ();

    // Convert the set to a vector
    std::vector < K2 *
    > uniqueKeySetVector (uniqueKeySet.begin (), uniqueKeySet.end ());

    // Sort the vector of pointers
    std::sort (uniqueKeySetVector.begin (), uniqueKeySetVector.end (),
               [] (const K2 *a, const K2 *b)
               {
                   return *b < *a;
               });

    for (const K2 *key: uniqueKeySetVector)
    {
      IntermediateVec key_vector;
      for (auto vector: jobContext->getIntermediateVectors ())
      {
        while (!vector.empty () && vector.back ().first == key)
        {
          key_vector.push_back (vector.back ());
          vector.pop_back ();
          castContext->atomic.fetch_add (1);
          float result = static_cast<float>(100.0f
                                            * static_cast<float>(castContext->atomic.load ())
                                            /jobContext->getInputLength ());
          jobContext->setJobState ({SHUFFLE_STAGE, result});
        }
      }
      jobContext->insertToShuffledVectors (key_vector);
    }
    printf ("shuffle: %ld\n", jobContext->getShuffledVectors ().size ());

    jobContext->setJobState ({SHUFFLE_STAGE, 100.0f});
    sem_post (jobContext->getShuffleSemaphore ());  // Notify all threads that shuffle is done
    castContext->atomic.exchange (0);
  }
  else
  {
    sem_wait (jobContext->getShuffleSemaphore ());  // Wait until shuffle is done
  }
  /**
   * ------------------------------- REDUCE PHASE -------------------------------
   */
  old_value = castContext->atomic.fetch_add (1);

  while (old_value < jobContext->getShuffledVectors ().size ())
  {
//    If this is the first iteration, set the job state to 0 - we're
//    entering reduce stage
    if (old_value == 0)
    {
      jobContext->setJobState ({REDUCE_STAGE, 0});
    }
//    printf("Thread %d is in REDUCE PHASE %d %ld\n", castContext->threadId,
//           old_value, jobContext->getShuffledVectors().size());

//    Reduce over the intermediate we got
    jobContext->getClient ().reduce
        (&(jobContext->getShuffledVectors ()[old_value]),
         context);
//    Update state
//TODO Percentage is not good
    float result = static_cast<float>(100.0f
                                      * static_cast<float>(castContext->atomic.load ())
                                      /
                                      jobContext->getShuffledVectors ().size ());
    if (castContext->atomic.load ()
        >= jobContext->getShuffledVectors ().size () - 1)
    {
      jobContext->setJobState ({REDUCE_STAGE, 100.0f});
      break;
    }
    else if (old_value < jobContext->getShuffledVectors ().size () - 1)
    {
      old_value = castContext->atomic.fetch_add (1);
      jobContext->setJobState ({REDUCE_STAGE, result});
    }

  }

  return nullptr;
}

JobContext::JobContext (const MapReduceClient &client, const InputVec &inputVec,
                        OutputVec &outputVec, int multiThreadLevel)
    : client (client), inputVec (inputVec), outputVec (outputVec),
      multiThreadLevel (multiThreadLevel), state ({UNDEFINED_STAGE, 0}),
      jobFinished (false), atomic (0)
{
  pthread_mutex_init (&jobMutex, nullptr);
  pthread_cond_init (&jobCond, nullptr);
  inputLength = inputVec.size ();
  barrier = new Barrier (multiThreadLevel);
  sem_init (&shuffleSemaphore, 0, 0);  // Initialize semaphore with value 0

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
  sem_destroy (&shuffleSemaphore);
  delete barrier;
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
    float result = static_cast<float>(atomic) / inputLength * 100.0f;
    if (result > 100) result = 100.0f;
    state.percentage = result;
  }
  return state;
}

void JobContext::addThread (int id)
{
  pthread_t thread;
  auto *context = new ThreadContext (id, atomic, this);

  pthread_attr_t attr;
  pthread_attr_init (&attr);
  if (pthread_create (&thread, &attr, runThread, static_cast<void *>
  (context)) != 0)
  {
    jobSystemError ("Could not create thread");
  };
  pthread_attr_destroy (&attr);
  threadContexts.push_back (context);
  threads.push_back (thread);
}

void JobContext::insertToShuffledVectors (IntermediateVec vectors)
{
  pthread_mutex_lock (&jobMutex);
  shuffledVectors.push_back (vectors);
  pthread_cond_broadcast (&jobCond);
  pthread_mutex_unlock (&jobMutex);
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

std::vector <IntermediateVec> JobContext::getShuffledVectors ()
{
  return shuffledVectors;
}

std::vector <IntermediateVec> JobContext::getIntermediateVectors ()
{
  return intermediateVectors;
}

void JobContext::setJobState (JobState state)
{
  pthread_mutex_lock (&jobMutex);
  this->state = state;
  pthread_cond_broadcast (&jobCond);
  pthread_mutex_unlock (&jobMutex);
}

void JobContext::insertToUniqueKeySet (K2 *uniqueKey)
{
  pthread_mutex_lock (&jobMutex);
  uniqueKeySet.insert (uniqueKey);
  pthread_cond_broadcast (&jobCond);
  pthread_mutex_unlock (&jobMutex);
}

std::set<K2 *, K2Comparator> JobContext::getUniqueKeySet ()
{
  return uniqueKeySet;
}

sem_t *JobContext::getShuffleSemaphore ()
{
  return &shuffleSemaphore;
}

void
JobContext::insertToIntermediateVectors (IntermediateVec intermediateVector)
{
  pthread_mutex_lock (&jobMutex);
  intermediateVectors.push_back (intermediateVector);
  pthread_cond_broadcast (&jobCond);
  pthread_mutex_unlock (&jobMutex);
}




