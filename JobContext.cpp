#include "JobContext.h"
#include <pthread.h>
void *runThread(void *context)
{
  auto *castContext = static_cast<ThreadContext *>(context);
  auto &jobContext = castContext->jobContext;

  long unsigned int old_value = 0;
  printf("new thread\n");

  while ((old_value = castContext->atomic_length.fetch_add(1)) < jobContext->getInputLength())
  {
    printf("old value %d\n", old_value);

    if (old_value == 0)
    {
      pthread_mutex_lock(&jobContext->jobMutex);
      jobContext->setJobState({MAP_STAGE, 0}); // Enter map stage
      pthread_mutex_unlock(&jobContext->jobMutex);
    }
    jobContext->getClient().map(jobContext->getInputVec()[old_value].first,
                               jobContext->getInputVec()[old_value].second,
                               &context);

    float result = static_cast<float>(100.0f * static_cast<float>(castContext->atomic_length.load()) /
                                      jobContext->getInputLength());
    if (old_value == jobContext->getInputLength() - 1)
    {
      result = 100.0f;
    }

    pthread_mutex_lock(&jobContext->jobMutex);
    jobContext->setJobState({MAP_STAGE, result});
    pthread_mutex_unlock(&jobContext->jobMutex);
  }

  printf("out\n");

  // Additional synchronization logic here if needed

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

//  TODO Save this in bits somehow
  for (int i = 0; i < multiThreadLevel; i++)
  {
    addThread ();
  }

}

JobContext::~JobContext ()
{
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
//  if (state.stage == MAP_STAGE)
//  {
//    float result = static_cast<float>(100.0f
//                                      * static_cast<float>(atomic_length) /
//                                      inputLength);
//    state.percentage = result;
//
//  }
  return state;
}

void JobContext::addThread ()
{
  pthread_t thread;
  auto *context = new ThreadContext (atomic_length, this);

  pthread_attr_t attr;
  pthread_attr_init (&attr);
  pthread_create (&thread, &attr, runThread, static_cast<void *>(context));
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

void JobContext::setJobState (JobState state)
{
  pthread_mutex_lock (&jobMutex);
  this->state = state;
  pthread_cond_broadcast (&jobCond);
  pthread_cond_broadcast (&jobCond);
  pthread_mutex_unlock (&jobMutex);
}




