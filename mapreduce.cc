#include "thread_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <pthread.h>
#include <mpi.h>
#include <sstream>

int rank, nproc, num_threads ;
pthread_t *threadpool;
pthread_mutex_t lock ;
pthread_mutex_t print_lock;
pthread_mutex_t output_lock;
pthread_mutex_t joblock ;
pthread_cond_t signal ;

std::string job_name;
std::string input_filename ;

int num_reducers ;
int delay ;
int chunk_size ;

void *thread_job(void *arg) ;
void thread_pool_destructor();


void thread_pool_destructor(){
  for (int i = 0 ; i < (num_threads-1) ; i++ ){
	pthread_join(threadpool[i], NULL);
  }
  free(threadpool);
}

void thread_pool_reduce_destructor(){
  pthread_join(threadpool[0], NULL);
  free(threadpool);
}

void *thread_job(void *arg){
  int *id = (int*) arg;
  int chunkID, nodeID ;
 
  while (1) {
    //-------------------wait for mapping requests------------------------------//
    pthread_mutex_lock(&lock);
    // send request to job tracker
    MPI_Send(&rank, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

    // receive response from job tracker
    MPI_Recv(&chunkID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(&nodeID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    pthread_mutex_unlock(&lock);

    if (chunkID == -1) break ; // no more request

    //-----------------------start to do the map job------------------------------//
    std::vector<InputKeyValueTable> input_table ;
    std::vector<OutputKeyValueTable> output_table ;

    // check whether the node need to get the data remotely
    //if (nodeID != rank) sleep(delay);

    // input split function 
    input_table = InputSplitFunction(input_filename, chunkID, chunk_size);
      
    // map function 
    output_table = MapFunction(input_table);

    // partition function
    PartitionFunction(output_table, num_reducers);

    // output records
    pthread_mutex_lock(&output_lock);
    OutputRecords(output_table, job_name, rank, num_reducers);
    pthread_mutex_unlock(&output_lock);
  }


  return NULL;
}


void *reduce_thread_job(void *arg){
  int reducerID;
  sleep(rank);
  while (1) {
    //-------------------wait for mapping requests------------------------------//
    // send request to job tracker
    MPI_Sendrecv(&rank, 1, MPI_INT, 0, 0, &reducerID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    if (reducerID == -1) break ; // no more request
    //-----------------------start to do the reduce job------------------------------//
    std::vector<OutputKeyValueTable> output_list;
    std::vector<OutputKeyValueTable> group_list;

    // read record
    output_list = ReadTmpFile(job_name, reducerID, (nproc-1)) ;
    
    // sort fuction
    SortFunction(output_list);
    
    // group and reducefunction
    group_list = GroupReduceFunction(output_list);

    // output file
    OutputFunction(group_list, job_name, reducerID);
  }

  return NULL; 
}

int main(int argc, char **argv)
{
  // set rank, nproc, ncpus
  MPI_Init(NULL, NULL) ;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);

  cpu_set_t cpuset ;
  sched_getaffinity(0, sizeof(cpuset), &cpuset);
  num_threads = CPU_COUNT(&cpuset) ;

  // jobtracker
  int queue_size, query_rank;

  // read cmd intput
  job_name = std::string(argv[1]);
  num_reducers = std::stoi(argv[2]);
  delay = std::stoi(argv[3]);
  input_filename = std::string(argv[4]);
  chunk_size = std::stoi(argv[5]);
  std::string locality_config_filename = std::string(argv[6]);



  if (rank==0){

    // read locality config filename
    std::vector<Locality_Config_Table> locality_config_table;
    Locality_Config_Table nextdata;
    locality_config_table = ReadLocalityConfigFile(locality_config_filename, nproc-1); 

    // generate map tasks and insert them into a scheduling queue
    std::vector<Locality_Config_Table> map_tasks_queue ;
    for (int i = 0 ; i < locality_config_table.size() ; i++) map_tasks_queue.push_back(locality_config_table.at(i));
    for (int i = 0 ; i < (num_threads-1)*(nproc-1); i++ ) map_tasks_queue.push_back({-1, -1});

    queue_size = map_tasks_queue.size();

    // distribute map tasks
    for (int i = 0 ; i < queue_size; i++){
      // receive requests from task tracker
      MPI_Recv(&query_rank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      // find the next sending data
      nextdata = GetNextDataAndRemove(map_tasks_queue, rank);
      if (nextdata.nodeID != query_rank && nextdata.nodeID != -1) sleep(delay);
      // send response to task tracker 
      MPI_Send(&nextdata.chunkID, 1, MPI_INT, query_rank, 0, MPI_COMM_WORLD);
      MPI_Send(&nextdata.nodeID, 1, MPI_INT, query_rank, 0, MPI_COMM_WORLD);
    }
   
    
    // generate reduce tasks and insert them into a scheduling queue
    std::vector<int> reduce_tasks_queue;
    for (int i = 1 ; i <= num_reducers ; i++) reduce_tasks_queue.push_back(i);
    for (int i = 0 ; i < (nproc-1); i++) reduce_tasks_queue.push_back(-1);

    // distribute reduce tasks
    for (int i = 0 ; i < reduce_tasks_queue.size(); i++ ){
      // receive requests from task tracker
      MPI_Recv(&query_rank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

      // send response to task tracker 
      MPI_Send(&reduce_tasks_queue.at(i), 1, MPI_INT, query_rank, 0, MPI_COMM_WORLD);
    }
  }

  else{
    // clean the existing file
    CleanExistingFile(job_name, rank, num_reducers);

    // thread_id
    int thread_id[num_threads-1];

    // create map thread pool
    threadpool = (pthread_t*)malloc(sizeof(pthread_t)*(num_threads-1));
    //sleep(rank);
    // create mapper threads
    for (int i = 0 ; i < (num_threads-1) ; i++){
      thread_id[i] = i ;
      pthread_create(&threadpool[i], NULL, &thread_job, &thread_id[i]) ;
    }

    // destructor
    thread_pool_destructor();

    //create reuduce thread pool
    threadpool = (pthread_t*)malloc(sizeof(pthread_t));
    pthread_create(&threadpool[0], NULL, &reduce_thread_job, NULL) ;
    thread_pool_reduce_destructor();


  }

  return 0;
}

