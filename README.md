# CS422PP-mapreduce

This repository contains a simple MapReduce distributed computing platform implemented in C++.

# Implementation
**Jobtracker**  
The Jobtracker is executed by rank 0 and is responsible for allocating map tasks and reduce tasks, which are mainly implemented using MPI_Send() and MPI_Recv(). First, the Jobtracker executes MPI_Recv() to receive requests from task trackers, and then decides which chunkID to allocate based on query_rank. The Jobtracker prioritizes allocating map tasks with query_rank equal to nodeID, indicating data locality. If there is no locality, it simulates the need for remote data transfer by executing sleep(). Once the chunkID to be allocated is determined, it executes MPI_Send() to send the computation data. When task trackers request data, it is possible that all map tasks and reduce tasks have already been allocated, causing the task tracker to keep waiting for requests, and the program cannot terminate smoothly. To successfully terminate the program, when all map tasks and reduce tasks have been allocated, the Jobtracker notifies the task tracker that there is no more data, and the mapper thread or reducer thread can finish execution. In the implementation, the Jobtracker sends a value of -1, indicating the end of execution.

**Mapper thread**  
The mapper thread is implemented using a thread pool. When the Jobtracker sends data to the task tracker, the mapper thread is created from the thread pool and processes the data. The mapper thread mainly executes three functions: InputSplitFunction(), MapFunction(), and PartitionFunction(). The InputSplitFunction() is responsible for storing the data to be processed line by line. Each line has a space at the end, as the MapFunction() will later split tokens based on spaces; otherwise, the last token of the line would not be read. Finally, the PartitionFunction() determines which reducers will process the key based on the ASCII code of the key's first character. This ensures that keys with the same initial character are processed by the same reducer.

**Reducer thread**
The reducer thread is also implemented using a thread pool. When the Jobtracker sends data to the task tracker, the reducer thread is created from the thread pool to process the data. The reducer thread reads the intermediate keys with the .tmp extension that correspond to the received reducerID. Next, the reducer thread executes the sort() function to sort the keys in ascending order, then merges the same keys together, and finally outputs the result.
