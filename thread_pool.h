#ifndef THREAD_POOL_H_
#define THREAD_POOL_H_

#include <vector>
#include <stdio.h>
#include <string>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <sstream>
#include <stdio.h>
#include <unistd.h>

struct Locality_Config_Table
{
  int chunkID;
  int nodeID;
};

struct InputKeyValueTable
{
  int key;
  std::string value;
};

struct OutputKeyValueTable
{
  std::string key;
  int value;
  int reducerID;
};

std::vector<Locality_Config_Table> ReadLocalityConfigFile(std::string filename, int total_worker_nodes);
void Test_ReadLocalityConfigFile(std::vector<Locality_Config_Table> list);

Locality_Config_Table GetNextDataAndRemove(std::vector<Locality_Config_Table> &locality_config_table, int rank);

std::vector<InputKeyValueTable> InputSplitFunction(std::string filename, int chunkID, int chunk_size);
void Test_InputSplitFunction(std::vector<InputKeyValueTable> input_list, int rank, int thread_id, int chunkID, int nodeID);

std::vector<OutputKeyValueTable> MapFunction(std::vector<InputKeyValueTable> input_list);
void Test_MapFunction(std::vector<OutputKeyValueTable> output_list, int rank, int thread_id, int chunkID, int nodeID);

void PartitionFunction(std::vector<OutputKeyValueTable> &output_list, int num_reducers);
void Test_PartitionFunction(std::vector<OutputKeyValueTable> output_list, int num_reducers);

void OutputRecords(std::vector<OutputKeyValueTable> output_list, std::string job_name, int rank, int num_reducers);
void CleanExistingFile(std::string job_name, int rank, int num_reducers);

std::vector<OutputKeyValueTable> ReadTmpFile(std::string job_name, int workernodes, int reducerID);
void Test_ReadTmpFile(std::vector<OutputKeyValueTable> output_list, int rank, int reducerID);

void SortFunction(std::vector<OutputKeyValueTable> &output_list);
std::vector<OutputKeyValueTable> GroupReduceFunction(std::vector<OutputKeyValueTable> output_list);

void OutputFunction(std::vector<OutputKeyValueTable> output_list, std::string job_name, int reducerID);

#endif /*THREAD_POOL_H_*/

