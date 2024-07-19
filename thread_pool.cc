#include "thread_pool.h"

std::vector<Locality_Config_Table> ReadLocalityConfigFile(std::string filename, int total_worker_nodes)
{
  std::vector<Locality_Config_Table> locality_config_table ;
  std::ifstream input_file(filename);
  int chunkID, nodeID;
  if (!input_file.is_open()) printf("File not found.\n");
  while (input_file >> chunkID){
	  input_file >> nodeID ;
	  if (nodeID > total_worker_nodes) {
		  nodeID %= total_worker_nodes ;
		  if (nodeID == 0) nodeID = total_worker_nodes;
	  }
	  locality_config_table.push_back({chunkID, nodeID});
  }
  input_file.close();
  return locality_config_table ;
}

void Test_ReadLocalityConfigFile(std::vector<Locality_Config_Table> list) {
	for ( int i = 0 ; i < list.size() ; i++ ) {
	  printf("chunckID: %d, nodeID: %d\n", list.at(i).chunkID, list.at(i).nodeID);
        }
}


Locality_Config_Table GetNextDataAndRemove(std::vector<Locality_Config_Table> &locality_config_file, int rank)
{
  int idx = 0 ;
  Locality_Config_Table nextdata;
  nextdata.chunkID = locality_config_file.at(0).chunkID;
  nextdata.nodeID = locality_config_file.at(0).nodeID;

  for (int i = 0 ; i < locality_config_file.size(); i++){
    if (locality_config_file.at(i).nodeID == rank && locality_config_file.at(idx).nodeID != rank) {
        idx = i;
	nextdata = locality_config_file.at(i);
    }
  }
  
  locality_config_file.erase(locality_config_file.begin()+idx); 
  return nextdata;
}

std::vector<InputKeyValueTable> InputSplitFunction(std::string filename, int chunkID, int chunk_size)
{
  std::vector<InputKeyValueTable> input_list ;
  std::ifstream input_file(filename);
  std::string line;
  int startline = (chunkID-1)*chunk_size+1 ;
  int currentline = 0 ;

  while((currentline < (chunkID-1)*chunk_size) && getline(input_file, line)) currentline++ ;
  currentline = 0 ;
  
  while((currentline < chunk_size) && getline(input_file, line)) {
    line+=" ";
    input_list.push_back({startline++, line});
    currentline++;
  }

  input_file.close();
  return input_list ;
}

void Test_InputSplitFunction(std::vector<InputKeyValueTable> input_list, int rank, int thread_id, int chunkID, int nodeID)
{
  std::cout << "rank: " << rank << ", thread_id: " << thread_id << ", chunkID: " << chunkID << ", nodeID: " << nodeID << std::endl ;
  for (int i = 0 ; i < input_list.size(); i++){
	  std::cout << "line: "<< input_list.at(i).key << " > " << input_list.at(i).value << std::endl ;
  }
  std::cout << std::endl << std::endl ;
}

std::vector<OutputKeyValueTable> MapFunction(std::vector<InputKeyValueTable> input_list)
{ 
    std::vector<OutputKeyValueTable> output_list ;
    std::vector<std::string> word_list;
    std::vector<int> count_list ;

    for (int i = 0 ; i < input_list.size(); i++ ){
      size_t pos = 0;
      std::string word;
      std::string line = input_list.at(i).value;

      while ((pos = line.find(" ")) != std::string::npos)
      {
        word = line.substr(0, pos);
	auto idx = std::find(word_list.begin(), word_list.end(), word);
	if (idx == word_list.end()) {
	  word_list.push_back(word);
	  count_list.push_back(1);
	}
	else {
	  int index = idx - word_list.begin();
	  count_list.at(index)++;
	}

        line.erase(0, pos+1);
      }

    }

    for (int i = 0 ; i < word_list.size() ; i++){
      output_list.push_back({word_list.at(i), count_list.at(i)});
    }

    return output_list;
}

void Test_MapFunction(std::vector<OutputKeyValueTable> output_list, int rank, int thread_id, int chunkID, int nodeID)
{
  std::cout << "rank: " << rank << ", thread_id: " << thread_id << ", chunkID: " << chunkID << ", nodeID: " << nodeID << std::endl ;
  for (int i = 0 ; i < output_list.size(); i++){
	  std::cout << output_list.at(i).key << "  " << output_list.at(i).value << std::endl ;
  }
  std::cout << std::endl << std::endl ;
}

void PartitionFunction(std::vector<OutputKeyValueTable> &output_list, int num_reducers)
{
  int dec, reducerID ;
  for (int i = 0 ; i < output_list.size(); i++) {
    dec = (int)output_list.at(i).key.at(0);
    reducerID = dec % num_reducers ;
    if (reducerID == 0){
      output_list.at(i).reducerID = num_reducers  ;
    }
    else {
      output_list.at(i).reducerID = reducerID  ;
    }
  } 
}

void Test_PartitionFunction(std::vector<OutputKeyValueTable> output_list, int num_reducers)
{
  for (int i = 0 ; i < output_list.size(); i++) {
    std::cout << output_list.at(i).key << ", ascii: " << (int)output_list.at(i).key.at(0) << ", mod: " << (int)output_list.at(i).key.at(0) % num_reducers << ", reducers: " << output_list.at(i).reducerID << std::endl;
  }
  std::cout << std::endl << std::endl ;
}

void OutputRecords(std::vector<OutputKeyValueTable> output_list, std::string job_name, int rank, int num_reducers)
{
  std::stringstream rank_id ;
  rank_id <<rank;
  for (int i=1 ; i <= num_reducers; i++){
      std::ofstream out;
      std::stringstream id;
      id << i;
      out.open("/home/pp21/pp21s01/"+job_name+"-"+rank_id.str()+"-p"+id.str()+".tmp", std::ofstream::app);
      for (int j=0 ; j < output_list.size(); j++){
	if (output_list.at(j).reducerID == i){
          out << output_list.at(j).key << " " << output_list.at(j).value << "\n";
        }
      }
      out.close();
  }
}

std::vector<OutputKeyValueTable> ReadTmpFile(std::string job_name, int reducerID, int worker_nodes){
  std::vector<OutputKeyValueTable> output_list;
  std::stringstream ssreduce ;
  ssreduce << reducerID;

  for (int i=1; i <= worker_nodes; i++){
    std::stringstream ssrank;
    ssrank << i;
    std::ifstream input_file("/home/pp21/pp21s01/"+job_name+"-"+ssrank.str()+"-p"+ssreduce.str()+".tmp");
    if(!input_file.is_open()){
      std::cout << "File Not Fount in ReadTmpFile()" << std::endl;
      break;
    }
    
    std::string word;
    int count ;
    while (input_file >> word) {
	input_file >> count ;
	output_list.push_back({word, count});
    }

    input_file.close();
  }

  return output_list;
}

void Test_ReadTmpFile(std::vector<OutputKeyValueTable> output_list, int rank, int reducerID)
{
  std::cout << "rank: " << rank << ", reduerID: " << reducerID << ", size: " << output_list.size() << std::endl;
  for (int i = 0 ; i < output_list.size(); i++ ){
	  std::cout << output_list.at(i).key << " " << output_list.at(i).value << std::endl;
  }

  std::cout << std::endl << std::endl ;  
}

void SortFunction(std::vector<OutputKeyValueTable> &output_list)
{
  std::sort(output_list.begin(), output_list.end(), [](const OutputKeyValueTable &item1, const OutputKeyValueTable &item2)->bool { return item1.key < item2.key; });
}

std::vector<OutputKeyValueTable> GroupReduceFunction(std::vector<OutputKeyValueTable> output_list)
{
  std::vector<OutputKeyValueTable> group_list;
  std::vector<std::string> word_list;
  std::vector<int> count_list;

  std::string word;
  int count ;

  for (int i = 0 ; i < output_list.size(); i++) {
    word = output_list.at(i).key;
    count = output_list.at(i).value;

    auto idx = std::find(word_list.begin(), word_list.end(), word);
    if (idx == word_list.end()){
      word_list.push_back(word);
      count_list.push_back(count);
    }
    else {
      int index = idx - word_list.begin();
      count_list.at(index)+=count;
    }
  }

  for (int i = 0 ; i < word_list.size(); i++ ) group_list.push_back({word_list.at(i), count_list.at(i)});
  return group_list;
}

void OutputFunction(std::vector<OutputKeyValueTable> output_list, std::string job_name, int reducerID){
  std::stringstream sstream;
  sstream << reducerID;
  std::ofstream out("/home/pp21/pp21s01/"+job_name+"-"+sstream.str()+".out");
  
  for (int i = 0 ; i < output_list.size(); i++ ){
    out << output_list.at(i).key << " " << output_list.at(i).value << "\n";
  }

  out.close();
}


void CleanExistingFile(std::string job_name,int rank, int num_reducers)
{
  std::stringstream rank_id ;
  rank_id << rank ;
  for (int i = 1; i <= num_reducers; i++ ){
    std::ofstream out;
    std::stringstream id;
    id << i;
    out.open("/home/pp21/pp21s01/"+job_name+"-"+rank_id.str()+"-p"+id.str()+".tmp", std::ofstream::out | std::ofstream::trunc);
    out.close();
  }
}


