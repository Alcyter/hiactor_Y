#include "DataFlow/DataFlow.h"
#include "Expand_vec.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include <cstdlib>





hiactor::DataType map_expand_vec(const hiactor::DataType& input, int label_index, int ID_site, int distance_site, bool is_option)
{
    std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
    long long ID_from = _input[ID_site].intValue;
    std::vector<hiactor::InternalValue> output;

    Graph_source* G =  Graph_source::GetInstance();

    // std::unordered_map<long long, std::vector<long long>> map_neighbor = G -> neighbor[label_index];

    int null_value =-1;

    if(ID_from == -1) { //then add NULL to the path
        std::vector<hiactor::InternalValue> ori_path = _input;
        hiactor::InternalValue node;
        node.intValue = -1;
        ori_path.push_back(node);
        hiactor::InternalValue result_path;
        result_path.vectorValue = new std::vector<hiactor::InternalValue>(ori_path);
        output.push_back(result_path);
    } else if(G -> neighbor[label_index].find(ID_from) != G -> neighbor[label_index].end()) {
        //add all the neighbors to the path
        std::vector<long long> neighbors = G -> neighbor[label_index][ID_from];

        for (unsigned i = 0; i < neighbors.size(); i++) {

            long long ID_to = neighbors[i];
            std::vector<hiactor::InternalValue> ori_path = _input;
            hiactor::InternalValue node;
            node.intValue = ID_to;
            ori_path.push_back(node);
            // distance ++ change the destination
            if (distance_site != -1)
                {
                    ori_path[distance_site].intValue = ori_path[distance_site].intValue+1;
                    ori_path[distance_site-1].intValue = ID_to;
                }

            hiactor::InternalValue result_path;
            result_path.vectorValue = new std::vector<hiactor::InternalValue>(ori_path);
            output.push_back(result_path);
        }
        if (distance_site != -1)
        {
            // reserve itself to the paths, by adding null 
            std::vector<hiactor::InternalValue> ori_path = _input;
            hiactor::InternalValue node;
            node.intValue = null_value;
            ori_path.push_back(node);
            hiactor::InternalValue result_path;
            result_path.vectorValue = new std::vector<hiactor::InternalValue> (ori_path);
            output.push_back(result_path);
        }
    } else if (is_option == true){
        // if this node cannot expand anymore, and this is option expand, then add null to the path
        std::vector<hiactor::InternalValue> ori_path = _input;
        hiactor::InternalValue node;
        node.intValue = null_value;
        ori_path.push_back(node);        
        hiactor::InternalValue result_path;
        result_path.vectorValue = new std::vector<hiactor::InternalValue> (ori_path);
        output.push_back(result_path);
        
    } else{
        //std::cout << "There is not any match personid" << std::endl;
        //output 是空的
    }
    //std::cout<<"neighbor output size"<<output.size()<<std::endl;
    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);
    return Data;
}


unsigned expand_vec_vector_hash_string(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;

       
    unsigned hash_value;
  
    if(input_vector.back().intValue>=0) {
        hash_value = input_vector.back().intValue;
    } else {        
        hash_value = rand();
    }

    return hash_value;
}

