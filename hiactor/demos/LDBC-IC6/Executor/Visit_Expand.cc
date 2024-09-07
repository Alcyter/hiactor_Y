#include "DataFlow/DataFlow.h"
#include "Visit_Expand.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include <cstdlib>


hiactor::DataType map_expand_visit(const hiactor::DataType& input, int label_index, int ID_site, int distance_site, bool is_option, std::bitset<100000>& visit)
{
    std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
    long long ID_from = _input[ID_site].intValue;
    std::vector<hiactor::InternalValue> output;

    Graph_source* G =  Graph_source::GetInstance();

    int null_value = -1;

    if(ID_from == -1) { //then add NULL to the path
        std::vector<hiactor::InternalValue> ori_path = _input;
        hiactor::InternalValue node;
        node.intValue = -1;
        ori_path.push_back(node);
        hiactor::InternalValue result_path;
        result_path.vectorValue = new std::vector<hiactor::InternalValue>(ori_path);
        output.push_back(result_path);
    } else if(visit[G -> bitset_map[ID_from]] == 1) {
        // just skip the process
    } else if(G -> neighbor[label_index].find(ID_from) != G -> neighbor[label_index].end()) {
        visit[G -> bitset_map[ID_from]] = 1;
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
            // reserve itself to the paths, by adding  
            std::vector<hiactor::InternalValue> ori_path = _input;
            hiactor::InternalValue node;
            node.intValue = ori_path[distance_site-1].intValue;
            ori_path.push_back(node);
            hiactor::InternalValue result_path;
            result_path.vectorValue = new std::vector<hiactor::InternalValue> (ori_path);
            output.push_back(result_path);
        }
    } else if (is_option == true){
        // if this node cannot expand anymore, and this is option expand, then add null to the path
        visit[G -> bitset_map[ID_from]] = 1; // probably useless
        std::vector<hiactor::InternalValue> ori_path = _input;
        hiactor::InternalValue node;
        node.intValue = null_value;
        ori_path.push_back(node);        
        hiactor::InternalValue result_path;
        result_path.vectorValue = new std::vector<hiactor::InternalValue> (ori_path);
        output.push_back(result_path);
        
    } else{

    }
    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);
    return Data;
}




unsigned expand_visit_vector_hash_string(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
    unsigned hash_value;
    if(input_vector.back().intValue>=0) {
        hash_value = input_vector.back().intValue;
    } else {        
        hash_value = rand();
    }
    return hash_value;
}