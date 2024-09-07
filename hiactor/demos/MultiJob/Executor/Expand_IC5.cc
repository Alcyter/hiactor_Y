#include "DataFlow/DataFlow.h"
#include "Expand_IC5.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include <cstdlib>

hiactor::DataType map_expand_IC5(const hiactor::DataType& input, std::function<bool(hiactor::InternalValue)> customized_func, std::bitset<100000>& visit)
{
    std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
    
    long long ID_from = _input[1].intValue;
    std::vector<hiactor::InternalValue> output;
    Graph_source* G =  Graph_source::GetInstance();
    // int null_value = -1;

    std::vector<long long> neighbors_forum = G -> neighbor[_person_isMemberOf_forum_][ID_from];
    std::vector<long long> neighbors_post = G -> neighbor[_person_hasCreated_post_][ID_from];

    // std::string joinDate_string ="1289805839104";
                                    
    
    // long long joinDate =  atoll(joinDate_string.c_str());


    for (unsigned i = 0; i < neighbors_forum.size(); i++) {
        long long ID_to = neighbors_forum[i];
        edge_tuple _edge(ID_from,ID_to);
        hiactor::InternalValue property_value=G->edge[_person_isMemberOf_forum_][_edge]["joinDate"];
        
        // std::cout<<"here is expand_IC5\n";
        if( customized_func(property_value) )
        {
            visit[G -> bitset_map_forum[ID_to]] = 1;
            // std::cout<<" true \n";
        }
        
            
    }

    for (unsigned i = 0; i < neighbors_post.size(); i++)
    {
        long long ID_to = neighbors_post[i];
        long long ID_forum = G->post_map_forum[ID_to];

        if (visit[G-> bitset_map_forum[ID_forum]] == 1)
        {
            std::vector<hiactor::InternalValue> ori_path = _input;
            hiactor::InternalValue node;
            node.intValue = ID_forum;
            ori_path.push_back(node);
            hiactor::InternalValue result_path;
            result_path.vectorValue = new std::vector<hiactor::InternalValue>(ori_path);
            output.push_back(result_path);
        }
    }

    for (unsigned i = 0; i < neighbors_forum.size(); i++) {
        long long ID_to = neighbors_forum[i]; 
        visit[G -> bitset_map_forum[ID_to]] = 0;         
    }

    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);
    return Data;
}




unsigned expand_IC5_vector_hash_string(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
    unsigned hash_value;
    if(input_vector[1].intValue>=0) {
        hash_value = input_vector[1].intValue;
    } else {        
        hash_value = rand();
    }
    return hash_value;
}