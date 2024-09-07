#include "DataFlow/DataFlow.h"
#include "Expand.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


#include <cstdlib>




hiactor::DataType map_expand(const hiactor::DataType& input, std::string labelname, int ID_site, int distance_site, bool is_option)
{
    std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
    long long ID_from = _input[ID_site].intValue;
    std::vector<hiactor::InternalValue> output;

    Graph_source* G =  Graph_source::GetInstance();

    // std::unordered_map<long long, std::vector<long long>> map_neighbor = G -> neighbor[labelname];
    std::unordered_map<long long, std::vector<long long>> map_neighbor = G -> neighbor[0];

    int null_value =-1;

    if(ID_from == -1) { //then add NULL to the path
        std::vector<hiactor::InternalValue> _schema = _input;
        hiactor::InternalValue node;
        node.intValue = -1;
        _schema.push_back(node);
        hiactor::InternalValue schema;
        schema.vectorValue = new std::vector<hiactor::InternalValue>(_schema);
        output.push_back(schema);
    } else if(map_neighbor.find(ID_from) != map_neighbor.end()) {
        //add all the neighbors to the path
        std::vector<long long> neighbors = map_neighbor[ID_from];

        for (unsigned i = 0; i < neighbors.size(); i++) {

            long long ID_to = neighbors[i];
            std::vector<hiactor::InternalValue> _schema = _input;
            hiactor::InternalValue node;
            node.intValue = ID_to;
            _schema.push_back(node);
            // distance ++ change the destination
            if (distance_site != -1)
                {
                    _schema[distance_site].intValue = _schema[distance_site].intValue+1;
                    _schema[distance_site-1].intValue = ID_to;
                }

            hiactor::InternalValue schema;
            schema.vectorValue = new std::vector<hiactor::InternalValue>(_schema);
            output.push_back(schema);
        }
        if (distance_site != -1)
        {
            // reserve itself to the paths, by adding null 
            std::vector<hiactor::InternalValue> _schema = _input;
            hiactor::InternalValue node;
            node.intValue = null_value;
            _schema.push_back(node);
            hiactor::InternalValue schema;
            schema.vectorValue = new std::vector<hiactor::InternalValue> (_schema);
            output.push_back(schema);
        }
    } else if (is_option == true){
        // if this node cannot expand anymore, and this is option expand, then add null to the path
        std::vector<hiactor::InternalValue> _schema = _input;
        hiactor::InternalValue node;
        node.intValue = null_value;
        _schema.push_back(node);        
        hiactor::InternalValue schema;
        schema.vectorValue = new std::vector<hiactor::InternalValue> (_schema);
        output.push_back(schema);
        
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


unsigned expand_vector_hash_string(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;

    // for (unsigned i=0;i<input_vector.size();i++)
    //         std::cout<<input_vector[i].intValue<<" ";
    // std::cout<<"\n";

    std::hash<std::string> hash_function;
    unsigned hash_value;
    //there exists a question , what about the path[id1, id2, -1, -1, -1]
    if(input_vector.back().intValue>=0) {
        hash_value = hash_function(std::to_string(input_vector.back().intValue));
    } else {
        // std::cout<<"the last is -1\n";
        // for (unsigned i=0;i<input_vector.size();i++)
        //     std::cout<<input_vector[i].intValue<<" ";
        // std::cout<<"\n";
        //srand (time(0));
        hash_value = rand();
    }

    return hash_value;
}

