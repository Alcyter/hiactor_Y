#include "DataFlow/DataFlow.h"
#include "Top.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <queue>
#include <algorithm>

hiactor::DataType reduce_function(const hiactor::DataType& input) {
    // std::cout<<"------------------this is  reduce_function\n";
    std::vector<hiactor::InternalValue> _vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> vec = *(_vec.back().vectorValue);
    std::vector<hiactor::InternalValue> reduced;
    std::string countryXName ="Vietnam";
    std::string countryYName ="Finland";
    
    std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;

    for(unsigned i = 0; i < vec.size(); i++) {
        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        // std::cout<<"ID:"<<msg[0].intValue<<" city name "<<msg[1].stringValue<<" post ID"<<msg[2].intValue<<"place ID"<<msg[3].intValue<<"\n";

        long long person_ID = msg[0].intValue;
        
        if (storage.find(person_ID) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue ID;
            hiactor::InternalValue xCount;
            hiactor::InternalValue yCount;
            hiactor::InternalValue Count;
            ID.intValue=person_ID;
            xCount.intValue=0;
            yCount.intValue=0;
            Count.intValue=1;
            if (strcmp(msg[1].stringValue,countryXName.c_str()) == 0) 
                xCount.intValue = 1;
            else 
                yCount.intValue = 1;
            _msg.push_back(ID);
            _msg.push_back(xCount);
            _msg.push_back(yCount);
            _msg.push_back(Count);
            
            storage[person_ID] = _msg;
            // std::cout<<"ID:"<<person_ID<<" city name "<<msg[1].stringValue<<" post ID"<<msg[2].intValue<<"place ID"<<msg[3].intValue<<"\n";
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[person_ID];
            if (strcmp(msg[1].stringValue,countryXName.c_str()) == 0) 
                _msg[1].intValue = _msg[1].intValue + 1;
            else 
                _msg[2].intValue = _msg[2].intValue + 1;

            _msg[3].intValue = _msg[3].intValue + 1;

            storage[person_ID] = _msg;
            // std::cout<<"ID:"<<person_ID<<" city name "<<msg[1].stringValue<<" post ID"<<msg[2].intValue<<"place ID"<<msg[3].intValue<<"\n";
        }
    }

    for(auto it = storage.begin(); it != storage.end(); ++it){
        hiactor::InternalValue _node;
        _node.vectorValue = new std::vector<hiactor::InternalValue> (it->second);
        reduced.push_back(_node);        
    }   

    hiactor::DataType output;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
    output.type = hiactor::DataType::VECTOR;

    return output;
}

unsigned key_shuffle_function(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
    std::hash<std::string> hash_function;
    unsigned hash_value = hash_function(std::to_string(input_vector[0].intValue));

    return hash_value;
}