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
    
    
    std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;

    for(unsigned i = 0; i < vec.size(); i++) {
        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        long long tag_ID = msg[0].intValue;
        
        if (storage.find(tag_ID) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue ID;
            hiactor::InternalValue min_Date;
            hiactor::InternalValue Count;
            ID.intValue=tag_ID;

            min_Date.stringValue= new char[strlen(msg[1].stringValue)+1];
            strcpy(min_Date.stringValue,msg[1].stringValue);

            Count.intValue=1;
            
            _msg.push_back(ID);
            _msg.push_back(min_Date);
            _msg.push_back(Count);
            
            storage[tag_ID] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[tag_ID];
            long long Date_one = atoll(_msg[1].stringValue);
            long long Date_two = atoll(msg[1].stringValue);
            if (Date_one>Date_two)
            {
                _msg[1].stringValue = new char[strlen(msg[1].stringValue)+1];
                strcpy(_msg[1].stringValue,msg[1].stringValue);
            }
           
            _msg[2].intValue = _msg[2].intValue + 1;

            storage[tag_ID] = _msg;
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