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
    
    
    std::unordered_map<std::string , std::vector<hiactor::InternalValue> > storage;

    for(unsigned i = 0; i < vec.size(); i++) {
        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        std::string tagName = msg[1].stringValue;
        
        if (storage.find(tagName) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue _tagName;
            hiactor::InternalValue Count;            

            _tagName.stringValue= new char[strlen(msg[1].stringValue)+1];
            strcpy(_tagName.stringValue,msg[1].stringValue);

            Count.intValue=1;
            
            _msg.push_back(_tagName);            
            _msg.push_back(Count);
            
            storage[tagName] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[tagName];                     
           
            _msg[1].intValue = _msg[1].intValue + 1;

            storage[tagName] = _msg;
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
    std::string _str=input_vector[1].stringValue;
    unsigned hash_value = hash_function(_str);

    return hash_value;
}

unsigned key_shuffle_function_two(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
    std::hash<std::string> hash_function;
    std::string _str=input_vector[0].stringValue;
    unsigned hash_value = hash_function(_str);

    return hash_value;
}