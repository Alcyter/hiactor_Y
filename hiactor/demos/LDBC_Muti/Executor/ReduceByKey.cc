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
    std::vector<hiactor::InternalValue> _vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> vec = *(_vec.back().vectorValue);
    std::vector<hiactor::InternalValue> reduced;
    std::unordered_map<long long, long long> storage;
    std::unordered_map<long long, std::vector<hiactor::InternalValue> > other_elements;
    for(unsigned i = 0; i < vec.size(); i++) {
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        long long person_ID = msg[0].intValue;
        long long creationDate = atoll(msg[2].stringValue);
        if (storage.find(person_ID) == storage.end())
        {
            storage[person_ID] = creationDate;
            other_elements[person_ID] = msg;
        }
        else
        {
            if (creationDate>storage[person_ID])
            {
                storage[person_ID] = creationDate;
                other_elements[person_ID] = msg;
            }
        }
    }

    for(auto it = other_elements.begin(); it != other_elements.end(); ++it){
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