#include "DataFlow/DataFlow.h"
#include "Project.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

hiactor::DataType distinct(const hiactor::DataType& input) {
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> filtered;
    for(const auto& pair : vec) {
        bool isDuplicate = false;
        for(const auto& existingVec : filtered) {
            if(((*existingVec.vectorValue)[0]).intValue == ((*pair.vectorValue)[0]).intValue &&
               ((*existingVec.vectorValue)[1]).intValue == ((*pair.vectorValue)[1]).intValue) {
                isDuplicate = true;
                break;
            }
        }
        if(!isDuplicate) {
            filtered.push_back(pair);
        }
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(filtered);
    return output;
}

hiactor::DataType filter_by_firstName(const hiactor::DataType& input) {
//////    //they are hard code following
    // std::cout<<"this is filter by firstName\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;
    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);
        if(strcmp((*vec[i].vectorValue)[2].stringValue , "Hans") == 0) {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
        }
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
//
//
    return input;
}