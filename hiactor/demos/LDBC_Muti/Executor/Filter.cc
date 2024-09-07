#include "DataFlow/DataFlow.h"
#include "Project.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


hiactor::DataType filter_by_firstName(const hiactor::DataType& input) {
//////    //they are hard code following
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
hiactor::DataType filter_by_creationDate(const hiactor::DataType& input)
{
    // std::cout<<"filter"<<std::endl;
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;
    std::string creationDate ="1344152433150";
    long long creationDate_int =  atoll(creationDate.c_str());

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);
        long long _creationDate =(*vec[i].vectorValue)[2].intValue;
        if(_creationDate<=creationDate_int) {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
        }
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;

}