#include "DataFlow/DataFlow.h"
#include "Project.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>



hiactor::DataType filter_by_post_one(const hiactor::DataType& input)
{
    // std::cout<<"------------------this is  filter_by_post_one\n";

    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string creationDate ="1324152433150";
    long long creationDate_end =  atoll(creationDate.c_str());
    

    for(unsigned i = 0; i < vec.size(); i++) {
        
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);

        long long _creationDate = atoll((*vec[i].vectorValue)[1].stringValue);
        if (_creationDate<creationDate_end)
        {
            tmp.push_back(vec[i]);
        }            
        
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_post_two(const hiactor::DataType& input)
{
    // std::cout<<"------------------this is  filter_by_post_two\n";

    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string creationDate ="1273416487305";
    //1290556897173
    long long creationDate_end =  atoll(creationDate.c_str());
    

    for(unsigned i = 0; i < vec.size(); i++) {
        
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);

        long long _creationDate = atoll((*vec[i].vectorValue)[1].stringValue);
        if (_creationDate>=creationDate_end)
        {
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