#include "DataFlow/DataFlow.h"
#include "Filter.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


hiactor::DataType filter_by_personID(const hiactor::DataType& input)
{
    // std::cout<<"-----------------this is  filter_by_personID\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    long long person_ID = 1129;

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        

        if(((*vec[i].vectorValue)[1]).intValue != person_ID)
        {
            tmp.push_back(vec[i]);
        }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_tag_one(const hiactor::DataType& input)
{
    //std::cout<<"-----------------this is  filter_by_tag_one\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string tagName = "Evo_Morales";

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        

        if(strcmp((*vec[i].vectorValue)[1].stringValue,tagName.c_str()) == 0)
        {
            tmp.push_back(vec[i]);
        }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_tag_two(const hiactor::DataType& input)
{
    // std::cout<<"-----------------this is  filter_by_tag_two\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string tagName = "Evo_Morales";

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        

        if(strcmp((*vec[i].vectorValue)[1].stringValue,tagName.c_str()) != 0)
        {
            tmp.push_back(vec[i]);
        }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}