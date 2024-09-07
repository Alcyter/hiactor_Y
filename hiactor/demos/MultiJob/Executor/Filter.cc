#include "DataFlow/DataFlow.h"
// #include "Project.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


hiactor::DataType filter_by_personID(const hiactor::DataType& input)
{
    //std::cout<<"-----------------this is  filter_by_personID\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    long long person_ID = 1129;

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        

        if((*vec[i].vectorValue)[1].intValue != person_ID)
        {
            tmp.push_back(vec[i]);
        }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_joinDate(const hiactor::DataType& input)
{
    //std::cout<<"-----------------this is  filter_by_joinDate\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string joinDate_string ="1289805839104";
    
    long long joinDate =  atoll(joinDate_string.c_str());

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        
        
        long long _joinDate = atoll((*vec[i].vectorValue)[2].stringValue);
        
        if(joinDate<_joinDate)
        {
            tmp.push_back(vec[i]);
        }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}



hiactor::DataType filter_function(const hiactor::DataType& input, std::function<bool(hiactor::InternalValue)> customized_func) {

    // std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    // std::vector<hiactor::InternalValue> tmp;
    for (auto it = (*(input._data.vectorValue)).begin(); it != (*(input._data.vectorValue)).end();)
    {
        if(customized_func(*it)) ++it;
        else{
             it = (*(input._data.vectorValue)).erase(it);
        }
    }
    // hiactor::DataType output;
    // output.type = hiactor::DataType::VECTOR;
    // output._data.vectorValue = input._data.vectorValue;
    return input;
}

// hiactor::DataType filter_function(const hiactor::DataType& input, std::function<bool(hiactor::InternalValue)> customized_func) {

//     std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
//     std::vector<hiactor::InternalValue> tmp;
//     for(unsigned i = 0; i < vec.size(); i++) {
//         if(customized_func(vec[i])) tmp.push_back(vec[i]);
//     }
//     hiactor::DataType output;
//     output.type = hiactor::DataType::VECTOR;
//     output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
//     return output;
// }