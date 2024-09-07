#include "DataFlow/DataFlow.h"
#include "ExpandInto.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


hiactor::DataType map_expandinto(const hiactor::DataType& input, int label_index, int site_from, int site_to)
{
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    Graph_source* G =  Graph_source::GetInstance();

    std::unordered_map<long long, std::vector<long long>> map_neighbor = G -> neighbor[label_index];

    for(unsigned i = 0; i < vec.size(); i++) {
        std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);
        long long from_ID = _vec[site_from].intValue;
        long long to_ID = _vec[site_to].intValue;
        std::vector<long long> neighbors = map_neighbor[from_ID];
        for (unsigned j = 0; j < neighbors.size(); j++)
        {
            if (neighbors[j] == to_ID)
            {
                hiactor::InternalValue val;
                val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
                tmp.push_back(val);
                break;
            }
        }
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType map_expand_add(const hiactor::DataType& input, int label_index, int site_from, int site_to) {
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp = vec;
    int flag = 0;

    Graph_source* G =  Graph_source::GetInstance();

    std::vector<long long> neighbors = G -> neighbor[label_index][vec[site_from].intValue];

    long long target = vec[site_to].intValue;
    for(unsigned i = 0; i < neighbors.size(); i++) {
        if(neighbors[i] == target) {
            hiactor::InternalValue value;
            value.intValue = 0;
            tmp.push_back(value);
            flag = 1;
            break;
        }
    }
    if(!flag) {
        hiactor::InternalValue value;
        value.intValue = 1;
        tmp.push_back(value);
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}