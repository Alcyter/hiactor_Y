#include "DataFlow/DataFlow.h"
#include "Add_distance.h"

#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


hiactor::DataType map_Add_distance(const hiactor::DataType& input)
{
    std::vector<hiactor::InternalValue> _input =*input._data.vectorValue;
    std::vector<hiactor::InternalValue> output;
    output = _input;

    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;    
    hiactor::InternalValue node_ID;
    node_ID = output.back();
    
    output.push_back(node_ID);
    node_ID.intValue=0;
    output.push_back(node_ID);
    std::cout<<"Add_distance success\n";
    for (unsigned i=0;i<output.size();i++)
        std::cout<<output[i].intValue<<" ";
    std::cout<<"\n";
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);

    return Data;
}