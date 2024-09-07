#include "DataFlow/DataFlow.h"
#include "Timing.h"
#include "NodeByIDScan.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

//#include "Graph_source/Graph_source.h"

hiactor::DataType map_ID_to_InternalRow(const hiactor::DataType& input, long long ID)
{
    startTiming();
    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    std::vector<hiactor::InternalValue> output;
    hiactor::InternalValue node_ID;
    
    // node_ID.intValue=atoll((*input._data.vectorValue)[0].stringValue);
    node_ID.intValue=ID;
    std::cout<<"NodeByID get is"<<node_ID.intValue<<"\n";
    output.push_back(node_ID);
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);

    return Data;
}

