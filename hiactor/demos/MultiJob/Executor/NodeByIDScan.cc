#include "DataFlow/DataFlow.h"
#include "NodeByIDScan.h"
#include "file_sink_exe.h"
#include "Timing.h"
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
    Data.job_id = input.job_id;
    std::vector<hiactor::InternalValue> output;
    hiactor::InternalValue node_ID, jobid;
    std::cout << ID << std::endl;
    std::cout << input.job_id << std::endl;
    jobid.intValue = input.job_id;
    node_ID.intValue=ID;
    output.push_back(jobid);
    output.push_back(node_ID);
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);

    return Data;
}

