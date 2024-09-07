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

hiactor::DataType map_addbitset(const hiactor::DataType& input, int index, int label)
{
    Graph_source* G =  Graph_source::GetInstance();
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    long long id = vec[index].intValue;
//    std::vector<long long> tagid = G -> neighbor[_person_hasInterest_tag_][id];
    std::vector<long long> tagid = G -> neighbor[label][id];
    for(auto ID : tagid) {
        G -> interested_tag[ID] = 1;
    }

    return input;
}

