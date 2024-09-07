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
Graph_source* G = Graph_source::GetInstance();

void transferToOneHop(long long tid, long long id, int relation_expand) {
    if(G -> interested_tag[tid][id] == 1) {
        return;
    }
    G -> interested_tag[tid][id] = 1;
    for(long long nei : G -> neighbor[relation_expand][id]) {
        transferToOneHop(tid, nei, relation_expand);
    }
}

hiactor::DataType transfer(const hiactor::DataType& input, std::string name, int relation_name, int relation_expand) {
    long long target_id = G -> name_to_id[name];
    long long tid = (*input._data.vectorValue)[0].intValue;
    transferToOneHop(tid, target_id, relation_expand);

    startTiming();
    return input;
}
