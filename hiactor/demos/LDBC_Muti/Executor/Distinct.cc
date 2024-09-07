#include "DataFlow/DataFlow.h"
#include "Top.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <queue>
#include <algorithm>

hiactor::DataType distinct_on_shard(const hiactor::DataType& input) {
    // stopTiming();
    std::vector<hiactor::InternalValue> vec = *((*(input._data.vectorValue)).back().vectorValue);
    std::unordered_set<long long> uniqueSet;
    std::vector<hiactor::InternalValue> filtered;
    for(const auto& value : vec) {
        if(uniqueSet.insert((*value.vectorValue)[0].intValue).second) {
            filtered.push_back(value);
        }
    }
    // stopTiming();
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(filtered);
    return output;
}


unsigned distinct_hash_string(const hiactor::InternalValue& input) {
    // return 0;
    return (*input.vectorValue)[0].intValue;
}

unsigned distinct_shuffle_hash_string(const hiactor::InternalValue& input) {
    return (*input.vectorValue)[0].intValue;
}