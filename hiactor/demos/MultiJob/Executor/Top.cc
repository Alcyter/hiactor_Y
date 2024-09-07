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

// bool compare(const std::vector<hiactor::InternalValue>& a, const std::vector<hiactor::InternalValue>& b) {
    
//     if(a[2].intValue != b[2].intValue) 
//         return a[2].intValue < b[2].intValue;
//     else 
//         return a[1].intValue > b[1].intValue;
      
// }


hiactor::DataType select_top_k_on_shard(const hiactor::DataType& input, std::function<bool(hiactor::InternalValue, hiactor::InternalValue)> compare_func, int k) {

    // stopTiming();
    std::priority_queue<hiactor::InternalValue,
                        std::vector<hiactor::InternalValue>,
                        decltype(compare_func)> pq(compare_func);

    //used for map_partition, thus output vec<vec<...>> for input vec<vec<vec<...>>>
    std::vector<hiactor::InternalValue> vec = *((*input._data.vectorValue).back().vectorValue);

    for(const auto& innerVec: vec) {  //push each element(vector) into priority_queue
        pq.push(innerVec);
    }

    std::vector<hiactor::InternalValue> result;
    int count = 0;
    while(!pq.empty() && count < k) {
        result.push_back(pq.top());
        pq.pop();
        count ++;
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(result);
    return output;
}

unsigned top_hash_string(const hiactor::InternalValue& input) {

    // unsigned hash_value = 0;
    return 0;
}
