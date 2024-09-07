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

bool compare(const std::vector<hiactor::InternalValue>& a, const std::vector<hiactor::InternalValue>& b) {
    
    long long Date1 = a[2].intValue;
    long long Date2 = b[2].intValue;
    if(Date1 != Date2) {
        return Date1 < Date2;
    } else 
        return a[0].intValue > b[0].intValue;
    
}
hiactor::DataType select_top_k_on_shard(const hiactor::DataType& input) {

    // std::cout<<"-------------top------------\n";
    // stopTiming();
    int k = 20;
    std::priority_queue<std::vector<hiactor::InternalValue>,
                        std::vector<std::vector<hiactor::InternalValue>>,
                        decltype(&compare)> pq(&compare);

    //used for map_partition, thus output vec<vec<...>> for input vec<vec<vec<...>>>
    std::vector<hiactor::InternalValue> vec = *((*input._data.vectorValue).back().vectorValue);

    for(const auto& innerVec: vec) {  //push each element(vector) into priority_queue
        pq.push(*(innerVec.vectorValue));
    }

    std::vector<hiactor::InternalValue> result;
    int count = 0;
    while(!pq.empty() && count < k) {
        hiactor::InternalValue value;
        value.vectorValue= new std::vector<hiactor::InternalValue>(pq.top());
        result.push_back(value);
        pq.pop();
        count ++;
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(result);
    return output;
}

// hiactor::DataType select_top_k(const hiactor::DataType& input) {

//     int k = 20;
//     // std::cout << "111111111111111111111111111111111111111111111111111111111111111111" << std::endl;
//     std::priority_queue<std::vector<hiactor::InternalValue>,
//             std::vector<std::vector<hiactor::InternalValue>>,
//             decltype(&compare)> pq(&compare);

//     //used for map, thus output vec<vec<...>> for input vec<vec<...>> is ok
//     std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);

//     for(const auto& innerVec: vec) {
//         std::vector<hiactor::InternalValue> temp = *(innerVec.vectorValue);
//         pq.push(temp);
//     }
//     std::vector<hiactor::InternalValue> result;
//     int count = 0;
//     while(!pq.empty() && count < k) {
//         std::vector<hiactor::InternalValue> topVec = pq.top();
//         hiactor::InternalValue value;
//         value.vectorValue= new std::vector<hiactor::InternalValue>(topVec);
//         //std::cout << (*value.vectorValue)[10].stringValue << std::endl;
//         result.push_back(value);
//         pq.pop();
//         count ++;
//     }
//     hiactor::DataType output;
//     output.type = hiactor::DataType::VECTOR;
//     output._data.vectorValue = new std::vector<hiactor::InternalValue>(result);
//     return output;
// }

unsigned top_hash_string(const hiactor::InternalValue& input) {

    // unsigned hash_value = 0;
    return 0;
}
