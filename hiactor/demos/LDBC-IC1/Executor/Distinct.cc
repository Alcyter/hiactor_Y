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

// hiactor::DataType distinct_on_shard(const hiactor::DataType& input) {
//     std::vector<hiactor::InternalValue> _vec = *(input._data.vectorValue);
//     std::vector<hiactor::InternalValue> vec = *(_vec.back().vectorValue);
//     std::vector<hiactor::InternalValue> filtered;
//     for(const auto& pair : vec) {
//         bool isDuplicate = false;
//         for(const auto& existingVec : filtered) {
//             if(((*existingVec.vectorValue)[0]).intValue == ((*pair.vectorValue)[0]).intValue &&
//                ((*existingVec.vectorValue)[1]).intValue == ((*pair.vectorValue)[1]).intValue) {
//                 isDuplicate = true;
//                 break;
//             }
//         }
//         if(!isDuplicate) {
//             filtered.push_back(pair);
//         }
//     }
//     hiactor::DataType output;
//     output.type = hiactor::DataType::VECTOR;
//     output._data.vectorValue = new std::vector<hiactor::InternalValue>(filtered);
//     return output;
// }



// unsigned distinct_hash_string(const hiactor::InternalValue& input) {

//     unsigned hash_value = 0;
//     return hash_value;
// }

// unsigned distinct_shuffle_hash_string(const hiactor::InternalValue& input) {

//     std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
//     std::hash<std::string> hash_function;
//     unsigned hash_value = hash_function(std::to_string(input_vector[0].intValue));

//     return hash_value;
// }


// hiactor::DataType distinct_on_shard(const hiactor::DataType& input) {
//     // stopTiming();
//     std::vector<hiactor::InternalValue> vec = *((*(input._data.vectorValue)).back().vectorValue);
//     std::unordered_set<long long> uniqueSet;
//     std::vector<hiactor::InternalValue> filtered;
//     for(const auto& value : vec) {
//         if(uniqueSet.insert((*value.vectorValue)[0].intValue).second) {
//             filtered.push_back(value);
//         }
//     }
//     // stopTiming();
//     hiactor::DataType output;
//     output.type = hiactor::DataType::VECTOR;
//     output._data.vectorValue = new std::vector<hiactor::InternalValue>(filtered);
//     return output;
// }


// unsigned distinct_hash_string(const hiactor::InternalValue& input) {
//     return 0;
// }

// unsigned distinct_shuffle_hash_string(const hiactor::InternalValue& input) {
//     return (*input.vectorValue)[0].intValue;
// }




hiactor::DataType distinct_on_shard(const hiactor::DataType& input) {
    //stopTiming();
    std::vector<hiactor::InternalValue> vec = *((*(input._data.vectorValue)).back().vectorValue);
    std::unordered_set<long long> uniqueSet;
    std::vector<hiactor::InternalValue> filtered;
    for(const auto& value : vec) {
        if(uniqueSet.insert((*value.vectorValue)[0].intValue).second) {
            filtered.push_back(value);
        }
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(filtered);
    //stopTiming();
    return output;
}


unsigned distinct_hash_string(const hiactor::InternalValue& input) {
//    unsigned hash_value = 0;
    return (*input.vectorValue)[0].intValue;
}

unsigned distinct_shuffle_hash_string(const hiactor::InternalValue& input) {

//    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
//    std::hash<std::string> hash_function;
//    unsigned hash_value = hash_function(std::to_string(input_vector[0].intValue));

    return (*input.vectorValue)[0].intValue;
}