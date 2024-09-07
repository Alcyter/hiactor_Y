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
// #include "Graph_source/Graph_source.h"

// hiactor::DataType reduce_function(const hiactor::DataType& input) {
//     std::cout<<"------------------this is  reduce_function\n";
//     std::vector<hiactor::InternalValue> _vec = *(input._data.vectorValue);
//     std::vector<hiactor::InternalValue> vec = *(_vec.back().vectorValue);
//     std::vector<hiactor::InternalValue> reduced;
    
    
//     // std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;
//     std::unordered_map<edge_tuple, std::vector<hiactor::InternalValue> ,myHash<edge_tuple>,myEqual<edge_tuple>> storage;

//     for(unsigned i = 0; i < vec.size(); i++) {
        
//         std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        
//         long long person_ID = msg[0].intValue;
//         long long forum_ID = msg[1].intValue;
//         edge_tuple person_with_forum(person_ID,forum_ID);
        
//         if (storage.find(person_with_forum) == storage.end())
//         {
//             std::vector<hiactor::InternalValue> _msg;
//             hiactor::InternalValue _person_ID;
//             hiactor::InternalValue _forum_ID;
//             hiactor::InternalValue post_count;
            
//             _person_ID.intValue = person_ID;
//             _forum_ID.intValue = forum_ID;
//             post_count.intValue = 1;
            
//             _msg.push_back(_person_ID);
//             _msg.push_back(_forum_ID);
//             _msg.push_back(post_count);            
            
//             storage[person_with_forum] = _msg;
//         }
//         else
//         {            
//             std::vector<hiactor::InternalValue> _msg=storage[person_with_forum];
                       
//             _msg[2].intValue = _msg[2].intValue + 1;

//             storage[person_with_forum] = _msg;
//         }
//     }

//     for(auto it = storage.begin(); it != storage.end(); ++it){
//         hiactor::InternalValue _node;
//         _node.vectorValue = new std::vector<hiactor::InternalValue> (it->second);
//         reduced.push_back(_node);        
//     }   

//     hiactor::DataType output;
//     output._data.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
//     output.type = hiactor::DataType::VECTOR;

//     return output;
// }


hiactor::DataType reduce_function(const hiactor::DataType& input, std::function<hiactor::InternalValue(hiactor::InternalValue)> reduce_func) {
    // stopTiming();
    // std::cout<<"here??\n";
    std::vector<hiactor::InternalValue> _vec = *(input._data.vectorValue);
    hiactor::InternalValue vec = _vec.back();

    hiactor::InternalValue reduced = reduce_func(vec);

    hiactor::DataType output;
    output._data = reduced;
    output.type = hiactor::DataType::VECTOR;
    // stopTiming();
    return output;
}


unsigned key_shuffle_function(const hiactor::InternalValue& input, int key_site) {
//    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
//    std::hash<std::string> hash_function;
//    unsigned hash_value = hash_function(std::to_string(input_vector[key_site].intValue));
//
//    return hash_value;
//
    return (*input.vectorValue)[key_site].intValue;
}