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


hiactor::DataType reduce_function(const hiactor::DataType& input) {
    std::vector<hiactor::InternalValue> _vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> vec = *(_vec.back().vectorValue);
    std::vector<hiactor::InternalValue> reduced;
    std::unordered_map<long long, std::tuple<std::set<std::tuple<std::string, std::string, std::string>>, std::set<std::tuple<std::string, std::string, std::string>>>> storage;
    std::unordered_map<long long, std::vector<hiactor::InternalValue>> other_elements;
    for(unsigned i = 0; i < vec.size(); i++) {

        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        long long id = msg[0].intValue;
        std::string str_left_1 = msg[2].stringValue;
        std::string str_left_2 = msg[3].stringValue;
        std::string str_left_3 = msg[4].stringValue;
        std::string str_right_1 = msg[5].stringValue;
        std::string str_right_2 = msg[6].stringValue;
        std::string str_right_3 = msg[7].stringValue;

        if(storage.find(id) == storage.end()) {
            std::tuple<std::set<std::tuple<std::string, std::string, std::string>>, std::set<std::tuple<std::string, std::string, std::string>>> newvalue;
            std::set<std::tuple<std::string, std::string, std::string>> set1;
            std::set<std::tuple<std::string, std::string, std::string>> set2;
            set1.emplace(str_left_1, str_left_2, str_left_3);
            set2.emplace(str_right_1, str_right_2, str_right_3);
            std::get<0>(newvalue) = set1;
            std::get<1>(newvalue) = set2;
            storage.insert(std::make_pair(id, newvalue));
        } else {
            std::get<0>(storage[id]).emplace(str_left_1, str_left_2, str_left_3);
            std::get<1>(storage[id]).emplace(str_right_1, str_right_2, str_right_3);
        }

        if(other_elements.find(id) == other_elements.end()) {
            std::vector<hiactor::InternalValue> new_vec;
            for(unsigned j = 0; j <= 1; j++) {
                new_vec.push_back(msg[j]);
            }            
            other_elements[id] = new_vec;
        } else {
            continue;
        }
    }

    for(auto it = storage.begin(); it != storage.end(); ++it) {
        auto id_value = it -> first;
        auto tuple_value = it -> second;
        std::vector<hiactor::InternalValue> left_vec, right_vec;
        std::set<std::tuple<std::string, std::string, std::string>> set1 = std::get<0>(tuple_value);
        for (const auto& tupleValue : set1) {
            std::string str1 = std::get<0>(tupleValue);
            std::string str2 = std::get<1>(tupleValue);
            std::string str3 = std::get<2>(tupleValue);
            hiactor::InternalValue val1, val2, val3;
            val1.stringValue = new char[str1.length() + 1];
            strcpy(val1.stringValue, str1.c_str());
            val2.stringValue = new char[str2.length() + 1];
            strcpy(val2.stringValue, str2.c_str());
            val3.stringValue = new char[str3.length() + 1];
            strcpy(val3.stringValue, str3.c_str());
            std::vector<hiactor::InternalValue> vec1;
            vec1.push_back(val1);
            vec1.push_back(val2);
            vec1.push_back(val3);
            hiactor::InternalValue temp_val;
            temp_val.vectorValue = new std::vector<hiactor::InternalValue>(vec1);
            left_vec.push_back(temp_val);
        }
        std::set<std::tuple<std::string, std::string, std::string>> set2 = std::get<1>(tuple_value);
        for (const auto& tupleValue : set2) {
            std::string str4 = std::get<0>(tupleValue);
            std::string str5 = std::get<1>(tupleValue);
            std::string str6 = std::get<2>(tupleValue);
            hiactor::InternalValue val4, val5, val6;
            val4.stringValue = new char[str4.length() + 1];
            strcpy(val4.stringValue, str4.c_str());
            val5.stringValue = new char[str5.length() + 1];
            strcpy(val5.stringValue, str5.c_str());
            val6.stringValue = new char[str6.length() + 1];
            strcpy(val6.stringValue, str6.c_str());
            std::vector<hiactor::InternalValue> vec2;
            vec2.push_back(val4);
            vec2.push_back(val5);
            vec2.push_back(val6);
            hiactor::InternalValue temp_val;
            temp_val.vectorValue = new std::vector<hiactor::InternalValue>(vec2);
            right_vec.push_back(temp_val);
        }

        hiactor::InternalValue left_temp_value, right_temp_value;
        left_temp_value.vectorValue = new std::vector<hiactor::InternalValue>(left_vec);
        right_temp_value.vectorValue = new std::vector<hiactor::InternalValue>(right_vec);

        std::vector<hiactor::InternalValue> result = other_elements[id_value];
        result.push_back(left_temp_value);
        result.push_back(right_temp_value);

        hiactor::InternalValue internalValue;
        internalValue.vectorValue = new std::vector<hiactor::InternalValue>(result);
        reduced.push_back(internalValue);
    }

    hiactor::DataType output;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
    output.type = hiactor::DataType::VECTOR;

    return output;
}

unsigned key_shuffle_function(const hiactor::InternalValue& input) {
    std::vector<hiactor::InternalValue> input_vector = *input.vectorValue;
    std::hash<std::string> hash_function;
    unsigned hash_value = hash_function(std::to_string(input_vector[0].intValue));

    return hash_value;
}