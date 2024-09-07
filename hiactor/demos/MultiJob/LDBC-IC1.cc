
// #include "Executor/Project.h"
#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
// #include "Executor/Expand.h"
#include "Executor/Expand_vec.h"
#include "Executor/ExpandInto.h"
#include "Executor/Add_distance.h"
#include "Executor/Top.h"
#include "Executor/Filter.h"
#include "Executor/Distinct.h"
#include "Executor/ReduceByKey.h"
#include "Executor/file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <thread>


long long person_ID = 1161;
std::string firstName = "Hans";

auto func_filter_by_personID = [](hiactor::InternalValue x){
    // std::cout<<"--------filter-------1\n";
    if((*x.vectorValue)[2].intValue != person_ID)
        return true;
    else
    {        
        return false;

    }
        
};

auto func_filter_by_firstName = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[3].stringValue , firstName.c_str()) == 0) 
    {
        // std::cout<<"True\n";
        return true;
    }        
    else    
        return false;
};


hiactor::InternalValue reduce_func(hiactor::InternalValue input) {

    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    std::unordered_map<long long, std::tuple<std::set<std::tuple<std::string, long long, std::string>>, std::set<std::tuple<std::string, long long, std::string>>>> storage;
    std::unordered_map<long long, std::vector<hiactor::InternalValue>> other_elements;
    for(unsigned i = 0; i < vec.size(); i++) {

        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        long long id = msg[1].intValue;
        std::string str_left_1 = msg[3].stringValue;
        long long str_left_2 = msg[4].intValue;
        std::string str_left_3 = msg[5].stringValue;
        std::string str_right_1 = msg[6].stringValue;
        long long str_right_2 = msg[7].intValue;
        std::string str_right_3 = msg[8].stringValue;

        if(storage.find(id) == storage.end()) {
            std::tuple<std::set<std::tuple<std::string, long long, std::string>>, std::set<std::tuple<std::string, long long, std::string>>> newvalue;
            std::set<std::tuple<std::string, long long, std::string>> set1;
            std::set<std::tuple<std::string, long long, std::string>> set2;
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
            for(unsigned j = 0; j <= 2; j++) {
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
        std::set<std::tuple<std::string, long long, std::string>> set1 = std::get<0>(tuple_value);
        for (const auto& tupleValue : set1) {
            std::string str1 = std::get<0>(tupleValue);
            long long str2 = std::get<1>(tupleValue);
            std::string str3 = std::get<2>(tupleValue);
            hiactor::InternalValue val1, val2, val3;
            val1.stringValue = new char[str1.length() + 1];
            strcpy(val1.stringValue, str1.c_str());
            
            val2.intValue = str2;

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
        std::set<std::tuple<std::string, long long, std::string>> set2 = std::get<1>(tuple_value);
        for (const auto& tupleValue : set2) {
            std::string str4 = std::get<0>(tupleValue);
            long long str5 = std::get<1>(tupleValue);
            std::string str6 = std::get<2>(tupleValue);
            hiactor::InternalValue val4, val5, val6;
            val4.stringValue = new char[str4.length() + 1];
            strcpy(val4.stringValue, str4.c_str());
            
            val5.intValue = str5;

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

    hiactor::InternalValue result;
    result.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
    return result;
}


bool compare(hiactor::InternalValue a, hiactor::InternalValue b) { 
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;

    std::string str1 = vec_a[4].stringValue;
    std::string str2 = vec_b[4].stringValue;
    if(vec_a[2].intValue != vec_b[2].intValue) {
        return vec_a[2].intValue > vec_b[2].intValue;
    } else if(str1 != str2) {
        return str1 > str2;
    } else if(vec_a[1].intValue != vec_b[1].intValue) {
        return vec_a[1].intValue > vec_b[1].intValue;
    } else {
        return false;
    }

}

 


void Run_IC1(int i)
{
    
    // std::cout<<"----------------thread_process-------------\n";
    ExecutorHandler _exe_hd(i);
    _exe_hd.nodeByIDScan(1161)
            .add_distance()
            .varExpand(3,_person_knows_person_, 2, 3, false)
            // .expand_vec(_person_knows_person_, 0, 2, false)
            // .expand_vec(_person_knows_person_, 3, 2, false)
            // .expand_vec(_person_knows_person_, 4, 2, false)
            .filter(func_filter_by_personID)
            .project_node({},{},{},{0,2,3})
            // .distinct()
            .project_node({1},{_person_},{{"firstName","lastName"}},{})
            .filter(func_filter_by_firstName)
            .top(compare, 20)
            .project_node({},{},{},{0,1,2})
            .expand_vec(_person_workAt_organisation_, 1, -1, true)
            .expand_vec(_person_studyAt_organisation_, 1, -1, true)
            .expand_vec(_organisation_isLocatedIn_place_, 3, -1, false) //get company countryID
            .expand_vec(_organisation_isLocatedIn_place_, 4, -1, false) //get University cityID
            .project_edge({1,1},{3,4},{_person_workAt_organisation_,_person_studyAt_organisation_},{{"workFrom"},{"classYear"}},{})
            .project_node({3,4,5,6},{_organisation_,_organisation_,_place_,_place_},{{"name"},{"name"},{"name"},{"name"}},{0,1,2,10,8,12,9,7,11})
            .reduceByKey(reduce_func, 1)
            .expand_vec(_person_isLocatedIn_place_, 1, -1, false) // person_place ID
            // .project_node({0},{_person_},{{"lastName"}},{})
            // .top()
            .project_node({5},{_place_},{{"name"}},{0,1,2,3,4,6})
            .project_node({1},{_person_},{{"lastName","birthday","creationDate","gender","browserUsed","locationIP","email","language"}},{})
            .fileSinkExe()
            .execute(); 

}

// void app_IC_1(int ac, char** av,int _index)
// {
//     hiactor::actor_app app;
//     app.run(ac, av, [_index]{
//         std::cout<<"app start"<<std::endl;
//         IC_1(_index);      
              
//     });
// }
seastar::future<> IC1(int i) {
    Run_IC1(i);

    return seastar::make_ready_future<>();
}


int main(int ac, char** av)
{
//    std::cin >> person_id;   // 772 933 9399 2191 8061
    // person_id = 772;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC1(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}

// 1129  142  144
// 143   224  209
// 1161  205  190

// 1-186
// 2-385
// 4-778
// 8-1536
// 16-2753



