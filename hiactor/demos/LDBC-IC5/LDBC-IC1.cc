
#include "Executor/Project.h"
#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
#include "Executor/Expand.h"
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

// void ExecutorTest()
// {
//     DataFlow df;
//     std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC1/actor/node.txt";
//     df.fromFile(filename);
//     std::cout<<"have set filename"<<std::endl;

//     NodeByIDScan nodeByIdScan_exe(143);  //scan id = 1129

//     Add_distance add_distance_exe;//dest dist

//     // label , site , distancesite, is_option
//     Expand_vec expand_exe1(_person_knows_person_, 0, 2, false); //expand this person one more hop by "know"

//     Expand_vec expand_exe2(_person_knows_person_, 3, 2, false);

//     Expand_vec expand_exe3(_person_knows_person_, 4, 2, false);

//     Project_node project_exe1({},{},{},{1,2});

//     Distinct distinct_exe;

//     Project_node project_exe2({0},{_person_},{{"firstName"}},{});

//     Filter filter_exe1("filter_by_firstName");

//     Project_node project_exe3({},{},{},{0,1}); //personID, dist

//     Expand_vec expand_exe4(_person_workAt_organisation_, 0, -1, true); // organisation ID
//     Expand_vec expand_exe5(_person_studyAt_organisation_, 0, -1, true);  // organisation ID

//     Expand_vec expand_exe6(_organisation_isLocatedIn_place_, 2, -1, false); //get company countryID
//     Expand_vec expand_exe7(_organisation_isLocatedIn_place_, 3, -1, false); //get University cityID


//     Project_edge project_exe4({0,0},{2,3},{_person_workAt_organisation_,_person_studyAt_organisation_},{{"workFrom"},{"classYear"}},{});

//     Project_node project_exe5({2,3,4,5},{_organisation_,_organisation_,_place_,_place_},{{"name"},{"name"},{"name"},{"name"}},{0,1,9,7,11,8,6,10});
    
//     ReduceByKey reduceByKey_exe;

//     Expand_vec expand_exe8(_person_isLocatedIn_place_,0, -1, false);// person_place ID

//     Project_node project_exe6({0},{_person_},{{"lastName"}},{});

//     Top top_exe;

//     Project_node project_exe7({4},{_place_},{{"name"}},{0,1,2,3,6});

//     Project_node project_exe8({0},{_person_},{{"lastName","birthday","creationDate","gender","browserUsed","locationIP","email","language"}},{});

//     FileSinkExe filesink_exe; // filesink doesn't need barrier because top has synchronous semantics


//     nodeByIdScan_exe.setDf(std::move(df));

//     nodeByIdScan_exe.setNext(&add_distance_exe);
//     add_distance_exe.setNext(&expand_exe1);
//     expand_exe1.setNext(&expand_exe2);    
//     expand_exe2.setNext(&expand_exe3);
//     expand_exe3.setNext(&project_exe1);
//     project_exe1.setNext(&distinct_exe);
//     distinct_exe.setNext(&project_exe2);
//     project_exe2.setNext(&filter_exe1);
//     filter_exe1.setNext(&project_exe3);
//     project_exe3.setNext(&expand_exe4);

//     expand_exe4.setNext(&expand_exe5);
//     expand_exe5.setNext(&expand_exe6);
//     expand_exe6.setNext(&expand_exe7);
//     expand_exe7.setNext(&project_exe4);
//     project_exe4.setNext(&project_exe5);
//     project_exe5.setNext(&reduceByKey_exe);
//     reduceByKey_exe.setNext(&expand_exe8);

//     expand_exe8.setNext(&project_exe6);
//     project_exe6.setNext(&top_exe);
//     top_exe.setNext(&project_exe7);
//     project_exe7.setNext(&project_exe8);
//     project_exe8.setNext(&filesink_exe);

//     nodeByIdScan_exe.process();

// }

long long person_ID = 1161;
std::string firstName = "Hans";

auto func_filter_by_personID = [](hiactor::InternalValue x){
    // std::cout<<"--------filter-------1\n";
    if((*x.vectorValue)[1].intValue != person_ID)
        return true;
    else
    {        
        return false;

    }
        
};

auto func_filter_by_firstName = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[2].stringValue , firstName.c_str()) == 0) 
        return true;
    else    
        return false;
};

// hiactor::InternalValue reduce_func(hiactor::InternalValue input) {

//     std::vector<hiactor::InternalValue> vec = *input.vectorValue;
//     std::vector<hiactor::InternalValue> reduced;
//     std::unordered_map<long long, std::tuple<std::set<std::tuple<std::string, std::string, std::string>>, std::set<std::tuple<std::string, std::string, std::string>>>> storage;
//     std::unordered_map<long long, std::vector<hiactor::InternalValue>> other_elements;
//     for(unsigned i = 0; i < vec.size(); i++) {

//         std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
//         long long id = msg[0].intValue;
//         std::string str_left_1 = msg[2].stringValue;
//         std::string str_left_2 = msg[3].stringValue;
//         std::string str_left_3 = msg[4].stringValue;
//         std::string str_right_1 = msg[5].stringValue;
//         std::string str_right_2 = msg[6].stringValue;
//         std::string str_right_3 = msg[7].stringValue;

//         if(storage.find(id) == storage.end()) {
//             std::tuple<std::set<std::tuple<std::string, std::string, std::string>>, std::set<std::tuple<std::string, std::string, std::string>>> newvalue;
//             std::set<std::tuple<std::string, std::string, std::string>> set1;
//             std::set<std::tuple<std::string, std::string, std::string>> set2;
//             set1.emplace(str_left_1, str_left_2, str_left_3);
//             set2.emplace(str_right_1, str_right_2, str_right_3);
//             std::get<0>(newvalue) = set1;
//             std::get<1>(newvalue) = set2;
//             storage.insert(std::make_pair(id, newvalue));
//         } else {
//             std::get<0>(storage[id]).emplace(str_left_1, str_left_2, str_left_3);
//             std::get<1>(storage[id]).emplace(str_right_1, str_right_2, str_right_3);
//         }

//         if(other_elements.find(id) == other_elements.end()) {
//             std::vector<hiactor::InternalValue> new_vec;
//             for(unsigned j = 0; j <= 1; j++) {
//                 new_vec.push_back(msg[j]);
//             }            
//             other_elements[id] = new_vec;
//         } else {
//             continue;
//         }
//     }

//     for(auto it = storage.begin(); it != storage.end(); ++it) {
//         auto id_value = it -> first;
//         auto tuple_value = it -> second;
//         std::vector<hiactor::InternalValue> left_vec, right_vec;
//         std::set<std::tuple<std::string, std::string, std::string>> set1 = std::get<0>(tuple_value);
//         for (const auto& tupleValue : set1) {
//             std::string str1 = std::get<0>(tupleValue);
//             std::string str2 = std::get<1>(tupleValue);
//             std::string str3 = std::get<2>(tupleValue);
//             hiactor::InternalValue val1, val2, val3;
//             val1.stringValue = new char[str1.length() + 1];
//             strcpy(val1.stringValue, str1.c_str());
//             val2.stringValue = new char[str2.length() + 1];
//             strcpy(val2.stringValue, str2.c_str());
//             val3.stringValue = new char[str3.length() + 1];
//             strcpy(val3.stringValue, str3.c_str());
//             std::vector<hiactor::InternalValue> vec1;
//             vec1.push_back(val1);
//             vec1.push_back(val2);
//             vec1.push_back(val3);
//             hiactor::InternalValue temp_val;
//             temp_val.vectorValue = new std::vector<hiactor::InternalValue>(vec1);
//             left_vec.push_back(temp_val);
//         }
//         std::set<std::tuple<std::string, std::string, std::string>> set2 = std::get<1>(tuple_value);
//         for (const auto& tupleValue : set2) {
//             std::string str4 = std::get<0>(tupleValue);
//             std::string str5 = std::get<1>(tupleValue);
//             std::string str6 = std::get<2>(tupleValue);
//             hiactor::InternalValue val4, val5, val6;
//             val4.stringValue = new char[str4.length() + 1];
//             strcpy(val4.stringValue, str4.c_str());
//             val5.stringValue = new char[str5.length() + 1];
//             strcpy(val5.stringValue, str5.c_str());
//             val6.stringValue = new char[str6.length() + 1];
//             strcpy(val6.stringValue, str6.c_str());
//             std::vector<hiactor::InternalValue> vec2;
//             vec2.push_back(val4);
//             vec2.push_back(val5);
//             vec2.push_back(val6);
//             hiactor::InternalValue temp_val;
//             temp_val.vectorValue = new std::vector<hiactor::InternalValue>(vec2);
//             right_vec.push_back(temp_val);
//         }

//         hiactor::InternalValue left_temp_value, right_temp_value;
//         left_temp_value.vectorValue = new std::vector<hiactor::InternalValue>(left_vec);
//         right_temp_value.vectorValue = new std::vector<hiactor::InternalValue>(right_vec);

//         std::vector<hiactor::InternalValue> result = other_elements[id_value];
//         result.push_back(left_temp_value);
//         result.push_back(right_temp_value);

//         hiactor::InternalValue internalValue;
//         internalValue.vectorValue = new std::vector<hiactor::InternalValue>(result);
//         reduced.push_back(internalValue);
//     }

//     hiactor::InternalValue result;
//     result.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
//     return result;
// }


hiactor::InternalValue reduce_func(hiactor::InternalValue input) {

    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    std::unordered_map<long long, std::tuple<std::set<std::tuple<std::string, long long, std::string>>, std::set<std::tuple<std::string, long long, std::string>>>> storage;
    std::unordered_map<long long, std::vector<hiactor::InternalValue>> other_elements;
    for(unsigned i = 0; i < vec.size(); i++) {

        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        long long id = msg[0].intValue;
        std::string str_left_1 = msg[2].stringValue;
        long long str_left_2 = msg[3].intValue;
        std::string str_left_3 = msg[4].stringValue;
        std::string str_right_1 = msg[5].stringValue;
        long long str_right_2 = msg[6].intValue;
        std::string str_right_3 = msg[7].stringValue;

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

    std::string str1 = vec_a[3].stringValue;
    std::string str2 = vec_b[3].stringValue;
    if(vec_a[1].intValue != vec_b[1].intValue) {
        return vec_a[1].intValue > vec_b[1].intValue;
    } else if(str1 != str2) {
        return str1 > str2;
    } else if(vec_a[0].intValue != vec_b[0].intValue) {
        return vec_a[0].intValue > vec_b[0].intValue;
    } else {
        return false;
    }

}

 


void IC_1()
{
    
    std::cout<<"----------------thread_process-------------\n";
    ExecutorHandler _exe_hd;
    // _exe_hd.nodeByIDScan(1129)
    //         .add_distance()
    //         .varExpand(3,_person_knows_person_, 1, 2, false)
    //         // .expand_vec(_person_knows_person_, 0, 2, false)
    //         // .expand_vec(_person_knows_person_, 3, 2, false)
    //         // .expand_vec(_person_knows_person_, 4, 2, false)
    //         .filter(func_filter_by_personID)
    //         .project_node({},{},{},{1,2})
    //         // .distinct()
    //         .project_node({0},{_person_},{{"firstName","lastName"}},{})
    //         .filter(func_filter_by_firstName)
    //         .top(compare, 20)
    //         .project_node({},{},{},{0,1})
    //         .expand_vec(_person_workAt_organisation_, 0, -1, true)
    //         .expand_vec(_person_studyAt_organisation_, 0, -1, true)
    //         .expand_vec(_organisation_isLocatedIn_place_, 2, -1, false) //get company countryID
    //         .expand_vec(_organisation_isLocatedIn_place_, 3, -1, false) //get University cityID
    //         .project_edge({0,0},{2,3},{_person_workAt_organisation_,_person_studyAt_organisation_},{{"workFrom"},{"classYear"}},{})
    //         .project_node({2,3,4,5},{_organisation_,_organisation_,_place_,_place_},{{"name"},{"name"},{"name"},{"name"}},{0,1,9,7,11,8,6,10})
    //         .reduceByKey(reduce_func, 0)
    //         .expand_vec(_person_isLocatedIn_place_,0, -1, false) // person_place ID
    //         // .project_node({0},{_person_},{{"lastName"}},{})
    //         // .top()
    //         .project_node({4},{_place_},{{"name"}},{0,1,2,3,5})
    //         .project_node({0},{_person_},{{"lastName","birthday","creationDate","gender","browserUsed","locationIP","email","language"}},{})
    //         .fileSinkExe()
    //         .execute(); 

    
    _exe_hd.nodeByIDScan(person_ID,"(a:person)")
            .add_distance("As distance")
            .varExpand(3,"(a:person)-[knows]->(b:person)",true)
            
            .filter(func_filter_by_personID)
            .project_node("keep=b.id,distance.distance")
            // .distinct()
            .project_node("b.firstName,b.lastName keep=all")
            .filter(func_filter_by_firstName)
            .top("distance.distance=asc,b.lastName=asc,b.id=asc", 20)
            .project_node("keep=b.id,distance.distance")

            .expand_vec("(b:person)-[c:studyAt]->(d:organisation)", true)           
            .expand_vec("(d:organisation)-[isLocatedIn]->(e:place)", false) //get University cityID
            .project_edge("c.classYear keep=all")
            .project_node("d.name,e.name keep=b.id,distance.distance,d.name,c.classYear,e.name")

            .expand_vec("(b:person)-[f:workAt]->(g:organisation)",true)
            .expand_vec("(g:organisation)-[isLocatedIn]->(h:place)",false) //get company countryID
            .project_edge("f.workFrom keep=all")
            .project_node("g.name,h.name keep=b.id,distance.distance,d.name,c.classYear,e.name,g.name,f.workFrom,h.name")    

            // .project_edge("person-[workAt.workFrom]->organisation,person-[studyAt.classYear]->organisation_two keep=all")
            // .project_node("organisation.name,organisation_two.name,place.name,place_two.name keep=person.id,distance.distance,organisation_two.name,(person-[studyAt]->organisation_two).classYear,place_two.name,organisation.name,(person-[workAt]->organisation).workFrom,place.name")
            .reduceByKey(reduce_func, "b.id","distance.distance,universities.name,companies.name")
            .expand_vec("(b:person)-[isLocatedIn]->(i:place)", false) // person_place ID
            // .project_node({0},{_person_},{{"lastName"}},{})
            // .top()
            .project_node("i.name keep=b.id,distance.distance,universities.name,companies.name,i.name") 
            .project_node("b.lastName,b.birthday,b.creationDate,b.gender,b.browserUsed,b.locationIP,b.email,b.language keep=all")
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

seastar::future<> app_IC1() {
    IC_1();
    return seastar::make_ready_future<>();
}




int main(int ac, char** av)
{
    // std::cout<<"begin"<<std::endl;
    // hiactor::actor_app app;
    // app.run(ac, av, []{
    //     std::cout<<"app start"<<std::endl;
    //     IC_1(1161);      
              
    // });
    person_ID = 1161;
    firstName = "Hans";
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 1; i++) {
            tasks.push_back(app_IC1());
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());

    });
      
    return 0;
}

// 1129  142  144
// 143   224  209
// 1161  205  190

// 1 168
// 8 163
// 16 170
// 32 175
// 64 186


