
// #include "Executor/Project.h"
#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
// #include "Executor/Expand.h"
#include "Executor/Expand_vec.h"
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


long long person_ID = 143;
std::string tagName = "Evo_Morales";

auto func_filter_by_personID = [](hiactor::InternalValue x){
    if((*x.vectorValue)[2].intValue != person_ID)
        return true;
    else
        return false;
};

auto func_filter_by_tag_one = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[2].stringValue,tagName.c_str()) == 0)
        return true;
    else
        return false;
};

auto func_filter_by_tag_two = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[2].stringValue,tagName.c_str()) != 0)
        return true;
    else
        return false;
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;
    std::string str1 = vec_a[3].stringValue;
    std::string str2 = vec_b[3].stringValue;
    if(vec_a[2].intValue != vec_b[2].intValue) 
        return vec_a[2].intValue < vec_b[2].intValue;
    else 
        return str1 > str2;
}


hiactor::InternalValue reduce_func(hiactor::InternalValue input) {
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    
    std::unordered_map<std::string , std::vector<hiactor::InternalValue> > storage;
    for(unsigned i = 0; i < vec.size(); i++) {        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        std::string tagName = msg[2].stringValue;
        
        if (storage.find(tagName) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            _msg.push_back(msg[0]);
            hiactor::InternalValue _tagid;
            hiactor::InternalValue _tagName;
            hiactor::InternalValue Count; 
            _tagid.intValue = msg[1].intValue;
            _msg.push_back(_tagid);      
            _tagName.stringValue= new char[strlen(msg[2].stringValue)+1];
            strcpy(_tagName.stringValue,msg[2].stringValue);
            Count.intValue=1; 
            _msg.push_back(Count);             
            _msg.push_back(_tagName);                      
            storage[tagName] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[tagName];         
            _msg[2].intValue = _msg[2].intValue + 1;
            storage[tagName] = _msg;
        }
    }

    for(auto it = storage.begin(); it != storage.end(); ++it){
        hiactor::InternalValue _node;
        _node.vectorValue = new std::vector<hiactor::InternalValue> (it->second);
        reduced.push_back(_node);        
    }

    hiactor::InternalValue result;
    result.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
    return result;
}



void Run_IC6(int i)
{
    // Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd(i);
    _exe_hd.nodeByIDScan(143)
            .add_distance()
            .varExpand(2,_person_knows_person_, 2, 3, false)
            
            .filter(func_filter_by_personID)
  
            .project_node({},{},{},{0,2})
            .expand_vec(_person_hasCreated_post_, 1, -1,false) 
            .expand_vec(_post_hasTag_tag_, 2, -1, false)
 
            .project_node({3},{_tag_},{{"name"}},{0,2,4})
            .filter(func_filter_by_tag_one)
            // 951 (679)
            .expand_vec(_post_hasTag_tag_, 1, -1, false)
            .project_node({3},{_tag_},{{"name"}},{0,3,4})
            .filter(func_filter_by_tag_two)
            // 941 (400)
            .reduceByKey(reduce_func, 1)
            .top(compare, 20)
            .fileSinkExe()   //1117
            .execute();

}

seastar::future<> IC6(int i) {
    Run_IC6(i);

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
            tasks.push_back(IC6(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}
// 1129  713
// 143   1012 979
// 1161  938  889

// 143 1-854
// 143 2-1664
// 143 4-3156
// 143 8-6105
// 143 16-11849



