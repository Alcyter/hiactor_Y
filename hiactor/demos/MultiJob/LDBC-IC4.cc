
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


long long person_ID = 1161;
std::string creationDate_one ="1324152433150";
long long creationDate_end_one =  atoll(creationDate_one.c_str());
std::string creationDate_two ="1273416487305";
long long creationDate_end_two =  atoll(creationDate_two.c_str());

std::string startDate = "1273363200000";
long long startDate_int = atoll(startDate.c_str());
long long duartionDays = 587; //1324080000000
long long endDate_int = startDate_int + duartionDays*24*3600*1000;


auto func_filter_by_post_one = [](hiactor::InternalValue x){
    // std::cout<< "filter one\n";
    
    // long long _creationDate = atoll((*x.vectorValue)[1].stringValue);
    long long _creationDate = (*x.vectorValue)[2].intValue;

    if (_creationDate<creationDate_end_one)     return true;

    // if (_creationDate < endDate_int) return true;

    return false;
};

auto func_filter_by_post_two = [](hiactor::InternalValue x){
    // std::cout<< "filter two\n";
    
    // long long _creationDate = atoll((*x.vectorValue)[1].stringValue);
    long long _creationDate = (*x.vectorValue)[2].intValue;

    if (_creationDate>=creationDate_end_two)    return true;
 
    // if (_creationDate >= startDate_int)     return true;
    return false;
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;

    std::string str1 = vec_a[1].stringValue;
    std::string str2 = vec_b[1].stringValue;
    if(vec_a[2].intValue != vec_b[2].intValue) 
        return vec_a[2].intValue < vec_b[2].intValue;
    else 
        return str1 > str2;
    
}


hiactor::InternalValue reduce_func(hiactor::InternalValue input) {
    // std::cout<<"reduce \n";
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;

    for(unsigned i = 0; i < vec.size(); i++) {        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        long long tag_ID = msg[1].intValue;        
        if (storage.find(tag_ID) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue ID;
            hiactor::InternalValue min_Date;
            hiactor::InternalValue Count;
            ID.intValue=tag_ID;

            min_Date.intValue=msg[2].intValue;            

            Count.intValue=1;
            
            _msg.push_back(msg[0]);
            _msg.push_back(ID);
            _msg.push_back(min_Date);
            _msg.push_back(Count);            
            storage[tag_ID] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[tag_ID];
            // long long Date_one = atoll(_msg[1].stringValue);
            // long long Date_two = atoll(msg[1].stringValue);
            if (_msg[2].intValue>msg[2].intValue)
            {
                _msg[2].intValue = msg[2].intValue;
                
            }           
            _msg[3].intValue = _msg[3].intValue + 1;
            storage[tag_ID] = _msg;
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



void Run_IC4(int i)
{
    // Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd(i);
    _exe_hd.nodeByIDScan(1161)
            .expand_vec(_person_knows_person_, 1, -1,false)
            .expand_vec(_person_hasCreated_post_, 2, -1,false)
            .project_node({3},{_post_},{{"creationDate"}},{0,3,4})
            .filter(func_filter_by_post_one)
            .expand_vec(_post_hasTag_tag_, 1, -1, false)
            .project_node({},{},{},{0,3,2})
            .reduceByKey(reduce_func, 1)
            .filter(func_filter_by_post_two)
            .project_node({1},{_tag_},{{"name"}},{0,4,3})
            .top(compare, 20)
            .fileSinkExe()
            .execute();
    // _exe_hd.nodeByIDScan(1161,"(a:person)")
    //         .expand_vec("(a:person)-[knows]->(b:person)",false)
    //         .expand_vec("(b:person)-[hasCreated]->(c:post)",false)
    //         .project_node("c.creationDate keep=c.id,c.creationDate")
    //         .filter(func_filter_by_post_one)
    //         .expand_vec("(c:post)-[hasTag]->(d:tag)",false)
    //         .project_node("keep=d.id,c.creationDate")
    //         .reduceByKey(reduce_func,"d.id","c.creationDate,c.count")
    //         .filter(func_filter_by_post_two)
    //         .project_node("d.name keep=d.name,c.count")
    //         .top("c.count=desc,d.name=asc", 20)
    //         .fileSinkExe()
    //         .execute();
}

seastar::future<> IC4(int i) {
    Run_IC4(i);

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
            tasks.push_back(IC4(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}
// 1129  15
// 143   46  37
// 1161  53  40

// 1-38
// 2-79
// 4-150
// 8-316
// 16-649

