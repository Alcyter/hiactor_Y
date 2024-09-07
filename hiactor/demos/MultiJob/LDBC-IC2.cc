
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
std::string maxDate ="1344152433150";
long long maxDate_int =  atoll(maxDate.c_str());

auto func = [](hiactor::InternalValue x){
    
    // long long _creationDate = atoll((*x.vectorValue)[2].stringValue);
    long long _creationDate = (*x.vectorValue)[3].intValue;
    if(_creationDate<=maxDate_int)
        return true;
    else
        return false;
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue; 
    // long long Date1 = atoll(vec_a[2].stringValue);
    // long long Date2 = atoll(vec_b[2].stringValue);
    if(vec_a[3].intValue != vec_b[3].intValue) {
        return vec_a[3].intValue < vec_b[3].intValue;
    } else 
        return vec_a[1].intValue > vec_b[1].intValue;
}

void Run_IC2(int i){
    // Graph_source* G =  Graph_source::GetInstance();

    ExecutorHandler _exe_hd(i);
    _exe_hd.nodeByIDScan(1161)
            .expand_vec(_person_knows_person_, 1, -1,false)
            .expand_vec(_person_hasCreated_message_, 2, -1,false)
            .project_node({3},{_message_},{{"creationDate"}},{0,2,3,4})
            .filter(func)
            .top(compare, 20)
            .project_node({1,2},{_person_,_message_},{{"firstName","lastName"},{"content","imageFile"}},{0,1,4,5,2,6,7,3})
            .fileSinkExe()
            .execute();
    // _exe_hd.nodeByIDScan(1161,"(a:person)")
    //         .expand_vec("(a:person)-[knows]->(b:person)",false)
    //         .expand_vec("(b:person)-[hasCreated]->(c:message)",false)
    //         .project_node("c.creationDate keep=b.id,c.id,c.creationDate")
    //         .filter(func)
    //         .top("c.creationDate=desc,c.id=asc", 20)
    //         .project_node("b.firstName,b.lastName,c.content,c.imageFile keep=b.id,b.firstName,b.lastName,c.id,c.content,c.imageFile,c.creationDate")
    //         .fileSinkExe()
    //         .execute();
    

}

seastar::future<> IC2(int i) {
    Run_IC2(i);

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
            tasks.push_back(IC2(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}

// 1129  4   26
// 143   21  88
// 1161  29  109

// 1-100
// 2-215
// 4-438
// 8-884
// 16-1785