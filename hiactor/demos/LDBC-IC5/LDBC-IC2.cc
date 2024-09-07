
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

// void ExecutorTest()
// {
//     DataFlow df;
//     std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC2/actor/node.txt";
//     df.fromFile(filename);
//     std::cout<<"have set filename"<<std::endl;

//     NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

//     Expand_vec expand_exe1(_person_knows_person_, 0, -1,false);

//     Expand_vec expand_exe2(_person_hasCreated_post_, 1, -1,false);

//     Project_node project_exe1({2},{_post_},{{"creationDate"}},{1,2,3});

//     Filter filter_exe("IC-2");

//     //ReduceByKey reduceByKey_exe;

//     Project_node project_exe2({0,1},{_person_,_post_},{{"firstName","lastName"},{"content","imageFile"}},{0,3,4,1,5,6,2});

//     Top top_exe;

//     FileSinkExe filesink_exe; // filesink doesn't need barrier because top has synchronous semantics

//     nodeByIdScan_exe.setDf(std::move(df));

//     nodeByIdScan_exe.setNext(&expand_exe1);
//     expand_exe1.setNext(&expand_exe2);    
//     expand_exe2.setNext(&project_exe1);
//     project_exe1.setNext(&filter_exe);   
//     filter_exe.setNext(&project_exe2);    

//     project_exe2.setNext(&top_exe);
//     top_exe.setNext(&filesink_exe);


//     nodeByIdScan_exe.process();

// }

long long person_ID = 1161;
std::string maxDate ="1344152433150";
long long maxDate_int =  atoll(maxDate.c_str());

auto func = [](hiactor::InternalValue x){
    
    // long long _creationDate = atoll((*x.vectorValue)[2].stringValue);
    long long _creationDate = (*x.vectorValue)[2].intValue;
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
    if(vec_a[2].intValue != vec_b[2].intValue) {
        return vec_a[2].intValue < vec_b[2].intValue;
    } else 
        return vec_a[0].intValue > vec_b[0].intValue;
}

void IC_2(){
    Graph_source* G =  Graph_source::GetInstance();

    ExecutorHandler _exe_hd;
    // _exe_hd.nodeByIDScan(1161)
    //         .expand_vec(_person_knows_person_, 0, -1,false)
    //         .expand_vec(_person_hasCreated_message_, 1, -1,false)
    //         .project_node({2},{_message_},{{"creationDate"}},{1,2,3})
    //         .filter(func)
    //         .top(compare, 20)
    //         .project_node({0,1},{_person_,_message_},{{"firstName","lastName"},{"content","imageFile"}},{0,3,4,1,5,6,2})
    //         .fileSinkExe()
    //         .execute();
    _exe_hd.nodeByIDScan(1161,"(a:person)")
            .expand_vec("(a:person)-[knows]->(b:person)",false)
            .expand_vec("(b:person)-[hasCreated]->(c:message)",false)
            .project_node("c.creationDate keep=b.id,c.id,c.creationDate")
            .filter(func)
            .top("c.creationDate=desc,c.id=asc", 20)
            .project_node("b.firstName,b.lastName,c.content,c.imageFile keep=b.id,b.firstName,b.lastName,c.id,c.content,c.imageFile,c.creationDate")
            .fileSinkExe()
            .execute();
    

}

seastar::future<> app_IC2() {
    IC_2();
    return seastar::make_ready_future<>();
}


int main(int ac, char** av)
{
    // std::cout<<"begin"<<std::endl;
    // hiactor::actor_app app;
    // app.run(ac, av, []{
    //     std::cout<<"app start"<<std::endl;
    //     // ExecutorTest();
    //     ExecutorApiTest();
    // });
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 1; i++) {
            tasks.push_back(app_IC2());
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());

    });
      
    return 0;

}
// 1129  4   26
// 143   21  88
// 1161  29  109

// 1 108
// 8  803
// 16-1601
// 32-3276
// 64-3355