
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

void ExecutorTest()
{
    DataFlow df;
    std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC2/actor/node.txt";
    df.fromFile(filename);
    std::cout<<"have set filename"<<std::endl;

    NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

    Expand_vec expand_exe1(_person_knows_person_, 0, -1,false);

    Expand_vec expand_exe2(_person_hasCreated_post_, 1, -1,false);

    Project_node project_exe1({2},{_post_},{{"creationDate"}},{1,2,3});

    Filter filter_exe("IC-2");

    //ReduceByKey reduceByKey_exe;

    Project_node project_exe2({0,1},{_person_,_post_},{{"firstName","lastName"},{"content","imageFile"}},{0,3,4,1,5,6,2});

    Top top_exe;

    FileSinkExe filesink_exe; // filesink doesn't need barrier because top has synchronous semantics

    nodeByIdScan_exe.setDf(std::move(df));

    nodeByIdScan_exe.setNext(&expand_exe1);
    expand_exe1.setNext(&expand_exe2);    
    expand_exe2.setNext(&project_exe1);
    project_exe1.setNext(&filter_exe);   
    filter_exe.setNext(&project_exe2);    

    project_exe2.setNext(&top_exe);
    top_exe.setNext(&filesink_exe);


    nodeByIdScan_exe.process();

}

void Run_IC2(int i){

    ExecutorHandler _exe_hd(i);
    // ExecutorHandler _exe_hd;
    _exe_hd.nodeByIDScan(1161)
            .expand_vec(_person_knows_person_, 0, -1,false)
            .expand_vec(_person_hasCreated_post_, 1, -1,false)
            .project_node({2},{_post_},{{"creationDate"}},{1,2,3})
            .filter("IC-2")
            .top()
            .project_node({0,1},{_person_,_post_},{{"firstName","lastName"},{"content","imageFile"}},{0,3,4,1,5,6,2})
            .fileSinkExe()
            .execute();
}



void IC2(int i) {
    std::cout << "Query " << i << std::endl;
    Run_IC2(i);
}

int main(int ac, char** av)
{
    std::cout<<"begin"<<std::endl;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        int id = -1;       
        seastar::repeat([id]() mutable {
            id++;
            std::cout << id << "\n";
            if (id < 16) {
                IC2(id);
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
            }
            else
                return seastar::sleep(std::chrono::seconds(5)).then([]() mutable {                
                    return seastar::stop_iteration::yes;                    
                });
        }).then([] {
            std::cout << "Finished repeating" << std::endl;
            hiactor::actor_engine().exit();
        });       
    });

    return 0;
}
// 1161
// 1  - 18
// 2  - 38
// 4  - 76
// 8  - 177
// 16 - 311