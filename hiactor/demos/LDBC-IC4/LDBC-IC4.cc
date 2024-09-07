
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
    std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC4/actor/node.txt";
    df.fromFile(filename);
    std::cout<<"have set filename"<<std::endl;

    NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

    Expand_vec expand_exe1(_person_knows_person_, 0, -1,false);

    Expand_vec expand_exe2(_person_hasCreated_post_, 1, -1,false);

    Project_node project_exe1({2},{_post_},{{"creationDate"}},{2,3});  //post_ID, creationDate

    Filter filter_exe1("filter_by_post_one");   

    Expand_vec expand_exe3(_post_hasTag_tag_, 0, -1, false);

    Project_node project_exe2({},{},{},{2,1});  //tag_ID, creationDate(post)

    ReduceByKey reduceByKey_exe;  // tag_ID, creationDate(post), Count

    Filter filter_exe2("filter_by_post_two"); 

    Project_node project_exe3({0},{_tag_},{{"name"}},{3,2});

    Top top_exe;

    FileSinkExe filesink_exe;



    nodeByIdScan_exe.setDf(std::move(df));

    nodeByIdScan_exe.setNext(&expand_exe1);
    expand_exe1.setNext(&expand_exe2);   
    expand_exe2.setNext(&project_exe1);
    project_exe1.setNext(&filter_exe1);
    filter_exe1.setNext(&expand_exe3);    
    expand_exe3.setNext(&project_exe2);      
    project_exe2.setNext(&reduceByKey_exe);    
    reduceByKey_exe.setNext(&filter_exe2);
    filter_exe2.setNext(&project_exe3);
    project_exe3.setNext(&top_exe);

    top_exe.setNext(&filesink_exe);


    nodeByIdScan_exe.process();

}

void ExecutorApiTest()
{
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    _exe_hd.nodeByIDScan(1161)
            .expand_vec(_person_knows_person_, 0, -1,false)
            .expand_vec(_person_hasCreated_post_, 1, -1,false)
            .project_node({2},{_post_},{{"creationDate"}},{2,3})
            .filter("filter_by_post_one")
            .expand_vec(_post_hasTag_tag_, 0, -1, false)
            .project_node({},{},{},{2,1})
            .reduceByKey()
            .filter("filter_by_post_two")
            .project_node({0},{_tag_},{{"name"}},{3,2})
            .top()
            .fileSinkExe()
            .execute();
}

int main(int ac, char** av)
{
    std::cout<<"begin"<<std::endl;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        // ExecutorTest();
        ExecutorApiTest();
    });
}
// 1129  6
// 143   11
// 1161  14

// 1129  15
// 143   50
// 1161  53