
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
    std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC3/actor/node.txt";
    Graph_source* G =  Graph_source::GetInstance();
    df.fromFile(filename);
    std::cout<<"have set filename"<<std::endl;

    NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

    Add_distance add_distance_exe; //dest dist

    Expand_vec expand_exe1(_person_knows_person_, 0, 2,false);

    Expand_vec expand_exe2(_person_knows_person_, 3, 2,false);

    Filter filter_exe("filter_by_personID");

    Project_node project_exe1({},{},{},{1,2});

    Distinct distince_exe;
    
    Project_node project_exe2({},{},{},{0});

    Expand_vec expand_exe3(_person_isLocatedIn_place_, 0, -1, false);

    Expand_vec expand_exe4(_place_isPartOf_place_, 1, -1, false);

    Project_node project_exe3({2},{_place_},{{"name"}},{0,3});  //personID , person_country

    Filter filter_exe1("filter_by_personCountry");

    Expand_vec expand_exe5(_person_hasCreated_post_, 0, -1,false);

    Expand_vec expand_exe6(_post_isLocatedIn_place_, 2, -1,false);

    Project_node project_exe4({3,2},{_place_,_post_},{{"name"},{"creationDate"}},{0,4,5}); //personID, post_country, creationDate

    Filter filter_exe2("filter_by_post");    

    ReduceByKey reduceByKey_exe;  // personID, xCount, yCount, Count

    Project_node project_exe5({0},{_person_},{{"firstName","lastName"}},{0,4,5,1,2,3});

    Top top_exe;

    FileSinkExe filesink_exe; // filesink doesn't need barrier because top has synchronous semantics


    nodeByIdScan_exe.setDf(std::move(df));

    nodeByIdScan_exe.setNext(&add_distance_exe);
    add_distance_exe.setNext(&expand_exe1);
    expand_exe1.setNext(&expand_exe2);   
    expand_exe2.setNext(&filter_exe);  
    filter_exe.setNext(&project_exe1);
    project_exe1.setNext(&distince_exe);
    distince_exe.setNext(&project_exe2);
    project_exe2.setNext(&expand_exe3);
    expand_exe3.setNext(&expand_exe4); 
   
    expand_exe4.setNext(&project_exe3);
    project_exe3.setNext(&filter_exe1);   
   

    filter_exe1.setNext(&expand_exe5);   
     
    
    expand_exe5.setNext(&expand_exe6);  
    expand_exe6.setNext(&project_exe4);  
    
    project_exe4.setNext(&filter_exe2);
    filter_exe2.setNext(&reduceByKey_exe); 
     
    reduceByKey_exe.setNext(&project_exe5);
   
    project_exe5.setNext(&top_exe);

    top_exe.setNext(&filesink_exe);


    nodeByIdScan_exe.process();

}

void ExecutorApiTest(){
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    _exe_hd.nodeByIDScan(1161)
            .add_distance()
            .varExpand(2,_person_knows_person_, 1, 2, false)
            // // .expand_vec(_person_knows_person_, 0, 2,false)
            // // .expand_vec(_person_knows_person_, 3, 2,false)
            .filter("filter_by_personID")
            // // .project_node({},{},{},{1,2})
            // // .distinct()
            // 13 (4400)
            .project_node({},{},{},{1})
            .expand_vec(_person_isLocatedIn_place_, 0, -1, false)
            .expand_vec(_place_isPartOf_place_, 1, -1, false)
            .project_node({2},{_place_},{{"name"}},{0,3})
            .filter("filter_by_personCountry")
            // 31 (4000)
            .expand_vec(_person_hasCreated_post_, 0, -1,false)
            .project_node({2},{_post_},{{"creationDate"}},{0,2,3})
            .filter("filter_by_post_time")
            // 668 （440000）
            .expand_vec(_post_isLocatedIn_place_, 1, -1,false)
            .project_node({3},{_place_},{{"name"}},{0,4,1,3})
            .filter("filter_by_post_place")
            // 1258 (300)
            .reduceByKey()         // reduceByKey_AD()
            .top()
            .project_node({0},{_person_},{{"firstName","lastName"}},{0,4,5,1,2,3})            
            .fileSinkExe()   //1280
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
// 1129  138
// 143   220
// 1161  154

// 1129  1879
// 143   1430  2259    1240
// 1161  2165  1170


// 1161  1280