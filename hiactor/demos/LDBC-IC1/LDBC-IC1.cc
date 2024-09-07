
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
    std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC1/actor/node.txt";
    df.fromFile(filename);
    std::cout<<"have set filename"<<std::endl;

    NodeByIDScan nodeByIdScan_exe(143);  //scan id = 1129

    Add_distance add_distance_exe;//dest dist

    // label , site , distancesite, is_option
    Expand_vec expand_exe1(_person_knows_person_, 0, 2, false); //expand this person one more hop by "know"

    Expand_vec expand_exe2(_person_knows_person_, 3, 2, false);

    Expand_vec expand_exe3(_person_knows_person_, 4, 2, false);

    Project_node project_exe1({},{},{},{1,2});

    Distinct distinct_exe;

    Project_node project_exe2({0},{_person_},{{"firstName"}},{});

    Filter filter_exe1("filter_by_firstName");

    Project_node project_exe3({},{},{},{0,1}); //personID, dist

    Expand_vec expand_exe4(_person_workAt_organisation_, 0, -1, true); // organisation ID
    Expand_vec expand_exe5(_person_studyAt_organisation_, 0, -1, true);  // organisation ID

    Expand_vec expand_exe6(_organisation_isLocatedIn_place_, 2, -1, false); //get company countryID
    Expand_vec expand_exe7(_organisation_isLocatedIn_place_, 3, -1, false); //get University cityID


    Project_edge project_exe4({0,0},{2,3},{_person_workAt_organisation_,_person_studyAt_organisation_},{{"workFrom"},{"classYear"}},{});

    Project_node project_exe5({2,3,4,5},{_organisation_,_organisation_,_place_,_place_},{{"name"},{"name"},{"name"},{"name"}},{0,1,9,7,11,8,6,10});
    
    ReduceByKey reduceByKey_exe;

    Expand_vec expand_exe8(_person_isLocatedIn_place_,0, -1, false);// person_place ID

    Project_node project_exe6({0},{_person_},{{"lastName"}},{});

    Top top_exe;

    Project_node project_exe7({4},{_place_},{{"name"}},{0,1,2,3,6});

    Project_node project_exe8({0},{_person_},{{"lastName","birthday","creationDate","gender","browserUsed","locationIP","email","language"}},{});

    FileSinkExe filesink_exe; // filesink doesn't need barrier because top has synchronous semantics


    nodeByIdScan_exe.setDf(std::move(df));

    nodeByIdScan_exe.setNext(&add_distance_exe);
    add_distance_exe.setNext(&expand_exe1);
    expand_exe1.setNext(&expand_exe2);    
    expand_exe2.setNext(&expand_exe3);
    expand_exe3.setNext(&project_exe1);
    project_exe1.setNext(&distinct_exe);
    distinct_exe.setNext(&project_exe2);
    project_exe2.setNext(&filter_exe1);
    filter_exe1.setNext(&project_exe3);
    project_exe3.setNext(&expand_exe4);

    expand_exe4.setNext(&expand_exe5);
    expand_exe5.setNext(&expand_exe6);
    expand_exe6.setNext(&expand_exe7);
    expand_exe7.setNext(&project_exe4);
    project_exe4.setNext(&project_exe5);
    project_exe5.setNext(&reduceByKey_exe);
    reduceByKey_exe.setNext(&expand_exe8);

    expand_exe8.setNext(&project_exe6);
    project_exe6.setNext(&top_exe);
    top_exe.setNext(&project_exe7);
    project_exe7.setNext(&project_exe8);
    project_exe8.setNext(&filesink_exe);

    nodeByIdScan_exe.process();

}

// void ExecutorApiTest()
// {
//     Graph_source* G =  Graph_source::GetInstance();

//     ExecutorHandler _exe_hd;
//     _exe_hd.nodeByIDScan(143)
//             .add_distance()
//             // .visit_Expand(_person_knows_person_, 0, 2, false)
//             // .visit_Expand(_person_knows_person_, 3, 2, false)
//             .varExpand(2,_person_knows_person_, 1, 2, false)
//             // .expand_vec(_person_knows_person_, 0, 2, false)
//             // .expand_vec(_person_knows_person_, 3, 2, false)           
//             .project_node({},{},{},{1,2})
//             // .distinct()
//             .top()
//             .fileSinkExe()
//             .execute(); 
// }

void ExecutorApiTest()
{
    Graph_source* G =  Graph_source::GetInstance();

    ExecutorHandler _exe_hd;
    _exe_hd.nodeByIDScan(1161)
            .add_distance()
            .varExpand(3,_person_knows_person_, 1, 2, false)
            // .expand_vec(_person_knows_person_, 0, 2, false)
            // .expand_vec(_person_knows_person_, 3, 2, false)
            // .expand_vec(_person_knows_person_, 4, 2, false)
            .project_node({},{},{},{1,2})
            // .distinct()
            .project_node({0},{_person_},{{"firstName","lastName"}},{})
            .filter("filter_by_firstName")
            .top()
            .project_node({},{},{},{0,1})
            .expand_vec(_person_workAt_organisation_, 0, -1, true)
            .expand_vec(_person_studyAt_organisation_, 0, -1, true)
            .expand_vec(_organisation_isLocatedIn_place_, 2, -1, false) //get company countryID
            .expand_vec(_organisation_isLocatedIn_place_, 3, -1, false) //get University cityID
            .project_edge({0,0},{2,3},{_person_workAt_organisation_,_person_studyAt_organisation_},{{"workFrom"},{"classYear"}},{})
            .project_node({2,3,4,5},{_organisation_,_organisation_,_place_,_place_},{{"name"},{"name"},{"name"},{"name"}},{0,1,9,7,11,8,6,10})
            .reduceByKey()
            .expand_vec(_person_isLocatedIn_place_,0, -1, false) // person_place ID
            .project_node({0},{_person_},{{"lastName"}},{})
            // .top()
            .project_node({4},{_place_},{{"name"}},{0,1,2,3,6})
            .project_node({0},{_person_},{{"lastName","birthday","creationDate","gender","browserUsed","locationIP","email","language"}},{})
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

// 1129  30
// 143   1005
// 1161  1008

// 1129  1018
// 143   2027  1009  190
// 1161  1022  1010  176
