
#include "Executor/Project.h"
#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
#include "Executor/Expand.h"
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


void ExecutorTest()
{
    DataFlow df;
    std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC6/actor/node.txt";
    df.fromFile(filename);
    std::cout<<"have set filename"<<std::endl;

    NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

    Add_distance add_distance_exe; //dest dist

    Expand_vec expand_exe1(_person_knows_person_, 0, 2,false);    

    Expand_vec expand_exe2(_person_knows_person_, 3, 2,false);

    Filter filter_exe1("filter_by_personID");

    Project_node project_exe1({},{},{},{1,2});

    Distinct distince_exe;
    
    Project_node project_exe2({},{},{},{0});

    Expand_vec expand_exe3(_person_hasCreated_post_, 0, -1,false);

    Expand_vec expand_exe4(_post_hasTag_tag_, 1, -1, false);

    Project_node project_exe3({2},{_tag_},{{"name"}},{1,3});  //post_ID, tagname

    Filter filter_exe2("filter_by_tag_one");

    Expand_vec expand_exe5(_post_hasTag_tag_, 0, -1, false);

    Project_node project_exe4({2,2},{_tag_},{{"name"}},{0,3}); //post_ID, other_tagname

    Filter filter_exe3("filter_by_tag_two");// post_ID, other tagname

    ReduceByKey reduceByKey_exe; //tagname count

    Top top_exe;    

    FileSinkExe filesink_exe;



    nodeByIdScan_exe.setDf(std::move(df));

    nodeByIdScan_exe.setNext(&add_distance_exe);
    add_distance_exe.setNext(&expand_exe1);

    expand_exe1.setNext(&expand_exe2);
    expand_exe2.setNext(&filter_exe1);
    filter_exe1.setNext(&project_exe1);
    project_exe1.setNext(&distince_exe);
    distince_exe.setNext(&project_exe2);
    project_exe2.setNext(&expand_exe3);    
    expand_exe3.setNext(&expand_exe4);    
    expand_exe4.setNext(&project_exe3);      
    project_exe3.setNext(&filter_exe2);
    filter_exe2.setNext(&expand_exe5);
    expand_exe5.setNext(&project_exe4);
    project_exe4.setNext(&filter_exe3);
    filter_exe3.setNext(&reduceByKey_exe);    
    reduceByKey_exe.setNext(&top_exe);

    top_exe.setNext(&filesink_exe);


    nodeByIdScan_exe.process();

}

void ExecutorApiTest()
{
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    _exe_hd.nodeByIDScan(1161)
            .add_distance()
            .varExpand(2,_person_knows_person_, 1, 2, false)
            // .expand_vec(_person_knows_person_, 0, 2,false)    
            // .expand_vec(_person_knows_person_, 3, 2,false)
            .filter("filter_by_personID")
            // .project_node({},{},{},{1,2})
            // .distinct()    
            .project_node({},{},{},{1})
            // 13(4400)
            .expand_vec(_person_hasCreated_post_, 0, -1,false) 
            // 323 220 (530000)
            .expand_vec(_post_hasTag_tag_, 1, -1, false)
            // 589
            .project_node({2},{_tag_},{{"name"}},{1,3})
            .filter("filter_by_tag_one")
            // 951 (679)
            .expand_vec(_post_hasTag_tag_, 0, -1, false)
            .project_node({2},{_tag_},{{"name"}},{0,3})
            .filter("filter_by_tag_two")
            // 941 (400)
            .reduceByKey()
            .top()
            .fileSinkExe()   //1117
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
// 1129  64
// 143   70
// 1161  76

// 1129  1634  658       641
// 143   2097  2003      983 999
// 1161  1942  1911      902