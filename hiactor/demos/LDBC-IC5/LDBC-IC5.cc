
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
#include "Executor/ExpandInto.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

// void ExecutorTest()
// {
//     DataFlow df;
//     std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC5/actor/node.txt";
//     df.fromFile(filename);
//     std::cout<<"have set filename"<<std::endl;

//     NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

//     Add_distance add_distance_exe; //dest dist

//     Expand_vec expand_exe1(_person_knows_person_, 0, 2,false);

//     Expand_vec expand_exe2(_person_knows_person_, 3, 2,false);

//     Filter filter_exe1("filter_by_personID");

//     Project_node project_exe1({},{},{},{1,2});

//     Distinct distince_exe;
    
//     Project_node project_exe2({},{},{},{0});

//     Expand_vec expand_exe3(_person_isMemberOf_forum_, 0, -1, false);

//     Project_edge project_exe3({0},{1},{_person_isMemberOf_forum_},{{"joinDate"}},{});

//     Filter filter_exe2("filter_by_joinDate");

//     Expand_vec expand_exe4(_forum_containerOf_post_, 1, -1, false);

//     ExpandInto expandinto_exe1(_person_hasCreated_post_, 0, 3);  //personID , forumID , joinDate, postID

//     ReduceByKey reduceByKey_exe;  // personID, forumID, postCount

//     Project_node project_exe4({},{},{},{1,2});

//     Top top_exe;

//     Project_node project_exe5({0},{_forum_},{{"title"}},{2,1});

//     FileSinkExe filesink_exe;



//     nodeByIdScan_exe.setDf(std::move(df));

//     nodeByIdScan_exe.setNext(&add_distance_exe);
//     add_distance_exe.setNext(&expand_exe1);
//     expand_exe1.setNext(&expand_exe2);   
//     expand_exe2.setNext(&filter_exe1);
//     filter_exe1.setNext(&project_exe1); 
//     project_exe1.setNext(&distince_exe);
//     distince_exe.setNext(&project_exe2);
//     project_exe2.setNext(&expand_exe3);     
//     expand_exe3.setNext(&project_exe3);      
//     project_exe3.setNext(&filter_exe2);
//     filter_exe2.setNext(&expand_exe4);
//     expand_exe4.setNext(&expandinto_exe1);
//     // expand_exe4.setNext(&reduceByKey_exe);
//     expandinto_exe1.setNext(&reduceByKey_exe);    
//     reduceByKey_exe.setNext(&project_exe4);
//     project_exe4.setNext(&top_exe);
//     top_exe.setNext(&project_exe5);
//     project_exe5.setNext(&filesink_exe);
//     nodeByIdScan_exe.process();

// }

long long person_ID = 1161;
std::string minDate ="1289805839104";
long long minDate_int =  atoll(minDate.c_str());


auto func = [](hiactor::InternalValue x){
    if((*x.vectorValue)[1].intValue != person_ID)
        return true;
    else
        return false;
};

auto func_filter_by_joinDate = [](hiactor::InternalValue x){
    // std::cout<< x.intValue <<std::endl;
    long long _joinDate = x.intValue;
        
    if(minDate_int <= _joinDate) return true;

    return false;
};


bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;

    if(vec_a[2].intValue != vec_b[2].intValue) 
        return vec_a[2].intValue < vec_b[2].intValue;
    else 
        return vec_a[1].intValue > vec_b[1].intValue;
      
}

hiactor::InternalValue reduce_func(hiactor::InternalValue input) {
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    // std::unordered_map<edge_tuple, std::vector<hiactor::InternalValue> ,myHash<edge_tuple>,myEqual<edge_tuple>> storage;

    std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;


    for(unsigned i = 0; i < vec.size(); i++) {        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;        
        long long person_ID = msg[0].intValue;
        long long forum_ID = msg[1].intValue;
        // edge_tuple person_with_forum(person_ID,forum_ID);        
        if (storage.find(forum_ID) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue _person_ID;
            hiactor::InternalValue _forum_ID;
            hiactor::InternalValue post_count;
            
            _person_ID.intValue = person_ID;
            _forum_ID.intValue = forum_ID;
            post_count.intValue = 1;
            

            _msg.push_back(_person_ID);  
            _msg.push_back(_forum_ID);                      
            _msg.push_back(post_count);            
            
            storage[forum_ID] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[forum_ID];                       
            _msg[2].intValue = _msg[2].intValue + 1;
            storage[forum_ID] = _msg;
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



void IC_5()
{
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    _exe_hd.nodeByIDScan(143,"(a:person)")
            .add_distance("As distance")
            .varExpand(2,"(a:person)-[knows]->(b:person)",true)
            
            // .expand_vec(_person_knows_person_, 0, 2,false)
            // .expand_vec(_person_knows_person_, 3, 2,false)
            .filter(func)
            // .project_node({},{},{},{1,2})
            // .distinct()
            .project_node("keep=b.id")
            .expand_IC5(func_filter_by_joinDate)
            // .expand_vec(_person_isMemberOf_forum_, 0, -1, false)
            // .project_edge({0},{1},{_person_isMemberOf_forum_},{{"joinDate"}},{})
            // .filter("filter_by_joinDate")
            
            // .expand_vec(_forum_containerOf_post_, 1, -1, false)
            // .expandInto(_person_hasCreated_post_, 0, 3)
            .reduceByKey(reduce_func, "b.id","c.id,d.count")
            .top("d.count=desc,c.id=asc", 20)
            .project_node("c.title keep=all")
            .fileSinkExe()
            .execute();
}

seastar::future<> app_IC5() {
    IC_5();
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
        for(int i = 0; i < 16; i++) {
            tasks.push_back(app_IC5());
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());

    });
}

// 1129  72268
// 143   80513
// 1161  106554 86229 85904
// 只到filter  256
// 有post，无expand into、reduce  91268 90511
// 有expand into， 无reduce   85490 
// 86426
// 


// 1161  person(235+216+207+212=870)   16
// 1161  person and forum (25292+25471+26477+25197=102437)   262
// 1161  person and forum and post (328617+325681+325425+331366=1311089)  93584
// 1161  expandinto (235+257+290+275=1057) 90048


//1161 288 205
//143  317 219
//1129 200


// 143  8-205
// 143  16-209
// 143  32-208
// 143  64-207


