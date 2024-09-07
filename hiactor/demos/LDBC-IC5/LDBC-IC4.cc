
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
//     std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC4/actor/node.txt";
//     df.fromFile(filename);
//     std::cout<<"have set filename"<<std::endl;

//     NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

//     Expand_vec expand_exe1(_person_knows_person_, 0, -1,false);

//     Expand_vec expand_exe2(_person_hasCreated_post_, 1, -1,false);

//     Project_node project_exe1({2},{_post_},{{"creationDate"}},{2,3});  //post_ID, creationDate

//     Filter filter_exe1("filter_by_post_one");   

//     Expand_vec expand_exe3(_post_hasTag_tag_, 0, -1, false);

//     Project_node project_exe2({},{},{},{2,1});  //tag_ID, creationDate(post)

//     ReduceByKey reduceByKey_exe;  // tag_ID, creationDate(post), Count

//     Filter filter_exe2("filter_by_post_two"); 

//     Project_node project_exe3({0},{_tag_},{{"name"}},{3,2});

//     Top top_exe;

//     FileSinkExe filesink_exe;



//     nodeByIdScan_exe.setDf(std::move(df));

//     nodeByIdScan_exe.setNext(&expand_exe1);
//     expand_exe1.setNext(&expand_exe2);   
//     expand_exe2.setNext(&project_exe1);
//     project_exe1.setNext(&filter_exe1);
//     filter_exe1.setNext(&expand_exe3);    
//     expand_exe3.setNext(&project_exe2);      
//     project_exe2.setNext(&reduceByKey_exe);    
//     reduceByKey_exe.setNext(&filter_exe2);
//     filter_exe2.setNext(&project_exe3);
//     project_exe3.setNext(&top_exe);

//     top_exe.setNext(&filesink_exe);


//     nodeByIdScan_exe.process();

// }

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
    long long _creationDate = (*x.vectorValue)[1].intValue;

    if (_creationDate<creationDate_end_one)     return true;

    // if (_creationDate < endDate_int) return true;

    return false;
};

auto func_filter_by_post_two = [](hiactor::InternalValue x){
    // std::cout<< "filter two\n";
    
    // long long _creationDate = atoll((*x.vectorValue)[1].stringValue);
    long long _creationDate = (*x.vectorValue)[1].intValue;

    if (_creationDate>=creationDate_end_two)    return true;
 
    // if (_creationDate >= startDate_int)     return true;
    return false;
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;

    std::string str1 = vec_a[0].stringValue;
    std::string str2 = vec_b[0].stringValue;
    if(vec_a[1].intValue != vec_b[1].intValue) 
        return vec_a[1].intValue < vec_b[1].intValue;
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
        long long tag_ID = msg[0].intValue;        
        if (storage.find(tag_ID) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue ID;
            hiactor::InternalValue min_Date;
            hiactor::InternalValue Count;
            ID.intValue=tag_ID;

            min_Date.intValue=msg[1].intValue;            

            Count.intValue=1;
            
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
            if (_msg[1].intValue>msg[1].intValue)
            {
                _msg[1].intValue = msg[1].intValue;
                
            }           
            _msg[2].intValue = _msg[2].intValue + 1;
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



void IC_4()
{
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    // _exe_hd.nodeByIDScan(143)
    //         .expand_vec(_person_knows_person_, 0, -1,false)
    //         .expand_vec(_person_hasCreated_post_, 1, -1,false)
    //         .project_node({2},{_post_},{{"creationDate"}},{2,3})
    //         .filter(func_filter_by_post_one)
    //         .expand_vec(_post_hasTag_tag_, 0, -1, false)
    //         .project_node({},{},{},{2,1})
    //         .reduceByKey(reduce_func, 0)
    //         .filter(func_filter_by_post_two)
    //         .project_node({0},{_tag_},{{"name"}},{3,2})
    //         .top(compare, 20)
    //         .fileSinkExe()
    //         .execute();
    _exe_hd.nodeByIDScan(1161,"(a:person)")
            .expand_vec("(a:person)-[knows]->(b:person)",false)
            .expand_vec("(b:person)-[hasCreated]->(c:post)",false)
            .project_node("c.creationDate keep=c.id,c.creationDate")
            .filter(func_filter_by_post_one)
            .expand_vec("(c:post)-[hasTag]->(d:tag)",false)
            .project_node("keep=d.id,c.creationDate")
            .reduceByKey(reduce_func,"d.id","c.creationDate,c.count")
            .filter(func_filter_by_post_two)
            .project_node("d.name keep=d.name,c.count")
            .top("c.count=desc,d.name=asc", 20)
            .fileSinkExe()
            .execute();
}

seastar::future<> app_IC4() {
    IC_4();
    return seastar::make_ready_future<>();
}

int main(int ac, char** av)
{
    std::cout<<"begin"<<std::endl;
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
        for(int i = 0; i < 8; i++) {
            tasks.push_back(app_IC4());
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());

    });

}
// 1129  15
// 143   46  37
// 1161  53  40

// 8-75
// 16-77
// 32-96
// 64-65

