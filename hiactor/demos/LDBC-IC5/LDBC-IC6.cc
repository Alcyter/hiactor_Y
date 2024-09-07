
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


// void ExecutorTest()
// {
//     DataFlow df;
//     std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC6/actor/node.txt";
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

//     Expand_vec expand_exe3(_person_hasCreated_post_, 0, -1,false);

//     Expand_vec expand_exe4(_post_hasTag_tag_, 1, -1, false);

//     Project_node project_exe3({2},{_tag_},{{"name"}},{1,3});  //post_ID, tagname

//     Filter filter_exe2("filter_by_tag_one");

//     Expand_vec expand_exe5(_post_hasTag_tag_, 0, -1, false);

//     Project_node project_exe4({2,2},{_tag_},{{"name"}},{0,3}); //post_ID, other_tagname

//     Filter filter_exe3("filter_by_tag_two");// post_ID, other tagname

//     ReduceByKey reduceByKey_exe; //tagname count

//     Top top_exe;    

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
//     expand_exe3.setNext(&expand_exe4);    
//     expand_exe4.setNext(&project_exe3);      
//     project_exe3.setNext(&filter_exe2);
//     filter_exe2.setNext(&expand_exe5);
//     expand_exe5.setNext(&project_exe4);
//     project_exe4.setNext(&filter_exe3);
//     filter_exe3.setNext(&reduceByKey_exe);    
//     reduceByKey_exe.setNext(&top_exe);

//     top_exe.setNext(&filesink_exe);


//     nodeByIdScan_exe.process();

// }

long long person_ID = 1129;
std::string tagName = "Evo_Morales";

auto func_filter_by_personID = [](hiactor::InternalValue x){
    if((*x.vectorValue)[1].intValue != person_ID)
        return true;
    else
        return false;
};

auto func_filter_by_tag_one = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[1].stringValue,tagName.c_str()) == 0)
        return true;
    else
        return false;
};

auto func_filter_by_tag_two = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[1].stringValue,tagName.c_str()) != 0)
        return true;
    else
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
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    
    std::unordered_map<std::string , std::vector<hiactor::InternalValue> > storage;
    for(unsigned i = 0; i < vec.size(); i++) {        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        std::string tagName = msg[1].stringValue;
        
        if (storage.find(tagName) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue _tagid;
            hiactor::InternalValue _tagName;
            hiactor::InternalValue Count; 
            _tagid.intValue = msg[0].intValue;
            _msg.push_back(_tagid);      
            _tagName.stringValue= new char[strlen(msg[1].stringValue)+1];
            strcpy(_tagName.stringValue,msg[1].stringValue);
            Count.intValue=1; 
            _msg.push_back(Count);             
            _msg.push_back(_tagName);                      
            storage[tagName] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[tagName];         
            _msg[1].intValue = _msg[1].intValue + 1;
            storage[tagName] = _msg;
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



void IC_6()
{
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    // _exe_hd.nodeByIDScan(143)
    //         .add_distance()
    //         .varExpand(2,_person_knows_person_, 1, 2, false)
    //         // .expand_vec(_person_knows_person_, 0, 2,false)    
    //         // .expand_vec(_person_knows_person_, 3, 2,false)
    //         .filter(func_filter_by_personID)
    //         // .project_node({},{},{},{1,2})
    //         // .distinct()    
    //         .project_node({},{},{},{1})
    //         // 13(4400)
    //         .expand_vec(_person_hasCreated_post_, 0, -1,false) 
    //         // 323 220 (530000)
    //         .expand_vec(_post_hasTag_tag_, 1, -1, false)
    //         // 589
    //         .project_node({2},{_tag_},{{"name"}},{1,3})
    //         .filter(func_filter_by_tag_one)
    //         // 951 (679)
    //         .expand_vec(_post_hasTag_tag_, 0, -1, false)
    //         .project_node({2},{_tag_},{{"name"}},{0,2,3})
    //         .filter(func_filter_by_tag_two)
    //         // 941 (400)
    //         .reduceByKey(reduce_func, 1)
    //         .top(compare, 20)
    //         .fileSinkExe()   //1117
    //         .execute();

    _exe_hd.nodeByIDScan(143,"(a:person)")
            .add_distance("As distance")
            .varExpand(2,"(a:person)-[knows]->(b:person)",true)
            .filter(func_filter_by_personID)
 
            .project_node("keep=b.id")
            // 13(4400)
            .expand_vec("(b:person)-[hasCreated]->(c:post)",false) 
            // 323 220 (530000)
            .expand_vec("(c:post)-[hasTag]->(d:tag)", false)
            // 589
            .project_node("d.name keep=c.id,d.name")
            .filter(func_filter_by_tag_one)
            // 951 (679)
            .expand_vec("(c:post)-[hasTag]->(e:tag)", false)
            .project_node("keep=c.id,e.id")
            .project_node("e.name keep=e.id,e.name")
            .filter(func_filter_by_tag_two)
            // 941 (400)
            .reduceByKey(reduce_func, "e.id","post.count,e.name")
            .top("post.count=desc,e.name=asc", 20)
            .project_node("keep=e.name,post.count")
            .fileSinkExe()   //1117
            .execute();
}

seastar::future<> app_IC6() {
    IC_6();
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
        for(int i = 0; i < 64; i++) {
            tasks.push_back(app_IC6());
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());

    });
}
// 1129  713
// 143   1012 979
// 1161  938  889

// 143 1- 812
// 143 4- 794
// 143 8- 796
// 143 16- 794
// 143 32- 809
// 143 64- 800


