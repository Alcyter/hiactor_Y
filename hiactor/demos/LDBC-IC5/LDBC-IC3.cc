
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
//     std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC3/actor/node.txt";
//     Graph_source* G =  Graph_source::GetInstance();
//     df.fromFile(filename);
//     std::cout<<"have set filename"<<std::endl;

//     NodeByIDScan nodeByIdScan_exe(1129);  //scan id = 1129

//     Add_distance add_distance_exe; //dest dist

//     Expand_vec expand_exe1(_person_knows_person_, 0, 2,false);

//     Expand_vec expand_exe2(_person_knows_person_, 3, 2,false);

//     Filter filter_exe("filter_by_personID");

//     Project_node project_exe1({},{},{},{1,2});

//     Distinct distince_exe;
    
//     Project_node project_exe2({},{},{},{0});

//     Expand_vec expand_exe3(_person_isLocatedIn_place_, 0, -1, false);

//     Expand_vec expand_exe4(_place_isPartOf_place_, 1, -1, false);

//     Project_node project_exe3({2},{_place_},{{"name"}},{0,3});  //personID , person_country

//     Filter filter_exe1("filter_by_personCountry");

//     Expand_vec expand_exe5(_person_hasCreated_post_, 0, -1,false);

//     Expand_vec expand_exe6(_post_isLocatedIn_place_, 2, -1,false);

//     Project_node project_exe4({3,2},{_place_,_post_},{{"name"},{"creationDate"}},{0,4,5}); //personID, post_country, creationDate

//     Filter filter_exe2("filter_by_post");    

//     ReduceByKey reduceByKey_exe;  // personID, xCount, yCount, Count

//     Project_node project_exe5({0},{_person_},{{"firstName","lastName"}},{0,4,5,1,2,3});

//     Top top_exe;

//     FileSinkExe filesink_exe; // filesink doesn't need barrier because top has synchronous semantics


//     nodeByIdScan_exe.setDf(std::move(df));

//     nodeByIdScan_exe.setNext(&add_distance_exe);
//     add_distance_exe.setNext(&expand_exe1);
//     expand_exe1.setNext(&expand_exe2);   
//     expand_exe2.setNext(&filter_exe);  
//     filter_exe.setNext(&project_exe1);
//     project_exe1.setNext(&distince_exe);
//     distince_exe.setNext(&project_exe2);
//     project_exe2.setNext(&expand_exe3);
//     expand_exe3.setNext(&expand_exe4); 
   
//     expand_exe4.setNext(&project_exe3);
//     project_exe3.setNext(&filter_exe1);   
   

//     filter_exe1.setNext(&expand_exe5);   
     
    
//     expand_exe5.setNext(&expand_exe6);  
//     expand_exe6.setNext(&project_exe4);  
    
//     project_exe4.setNext(&filter_exe2);
//     filter_exe2.setNext(&reduceByKey_exe); 
     
//     reduceByKey_exe.setNext(&project_exe5);
   
//     project_exe5.setNext(&top_exe);

//     top_exe.setNext(&filesink_exe);


//     nodeByIdScan_exe.process();

// }

long long personID = 1161;

std::string countryXName ="Vietnam";
std::string countryYName ="Finland";
std::string creationDate_start ="1275059059034";
std::string creationDate_end ="1344152433150";
long long creationDate_int_strat =  atoll(creationDate_start.c_str());
long long creationDate_int_end =  atoll(creationDate_end.c_str());

std::string startDate = "1275004800000";
long long startDate_int = atoll(startDate.c_str());
long long duartionDays = 800; //1344124800000
long long endDate_int = startDate_int + duartionDays*24*3600*1000;

auto func_filter_by_personID = [](hiactor::InternalValue x){
     if((*x.vectorValue)[1].intValue != personID)
        return true;
    else
        return false;
};

auto func_filter_by_personCountry = [](hiactor::InternalValue x){

    if(strcmp((*x.vectorValue)[1].stringValue,countryXName.c_str()) == 0) return false;
    if(strcmp((*x.vectorValue)[1].stringValue,countryYName.c_str()) == 0) return false;
    
    return true;
};

auto func_filter_by_message_time = [](hiactor::InternalValue x){

    // long long _creationDate = atoll((*x.vectorValue)[2].stringValue);
    long long _creationDate = (*x.vectorValue)[2].intValue;


    if (_creationDate<creationDate_int_strat) return false;
    if (_creationDate>=creationDate_int_end) return false;

    // if (_creationDate < startDate_int) return false;
    // if (_creationDate >= endDate_int ) return false;

    return true;
};

auto func_filter_by_message_place = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[1].stringValue,countryXName.c_str()) == 0) return true;
    if(strcmp((*x.vectorValue)[1].stringValue,countryYName.c_str()) == 0) return true;
    
    return false;
};





bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;

    if (vec_a[3].intValue != vec_b[3].intValue)
        return vec_a[3].intValue < vec_b[3].intValue;
    else
        return vec_a[0].intValue > vec_b[0].intValue;
      
}


hiactor::InternalValue reduce_func(hiactor::InternalValue input) {
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    
    std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;

    for(unsigned i = 0; i < vec.size(); i++) {        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        // std::cout<<"ID:"<<msg[0].intValue<<" city name "<<msg[1].stringValue<<" post ID"<<msg[2].intValue<<"place ID"<<msg[3].intValue<<"\n";
        long long person_ID = msg[0].intValue;
        
        if (storage.find(person_ID) == storage.end())
        {
            std::vector<hiactor::InternalValue> _msg;
            hiactor::InternalValue ID;
            hiactor::InternalValue xCount;
            hiactor::InternalValue yCount;
            hiactor::InternalValue Count;
            ID.intValue=person_ID;
            xCount.intValue=0;
            yCount.intValue=0;
            Count.intValue=1;
            if (strcmp(msg[1].stringValue,countryXName.c_str()) == 0) 
                xCount.intValue = 1;
            else 
                yCount.intValue = 1;
            _msg.push_back(ID);
            _msg.push_back(xCount);
            _msg.push_back(yCount);
            _msg.push_back(Count);
            
            storage[person_ID] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[person_ID];
            if (strcmp(msg[1].stringValue,countryXName.c_str()) == 0) 
                _msg[1].intValue = _msg[1].intValue + 1;
            else 
                _msg[2].intValue = _msg[2].intValue + 1;

            _msg[3].intValue = _msg[3].intValue + 1;

            storage[person_ID] = _msg;
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





void IC_3(){
    Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd;
    // _exe_hd.nodeByIDScan(1129)
    //         .add_distance()
    //         .varExpand(2,_person_knows_person_, 1, 2, false)
    //         // .expand_vec(_person_knows_person_, 0, 2,false)
    //         // .expand_vec(_person_knows_person_, 3, 2,false)
    //         .filter(func_filter_by_personID)
    //         // .project_node({},{},{},{1,2})
    //         // .distinct()
    //         // 13 (4400)
    //         .project_node({},{},{},{1})
    //         .expand_vec(_person_isLocatedIn_place_, 0, -1, false)
    //         .expand_vec(_place_isPartOf_place_, 1, -1, false)
    //         .project_node({2},{_place_},{{"name"}},{0,3})
    //         .filter(func_filter_by_personCountry)
    //         // 31 (4000)
    //         .expand_vec(_person_hasCreated_message_, 0, -1,false)
    //         .project_node({2},{_message_},{{"creationDate"}},{0,2,3})
    //         .filter(func_filter_by_message_time)
    //         // 668 （440000）
    //         .expand_vec(_message_isLocatedIn_place_, 1, -1,false)
    //         .project_node({3},{_place_},{{"name"}},{0,4,1,3})
    //         .filter(func_filter_by_message_place)
    //         // 1258 (300)
    //         .reduceByKey(reduce_func, 0)         // reduceByKey_AD()
    //         .top(compare , 20)
    //         .project_node({0},{_person_},{{"firstName","lastName"}},{0,4,5,1,2,3})            
    //         .fileSinkExe()   //1280
    //         .execute();    

    _exe_hd.nodeByIDScan(143,"(a:person)")
            .add_distance("As distance")
            .varExpand(2,"(a:person)-[knows]->(b:person)",true)            
            .filter(func_filter_by_personID)
           
            .project_node("keep=b.id")
            .expand_vec("(b:person)-[isLocatedIn]->(c:place)",false)
            .expand_vec("(c:place)-[isPartOf]->(d:place)",false)
            .project_node("d.name keep=b.id,d.name")
            .filter(func_filter_by_personCountry)

            .expand_vec("(b:person)-[hasCreated]->(e:message)",false)
            .project_node("e.creationDate keep=b.id,e.id,e.creationDate")
            .filter(func_filter_by_message_time)

            .expand_vec("(e:message)-[isLocatedIn]->(f:place)",false)
            .project_node("f.name keep=b.id,f.name,e.id,f.id")
            .filter(func_filter_by_message_place)

            .reduceByKey(reduce_func, "b.id","postX.count,postY.count,post.count")       
            .top("post.count=desc,b.id=asc" , 20)
            .project_node("b.firstName,b.lastName keep=b.id,b.firstName,b.lastName,postX.count,postY.count,post.count")            
            .fileSinkExe()   //1280
            .execute();  
}

seastar::future<> app_IC3() {
    IC_3();
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
        for(int i = 0; i < 32; i++) {
            tasks.push_back(app_IC3());
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());

    });


}
// 1129  910   3212
// 143   1334  4498      3988
// 1161  1244  4267 4317

// 4-3750
// 8-3670
// 16-3699
// 32-3685
// 64-3620