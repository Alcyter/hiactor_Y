
// #include "Executor/Project.h"
#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
// #include "Executor/Expand.h"
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
     if((*x.vectorValue)[2].intValue != personID)
        return true;
    else
        return false;
};

auto func_filter_by_personCountry = [](hiactor::InternalValue x){

    if(strcmp((*x.vectorValue)[2].stringValue,countryXName.c_str()) == 0) return false;
    if(strcmp((*x.vectorValue)[2].stringValue,countryYName.c_str()) == 0) return false;
    
    return true;
};

auto func_filter_by_message_time = [](hiactor::InternalValue x){
    // std::cout<<"message_time\n";
    // long long _creationDate = atoll((*x.vectorValue)[2].stringValue);
    long long _creationDate = (*x.vectorValue)[3].intValue;


    if (_creationDate<creationDate_int_strat) return false;
    if (_creationDate>=creationDate_int_end) return false;

    // if (_creationDate < startDate_int) return false;
    // if (_creationDate >= endDate_int ) return false;

    return true;
};

auto func_filter_by_message_place = [](hiactor::InternalValue x){
    if(strcmp((*x.vectorValue)[2].stringValue,countryXName.c_str()) == 0) return true;
    if(strcmp((*x.vectorValue)[2].stringValue,countryYName.c_str()) == 0) return true;
    
    return false;
};





bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;

    if (vec_a[4].intValue != vec_b[4].intValue)
        return vec_a[4].intValue < vec_b[4].intValue;
    else
        return vec_a[1].intValue > vec_b[1].intValue;
      
}


hiactor::InternalValue reduce_func(hiactor::InternalValue input) {
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    
    std::unordered_map<long long, std::vector<hiactor::InternalValue> > storage;

    for(unsigned i = 0; i < vec.size(); i++) {        
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        // std::cout<<"ID:"<<msg[0].intValue<<" city name "<<msg[1].stringValue<<" post ID"<<msg[2].intValue<<"place ID"<<msg[3].intValue<<"\n";
        long long person_ID = msg[1].intValue;
        
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
            if (strcmp(msg[2].stringValue,countryXName.c_str()) == 0) 
                xCount.intValue = 1;
            else 
                yCount.intValue = 1;
            _msg.push_back(msg[0]);
            _msg.push_back(ID);
            _msg.push_back(xCount);
            _msg.push_back(yCount);
            _msg.push_back(Count);
            
            storage[person_ID] = _msg;
        }
        else
        {            
            std::vector<hiactor::InternalValue> _msg=storage[person_ID];
            if (strcmp(msg[2].stringValue,countryXName.c_str()) == 0) 
                _msg[2].intValue = _msg[2].intValue + 1;
            else 
                _msg[3].intValue = _msg[3].intValue + 1;

            _msg[4].intValue = _msg[4].intValue + 1;

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





void Run_IC3(int i){
    // Graph_source* G =  Graph_source::GetInstance();
    ExecutorHandler _exe_hd(i);
    _exe_hd.nodeByIDScan(1161)
            .add_distance()
            .varExpand(2,_person_knows_person_, 2, 3, false)
            // .expand_vec(_person_knows_person_, 0, 2,false)
            // .expand_vec(_person_knows_person_, 3, 2,false)
            .filter(func_filter_by_personID)
            // .project_node({},{},{},{1,2})
            // .distinct()
            // 13 (4400)
            .project_node({},{},{},{0,2})
            .expand_vec(_person_isLocatedIn_place_, 1, -1, false)
            .expand_vec(_place_isPartOf_place_, 2, -1, false)
            .project_node({3},{_place_},{{"name"}},{0,1,4})
            .filter(func_filter_by_personCountry)
            // 31 (4000)
            .expand_vec(_person_hasCreated_message_, 1, -1,false)
            .project_node({3},{_message_},{{"creationDate"}},{0,1,3,4})
            .filter(func_filter_by_message_time)
            // 668 （440000）
            .expand_vec(_message_isLocatedIn_place_, 2, -1,false)
            .project_node({4},{_place_},{{"name"}},{0,1,5,2,4})
            .filter(func_filter_by_message_place)
            // 1258 (300)
            .reduceByKey(reduce_func, 1)         // reduceByKey_AD()
            .top(compare , 20)
            .project_node({1},{_person_},{{"firstName","lastName"}},{0,1,5,6,2,3,4})            
            .fileSinkExe()   //1280
            .execute();    

}

seastar::future<> IC3(int i) {
    Run_IC3(i);

    return seastar::make_ready_future<>();
}


int main(int ac, char** av)
{
//    std::cin >> person_id;   // 772 933 9399 2191 8061
    // person_id = 772;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC3(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}
// 1129  910   3212
// 143   1334  4498      3988
// 1161  1244  4267 4317


// 1-3986
// 2-7522
// 4-14757
// 8-30001
// 16-?
