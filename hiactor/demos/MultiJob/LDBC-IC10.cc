#include "Executor/Project_edge.h"
#include "Executor/Project_node.h"
#include "Executor/NodeByIDScan.h"
#include "Executor/Expand_vec.h"
#include "Executor/VarExpand.h"
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
long long person_id;
int specific_month;

auto func = [](hiactor::InternalValue x) {
    time_t timestamp = (*x.vectorValue)[2].intValue / 1000; // original data is integer
    struct tm *timeinfo;
    timeinfo = localtime(&timestamp);
    int month = timeinfo -> tm_mon + 1;
    int day = timeinfo -> tm_mday;
    if(((month == specific_month) && (day >= 21)) || ((month == (specific_month + 1) % 12) && (day < 22))) {
        return true;
    } else return false;
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;
    if(vec_a[2].intValue != vec_b[2].intValue) {
        return vec_a[2].intValue < vec_b[2].intValue;
    } else if(vec_a[1].intValue != vec_b[1].intValue) {
        return vec_a[1].intValue > vec_b[1].intValue;
    } else {
        return false;
    }
}

hiactor::InternalValue reduce_func(hiactor::InternalValue input) {
    std::vector<hiactor::InternalValue> vec = *input.vectorValue;
    std::vector<hiactor::InternalValue> reduced;
    Graph_source* G =  Graph_source::GetInstance();
    std::unordered_map<long long, std::unordered_map<long long, int>> storage; //<personid, map<pid, 0/
    long long job_id;
    for(unsigned i = 0; i < vec.size(); i++) {
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        job_id = msg[0].intValue;
        long long id = msg[1].intValue;
        if(storage.find(id) == storage.end()) {
            std::unordered_map<long long ,int> tmp;
            storage[id] = tmp;
        }
        long long post_id = msg[3].intValue;
        long long tag_id = msg[4].intValue;
        if(storage[id].find(post_id) == storage[id].end()) {
            if(post_id == -1) {
                storage[id][post_id] = -1; // represent that commonscore is 0
                continue;
            }
            storage[id][post_id] = 0;
            if(tag_id == -1) { }
            else if(G->interested_tag[job_id][tag_id] == 1) { storage[id][post_id] = 1;}
            else { }
        } else {
            if(storage[id][post_id] == 1) continue;
            if(tag_id == -1) { continue;}
            else if(G->interested_tag[job_id][tag_id] == 1) {
                storage[id][post_id] = 1;
            } else {
                continue;
            }
        }
    }
    for(auto& outerPair : storage) {
        long long pid = outerPair.first;
        std::unordered_map<long long, int> innerMap = outerPair.second;
        int common = 0;
        int uncommon = 0;
        int commonInterestScore;
        for(auto& innerPair : innerMap) {
            if(innerPair.second == -1) {
                break;
            }
            else if(innerPair.second == 0) {
                uncommon++;
            } else {
                common++;
            }
        }
        commonInterestScore = common - uncommon;
        hiactor::InternalValue person, com , jobID;
        jobID.intValue = job_id;
        person.intValue = pid;
        com.intValue = commonInterestScore;
        std::vector<hiactor::InternalValue> tmp;
        tmp.push_back(jobID);
        tmp.push_back(person);
        tmp.push_back(com);
        hiactor::InternalValue result;
        result.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
        reduced.push_back(result);
    }
    hiactor::InternalValue result;
    result.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
    return result;
}

void Run_IC10(int job_id) {

    ExecutorHandler exe_hd(job_id);
    exe_hd.nodeByIDScan(person_id)
          .addBitset(1, _person_hasInterest_tag_)
          .add_distance()
          .varExpand(2, _person_knows_person_, 2, 3, false)
          .project_node({2},{_person_},{{"birthday"}},{0,2,6}) // destination_person.birthday
          .filter(func)
          .expand_vec(_person_hasCreated_post_, 1, -1, true)
          .expand_vec(_post_hasTag_tag_, 3, -1, true)
          .reduceByKey(reduce_func, 1)
          .top(compare, 10)
          .expand_vec(_person_isLocatedIn_place_, 1, -1, false)
          .project_node({1,3}, {_person_,_place_}, {{"firstName","lastName","gender"},{"name"}}, {})
          .fileSinkExe()
          .execute();

//    exe_hd.nodeByIDScan(person_id, "(a:person)")
////            .addBitset("(a:person)-[hasInterest]->tag")  // addBitset("(a:person)-[hasInterest]->tag")
////            .add_distance("As distance")
////            .varExpand(2, "(a:person)-[knows]->(b:person)", false)
////            .project_node("b.birthday keep=b.id,b.birthday")
////            .filter(func)
////            .expand_vec("(b:person)-[hasCreated]->(c:post)", true)
////            .expand_vec("(c:post)-[hasTag]->(d:tag)", true)
////            .reduceByKey(reduce_func, "b.id", "b.commonscore")
////            .top("b.commonscore=desc,b.id=asc", 10)
////            .expand_vec("(b:person)-[isLocatedIn]->(e:place)", false)
////            .project_node("b.firstName,b.lastName,b.gender,e.name keep=b.id,b.firstName,b.lastName,b.commonscore,b.gender,e.name")
//            .fileSinkExe()
//            .execute();
}

seastar::future<> IC10(int i) {
    Run_IC10(i);
    return seastar::make_ready_future<>();
}

int main(int ac, char** av)
{
//    std::cin >> person_id;
//    std::cin >> specific_month;
    person_id = 933; // "1129" "1129" "933" "772" "9399"
    specific_month = 6; // 4 5 6 7 8
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC10(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}