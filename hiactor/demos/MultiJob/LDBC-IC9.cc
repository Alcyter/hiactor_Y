#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/Expand_vec.h"
#include "Executor/ExpandInto.h"
#include "Executor/NodeByIDScan.h"
#include "Executor/Add_distance.h"
#include "Executor/VarExpand.h"
#include "Executor/Top.h"
#include "Executor/Filter.h"
#include "Executor/Distinct.h"
#include "Executor/ReduceByKey.h"
#include "Executor/file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
long long max_creationDate;
long long person_id;

auto func = [](hiactor::InternalValue x) {
    return (*x.vectorValue)[4].intValue < max_creationDate;
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;
//    long long cd1 = std::stoll(vec_a[3].stringValue);
//    long long cd2 = std::stoll(vec_b[3].stringValue);
    if(vec_a[4].intValue != vec_b[4].intValue) {
        return vec_a[4].intValue < vec_b[4].intValue;
    } else if(vec_a[3].intValue != vec_b[3].intValue) {
        return vec_a[3].intValue > vec_b[3].intValue;
    } else {
        return false;
    }
}

void Run_IC9(int i) {

    ExecutorHandler exe_hd(i);

    exe_hd.nodeByIDScan(person_id)
          .add_distance()
          .varExpand(2, _person_knows_person_, 2, 2, false)
          .project_node({}, {}, {}, {0,2,3})
          .expand_vec(_person_hasCreated_message_, 1, -1, false)
          .project_node({3}, {_message_}, {{"creationDate"}}, {})
          .filter(func)
          .top(compare, 20)
          .project_node({1,3}, {_person_, _message_}, {{"firstName","lastName"}, {"content","imageFile"}}, {0,1,5,6,3,5,7,8})
          .fileSinkExe()
          .execute();

//    exe_hd.nodeByIDScan(person_id)
//          .add_distance()
//          .varExpand(2, "person-[knows]->person", true)
//          .project_node("keep=destination_person.id,distance.distance")
//          .expand_vec("person-[hasCreated]->post", false)
//          .project_node("post.creationDate keep=all")
//          .filter("post.creationDate<1288553757153")
//          .top("post.creationDate=desc,post.id=asc", 20)
//          .project_node("person.firstName,person.lastName,post.content,post.imageFile keep=person.id,person.firstName,person.lastName,post.id,post.creationDate,post.content,post.imageFile")
//          .fileSinkExe()
//          .execute();

//    exe_hd.nodeByIDScan(person_id)
//            .add_distance()
//            .varExpand(2, "person-[knows]->person", true)
//            .project_node("keep=destination_person.id,distance.distance")
//            .expand_vec("person-[hasCreated]->message", false)
//            .project_node("message.creationDate keep=all")
//            .filter("message.creationDate<1288553757153")
//            .top("message.creationDate=desc,message.id=asc", 20)
//            .project_node("person.firstName,person.lastName,message.content,message.imageFile keep=person.id,person.firstName,person.lastName,message.id,message.creationDate,message.content,message.imageFile")
//            .fileSinkExe()
//            .execute();

//    exe_hd.nodeByIDScan(person_id, "(a:person)")
//            .add_distance("As distance")
//            .varExpand(2, "(a:person)-[knows]->(b:person)", true)
//            .project_node("keep=b.id,distance.distance")
//            .expand_vec("(b:person)-[hasCreated]->(c:message)", false)
//            .project_node("c.creationDate keep=all")
//            .filter("c.creationDate<1288553757153")
//            .top("c.creationDate=desc,c.id=asc", 20)
//            .project_node("b.firstName,b.lastName,c.content,c.imageFile keep=b.id,b.firstName,b.lastName,c.id,c.creationDate,c.content,c.imageFile")
//            .fileSinkExe()
//            .execute();
}

seastar::future<> IC9(int i) {
    Run_IC9(i);

    return seastar::make_ready_future<>();
}

int main(int ac, char** av)
{
//    std::cin >> person_id;
//    std::cin >> max_creationDate;
    person_id = 772;  // 772 933 1129 4398046512167 9399
    max_creationDate = 1288553757153; //1288553757153 1308553757153 1298553757153  1278553757153 1268553757153
    std::cout<<"begin"<<std::endl;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC9(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}