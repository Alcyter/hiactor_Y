#include "Executor/ExpandInto.h"
#include "Executor/Expand_vec.h"
#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
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

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;
//    std::string str1 = vec_a[5].stringValue;
//    std::string str2 = vec_b[5].stringValue;
    if(vec_a[5].intValue != vec_b[5].intValue) {
        return vec_a[5].intValue < vec_b[5].intValue;
    } else if(vec_a[3].intValue != vec_b[3].intValue) {
        return vec_a[3].intValue < vec_b[3].intValue;
    } else {
        return false;
    }
}


void Run_IC7(int i) {

    ExecutorHandler exe_hd(i);

    exe_hd.nodeByIDScan(person_id)
          .expand_vec(_person_hasCreated_message_, 1, -1, false)
          .expand_vec(_message_beliked_person_, 2, -1, false)
          .expandInto(_person_knows_person_, 1, 3)
          .project_edge({2},{3},{_message_beliked_person_},{{"creationDate"}} ,{})
          .top(compare, 20)
          .project_node({2,3},{_message_,_person_},{{"creationDate","imageFile","content"},{"firstName","lastName"}}, {0,3,9,10,5,2,7,8,6,4})
          .fileSinkExe()
          .execute();
//    exe_hd.nodeByIDScan(person_id)
//          .expand_vec("person-[hasCreated]->post", false)
//          .expand_vec("post-[beliked]->person", false)
//          .expandInto("person-[knows]->person", "isNew")
//          .project_edge("person-[likes.creationDate]->post keep=all")
//          .top("(person-[likes]->post).creationDate=desc,person.id=asc", 20)
//          .project_node("post.creationDate,post.imageFile,post.content,person.firstName,person.lastName keep=person.id,person.firstName,person.lastName,(person-[likes]->post).creationDate,post.id,post.imageFile,post.content,post.creationDate,person.isNew")
//          .fileSinkExe()
//          .execute();
//    exe_hd.nodeByIDScan(person_id)
//            .expand_vec("person-[hasCreated]->message", false)
//            .expand_vec("message-[beliked]->person", false)
//            .expandInto("person-[knows]->person", "isNew")
//            .project_edge("message-[beliked.creationDate]->person keep=all")
//            .top("(message-[beliked]->person).creationDate=desc,person.id=asc", 20)
//            .project_node("message.creationDate,message.imageFile,message.content,person.firstName,person.lastName keep=person.id,person.firstName,person.lastName,(message-[beliked]->person).creationDate,message.id,message.imageFile,message.content,message.creationDate,person.isNew")
//            .fileSinkExe()
//            .execute();

//    exe_hd.nodeByIDScan(person_id, "(a:person)")
//            .expand_vec("(a:person)-[hasCreated]->(b:message)", false)
//            .expand_vec("(b:message)-[e:beliked]->(c:person)", false)
//            .expandInto("(c:person)-[knows]->(a:person)", "a.isNew")
//            .project_edge("e.creationDate keep=all")
//            .top("e.creationDate=desc,c.id=asc", 20)
//            .project_node("b.creationDate,b.imageFile,b.content,c.firstName,c.lastName keep=c.id,c.firstName,c.lastName,e.creationDate,b.id,b.imageFile,b.content,b.creationDate,a.isNew")
//            .fileSinkExe()
//            .execute();

}
seastar::future<> IC7(int i) {
    Run_IC7(i);

    return seastar::make_ready_future<>();
}


int main(int ac, char** av)
{
//    std::cin >> person_id;   // 772 933 9399 2191 8061
    person_id = 772;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC7(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}