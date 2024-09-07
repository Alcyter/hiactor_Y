#include "Executor/Expand_vec.h"
#include "Executor/ExpandInto.h"
#include "Executor/Project_edge.h"
#include "Executor/Project_node.h"
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
    } else if(vec_a[4].intValue != vec_b[4].intValue) {
        return vec_a[4].intValue > vec_b[4].intValue;
    } else {
        return false;
    }
}

void Run_IC8(int i) {

    ExecutorHandler exe_hd(i);

    exe_hd.nodeByIDScan(person_id)
          .expand_vec(_person_hasCreated_message_, 1, -1, false)
          .expand_vec(_message_hasReply_comment_, 2, -1, false)
          .expand_vec(_comment_hasCreator_person_, 3, -1, false)
          .project_node({3}, {_comment_}, {{"creationDate"}},{})
          .top(compare, 20)
          .project_node({4,3}, {_person_,_comment_}, {{"firstName","lastName"},{"content"}},{0,4,6,7,5,3,8})
          .fileSinkExe()
          .execute();

//    exe_hd.nodeByIDScan(person_id)
//          .expand_vec("person-[hasCreated]->post", false)
//          .expand_vec("post-[hasReply]->comment", false)
//          .expand_vec("comment-[hasCreator]->person", false)
//          .project_node("comment.creationDate keep=all")
//          .top("comment.creationDate=desc,comment.id=asc", 20)
//          .project_node("person.firstName,person.lastName,comment.content keep=person.id,person.firstName,person.lastName,comment.creationDate,comment.id,comment.content")
//          .fileSinkExe()
//          .execute();

//    exe_hd.nodeByIDScan(person_id)
//            .expand_vec("person-[hasCreated]->message", false)
//            .expand_vec("message-[hasReply]->comment", false)
//            .expand_vec("comment-[hasCreator]->person", false)
//            .project_node("comment.creationDate keep=all")
//            .top("comment.creationDate=desc,comment.id=asc", 20)
//            .project_node("person.firstName,person.lastName,comment.content keep=person.id,person.firstName,person.lastName,comment.creationDate,comment.id,comment.content")
//            .fileSinkExe()
//            .execute();

//    exe_hd.nodeByIDScan(person_id, "(a:person)")
//            .expand_vec("(a:person)-[hasCreated]->(b:message)", false)
//            .expand_vec("(b:message)-[hasReply]->(c:comment)", false)
//            .expand_vec("(c:comment)-[hasCreator]->(d:person)", false)
//            .project_node("c.creationDate keep=all")
//            .top("c.creationDate=desc,c.id=asc", 20)
//            .project_node("d.firstName,d.lastName,c.content keep=d.id,d.firstName,d.lastName,c.creationDate,c.id,c.content")
//            .fileSinkExe()
//            .execute();
}

seastar::future<> IC8(int i) {
    Run_IC8(i);

    return seastar::make_ready_future<>();
}

int main(int ac, char** av)
{
//    std::cin >> person_id; //4398046512167 1129 933 772 9399
    person_id = 4398046512167;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC8(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}