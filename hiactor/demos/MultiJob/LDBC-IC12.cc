#include "Executor/Project_node.h"
#include "Executor/Project_edge.h"
#include "Executor/NodeByIDScan.h"
#include "Executor/Expand_vec.h"
#include "Executor/ExpandInto.h"
#include "Executor/Add_distance.h"
#include "Executor/Top.h"
#include "Executor/Filter.h"
#include "Executor/Distinct.h"
#include "Executor/ReduceByKey.h"
#include "Executor/VarExpand.h"
#include "Executor/TransferToOneHop.h"
#include "Executor/file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <thread>
long long personid;
std::string tagclass_name;

auto func_1 = [](hiactor::InternalValue x) {
    Graph_source* G =  Graph_source::GetInstance();
    return (G -> interested_tag[(*x.vectorValue)[0].intValue][(*x.vectorValue)[6].intValue]) == 1;
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
    long long tid;
    std::unordered_map<long long, std::unordered_map<long long, std::vector<long long>>> storage; //<personid, <commentid, <tagid>>>
    for(unsigned i = 0; i < vec.size(); i++) {
        std::vector<hiactor::InternalValue> msg = *vec[i].vectorValue;
        tid = msg[0].intValue;
        long long person_id = msg[2].intValue;
        if(storage.find(person_id) == storage.end()) {
            std::unordered_map<long long ,std::vector<long long>> tmp;
            storage[person_id] = tmp;
        }
        long long comment_id = msg[3].intValue;
        long long tag_id = msg[5].intValue;
        if(storage[person_id].find(comment_id) == storage[person_id].end()) {
            std::vector<long long> tag_tmp;
            storage[person_id][comment_id] = tag_tmp;
            storage[person_id][comment_id].push_back(tag_id);
        } else {
            storage[person_id][comment_id].push_back(tag_id);
        }

    }
    for(auto& outerPair : storage) {
        std::vector<hiactor::InternalValue> _vec;
        hiactor::InternalValue t;
        t.intValue = tid;
        _vec.push_back(t);

        long long person_id = outerPair.first;
        hiactor::InternalValue person;
        person.intValue = person_id;
        _vec.push_back(person);

        std::unordered_map<long long, std::vector<long long>> comment_table = outerPair.second;
        long long count = static_cast<int>(comment_table.size());
        hiactor::InternalValue number;
        number.intValue = count;
        _vec.push_back(number);

        std::vector<hiactor::InternalValue> tag_set;
        for(auto& innerPair : comment_table) {
            for(auto& id : innerPair.second) {
                hiactor::InternalValue tag_name;
                tag_name.stringValue = (G -> node[_tag_][id]["name"]).stringValue;  // a project
                tag_set.push_back(tag_name);
            }
        }
        hiactor::InternalValue tags;
        tags.vectorValue = new std::vector<hiactor::InternalValue>(tag_set);
        _vec.push_back(tags);

        hiactor::InternalValue val;
        val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
        reduced.push_back(val);
    }
    hiactor::InternalValue result;
    result.vectorValue = new std::vector<hiactor::InternalValue>(reduced);
    return result;
}

void Run_IC12(int i) {

    ExecutorHandler exe_hd(i);

    exe_hd.nodeByIDScan(1129)
          .transferToOneHop("Thing", _tagclass_, _tagclass_hasSubclass_tagclass_) // OfficeHolder Person Thing
          .expand_vec(_person_knows_person_, 1, -1, false)
          .expand_vec(_person_hasCreated_comment_, 2, -1, false)
          .expand_vec(_comment_replyOf_post_, 3, -1, false)
          .expand_vec(_post_hasTag_tag_, 4, -1, false)
          .expand_vec(_tag_hasType_tagclass_, 5, -1, false)
          .filter(func_1)
          .reduceByKey(reduce_func, 2)
          .top(compare, 20)
          .project_node({1}, {_person_}, {{"firstName","lastName"}}, {0,1,4,5,2,3})
          .fileSinkExe()
          .execute();

//    exe_hd.nodeByIDScan(personid)
//          .transferToOneHop(tagclass_name, "tagclass", "tagclass-[hasSubclass]->tagclass")
//          .expand_vec("person-[knows]->person", false)
//          .expand_vec("person-[hasCreated]->comment", false)
//          .expand_vec("comment-[replyOf]->post", false)
//          .expand_vec("post-[hasTag]->tag", false)
//          .expand_vec("tag-[hasType]->tagclass", false)
//          .filter(func_1)
//          .reduceByKey(reduce_func, "person.id", "comment.count,tag.names")
//          .top("comment.count=desc,person.id=asc", 20)
//          .project_node("person.firstName,person.lastName keep=person.id,person.firstName,person.lastName,comment.count,tag.names")
//          .fileSinkExe()
//          .execute();

//    exe_hd.nodeByIDScan(personid, "(a:person)")
//            .transferToOneHop(tagclass_name, "tagclass", "tagclass-[hasSubclass]->tagclass")
//            .expand_vec("(a:person)-[knows]->(b:person)", false)
//            .expand_vec("(b:person)-[hasCreated]->(c:comment)", false)
//            .expand_vec("(c:comment)-[replyOf]->(d:post)", false)
//            .expand_vec("(d:post)-[hasTag]->(e:tag)", false)
//            .expand_vec("(e:tag)-[hasType]->(f:tagclass)", false)
//            .filter(func_1)
//            .reduceByKey(reduce_func, "b.id", "c.count,e.names")
//            .top("c.count=desc,b.id=asc", 20)
//            .project_node("b.firstName,b.lastName keep=b.id,b.firstName,b.lastName,c.count,e.names")
//            .fileSinkExe()
//            .execute();

}

seastar::future<> IC12(int i) {
    Run_IC12(i);

    return seastar::make_ready_future<>();
}

int main(int ac, char* av[])
{
    personid = 1129;
    tagclass_name = "Thing";
    std::cout<<"begin"<<std::endl;

    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC12(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}