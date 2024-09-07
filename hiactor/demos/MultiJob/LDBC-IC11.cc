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
#include "Executor/file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

long long person_id;
int max_year = 2002;
std::string country_name = "Germany";

auto func_1 = [](hiactor::InternalValue x) {
//   return (std::stoi(((*x.vectorValue)[2]).stringValue) < max_year);
    return ((*x.vectorValue)[3]).intValue < 2002;
};
//
auto func_2 = [](hiactor::InternalValue x) {
    return ((*x.vectorValue)[5]).stringValue == country_name;
//    return (strcmp(((*x.vectorValue)[4]).stringValue, country_name) == 0);
};

bool compare(hiactor::InternalValue a, hiactor::InternalValue b) {
    std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
    std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;
    auto str1 = (vec_a[3].intValue);
    auto str2 = (vec_b[3].intValue);
    if(str1 != str2) {
        return str1 > str2;
    } else if(vec_a[1].intValue != vec_b[1].intValue) {
        return vec_a[1].intValue > vec_b[1].intValue;
    } else if(vec_a[6].stringValue != vec_b[6].stringValue) {
        return vec_a[6].stringValue < vec_b[6].stringValue;
    } else {
        return false;
    }
}


void Run_IC11(int i) {

    ExecutorHandler exe_hd(i);
    exe_hd.nodeByIDScan(person_id)
          .add_distance()
          .varExpand(2, _person_knows_person_, 2, 2, false)
          .project_node({},{},{}, {0,2})
          .expand_vec(_person_workAt_organisation_, 1, -1, false)
          .project_edge({1}, {2}, {_person_workAt_organisation_},{{"workFrom"}},{})
          .filter(func_1)
          .expand_vec(_organisation_isLocatedIn_place_, 2, -1, false)
          .project_node({4,2},{_place_,_organisation_},{{"name"},{"name"}},{})
          .filter(func_2)
          .top(compare, 10)
          .project_node({0},{_person_},{{"firstName","lastName"}},{0,1,7,8,6,3})
          .fileSinkExe()
          .execute();
//    exe_hd.nodeByIDScan(person_id)
//          .add_distance()
//          .varExpand(2, "person-[knows]->person", true)
//          .project_node("keep=destination_person.id")
//          .expand_vec("person-[workAt]->organisation", false)
//          .project_edge("person-[workAt.workFrom]->organisation keep=all")
//          .filter("(person-[workAt]->organisation).workFrom<2002")
//          .expand_vec("organisation-[isLocatedIn]->place", false)
//          .project_node("place.name,organisation.name keep=all")
//          .filter("place.name=Germany")
//          .top("(person-[workAt]->organisation).workFrom=asc,person.id=asc,organisation.name=desc", 10)
//          .project_node("person.firstName,person.lastName keep=person.id,person.firstName,person.lastName,organisation.name,(person-[workAt]->organisation).workFrom")
//          .fileSinkExe()
//          .execute();
//    exe_hd.nodeByIDScan(person_id, "(a:person)")
//            .add_distance("As distance")
//            .varExpand(2, "(a:person)-[knows]->(b:person)", true)
//            .project_node("keep=b.id")
//            .expand_vec("(b:person)-[c:workAt]->(d:organisation)", false)
//            .project_edge("c.workFrom keep=all")
//            .filter("c.workFrom<2002")
//            .expand_vec("(d:organisation)-[isLocatedIn]->(e:place)", false)
//            .project_node("e.name,d.name keep=all")
//            .filter("e.name=Germany")
//            .top("c.workFrom=asc,b.id=asc,d.name=desc", 10)
//            .project_node("b.firstName,b.lastName keep=b.id,b.firstName,b.lastName,d.name,c.workFrom")
//            .fileSinkExe()
//            .execute();
}
seastar::future<> IC11(int i) {
              Run_IC11(i);

          return seastar::make_ready_future<>();
}

int main(int ac, char* av[])
{

    person_id = 9399;
    max_year = 2002;
    country_name = "Germany";

    std::cout<<"begin"<<std::endl;
    hiactor::actor_app app;
    app.run(ac, av, []{
        std::cout<<"app start"<<std::endl;
        std::vector<seastar::future<>> tasks;
        for(int i = 0; i < 16; i++) {
            tasks.push_back(IC11(i));
        }

        return seastar::when_all_succeed(tasks.begin(), tasks.end());
    });
}