
#pragma once

#include "DataFlow/DataFlow.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include "Graph_source/Graph_source.h"
#include "Graph_source/Graph_source_data.h"


class Executor{
public:
    Executor() = default;
    ~Executor() = default;

    virtual void process() = 0;
    virtual void setDf(DataFlow&& _df) = 0;
    virtual std::string get_type() = 0;
    virtual void setNext(Executor* next_exe){
        std::cout << "didn't override" << std::endl;
    }

};

class ExecutorHandler {
private:
    // hiactor::DataType _input_data;
    std::vector<Executor*> executors;  //（防止内存泄漏）
    Executor* _executor;
    DataFlow _df;
    int ptr = 0;
    std::unordered_map<int, std::tuple<std::string, std::string, std::string>> schema;
    // <index, (name, label, attribute)>  e.g  <0, ("start_person","person", "id")>, <2, ("a","person", "id")>, <3, ("b", "post","content")>
    std::unordered_map<std::string, std::tuple<std::string, std::string, std::string, std::string, std::string>> relation_notes;
    // <b, <a, c, person, person ,beliked>>

public:
    ExecutorHandler() {
        std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC5/actor/node.txt";
        _df.fromFile(filename);
    }
    ExecutorHandler(int index) {
        std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC5/actor/"+std::to_string(index)+"_node.txt";
        _df.fromFile(filename);
    }

    // 非惰性的 NodeByIDScan 操作
    ExecutorHandler& nodeByIDScan(long long _ID, std::string alter);

    // 非惰性的 Add_distance 操作
    ExecutorHandler& add_distance(std::string alter);

    // 非惰性的 ReduceByKey 操作
//    ExecutorHandler& reduceByKey(const std::function<hiactor::InternalValue(hiactor::InternalValue)>& func, int key_site);
    ExecutorHandler& reduceByKey(const std::function<hiactor::InternalValue(hiactor::InternalValue)>& func, std::string key, std::string new_elements);

    // 非惰性的 Filter 操作
//    ExecutorHandler& filter(const std::function<bool(hiactor::InternalValue)>& func);
    ExecutorHandler& filter(std::string condition);
    ExecutorHandler& filter(const std::function<bool(hiactor::InternalValue)>& func); // overload filter function

    // 非惰性的 Top 操作
//    ExecutorHandler& top(const std::function<bool(hiactor::InternalValue, hiactor::InternalValue)>& func, int number_k);
    ExecutorHandler& top(std::string compare_expression, int number_k);

    ExecutorHandler& fileSinkExe();

//    ExecutorHandler& addBitset(int index, int label);
    ExecutorHandler& addBitset(std::string edge);

//    ExecutorHandler& transferToOneHop(std::string, int relation_name, int relation_expand);
    ExecutorHandler& transferToOneHop(std::string name, std::string relation_name, std::string relation_expand);

//    ExecutorHandler& varExpand(int _loop_time, int _label, int _site, int _distance_site, bool _is_option);
    ExecutorHandler& varExpand(int _loop_time, std::string edge, bool _is_union);

//    ExecutorHandler& expandInto(int _label, int _site_from, int _site_to);
    ExecutorHandler& expandInto(std::string edge, std::string new_attribute);

//    ExecutorHandler& expand_vec(int _label, int _site, int _distance_site, bool _is_option);
    ExecutorHandler& expand_vec(std::string edge, bool _is_option);

//    ExecutorHandler& project_node(std::vector<int> _site,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site);
    ExecutorHandler& project_node(std::string input);

//    ExecutorHandler& project_edge(std::vector<int> _site_from,std::vector<int> _site_to ,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site);
    ExecutorHandler& project_edge(std::string input);
    
    ExecutorHandler& expand_IC5(const std::function<bool(hiactor::InternalValue)>& func);

    // 触发执行计划 process
    void execute();
};