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
    // std::vector<Operation> operations;

public:
    ExecutorHandler() {
        std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC6/actor/node.txt";
        _df.fromFile(filename);
    }

    // 非惰性的 expand 操作
    ExecutorHandler& expand(std::string _label, int _site, int _distance_site, bool _is_option);

    // 非惰性的 NodeByIDScan 操作
    ExecutorHandler& nodeByIDScan(long long _ID);

    // 非惰性的 Project 操作
    ExecutorHandler& project(std::vector<int> _site,std::vector<std::string> _label,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site);

    // 非惰性的 Add_distance 操作
    ExecutorHandler& add_distance();

    // 非惰性的 Distinct 操作
    ExecutorHandler& distinct();

    // 非惰性的 ReduceByKey 操作
    ExecutorHandler& reduceByKey();

    // 非惰性的 Filter 操作
    ExecutorHandler& filter(std::string str);

    // 非惰性的 Top 操作
    ExecutorHandler& top();

    ExecutorHandler& fileSinkExe();

    ExecutorHandler& varExpand(int _loop_time, int _label, int _site, int _distance_site, bool _is_option);

    ExecutorHandler& expandInto(int _label, int _site_from, int _site_to);

    ExecutorHandler& expand_vec(int _label, int _site, int _distance_site, bool _is_option);

    ExecutorHandler& project_node(std::vector<int> _site,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site);

    ExecutorHandler& project_edge(std::vector<int> _site_from,std::vector<int> _site_to ,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site);

    ExecutorHandler& visit_Expand(int _label, int _site, int _distance_site, bool _is_option);



    // 触发执行计划 process
    void execute();
};