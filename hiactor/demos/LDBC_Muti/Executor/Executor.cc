#include "Executor.h"

#include "Expand.h"
#include "NodeByIDScan.h"
#include "Project.h"
#include "Distinct.h"
#include "ReduceByKey.h"
#include "Top.h"
#include "file_sink_exe.h"
#include "Add_distance.h"
#include "ExpandInto.h"
#include "Filter.h"
#include "Expand_vec.h"
#include "Project_edge.h"
#include "Project_node.h"
#include "VarExpand.h"
#include "Visit_Expand.h"



ExecutorHandler& ExecutorHandler::expand(std::string _label, int _site, int _distance_site, bool _is_option) {
    // executors.push_back({});
    _executor = new Expand(_label, _site, _distance_site, _is_option);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }
    
    // 此步是否必要（防止内存泄漏）
    executors.push_back(_executor);

    return *this;
}


ExecutorHandler& ExecutorHandler::nodeByIDScan(long long _ID) {
    _executor = new NodeByIDScan(_ID);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::project(std::vector<int> _site,std::vector<std::string> _label,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site) {
    _executor = new Project(_site, _label, _properties, _reserve_site);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::distinct(){
    _executor = new Distinct();
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::add_distance(){
    _executor = new Add_distance();
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::reduceByKey(){
    _executor = new ReduceByKey();
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::expandInto(int _label, int _site_from, int _site_to){
    _executor = new ExpandInto(_label, _site_from, _site_to);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::top(){
    _executor = new Top();
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::fileSinkExe(){
    _executor = new FileSinkExe();
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::filter(std::string str){
    _executor = new Filter(str);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}



ExecutorHandler& ExecutorHandler::expand_vec(int _label, int _site, int _distance_site, bool _is_option){
    _executor = new Expand_vec(_label, _site, _distance_site, _is_option);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::project_node(std::vector<int> _site,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site){
    _executor = new Project_node(_site, _label_index, _properties, _reserve_site);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);

    return *this;
}


ExecutorHandler& ExecutorHandler::project_edge(std::vector<int> _site_from,std::vector<int> _site_to ,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site){
    _executor = new Project_edge(_site_from, _site_to , _label_index, _properties, _reserve_site);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::varExpand(int _loop_time , int _label, int _site, int _distance_site, bool _is_option){
    _executor = new VarExpand(_loop_time,_label,_site,_distance_site, _is_option);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::visit_Expand(int _label, int _site, int _distance_site, bool _is_option){
    _executor = new Visit_Expand(_label, _site, _distance_site, _is_option);
    
    // 第一个算子的话不需要取 上一个executor进行setNext，但需要setDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

void ExecutorHandler::execute() {
    if(executors.size() > 0){
        executors[0]->process();
    }
    else
    std::cout << "ERROR EXECUTORAPI::execute()" << std::endl;
}
