#include "Executor.h"
#include "NodeByIDScan.h"
#include "Distinct.h"
#include "ReduceByKey.h"
#include "Top.h"
#include "file_sink_exe.h"
#include "Add_distance.h"
#include "ExpandInto.h"
#include "Expand_IC5.h"
#include "Filter.h"
#include "Expand_vec.h"
#include "Project_edge.h"
#include "Project_node.h"
#include "VarExpand.h"
// #include "AddBitset.h"
// #include "TransferToOneHop.h"
#include <sstream>
#include <regex>



ExecutorHandler& ExecutorHandler::nodeByIDScan(long long _ID) { // (a:person)

    _executor = new NodeByIDScan(_ID);

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::add_distance(){ // .add_distance("As distance")

    _executor = new Add_distance();

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

// ExecutorHandler& ExecutorHandler::addBitset(int index, int label){  // addBitset("(a:person)-[hasInterest]->tag")

//     _executor = new AddBitset(index, label);

//     // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
//     if(executors.size() == 0){
//         _executor->setDf(std::move(_df));
//     }
//     else{
//         executors.back()->setNext(_executor);
//     }

//     executors.push_back(_executor);
//     return *this;
// }

// ExecutorHandler& ExecutorHandler::transferToOneHop(std::string name, int relation_name, int relation_expand) {

//     _executor = new TransferToOneHop(name, relation_name, relation_expand);

//     // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
//     if(executors.size() == 0){
//         _executor->setDf(std::move(_df));
//     }
//     else{
//         executors.back()->setNext(_executor);
//     }

//     executors.push_back(_executor);
//     return *this;
// }


ExecutorHandler& ExecutorHandler::reduceByKey(const std::function<hiactor::InternalValue(hiactor::InternalValue)>& func, int key_site){

    _executor = new ReduceByKey(func, key_site);

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
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

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::top(const std::function<bool(hiactor::InternalValue, hiactor::InternalValue)>& func, int number_k){

    _executor = new Top(func, number_k);

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
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

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}


ExecutorHandler& ExecutorHandler::filter(const std::function<bool(hiactor::InternalValue)>& func) { // overload
    _executor = new Filter(func);

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
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

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
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

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
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

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::varExpand(int _loop_time, int _label, int _site, int _distance_site, bool _is_option){


    _executor = new VarExpand(_loop_time,_label,_site,_distance_site, _is_option);

    // 绗竴涓畻瀛愮殑璇濅笉闇€瑕佸彇 涓婁竴涓猠xecutor杩涜setNext锛屼絾闇€瑕乻etDf
    if(executors.size() == 0){
        _executor->setDf(std::move(_df));
    }
    else{
        executors.back()->setNext(_executor);
    }

    executors.push_back(_executor);
    return *this;
}

ExecutorHandler& ExecutorHandler::expand_IC5(const std::function<bool(hiactor::InternalValue)>& func){
    _executor = new Expand_IC5(func);
    
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
