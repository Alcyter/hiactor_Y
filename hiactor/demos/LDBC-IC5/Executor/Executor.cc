
#include "Executor.h"
#include "NodeByIDScan.h"
#include "Distinct.h"
#include "ReduceByKey.h"
#include "Expand_IC5.h"
#include "Top.h"
#include "file_sink_exe.h"
#include "Add_distance.h"
#include "ExpandInto.h"
#include "Filter.h"
#include "Expand_vec.h"
#include "Project_edge.h"
#include "Project_node.h"
#include "VarExpand.h"
// #include "AddBitset.h"
// #include "TransferToOneHop.h"
#include <sstream>
#include <regex>



ExecutorHandler& ExecutorHandler::nodeByIDScan(long long _ID, std::string alter) { // (a:person)

    size_t dotPos = alter.find(':');
    std::string note = alter.substr(1, dotPos - 1);
    size_t dotPos_1 = alter.find(')');
    std::string label = alter.substr(dotPos + 1, dotPos_1 - dotPos - 1);
    std::cout << note << " " << label << std::endl;

    ptr = 0;
    std::tuple tuple = std::make_tuple(note, label, "id");
    schema[ptr++] = tuple;

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


ExecutorHandler& ExecutorHandler::add_distance(std::string alter){ // .add_distance("As distance")

    size_t dotPos = alter.find("As");
    std::string note = alter.substr(dotPos + 3);
    std::tuple tuple1 = std::make_tuple("destination_" + std::get<0>(schema[0]), "person", "id");
    std::tuple tuple2 = std::make_tuple(note, "distance", "distance");
    schema[ptr++] = tuple1;
    schema[ptr++] = tuple2;

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

// ExecutorHandler& ExecutorHandler::addBitset(std::string edge){  // addBitset("(a:person)-[hasInterest]->tag")

//     size_t dotPos_1 = edge.find('(');
//     size_t dotPos_2 = edge.find(':');
//     std::string note = edge.substr(dotPos_1 + 1, dotPos_2 - dotPos_1 - 1);

//     int index = -1;
//     int relation = 0;
//     for(const auto& pair : schema) {
//         if(std::get<0>(pair.second) == note) {
//             index = pair.first;
//             break;
//         }
//     }
//     if(index == -1) std::cout << "Error: The first label input does not exist in the path." << std::endl;

//     dotPos_1 = edge.find(':');
//     dotPos_2 = edge.find(')');
//     std::string str1 = edge.substr(dotPos_1 + 1, dotPos_2 - dotPos_1 - 1);
//     std::string str2 = edge.substr(dotPos_2 + 1);
//     std::string result_edge = str1 + str2;

//     auto it = string_to_index.find(result_edge);
//     if(it != string_to_index.end()) {
//         relation = it -> second;
//     } else {
//         std::cout << "Error: The second input edge does not have a corresponding pattern." << std::endl;
//     }

//     std::cout << "addBitset" << index << relation << std::endl;
//     _executor = new AddBitset(index, relation);

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

// ExecutorHandler& ExecutorHandler::transferToOneHop(std::string name, std::string _relation_name, std::string _relation_expand) {
// // no change for this function
//     int relation_name = -1;
//     int relation_expand = -1;

//     auto it = string_to_index.find(_relation_name);
//     auto is = string_to_index.find(_relation_expand);
//     if(it != string_to_index.end()) {
//         relation_name = it -> second;
//     } else {
//         std::cout << "Error: The input name does not have a corresponding node." << std::endl;
//     }
//     if(is != string_to_index.end()) {
//         relation_expand = is -> second;
//     } else {
//         std::cout << "Error: The input relation does not have a corresponding pattern." << std::endl;
//     }

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


ExecutorHandler& ExecutorHandler::reduceByKey(const std::function<hiactor::InternalValue(hiactor::InternalValue)>& func, std::string key, std::string new_elements){
 // .reduceByKey(reduce_func, "b.id", "c.count,e.names")
    int key_site = -1;

    size_t dotPos = key.find('.');
    std::string key_label = key.substr(0, dotPos);
    std::string key_attribute = key.substr(dotPos + 1);

    for(const auto& pair : schema) {
        if((std::get<0>(pair.second) == key_label) && (std::get<2>(pair.second) == key_attribute)) {
            key_site = pair.first;
            break;
        }
    }
    if(key_site == -1) std::cout << "Error: The input key does not have a corresponding node." << std::endl;

    std::vector<std::string> token_list;
    std::stringstream ss1(new_elements);
    std::string token;
    token_list.push_back(key);
    while(std::getline(ss1, token, ',')) {
        token_list.push_back(token);
    }

    std::unordered_map<int, std::tuple<std::string, std::string, std::string>> tmp;
    for(unsigned i = 0; i < token_list.size(); i++) {
        size_t pos = token_list[i].find('.');
        std::string new_note = token_list[i].substr(0, pos);
        std::string new_attribute = token_list[i].substr(pos + 1);
        std::string new_label;
        for(const auto& pair : schema) {
            if(std::get<0>(pair.second) == new_note) {
                new_label = std::get<1>(schema[pair.first]);
                break;
            }
        }
        tmp[i] = std::make_tuple(new_note, new_label, new_attribute);
    }
    schema = tmp;
    ptr = static_cast<int>(token_list.size());

    // std::cout << "reduceByKey" << key_site << std::endl;

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


ExecutorHandler& ExecutorHandler::expandInto(std::string edge, std::string new_attribute){
 // .expandInto("(c:person)-[k:knows]->(a:person)", "a.isNew") note that k is optional
    int _label = -1;
    int _site_from = -1;
    int _site_to = -1;
    std::regex regex_1("\\((\\w+):(\\w+)\\)-\\[(\\w+)\\]->\\((\\w+):(\\w+)\\)");
    std::regex regex_2("\\((\\w+):(\\w+)\\)-\\[(\\w+):(\\w+)\\]->\\((\\w+):(\\w+)\\)");
    std::smatch match;
    std::string str2, str3, str4, str5, str6;
    std::string result, find_1, find_2;
    bool edge_note = false;

    // check whether there is an edge notation
    std::string substring = edge.substr(edge.find('[') + 1, edge.find(']') - edge.find('[') - 1);
    if(substring.find(':') != std::string::npos) {
        edge_note = true;
    }

    if(edge_note) {
        if (std::regex_search(edge, match, regex_2)) {
            find_1 = match[1];
            str2 = match[2];
            str3 = match[3];
            str4 = match[4];
            find_2 = match[5];
            str6 = match[6];
            result = str2 + "-[" + str4 + "]->" + str6;
        } else {
            std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
        }
    } else {
        if (std::regex_search(edge, match, regex_1)) {
            find_1 = match[1];
            str2 = match[2];
            str3 = match[3];
            find_2 = match[4];
            str5 = match[5];
            result = str2 + "-[" + str3 + "]->" + str5;
        } else {
            std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
        }
    }

    for(const auto& pair : schema) {
        if(std::get<0>(pair.second) == find_1) {
            _site_from = pair.first;
        }
        if(std::get<0>(pair.second) == find_2) {
            _site_to = pair.first;
        }
    }
    if(_site_from == -1 || _site_to == -1) std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
    if(edge_note) {
        relation_notes[str3] = std::make_tuple(find_1, find_2, str2, str6, str4);
    }

    auto it = string_to_index.find(result);
    if(it != string_to_index.end()) {
        _label = it -> second;
    } else {
        std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
    }

    size_t pos = new_attribute.find('.');
    std::string new_note = new_attribute.substr(0, pos);
    std::string attribute = new_attribute.substr(pos + 1);
    std::string new_label;
    for(const auto& pair : schema) {
        if(std::get<0>(pair.second) == new_note) {
            new_label = std::get<1>(pair.second);
            break;
        }
    }

    //handle the new item
    std::tuple tuple = std::make_tuple(new_note, new_label, attribute);
    schema[ptr++] = tuple;

    // std::cout << "expandInto" << _label << _site_from << _site_to << std::endl;
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

std::function<bool(hiactor::InternalValue, hiactor::InternalValue)> generateTopFunction(const std::vector<std::tuple<std::string, int, std::string>>& arguments) {
    return [arguments](hiactor::InternalValue a, hiactor::InternalValue b) {
        std::vector<hiactor::InternalValue> vec_a = *a.vectorValue;
        std::vector<hiactor::InternalValue> vec_b = *b.vectorValue;
        for(const auto & argument : arguments) {
            std::string type = attribute_to_type[std::get<0>(argument)];
            int site = std::get<1>(argument);
            std::string cT = std::get<2>(argument);
            if(type == "intValue") {
                if(cT == "asc") {
                    if(vec_a[site].intValue != vec_b[site].intValue) {
                        return vec_a[site].intValue > vec_b[site].intValue;
                    }
                } else { // desc
                    if(vec_a[site].intValue != vec_b[site].intValue) {
                        return vec_a[site].intValue < vec_b[site].intValue;
                    }
                }
            } else if(type == "doubleValue") {
                if(cT == "asc") {
                    if(vec_a[site].doubleValue != vec_b[site].doubleValue) {
                        return vec_a[site].doubleValue > vec_b[site].doubleValue;
                    }
                } else { // desc
                    if(vec_a[site].doubleValue != vec_b[site].doubleValue) {
                        return vec_a[site].doubleValue < vec_b[site].doubleValue;
                    }
                }
            } else if(type == "stringValue") {
                if(cT == "asc") {
                    if(vec_a[site].stringValue != vec_b[site].stringValue) {
                        return vec_a[site].stringValue > vec_b[site].stringValue;
                    }
                } else { // desc
                    if(vec_a[site].stringValue != vec_b[site].stringValue) {
                        return vec_a[site].stringValue < vec_b[site].stringValue;
                    }
                }
            }
        }
        return false;
    };
}

ExecutorHandler& ExecutorHandler::top(std::string compare_expression, int number_k){

    std::vector<std::tuple<std::string, int, std::string>> arguments;

    std::stringstream ss(compare_expression);
    std::string str1;
    while(std::getline(ss, str1, ',')) {
        size_t pos = str1.find('=');
        std::string str2 = str1.substr(0, pos); // b.name
        std::string str3 = str1.substr(pos + 1);  // asc
        pos = str2.find('.');
        std::string str4 = str2.substr(0, pos); // b
        std::string str5 = str2.substr(pos + 1); // name

        int site = -1;
        for(const auto& pair : schema) {
            if((std::get<0>(pair.second) == str4) && (std::get<2>(pair.second) == str5)) {
                site = pair.first;
                break;
            }
        }
        if(site == -1) std::cout << "The first input does not have a corresponding node in the path." << std::endl;

        arguments.emplace_back(str5, site, str3);
    }

    std::function<bool(hiactor::InternalValue, hiactor::InternalValue)> func = generateTopFunction(arguments);
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

std::function<bool(hiactor::InternalValue)> generateFilterFunction(const std::tuple<std::string, int, std::string, std::string>& expression) {
    return [expression](hiactor::InternalValue a) {
        std::string type = attribute_to_type[std::get<0>(expression)];
        int site = std::get<1>(expression);
        std::string op = std::get<2>(expression);
        std::string constValue = std::get<3>(expression);
        if(type == "intValue") {
            long long cV = std::stoll(constValue);
            if(op == ">") {
                return (*a.vectorValue)[site].intValue > cV;
            } else if(op == "<") {
                return (*a.vectorValue)[site].intValue < cV;
            } else if(op == "=") {
                return (*a.vectorValue)[site].intValue == cV;
            }
        } else if(type == "doubleValue") {
            double cV = std::stod(constValue);
            if(op == ">") {
                return (*a.vectorValue)[site].doubleValue > cV;
            } else if(op == "<") {
                return (*a.vectorValue)[site].doubleValue < cV;
            } else if(op == "=") {
                return (*a.vectorValue)[site].doubleValue == cV;
            }
        } else if(type == "stringValue") {
            if(op == "=") return (*a.vectorValue)[site].stringValue == constValue;
        }
        return true;
    };
}

ExecutorHandler& ExecutorHandler::filter(std::string condition){
 // b.name=xxx
    size_t pos = condition.find_last_of("=<>");
    std::string str1 = condition.substr(0, pos);
    std::string str2 = condition.substr(pos, 1);
    std::string str3 = condition.substr(pos + 1);
    pos = str1.find('.');
    std::string label = str1.substr(0, pos);  // b
    std::string attribute = str1.substr(pos + 1); // name
    int site, flag = 0;

    for(const auto& pair : schema) {
        if((std::get<0>(pair.second) == label) && (std::get<2>(pair.second) == attribute)) {
            site = pair.first;
            flag = 1;
            break;
        }
    }
    if(!flag) std::cout << "The first input does not have a coresponding node in the path." << std::endl;

    std::tuple tup = std::make_tuple(attribute, site, str2, str3);

    std::function<bool(hiactor::InternalValue)> func = generateFilterFunction(tup);
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



ExecutorHandler& ExecutorHandler::expand_vec(std::string edge, bool _is_option){
    // .expand_vec("(b:person)-[c:workAt]->(d:organisation)", false)
    // for(const auto& pair : schema) {
        // std::cout << pair.first << " " << std::get<0>(pair.second) << " " << std::get<1>(pair.second) <<  " " << std::get<2>(pair.second) << std::endl;
    // }
    int _label = -1;
    int _site = -1;
    int _distance_site = -1;
    std::string expand_attribute = "id";

    std::regex regex_1("\\((\\w+):(\\w+)\\)-\\[(\\w+)\\]->\\((\\w+):(\\w+)\\)");
    std::regex regex_2("\\((\\w+):(\\w+)\\)-\\[(\\w+):(\\w+)\\]->\\((\\w+):(\\w+)\\)");
    std::smatch match;
    std::string str2, str3, str4, str5, str6;
    std::string result, find_1, find_2;
    bool edge_note = false;

    // check whether there is an edge notation
    std::string substring = edge.substr(edge.find('[') + 1, edge.find(']') - edge.find('[') - 1);
    if(substring.find(':') != std::string::npos) {
        edge_note = true;
    }

    if(edge_note) {
        if (std::regex_search(edge, match, regex_2)) {
            find_1 = match[1];
            str2 = match[2];
            str3 = match[3];
            str4 = match[4];
            find_2 = match[5];
            str6 = match[6];
            result = str2 + "-[" + str4 + "]->" + str6;
        } else {
            std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
        }
    } else {
        if (std::regex_search(edge, match, regex_1)) {
            find_1 = match[1];
            str2 = match[2];
            str3 = match[3];
            find_2 = match[4];
            str5 = match[5];
            result = str2 + "-[" + str3 + "]->" + str5;
        } else {
            std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
        }
    }

    for(const auto& pair : schema) {
        if((std::get<0>(pair.second) == find_1) && (std::get<2>(pair.second) == "id")) {
            _site = pair.first;
            break;
        }
    }
    if(_site == -1) std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;

    auto it = string_to_index.find(result);
    if(it != string_to_index.end()) {
        _label = it -> second;
    } else {
        std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
    }

    // handle the coming new item (must be an id)
    std::tuple tuple = std::make_tuple(find_2, (edge_note ? str6 : str5), "id");
    if(edge_note) {
        relation_notes[str3] = std::make_tuple(find_1, find_2, str2, str6, str4);
    }
    schema[ptr++] = tuple;

    // std::cout << "expand_vec" << _label << _site << _distance_site << _is_option << std::endl;
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


ExecutorHandler& ExecutorHandler::project_node(std::string input){
// .project_node("b.birthday keep=b.id,b.birthday")
    // for(const auto& pair : schema) {
        // std::cout << pair.first << " " << std::get<0>(pair.second) << " " << std::get<1>(pair.second) <<  " " << std::get<2>(pair.second) << std::endl;
    // }

    std::vector<int> _site = {};
    std::vector<int> _label_index = {};
    std::vector<std::string> _label_string_index = {}; // this only to store std::string in _label_index
    std::vector<std::string> _label_note = {}; // this only to store std::string in note
    std::vector<std::vector<std::string>> _properties = {};
    std::vector<int> _reserve_site = {};

    std::string project_elements, reserve_elements;
    size_t pos = input.find(' ');
    if(pos != std::string::npos) {
        project_elements = input.substr(0, pos);
    } else {
        project_elements = "";
    }
    pos = input.find('=');
    reserve_elements = input.substr(pos + 1);
    if(reserve_elements == "all") {
        reserve_elements = "";
    }
    // std::cout << project_elements << std::endl;
    // std::cout << reserve_elements << std::endl;

    std::vector<std::string> token_list;
    std::vector<std::string> reserve_list;
    std::stringstream ss1(project_elements);
    std::stringstream ss2(reserve_elements);
    std::string token;
    while(std::getline(ss1, token, ',')) {
        token_list.push_back(token);
    }
    while(std::getline(ss2, token, ',')) {
        reserve_list.push_back(token);
    }

    for(const auto & item : token_list) {
        size_t dotPos = item.find('.');
        std::string project_note = item.substr(0, dotPos); // b
        std::string project_attribute = item.substr(dotPos + 1); // name
        std::string project_label; // person

        for(const auto& pair : schema) {
            if(std::get<0>(pair.second) == project_note) {
                project_label = std::get<1>(pair.second);
                break;
            }
        }

        auto it = string_to_index.find(project_label);
        int pro_label_index;
        int index;
        if(it != string_to_index.end()) {
            pro_label_index = it -> second;
        } else {
            std::cout << "Error: The first input list does not have a corresponding node." << std::endl;
        }

        auto is = std::find(_label_index.begin(), _label_index.end(), pro_label_index);
        if(is != _label_index.end()) { //  if label already in _label_index
            index = std::distance(_label_index.begin(), is);
            _properties[index].push_back(project_attribute);
        } else { // no such label in _label_index
            _label_index.push_back(pro_label_index);
            _label_string_index.push_back(project_label);
            _label_note.push_back(project_note);
            std::vector<std::string> tmp;
            tmp.push_back(project_attribute);
            _properties.push_back(tmp);

            int tmp_site = -1;
            for(const auto& pair : schema) {
                if((std::get<0>(pair.second) == project_note) && (std::get<2>(pair.second) == "id")) {
                    tmp_site = pair.first;
                    break;
                }
            }
            if(tmp_site == -1) std::cout << "Error: The input list does not have a corresponding node." << std::endl;
            _site.push_back(tmp_site);
        }
    }

    // handle new items
    for(unsigned  i = 0; i < _label_string_index.size(); i++) {
        for(unsigned  j = 0; j < _properties[i].size(); j++) {
            std::tuple tuple = std::make_tuple(_label_note[i], _label_string_index[i], _properties[i][j]);
            schema[ptr++] = tuple;
        }
    }

    for(const auto & item : reserve_list) {
        size_t dotPos = item.find('.');
        std::string res_note = item.substr(0, dotPos);
        std::string res_attribute = item.substr(dotPos + 1);
        for(const auto& pair : schema) {
            if((std::get<0>(pair.second) == res_note) && (std::get<2>(pair.second) == res_attribute)) {
                _reserve_site.push_back(pair.first);
            }
        }
    }

    // reorder the schema and ptr
    std::unordered_map<int, std::tuple<std::string, std::string, std::string>> tmp;
    for(unsigned i = 0; i < _reserve_site.size(); i++) {  // change the destination_person to person when reordering
        tmp[i] = schema[_reserve_site[i]];
    }
    if(_reserve_site.size() != 0) {
        schema = tmp;
        ptr = static_cast<int>(_reserve_site.size());
    }

    // std::cout << "project_node " ;
    // for(unsigned i = 0; i < _site.size(); i++) std::cout << _site[i] << " ";
    // for(unsigned i = 0; i < _label_index.size(); i++) std::cout << _label_index[i] << " ";
    // for(unsigned i = 0; i < _properties.size(); i++) {
        // for(unsigned j = 0; j < _properties[i].size(); j++) std::cout << _properties[i][j] << " ";
    // }
    // for(unsigned i = 0; i < _reserve_site.size(); i++) std::cout << _reserve_site[i] << " ";
    // std::cout << std::endl;


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

ExecutorHandler& ExecutorHandler::project_edge(std::string input){
    // for(const auto& pair : schema) {
        // std::cout << pair.first << " " << std::get<0>(pair.second) << " " << std::get<1>(pair.second) <<  " "  << std::get<2>(pair.second) << std::endl;
    // }
    std::vector<int> _site_from = {};
    std::vector<int> _site_to = {};
    std::vector<int> _label_index = {};
    std::vector<std::string> _label_string_index = {}; // this only to store std::string in _label_index
    std::vector<std::string> _label_note = {}; // this only to store std::string in note
    std::vector<std::vector<std::string>> _properties = {};
    std::vector<int> _reserve_site = {};

    std::string project_elements, reserve_elements;
    size_t pos = input.find(' ');
    if(pos != std::string::npos) {
        project_elements = input.substr(0, pos);
    } else {
        project_elements = "";
    }
    pos = input.find('=');
    reserve_elements = input.substr(pos + 1);
    if(reserve_elements == "all") {
        reserve_elements = "";
    }

    std::vector<std::string> token_list;
    std::vector<std::string> reserve_list;
    std::stringstream ss1(project_elements);
    std::stringstream ss2(reserve_elements);
    std::string token;
    while(std::getline(ss1, token, ',')) {
        token_list.push_back(token);
    }
    while(std::getline(ss2, token, ',')) {
        reserve_list.push_back(token);
    }

    for(const auto & item : token_list) { // e.name

        size_t Pos = item.find('.');
        std::string note = item.substr(0, Pos);
        std::string attribute = item.substr(Pos + 1);
        std::string from = std::get<0>(relation_notes[note]);
        std::string to = std::get<1>(relation_notes[note]);
        std::string pattern = std::get<2>(relation_notes[note]) + "-[" + std::get<4>(relation_notes[note]) + "]->" + std::get<3>(relation_notes[note]);
        std::string _label = std::get<4>(relation_notes[note]); // knows

        auto it = string_to_index.find(pattern);
        int pro_label_index;
        int index;
        if(it != string_to_index.end()) {
            pro_label_index = it -> second;
        } else {
            std::cout << "Error: The first input list does not have a corresponding edge." << std::endl;
        }

        auto is = std::find(_label_index.begin(), _label_index.end(), pro_label_index);
        if(is != _label_index.end()) { //  if label already in _label_index
            index = std::distance(_label_index.begin(), is);
            _properties[index].push_back(attribute);
        } else { // no such label in _label_index
            _label_index.push_back(pro_label_index);
            _label_string_index.push_back(_label); // this time push "know" label
            _label_note.push_back(note);
            std::vector<std::string> tmp;
            tmp.push_back(attribute);
            _properties.push_back(tmp);
        }

        int from_site = -1;
        int to_site = -1;
        for(const auto& pair : schema) {
            if(std::get<0>(pair.second) == from) {
                from_site = pair.first;
            }
            if(std::get<0>(pair.second) == to) {
                to_site = pair.first;
            }
        }
        if(from_site == -1 || to_site == -1) std::cout << "Error: The first list does not have a corresponding edge." << std::endl;
        _site_from.push_back(from_site);
        _site_to.push_back(to_site);
    }

    // handle new items
    for(unsigned  i = 0; i < _label_string_index.size(); i++) {
        for(unsigned  j = 0; j < _properties[i].size(); j++) {
            std::tuple tuple = std::make_tuple(_label_note[i], _label_string_index[i], _properties[i][j]);
            schema[ptr++] = tuple;
        }
    }

    for(const auto & item : reserve_list) {
        size_t dotPos = item.find('.');
        std::string res_label = item.substr(0, dotPos);
        std::string res_attribute = item.substr(dotPos + 1);
        for(const auto& pair : schema) {
            if((std::get<0>(pair.second) == res_label) && (std::get<2>(pair.second) == res_attribute)) {
                _reserve_site.push_back(pair.first);
            }
        }
    }

    // reorder the schema and ptr
    std::unordered_map<int, std::tuple<std::string, std::string, std::string>> tmp;
    for(unsigned i = 0; i < _reserve_site.size(); i++) {
        tmp[i] = schema[_reserve_site[i]];
    }
    if(_reserve_site.size() != 0) {
        schema = tmp;
        ptr = static_cast<int>(_reserve_site.size());
    }

    // std::cout << "project_edge " ;
    // for(unsigned i = 0; i < _site_from.size(); i++) std::cout << _site_from[i] << " ";
    // for(unsigned i = 0; i < _site_to.size(); i++) std::cout << _site_to[i] << " ";
    // for(unsigned i = 0; i < _label_index.size(); i++) std::cout << _label_index[i] << " ";
    // for(unsigned i = 0; i < _properties.size(); i++) {
        // for(unsigned j = 0; j < _properties[i].size(); j++) std::cout << _properties[i][j] << " ";
    // }
    // for(unsigned i = 0; i < _reserve_site.size(); i++) std::cout << _reserve_site[i] << " ";
    // std::cout << std::endl;
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

ExecutorHandler& ExecutorHandler::varExpand(int _loop_time, std::string edge, bool _is_union){

    int _label = -1;
    int _site = -1;
    int _distance_site;
    int union_distance_site = 2;
    int distance_site = 3;
    bool _is_option = false;

    _distance_site = (_is_union ? union_distance_site : distance_site);

    std::regex regex_1("\\((\\w+):(\\w+)\\)-\\[(\\w+)\\]->\\((\\w+):(\\w+)\\)");
    std::regex regex_2("\\((\\w+):(\\w+)\\)-\\[(\\w+):(\\w+)\\]->\\((\\w+):(\\w+)\\)");
    std::smatch match;
    std::string str2, str3, str4, str5, str6;
    std::string result, find_1, find_2;
    bool edge_note = false;

    // check whether there is an edge notation
    std::string substring = edge.substr(edge.find('[') + 1, edge.find(']') - edge.find('[') - 1);
    if(substring.find(':') != std::string::npos) {
        edge_note = true;
    }

    if(edge_note) {
        if (std::regex_search(edge, match, regex_2)) {
            find_1 = match[1];
            str2 = match[2];
            str3 = match[3];
            str4 = match[4];
            find_2 = match[5];
            str6 = match[6];
            result = str2 + "-[" + str4 + "]->" + str6;
        } else {
            std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
        }
    } else {
        if (std::regex_search(edge, match, regex_1)) {
            find_1 = match[1];
            str2 = match[2];
            str3 = match[3];
            find_2 = match[4];
            str5 = match[5];
            result = str2 + "-[" + str3 + "]->" + str5;
            std::cout << find_1 << " " << str2 << " " << str3 << " " << find_2 << " " << str5 << " " << result << std::endl;
        } else {
            std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
        }
    }

    for(const auto& pair : schema) {
        if((std::get<0>(pair.second) == ("destination_" + find_1)) && (std::get<2>(pair.second) == "id")) {
            _site = pair.first;
            break;
        }
    }
    if(_site == -1) std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;

    auto it = string_to_index.find(result);
    if(it != string_to_index.end()) {
        _label = it -> second;
    } else {
        std::cout << "Error: The first input edge does not have a corresponding pattern." << std::endl;
    }

    //change the destination person to the given note
    int des_site = ptr - 2;
    std::string& str = std::get<0>(schema[des_site]);
    str = find_2;
    //handle the new item
    for(int i = 0; i < _loop_time; i++) {
        std::tuple tuple = std::make_tuple("temp_value", (edge_note ? str6 : str5), "id");
        schema[ptr++] = tuple;
    }

    if(edge_note) {
        relation_notes[str3] = std::make_tuple(find_1, find_2, str2, str6, str4);
    }

    // std::cout << "varexpand " ;
    // std::cout << _loop_time << _label << _site << _distance_site << _is_option << std::endl;
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


ExecutorHandler& ExecutorHandler::expand_IC5(const std::function<bool(hiactor::InternalValue)>& func) { // overload
    
    std::unordered_map<int, std::tuple<std::string, std::string, std::string>> tmp;
    std::string new_note = "b";
    std::string new_attribute = "id";
    std::string new_label = "person";        
    tmp[0] = std::make_tuple(new_note, new_label, new_attribute);
    new_note = "c";
    new_attribute = "id";
    new_label = "forum";   
    tmp[1] = std::make_tuple(new_note, new_label, new_attribute);
    new_note = "d";
    new_attribute = "count";
    new_label = "post";   
    tmp[2] = std::make_tuple(new_note, new_label, new_attribute);   

    schema = tmp;
    ptr = static_cast<int>(tmp.size());;
    
    _executor = new Expand_IC5(func);

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



void ExecutorHandler::execute() {
    std::cout<<"----------------executor----------------\n";
    if(executors.size() > 0){
        executors[0]->process();
    }
    else
        std::cout << "ERROR EXECUTORAPI::execute()" << std::endl;
}
