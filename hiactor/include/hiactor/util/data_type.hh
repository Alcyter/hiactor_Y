/** Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <hiactor/core/reference_base.hh>
#include <hiactor/core/actor_message.hh>
#include <hiactor/net/serializable_queue.hh>
#include <vector>
#include <functional>
#include <tuple>
//根据banyan路径对应修改后的头文件
#include "common-utils.hh"
#include "hiactor/core/column_batch.hh"
//根据banyan新增头文件
#include "configs.hh"
#include "path_eos.hh"

namespace hiactor {

enum : uint8_t {
	TYPE_SHUFFLE = 0,
	TYPE_MAP = 1,
	TYPE_BARRIER = 2,
    TYPE_FILESOURCE = 3,
    TYPE_DATASOURCE = 4,
    TYPE_FILESINK = 5,
    TYPE_DATASINK = 6,
    TYPE_ENDDATASINK = 7,
    TYPE_ENDFILESINK = 8,
    TYPE_SENDBACKRESULT = 9,
    TYPE_MAPPARTITION = 10,
    TYPE_FLATMAP = 11,
};

template<typename _Res, typename... _ArgTypes>
struct new_function;

template <typename Type>
struct SerializablePrimitive {
    SerializablePrimitive() = default;
    explicit SerializablePrimitive(Type v) : val(v) {}
    SerializablePrimitive(SerializablePrimitive&& x) noexcept = default;

    void dump_to(serializable_queue& qu) {
        auto buf = seastar::temporary_buffer<char>(sizeof(val));
        memcpy(buf.get_write(), &val, sizeof(val));
        qu.push(std::move(buf));
    }

    static SerializablePrimitive load_from(serializable_queue& qu) {
        auto new_val = *reinterpret_cast<const Type*>(qu.pop().get());
        return SerializablePrimitive{new_val};
    }
    Type val;
};

using Eos = SerializablePrimitive<bool>;
using StartVertex = SerializablePrimitive<int64_t>;

using VertexBatch = hiactor::cb::column_batch<int64_t>;
using LabelBatch = hiactor::cb::column_batch<hiactor::cb::string>;
using StringBatch = hiactor::cb::column_batch<hiactor::cb::string>;
using PathBatch = hiactor::cb::column_batch<hiactor::cb::path>;
using PathLenBatch = hiactor::cb::column_batch<uint8_t>;
using TagNodeBatch = hiactor::cb::column_batch<int64_t>;
using IntPropBatch = hiactor::cb::column_batch<int64_t>;
using uIntBatch = hiactor::cb::column_batch<unsigned>;

template <>
struct SerializablePrimitive<void> {
    SerializablePrimitive() = default;
    SerializablePrimitive(SerializablePrimitive&& x) = default;

    void dump_to(serializable_queue&) {}

    static SerializablePrimitive load_from(serializable_queue&) {
        return SerializablePrimitive{};
    }
};

using Integer = SerializablePrimitive<int>;
using Boolean = SerializablePrimitive<bool>;
using Void = SerializablePrimitive<void>;

//新增struct
struct vertex_nameList {
    int64_t v_id;
    std::string name;

    vertex_nameList(int64_t v_id, std::string name)
        : v_id(v_id), name(name) {}
    ~vertex_nameList() = default;
};
using vertex_nameList_Batch = hiactor::cb::column_batch<vertex_nameList>;

struct vertex_pathLen {
    int64_t v_id;
    uint8_t path_len;

    vertex_pathLen(int64_t v_id, uint8_t path_len)
        : v_id(v_id), path_len(path_len) {}
    ~vertex_pathLen() = default;
};
using vertex_pathLen_Batch = hiactor::cb::column_batch<vertex_pathLen>;

struct vertex_nameList_pathLen {
    int64_t v_id;
    std::string name;
    uint8_t path_len;

    vertex_nameList_pathLen(int64_t v_id, std::string name, uint8_t path_len)
        : v_id(v_id), name(name), path_len(path_len) {}
    ~vertex_nameList_pathLen() = default;
};
using vertex_nameList_pathLen_Batch = hiactor::cb::column_batch<vertex_nameList_pathLen>;

struct vertex_int {
    std::vector<int64_t> v_ids;
    
    vertex_int() = default;
    vertex_int(const std::vector<int64_t> &input_v_ids) {
        v_ids = input_v_ids;
    }

    inline void emplace_back(int64_t v_id) {
        v_ids.push_back(v_id);
    }

    void dump_to(serializable_queue& qu) {}

    static vertex_int load_from(serializable_queue& qu) {
        vertex_int result;
        return result;
    }
};

// struct word_count {
//     std::string word;
//     int count;

//     word_count(std::string word = "", int count = 0)
//         : word(word), count(count){}
//     ~word_count() = default;
// };
// using word_count_Batch = hiactor::cb::column_batch<word_count>;

struct word_count_Batch {
    std::vector<std::string> words;
    std::vector<int> counts;

    word_count_Batch() = default;
    word_count_Batch(const word_count_Batch &batch) {
        words = batch.words;
        counts = batch.counts;
    }

    inline void emplace_back(std::string word, int count) {
        words.push_back(word);
        counts.push_back(count);
    }

    unsigned size() const { return words.size(); }

    void dump_to(serializable_queue& qu) {}

    static word_count_Batch load_from(serializable_queue& qu) {
        word_count_Batch result;
        return result;
    }
};


// struct InternalValue {
//     enum Type { INT, DOUBLE, STRING , VECTOR, TUPLE};
//     Type type;
//     union {
//         int iv;
//         double dv;
//         char* sv;
//         std::vector<InternalValue>* vec;
//         std::tuple<InternalValue, InternalValue>* tuple;
//     };

//     InternalValue(int v) : type(INT), iv(v) {}
//     InternalValue(double v) : type(DOUBLE), dv(v) {}
//     InternalValue(const char* s) : type(STRING) {
//         sv = new char[strlen(s) + 1];
//         strcpy(sv, s);
//     }
// };

union InternalValue {
    long long intValue;
    double doubleValue;
    char* stringValue;
    std::vector<InternalValue>* vectorValue;
    std::tuple<InternalValue, InternalValue>* tupleValue;

};

struct DataType {
    enum Type { INT, DOUBLE, STRING , VECTOR, TUPLE, VECTOR_TUPLE, VECTOR_VECTOR_TUPLE };
    Type type;
    int job_id;
    union InternalValue _data;

    DataType() {}
    DataType(int v, Type t = INT) : type(t) { _data.intValue = v; }
    DataType(double v, Type t = DOUBLE) : type(t) { _data.doubleValue = v; }
    DataType(const char* s, Type t = STRING) : type(t) {
        _data.stringValue = new char[strlen(s) + 1];
        strcpy(_data.stringValue, s);
    }
    DataType(std::vector<InternalValue>* vec, Type t = VECTOR) : type(t) {
        _data.vectorValue = vec;
    }
    DataType(std::tuple<InternalValue, InternalValue>* tup, Type t = TUPLE) : type(t) {
        _data.tupleValue = tup;
    }
    DataType(InternalValue i) : _data(i) {}

    void dump_to(serializable_queue& qu) {}

    static DataType load_from(serializable_queue& qu) {
        DataType result;
        return result;
    }
    
};




class MapFunc {
public:
    MapFunc() : func(nullptr), project_func(nullptr), expand_func(nullptr) {
        is_nodebyid = false;
        is_project = false;
        is_expand = false;
    }

    MapFunc(std::vector<int> project_site,std::vector<std::string> project_label, std::vector<std::vector<std::string>> strings,std::vector<int> project_reserve_site) : func(nullptr), project_func(nullptr), expand_func(nullptr)
    {
        this -> project_site = project_site;
        this -> project_label = project_label;        
        this -> project_propertyname = strings;
        this -> project_reserve_site = project_reserve_site;
    }

    MapFunc(std::vector<int> project_node_site,std::vector<int> project_node_label, std::vector<std::vector<std::string>> strings,std::vector<int> project_node_reserve_site) : func(nullptr), project_func(nullptr), expand_func(nullptr)
    {
        this -> project_node_site = project_node_site;
        this -> project_node_label = project_node_label;        
        this -> project_node_propertyname = strings;
        this -> project_node_reserve_site = project_node_reserve_site;
    }

    MapFunc(std::vector<int> project_edge_site_from, std::vector<int> project_edge_site_to,std::vector<int> project_edge_label, std::vector<std::vector<std::string>> strings,std::vector<int> project_edge_reserve_site) : func(nullptr), project_func(nullptr), expand_func(nullptr)
    {
        this -> project_edge_site_from = project_edge_site_from;
        this -> project_edge_site_to = project_edge_site_to;
        this -> project_edge_label = project_edge_label;        
        this -> project_edge_propertyname = strings;
        this -> project_edge_reserve_site = project_edge_reserve_site;
    }

    MapFunc(std::string expand_label, int ID_site, int _distance_site, bool _is_option) : func(nullptr), project_func(nullptr), expand_func(nullptr)
    {
        this -> expand_label = expand_label;
        this -> expand_site = ID_site;
        this -> expand_distance_site = _distance_site;
        this -> is_option = _is_option;
    }

    MapFunc(int expand_vec_index, int ID_site, int _distance_site, bool _is_option) : func(nullptr), project_func(nullptr), expand_func(nullptr)
    {
        this -> expand_vec_index = expand_vec_index;
        this -> expand_vec_site = ID_site;
        this -> expand_vec_distance_site = _distance_site;
        this -> is_option = _is_option;
    }

    MapFunc(int expandinto_label_index, int expandinto_site_from, int expandinto_site_to) : func(nullptr), project_func(nullptr), expand_func(nullptr)
    {
        this -> expandinto_label_index = expandinto_label_index;
        this -> expandinto_site_from = expandinto_site_from;
        this -> expandinto_site_to = expandinto_site_to;
    }

    MapFunc(long long _ID) : func(nullptr), project_func(nullptr), expand_func(nullptr){
        this -> nodebyid_ID = _ID;
    }

     MapFunc(std::function<bool(hiactor::InternalValue)> fun) : func(nullptr)
    {
        this -> customized_func = fun;
    }

    MapFunc(std::function<bool(hiactor::InternalValue, hiactor::InternalValue)> fun, int k) : func(nullptr)
    {
        this -> compare_func = fun;
        this -> number_k = k;
    }

    MapFunc(std::function<hiactor::InternalValue(hiactor::InternalValue)> fun) : func(nullptr)
    {
        this -> agg_func = fun;
    }

    // 设置函数
    void setFunction(const std::function<hiactor::DataType(hiactor::DataType)>& f) {
        func = f;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, long long)>& f){
        nodebyid_func = f;
        is_nodebyid = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<std::string>, std::vector<std::vector<std::string>>, std::vector<int>)>& f) {
        project_func = f;
        is_project = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<int>, std::vector<std::vector<std::string>>, std::vector<int>)>& f) {
        project_node_func = f;
        is_project_node = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<int>,std::vector<int>, std::vector<std::vector<std::string>>, std::vector<int>)>& f) {
        project_edge_func = f;
        is_project_edge = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::string, int, int, bool)>& f) {
        expand_func = f;
        is_expand = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, int, int, int, bool)>& f) {
        expand_vec_func = f;
        is_expand_vec = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, int, int, int)>& f) {
        expandinto_func = f;
        is_expandinto = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, int, int, int, bool, std::bitset<100000>&)>& f) {
        
        visited_expand_func = f;
        is_visited_expand = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::function<bool(hiactor::InternalValue)>, std::bitset<100000>&)>& f) {
        
        expand_IC5_func = f;
        is_expand_IC5 = true;
        is_visited_expand = true;
    }

    bool get_is_visited_expand()
    {
        return is_visited_expand;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::function<bool(hiactor::InternalValue)>)>& f) {
        filter_func = f;
        is_filter = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::function<bool(hiactor::InternalValue, hiactor::InternalValue)>, int)>& f) {
        top_func = f;
        is_top = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::function<hiactor::InternalValue(hiactor::InternalValue)>)>& f) {
        reduce_func = f;
        is_reduce = true;
    }

    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, int, int, int, bool,hiactor::InternalValue& ,std::vector<hiactor::InternalValue>&)>& f){
        expand_vec_pro_func = f;
        is_expand_vec_pro = true;
    }


    void setFunction(const std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<int>, std::vector<std::vector<std::string>>, std::vector<int>, hiactor::InternalValue& ,std::vector<hiactor::InternalValue>&)>& f) {
        project_node_pro_func = f;
        is_project_node_pro = true;
    }

    bool get_is_project_node_pro()
    {
        return is_project_node_pro;
    }

    // 处理输入数据
    hiactor::DataType process(hiactor::DataType input) {
        //std::cout<<"MapFunc process begin\n";
        if(is_project) {
            return project_func(input, project_site, project_label, project_propertyname, project_reserve_site);
        } else if(is_expand) {
            return expand_func(input, expand_label, expand_site, expand_distance_site, is_option);
        } else if(is_nodebyid){
            return nodebyid_func(input, nodebyid_ID);
        } else if(is_expandinto){
            return expandinto_func(input, expandinto_label_index, expandinto_site_from, expandinto_site_to);
        } else if(is_expand_vec){
            return expand_vec_func(input, expand_vec_index, expand_vec_site, expand_vec_distance_site, is_option);
        } else if(is_project_node){
            return project_node_func(input, project_node_site, project_node_label, project_node_propertyname, project_node_reserve_site);
        } else if(is_project_edge){
            return project_edge_func(input, project_edge_site_from, project_edge_site_to,project_edge_label,project_edge_propertyname,project_edge_reserve_site);
        } else if(is_filter){
            return filter_func(input, customized_func);
        } else if(is_top){
            return top_func(input, compare_func, number_k);
        } else if(is_reduce){
             return reduce_func(input, agg_func);
        } else {
            return func(input);
        }
    }

    hiactor::DataType process(hiactor::DataType input, std::bitset<100000>& visit) {
        if (is_expand_IC5)
            return expand_IC5_func(input, customized_func, visit);
        
        return visited_expand_func(input, expand_vec_index, expand_vec_site, expand_vec_distance_site, is_option, visit);
    }

    hiactor::DataType process(hiactor::DataType input, hiactor::InternalValue& _node, std::vector<hiactor::InternalValue>& _ori_path)
    {
        if (is_expand_vec_pro)
            return expand_vec_pro_func(input, expand_vec_index, expand_vec_site, expand_vec_distance_site, is_option, _node, _ori_path);
        else if (is_project_node_pro)
            return project_node_pro_func(input, project_node_site, project_node_label, project_node_propertyname, project_node_reserve_site, _node, _ori_path);
        else return func(input);
    }


    void dump_to(serializable_queue& qu) {}

    static MapFunc load_from(serializable_queue& qu) {
        MapFunc result;
        return result;
    }

private:
    std::function<hiactor::DataType(hiactor::DataType)> func;
    std::function<hiactor::DataType(hiactor::DataType, long long)> nodebyid_func;

    std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<std::string>, std::vector<std::vector<std::string>>, std::vector<int>)> project_func;
    std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<int>, std::vector<std::vector<std::string>>, std::vector<int>)> project_node_func;
    std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<int>, std::vector<int>, std::vector<std::vector<std::string>>, std::vector<int>)> project_edge_func;
    std::function<hiactor::DataType(hiactor::DataType, std::vector<int>,std::vector<int>, std::vector<std::vector<std::string>>, std::vector<int>, hiactor::InternalValue&, std::vector<hiactor::InternalValue>&)> project_node_pro_func;


    std::function<hiactor::DataType(hiactor::DataType, std::string, int, int, bool)> expand_func;
    std::function<hiactor::DataType(hiactor::DataType, int, int, int, bool)> expand_vec_func;
    std::function<hiactor::DataType(hiactor::DataType, int, int, int, bool, std::bitset<100000>&)> visited_expand_func;
    std::function<hiactor::DataType(hiactor::DataType, int, int, int, bool, hiactor::InternalValue&, std::vector<hiactor::InternalValue>&)> expand_vec_pro_func;
    std::function<hiactor::DataType(hiactor::DataType, std::function<bool(hiactor::InternalValue)>, std::bitset<100000>&)> expand_IC5_func;


    std::function<hiactor::DataType(hiactor::DataType, int, int, int)> expandinto_func;

    std::function<hiactor::DataType(hiactor::DataType, std::function<bool(hiactor::InternalValue)>)> filter_func;
    std::function<hiactor::DataType(hiactor::DataType, std::function<bool(hiactor::InternalValue, hiactor::InternalValue)>, int)> top_func;
    std::function<hiactor::DataType(hiactor::DataType, std::function<hiactor::InternalValue(hiactor::InternalValue)>)> reduce_func;

    bool is_nodebyid = false;
    bool is_project = false;
    bool is_project_node = false;
    bool is_project_edge = false;
    bool is_project_node_pro = false;
    bool is_expand = false;
    bool is_expand_vec = false;
    bool is_expand_vec_pro = false;

    bool is_expandinto = false;
    bool is_visited_expand = false;
    bool is_filter = false;
    bool is_top = false;
    bool is_reduce = false;
    bool is_expand_IC5 = false;

    long long nodebyid_ID;

    int expandinto_label_index;
    int expandinto_site_from;
    int expandinto_site_to;

    std::vector<std::string> project_label;
    std::vector<int> project_site;
    std::vector<std::vector<std::string>> project_propertyname;
    std::vector<int> project_reserve_site;

    std::vector<int> project_node_label;
    std::vector<int> project_node_site;
    std::vector<std::vector<std::string>> project_node_propertyname;
    std::vector<int> project_node_reserve_site;

    std::vector<int> project_edge_label;
    std::vector<int> project_edge_site_from;
    std::vector<int> project_edge_site_to;
    std::vector<std::vector<std::string>> project_edge_propertyname;
    std::vector<int> project_edge_reserve_site;


    std::string expand_label;
    int expand_site;
    int expand_distance_site;
    bool is_option = false;

    int expand_vec_index;
    int expand_vec_site;
    int expand_vec_distance_site;
    // bool is_option =false;

    std::function<bool(hiactor::InternalValue)> customized_func;
    std::function<bool(hiactor::InternalValue, hiactor::InternalValue)> compare_func;
    std::function<hiactor::InternalValue(hiactor::InternalValue)> agg_func;
    int number_k;

};

//改为读入DataType，输出unsigned
class HashFunc {
public:
    HashFunc() : func(nullptr) {}

    HashFunc(int key_site) : func(nullptr) {
        this -> key_site = key_site;
    }

    // 设置函数
    void setFunction(const std::function<unsigned(hiactor::InternalValue)>& f) {
        func = f;
    }

    void setFunction(const std::function<unsigned(hiactor::InternalValue, int key_site)>& f) {
        agg_func = f;
        is_agg = true;
    }

    // 处理输入数据
    unsigned process(hiactor::InternalValue input) {
        if (func) {
            return func(input);
        } else if (is_agg) {
            return agg_func(input, key_site);
        }
        // 如果没有设置函数，返回默认值
        return 0;
    }

    void dump_to(serializable_queue& qu) {}

    static HashFunc load_from(serializable_queue& qu) {
        HashFunc result;
        return result;
    }

private:
    std::function<unsigned(hiactor::InternalValue)> func;
    std::function<unsigned(hiactor::InternalValue, int key_site)> agg_func;
    int key_site;

    bool is_agg = false;
};

//记录actor的scope和id
struct actor_config {
    hiactor::scope_builder _builder;
    unsigned _ds_op_id;
    std::string _actor_type;

    actor_config(hiactor::scope_builder builder, unsigned ds_op_id, std::string actor_type)
        : _builder(builder), _ds_op_id(ds_op_id), _actor_type(actor_type){}
    ~actor_config() = default;
};
using actor_config_Batch = hiactor::cb::column_batch<actor_config>;

struct line_Batch {
    std::vector<std::string> lines;
    unsigned _size = 0;

    line_Batch() = default;

    void emplace_back(std::string& line) {
        lines.push_back(line);
        _size++;
    }

    unsigned size() { return _size; }

    void dump_to(serializable_queue& qu) {
        std::cout << "dump_to" <<'\n';
    }

    static line_Batch load_from(serializable_queue& qu) {
        std::cout << "load_from" <<'\n';
        line_Batch result;
        return result;
    }
};

template <typename Key, typename T>
struct new_map : public std::map<Key, T> {
public:
    // 使用基类的构造函数
    using std::map<Key, T>::map;

    void dump_to(serializable_queue& qu) {}

    new_map<Key, T> load_from(serializable_queue& qu) {
        return new_map<Key, T>();
    }
};

template <typename T>
class new_vector : public std::vector<T> {
public:
    using std::vector<T>::vector;

    void dump_to(serializable_queue& qu) {}

    static new_vector<T> load_from(serializable_queue& qu) {
        new_vector<T> result;
        return result;
    }
};

template<typename _Res, typename... _ArgTypes>
    struct new_function<_Res(_ArgTypes...)>
    : public std::function<_Res(_ArgTypes...)>
    {
        using std::function<_Res(_ArgTypes...)>::function; // 继承构造函数

        void dump_to(serializable_queue& qu) {}

        static new_function<_Res(_ArgTypes...)> load_from(serializable_queue& qu) {
            return new_function<_Res(_ArgTypes...)> (std::move(qu.pop()[0]));
        }
};


struct simple_string {
    char* str{};
    size_t len{};

    simple_string() : str(nullptr), len(0) {}
    explicit simple_string(const char* c_str) : str(new char[strlen(c_str)]), len(strlen(c_str)) {
        memcpy(str, c_str, len);
    }
    explicit simple_string(const std::string& std_str) : simple_string(std_str.data(), std_str.size()) {}
    simple_string(const char* data, size_t len) : str(new char[len]), len(len) {
        memcpy(str, data, len);
    }
    simple_string(simple_string&& x) noexcept: str(x.str), len(x.len) {
        x.str = nullptr;
        x.len = 0;
    }
    simple_string(const simple_string& x) : simple_string(x.str, x.len) {}
    simple_string& operator=(simple_string&& x) noexcept {
        if (this != &x) {
            str = x.str;
            len = x.len;
            x.str = nullptr;
            x.len = 0;
        }
        return *this;
    }
    ~simple_string() { delete[] str; }

    static simple_string from_exception(const std::exception_ptr& ep = std::current_exception()) {
        if (!ep) { throw std::bad_exception(); }
        try {
            std::rethrow_exception(ep);
        } catch (const std::exception& e) {
            return simple_string{e.what()};
        } catch (const std::string& e) {
            return simple_string{e};
        } catch (const char* e) {
            return simple_string{e};
        } catch (...) {
            return simple_string{"unknown exception!"};
        }
    }

    void dump_to(serializable_queue& qu) {
        auto buf = seastar::temporary_buffer<char>(len);
        if (str && len > 0) {
            memcpy(buf.get_write(), &str, len);
        }
        qu.push(std::move(buf));
    }

    static simple_string load_from(serializable_queue& qu) {
        auto q = qu.pop();
        return simple_string{q.get(), q.size()};
    }
};

} // namespace hiactor
