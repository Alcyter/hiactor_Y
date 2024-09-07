#include "Executor.h"
#include <cstdarg>

hiactor::DataType map_expand_var(const hiactor::DataType& input, int label_index, int ID_site, int distance_site, bool is_option, std::bitset<10000>& visit);

hiactor::DataType map_distinct(const hiactor::DataType& input, int label_index, int ID_site, int distance_site, bool is_option, std::bitset<10000>& visit);

unsigned expand_var_vector_hash_string(const hiactor::InternalValue& input);

class VarExpand: public Executor{
public:
    VarExpand(int _loop_time , int _label, int _site, int _distance_site, bool _is_option)
    {
        loop_time = _loop_time;
        label_index = _label;
        ID_site = _site;
        distance_site = _distance_site;
        is_option = _is_option;
        have_next = false;
    }

    std::string get_type(){
        return "VarExpand";
    }
    void setNext(Executor* next_exe)
    {
        _next_exe = next_exe;
        have_next = true;
    }
    void setDf(DataFlow&& df)
    {
        _df = df;
    }

    void process()
    {
        hiactor::MapFunc map_func_1(label_index,ID_site,distance_site, is_option);
        hiactor::HashFunc hash_func;
        hiactor::MapFunc map_func_2(label_index,ID_site,distance_site, is_option);

        map_func_1.setFunction(map_expand_var);
        hash_func.setFunction(expand_var_vector_hash_string);
        map_func_2.setFunction(map_distinct);

        _df._scope_ingress(loop_time)
           .flatmap(map_func_1)
           .shuffle(hash_func)
           ._scope_egress()
        //    距离不同，id相同保留哪个
           .flatmap(map_func_2);

        if (have_next == true)
        {
            _next_exe->setDf(std::move(_df));
            _next_exe->process();
        }
    }

private:
    Executor* _next_exe;
    DataFlow _df;
    bool have_next;
    int label_index;
    int ID_site;
    int distance_site;
    bool is_option;
    int loop_time;

};