#include "Executor.h"
#include <cstdarg>

hiactor::DataType map_expand(const hiactor::DataType& input, std::string labelname, int ID_site, int distance_site, bool is_option);

unsigned expand_vector_hash_string(const hiactor::InternalValue& input);

class Expand: public Executor{
public:
    Expand(std::string _label, int _site, int _distance_site, bool _is_option) {
        labelname = _label;
        ID_site = _site;
        distance_site = _distance_site;
        is_option = _is_option;
        have_next = false;
    }

    std::string get_type(){
        return "Expand";
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
        hiactor::MapFunc map_func_1(labelname, ID_site, distance_site, is_option);
        hiactor::HashFunc hash_func;

        map_func_1.setFunction(map_expand);
        hash_func.setFunction(expand_vector_hash_string);

        _df.flatmap(map_func_1)
           .shuffle(hash_func);

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
    std::string labelname;
    int ID_site;
    int distance_site;
    bool is_option;
};