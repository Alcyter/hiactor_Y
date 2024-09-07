#include "Executor.h"
#include <cstdarg>

hiactor::DataType map_expand_vec(const hiactor::DataType& input, int label_index, int ID_site, int distance_site, bool is_option, hiactor::InternalValue& node, std::vector<hiactor::InternalValue>& ori_path );

unsigned expand_vec_vector_hash_string(const hiactor::InternalValue& input);

class Expand_vec: public Executor{
public:
    Expand_vec(int _label, int _site, int _distance_site, bool _is_option) {
        label_index = _label;
        ID_site = _site;
        distance_site = _distance_site;
        is_option = _is_option;
        have_next = false;
    }

    std::string get_type(){
        return "Expand_vec";
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

        map_func_1.setFunction(map_expand_vec);
        hash_func.setFunction(expand_vec_vector_hash_string);

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
    int label_index;
    int ID_site;
    int distance_site;
    bool is_option;
};