#include "Executor.h"
#include <cstdarg>

hiactor::DataType map_expand_visit(const hiactor::DataType& input, int label_index, int ID_site, int distance_site, bool is_option, std::bitset<100000>& visit);

unsigned expand_visit_vector_hash_string(const hiactor::InternalValue& input);


class Visit_Expand: public Executor{
public:
    Visit_Expand(int _label, int _site, int _distance_site, bool _is_option) {
        label_index = _label;
        ID_site = _site;
        distance_site = _distance_site;
        is_option = _is_option;
        have_next = false;
    }

    std::string get_type(){
        return "Visit_Expand";
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
        hiactor::HashFunc hash_func_1;

        map_func_1.setFunction(map_expand_visit);
        hash_func_1.setFunction(expand_visit_vector_hash_string);
        

        
        _df.flatmap(map_func_1)
            .shuffle(hash_func_1);
        

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