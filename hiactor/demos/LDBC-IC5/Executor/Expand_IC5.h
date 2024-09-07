#include "Executor.h"
#include <cstdarg>

hiactor::DataType map_expand_IC5(const hiactor::DataType& input, std::function<bool(hiactor::InternalValue)> customized_func, std::bitset<100000>& visit);

unsigned expand_IC5_vector_hash_string(const hiactor::InternalValue& input);

class Expand_IC5: public Executor{
public:
    Expand_IC5(const std::function<bool(hiactor::InternalValue)>& func) {
        
        customized_func = func;
        have_next = false;
    }

    std::string get_type(){
        return "Expand_IC5";
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
        hiactor::MapFunc map_func_1(customized_func);
        hiactor::HashFunc hash_func_1;

        map_func_1.setFunction(map_expand_IC5);
        hash_func_1.setFunction(expand_IC5_vector_hash_string);
        

        
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
    std::function<bool(hiactor::InternalValue)> customized_func; 

};