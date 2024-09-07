#include "Executor.h"

hiactor::DataType reduce_function(const hiactor::DataType& input, std::function<hiactor::InternalValue(hiactor::InternalValue)> reduce_func);

unsigned key_shuffle_function(const hiactor::InternalValue& input, int key_site);

class ReduceByKey: public Executor{
public:

    ReduceByKey(const std::function<hiactor::InternalValue(hiactor::InternalValue)>& func, int key_site){
        this -> reduce_func = func;
        this -> key_site = key_site;
        have_next = false;
    }

    std::string get_type(){
        return "ReduceByKey";
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

        hiactor::HashFunc hash_func_1(key_site);
        hiactor::MapFunc map_func_1(reduce_func);

        hash_func_1.setFunction(key_shuffle_function);
        map_func_1.setFunction(reduce_function);


        _df.shuffle(hash_func_1)
           .barrier()
           .map_partition(map_func_1)
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
    std::function<hiactor::InternalValue(hiactor::InternalValue)> reduce_func;
    int key_site;

};