#include "Executor.h"

hiactor::DataType select_top_k(const hiactor::DataType& input);

hiactor::DataType select_top_k_on_shard(const hiactor::DataType& input);

unsigned top_hash_string(const hiactor::InternalValue& input);

class Top: public Executor{
public:

    Top(){
        have_next = false;
    }

    std::string get_type(){
        return "Top";
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

        hiactor::MapFunc map_func_1;
        hiactor::HashFunc hash_func;
        hiactor::MapFunc map_func_2;

        map_func_1.setFunction(select_top_k_on_shard);
        hash_func.setFunction(top_hash_string);
        map_func_2.setFunction(select_top_k_on_shard);

        //input : vec<vec<...>>
        _df.barrier()   //output : vec<vec<vec<...>>>
           .map_partition(map_func_1)  //output : vec<vec<...>>
           .shuffle(hash_func)  //output : vec<vec<...>>
           .barrier()  //output : vec<vec<vec<...>>>
           .map_partition(map_func_2)  //output : vec<vec<vec<...>>>
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


};