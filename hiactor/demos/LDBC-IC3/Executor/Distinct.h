#include "Executor.h"

hiactor::DataType distinct_on_shard(const hiactor::DataType& input);

unsigned distinct_hash_string(const hiactor::InternalValue& input);

unsigned distinct_shuffle_hash_string(const hiactor::InternalValue& input);

class Distinct: public Executor{
public:

    Distinct(){
        have_next = false;
    }

    std::string get_type(){
        return "Distinct";
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
        hiactor::HashFunc hash_func_1;
        hiactor::MapFunc map_func_2;
        hiactor::HashFunc hash_func_2;

        map_func_1.setFunction(distinct_on_shard);
        hash_func_1.setFunction(distinct_hash_string);
        map_func_2.setFunction(distinct_on_shard);
        hash_func_2.setFunction(distinct_shuffle_hash_string);

        //input : vec<vec<...>>
        _df.barrier()   //output : vec<vec<vec<...>>>
           .map_partition(map_func_1)  //output : vec<vec<...>>
           .shuffle(hash_func_1)  //output : vec<vec<...>>
           .barrier()  //output : vec<vec<vec<...>>>
           .map_partition(map_func_2);  //output : vec<vec<...>>
        //    .shuffle(hash_func_2);

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