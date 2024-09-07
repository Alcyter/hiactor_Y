#include "Executor.h"

hiactor::DataType reduce_function(const hiactor::DataType& input);

unsigned key_shuffle_function(const hiactor::InternalValue& input);

unsigned key_shuffle_function_two(const hiactor::InternalValue& input);


class ReduceByKey: public Executor{
public:

    ReduceByKey(){
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

        hiactor::HashFunc hash_func_1;
        hiactor::HashFunc hash_func_2;
        hiactor::MapFunc map_func_1;

        hash_func_1.setFunction(key_shuffle_function);
        hash_func_2.setFunction(key_shuffle_function_two);
        map_func_1.setFunction(reduce_function);

        //input : vec<vec<...>>
//        _df.barrier()   //output : vec<vec<vec<...>>>
//           .map_partition(map_func_1)  //output : vec<vec<...>>
//           .shuffle(hash_func_1)  //output : vec<vec<...>>
//           .barrier()  //output : vec<vec<vec<...>>>
//           .map_partition(map_func_2)  //output : vec<vec<...>>
//           .shuffle(hash_func_2);
        _df.shuffle(hash_func_1)
           .barrier()
           .map_partition(map_func_1)
           .shuffle(hash_func_2);

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