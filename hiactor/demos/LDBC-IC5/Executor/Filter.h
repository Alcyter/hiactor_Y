#include "Executor.h"

//#include "Graph_source/Graph_source.h"



hiactor::DataType filter_by_personID(const hiactor::DataType& input);
hiactor::DataType filter_by_joinDate(const hiactor::DataType& input);

hiactor::DataType filter_function(const hiactor::DataType& input, std::function<bool(hiactor::InternalValue)> customized_func);


class Filter: public Executor{
public:
    Filter(const std::function<bool(hiactor::InternalValue)>& func) {
        customized_func = func;
        have_next = false;
    }

    std::string get_type(){
        return "Filter";
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

        // if(type == "filter_by_personID")
        //     map_func_1.setFunction(filter_by_personID);
        // else    
        //     map_func_1.setFunction(filter_by_joinDate);

        map_func_1.setFunction(filter_function);

        _df.map_partition(map_func_1);

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
    std::string type;
    std::function<bool(hiactor::InternalValue)> customized_func;

};