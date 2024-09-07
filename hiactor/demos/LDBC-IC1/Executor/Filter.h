#include "Executor.h"

//#include "Graph_source/Graph_source.h"

hiactor::DataType distinct(const hiactor::DataType& input);

hiactor::DataType filter_by_firstName(const hiactor::DataType& input);

class Filter: public Executor{
public:
    Filter(std::string str) {
        type = str;
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
        hiactor::MapFunc map_func_1;

        if(type == "distinct") {
            map_func_1.setFunction(distinct);
        } else {
            map_func_1.setFunction(filter_by_firstName);
        }

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
};