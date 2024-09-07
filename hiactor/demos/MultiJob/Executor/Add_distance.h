
#include "Executor.h"

//#include "Graph_source/Graph_source.h"

hiactor::DataType map_Add_distance(const hiactor::DataType& input);

class Add_distance: public Executor{
public:
    Add_distance(){      
        have_next = false;
    }

    std::string get_type(){
        return "Add_distance";
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
        hiactor::MapFunc map_func;

        map_func.setFunction(map_Add_distance);

        _df.map(map_func);

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