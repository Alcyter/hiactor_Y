
#include "Executor.h"

//#include "Graph_source/Graph_source.h"

hiactor::DataType map_addbitset(const hiactor::DataType& input, int index, int label);

class AddBitset: public Executor{
public:
    AddBitset(int index, int label){
        this -> index = index;
        this -> label = label;
        have_next = false;
    }

    std::string get_type(){
        return "AddBitset";
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
        hiactor::MapFunc map_func_1(index, label);

        map_func_1.setFunction(map_addbitset);

        _df.map(map_func_1);

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
    int index;
    int label;
};