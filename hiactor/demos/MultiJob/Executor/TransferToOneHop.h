
#include "Executor.h"

//#include "Graph_source/Graph_source.h"

hiactor::DataType transfer(const hiactor::DataType& input, std::string name, int relation_name, int relation_expand);

class TransferToOneHop: public Executor{
public:
    TransferToOneHop(std::string name, int relation_name, int relation_expand){
        this -> name = name;
        this -> relation_name = relation_name;
        this -> relation_expand = relation_expand;
        have_next = false;
    }

    std::string get_type(){
        return "TransferToOneHop";
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
        hiactor::MapFunc map_func_1(name, relation_name, relation_expand);

        map_func_1.setFunction(transfer);

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
    std::string name;
    int relation_name;
    int relation_expand;
};