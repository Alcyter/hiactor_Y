
#include "Executor.h"

//#include "Graph_source/Graph_source.h"

hiactor::DataType map_ID_to_InternalRow(const hiactor::DataType& input, long long ID);

class NodeByIDScan: public Executor{
public:
    NodeByIDScan(long long _ID){
        ID = _ID;
        have_next = false;
    }

    std::string get_type(){
        return "NodeByIDScan";
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
        hiactor::MapFunc map_func_1(ID);

        map_func_1.setFunction(map_ID_to_InternalRow);

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
    long long ID;
};