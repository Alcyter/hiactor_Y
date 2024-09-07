#include "Executor.h"

//#include "Graph_source/Graph_source.h"

hiactor::DataType map_expandinto(const hiactor::DataType& input, int label_index, int site_from, int site_to);

hiactor::DataType map_expand_add(const hiactor::DataType& input, int label_index, int site_from, int site_to);

class ExpandInto: public Executor{
public:
    ExpandInto(int _label, int _site_from, int _site_to) {
        label_index = _label;
        site_from = _site_from;
        site_to = _site_to;
        have_next = false;
    }

    std::string get_type(){
        return "ExpandInto";
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
        hiactor::MapFunc map_func_1(label_index,site_from,site_to);

        map_func_1.setFunction(map_expand_add);

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
    int label_index;
    int site_from;
    int site_to;
};