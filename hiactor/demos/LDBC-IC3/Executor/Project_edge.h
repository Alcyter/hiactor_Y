#include "Executor.h"
#include <cstdarg>

hiactor::DataType map_project_edge(const hiactor::DataType& input,  std::vector<int> ID_site_from, std::vector<int> ID_site_to, std::vector<int> label_index, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site);

class Project_edge: public Executor{
public:
    Project_edge(std::vector<int> _site_from,std::vector<int> _site_to ,std::vector<int> _label_index,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site)
    :  label_index(_label_index),  ID_site_from(_site_from), ID_site_to(_site_to), propertyname(_properties), reserve_site(_reserve_site)
    {

    }

    std::string get_type(){
        return "Project_edge";
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
        hiactor::MapFunc map_func_1(ID_site_from, ID_site_to,label_index,propertyname,reserve_site);

        map_func_1.setFunction(map_project_edge);

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
    std::vector<int> label_index;
    std::vector<int> ID_site_from;
    std::vector<int> ID_site_to;
    std::vector<std::vector<std::string>> propertyname;
    std::vector<int> reserve_site;

};
