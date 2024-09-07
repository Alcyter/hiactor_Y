#include "Executor.h"
#include <cstdarg>

//#include "Graph_source/Graph_source.h"

hiactor::DataType map_project(const hiactor::DataType& input,  std::vector<int> ID_site, std::vector<std::string> labelname, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site);



class Project: public Executor{
public:
    // Project(std::string _label, int _site_one, int _site_two, std::initializer_list<std::string> strings)
    // : propertyname(strings) {
    //     labelname = _label;
    //     ID_site_one = _site_one;
    //     ID_site_two = _site_two;
    //     have_next = false;
    // }

    Project(std::vector<int> _site,std::vector<std::string> _label,std::vector<std::vector<std::string>> _properties, std::vector<int> _reserve_site)
    :  labelname(_label),  ID_site(_site), propertyname(_properties), reserve_site(_reserve_site)
    {

        
    }

    std::string get_type(){
        return "Project";
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
        hiactor::MapFunc map_func_1(ID_site,labelname,propertyname,reserve_site);

        map_func_1.setFunction(map_project);

        _df.map(map_func_1);

//         Map_project map_func(labelname,ID_site,propertyname);
//         _df.map(&map_func);

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
    std::vector<std::string> labelname;
    std::vector<int> ID_site;
    std::vector<std::vector<std::string>> propertyname;
    std::vector<int> reserve_site;

};