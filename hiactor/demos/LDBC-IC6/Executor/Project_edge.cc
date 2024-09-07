#include "DataFlow/DataFlow.h"
#include "Project_edge.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>




hiactor::DataType map_project_edge(const hiactor::DataType& input,  std::vector<int> ID_site_from, std::vector<int> ID_site_to, std::vector<int> label_index, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site)
{
    std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;

    Graph_source* G =  Graph_source::GetInstance();
    std::string null_value = "NULL";

    for (unsigned i=0;i<label_index.size();i++)
    {
        long long ID_one = _input[ID_site_from[i]].intValue;
        long long ID_two = _input[ID_site_to[i]].intValue; 
        std::vector<std::string> _property = propertyname[i];
        if (ID_one == -1 || ID_two == -1)
        {
            for(unsigned j = 0; j < _property.size(); j++) {    
                hiactor::InternalValue _node;
                _node.stringValue = new char[null_value.length()+1];
                strcpy(_node.stringValue,null_value.c_str());
                _input.push_back(_node);
            }
        }       
        else 
        {
            //std::unordered_map<edge_tuple,std::unordered_map<std::string,hiactor::InternalValue>,myHash<edge_tuple>,myEqual<edge_tuple>> map_edgeinfo=G->edge[labelname[i]];
            edge_tuple _edge(ID_one,ID_two);
            //std::unordered_map<std::string,hiactor::InternalValue> map_property=map_edgeinfo[_edge];
            for(unsigned j = 0; j < _property.size(); j++) {
                // hiactor::InternalValue property_value = map_property[_property[j]];
                hiactor::InternalValue property_value = G->edge[label_index[i]][_edge][_property[j]];

                // hiactor::InternalValue property_value = G->edge[labelname[i]][_edge][_property[j]];
                _input.push_back(property_value);
            }
        }
        
        
        

    }

    std::vector<hiactor::InternalValue> output;
    for (unsigned i=0;i<reserve_site.size();i++)
    {
        output.push_back(_input[reserve_site[i]]);
    }
    if (reserve_site.size()==0)
        output = _input;

    hiactor::DataType Data;
    Data.type = hiactor::DataType::VECTOR;
    Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);
    return Data;

}





