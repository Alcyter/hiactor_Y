#include "DataFlow/DataFlow.h"
#include "Project.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>




hiactor::DataType map_project(const hiactor::DataType& input,  std::vector<int> ID_site, std::vector<std::string> labelname, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site)
{
    std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
    

    Graph_source* G =  Graph_source::GetInstance();
    std::string null_value = "NULL";

    for (unsigned i=0;i<labelname.size();i++)
    {
        long long ID_one = _input[ID_site[i*2]].intValue;
        long long ID_two = _input[ID_site[i*2+1]].intValue; 
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
        else if (ID_one == ID_two)  //表示需要node的属性
        {
            //std::unordered_map<long long, std::unordered_map<std::string,hiactor::InternalValue>> map_nodeinfo=G->node[labelname[i]];

            //std::unordered_map<std::string,hiactor::InternalValue> map_property=map_nodeinfo[ID_one];
            for(unsigned j = 0; j < _property.size(); j++) {
                //hiactor::InternalValue property_value = map_property[_property[j]];
                hiactor::InternalValue property_value = G->node[0][ID_one][_property[j]];
                // hiactor::InternalValue property_value = G->node[labelname[i]][ID_one][_property[j]];

                _input.push_back(property_value);
            }
        }
        else
        {
            //std::unordered_map<edge_tuple,std::unordered_map<std::string,hiactor::InternalValue>,myHash<edge_tuple>,myEqual<edge_tuple>> map_edgeinfo=G->edge[labelname[i]];
            edge_tuple _edge(ID_one,ID_two);
            //std::unordered_map<std::string,hiactor::InternalValue> map_property=map_edgeinfo[_edge];
            for(unsigned j = 0; j < _property.size(); j++) {
                // hiactor::InternalValue property_value = map_property[_property[j]];
                hiactor::InternalValue property_value = G->edge[0][_edge][_property[j]];

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




// hiactor::DataType map_project(const hiactor::DataType& input, std::string labelname, int ID_site_one, int ID_site_two, std::vector<std::string> propertynames)
// {

//     std::cout << "Hi, this is the extended class" << std::endl;
//     std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
//     std::vector<hiactor::InternalValue> output;

//     Graph_source* G =  Graph_source::GetInstance();
//     std::string null_value = "NULL";
    

//     if (ID_site_one == -2) //重组信息
//     {
        
//         for (unsigned i = 0; i<propertynames.size(); i++)
//         {
           
//             int _site = atoi(propertynames[i].c_str());
//             output.push_back(_input[_site]);
//         }
//     }
//     else
//     {
//         output = _input;
//         long long ID_one = _input[ID_site_one].intValue;
//         long long ID_two = _input[ID_site_two].intValue;
//         if (ID_one == -1) //当前位置没有数据，填NULL
//         {
//             for(unsigned i = 0; i < propertynames.size(); i++) {    
//                 hiactor::InternalValue _node;
//                 _node.stringValue = new char[null_value.length()+1];
//                 strcpy(_node.stringValue,null_value.c_str());
//                 output.push_back(_node);
//             }
//         }
//         else if (ID_site_one == ID_site_two)  //表示需要node的属性
//         {
//             std::unordered_map<long long, std::unordered_map<std::string,hiactor::InternalValue>> map_nodeinfo=G->node[labelname];

//             std::unordered_map<std::string,hiactor::InternalValue> map_property=map_nodeinfo[ID_one];
//             for(unsigned i = 0; i < propertynames.size(); i++) {
//                 hiactor::InternalValue property_value = map_property[propertynames[i]];
//                 output.push_back(property_value);
//             }
//         }
//         else{
//             if (ID_two == -1)  //边的信息缺失，填NULL
//             {
//                 for(unsigned i = 0; i < propertynames.size(); i++) {    
//                     hiactor::InternalValue _node;
//                     _node.stringValue = new char[null_value.length()+1];
//                     strcpy(_node.stringValue,null_value.c_str());
//                     output.push_back(_node);
//                 }
//             }
//             else  //正常添加信息
//             {
//                 std::unordered_map<edge_tuple,std::unordered_map<std::string,hiactor::InternalValue>,myHash<edge_tuple>,myEqual<edge_tuple>> map_edgeinfo=G->edge[labelname];
//                 edge_tuple _edge(ID_one,ID_two);
//                 std::unordered_map<std::string,hiactor::InternalValue> map_property=map_edgeinfo[_edge];
//                 for(unsigned i = 0; i < propertynames.size(); i++) {
//                     hiactor::InternalValue property_value = map_property[propertynames[i]];
//                     output.push_back(property_value);
//                 }
//             }
            
//         }
//     }    

//     // std::cout<<"after project size is:"<<output.size()<<"\n";

//     hiactor::DataType Data;
//     Data.type = hiactor::DataType::VECTOR;
//     Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);
//     return Data;
// }

