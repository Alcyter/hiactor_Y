#include "DataFlow/DataFlow.h"
#include "Project_node.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>



// hiactor::DataType map_project_node(const hiactor::DataType& input,  std::vector<int> ID_site, std::vector<int> label_index, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site)
// {
//     std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;
    

//     Graph_source* G =  Graph_source::GetInstance();
//     std::string null_value = "NULL";

//     for (unsigned i=0;i<label_index.size();i++)
//     {
//         long long ID_one = _input[ID_site[i]].intValue;
//         // long long ID_two = _input[ID_site[i*2+1]].intValue; 
//         std::vector<std::string> _property = propertyname[i];
//         if (ID_one == -1 )
//         {
//             for(unsigned j = 0; j < _property.size(); j++) {    
//                 hiactor::InternalValue _node;
//                 _node.stringValue = new char[null_value.length()+1];
//                 strcpy(_node.stringValue,null_value.c_str());
//                 _input.push_back(_node);
//             }
//         }       
//         else //node 信息
//         {
//             //std::unordered_map<long long, std::unordered_map<std::string,hiactor::InternalValue>> map_nodeinfo=G->node[labelname[i]];

//             //std::unordered_map<std::string,hiactor::InternalValue> map_property=map_nodeinfo[ID_one];
//             for(unsigned j = 0; j < _property.size(); j++) {
//                 //hiactor::InternalValue property_value = map_property[_property[j]];
//                 hiactor::InternalValue property_value = G->node[label_index[i]][ID_one][_property[j]];
//                 // hiactor::InternalValue property_value = G->node[labelname[i]][ID_one][_property[j]];

//                 _input.push_back(property_value);
//             }
//         }
        
               

//     }

//     std::vector<hiactor::InternalValue> output;
//     for (unsigned i=0;i<reserve_site.size();i++)
//     {
//         output.push_back(_input[reserve_site[i]]);
//     }
//     if (reserve_site.size()==0)
//         output = _input;

//     hiactor::DataType Data;
//     Data.type = hiactor::DataType::VECTOR;
//     Data._data.vectorValue = new std::vector<hiactor::InternalValue> (output);
//     return Data;

// }



// hiactor::DataType map_project_node(const hiactor::DataType& input,  std::vector<int> ID_site, std::vector<int> label_index, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site)
// {
//     // std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;

//     Graph_source* G =  Graph_source::GetInstance();
//     std::string null_value = "NULL";
//     hiactor::InternalValue _node;
//     for (unsigned i=0;i<label_index.size();i++)
//     {
//         long long ID_one = (*input._data.vectorValue)[ID_site[i]].intValue;
//         // long long ID_two = _input[ID_site[i*2+1]].intValue; 
//         // std::vector<std::string> _property = propertyname[i];
//         if (ID_one == -1 ){       
//             for(unsigned j = 0; j < propertyname[i].size(); j++) {    
               
//                 _node.stringValue = new char[null_value.length()+1];
//                 strcpy(_node.stringValue,null_value.c_str());
//                 (*input._data.vectorValue).push_back(_node);
//             }
//         }       
//         else{ //node 信息
//             for(unsigned j = 0; j < propertyname[i].size(); j++) {                
                
//                 (*input._data.vectorValue).push_back(G->node[label_index[i]][ID_one][propertyname[i][j]]);
//             }
//         }
                     
//     }

//     hiactor::DataType Data;
//     Data.type = hiactor::DataType::VECTOR;
//     Data._data.vectorValue = new std::vector<hiactor::InternalValue> ;

//     // std::vector<hiactor::InternalValue> output;
//     for (unsigned i=0;i<reserve_site.size();i++)    
//         (*Data._data.vectorValue).push_back((*input._data.vectorValue)[reserve_site[i]]);
    
//     if (reserve_site.size()==0)    
//         Data._data.vectorValue = input._data.vectorValue;

   
//     return Data;

// }


hiactor::DataType map_project_node(const hiactor::DataType& input,  std::vector<int> ID_site, std::vector<int> label_index, std::vector<std::vector<std::string>> propertyname, std::vector<int> reserve_site, hiactor::InternalValue& _node, std::vector<hiactor::InternalValue>& ori_path)
{
    // std::vector<hiactor::InternalValue> _input = *input._data.vectorValue;

    Graph_source* G =  Graph_source::GetInstance();
    std::string null_value = "NULL";
    // hiactor::InternalValue _node;
    long long ID_one;

    for (unsigned i=0;i<label_index.size();i++)
    {
        ID_one = (*input._data.vectorValue)[ID_site[i]].intValue;
        
        if (ID_one == -1 ){       
            for(unsigned j = 0; j < propertyname[i].size(); j++) {    
               
                _node.stringValue = new char[null_value.length()+1];
                strcpy(_node.stringValue,null_value.c_str());
                (*input._data.vectorValue).push_back(_node);
            }
        }       
        else{ //node 信息
            for(unsigned j = 0; j < propertyname[i].size(); j++) {                
                
                (*input._data.vectorValue).push_back(G->node[label_index[i]][ID_one][propertyname[i][j]]);
            }
        }
                     
    }  

    if (reserve_site.size()==0)     return input;

    ori_path.clear();
    for (unsigned i=0;i<reserve_site.size();i++)
    {
        ori_path.push_back((*input._data.vectorValue)[reserve_site[i]]);
    }

    (*input._data.vectorValue).clear();
    for (unsigned i=0; i<ori_path.size(); i++)
    {
        (*input._data.vectorValue).push_back(ori_path[i]);
    }   
       
    // hiactor::DataType output;
    // output.type = hiactor::DataType::VECTOR;
    // output._data.vectorValue = input._data.vectorValue;
    // input.type = hiactor::DataType::VECTOR;
    return input;
}

