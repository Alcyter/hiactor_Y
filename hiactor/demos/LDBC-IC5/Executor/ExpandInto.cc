#include "DataFlow/DataFlow.h"
#include "ExpandInto.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>


hiactor::DataType map_expandinto(const hiactor::DataType& input, int label_index, int site_from, int site_to)
{
    //std::cout<<"-----------------this is  filter_by_personID\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    Graph_source* G =  Graph_source::GetInstance();

    // std::unordered_map<long long, std::vector<long long>> map_neighbor = G -> neighbor[label_index];

    for(unsigned i = 0; i < vec.size(); i++) {
        std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        
        
        edge_tuple person_with_post(_vec[site_from].intValue,_vec[site_to].intValue);

        // if ( G->edge_index.find(person_with_post) != G->edge_index.end())
        // {
        //     tmp.push_back(vec[i]);
        // }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}
