#include "DataFlow/DataFlow.h"
#include "Project.h"
#include "file_sink_exe.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <fstream>

#include "Timing.h"


hiactor::DataType filter_by_personCountry(const hiactor::DataType& input)
{
    // std::cout<<"------------------this is  filter_by_personCountry\n";
    // stopTiming();

    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string countryXName ="Vietnam";
    std::string countryYName ="Finland";
    

    for(unsigned i = 0; i < vec.size(); i++) {
        
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);

        // std::cout<<"the _vec size is"<<_vec.size()<<"\n";
        //std::cout<<"the _vec  "<<_vec[0].intValue<<"\n";
        //std::cout<<"the _vec  "<<_vec[1].intValue<<"\n";

        if(strcmp((*vec[i].vectorValue)[1].stringValue,countryXName.c_str()) == 0) continue;
        if(strcmp((*vec[i].vectorValue)[1].stringValue,countryYName.c_str()) == 0) continue;
        
        
        // hiactor::InternalValue val;
        // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
        tmp.push_back(vec[i]);
        
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_post(const hiactor::DataType& input)
{
    // std::cout<<"-----------------this is  filter_by_post\n";
    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string countryXName ="Vietnam";
    std::string countryYName ="Finland";
    std::string creationDate_start ="1275059059034";
    std::string creationDate_end ="1344152433150";
    long long creationDate_int_strat =  atoll(creationDate_start.c_str());
    long long creationDate_int_end =  atoll(creationDate_end.c_str());

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);
        long long _creationDate = atoll((*vec[i].vectorValue)[2].stringValue);

        if (_creationDate<creationDate_int_strat) continue;
        if (_creationDate>=creationDate_int_end) continue;

        if(strcmp((*vec[i].vectorValue)[1].stringValue,countryXName.c_str()) == 0)
        {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
        }
        else if(strcmp((*vec[i].vectorValue)[1].stringValue,countryYName.c_str()) == 0)
        {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
        }
              
        
        
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_personID(const hiactor::DataType& input)
{
    
    // std::cout<<"-----------------this is  filter_by_ID\n";
    // stopTiming();

    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    long long person_ID = 143;

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);        

        if((*(vec[i].vectorValue))[1].intValue != person_ID)
        {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
        }
        // else
        // {
        //     // std::cout<< "this id is 143\n";
        // }
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}

hiactor::DataType filter_by_post_time(const hiactor::DataType& input)
{
    // std::cout<<"-----------------this is  filter_by_post_time\n";
    // stopTiming();

    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string creationDate_start ="1275059059034";
    std::string creationDate_end ="1344152433150";
    long long creationDate_int_strat =  atoll(creationDate_start.c_str());
    long long creationDate_int_end =  atoll(creationDate_end.c_str());

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);
        long long _creationDate = atoll((*vec[i].vectorValue)[2].stringValue);

        if (_creationDate<creationDate_int_strat) continue;
        if (_creationDate>=creationDate_int_end) continue;
        
        tmp.push_back(vec[i]);
        
              
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}


hiactor::DataType filter_by_post_place(const hiactor::DataType& input)
{
    // std::cout<<"-----------------this is  filter_by_post_place\n";
    // stopTiming();

    std::vector<hiactor::InternalValue> vec = *(input._data.vectorValue);
    std::vector<hiactor::InternalValue> tmp;

    std::string countryXName ="Vietnam";
    std::string countryYName ="Finland";

    for(unsigned i = 0; i < vec.size(); i++) {
        // std::vector<hiactor::InternalValue> _vec = *(vec[i].vectorValue);

        if(strcmp((*vec[i].vectorValue)[1].stringValue,countryXName.c_str()) == 0)
        {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
            // std::cout<<"----I1D:"<<(*vec[i].vectorValue)[0].intValue<<" city name "<<(*vec[i].vectorValue)[1].stringValue<<" post ID"<<(*vec[i].vectorValue)[2].intValue<<"place ID"<<(*vec[i].vectorValue)[3].intValue<<"\n";
        }
        else if(strcmp((*vec[i].vectorValue)[1].stringValue,countryYName.c_str()) == 0)
        {
            // hiactor::InternalValue val;
            // val.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
            tmp.push_back(vec[i]);
            // std::cout<<"---I2D:"<<(*vec[i].vectorValue)[0].intValue<<" city name "<<(*vec[i].vectorValue)[1].stringValue<<" post ID"<<(*vec[i].vectorValue)[2].intValue<<"place ID"<<(*vec[i].vectorValue)[3].intValue<<"\n";

        }
        
    }
    hiactor::DataType output;
    output.type = hiactor::DataType::VECTOR;
    output._data.vectorValue = new std::vector<hiactor::InternalValue>(tmp);
    return output;
}
