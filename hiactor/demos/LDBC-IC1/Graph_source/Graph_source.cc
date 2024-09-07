
#include<iostream>
#include<fstream>
#include<sstream>
#include<vector>

#include"Graph_source/Graph_source.h"

 Graph_source * Graph_source::G_Instance = Graph_source::GetInstance();

void Graph_source::Get_Node_data() {
    // std::cout << "what??\n";
    std::string path="/home/yaojx/hiactor/demos/LDBC-IC1/Graph_source";
    std::vector<std::string> vec_fname;
    std::vector<std::string> vec_labelname;
    std::string fname = path + "/dynamic/comment_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/dynamic/forum_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/dynamic/person_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/dynamic/post_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/static/organisation_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/static/place_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/static/tag_0_0.csv";
    vec_fname.push_back(fname);
    fname = path + "/static/tagclass_0_0.csv";
    vec_fname.push_back(fname);


    // fname = "tag";
    // vec_labelname.push_back(fname);
    // fname = "post";
    // vec_labelname.push_back(fname);

    int index = 0;

    for (unsigned kk = 0; kk < vec_fname.size(); kk++) {
        std::ifstream csv_data(vec_fname[kk], std::ios::in);

        if (!csv_data.is_open())
            std::cout << "Error: opening file fail" << std::endl;
        else {
            std::string line;  //存储每一行的字符串
            std::string word;  //每行中每一个属性/值
            std::vector<std::string> properties_name; //用来存储CSV文件第一行，标题行
            std::istringstream sin;
            getline(csv_data, line);

            sin.clear();
            sin.str(line);  //设置sin扫描的字符串
            while (getline(sin, word, '|')) //sin扫描到 | 或者末尾 就停止，扫描到的字符串传给word
            {
//                std::cout << " preperties_name " << word << std::endl;
                properties_name.push_back(word);
            }
            long long ID;
            std::unordered_map<long long, std::unordered_map<std::string, hiactor::InternalValue>> ID_mapto_properties;
            while (getline(csv_data, line)) //每行处理
            {
                std::unordered_map<std::string, hiactor::InternalValue> propertyName_mapto_value;
                hiactor::InternalValue _str;
                sin.clear();
                sin.str(line);
                for (unsigned i = 0; i < properties_name.size(); i++) {
                    getline(sin, word, '|');
                    if (i == 0) //第一列是ID
                    {
                        ID = atoll(word.c_str());
                        if (kk == 2)
                        {
                            bitset_map[ID] = index;
                            index++; 
                        }
                    }
                    else {
                        _str.stringValue = new char[word.length() + 1];
                        strcpy(_str.stringValue, word.c_str());
                        propertyName_mapto_value[properties_name[i]] = _str;
//                        std::cout << " preperty " << word;
                    }
                }
                ID_mapto_properties[ID] = propertyName_mapto_value;
            }
//            std::cout<<"长度问题？"<<vec_labelname[kk]<<"\n";
            // node[vec_labelname[kk]] = ID_mapto_properties;
            node.push_back(ID_mapto_properties);
        }

    }
    std::cout<<"Get_Node_data success\n";

}

void Graph_source::Get_Edge_data()
{
    std::vector<std::string> vec_fname;
    std::vector<std::string> vec_labelname;
    std::string path="/home/yaojx/hiactor/demos/LDBC-IC1/Graph_source";

    std::string fname= path + "/dynamic/comment_hasCreator_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/comment_hasTag_tag_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/comment_isLocatedIn_place_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/comment_replyOf_comment_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/comment_replyOf_post_0_0.csv";
    vec_fname.push_back(fname);

    fname= path + "/dynamic/forum_containerOf_post_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/forum_hasMember_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/forum_hasModerator_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/forum_hasTag_tag_0_0.csv";
    vec_fname.push_back(fname);

    fname= path + "/dynamic/person_hasInterest_tag_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_isLocatedIn_place_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_knows_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_likes_comment_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_likes_post_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_studyAt_organisation_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_workAt_organisation_0_0.csv";
    vec_fname.push_back(fname);

    fname= path + "/dynamic/post_hasCreator_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/post_hasTag_tag_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/post_isLocatedIn_place_0_0.csv";
    vec_fname.push_back(fname);

    fname= path + "/static/organisation_isLocatedIn_place_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/static/place_isPartOf_place_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/static/tag_hasType_tagclass_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/static/tagclass_isSubclassOf_tagclass_0_0.csv";
    vec_fname.push_back(fname);


    fname= path + "/dynamic/post_hasCreator_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/comment_hasCreator_person_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_likes_post_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/comment_replyOf_post_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/person_hasInterest_tag_0_0.csv";
    vec_fname.push_back(fname);
    fname= path + "/dynamic/forum_hasMember_person_0_0.csv";
    vec_fname.push_back(fname);
  

    fname="person_knows_person";            vec_labelname.push_back(fname);
    fname="person_hasCreated_post";         vec_labelname.push_back(fname);
    fname="post_hasTag_tag";                vec_labelname.push_back(fname);


    for (unsigned kk=0;kk<vec_fname.size();kk++)
    {    
        std::ifstream csv_data(vec_fname[kk], std::ios::in);

        if (!csv_data.is_open())        
        std::cout<<"Error: opening file fail"<<std::endl;
        else
        {
            std::string line;
            std::string word;
            std::vector<std::string> properties_name;
            std::istringstream sin;
            getline(csv_data, line);

            sin.clear();
            sin.str(line);
            while (getline(sin,word,'|'))
            {
//                std::cout<<"properties_name"<<word<<std::endl;
                properties_name.push_back(word);
            }
            long long ID1,ID2;
            std::unordered_map<edge_tuple, std::unordered_map<std::string,hiactor::InternalValue>,myHash<edge_tuple>,myEqual<edge_tuple>> IDpair_mapto_properties;
            // std::string label_name=vec_labelname[kk];
            while (getline(csv_data, line))
            {
                std::unordered_map<std::string,hiactor::InternalValue> propertyName_mapto_value;
                hiactor::InternalValue _str;
                sin.clear();
                sin.str(line);
                for (unsigned i=0;i<properties_name.size();i++)
                {
                    getline(sin,word,'|');
                    if (i==0)
                        ID1=atoll(word.c_str());
                    else if(i==1)
                        ID2=atoll(word.c_str());                  
                    else
                    {
                        _str.stringValue= new char[word.length()+1];
                        strcpy(_str.stringValue,word.c_str());
                        propertyName_mapto_value[properties_name[i]]=_str;
//                        std::cout<<"property"<<word;
//                        std::cout<<properties_name[i]<<" "<<ID1<<"  "<<ID2<<std::endl;
                    }                    
                }
                if (kk>=23)  //建反向边
                {
                    //std::cout<<"string is the same\n";
                    long long ID3=ID1;
                    ID1=ID2;
                    ID2=ID3;
                }
            // 处理neighbor
                if (neighbor.size()<=kk)  //当前label下没有存储任何信息
                {
                    if(kk == 11) { //double edge
                        std::unordered_map<long long,std::vector<long long>> ID_mapto_neighbor;
                        std::vector<long long> ID1_neighbors,ID2_neighbors;
                        ID1_neighbors.push_back(ID2);
                        ID2_neighbors.push_back(ID1);
                        ID_mapto_neighbor[ID1]=ID1_neighbors;
                        ID_mapto_neighbor[ID2]=ID2_neighbors;
                        neighbor.push_back(ID_mapto_neighbor);
                    } else {
                        std::unordered_map<long long,std::vector<long long>> ID_mapto_neighbor;
                        std::vector<long long> ID1_neighbors;
                        ID1_neighbors.push_back(ID2);
                        ID_mapto_neighbor[ID1]=ID1_neighbors;
                        neighbor.push_back(ID_mapto_neighbor);
                    }
                            
                }
                else
                {
                    if(kk == 11) {     
                        if (neighbor[kk].find(ID2)!=neighbor[kk].end())
                            neighbor[kk][ID2].push_back(ID1);
                        else{
                            std::vector<long long> ID2_neighbors;
                            ID2_neighbors.push_back(ID1);
                            neighbor[kk][ID2]=ID2_neighbors;
                        }
                    } 
                    if (neighbor[kk].find(ID1)!=neighbor[kk].end())
                        neighbor[kk][ID1].push_back(ID2);
                    else{
                        std::vector<long long> ID1_neighbors;
                        ID1_neighbors.push_back(ID2);
                        neighbor[kk][ID1]=ID1_neighbors;
                    }                   
                            
                }                   

                if(kk == 11) {                    
                    edge_tuple person_pair2(ID2,ID1);                    
                    IDpair_mapto_properties[person_pair2]=propertyName_mapto_value;
                } 
                edge_tuple person_pair1(ID1,ID2);
                IDpair_mapto_properties[person_pair1]=propertyName_mapto_value;   
            }
            edge.push_back(IDpair_mapto_properties);
        }

    }
    
    std::cout<<"Get_Edge_data success\n";


}