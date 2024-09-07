
#include<map>
#include<unordered_map>
#include<hiactor/util/data_type.hh>



template<class T>
struct myHash {
    std::size_t operator()(const T& key) const{
        std::size_t seed = 0;
        seed ^=std::hash<long long>()(key.ID_from) + 0x9e3779b9 + (seed << 6) + (seed << 2);
        seed ^=std::hash<long long>()(key.ID_to) + 0x9e3779b9 + (seed << 6) + (seed << 2);
        return seed;
    }
};

struct edge_tuple{
    edge_tuple(long long a,long long b)
    {
        ID_from = a;
        ID_to = b;
    }
    bool operator == (const edge_tuple& a) const{
        return ID_from == a.ID_from && ID_to == a.ID_to;
    }
    long long ID_from,ID_to;
};

template<class T>
struct myEqual{
    bool operator()(const T& a,const T& b)const{
        return a.ID_from == b.ID_from && a.ID_to == b.ID_to;
    }
};


class Graph_source{

private:
    
    Graph_source()
    {
        a_size = 5;
        // std::cout<<"Get_Node_data??\n";
        Get_Node_data();
        Get_Edge_data();
    }

    void Get_Node_data();

    void Get_Edge_data();

    void Get_Neighbor_data();    

    static Graph_source *G_Instance;
    

public:
    
    static Graph_source * GetInstance()
    {
        if (G_Instance == NULL)
            G_Instance = new Graph_source;
        //std::cout<<"here??\n";
        return G_Instance;
    }

    int a_size;
    // // <ID, <property_name, Value> >
    // std::unordered_map<long long ,std::unordered_map<std::string,hiactor::InternalValue>> node;
    // // <<ID1, ID2>, <label, Value> >
    // std::map<std::tuple<long long,long long>,std::unordered_map<std::string,hiactor::InternalValue>> edge;
    // // <label, <ID, vec<ID> > >
    // std::unordered_map<std::string,std::unordered_map<long long,std::vector<long long>>> neighbor;

    // <label, <ID, <property_name, Value>>>
    // std::unordered_map<std::string, std::unordered_map<long long, std::unordered_map<std::string,hiactor::InternalValue>>> node;
    // <label, <(ID,ID), <property_name, Value>>>
    // std::unordered_map<std::string, std::unordered_map<edge_tuple,std::unordered_map<std::string,hiactor::InternalValue>,myHash<edge_tuple>,myEqual<edge_tuple>>> edge;
    // <label, <ID, vec<ID>>>
    // std::unordered_map<std::string,std::unordered_map<long long,std::vector<long long>>> neighbor;

    std::vector<std::unordered_map<long long, std::unordered_map<std::string,hiactor::InternalValue>>> node;
    std::vector<std::unordered_map<edge_tuple,std::unordered_map<std::string,hiactor::InternalValue>,myHash<edge_tuple>,myEqual<edge_tuple>>> edge;
    std::vector<std::unordered_map<long long,std::vector<long long>>> neighbor;
    std::unordered_map<long long, int> bitset_map;


    // std::vector<std::unordered_map<long long,std::vector<long long>>> neighbor;
    
};

