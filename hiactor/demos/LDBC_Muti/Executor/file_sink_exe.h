#include "Executor.h"

class FileSinkExe : public Executor{
public:
    FileSinkExe(){    }
    FileSinkExe(Executor& exe):Executor(exe){
        
    }
    
    void process(){
//        _df.barrier()
//            .execute();
            _df.barrier()
               .execute();
//        _df.execute();
    }

    std::string get_type(){
        return "filesink";
    }
    void setNext(Executor* next_exe){}

    void setDf(DataFlow&& df){_df = df;}

private:
    Executor* _next_exe;
    DataFlow _df;
};