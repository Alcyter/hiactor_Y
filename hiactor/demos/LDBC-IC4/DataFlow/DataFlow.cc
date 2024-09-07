#include "DataFlow.h"

// operation[startIndex - 1] = scope_ingress
// operation[endIndex] = scope_egress
unsigned buildOperators(hiactor::scope_builder& sc_builder, const std::vector<Operation>& operations, std::vector<hiactor::OperatorBase*>& operators, unsigned startIndex) {
    unsigned endIndex = startIndex;
    for (unsigned i = startIndex; i < operations.size(); i++) {
        endIndex = i;
        hiactor::OperatorBase* _operator;
        auto& op = operations[i];

        switch (op.type) {
            case _map_operator: {
                _operator = new Map(sc_builder, i);
                hiactor::MapFunc mapFuncCopy = op._map_func;
                _operator->setFunc(std::move(mapFuncCopy));
                operators.push_back(_operator);
                
                break;
            }
            case _shuffle_operator: {
                _operator = new Shuffle(sc_builder, i);
                hiactor::HashFunc hashFuncCopy = op._shuffle_func;
                _operator->setFunc(std::move(hashFuncCopy));
                operators.push_back(_operator);
                
                break;
            }
            case _barrier_operator: {
                _operator = new Barrier(sc_builder, i);
                operators.push_back(_operator);
                
                break;
            }
            case _flat_map_operator: {
                _operator = new FlatMap(sc_builder, i);
                hiactor::MapFunc mapFuncCopy = op._map_func;
                _operator->setFunc(std::move(mapFuncCopy));
                operators.push_back(_operator);
                
                break;
            }
            case _map_partition_operator: {
                _operator = new MapPartition(sc_builder, i);
                hiactor::MapFunc mapFuncCopy = op._map_func;
                _operator->setFunc(std::move(mapFuncCopy));
                operators.push_back(_operator);
                
                break;
            }
            case _scope_ingress_operator: {
                // _cur_loop_times = sc_builder.get_current_scope_id() + 1;
                // sc_builder.enter_sub_scope(hiactor::scope<hiactor::actor_group>(_cur_loop_time));
                for(int _cur_loop_times = 1 ; _cur_loop_times <= op.loop_num ; _cur_loop_times++ ){
                    sc_builder.enter_sub_scope(hiactor::scope<hiactor::actor_group>(_cur_loop_times));
                    endIndex = buildOperators(sc_builder, operations, operators, i+1);
                }
                i = endIndex;
                break;
            }
            case _scope_egress_operator: {
                sc_builder.back_to_parent_scope();

                return endIndex;
            }
            default: {
                std::cout << "Error. Invalid operator type." << '\n';
                hiactor::actor_engine().exit();
            }
        }

        if(operators.size() > 1) operators[operators.size() - 2]->setNext(std::move(*operators[operators.size() - 1]));
        
    }
    return endIndex;
}


 // 触发执行计划
void DataFlow::execute() {
    //改为全局变量，从main传递进来
    //需要一个外侧config，记录operator id的使用记录，加锁
    hiactor::scope_builder sc_builder;
    sc_builder
        .set_shard(0)
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(1));
    
    buildOperators(sc_builder, operations, operators, 0);

    hiactor::Source* _source_operator;
    if(_from_file) {
        _source_operator = new FileSource(sc_builder, 0);
    } else {
        _source_operator = new DataSource(sc_builder, 0);
    }

    hiactor::Sink* _sink_operator;
    _sink_operator = new FileSink(sc_builder, 0);

    // source -> operators -> sink
    _source_operator -> setNext(std::move(*operators[0]));
    operators[operators.size() - 1] -> setNext(std::move(*_sink_operator));

    _source_operator -> process(0, std::move(_file_name));
    _source_operator -> receive_eos();
}