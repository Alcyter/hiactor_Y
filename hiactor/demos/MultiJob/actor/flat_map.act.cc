/** Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "actor/flat_map.act.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_flat_map_process = 0,
	k_flat_map_receive_eos = 1,
	k_flat_map_setFunc = 2,
    k_flat_map_setNext = 3,
    k_flat_map_useBuffer = 4,
    k_file_source_receive_eos0 = 5,
    k_file_source_receive_eos1 = 6,
    k_file_source_receive_eos2 = 7,
    k_file_source_receive_eos3 = 8,
    k_file_source_receive_eos4 = 9,
    k_file_source_receive_eos5 = 10,
    k_file_source_receive_eos6 = 11,
    k_file_source_receive_eos7 = 12,
    k_file_source_receive_eos8 = 13,
    k_file_source_receive_eos9 = 14,
    k_file_source_receive_eos10 = 15,
    k_file_source_receive_eos11 = 16,
    k_file_source_receive_eos12 = 17,
    k_file_source_receive_eos13 = 18,
    k_file_source_receive_eos14 = 19,
    k_file_source_receive_eos15 = 20
};

FlatMapActor_Ref::FlatMapActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_FLATMAP; }

void FlatMapActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_process, std::forward<hiactor::DataType>(input));
}

void FlatMapActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_receive_eos, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos0(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos0, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos1(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos1, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos2(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos2, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos3(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos3, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos4(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos4, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos5(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos5, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos6(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos6, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos7(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos7, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos8(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos8, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos9(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos9, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos10(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos10, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos11(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos11, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos12(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos12, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos13(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos13, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos14(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos14, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::receive_eos15(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
    hiactor::actor_client::send(addr, k_file_source_receive_eos15, std::forward<hiactor::Eos>(eos));
}

void FlatMapActor_Ref::setFunc(hiactor::MapFunc &&func) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_setFunc, std::forward<hiactor::MapFunc>(func));
}

void FlatMapActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_setNext, std::forward<hiactor::actor_config_Batch>(next));
}

void FlatMapActor_Ref::useBuffer(hiactor::Eos &&use_buf) {
	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
	hiactor::actor_client::send(addr, k_flat_map_useBuffer, std::forward<hiactor::Eos>(use_buf));
}

// seastar::future<hiactor::DataType> FlatMapActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_FLATMAP);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_flat_map_send_back);
// }




void FlatMapActor::setNext(hiactor::actor_config_Batch&& next) {
    auto builder = next[0]._builder;
    auto ds_op_id = next[0]._ds_op_id;
    _next_actor_type = next[0]._actor_type;
    _have_next = true;

    if (_next_actor_type == "shuffle") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<ShuffleActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "map") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<MapActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "map_partition") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<MapPartitionActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "flat_map") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FlatMapActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "barrier") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<BarrierActor_Ref>(ds_op_id));
        }
    } else if (_next_actor_type == "file_sink") {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FileSinkActor_Ref>(ds_op_id));
        }
    } else {
        std::cout << "Invalid actor type" << '\n';
        hiactor::actor_engine().exit();
    }
}

void FlatMapActor::setFunc(hiactor::MapFunc&& func) {
    // std::cout << "setFunc" << '\n';
    _func = func;
}

//可能接收两种类型，具体内容用户自己编译
void FlatMapActor::process(hiactor::DataType&& input) {
    // std::cout << "FlatMapActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';

    std::vector<hiactor::InternalValue> _buf;

    std::vector<hiactor::InternalValue> _input_data_buf = *input._data.vectorValue;// data_buffer
    
    for (unsigned j = 0; j < _input_data_buf.size(); j++) {
        hiactor::DataType _intermediate_result;
        if(_func.get_is_visited_expand() == true) {
            switch((*_input_data_buf[j].vectorValue)[0].intValue) {
                case 0 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit0); break;}
                case 1 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit1); break;}
                case 2 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit2); break;}
                case 3 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit3); break;}
                case 4 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit4); break;}
                case 5 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit5); break;}
                case 6 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit6); break;}
                case 7 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit7); break;}
                case 8 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit8); break;}
                case 9 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit9); break;}
                case 10 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit10); break;}
                case 11 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit11); break;}
                case 12 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit12); break;}
                case 13 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit13); break;}
                case 14 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit14); break;}
                case 15 : { _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]), visit15); break;}
            }
        } else {
            _intermediate_result = _func.process(hiactor::DataType(_input_data_buf[j]),_node,_ori_path);
        }
        for(unsigned i = 0; i < (*_intermediate_result._data.vectorValue).size(); i++) {
            _buf.push_back((*_intermediate_result._data.vectorValue)[i]);
        }
        _output.type = _intermediate_result.type;
    }
    if (_have_next) {
        //如果不使用data_buf，也在数据外面套一层vector，便于所有actor统一操作
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);
        (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
    }
}

void FlatMapActor::receive_eos(hiactor::Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos0(hiactor::Eos&& eos) {
    if (++_eos_rcv_num0 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos0(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos1(hiactor::Eos&& eos) {
    if (++_eos_rcv_num1 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos1(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos2(hiactor::Eos&& eos) {
    if (++_eos_rcv_num2 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos2(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos3(hiactor::Eos&& eos) {
    if (++_eos_rcv_num3 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos3(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos4(hiactor::Eos&& eos) {
    if (++_eos_rcv_num4 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos4(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos5(hiactor::Eos&& eos) {
    if (++_eos_rcv_num5 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos5(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos6(hiactor::Eos&& eos) {
    if (++_eos_rcv_num6 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos6(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos7(hiactor::Eos&& eos) {
    if (++_eos_rcv_num7 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos7(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos8(hiactor::Eos&& eos) {
    if (++_eos_rcv_num8 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos8(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos9(hiactor::Eos&& eos) {
    if (++_eos_rcv_num9 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos9(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos10(hiactor::Eos&& eos) {
    if (++_eos_rcv_num10 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos10(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos11(hiactor::Eos&& eos) {
    if (++_eos_rcv_num11 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos11(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos12(hiactor::Eos&& eos) {
    if (++_eos_rcv_num12 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos12(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos13(hiactor::Eos&& eos) {
    if (++_eos_rcv_num13 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos13(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos14(hiactor::Eos&& eos) {
    if (++_eos_rcv_num14 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos14(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::receive_eos15(hiactor::Eos&& eos) {
    if (++_eos_rcv_num15 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos15(hiactor::Eos{_have_send_data});
        }
    }
}

void FlatMapActor::emplace_data(unsigned dst_shard, hiactor::DataType&& data) {
    std::vector<hiactor::InternalValue> _vec = *_output_data_buf[dst_shard]._data.vectorValue;
    _vec.push_back(data._data);
    _output_data_buf[dst_shard].type = data.type;
    _output_data_buf[dst_shard]._data.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
    if(_vec.size() == _batch_sz) {
        (_refs[dst_shard]->process)(std::move(_output_data_buf[dst_shard]));
        _vec.clear();
    }
}


void FlatMapActor::flush() {
    for(unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        if((*_output_data_buf[i]._data.vectorValue).size() > 0) {
            hiactor::DataType data;
            data._data.vectorValue = new std::vector<hiactor::InternalValue>(*_output_data_buf[i]._data.vectorValue);
            (_refs[i]->process)(std::move(_output_data_buf[i]));
            (*_output_data_buf[i]._data.vectorValue).clear();
        }
    }
}



void FlatMapActor::output() {

}

seastar::future<hiactor::stop_reaction> FlatMapActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "FlatMapActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_flat_map_process: {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_flat_map_receive_eos: {
                receive_eos(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_file_source_receive_eos0: {
            receive_eos0(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos1: {
            receive_eos1(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos2: {
            receive_eos2(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos3: {
            receive_eos3(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos4: {
            receive_eos4(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos5: {
            receive_eos5(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos6: {
            receive_eos6(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos7: {
            receive_eos7(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos8: {
            receive_eos8(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos9: {
            receive_eos9(std::move(reinterpret_cast<
                                           hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos10: {
            receive_eos10(std::move(reinterpret_cast<
                                            hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos11: {
            receive_eos11(std::move(reinterpret_cast<
                                            hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos12: {
            receive_eos12(std::move(reinterpret_cast<
                                            hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos13: {
            receive_eos13(std::move(reinterpret_cast<
                                            hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos14: {
            receive_eos14(std::move(reinterpret_cast<
                                            hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
        case k_file_source_receive_eos15: {
            receive_eos15(std::move(reinterpret_cast<
                                            hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
        }
		case k_flat_map_setFunc: {
                setFunc(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::MapFunc>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_flat_map_setNext: {
                setNext(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::actor_config_Batch>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		default: {
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::yes);
		}
	}
}

namespace auto_registration {

hiactor::registration::actor_registration<FlatMapActor> _flat_map_auto_registration(hiactor::TYPE_FLATMAP);

} // namespace auto_registration