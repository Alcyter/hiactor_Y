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

#include "actor/shuffle.act.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_shuffle_process = 0,
	k_shuffle_receive_eos = 1,
	k_shuffle_setFunc = 2,
    k_shuffle_setNext = 3,
    k_shuffle_useBuffer = 4,
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

ShuffleActor_Ref::ShuffleActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_SHUFFLE; }

void ShuffleActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
	hiactor::actor_client::send(addr, k_shuffle_process, std::forward<hiactor::DataType>(input));
}

void ShuffleActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
	hiactor::actor_client::send(addr, k_shuffle_receive_eos, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos0(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos0, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos1(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos1, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos2(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos2, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos3(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos3, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos4(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos4, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos5(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos5, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos6(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos6, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos7(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos7, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos8(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos8, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos9(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos9, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos10(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos10, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos11(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos11, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos12(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos12, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos13(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos13, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos14(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos14, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::receive_eos15(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos15, std::forward<hiactor::Eos>(eos));
}

void ShuffleActor_Ref::setFunc(hiactor::HashFunc&& func) {
	addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
	hiactor::actor_client::send(addr, k_shuffle_setFunc, std::forward<hiactor::HashFunc>(func));
}

void ShuffleActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
	hiactor::actor_client::send(addr, k_shuffle_setNext, std::forward<hiactor::actor_config_Batch>(next));
}

void ShuffleActor_Ref::useBuffer(hiactor::Eos &&use_buf) {
	addr.set_method_actor_tid(hiactor::TYPE_SHUFFLE);
	hiactor::actor_client::send(addr, k_shuffle_useBuffer, std::forward<hiactor::Eos>(use_buf));
}

// seastar::future<hiactor::DataType> ShuffleActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_shuffle_send_back);
// }



//后续修改
//根据输入的字符串类型将_next设置成相应的OperatorBase
//输入scope builder和actor id直接生成一个Map
void ShuffleActor::setNext(hiactor::actor_config_Batch&& next) {
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
    }  else {
        std::cout << "Invalid actor type" << '\n';
        hiactor::actor_engine().exit();
    }
}


void ShuffleActor::setFunc(hiactor::HashFunc&& func) {
    _func = func;
}

//不论使不使用buffer，都在外面套一层vector，如果不使用，该vector就只有1个元素，如果使用，就有多个元素，交给下一个actor自己遍历
//可能处理两种类型数据：vector<string>或vector<tuple>, process函数内根据InternalValue类型自己判断
void ShuffleActor::process(hiactor::DataType&& input) {
    hiactor::DataType _output;

    std::vector<hiactor::InternalValue> _input_data_buf = *input._data.vectorValue;// data_buffer

    for (unsigned j = 0; j < _input_data_buf.size(); j++) {
        hiactor::InternalValue _vec = _input_data_buf[j];
//        std::vector<hiactor::InternalValue> _vec = *_input_data_buf[j].vectorValue;

        switch (input.type) {
            case hiactor::DataType::VECTOR: {// input:vector<string>*, output:string
                    // _vec is vector<string>
//                    for (unsigned i = 0; i < _vec.size(); i++) {
                        unsigned dst_shard = _func.process(_vec) % hiactor::global_shard_count();
                        _output = hiactor::DataType(_vec); //string
                        
                        if (_have_next && _use_buffer) {
                            emplace_data(dst_shard, std::move(_output));
                        } else if (_have_next){
                            //如果不使用data_buf，也在数据外面套一层vector，便于所有actor统一操作
                            std::vector<hiactor::InternalValue> _buf;
                            _buf.push_back(_output._data);
                            _output.type = hiactor::DataType::VECTOR;
                            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

                            (_refs[dst_shard]->process)(std::move(_output));
                        } else {
                            std::cout << "Line " << j << ": " << _output._data.stringValue << std::endl;
                        }
//                    }
                }
                break;
            default:
               unsigned dst_shard = _func.process(_vec) % hiactor::global_shard_count();
                        _output = hiactor::DataType(_vec); //string
                        
                        if (_have_next && _use_buffer) {
                            emplace_data(dst_shard, std::move(_output));
                        } else if (_have_next){
                            //如果不使用data_buf，也在数据外面套一层vector，便于所有actor统一操作
                            std::vector<hiactor::InternalValue> _buf;
                            _buf.push_back(_output._data);
                            _output.type = hiactor::DataType::VECTOR;
                            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

                            (_refs[dst_shard]->process)(std::move(_output));
                        } else {
                            std::cout << "Line " << j << ": " << _output._data.stringValue << std::endl;
                        }
                // std::cout << "Unknown data type" << std::endl;
                break;
        }
    }
    if (_have_next && _use_buffer) {
        flush();
        if (!_have_send_data) { _have_send_data = true; }
    }
    // delete _output._data.vectorValue;
}

void ShuffleActor::receive_eos(hiactor::Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos0(hiactor::Eos&& eos) {
    if (++_eos_rcv_num0 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos0(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos1(hiactor::Eos&& eos) {
    if (++_eos_rcv_num1 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos1(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos2(hiactor::Eos&& eos) {
    if (++_eos_rcv_num2 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos2(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos3(hiactor::Eos&& eos) {
    if (++_eos_rcv_num3 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos3(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos4(hiactor::Eos&& eos) {
    if (++_eos_rcv_num4 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos4(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos5(hiactor::Eos&& eos) {
    if (++_eos_rcv_num5 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos5(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos6(hiactor::Eos&& eos) {
    if (++_eos_rcv_num6 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos6(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos7(hiactor::Eos&& eos) {
    if (++_eos_rcv_num7 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos7(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos8(hiactor::Eos&& eos) {
    if (++_eos_rcv_num8 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos8(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos9(hiactor::Eos&& eos) {
    if (++_eos_rcv_num9 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos9(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos10(hiactor::Eos&& eos) {
    if (++_eos_rcv_num10 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos10(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos11(hiactor::Eos&& eos) {
    if (++_eos_rcv_num11 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos11(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos12(hiactor::Eos&& eos) {
    if (++_eos_rcv_num12 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos12(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos13(hiactor::Eos&& eos) {
    if (++_eos_rcv_num13 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos13(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos14(hiactor::Eos&& eos) {
    if (++_eos_rcv_num14 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos14(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::receive_eos15(hiactor::Eos&& eos) {
    if (++_eos_rcv_num15 == _expect_eos_num && _have_next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos15(hiactor::Eos{_have_send_data});
        }
    }
}

void ShuffleActor::useBuffer(hiactor::Eos&& use_buf) {
    // std::cout << "ShuffleActor::eos" << '\n';
    _use_buffer = use_buf.val;
}

void ShuffleActor::emplace_data(unsigned dst_shard, hiactor::DataType&& data) {
    // std::cout << "shuffle::emplace_data" << '\n';
    // std::vector<hiactor::InternalValue> _vec = *_output_data_buf[dst_shard]._data.vectorValue;
    // _vec.push_back(data._data);
    // _output_data_buf[dst_shard].type = data.type;
    // _output_data_buf[dst_shard]._data.vectorValue = new std::vector<hiactor::InternalValue>(_vec);

    (*_output_data_buf[dst_shard]._data.vectorValue).push_back(data._data);

    // if(data.type == hiactor::DataType::TUPLE) {
    //     std::cout << "send to shard " << dst_shard << '\n';
    //     for(unsigned i = 0; i < (*_output_data_buf[dst_shard]._data.vectorValue).size(); i++) {
    //         std::cout << "Tuple " << i << ": " << std::get<0>(*(*_output_data_buf[dst_shard]._data.vectorValue)[i].tupleValue).stringValue << ", " 
    //                                             << std::get<1>(*(*_output_data_buf[dst_shard]._data.vectorValue)[i].tupleValue).intValue << '\n';
    //     }
    // }
    
    // std::cout << "send to shard " << dst_shard << '\n';
    // for(unsigned i = 0; i < _vec.size(); i++) {
    //     std::cout << "Line: " << i << ": " << _vec[i].stringValue << std::endl;
    // }
    
    if((*_output_data_buf[dst_shard]._data.vectorValue).size() == _batch_sz) {
        hiactor::DataType data;
        data._data.vectorValue = new std::vector<hiactor::InternalValue>(*_output_data_buf[dst_shard]._data.vectorValue);
        (_refs[dst_shard]->process)(std::move(data));
        (*_output_data_buf[dst_shard]._data.vectorValue).clear();
    }
}


void ShuffleActor::flush() {
    // std::cout << "shuffle::flush" << '\n';
    for(unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        if((*_output_data_buf[i]._data.vectorValue).size() > 0) {
            hiactor::DataType data;
            data._data.vectorValue = new std::vector<hiactor::InternalValue>(*_output_data_buf[i]._data.vectorValue);
            // std::cout << "send to shard " << i << '\n';
            // for(unsigned j = 0; j < (*data._data.vectorValue).size(); j++) {
            //     std::cout << "Line: " << j << ": " << (*data._data.vectorValue)[j].stringValue << std::endl;
            // }

            (_refs[i]->process)(std::move(data));
            (*_output_data_buf[i]._data.vectorValue).clear();
            // delete data._data.vectorValue;
        }
    }
}

// seastar::future<hiactor::DataType> ShuffleActor::send_back() {
// 	// std::cout << "EndDataSink::send_back" << '\n';
//     return _refs[0]->send_back();
// }

// DataType设置为一个vector，储存原先的DataType数据，即用一个vector把原来的数据包起来
// 传入下一个actor之后，依次提取vector中的数据进行处理，相当于所有actor的process都要加一层循环，遍历vector数据
// void ShuffleActor::emplace_data(hiactor::DataType&& data) {
//     // _data_buf[dst_shard].emplace_back(data);
//     // if (_data_buf[dst_shard].size() == _data_buf[dst_shard].capacity()) {
//     //     process(dst_shard, std::move(_data_buf[dst_shard]));
//     //     _data_buf[dst_shard].reset(_batch_sz);
//     // }
// }

void ShuffleActor::output() {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        std::cout << "Send to shard: " << i << '\n';
        // for (unsigned j = 0; j < _output_0[i].size(); j++) {
        //     std::cout << _output_0[i].lines[j] << "\n";
        // }
    }
    std::cout << "Shuffle Successful." << std::endl;
    // std::cout << "default global shards number is:" << hiactor::global_shard_count() << std::endl;
}

seastar::future<hiactor::stop_reaction> ShuffleActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "ShuffleActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
        case k_shuffle_process: {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_shuffle_receive_eos: {
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
        case k_shuffle_useBuffer: {
                useBuffer(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_shuffle_setFunc: {
                setFunc(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::HashFunc>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_shuffle_setNext: {
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

hiactor::registration::actor_registration<ShuffleActor> _shuffle_auto_registration(hiactor::TYPE_SHUFFLE);

} // namespace auto_registration