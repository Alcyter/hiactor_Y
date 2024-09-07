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

#include "actor/barrier.act.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_barrier_process = 0,
	k_barrier_receive_eos = 1,
    k_barrier_setNext = 2,
    k_barrier_useBuffer = 3,
    k_file_source_receive_eos0 = 4,
    k_file_source_receive_eos1 = 5,
    k_file_source_receive_eos2 = 6,
    k_file_source_receive_eos3 = 7,
    k_file_source_receive_eos4 = 8,
    k_file_source_receive_eos5 = 9,
    k_file_source_receive_eos6 = 10,
    k_file_source_receive_eos7 = 11,
    k_file_source_receive_eos8 = 12,
    k_file_source_receive_eos9 = 13,
    k_file_source_receive_eos10 = 14,
    k_file_source_receive_eos11 = 15,
    k_file_source_receive_eos12 = 16,
    k_file_source_receive_eos13 = 17,
    k_file_source_receive_eos14 = 18,
    k_file_source_receive_eos15 = 19
};

BarrierActor_Ref::BarrierActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_BARRIER; }

void BarrierActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_process, std::forward<hiactor::DataType>(input));
}

void BarrierActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_receive_eos, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos0(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos0, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos1(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos1, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos2(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos2, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos3(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos3, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos4(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos4, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos5(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos5, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos6(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos6, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos7(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos7, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos8(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos8, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos9(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos9, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos10(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos10, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos11(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos11, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos12(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos12, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos13(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos13, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos14(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos14, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::receive_eos15(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
    hiactor::actor_client::send(addr, k_file_source_receive_eos15, std::forward<hiactor::Eos>(eos));
}

void BarrierActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_setNext, std::forward<hiactor::actor_config_Batch>(next));
}

void BarrierActor_Ref::useBuffer(hiactor::Eos &&use_buf) {
	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
	hiactor::actor_client::send(addr, k_barrier_useBuffer, std::forward<hiactor::Eos>(use_buf));
}

// seastar::future<hiactor::DataType> BarrierActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_BARRIER);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_barrier_send_back);
// }




void BarrierActor::setNext(hiactor::actor_config_Batch&& next) {
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

//后续修改，将输入数据解包成InternalValue后存入vector，最后包装为DataType
//输入tuple，输出vector<tuple>
//这里只负责收集所有map的数据并打包，不能进行join
void BarrierActor::process(hiactor::DataType&& input) {
    // std::cout << "BarrierActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';
    // std::cout << "eos received number is: " << _eos_rcv_num << '\n';
//    std::vector<hiactor::InternalValue> _input_data_buf = *input._data.vectorValue;// data_buffer
//    int id = input.job_id;
//    std::cout << "barrier process " << std::endl;
//    for (unsigned j = 0; j < _input_data_buf.size(); j++) {
//        hiactor::InternalValue _tuple = _input_data_buf[j];
//        _word_count_vec.push_back(_tuple);
//    }
    std::vector<hiactor::InternalValue> _input_data_buf = *input._data.vectorValue;// data_buffer
    for (unsigned j = 0; j < _input_data_buf.size(); j++) {
        hiactor::InternalValue _tuple = _input_data_buf[j];
        long long id = (*_tuple.vectorValue)[0].intValue;
        switch(id) {
            case 0 : { bucket0.push_back(_tuple); break;}
            case 1 : { bucket1.push_back(_tuple); break;}
            case 2 : { bucket2.push_back(_tuple); break;}
            case 3 : { bucket3.push_back(_tuple); break;}
            case 4 : { bucket4.push_back(_tuple); break;}
            case 5 : { bucket5.push_back(_tuple); break;}
            case 6 : { bucket6.push_back(_tuple); break;}
            case 7 : { bucket7.push_back(_tuple); break;}
            case 8 : { bucket8.push_back(_tuple); break;}
            case 9 : { bucket9.push_back(_tuple); break;}
            case 10 : { bucket10.push_back(_tuple); break;}
            case 11 : { bucket11.push_back(_tuple); break;}
            case 12 : { bucket12.push_back(_tuple); break;}
            case 13 : { bucket13.push_back(_tuple); break;}
            case 14 : { bucket14.push_back(_tuple); break;}
            case 15 : { bucket15.push_back(_tuple); break;}
        }
    }
}

void BarrierActor::receive_eos(hiactor::Eos&& eos) {
    if ((++_eos_rcv_num) % _expect_eos_num == 0) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_word_count_vec);

        if (_have_next) {
            //如果不使用data_buf，也在数据外面套一层vector，便于所有actor统一操作
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }
            
            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos0(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num0) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket0);
        _output.job_id = 0;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos0(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos1(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num1) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket1);
        _output.job_id = 1;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos1(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos2(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num2) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket2);
        _output.job_id = 2;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos2(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos3(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num3) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket3);
        _output.job_id = 3;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos3(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos4(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num4) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket4);
        _output.job_id = 4;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos4(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos5(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num5) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket5);
        _output.job_id = 5;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos5(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos6(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num6) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket6);
        _output.job_id = 6;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos6(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos7(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num7) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket7);
        _output.job_id = 7;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos7(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos8(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num8) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket8);
        _output.job_id = 8;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos8(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos9(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num9) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket9);
        _output.job_id = 9;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos9(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos10(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num10) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket10);
        _output.job_id = 10;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos10(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos11(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num11) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket11);
        _output.job_id = 11;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos11(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos12(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num12) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket12);
        _output.job_id = 12;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos12(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos13(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num13) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket13);
        _output.job_id = 13;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos13(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos14(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num14) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket14);
        _output.job_id = 14;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos14(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}

void BarrierActor::receive_eos15(hiactor::Eos&& eos) {
    if((++ _eos_rcv_num15) == _expect_eos_num) {
        hiactor::DataType _output;
        _output.type = hiactor::DataType::VECTOR;
        _output._data.vectorValue = new std::vector<hiactor::InternalValue>(bucket15);
        _output.job_id = 15;
        if (_have_next) {
            std::vector<hiactor::InternalValue> _buf;
            _buf.push_back(_output._data);
            _output._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);

            (_refs[hiactor::global_shard_id()]->process)(std::move(_output));
            if (!_have_send_data) { _have_send_data = true; }

            for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
                _refs[i]->receive_eos15(hiactor::Eos{_have_send_data});
            }
        } else {
            output();
        }
    }
}


void BarrierActor::useBuffer(hiactor::Eos&& use_buf) {
    // std::cout << "ShuffleActor::eos" << '\n';
    _use_buffer = use_buf.val;
}

void BarrierActor::emplace_data(unsigned dst_shard, hiactor::DataType&& data) {
    std::vector<hiactor::InternalValue> _vec = *_output_data_buf[dst_shard]._data.vectorValue;
    _vec.push_back(data._data);
    _output_data_buf[dst_shard].type = data.type;
    _output_data_buf[dst_shard]._data.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
    if(_vec.size() == _batch_sz) {
        (_refs[dst_shard]->process)(std::move(_output_data_buf[dst_shard]));
        _vec.clear();
    }
}


void BarrierActor::flush() {
    for(unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        if((*_output_data_buf[i]._data.vectorValue).size() > 0) {
            hiactor::DataType data;
            data._data.vectorValue = new std::vector<hiactor::InternalValue>(*_output_data_buf[i]._data.vectorValue);
            (_refs[i]->process)(std::move(_output_data_buf[i]));
            (*_output_data_buf[i]._data.vectorValue).clear();
        }
    }
}

// seastar::future<hiactor::DataType> BarrierActor::send_back() {
// 	// std::cout << "EndDataSink::send_back" << '\n';
//     return _refs[0]->send_back();
// }

void BarrierActor::output() {
    std::cout << "This shard is: " << hiactor::global_shard_id() << '\n';
    // for (unsigned i = 0; i < _output.size(); i++) {
    //     for (unsigned j = 0; j < _output[i].size(); j++) {
    //         std::cout << "Word: " << std::setw(6) << _output[i].words[j] << ", Count: " << _output[i].counts[j] << '\n';
    //     }
    // }
    
    if (++_output_count == hiactor::global_shard_count()) {
        std::cout << "Barrier Successful." << std::endl;
        hiactor::actor_engine().exit();
    }
    // std::cout << "default global shards number is:" << hiactor::global_shard_count() << std::endl;
}

seastar::future<hiactor::stop_reaction> BarrierActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "BarrierActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_barrier_process: {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_barrier_receive_eos: {
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
        case k_barrier_useBuffer: {
                useBuffer(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        case k_barrier_setNext: {
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

hiactor::registration::actor_registration<BarrierActor> _barrier_auto_registration(hiactor::TYPE_BARRIER);

} // namespace auto_registration