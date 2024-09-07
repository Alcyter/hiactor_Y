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

#include "actor/file_source.act.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_file_source_process = 0,
	k_file_source_receive_eos = 1,
    k_file_source_setNext = 2,
    k_file_source_receive_eos0 = 3,
    k_file_source_receive_eos1 = 4,
    k_file_source_receive_eos2 = 5,
    k_file_source_receive_eos3 = 6,
    k_file_source_receive_eos4 = 7,
    k_file_source_receive_eos5 = 8,
    k_file_source_receive_eos6 = 9,
    k_file_source_receive_eos7 = 10,
    k_file_source_receive_eos8 = 11,
    k_file_source_receive_eos9 = 12,
    k_file_source_receive_eos10 = 13,
    k_file_source_receive_eos11 = 14,
    k_file_source_receive_eos12 = 15,
    k_file_source_receive_eos13 = 16,
    k_file_source_receive_eos14 = 17,
    k_file_source_receive_eos15 = 18
    // k_file_source_send_back = 3,
};

FileSourceActor_Ref::FileSourceActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_FILESOURCE; }

void FileSourceActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
	hiactor::actor_client::send(addr, k_file_source_process, std::forward<hiactor::DataType>(input));
}

void FileSourceActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
	hiactor::actor_client::send(addr, k_file_source_receive_eos, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos0(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos0, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos1(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos1, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos2(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos2, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos3(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos3, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos4(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos4, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos5(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos5, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos6(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos6, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos7(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos7, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos8(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos8, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos9(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos9, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos10(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos10, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos11(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos11, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos12(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos12, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos13(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos13, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos14(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos14, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::receive_eos15(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
    hiactor::actor_client::send(addr, k_file_source_receive_eos15, std::forward<hiactor::Eos>(eos));
}

void FileSourceActor_Ref::setNext(hiactor::actor_config_Batch&& next) {
	addr.set_method_actor_tid(hiactor::TYPE_FILESOURCE);
	hiactor::actor_client::send(addr, k_file_source_setNext, std::forward<hiactor::actor_config_Batch>(next));
}




void FileSourceActor::setNext(hiactor::actor_config_Batch&& next) {
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


//从文件夹读取数据后直接发送，相当于map_sample.cc中定义的readFile函数
void FileSourceActor::process(hiactor::DataType&& input) {
    // std::cout << "FileSourceActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';
    std::cout << "Process id: " << input.job_id << std::endl;

    std::string filename = "/home/yaojx/hiactor/demos/LDBC-IC5/actor/node.txt";
    hiactor::DataType data;//vector<string>*
    std::ifstream inputFile(filename);

    if (!inputFile.is_open()) {
        std::cerr << "Error: Unable to open input file." << std::endl;
        hiactor::actor_engine().exit();
    }

    std::string line;
    std::vector<hiactor::InternalValue> _vec; //vector<string>
    hiactor::InternalValue _str; //string
    while (std::getline(inputFile, line)) {

        _str.stringValue = new char[line.length() + 1];
        strcpy(_str.stringValue, line.c_str()); //string to char*
        _vec.push_back(_str);
    }

    inputFile.close();

    //在数据外多套一层data_buf，便于所有actor统一操作
    std::vector<hiactor::InternalValue> _buf;
    hiactor::InternalValue _data;
    _data.vectorValue = new std::vector<hiactor::InternalValue>(_vec);
    _buf.push_back(_data);

    data.type = hiactor::DataType::VECTOR;
    data._data.vectorValue = new std::vector<hiactor::InternalValue>(_buf);
    data.job_id = input.job_id;

    std::cout << (*(*data._data.vectorValue)[0].vectorValue)[0].stringValue << std::endl;
    
    if (_have_next) {
        (_refs[hiactor::global_shard_id()]->process)(std::move(data));
        if (!_have_send_data) { _have_send_data = true; }
    } else {
        std::cout << "Error! No operators to process data!" << '\n';
        hiactor::actor_engine().exit();
    }
}

void FileSourceActor::receive_eos(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos0(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos0(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos1(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos1(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos2(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos2(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos3(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos3(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos4(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos4(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos5(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos5(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos6(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos6(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos7(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos7(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos8(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos8(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos9(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos9(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos10(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos10(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos11(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos11(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos12(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos12(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos13(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos13(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos14(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos14(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::receive_eos15(hiactor::Eos&& eos) {
    for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
        _refs[i]->receive_eos15(hiactor::Eos{_have_send_data});
    }
}

void FileSourceActor::output() {

}

seastar::future<hiactor::stop_reaction> FileSourceActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "FileSourceActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_file_source_process: {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_file_source_receive_eos: {
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
        case k_file_source_setNext: {
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

hiactor::registration::actor_registration<FileSourceActor> _file_source_auto_registration(hiactor::TYPE_FILESOURCE);

} // namespace auto_registration