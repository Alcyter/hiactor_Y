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

#include "actor/file_sink.act.h"
#include "Executor/Timing.h"
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_file_sink_process = 0,
	k_file_sink_receive_eos = 1,
    k_file_source_receive_eos0 = 2,
    k_file_source_receive_eos1 = 3,
    k_file_source_receive_eos2 = 4,
    k_file_source_receive_eos3 = 5,
    k_file_source_receive_eos4 = 6,
    k_file_source_receive_eos5 = 7,
    k_file_source_receive_eos6 = 8,
    k_file_source_receive_eos7 = 9,
    k_file_source_receive_eos8 = 10,
    k_file_source_receive_eos9 = 11,
    k_file_source_receive_eos10 = 12,
    k_file_source_receive_eos11 = 13,
    k_file_source_receive_eos12 = 14,
    k_file_source_receive_eos13 = 15,
    k_file_source_receive_eos14 = 16,
    k_file_source_receive_eos15 = 17
    // k_file_sink_send_back = 2,
};

FileSinkActor_Ref::FileSinkActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_FILESINK; }

void FileSinkActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
	hiactor::actor_client::send(addr, k_file_sink_process, std::forward<hiactor::DataType>(input));
}

void FileSinkActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
	hiactor::actor_client::send(addr, k_file_sink_receive_eos, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos0(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos0, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos1(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos1, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos2(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos2, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos3(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos3, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos4(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos4, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos5(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos5, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos6(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos6, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos7(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos7, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos8(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos8, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos9(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos9, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos10(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos10, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos11(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos11, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos12(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos12, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos13(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos13, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos14(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos14, std::forward<hiactor::Eos>(eos));
}

void FileSinkActor_Ref::receive_eos15(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_FILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos15, std::forward<hiactor::Eos>(eos));
}


// seastar::future<hiactor::DataType> FileSinkActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_DATASINK);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_file_sink_send_back);
// }



//直接将收集到的数据写入对应shard的文件
void FileSinkActor::process(hiactor::DataType&& input) {
    // std::cout << "FileSinkActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';
    std::vector<hiactor::InternalValue> vec = *input._data.vectorValue;
    for(auto j : vec) {
        std::vector<hiactor::InternalValue> v = *j.vectorValue;
        for(auto i : v) {
            long long id = (*i.vectorValue)[0].intValue;
            switch(id) {
                case 0 : { _output0.push_back(i); break; }
                case 1 : { _output1.push_back(i); break; }
                case 2 : { _output2.push_back(i); break; }
                case 3 : { _output3.push_back(i); break; }
                case 4 : { _output4.push_back(i); break; }
                case 5 : { _output5.push_back(i); break; }
                case 6 : { _output6.push_back(i); break; }
                case 7 : { _output7.push_back(i); break; }
                case 8 : { _output8.push_back(i); break; }
                case 9 : { _output9.push_back(i); break; }
                case 10 : { _output10.push_back(i); break; }
                case 11 : { _output11.push_back(i); break; }
                case 12 : { _output12.push_back(i); break; }
                case 13 : { _output13.push_back(i); break; }
                case 14 : { _output14.push_back(i); break; }
                case 15 : { _output15.push_back(i); break; }
            }
        }
    }
    _ref->process(std::move(_output));
    _have_send_data = true;
}

void FileSinkActor::receive_eos(hiactor::Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        output(0);
        _ref->receive_eos(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos0(hiactor::Eos&& eos) {
    if (++_eos_rcv_num0 == _expect_eos_num) {
        output(0);
        _ref->receive_eos0(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos1(hiactor::Eos&& eos) {
    if (++_eos_rcv_num1 == _expect_eos_num) {
        output(1);
        _ref->receive_eos1(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos2(hiactor::Eos&& eos) {
    if (++_eos_rcv_num2 == _expect_eos_num) {
        output(2);
        _ref->receive_eos2(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos3(hiactor::Eos&& eos) {
    if (++_eos_rcv_num3 == _expect_eos_num) {
        output(3);
        _ref->receive_eos3(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos4(hiactor::Eos&& eos) {
    if (++_eos_rcv_num4 == _expect_eos_num) {
        output(4);
        _ref->receive_eos4(hiactor::Eos{_have_send_data});
    }
}
void FileSinkActor::receive_eos5(hiactor::Eos&& eos) {
    if (++_eos_rcv_num5 == _expect_eos_num) {
        output(5);
        _ref->receive_eos5(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos6(hiactor::Eos&& eos) {
    if (++_eos_rcv_num6 == _expect_eos_num) {
        output(6);
        _ref->receive_eos6(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos7(hiactor::Eos&& eos) {
    if (++_eos_rcv_num7 == _expect_eos_num) {
        output(7);
        _ref->receive_eos7(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos8(hiactor::Eos&& eos) {
    if (++_eos_rcv_num8 == _expect_eos_num) {
        output(8);
        _ref->receive_eos8(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos9(hiactor::Eos&& eos) {
    if (++_eos_rcv_num9 == _expect_eos_num) {
        output(9);
        _ref->receive_eos9(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos10(hiactor::Eos&& eos) {
    if (++_eos_rcv_num10 == _expect_eos_num) {
        output(10);
        _ref->receive_eos10(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos11(hiactor::Eos&& eos) {
    if (++_eos_rcv_num11 == _expect_eos_num) {
        output(11);
        _ref->receive_eos11(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos12(hiactor::Eos&& eos) {
    if (++_eos_rcv_num12 == _expect_eos_num) {
        output(12);
        _ref->receive_eos12(hiactor::Eos{_have_send_data});
    }
}
void FileSinkActor::receive_eos13(hiactor::Eos&& eos) {
    if (++_eos_rcv_num13 == _expect_eos_num) {
        output(13);
        _ref->receive_eos13(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos14(hiactor::Eos&& eos) {
    if (++_eos_rcv_num14 == _expect_eos_num) {
        output(14);
        _ref->receive_eos14(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::receive_eos15(hiactor::Eos&& eos) {
    if (++_eos_rcv_num15 == _expect_eos_num) {
        output(15);
        _ref->receive_eos15(hiactor::Eos{_have_send_data});
    }
}

void FileSinkActor::output(int t_id) {
    stopTiming();
    std::vector<hiactor::InternalValue> _buf;
    switch (t_id) {
        case 0 : { _buf = _output0; break; }
        case 1 : { _buf = _output1; break; }
        case 2 : { _buf = _output2; break; }
        case 3 : { _buf = _output3; break; }
        case 4 : { _buf = _output4; break; }
        case 5 : { _buf = _output5; break; }
        case 6 : { _buf = _output6; break; }
        case 7 : { _buf = _output7; break; }
        case 8 : { _buf = _output8; break; }
        case 9 : { _buf = _output9; break; }
        case 10 : { _buf = _output10; break; }
        case 11 : { _buf = _output11; break; }
        case 12 : { _buf = _output12; break; }
        case 13 : { _buf = _output13; break; }
        case 14 : { _buf = _output14; break; }
        case 15 : { _buf = _output15; break; }
    }
    int number = 3;
    unsigned shard_id = hiactor::global_shard_id();
    // 根据shard id构建文件名
    std::string file_name = "/home/yaojx/hiactor/demos/MultiJob/Job" + std::to_string(t_id) + "/shard" + std::to_string(shard_id) + ".txt";

    // 打开文件输出流，如果文件存在则覆盖原有内容
    std::ofstream out_file(file_name, std::ios::out | std::ios::trunc); // 使用trunc模式，将删除文件内容并重新写入

    // 检查文件是否成功打开
    if (!out_file.is_open()) {
        std::cerr << "Failed to open file: " << file_name << std::endl;
        return;
    }

    for(unsigned j = 0; j < _buf.size(); j++) {
        std::vector<hiactor::InternalValue> _schema = *_buf[j].vectorValue; //vector<tuple*>
            for (unsigned n = 0; n < _schema.size(); n++) {
                switch (number) {
                    case 1 : {
                        if(n == 0 || n == 1 || n == 2 || n == 7 || n == 8) {
                            out_file << _schema[n].intValue;
                            out_file <<" ";
                        } else if (n >= 5) {
                            out_file << _schema[n].stringValue;
                            out_file <<" ";
                        } else if(n == 3 || n == 4) {
                            std::vector<hiactor::InternalValue> vec = *(_schema[n].vectorValue);
                            for(unsigned k = 0; k < vec.size(); k++) {
                                std::vector<hiactor::InternalValue> tuple = *(vec[k].vectorValue);
                                out_file << tuple[0].stringValue;
                                out_file << " ";
                                out_file << tuple[1].intValue;
                                out_file << " ";
                                out_file << tuple[2].stringValue;
                                out_file << " ";
                            }
                        }
                        break;
                    }
                    case 2 : {
                        if(n == 0 || n == 1 || n == 4 || n == 7) {
                            out_file << _schema[n].intValue;
                            out_file <<" ";
                        } else {
                            out_file << _schema[n].stringValue;
                            out_file <<" ";
                        }
                        break;
                    }
                    case 3 : {
                        if(n == 2 || n == 3 ) {
                            out_file << _schema[n].stringValue;
                            out_file <<" ";
                        } else {
                            out_file <<_schema[n].intValue ;
                            out_file <<" ";
                        }
                        break;
                    }
                    case 4 : {
                        if(n == 1  ) {
                            out_file << _schema[n].stringValue;
                            out_file <<" ";
                        } else {
                            out_file <<_schema[n].intValue ;
                            out_file <<" ";
                        } 
                        break;
                    }
                    case 5 : {
                        if(n == 1  ) {
                            out_file << _schema[n].stringValue;
                            out_file <<" ";
                        } else {
                            out_file <<_schema[n].intValue ;
                            out_file <<" ";
                        }
                        break;
                    }
                    case 6 : {
                        if(n == 3   ) {
                            out_file << _schema[n].stringValue;
                            out_file <<" ";
                        } else {
                            out_file <<_schema[n].intValue ;
                            out_file <<" ";
                        } 
                        break;
                    }
                    case 7 : {
                        if (n == 0 || n == 4 || n == 8 || n == 7 || n == 3) {
                            out_file << _schema[n].intValue;
                        } else {
                            out_file << _schema[n].stringValue;
                        }
                        break;
                    }
                    case 8 : {
                        if (n == 0 || n == 3 || n == 4) {
                            out_file << _schema[n].intValue;
                        } else {
                            out_file << _schema[n].stringValue;
                        }
                        break;
                    }
                    case 9 : {
                        if (n == 0 || n == 3 || n == 4) {
                            out_file << _schema[n].intValue;
                        } else {
                            out_file << _schema[n].intValue;
                        }
                        break;
                    }
                    case 10 : {
                        if (n == 0 || n == 3) {
                            out_file << _schema[n].intValue;
                        } else {
                            out_file << _schema[n].intValue;
                        }
                        break;
                    }
                    case 11 : {
                        if (n == 0 || n == 4) {
                            out_file << _schema[n].intValue;
                        } else {
                            out_file << _schema[n].stringValue;
                        }
                        break;
                    }
                    case 12 : {
                        if (n == 0 || n == 3) {
                            out_file << _schema[n].intValue;
                        } else if (n == 1 || n == 2) {
                            out_file << _schema[n].stringValue;
                        } else if (n == 4) {
                            std::vector<hiactor::InternalValue> vec = *(_schema[n].vectorValue);
                            for (unsigned k = 0; k < vec.size(); k++) {
                                out_file << vec[k].stringValue;
                                out_file << " ";
                            }
                        }
                        break;
//                       if(n == 0 || n == 1) {
//                           out_file << _schema[n].intValue;
//                       }
//                       else if(n == 2) {
//                           std::vector<hiactor::InternalValue> vec = *(_schema[n].vectorValue);
//                           for(unsigned k = 0; k < vec.size(); k++) {
//                               out_file << vec[k].stringValue;
//                               out_file << " ";
//                           }
//                       }
//                       break;
                    }
                }
                out_file << " ";
            }
            out_file <<"\n";
        // 关闭文件
    }
    out_file.close();
//    std::cout<<"this is FileSinkActor output is over\n";
}

seastar::future<hiactor::stop_reaction> FileSinkActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "FileSinkActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_file_sink_process: {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_file_sink_receive_eos: {
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
		default: {
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::yes);
		}
	}
}

namespace auto_registration {

hiactor::registration::actor_registration<FileSinkActor> _file_sink_auto_registration(hiactor::TYPE_FILESINK);

} // namespace auto_registration