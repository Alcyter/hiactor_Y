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
#include <hiactor/core/actor_client.hh>
#include <hiactor/core/actor_factory.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

enum : uint8_t {
	k_end_file_sink_process = 0,
	k_end_file_sink_receive_eos = 1,
    // k_end_file_sink_send_back = 2,
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
};

EndFileSinkActor_Ref::EndFileSinkActor_Ref(): ::hiactor::reference_base() { actor_type = hiactor::TYPE_ENDFILESINK; }

void EndFileSinkActor_Ref::process(hiactor::DataType &&input) {
	addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
	hiactor::actor_client::send(addr, k_end_file_sink_process, std::forward<hiactor::DataType>(input));
}

void EndFileSinkActor_Ref::receive_eos(hiactor::Eos &&eos) {
	addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
	hiactor::actor_client::send(addr, k_end_file_sink_receive_eos, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos0(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos0, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos1(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos1, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos2(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos2, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos3(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos3, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos4(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos4, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos5(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos5, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos6(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos6, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos7(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos7, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos8(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos8, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos9(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos9, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos10(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos10, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos11(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos11, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos12(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos12, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos13(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos13, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos14(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos14, std::forward<hiactor::Eos>(eos));
}

void EndFileSinkActor_Ref::receive_eos15(hiactor::Eos &&eos) {
    addr.set_method_actor_tid(hiactor::TYPE_ENDFILESINK);
    hiactor::actor_client::send(addr, k_file_source_receive_eos15, std::forward<hiactor::Eos>(eos));
}

//直接将收集到的数据写入对应shard的文件
void EndFileSinkActor::process(hiactor::DataType&& input) {

}

void EndFileSinkActor::receive_eos(hiactor::Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'." << '\n';
		hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos0(hiactor::Eos&& eos) {
    if (++_eos_rcv_num0 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_0" << '\n';
    //    sleep(3);
    //    hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos1(hiactor::Eos&& eos) {
    if (++_eos_rcv_num1 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_1" << '\n';
    //    sleep(3);
    //    hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos2(hiactor::Eos&& eos) {
    if (++_eos_rcv_num2 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_2" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos3(hiactor::Eos&& eos) {
    if (++_eos_rcv_num3 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_3" << '\n';
    //    sleep(3);
    //    hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos4(hiactor::Eos&& eos) {
    if (++_eos_rcv_num4 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_4" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos5(hiactor::Eos&& eos) {
    if (++_eos_rcv_num5 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_5" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos6(hiactor::Eos&& eos) {
    if (++_eos_rcv_num6 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_6" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos7(hiactor::Eos&& eos) {
    if (++_eos_rcv_num7 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_7" << '\n';
    //    sleep(3);
    //    hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos8(hiactor::Eos&& eos) {
    if (++_eos_rcv_num8 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_8" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos9(hiactor::Eos&& eos) {
    if (++_eos_rcv_num9 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_9" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos10(hiactor::Eos&& eos) {
    if (++_eos_rcv_num10 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_10" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos11(hiactor::Eos&& eos) {
    if (++_eos_rcv_num11 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_11" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos12(hiactor::Eos&& eos) {
    if (++_eos_rcv_num12 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_12" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos13(hiactor::Eos&& eos) {
    if (++_eos_rcv_num13 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_13" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos14(hiactor::Eos&& eos) {
    if (++_eos_rcv_num14 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_14" << '\n';
//        sleep(3);
//        hiactor::actor_engine().exit();
    }
}

void EndFileSinkActor::receive_eos15(hiactor::Eos&& eos) {
    if (++_eos_rcv_num15 == _expect_eos_num) {
        std::cout << '\n' << "Results have been written into folder: 'hiactor/demos/map_sample/Result_IC9'. for job_15" << '\n';
        sleep(3);
        hiactor::actor_engine().exit();
    }
}

// seastar::future<hiactor::DataType> EndFileSinkActor::send_back() {
// 	std::cout << "EndDataSink::send_back" << '\n';
//     return seastar::make_ready_future<hiactor::DataType>(28);
// }


seastar::future<hiactor::stop_reaction> EndFileSinkActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "EndFileSinkActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_end_file_sink_process: {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_end_file_sink_receive_eos: {
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

hiactor::registration::actor_registration<EndFileSinkActor> _end_file_sink_auto_registration(hiactor::TYPE_ENDFILESINK);

} // namespace auto_registration