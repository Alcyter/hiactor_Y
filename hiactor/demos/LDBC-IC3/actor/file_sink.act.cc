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

#include "Executor/Timing.h"
#include "actor/file_sink.act.h"
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

// seastar::future<hiactor::DataType> FileSinkActor_Ref::send_back() {
// 	addr.set_method_actor_tid(hiactor::TYPE_DATASINK);
// 	return hiactor::actor_client::request<hiactor::DataType>(addr, k_file_sink_send_back);
// }



//直接将收集到的数据写入对应shard的文件
void FileSinkActor::process(hiactor::DataType&& input) {
    // std::cout << "FileSinkActor::process" << '\n';
    // std::cout << "next actor type is: " << _next_actor_type << '\n';

    _output = input;
    _ref->process(std::move(_output));
    _have_send_data = true;
}

void FileSinkActor::receive_eos(hiactor::Eos&& eos) {
    if (++_eos_rcv_num == _expect_eos_num) {
        output();
        _ref->receive_eos(hiactor::Eos{_have_send_data});
    }
}

// seastar::future<hiactor::DataType> FileSinkActor::send_back() {
// 	std::cout << "DataSink::send_back" << '\n';
//     return _ref->send_back();
// }

void FileSinkActor::output() {
    stopTiming();
    std::vector<hiactor::InternalValue> _buf = *_output._data.vectorValue;

    std::cout<<"this is FileSinkActor output\n";

    for(unsigned j = 0; j < _buf.size(); j++) {
        std::vector<hiactor::InternalValue> _vec = *_buf[j].vectorValue; //vector<tuple*>
        std::tuple<hiactor::InternalValue, hiactor::InternalValue> _word_count_tuple; //tuple<string, int>
        
        unsigned shard_id = hiactor::global_shard_id();
        // 根据shard id构建文件名
        std::string file_name = "/home/yaojx/hiactor/demos/LDBC-IC3/result/shard" + std::to_string(shard_id) + ".txt";

        // 打开文件输出流，如果文件存在则覆盖原有内容
        std::ofstream out_file(file_name, std::ios::out | std::ios::trunc); // 使用trunc模式，将删除文件内容并重新写入

        // 检查文件是否成功打开
        if (!out_file.is_open()) {
            std::cerr << "Failed to open file: " << file_name << std::endl;
            return;
        }
        
        std::cout<<"the size"<<_vec.size()<<"\n";
        for (unsigned i = 0; i < _vec.size(); i++) {
//            _word_count_tuple = *_vec[i].tupleValue;
//            if (std::get<0>(_word_count_tuple).stringValue == NULL) break;
//            std::string word = std::get<0>(_word_count_tuple).stringValue; //get word from tuple
//            int count = std::get<1>(_word_count_tuple).intValue;
//
//            out_file  << "Word: " << std::left << std::setw(6) << word << ", Count: " << count << '\n';
            out_file <<_vec.size()<<"\n";
            break;
            // std::cout<<"the size"<<_vec.size()<<"\n";
            std::vector<hiactor::InternalValue> _schema =*(_vec[i].vectorValue);
            // std::cout<<"_schema the size"<<_schema.size()<<"\n";
            for (unsigned n = 0; n < _schema.size(); n++)
            {     
                if(n == 1 || n == 2 ) {
                    out_file << _schema[n].stringValue;
                    out_file <<" ";
                } else {
                    out_file <<_schema[n].intValue ;
                    out_file <<" ";
                } 

            }
            out_file <<"\n";
            //std::cout<<"\n";
            
        }

        // 关闭文件
        out_file.close();
    }
    std::cout<<"this is FileSinkActor output is over\n";
}

seastar::future<hiactor::stop_reaction> FileSinkActor::do_work(hiactor::actor_message* msg) {
	// std::cout << "FileSinkActor::do_work" << '\n';
    switch (msg->hdr.behavior_tid) {
		case k_file_sink_process: {
            // if (!msg->hdr.from_network) {
                process(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::DataType>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     process(InDataT::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
		case k_file_sink_receive_eos: {
            // if (!msg->hdr.from_network) {
                receive_eos(std::move(reinterpret_cast<
                    hiactor::actor_message_with_payload<hiactor::Eos>*>(msg)->data));
            // } else {
            //     auto* ori_msg = reinterpret_cast<hiactor::actor_message_with_payload<
            //         hiactor::serializable_queue>*>(msg);
            //     receive_eos(hiactor::Eos::load_from(ori_msg->data));
            // }
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		}
        // case k_file_sink_send_back: {
		// 	return send_back().then_wrapped([msg] (seastar::future<hiactor::DataType> fut) {
		// 		if (__builtin_expect(fut.failed(), false)) {
		// 			auto* ex_msg = hiactor::make_response_message(
		// 				msg->hdr.src_shard_id,
		// 				hiactor::simple_string::from_exception(fut.get_exception()),
		// 				msg->hdr.pr_id,
		// 				hiactor::message_type::EXCEPTION_RESPONSE);
		// 			return hiactor::actor_engine().send(ex_msg);
		// 		}
		// 		auto* response_msg = hiactor::make_response_message(
		// 			msg->hdr.src_shard_id, fut.get0(), msg->hdr.pr_id, hiactor::message_type::RESPONSE);
		// 		return hiactor::actor_engine().send(response_msg);
		// 	}).then([] {
		// 		return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::no);
		// 	});
		// }
		default: {
			return seastar::make_ready_future<hiactor::stop_reaction>(hiactor::stop_reaction::yes);
		}
	}
}

namespace auto_registration {

hiactor::registration::actor_registration<FileSinkActor> _file_sink_auto_registration(hiactor::TYPE_FILESINK);

} // namespace auto_registration