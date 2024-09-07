#pragma once

#include <hiactor/core/reference_base.hh>
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <vector>
#include <functional>

#include <hiactor/core/actor_client.hh>
#include <seastar/core/print.hh>
#include <iostream>
#include <fstream>
#include <iomanip>

#include "actor/map.act.h"
#include "actor/shuffle.act.h"
#include "actor/map_partition.act.h"
#include "actor/flat_map.act.h"
#include "actor/file_sink.act.h"
#include "actor/file_source.act.h"

class Barrier;


class BarrierActor : public hiactor::actor {
public:
    BarrierActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
        _refs.reserve(hiactor::global_shard_count());
        _output_data_buf.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            std::vector<hiactor::InternalValue> _vec;
            _output_data_buf.emplace_back(&_vec);
        }
    }
    ~BarrierActor() override = default;
    
//boost::any or internal block
    void setNext(hiactor::actor_config_Batch&& next);
    void process(hiactor::DataType&& input);
    void receive_eos(hiactor::Eos&& eos);
    void receive_eos0(hiactor::Eos &&eos);
    void receive_eos1(hiactor::Eos &&eos);
    void receive_eos2(hiactor::Eos &&eos);
    void receive_eos3(hiactor::Eos &&eos);
    void receive_eos4(hiactor::Eos &&eos);
    void receive_eos5(hiactor::Eos &&eos);
    void receive_eos6(hiactor::Eos &&eos);
    void receive_eos7(hiactor::Eos &&eos);
    void receive_eos8(hiactor::Eos &&eos);
    void receive_eos9(hiactor::Eos &&eos);
    void receive_eos10(hiactor::Eos &&eos);
    void receive_eos11(hiactor::Eos &&eos);
    void receive_eos12(hiactor::Eos &&eos);
    void receive_eos13(hiactor::Eos &&eos);
    void receive_eos14(hiactor::Eos &&eos);
    void receive_eos15(hiactor::Eos &&eos);
    void output();
    // seastar::future<hiactor::DataType> send_back();
    void useBuffer(hiactor::Eos &&eos);
    void emplace_data(unsigned dst_shard, hiactor::DataType&& data);
    void flush();

    seastar::future<hiactor::stop_reaction> do_work(hiactor::actor_message* msg);

private:
    std::vector<hiactor::Actor_Ref*> _refs{};
    //记录shards数量的DataType，根据_refs发送到对应的actor
    std::vector<hiactor::DataType> _output_data_buf{};
    std::string _next_actor_type;
    std::vector<hiactor::InternalValue> _word_count_vec;
    std::vector<hiactor::InternalValue> bucket0;
    std::vector<hiactor::InternalValue> bucket1;
    std::vector<hiactor::InternalValue> bucket2;
    std::vector<hiactor::InternalValue> bucket3;
    std::vector<hiactor::InternalValue> bucket4;
    std::vector<hiactor::InternalValue> bucket5;
    std::vector<hiactor::InternalValue> bucket6;
    std::vector<hiactor::InternalValue> bucket7;
    std::vector<hiactor::InternalValue> bucket8;
    std::vector<hiactor::InternalValue> bucket9;
    std::vector<hiactor::InternalValue> bucket10;
    std::vector<hiactor::InternalValue> bucket11;
    std::vector<hiactor::InternalValue> bucket12;
    std::vector<hiactor::InternalValue> bucket13;
    std::vector<hiactor::InternalValue> bucket14;
    std::vector<hiactor::InternalValue> bucket15;
    unsigned _eos_rcv_num = 0;
    unsigned _eos_rcv_num0 = 0;
    unsigned _eos_rcv_num1 = 0;
    unsigned _eos_rcv_num2 = 0;
    unsigned _eos_rcv_num3 = 0;
    unsigned _eos_rcv_num4 = 0;
    unsigned _eos_rcv_num5 = 0;
    unsigned _eos_rcv_num6 = 0;
    unsigned _eos_rcv_num7 = 0;
    unsigned _eos_rcv_num8 = 0;
    unsigned _eos_rcv_num9 = 0;
    unsigned _eos_rcv_num10 = 0;
    unsigned _eos_rcv_num11 = 0;
    unsigned _eos_rcv_num12 = 0;
    unsigned _eos_rcv_num13 = 0;
    unsigned _eos_rcv_num14 = 0;
    unsigned _eos_rcv_num15 = 0;
    // const unsigned _expect_eos_num = hiactor::global_shard_count() * hiactor::global_shard_count() * hiactor::global_shard_count();
    const unsigned _expect_eos_num = hiactor::global_shard_count();
    unsigned _output_count = 0;
    bool _have_send_data = false;
    bool _have_next = false;
    unsigned _batch_sz = batch_size;
    bool _use_buffer = false;
};


class BarrierActor_Ref : public ::hiactor::reference_base, public hiactor::Actor_Ref {
public:
    BarrierActor_Ref();
    ~BarrierActor_Ref() override = default;
    /// actor methods
    void setNext(hiactor::actor_config_Batch&& next);
    void process(hiactor::DataType &&input);
    void receive_eos(hiactor::Eos &&eos);
    void receive_eos0(hiactor::Eos &&eos);
    void receive_eos1(hiactor::Eos &&eos);
    void receive_eos2(hiactor::Eos &&eos);
    void receive_eos3(hiactor::Eos &&eos);
    void receive_eos4(hiactor::Eos &&eos);
    void receive_eos5(hiactor::Eos &&eos);
    void receive_eos6(hiactor::Eos &&eos);
    void receive_eos7(hiactor::Eos &&eos);
    void receive_eos8(hiactor::Eos &&eos);
    void receive_eos9(hiactor::Eos &&eos);
    void receive_eos10(hiactor::Eos &&eos);
    void receive_eos11(hiactor::Eos &&eos);
    void receive_eos12(hiactor::Eos &&eos);
    void receive_eos13(hiactor::Eos &&eos);
    void receive_eos14(hiactor::Eos &&eos);
    void receive_eos15(hiactor::Eos &&eos);
    // seastar::future<hiactor::DataType> send_back();
    void useBuffer(hiactor::Eos &&eos);
};


class Barrier : public hiactor::OperatorBase {
public:
    Barrier(hiactor::scope_builder& builder, unsigned ds_op_id, const unsigned batch_sz = batch_size)
            : OperatorBase(builder, ds_op_id, batch_sz, false) {
        _refs.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<BarrierActor_Ref>(ds_op_id));
        }
    }
    ~Barrier() {
        for (auto ref : _refs) { delete ref; }
    }

    void process(unsigned dst_shard, hiactor::DataType&& data) override {
        (_refs[dst_shard]->process)(std::move(data));
        if (!_have_send_data) { _have_send_data = true; }
    }

    //以下process函数仅为了通过编译，无实际作用，后续统一数据传递格式后可删除
    // void process(unsigned dst_shard, hiactor::DataType&& data) override {}
    // void process(unsigned dst_shard, hiactor::new_vector<hiactor::DataType>&& data) override {}
    

    void receive_eos() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos0() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos0(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos1() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos1(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos2() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos2(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos3() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos3(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos4() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos4(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos5() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos5(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos6() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos6(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos7() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos7(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos8() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos8(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos9() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos9(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos10() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos10(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos11() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos11(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos12() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos12(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos13() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos13(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos14() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos14(hiactor::Eos{_have_send_data});
        }
    }

    void receive_eos15() override  {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->receive_eos15(hiactor::Eos{_have_send_data});
        }
    }

    void useBuffer(bool use_buf) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            _refs[i]->useBuffer(hiactor::Eos{use_buf});
        }
    }
    
    // seastar::future<hiactor::DataType> send_back() override {
    //     return _refs[0]->send_back();
    // }

    void setNext(hiactor::OperatorBase&& next) {
        // hiactor::actor_config_Batch next_config{1};
        // next_config.emplace_back(next.getBuilder(), next.getActorId(), next.getActorType());

        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            hiactor::actor_config_Batch next_config{1};
            next_config.emplace_back(next.getBuilder(), next.getActorId(), next.getActorType());
            _refs[i]->setNext(std::move(next_config));
        }
    }

    void setFunc(hiactor::MapFunc&& func) {}
    void setFunc(hiactor::HashFunc&& func) {}

    std::string getActorType() override {
        return "barrier";
    }

    hiactor::scope_builder getBuilder() override {
        return _builder;
    }

    unsigned getActorId() override {
        return _ds_op_id;
    }

private:
    std::vector<BarrierActor_Ref*> _refs{};
    
    // hiactor::scope_builder _builder;
    // unsigned _ds_op_id;
    // unsigned _batch_sz;
    // bool _have_send_data;
};


