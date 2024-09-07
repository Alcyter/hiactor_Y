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

#include "actor/shuffle.act.h"
#include "actor/barrier.act.h"
#include "actor/map_partition.act.h"
#include "actor/file_sink.act.h"
#include "actor/file_source.act.h"

class FlatMap;

class FlatMapActor : public hiactor::actor {
public:
    FlatMapActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
        _refs.reserve(hiactor::global_shard_count());
        _output_data_buf.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            std::vector<hiactor::InternalValue> _vec;
            _output_data_buf.emplace_back(&_vec);
        }
    }
    ~FlatMapActor() override = default;
    
//boost::any or internal block
    void setNext(hiactor::actor_config_Batch&& next);
    void setFunc(hiactor::MapFunc &&func);
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
    hiactor::MapFunc _func;
    hiactor::DataType _output;
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
    const unsigned _expect_eos_num = hiactor::global_shard_count();
    bool _have_send_data = false;
    bool _have_next = false;
    unsigned _batch_sz = batch_size;
    bool _use_buffer = false;
    std::bitset<100000> visit0;
    std::bitset<100000> visit1;
    std::bitset<100000> visit2;
    std::bitset<100000> visit3;
    std::bitset<100000> visit4;
    std::bitset<100000> visit5;
    std::bitset<100000> visit6;
    std::bitset<100000> visit7;
    std::bitset<100000> visit8;
    std::bitset<100000> visit9;
    std::bitset<100000> visit10;
    std::bitset<100000> visit11;
    std::bitset<100000> visit12;
    std::bitset<100000> visit13;
    std::bitset<100000> visit14;
    std::bitset<100000> visit15;
    hiactor::InternalValue _node;
    std::vector<hiactor::InternalValue> _ori_path;
};


class FlatMapActor_Ref : public ::hiactor::reference_base, public hiactor::Actor_Ref {
public:
    FlatMapActor_Ref();
    ~FlatMapActor_Ref() override = default;
    /// actor methods
    void setNext(hiactor::actor_config_Batch&& next);
    void setFunc(hiactor::MapFunc &&func);
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


class FlatMap : public hiactor::OperatorBase {
public:
    FlatMap(hiactor::scope_builder& builder, unsigned ds_op_id, const unsigned batch_sz = batch_size)
            : OperatorBase(builder, ds_op_id, batch_sz, false) {
        _refs.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FlatMapActor_Ref>(ds_op_id));
        }
    }
    ~FlatMap() {
        for (auto ref : _refs) { delete ref; }
    }

    void process(unsigned dst_shard, hiactor::DataType&& data) override {
        (_refs[dst_shard]->process)(std::move(data));
        if (!_have_send_data) { _have_send_data = true; }
    }


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

    void setFunc(hiactor::MapFunc&& func) {
        //clone func然后再move
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            hiactor::MapFunc _func = func;
            (_refs[i]->setFunc)(std::move(_func));
        }
    }

    void setFunc(hiactor::HashFunc&& func) {}

    void setNext(hiactor::OperatorBase&& next) {
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            hiactor::actor_config_Batch next_config{1};
            next_config.emplace_back(next.getBuilder(), next.getActorId(), next.getActorType());
            _refs[i]->setNext(std::move(next_config));
        }
    }

    std::string getActorType() override {
        return "flat_map";
    }

    hiactor::scope_builder getBuilder() override {
        return _builder;
    }

    unsigned getActorId() override {
        return _ds_op_id;
    }

private:
    std::vector<FlatMapActor_Ref*> _refs{};

    // hiactor::scope_builder _builder;
    // unsigned _ds_op_id;
    // unsigned _batch_sz;
    // bool _have_send_data;
};


