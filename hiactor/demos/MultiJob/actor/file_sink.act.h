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
#include "actor/end_file_sink.act.h"

class FileSinkActor : public hiactor::actor {
public:
    FileSinkActor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr, false) {
        auto builder = get_scope();
        builder.set_shard(0);
        _ref = builder.new_ref<EndFileSinkActor_Ref>(0);
    }
    ~FileSinkActor() override = default;
    
//boost::any or internal block
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
    void output(int t_id);
    // seastar::future<hiactor::DataType> send_back();

    seastar::future<hiactor::stop_reaction> do_work(hiactor::actor_message* msg);

private:
    // std::vector<hiactor::Actor_Ref*> _refs{};
    EndFileSinkActor_Ref* _ref;
    hiactor::DataType _output;
    std::vector<hiactor::InternalValue> _output0;
    std::vector<hiactor::InternalValue> _output1;
    std::vector<hiactor::InternalValue> _output2;
    std::vector<hiactor::InternalValue> _output3;
    std::vector<hiactor::InternalValue> _output4;
    std::vector<hiactor::InternalValue> _output5;
    std::vector<hiactor::InternalValue> _output6;
    std::vector<hiactor::InternalValue> _output7;
    std::vector<hiactor::InternalValue> _output8;
    std::vector<hiactor::InternalValue> _output9;
    std::vector<hiactor::InternalValue> _output10;
    std::vector<hiactor::InternalValue> _output11;
    std::vector<hiactor::InternalValue> _output12;
    std::vector<hiactor::InternalValue> _output13;
    std::vector<hiactor::InternalValue> _output14;
    std::vector<hiactor::InternalValue> _output15;
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
    bool _have_send_data;
};


class FileSinkActor_Ref : public ::hiactor::reference_base, public hiactor::Actor_Ref {
public:
    FileSinkActor_Ref();
    ~FileSinkActor_Ref() override = default;
    /// actor methods
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
};


class FileSink : public hiactor::Sink {
public:
    FileSink(hiactor::scope_builder& builder, unsigned ds_op_id, const unsigned batch_sz = batch_size)
            : Sink(builder, ds_op_id, batch_sz, false) {
        _refs.reserve(hiactor::global_shard_count());
        for (unsigned i = 0; i < hiactor::global_shard_count(); i++) {
            builder.set_shard(i);
            _refs.emplace_back(builder.new_ref<FileSinkActor_Ref>(ds_op_id));
        }
    }
    ~FileSink() {
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

    // seastar::future<hiactor::DataType> send_back() override {
    //     return _refs[0]->send_back();
    // }

    void setNext(hiactor::OperatorBase&& next) {}
    void setFunc(hiactor::MapFunc&& func) {}
    void setFunc(hiactor::HashFunc&& func) {}

    std::string getActorType() override {
        return "file_sink";
    }

    hiactor::scope_builder getBuilder() override {
        return _builder;
    }

    unsigned getActorId() override {
        return _ds_op_id;
    }

private:
    std::vector<FileSinkActor_Ref*> _refs{};
};


