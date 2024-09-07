#include "generated/bank_account_ref.act.autogen.h"
#include <hiactor/core/actor-app.hh>
#include <seastar/core/print.hh>

seastar::future<> simulate() {
    hiactor::scope_builder builder;
    auto ba_ref = builder
        .set_shard(0)
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(1))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(2))
        .build_ref<bank_account_ref>(10);
    return ba_ref.deposit(hiactor::Integer(5)).then([] (hiactor::Integer&& balance) {
        fmt::print("Successful deposit, current balance: {}\n", balance.val);
    });
}

int main(int ac, char** av) {
    hiactor::actor_app app;
    app.run(ac, av, [] {
        return simulate().then([] {
            hiactor::actor_engine().exit();
            fmt::print("Exit hiactor system.\n");
        });
    });
}