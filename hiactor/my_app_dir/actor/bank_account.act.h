#pragma once

#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

class ANNOTATION(actor:impl) bank_account : public hiactor::actor {
    int balance = 0;
public:
    bank_account(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr) : hiactor::actor(exec_ctx, addr) {
        set_max_concurrency(1); // set max concurrency for task reentrancy
    }
    ~bank_account() override = default;

    /// Withdraw from this bank account by `amount`, return the remaining balance after withdrawing,
    /// if the current balance is not enough, an exception future will be returned.
    seastar::future<hiactor::Integer> ANNOTATION(actor:method) withdraw(hiactor::Integer&& amount);

    /// Deposit `amount` into this bank account, return the remaining balance after depositing.
    seastar::future<hiactor::Integer> ANNOTATION(actor:method) deposit(hiactor::Integer&& amount);

    /// Return the account balance.
    seastar::future<hiactor::Integer> ANNOTATION(actor:method) check();

    /// Declare a `do_work` func here, no need to implement.
    ACTOR_DO_WORK()
};