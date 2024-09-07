#include "bank_account.act.h"
#include <seastar/core/print.hh>

seastar::future<hiactor::Integer> bank_account::withdraw(hiactor::Integer&& amount) {
    auto request = amount.val;
    if (request > balance) {
        return seastar::make_exception_future<hiactor::Integer>(std::runtime_error(
                seastar::format("Account balance is not enough, request: {}, remaining: {}.", request, balance)));
    } else {
        balance -= request;
        return seastar::make_ready_future<hiactor::Integer>(balance);
    }
}

seastar::future<hiactor::Integer> bank_account::deposit(hiactor::Integer&& amount) {
    auto request = amount.val;
    balance += request;
    return seastar::make_ready_future<hiactor::Integer>(balance);
}

seastar::future<hiactor::Integer> bank_account::check() {
    return seastar::make_ready_future<hiactor::Integer>(balance);
}