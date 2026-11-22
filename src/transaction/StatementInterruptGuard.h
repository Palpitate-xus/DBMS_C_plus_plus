#pragma once

#include "LockManager.h"
#include "commands/TableManage.h"
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace dbms {

class StatementInterruptGuard {
    std::shared_ptr<SessionInterruptState> state_;
    LockManager& locks_;
    std::mutex mutex_;
    std::condition_variable completed_;
    bool done_ = false;
    std::thread timer_;
public:
    StatementInterruptGuard(std::shared_ptr<SessionInterruptState> state, LockManager& locks)
        : state_(std::move(state)), locks_(locks) {
        locks_.setInterruptHandler(checkForQueryInterrupt);
        setCurrentQueryInterruptState(state_);
        state_->cancelRequested.store(false, std::memory_order_release);
        state_->timeoutRequested.store(false, std::memory_order_release);
        state_->queryActive.store(true, std::memory_order_release);
    }

    void startTimer(int milliseconds) {
        if (milliseconds <= 0) return;
        timer_ = std::thread([this, milliseconds] {
            std::unique_lock<std::mutex> lock(mutex_);
            if (!completed_.wait_for(lock, std::chrono::milliseconds(milliseconds),
                                    [this] { return done_; })) {
                state_->timeoutRequested.store(true, std::memory_order_release);
                state_->cancelRequested.store(true, std::memory_order_release);
            }
        });
    }

    bool finishTimer() {
        if (timer_.joinable()) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                done_ = true;
            }
            completed_.notify_one();
            timer_.join();
        }
        return state_->timeoutRequested.load(std::memory_order_acquire);
    }

    ~StatementInterruptGuard() {
        (void)finishTimer();
        locks_.clearInterruptHandler();
        setCurrentQueryInterruptState(nullptr);
        state_->queryActive.store(false, std::memory_order_release);
        state_->cancelRequested.store(false, std::memory_order_release);
        state_->timeoutRequested.store(false, std::memory_order_release);
    }
};

} // namespace dbms
