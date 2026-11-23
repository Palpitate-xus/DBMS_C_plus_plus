#pragma once

namespace dbms {

// The simple-query receiver can publish the rows delivered before an error.
// The scope lives on the backend thread that handles the Query message.
class SimpleQueryResultScope {
public:
    SimpleQueryResultScope() noexcept : previous_(active_) { active_ = true; }
    ~SimpleQueryResultScope() { active_ = previous_; }

    SimpleQueryResultScope(const SimpleQueryResultScope&) = delete;
    SimpleQueryResultScope& operator=(const SimpleQueryResultScope&) = delete;

    static bool active() noexcept { return active_; }

private:
    bool previous_;
    inline static thread_local bool active_ = false;
};

} // namespace dbms
