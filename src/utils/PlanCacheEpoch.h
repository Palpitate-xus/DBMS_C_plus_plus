#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace dbms {

// Callers protect this state with the same mutex as the cached entries. A
// planner may release that mutex while building a plan, but must retain its
// token: a concurrent invalidation must not let the old plan repopulate cache.
class PlanCacheEpoch {
public:
    using Token = uint64_t;

    Token token() const { return epoch_; }
    bool stable() const { return mutations_ == 0; }
    bool accepts(Token candidate) const {
        return stable() && candidate == epoch_;
    }
    void invalidate() { ++epoch_; }
    void beginMutation() {
        ++mutations_;
        invalidate();
    }
    void endMutation() {
        assert(mutations_ != 0);
        --mutations_;
        invalidate();
    }

private:
    Token epoch_ = 0;
    size_t mutations_ = 0;
};

} // namespace dbms
