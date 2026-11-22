#pragma once

#include <string>
#include <utility>
#include <vector>

namespace dbms {

// Each savepoint records both the visible value and the value that would
// survive a commit. SET LOCAL changes only the visible value.
template<class Value>
class SessionSetting {
    struct Savepoint { std::string name; Value visible, persistent; };
    Value visible_{}, original_{}, persistent_{};
    bool active_ = false;
    std::vector<Savepoint> savepoints_;
public:
    const Value& get() const { return visible_; }

    template<class Change>
    void update(Change change, bool local) {
        if (local && !active_) return;
        change(visible_);
        if (active_ && !local) change(persistent_);
    }

    void assign(const Value& value, bool local) {
        update([&](Value& target) { target = value; }, local);
    }

    void begin() {
        if (active_) return;
        original_ = persistent_ = visible_;
        active_ = true;
    }

    void finish(bool commit) {
        if (!active_) return;
        visible_ = commit ? persistent_ : original_;
        active_ = false;
        savepoints_.clear();
    }

    void save(const std::string& name) {
        begin();
        savepoints_.push_back({name, visible_, persistent_});
    }

    void rollback(const std::string& name) {
        for (size_t i = savepoints_.size(); i > 0; --i) {
            const auto& saved = savepoints_[i - 1];
            if (saved.name != name) continue;
            visible_ = saved.visible;
            persistent_ = saved.persistent;
            savepoints_.resize(i);
            return;
        }
    }

    void release(const std::string& name) {
        for (size_t i = savepoints_.size(); i > 0; --i) {
            if (savepoints_[i - 1].name != name) continue;
            savepoints_.resize(i - 1);
            return;
        }
    }
};

} // namespace dbms
