#pragma once
#include <map>
#include <string>

namespace dbms::expr_registry_detail {
// Borrowed view only. Both maps remain owned by their original registry;
// lookups return the actual pair/std::function, preserving callback targets.
template<class Value>
class BuiltinRegistryView {
public:
    using Map = std::map<std::string, Value, std::less<>>;
    using const_iterator = const typename Map::value_type*;
    BuiltinRegistryView(const Map& overlay, const Map& builtin)
        : overlay_(overlay), builtin_(builtin) {}
    template<class Key> const_iterator find(const Key& key) const {
        const auto local = overlay_.find(key);
        if (local != overlay_.end()) return &*local;
        const auto shared = builtin_.find(key);
        return shared == builtin_.end() ? nullptr : &*shared;
    }
    const_iterator end() const { return nullptr; }
    std::size_t size() const {
        std::size_t count = builtin_.size();
        for (const auto& entry : overlay_)
            if (builtin_.find(entry.first) == builtin_.end()) ++count;
        return count;
    }
private:
    const Map& overlay_;
    const Map& builtin_;
};
} // namespace dbms::expr_registry_detail
