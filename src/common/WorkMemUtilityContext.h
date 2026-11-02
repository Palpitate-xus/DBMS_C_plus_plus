#pragma once

namespace dbms {
// A wire utility can execute inside an implicit engine transaction without
// having an explicit SQL transaction block. Scope this distinction to the
// calling thread/statement; it is not persistent session or portal state.
class ScopedWorkMemUtilityContext {
    inline static thread_local bool localAllowed_ = true;
    bool previous_;
public:
    explicit ScopedWorkMemUtilityContext(bool localAllowed) noexcept
        : previous_(localAllowed_) { localAllowed_ = localAllowed; }
    ~ScopedWorkMemUtilityContext() { localAllowed_ = previous_; }
    ScopedWorkMemUtilityContext(const ScopedWorkMemUtilityContext&) = delete;
    ScopedWorkMemUtilityContext& operator=(const ScopedWorkMemUtilityContext&) = delete;
    static bool localAllowed() noexcept { return localAllowed_; }
};
} // namespace dbms
