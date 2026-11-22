#pragma once

namespace dbms {
// A wire utility can execute inside an implicit engine transaction without
// having an explicit SQL transaction block. Scope this distinction to the
// calling thread/statement; it is not persistent session or portal state.
class ScopedWorkMemUtilityContext {
    inline static thread_local bool localAllowed_ = true;
    inline static thread_local bool continuedImplicitBlock_ = false;
    bool previous_, previousContinued_;
public:
    explicit ScopedWorkMemUtilityContext(bool localAllowed,
                                         bool continuedImplicitBlock = false) noexcept
        : previous_(localAllowed_), previousContinued_(continuedImplicitBlock_) {
        localAllowed_ = localAllowed;
        continuedImplicitBlock_ = continuedImplicitBlock;
    }
    ~ScopedWorkMemUtilityContext() {
        localAllowed_ = previous_;
        continuedImplicitBlock_ = previousContinued_;
    }
    ScopedWorkMemUtilityContext(const ScopedWorkMemUtilityContext&) = delete;
    ScopedWorkMemUtilityContext& operator=(const ScopedWorkMemUtilityContext&) = delete;
    static bool localAllowed() noexcept { return localAllowed_; }
    static bool insideBlock() noexcept { return localAllowed_ || continuedImplicitBlock_; }
};
} // namespace dbms
