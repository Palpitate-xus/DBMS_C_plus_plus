#include "PlanCacheEpoch.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::PlanCacheEpoch epoch;
    const auto initial = epoch.token();
    assert(epoch.stable() && epoch.accepts(initial));
    epoch.invalidate();
    assert(epoch.stable() && !epoch.accepts(initial));

    const auto beforeDdl = epoch.token();
    epoch.beginMutation();
    assert(!epoch.stable() && !epoch.accepts(beforeDdl));
    const auto duringDdl = epoch.token();
    assert(!epoch.accepts(duringDdl));
    epoch.beginMutation();  // A second writer overlaps the first.
    const auto duringBoth = epoch.token();
    epoch.endMutation();
    assert(!epoch.stable() && !epoch.accepts(duringBoth));
    const auto afterFirst = epoch.token();
    epoch.endMutation();
    assert(epoch.stable());
    for (const auto stale : {beforeDdl, duringDdl, duringBoth, afterFirst})
        assert(!epoch.accepts(stale));
    assert(epoch.accepts(epoch.token()));

    // Finishing rollback invalidates a plan built against the displaced
    // catalog even when the catalog ultimately has its original definition.
    const auto beforeRollback = epoch.token();
    epoch.beginMutation();
    epoch.endMutation();
    assert(!epoch.accepts(beforeRollback));
    assert(epoch.accepts(epoch.token()));
    std::cout << "[PLAN CACHE EPOCH TEST] passed\n";
}
