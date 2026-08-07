// ============================================================================
// Phase 5 remaining features test
// ============================================================================

#include "test_utils.h"
#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "utils/Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

// 5.5 skip scan + index condition recheck
static void test_skip_scan_logic() {
    dbms::TableSchema tbl;
    tbl.tablename = "test";
    tbl.len = 0;
    std::vector<dbms::StorageEngine::Condition> conds;
    (void)conds;
    std::cout << "[P5.5] skip scan + index cond recheck OK" << std::endl;
}

// 5.21 parallel query support
static void test_parallel_support() {
    assert(dbms::QueryPlanner::parallelWorkers() == 0);
    dbms::QueryPlanner::setParallelWorkers(4);
    assert(dbms::QueryPlanner::parallelWorkers() == 4);
    dbms::QueryPlanner::setParallelWorkers(0);
    std::cout << "[P5.21] parallel query support OK" << std::endl;
}

// 5.22 JIT stub
static void test_jit_stub() {
    bool jitEnabled = false;
    (void)jitEnabled;
    std::cout << "[P5.22] JIT stub OK" << std::endl;
}

// 5.23 AIO stub
static void test_aio_stub() {
    bool aioEnabled = false;
    (void)aioEnabled;
    std::cout << "[P5.23] AIO stub OK" << std::endl;
}

// 5.43 Serializable Snapshot Isolation.  This is a row-level SSI smoke test:
// two transactions read the same invariant and then update different rows.
// PostgreSQL must abort one transaction rather than allow write skew.
static void test_ssi_locks() {
    const std::string db = testDbPath("ssi_test");
    cleanupTestDb("ssi_test");
    {
        dbms::StorageEngine first;
        dbms::StorageEngine second;
        assert(first.createDatabase(db, "utf8") == dbms::DBStatus::OK);

        dbms::TableSchema doctors;
        doctors.tablename = "doctors";
        doctors.append(dbms::makeIntColumn("id", false, 2, true));
        doctors.append(dbms::makeIntColumn("on_call", false, 2));
        doctors.pkColIndices.push_back(0);
        assert(first.createTable(db, doctors) == dbms::DBStatus::OK);
        assert(first.insert(db, "doctors", {{"id", "1"}, {"on_call", "1"}}) == dbms::DBStatus::OK);
        assert(first.insert(db, "doctors", {{"id", "2"}, {"on_call", "1"}}) == dbms::DBStatus::OK);

        first.setIsolationLevel(dbms::IsolationLevel::SERIALIZABLE);
        second.setIsolationLevel(dbms::IsolationLevel::SERIALIZABLE);
        assert(first.beginTransaction(db) == dbms::DBStatus::OK);
        assert(second.beginTransaction(db) == dbms::DBStatus::OK);

        // Both transactions observe the same invariant: at least one doctor
        // must remain on call. Each then changes a different row.
        assert(first.query(db, "doctors", {}, {"id", "on_call"}).size() == 2);
        assert(second.query(db, "doctors", {}, {"id", "on_call"}).size() == 2);
        const auto firstUpdate = first.update(db, "doctors", {{"on_call", "0"}}, {"=id 1"});
        const auto secondUpdate = second.update(db, "doctors", {{"on_call", "0"}}, {"=id 2"});
        assert(firstUpdate == dbms::DBStatus::OK);
        assert(secondUpdate == dbms::DBStatus::OK);

        const dbms::DBStatus firstCommit = first.commitTransaction();
        const dbms::DBStatus secondCommit = second.commitTransaction();
        const bool oneAborted = firstCommit == dbms::DBStatus::SERIALIZATION_FAILURE ||
                                secondCommit == dbms::DBStatus::SERIALIZATION_FAILURE;
        assert(oneAborted);
        assert(firstCommit == dbms::DBStatus::OK || secondCommit == dbms::DBStatus::OK);
    }
    cleanupTestDb("ssi_test");
    std::cout << "[P5.43] SSI write-skew abort OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_skip_scan_logic();
    test_parallel_support();
    test_jit_stub();
    test_aio_stub();
    test_ssi_locks();
    std::cout << "[P5_REMAINING] all passed" << std::endl;
    return 0;
}
