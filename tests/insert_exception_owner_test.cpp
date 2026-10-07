#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
namespace {
class TriggerFailure final : public dbms::DbError {
public: TriggerFailure() : DbError("P0001", "original typed trigger failure") {}
};
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("insert_exception_owner");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table; table.tablename = "rows"; table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    table.tablename = "audit";
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    assert(g_engine.createTrigger(db, {"throw_after", "after", "insert", "rows",
        "throw", "", true, true, {}}) == DBStatus::OK);
    auto fail = [&] {
        g_engine.setTriggerExecutor([&](const std::string&) -> bool {
            assert(g_engine.insert(db, "audit", {{"id", "9"}}) == DBStatus::OK);
            throw TriggerFailure();
        });
        std::vector<StorageEngine::SqlRow> returned{{{"id", "sentinel"}}};
        bool caught = false;
        try { (void)g_engine.insertRow(db, "rows", {{"id", "9"}}, &returned); }
        catch (const TriggerFailure& error) {
            caught = error.sqlState() == "P0001" && error.message() == "original typed trigger failure";
        }
        g_engine.setTriggerExecutor({});
        std::cerr << "INSERT_EXCEPTION_RESULT typed=" << caught << " returned="
                  << returned.size() << " active=" << g_engine.inTransaction() << '\n';
        assert(caught && returned.size() == 1 && returned[0].at("id") == "sentinel");
    };
    fail();
    std::cerr << "INSERT_EXCEPTION_AUTOCOMMIT_ACTIVE " << g_engine.inTransaction() << '\n';
    assert(!g_engine.inTransaction());
    assert(g_engine.getLockManager().lockedTables().empty());
    assert(g_engine.query(db, "rows", {}, {"id"}).empty());
    assert(g_engine.query(db, "audit", {}, {"id"}).empty());

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "rows", {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.savepoint("user_work") == DBStatus::OK);
    fail();
    assert(g_engine.inTransaction());
    assert(g_engine.rollbackToSavepoint("user_work") == DBStatus::OK);
    assert(g_engine.releaseSavepoint("user_work") == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.query(db, "rows", {}, {"id"}).size() == 1);
    assert(g_engine.query(db, "rows", {"=id 1"}, {"id"}).size() == 1);
    assert(g_engine.query(db, "rows", {"=id 9"}, {"id"}).empty());
    assert(g_engine.query(db, "audit", {}, {"id"}).empty());
    assert(g_engine.insert(db, "rows", {{"id", "2"}}) == DBStatus::OK);
    std::cout << "[INSERT EXCEPTION OWNER] typed primary, side effects, rows, own transaction and user savepoint passed\n";
}
