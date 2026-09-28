#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <thread>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "enum_cache_relation_lock_order";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TYPE mood AS ENUM ('sad', 'ok')", session));
    assert(!ddl.executeSql("CREATE TABLE items (id INT PRIMARY KEY, m mood)", session));

    auto& locks = g_engine.getLockManager();
    locks.setResourceNamespace(db);
    assert(locks.lockShared("items"));

    auto updated = g_engine.getEnumType(db, "mood");
    updated.labels.push_back("happy");
    dbms::DBStatus ddlStatus = dbms::DBStatus::OK;
    std::thread alter([&] {
        locks.setResourceNamespace(db);
        locks.setLockTimeout(1000);
        ddlStatus = g_engine.updateEnumType(db, updated);
    });
    bool ddlWaiting = false;
    const auto waitDeadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < waitDeadline) {
        if (!locks.getLockWaits().empty()) {
            ddlWaiting = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(ddlWaiting);

    bool readerLocked = false;
    std::chrono::milliseconds readerWait{0};
    std::thread reader([&] {
        locks.setResourceNamespace(db);
        const auto start = std::chrono::steady_clock::now();
        readerLocked = locks.lockShared("items");
        if (readerLocked) {
            assert(g_engine.getCommitLog(db) != nullptr);
            locks.unlock("items");
        }
        readerWait = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
    });
    reader.join();
    alter.join();
    locks.unlock("items");

    assert(readerLocked);
    assert(readerWait < std::chrono::milliseconds(400));
    assert(ddlStatus == dbms::DBStatus::LOCK_CONFLICT);
    const auto unchanged = g_engine.getEnumType(db, "mood");
    assert(unchanged.labels.size() == 2);
    cleanupTestDb(name);
}
