// ============================================================================
// parallel_vacuum_test — VACUUM (PARALLEL n):
//   * engine vacuum() with workers>1 reclaims the same dead pages as serial
//     (freed-page counts and surviving rows identical)
//   * large worker counts cap at the page count without deadlock
//   * option parsing sanity is covered by the shell-level grammar (main)
// ============================================================================

#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;
using namespace dbms;

static void cleanupDb(const std::string& db) {
    if (g_engine.databaseExists(db)) {
        const auto status = g_engine.dropDatabase(db);
        assert(status == DBStatus::OK || status == DBStatus::DATABASE_NOT_FOUND);
    }
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("parallel_vacuum");
    cleanupDb(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    for (const char* name : {"vt_serial", "vt_par"}) {
        TableSchema t;
        t.tablename = name;
        t.formatVersion = 2;
        t.append(makeIntColumn("id", false, 0, true));
        t.append(makeVarCharColumn("payload", false, 10000, false));
        assert(g_engine.createTable(db, t) == DBStatus::OK);
        // enough rows to span multiple 8KB pages with a wide payload
        for (int i = 0; i < 400; ++i) {
            std::string payload(400, 'a' + (i % 26));
            assert(g_engine.insert(db, name,
                {{"id", std::to_string(i)}, {"payload", payload}})
                == DBStatus::OK);
        }
    }

    // kill 1/4 of the rows in both tables
    for (int i = 0; i < 400; i += 4) {
        std::string cond = "=id " + std::to_string(i);
        assert(g_engine.remove(db, "vt_serial", {cond}) == DBStatus::OK);
        assert(g_engine.remove(db, "vt_par", {cond}) == DBStatus::OK);
    }

    auto liveSerial = g_engine.query(db, "vt_serial", {}, {}, {});
    auto livePar = g_engine.query(db, "vt_par", {}, {}, {});
    assert(liveSerial.size() == 300);
    assert(livePar.size() == 300);

    // Serial baseline vs parallel workers
    size_t freedSerial = g_engine.vacuum(db, "vt_serial", false, 1);
    size_t freedPar = g_engine.vacuum(db, "vt_par", false, 4);
    std::cout << "[VACUUM] serial freed=" << freedSerial
              << " parallel(4) freed=" << freedPar << std::endl;
    assert(freedSerial == freedPar);

    // Both tables retain exactly the live rows
    assert(g_engine.query(db, "vt_serial", {}, {}, {}).size() == 300);
    assert(g_engine.query(db, "vt_par", {}, {}, {}).size() == 300);

    // Idempotence: second parallel vacuum frees nothing more
    assert(g_engine.vacuum(db, "vt_par", false, 4) == 0);

    // Data integrity: every deleted id is gone, survivors intact
    for (const auto& row : g_engine.query(db, "vt_par", {}, {}, {})) {
        size_t bar = row.find('|');
        int id = atoi(row.substr(0, bar).c_str());
        assert(id % 4 != 0);
        assert(row.size() - bar - 1 >= 300);  // wide payload survived
    }

    // Large worker count caps at page count without deadlock
    (void)g_engine.vacuum(db, "vt_par", false, 64);

    cleanupDb(db);
    std::cout << "[VACUUM] all parallel vacuum tests passed" << std::endl;
    return 0;
}
