#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "subquery_toast_failure";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema outer;
    outer.tablename = "outer_rows";
    outer.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    outer.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, outer) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, outer.tablename, {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    dbms::TableSchema inner = outer;
    inner.tablename = "inner_rows";
    inner.append(dbms::makeVarCharColumn("payload", false, 12000, false));
    assert(g_engine.createTable(database, inner) == dbms::DBStatus::OK);
    const std::string payload(10000, 'x');
    assert(g_engine.insert(database, inner.tablename,
                           {{"id", "1"}, {"payload", payload}}) == dbms::DBStatus::OK);

    auto project = [&](const std::string& kind, const std::string& sql) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "value";
        expression.isScalar = true;
        expression.funcName = kind;
        expression.funcArgs = {sql};
        return g_engine.queryExpr(database, outer.tablename, {}, {expression});
    };
    assert((project("subquery", "select payload from inner_rows where id = 1") ==
            std::vector<std::string>{payload + " "}));
    assert((project("exists_sub", "select 1 from inner_rows where length(payload) = 10000") ==
            std::vector<std::string>{"t "}));

    uint64_t toastId = 0;
    assert(g_engine.forEachRow(database, inner.tablename,
        [&](uint32_t, uint16_t, const char* data, size_t length) {
            const std::string raw = dbms::StorageEngine::extractColumnValueStatic(
                std::string(data, length), inner, 1);
            assert(dbms::StorageEngine::parseToastMarker(raw, toastId));
        }));
    assert(toastId > 0);
    // Remove only this fixture's external value while retaining the heap
    // pointer, simulating missing TOAST chunks in an existing row.
    g_engine.deleteToast(database, inner.tablename, toastId);
    std::string recovered;
    assert(!g_engine.readToast(database, inner.tablename, toastId, 12000, recovered));

    for (const std::string kind : {"subquery", "exists_sub"}) {
        const std::string sql = kind == "subquery"
            ? "select payload from inner_rows where id = 1"
            : "select 1 from inner_rows where payload = ''";
        bool rejected = false;
        try {
            (void)project(kind, sql);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("TOAST") != std::string::npos;
        }
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }

    // EXISTS without a predicate does not need to read its projection value.
    assert((project("exists_sub", "select payload from inner_rows") ==
            std::vector<std::string>{"t "}));
    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SUBQUERY TOAST FAILURE] passed\n";
}
