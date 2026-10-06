#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "update_quoted_target", database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    Session session;
    session.username = "admin"; session.permission = 1; session.currentDB = database;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rows_table(id INT PRIMARY KEY,\"OnlyV\" INT)", session));
    assert(g_engine.insertRow(database, "rows_table", {{"id", "1"}, {"OnlyV", "10"}}) == DBStatus::OK);
    const auto update = [&](const std::string& sql) {
        bool handled = false;
        assert(!tryDmlBridge(sql, SQLParser::classify(sql), session, handled, sql));
        assert(handled);
    };
    update("UPDATE rows_table SET \"OnlyV\"=11 WHERE id=1");
    update("UPDATE rows_table AS \"Q\" SET \"OnlyV\"=(SELECT \"Q\".\"OnlyV\"+1) WHERE \"Q\".id=1");
    update("UPDATE rows_table SET \"OnlyV\"=\"OnlyV\"+1 RETURNING id,\"OnlyV\"");
    auto result = takeLastDmlResult();
    assert(result.available && result.commandTag == "UPDATE 1");
    assert((result.columns == std::vector<std::string>{"id", "OnlyV"}));
    assert((result.rows == std::vector<std::vector<std::string>>{{"1", "13"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{{false, false}}));
    std::cerr << "RETURNING declared type=" << result.columnTypes.at(1) << '\n';
    assert(result.columnTypes.size() == 2 &&
           ExprHelper::canonicalResultTypeName(result.columnTypes[1]) == "integer");
    for (const auto& item : {
            std::make_pair("UPDATE rows_table SET onlyv=99", "42703"),
            std::make_pair("UPDATE rows_table SET \"OnlyV\"='bad' WHERE false", "22P02")}) {
        bool rejected = false, handled = false;
        try { (void)tryDmlBridge(item.first, SQLParser::classify(item.first), session, handled, item.first); }
        catch (const DbError& error) { assert(error.sqlState() == item.second); rejected = true; }
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }
    update("UPDATE rows_table SET \"OnlyV\"=NULL RETURNING id,\"OnlyV\"");
    result = takeLastDmlResult();
    assert(result.available && result.commandTag == "UPDATE 1");
    assert((result.columns == std::vector<std::string>{"id", "OnlyV"}));
    assert(result.rows.size() == 1 && result.rows.front().size() == 2 &&
           result.rows.front().front() == "1");
    // The NULL bit is authoritative. Its display payload may be "NULL";
    // it must not be mistaken for a non-NULL empty string or four-byte text.
    assert((result.nulls == std::vector<std::vector<bool>>{{false, true}}));
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[QUOTED UPDATE TARGET] uppercase-only/OLD binding/RETURNING/NULL/error passed\n";
}
