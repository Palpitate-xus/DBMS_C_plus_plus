#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "stored_function_atomicity";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE native_atomic_rows(id INT)", session));
    const auto function = [&](const std::string& fn, const std::string& body, char volatility = 'v') {
        assert(g_engine.createUDF(db, fn, std::vector<std::string>{},
            std::vector<std::string>{}, body, volatility, "plpgsql", "int") == dbms::DBStatus::OK);
    };
    const auto failed = [&](const std::string& fn, const std::string& state) {
        std::string value;
        bool caught = false;
        try { (void)g_engine.callUDF(db, fn, {}, value); }
        catch (const dbms::DbError& error) { caught = error.sqlState() == state; }
        assert(caught && !g_engine.inTransaction());
        const auto rows = g_engine.plpgsqlQuery(db, "SELECT id FROM native_atomic_rows");
        assert(rows.ok && rows.rowCount == 0);
    };
    function("native_fail", "DECLARE n INT; BEGIN INSERT INTO native_atomic_rows VALUES(1); INSERT INTO native_atomic_rows VALUES(2); SELECT 'bad' INTO n; RETURN n; END;");
    failed("native_fail", "22P02");
    function("native_strict", "DECLARE n INT; BEGIN INSERT INTO native_atomic_rows VALUES(3); SELECT id INTO STRICT n FROM native_atomic_rows WHERE id=-1; RETURN n; END;");
    failed("native_strict", "P0002");
    assert(g_engine.createUDF(db, "native_sql_failure", std::vector<std::string>{},
        std::vector<std::string>{}, "SELECT native_strict()", 'v', "sql", "int") == dbms::DBStatus::OK);
    failed("native_sql_failure", "P0002");
    function("native_stable", "BEGIN INSERT INTO native_atomic_rows VALUES(4); RETURN 4; END;", 's');
    failed("native_stable", "0A000");
    function("native_commit", "BEGIN INSERT INTO native_atomic_rows VALUES(5); COMMIT; RETURN 5; END;");
    failed("native_commit", "2D000");
    function("native_success", "DECLARE n INT; BEGIN INSERT INTO native_atomic_rows VALUES(6); SELECT id INTO STRICT n FROM native_atomic_rows WHERE id=6; RETURN n; END;");
    std::string value;
    assert(g_engine.callUDF(db, "native_success", {}, value) && value == "6");
    assert(!g_engine.inTransaction());
    const auto rows = g_engine.plpgsqlQuery(db, "SELECT id FROM native_atomic_rows");
    assert(rows.ok && rows.rowCount == 1 && rows.firstRow.front() == "6");
    assert(!ddl.executeSql("CREATE TABLE native_nullable(id INT,payload TEXT)", session));
    function("native_nullable_write", "BEGIN INSERT INTO native_nullable(payload,id) VALUES(NULL,1),('null',2),('',3),('O''Brien',4); RETURN 4; END;");
    assert(g_engine.callUDF(db, "native_nullable_write", {}, value) && value == "4");
    for (int id = 1; id <= 4; ++id) {
        const auto row = g_engine.plpgsqlQuery(db,
            "SELECT payload FROM native_nullable WHERE id=" + std::to_string(id));
        assert(row.ok && row.rowCount == 1);
        if (id == 1) assert(row.firstRow.front() == std::nullopt);
        if (id == 2) assert(row.firstRow.front() == "null");
        if (id == 3) assert(row.firstRow.front() == "");
        if (id == 4) assert(row.firstRow.front() == "O'Brien");
    }
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[STORED FUNCTION ATOMICITY] passed\n";
}
