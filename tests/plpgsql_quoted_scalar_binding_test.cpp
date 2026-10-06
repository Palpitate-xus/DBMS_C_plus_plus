#include "catalog/type_registry.h"
#include "Session.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "plpgsql_quoted_scalar_binding";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "review_user";
    session.permission = 1;
    session.currentDB = db;
    dbms::setCurrentSession(&session);
    int serial = 0;
    bool passed = true;
    const auto call = [&](const std::string& body, const std::string& type,
                          const std::optional<std::string>& expected) {
        const std::string function = "quoted_binding_" + std::to_string(++serial);
        assert(g_engine.createUDF(db, function, {}, {}, body, 'v', "plpgsql", type)
            == dbms::DBStatus::OK);
        std::string value;
        bool isNull = false;
        try {
            const bool ok = g_engine.callUDF(db, function, {}, value, &isNull);
            if (!ok || isNull != !expected || (expected && value != *expected)) {
                std::cerr << body << " expected " << expected.value_or("SQLNULL")
                          << " got ok=" << ok << " null=" << isNull << " value=" << value << '\n';
                passed = false;
            }
        } catch (const dbms::DbError& error) {
            std::cerr << body << " unexpected " << error.sqlState() << " " << error.message() << '\n';
            passed = false;
        }
    };
    call("DECLARE \"X\" INT := 1; x INT := 2; BEGIN RETURN \"X\"+x; END;", "int", "3");
    call("DECLARE \"X\" BIGINT := 5000000000; x INT := 2; BEGIN RETURN \"X\"+x; END;", "bigint", "5000000002");
    call("DECLARE \"X\" INT; x INT := 2; BEGIN RETURN coalesce(\"X\",9)+x; END;", "int", "11");
    call("DECLARE \"X\" INT := 1; x INT; BEGIN RETURN \"X\"+coalesce(x,9); END;", "int", "10");
    call("DECLARE \"X\" TEXT := '00123'; x INT := 2; BEGIN RETURN \"X\"||':'||CAST(x AS TEXT); END;", "text", "00123:2");
    call("DECLARE \"X\" TEXT := ''; x INT := 2; BEGIN RETURN length(\"X\")+x; END;", "int", "2");
    call("DECLARE \"X\" INT := 1; x INT := 2; BEGIN RETURN CASE WHEN \"X\"=1 THEN x+10 ELSE 99 END; END;", "int", "12");
    call("DECLARE \"Value Name\" INT := 3; \"Q\"\"Col\" INT := 4; BEGIN RETURN \"Value Name\"*10+\"Q\"\"Col\"; END;", "int", "34");
    call("DECLARE \"X\" INT := 1; x INT := 2; BEGIN RETURN CAST(ARRAY[\"X\",x][1] AS INT)+CAST(ARRAY[\"X\",x][2] AS INT); END;", "int", "3");
    call("DECLARE \"X\" INT := 1; x INT := 2; BEGIN RETURN (\"X\"+x)*10; END;", "int", "30");
    call("DECLARE \"__plpgsql_variable_0\" INT := 3; \"X\" INT := 1; x INT := 2; BEGIN RETURN \"__plpgsql_variable_0\"+\"X\"+x; END;", "int", "6");
    call("BEGIN RETURN current_user||':'||session_user; END;", "text", "review_user:review_user");
    call("DECLARE \"current_user\" TEXT := 'local'; BEGIN RETURN \"current_user\"||':'||session_user; END;", "text", "local:review_user");
    call("BEGIN RETURN current_date IS NOT NULL AND current_timestamp IS NOT NULL AND localtimestamp IS NOT NULL; END;", "boolean", "t");
    call("BEGIN RETURN EXTRACT(year FROM DATE '2026-10-06'); END;", "numeric", "2026");
    call("BEGIN RETURN DATE '2026-10-06'+1; END;", "date", "2026-10-07");
    assert(g_engine.createUDF(db, "quoted_parameter_binding", {"X", "x"}, {"int", "int"},
        "BEGIN RETURN \"X\"+x; END;", 'v', "plpgsql", "int") == dbms::DBStatus::OK);
    std::string parameterValue;
    bool parameterNull = false;
    assert(g_engine.callUDF(db, "quoted_parameter_binding", {"1", "2"}, parameterValue,
        &parameterNull) && !parameterNull && parameterValue == "3");

    for (const auto& errorCase : std::vector<std::pair<std::string, std::string>>{
        {"DECLARE x INT := 2; BEGIN RETURN missing.x+1; END;", "42P01"},
        {"BEGIN RETURN no_such_variable+1; END;", "42703"},
        {"BEGIN RETURN __plpgsql_variable_0+1; END;", "42703"},
        {"DECLARE \"X\" INT := 1; BEGIN RETURN no_such_function(\"X\"); END;", "42883"},
        {"BEGIN RETURN coalesce(1,no_such_function(1)); END;", "42883"},
        {"BEGIN RETURN coalesce(1,no_such_variable); END;", "42703"},
        {"DECLARE \"X\" INT := 1; BEGIN RETURN \"X\"/0; END;", "22012"}
    }) {
        const std::string function = "quoted_binding_" + std::to_string(++serial);
        assert(g_engine.createUDF(db, function, {}, {}, errorCase.first, 'v', "plpgsql", "int")
            == dbms::DBStatus::OK);
        std::string value;
        try {
            g_engine.callUDF(db, function, {}, value);
            std::cerr << errorCase.first << " did not throw\n";
            passed = false;
        } catch (const dbms::DbError& error) {
            if (error.sqlState() != errorCase.second) {
                std::cerr << errorCase.first << " expected " << errorCase.second
                          << " got " << error.sqlState() << " " << error.message() << '\n';
                passed = false;
            }
        }
    }
    assert(g_engine.createUDF(db, "quoted_trigger_binding", {}, {},
        "DECLARE id INT := 99; BEGIN RETURN concat(NEW.id,':',tg_name); END;", 'v', "plpgsql", "text")
        == dbms::DBStatus::OK);
    dbms::StorageEngine::TriggerCtx trigger;
    trigger.vars = {{"new.id", "7"}, {"tg_name", "context"}};
    std::string value;
    bool isNull = false;
    if (!g_engine.callUDFWithCtx(db, "quoted_trigger_binding", {}, trigger, value, &isNull)
        || isNull || value != "7:context") passed = false;
    // A failure must not leak one function's private slots into the next.
    call("DECLARE \"X\" INT := 1; x INT := 2; BEGIN RETURN \"X\"+x; END;", "int", "3");
    dbms::setCurrentSession(nullptr);
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    cleanupTestDb(name);
    if (!passed) return 1;
    std::cout << "[PLPGSQL QUOTED SCALAR BINDING] passed\n";
}
