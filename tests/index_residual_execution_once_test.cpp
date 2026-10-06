#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "process/RuntimeStats.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("index_residual_execution_once");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    dbms::TableSchema input;
    input.tablename = "residual_input";
    input.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    input.append(dbms::makeIntColumn("id", false, 2, true));
    input.append(dbms::makeIntColumn("nullable_value", true, 2));
    assert(g_engine.createTable(db, input) == DBStatus::OK);
    dbms::TableSchema effects;
    effects.tablename = "residual_effects";
    effects.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    effects.append(dbms::makeIntColumn("id", false, 2));
    assert(g_engine.createTable(db, effects) == DBStatus::OK);
    assert(g_engine.insert(db, input.tablename, {{"id", "1"}}) == DBStatus::OK);
    for (const auto& suffix : {"match", "reject", "null"}) {
        const std::string result = std::string(suffix) == "null" ? "NULL" :
            std::string(suffix) == "reject" ? "p+1" : "p";
        assert(g_engine.createUDF(db, "residual_" + std::string(suffix),
            {"p"}, {"int"}, "BEGIN INSERT INTO residual_effects VALUES(p); RETURN " +
            result + "; END;", 'v', "plpgsql", "int") == DBStatus::OK);
    }
    const auto stats = [&]() {
        const auto rows = dbms::getRuntimeTableStats(db);
        const auto found = std::find_if(rows.begin(), rows.end(), [&](const auto& row) {
            return row.relname == input.tablename;
        });
        assert(found != rows.end());
        return *found;
    };
    for (const auto& suffix : {"match", "reject", "null"}) {
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
        assert(g_engine.beginSqlCommand());
        const auto before = stats();
        const auto rows = g_engine.query(db, input.tablename,
            {"=id 1", "typedexpr residual_" + std::string(suffix) +
                "(id) = 1 AND nullable_value IS NULL"}, {"id"});
        assert(rows.size() == (std::string(suffix) == "match" ? 1U : 0U));
        const auto after = stats();
        assert(g_engine.finishSqlCommand());
        const auto actual = g_engine.plpgsqlQuery(db, "SELECT id FROM residual_effects");
        std::cerr << "[INDEX RESIDUAL " << suffix << "] actual calls=" << actual.rowCount << '\n';
        assert(actual.ok && actual.rowCount == 1);
        assert(after.idxScan == before.idxScan + 1);
        assert(after.seqScan == before.seqScan);
        assert(g_engine.rollbackTransaction() == DBStatus::OK);
    }
    // Two physical candidates retain their original bytes AND NULL bitmap.
    // Source UPDATE is additionally exercised by the real frontend protocol;
    // the standalone native PL bridge requires a full SQL host for UPDATE.
    dbms::TableSchema multi;
    multi.tablename = "residual_multi";
    multi.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    multi.append(dbms::makeIntColumn("id", false, 2));
    multi.append(dbms::makeIntColumn("bucket", false, 2));
    multi.append(dbms::makeIntColumn("n", true, 2));
    assert(g_engine.createTable(db, multi) == DBStatus::OK);
    assert(g_engine.insert(db, multi.tablename, {{"id", "1"}, {"bucket", "1"}}) == DBStatus::OK);
    assert(g_engine.insert(db, multi.tablename, {{"id", "2"}, {"bucket", "1"}}) == DBStatus::OK);
    assert(g_engine.createIndex(db, multi.tablename, "bucket") == DBStatus::OK);
    assert(g_engine.createUDF(db, "residual_mutating", {"p"}, {"int"},
        "BEGIN INSERT INTO residual_effects VALUES(p); UPDATE residual_multi SET n=0 WHERE id=2; RETURN p; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    bool unsupportedUpdate = false;
    try {
        (void)g_engine.query(db, multi.tablename,
            {"=bucket 1", "typedexpr residual_mutating(id)>0 AND n IS NULL"}, {"id"});
    } catch (const std::exception& error) {
        unsupportedUpdate = std::string(error.what()).find("SQLSTATE 0A000") != std::string::npos;
    }
    assert(unsupportedUpdate);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.createUDF(db, "residual_multi_writer", {"p"}, {"int"},
        "BEGIN INSERT INTO residual_effects VALUES(p); RETURN p; END;",
        'v', "plpgsql", "int") == DBStatus::OK);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    const auto multiRows = g_engine.query(db, multi.tablename,
        {"=bucket 1", "typedexpr residual_multi_writer(id)>0 AND n IS NULL"}, {"id"});
    assert(multiRows.size() == 2);
    assert(g_engine.finishSqlCommand());
    const auto multiEffects = g_engine.plpgsqlQuery(db, "SELECT id FROM residual_effects");
    assert(multiEffects.ok && multiEffects.rowCount == 2);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    std::cout << "[INDEX RESIDUAL EXECUTION ONCE] passed\n";
}
