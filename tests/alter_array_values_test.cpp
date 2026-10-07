#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <map>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "alter_array_values";
    cleanupTestDb(name);
    const std::string database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT PRIMARY KEY,i INT[],n NUMERIC(6,2)[],v VARCHAR(4)[])", session));
    assert(g_engine.insert(database, "t", {{"id","1"},{"i","{{32768,NULL},{-32769,2}}"},
        {"n","{12.35,NULL,-7.89}"},{"v","{\"NULL\",\"\",abcd,NULL}"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(database, "t", {{"id","2"},{"i",std::nullopt},
        {"n",std::nullopt},{"v",std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id","3"},{"i","{}"},{"n","{}"},{"v","{}"}}) == dbms::DBStatus::OK);
    auto values = [&]() {
        const auto schema = g_engine.getTableSchema(database, "t");
        std::map<std::string,std::map<std::string,std::optional<std::string>>> rows;
        assert(g_engine.forEachRow(database, "t", [&](uint32_t page, uint16_t slot, const char* data, size_t length) {
            const std::string row(data,length);
            const std::string id = g_engine.extractColumnValue(row,schema,0,database,true);
            const int64_t rid = dbms::StorageEngine::encodeRid(page,slot);
            for (size_t column=0;column<schema.len;++column)
                rows[id][schema.cols[column].dataName] = g_engine.isColumnNullByRid(database,"t",rid,column)
                    ? std::nullopt : std::optional<std::string>(g_engine.extractColumnValue(row,schema,column,database,true));
        }));
        return rows;
    };
    dbms::Column bigint = dbms::makeIntColumn("i",true,3);
    bigint.isArray = true;
    assert(g_engine.alterTableAlterColumnType(database,"t","i",bigint) == dbms::DBStatus::OK);
    const auto widened = values();
    assert(widened.size() == 3 && widened.at("1").at("i") == "{{32768,NULL},{-32769,2}}");
    assert(!widened.at("2").at("i") && widened.at("3").at("i") == "{}");
    const auto schema = g_engine.getTableSchema(database,"t");
    assert(schema.cols[1].dataType == "bigint" && schema.cols[1].isArray && schema.cols[1].isVariableLength);
    dbms::Column smallint = dbms::makeIntColumn("i",true,0);
    smallint.isArray = true;
    bool rejected = false;
    try { (void)g_engine.alterTableAlterColumnType(database,"t","i",smallint); }
    catch (const dbms::DbError& error) { rejected = error.sqlState() == "22003"; }
    assert(rejected && values() == widened);
    dbms::Column numeric = dbms::makeDecimalColumn("n",true,4,1);
    numeric.isArray = true;
    assert(g_engine.alterTableAlterColumnType(database,"t","n",numeric,{"4","1"}) == dbms::DBStatus::OK);
    assert(values().at("1").at("n") == "{12.4,NULL,-7.9}");
    dbms::Column varchar = dbms::makeVarCharColumn("v",true,2);
    varchar.isArray = true;
    const auto beforeNarrowing = values();
    rejected = false;
    try { (void)g_engine.alterTableAlterColumnType(database,"t","v",varchar,{"2"}); }
    catch (const dbms::DbError& error) { rejected = error.sqlState() == "22001"; }
    assert(rejected && values() == beforeNarrowing);
    assert(!ddl.executeSql("DROP TABLE t",session));
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[ALTER ARRAY VALUES] passed\n";
}
