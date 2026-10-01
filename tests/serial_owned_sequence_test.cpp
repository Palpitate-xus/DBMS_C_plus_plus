#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("serial_owned_sequence");
    cleanupTestDb("serial_owned_sequence");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE serial_table(id SERIAL PRIMARY KEY,code TEXT)", session));
    dbms::SequenceInfo sequence;
    assert(g_engine.getSequenceInfo(database, "serial_table_id_seq", sequence) == dbms::DBStatus::OK);
    assert(sequence.ownedByTable == "serial_table" && sequence.ownedByColumn == "id");
    assert(sequence.minValue == 1 && sequence.maxValue == 2147483647);
    const dbms::TableSchema schema = g_engine.getTableSchema(database, "serial_table");
    assert(!schema.cols[0].isNull && !schema.cols[0].isAutoIncrement);
    assert(!schema.cols[0].defaultValue.empty());
    assert(g_engine.insert(database, "serial_table", {{"code", "kept"}}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "serial_table", {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.nextval(database, "serial_table_id_seq") == 2);
    assert(g_engine.insert(database, "serial_table", {{"code", "next"}}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "serial_table", {}, {"id"}) == (std::vector<std::string>{"1 ", "3 "}));
    assert(!ddl.executeSql("DROP TABLE serial_table", session));
    assert(!g_engine.sequenceExists(database, "serial_table_id_seq"));
    cleanupTestDb("serial_owned_sequence");
    std::cout << "[SERIAL OWNED SEQUENCE] passed" << std::endl;
}
