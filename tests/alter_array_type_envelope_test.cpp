#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    dbms::SQLParser parser;
    for (const auto& spelling : {std::string("TYPE"), std::string("SET DATA TYPE")}) {
        const auto parsed = parser.parse("ALTER TABLE t ALTER COLUMN a "+spelling+
            " NUMERIC(8,3)[], ALTER COLUMN b "+spelling+" CHARACTER VARYING(7)[][]");
        assert(parsed.success);
        const auto* alter = dynamic_cast<const dbms::AlterTableStmt*>(parsed.stmt.get());
        assert(alter && alter->subCommands.size() == 2);
        if (alter->subCommands[0].dataType != "NUMERIC(8,3)[]")
            std::cerr << "actual type envelope: " << alter->subCommands[0].dataType << '\n';
        assert(alter->subCommands[0].dataType == "NUMERIC(8,3)[]");
        assert(alter->subCommands[1].dataType == "CHARACTER VARYING(7)[][]");
    }
    for (const auto& envelope : {std::string("NUMERIC(8,3)[2][4]"),
         std::string("INTERVAL DAY TO SECOND(3)[][]"),
         std::string("TIMESTAMP(3) WITH TIME ZONE[][]"),
         std::string("NUMERIC(8,-2)[][]")}) {
        const auto parsed = parser.parse("ALTER TABLE t ALTER COLUMN a TYPE " + envelope);
        assert(parsed.success);
        const auto* alter = dynamic_cast<const dbms::AlterTableStmt*>(parsed.stmt.get());
        assert(alter && alter->subCommands.size() == 1 && alter->subCommands[0].dataType == envelope);
        const auto declaration = dbms::SQLParser::parseTypeSpecification(alter->subCommands[0].dataType);
        assert(declaration.isArray);
    }
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "alter_array_type_envelope";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(a NUMERIC(6,2)[],b VARCHAR(4)[])", session));
    assert(!ddl.executeSql("ALTER TABLE t ALTER COLUMN a TYPE NUMERIC(8,3)[], ALTER COLUMN b SET DATA TYPE VARCHAR(7)[]", session));
    const auto schema = g_engine.getTableSchema(database, "t");
    assert(schema.len == 2 && schema.cols[0].isArray && schema.cols[1].isArray);
    assert(schema.cols[0].dataType == "numeric");
    assert(schema.cols[1].dataType == "varchar" && schema.cols[1].dsize == 7);
    assert(!ddl.executeSql("DROP TABLE t", session));
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "[ALTER ARRAY TYPE ENVELOPE] passed\n";
}
