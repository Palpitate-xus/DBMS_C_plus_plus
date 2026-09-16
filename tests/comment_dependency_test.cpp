#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void setupSession(Session& session, const std::string& database) {
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 212121;
}

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("comment_dependency");
    const std::string backup = testDbPath("comment_dependency_backup");
    cleanupTestDb("comment_dependency");
    cleanupTestDb("comment_dependency_backup");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);

    Session session;
    setupSession(session, database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE notes (id INT, body VARCHAR(80))", session));
    assert(!ddl.executeSql(
        "CREATE INDEX notes_body_idx ON notes (body)", session));
    assert(!ddl.executeSql(
        "CREATE VIEW notes_view AS SELECT id, body FROM notes", session));
    assert(!ddl.executeSql("CREATE SEQUENCE notes_seq", session));

    assert(!ddl.executeSql(
        "COMMENT ON TABLE notes IS 'table comment'", session));
    assert(!ddl.executeSql(
        "COMMENT ON COLUMN notes.body IS 'column comment'", session));
    assert(!ddl.executeSql(
        "COMMENT ON INDEX notes_body_idx IS 'index comment'", session));
    assert(!ddl.executeSql(
        "COMMENT ON VIEW notes_view IS 'view comment'", session));
    assert(!ddl.executeSql(
        "COMMENT ON COLUMN notes_view.body IS 'view column comment'",
        session));
    assert(!ddl.executeSql(
        "COMMENT ON SEQUENCE notes_seq IS 'sequence comment'", session));
    assert(!ddl.executeSql(
        "COMMENT ON TYPE pg_catalog.int4 IS 'integer comment'", session));

    dbms::Oid tableOid = dbms::INVALID_OID;
    dbms::Oid indexOid = dbms::INVALID_OID;
    dbms::Oid viewOid = dbms::INVALID_OID;
    dbms::Oid sequenceOid = dbms::INVALID_OID;
    int32_t bodyNumber = 0;
    {
        auto& catalog = g_engine.catalogService().get(database);
        const auto* publicNamespace =
            catalog.findNamespaceByName("public");
        assert(publicNamespace != nullptr);
        const auto* table =
            catalog.findClassByName("notes", publicNamespace->oid);
        const auto* index = catalog.findClassByName(
            "notes_body_idx", publicNamespace->oid);
        const auto* view = catalog.findClassByName(
            "notes_view", publicNamespace->oid);
        const auto* sequence = catalog.findClassByName(
            "notes_seq", publicNamespace->oid);
        assert(table && table->relkind == 'r');
        assert(index && index->relkind == 'i');
        assert(view && view->relkind == 'v');
        assert(sequence && sequence->relkind == 'S');
        tableOid = table->oid;
        indexOid = index->oid;
        viewOid = view->oid;
        sequenceOid = sequence->oid;
        const auto* body = catalog.findAttribute(tableOid, "body");
        assert(body != nullptr);
        bodyNumber = body->attnum;
        const auto* viewBody = catalog.findAttribute(viewOid, "body");
        assert(viewBody != nullptr);
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, 0) ==
               "table comment");
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, bodyNumber) ==
               "column comment");
        assert(catalog.getDescription(
                   indexOid, dbms::PgClassOid_Class, 0) ==
               "index comment");
        assert(catalog.getDescription(
                   viewOid, dbms::PgClassOid_Class, 0) ==
               "view comment");
        assert(catalog.getDescription(
                   viewOid, dbms::PgClassOid_Class, viewBody->attnum) ==
               "view column comment");
        assert(catalog.getDescription(
                   sequenceOid, dbms::PgClassOid_Class, 0) ==
               "sequence comment");
        const auto* int4 = catalog.findTypeByName("int4", 11);
        assert(int4 != nullptr);
        assert(catalog.getDescription(
                   int4->oid, dbms::PgClassOid_Type, 0) ==
               "integer comment");
    }

    // Empty text is a stored pg_description row; only IS NULL removes it.
    assert(!ddl.executeSql("COMMENT ON TABLE notes IS ''", session));
    {
        auto& catalog = g_engine.catalogService().get(database);
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, 0).empty());
        assert(catalog.removeDescription(
            tableOid, dbms::PgClassOid_Class, 0));
    }
    assert(!ddl.executeSql(
        "COMMENT ON TABLE notes IS 'table comment'", session));
    assert(!ddl.executeSql("COMMENT ON TABLE notes IS NULL", session));
    {
        auto& catalog = g_engine.catalogService().get(database);
        assert(!catalog.removeDescription(
            tableOid, dbms::PgClassOid_Class, 0));
    }
    assert(!ddl.executeSql(
        "COMMENT ON TABLE notes IS 'backup comment'", session));

    // Object-address metadata follows relation and attribute renames.
    assert(!ddl.executeSql("ALTER TABLE notes RENAME TO journal", session));
    assert(!ddl.executeSql(
        "ALTER TABLE journal RENAME COLUMN body TO text", session));
    {
        auto& catalog = g_engine.catalogService().get(database);
        const auto* publicNamespace =
            catalog.findNamespaceByName("public");
        assert(publicNamespace != nullptr);
        const auto* renamed = catalog.findClassByName(
            "journal", publicNamespace->oid);
        assert(renamed && renamed->oid == tableOid);
        const auto* renamedColumn =
            catalog.findAttribute(tableOid, "text");
        assert(renamedColumn && renamedColumn->attnum == bodyNumber);
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, 0) ==
               "backup comment");
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, bodyNumber) ==
               "column comment");
    }

    // A physical backup carries pg_description.  Restore must also evict the
    // live CatalogManager so the restored rows, rather than stale memory, win.
    assert(g_engine.physicalBackup(database, backup));
    assert(!ddl.executeSql(
        "COMMENT ON TABLE journal IS 'after backup'", session));
    assert(g_engine.physicalRestore(database, backup));
    {
        auto& restored = g_engine.catalogService().get(database);
        const auto* publicNamespace =
            restored.findNamespaceByName("public");
        assert(publicNamespace != nullptr);
        const auto* table = restored.findClassByName(
            "journal", publicNamespace->oid);
        assert(table && table->oid == tableOid);
        assert(restored.getDescription(
                   tableOid, dbms::PgClassOid_Class, 0) ==
               "backup comment");
        assert(restored.getDescription(
                   tableOid, dbms::PgClassOid_Class, bodyNumber) ==
               "column comment");
    }

    // DROP TABLE CASCADE removes both the table and automatically dependent
    // index descriptions.  Names can be reused without inheriting metadata.
    assert(!ddl.executeSql("DROP TABLE journal CASCADE", session));
    {
        auto& catalog = g_engine.catalogService().get(database);
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, 0).empty());
        assert(catalog.getDescription(
                   tableOid, dbms::PgClassOid_Class, bodyNumber).empty());
        assert(catalog.getDescription(
                   indexOid, dbms::PgClassOid_Class, 0).empty());
    }
    assert(!ddl.executeSql("CREATE TABLE journal (id INT, text TEXT)", session));
    {
        auto& catalog = g_engine.catalogService().get(database);
        const auto* publicNamespace =
            catalog.findNamespaceByName("public");
        assert(publicNamespace != nullptr);
        const auto* replacement = catalog.findClassByName(
            "journal", publicNamespace->oid);
        assert(replacement && replacement->oid != tableOid);
        assert(catalog.getDescription(
                   replacement->oid, dbms::PgClassOid_Class, 0).empty());
    }

    // Unsupported families and missing objects fail; they cannot report
    // success while creating name-keyed orphan metadata.
    assert(ddl.executeSql(
        "COMMENT ON FUNCTION unsupported(integer) IS 'x'", session));
    assert(ddl.executeSql(
        "COMMENT ON DATABASE unsupported IS 'x'", session));
    assert(ddl.executeSql(
        "COMMENT ON TABLE missing_relation IS 'x'", session));
    assert(ddl.executeSql(
        "COMMENT ON COLUMN journal.missing_column IS 'x'", session));

    g_engine.catalogService().evict(database);
    fs::remove_all(database);
    fs::remove_all(backup);
    finalCleanupTestData();
    std::cout << "[COMMENT DEPENDENCY] all passed\n";
    return 0;
}
