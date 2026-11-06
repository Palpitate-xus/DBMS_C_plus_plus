#include "catalog_generation_test_view.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "view_column_dependency";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    dbms_test::CatalogGenerationView catalog(g_engine, db);
    size_t checks = 0, failures = 0;
    const auto check = [&](bool condition, const std::string& description) {
        ++checks;
        if (!condition) { ++failures; std::cerr << "FAIL " << description << '\n'; }
    };
    const auto execute = [&](const std::string& sql) {
        std::ostringstream output;
        bool failed;
        {
            struct Capture {
                std::streambuf* old;
                explicit Capture(std::ostream& replacement) : old(std::cout.rdbuf(replacement.rdbuf())) {}
                ~Capture() { std::cout.rdbuf(old); }
            } capture(output);
            failed = ddl.executeSql(sql, session);
        }
        std::cout << "VIEW_COLUMN_NATIVE " << sql << " => " << output.str() << std::flush;
        return std::make_pair(failed, output.str());
    };
    const auto setup = [&](const std::string& sql) { assert(!execute(sql).first); };
    const auto columnExists = [&](const std::string& table, const std::string& column) {
        const auto schema = g_engine.getTableSchema(db, table);
        for (size_t i = 0; i < schema.len; ++i)
            if (schema.cols[i].dataName == column) return true;
        return false;
    };
    const auto restrictColumn = [&](const std::string& table, const std::string& column) {
        const auto result = execute("ALTER TABLE " + table + " DROP COLUMN " + column);
        check(result.first && result.second.find("SQLSTATE 2BP01") != std::string::npos,
              table + " rejects referenced " + column + " with 2BP01");
        check(columnExists(table, column), table + " preserves referenced column");
        check(!g_engine.inTransaction(), table + " closes rejected DDL owner");
    };
    setup("CREATE TABLE rhs(n INT)");
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"direct", "SELECT n FROM $"},
        {"expression", "SELECT n+1 AS value FROM $"},
        {"alias", "SELECT a.hidden AS value FROM $ a(first,k,hidden,pad)"},
        {"predicate", "SELECT id FROM $ WHERE n>0"},
        {"order", "SELECT id FROM $ ORDER BY n"},
        {"group", "SELECT n,count(*) AS total FROM $ GROUP BY n HAVING sum(id)>0"},
        {"distinct", "SELECT DISTINCT ON(n) id FROM $ ORDER BY n,id"},
        {"window", "SELECT row_number() OVER(PARTITION BY n ORDER BY id) AS pos FROM $"},
        {"cte", "WITH q AS (SELECT id,n FROM $) SELECT id FROM q"},
        {"derived", "SELECT q.id FROM (SELECT id,n FROM $) q"},
        {"sublink", "SELECT id FROM $ WHERE EXISTS(SELECT 1 FROM rhs r WHERE r.n=$.n)"},
        {"using", "SELECT a.id FROM $ a JOIN rhs b USING(n)"},
        {"natural", "SELECT 1 AS value FROM $ a NATURAL JOIN rhs b"},
        {"join", "SELECT a.id FROM $ a JOIN rhs b ON a.n=b.n"},
        {"selfjoin", "SELECT a.id FROM $ a JOIN $ b ON a.n=b.id"},
        {"case_array_row", "SELECT CASE WHEN id>0 THEN ARRAY[n] ELSE ARRAY[id] END AS value FROM $ WHERE ROW(n,id)=ROW(20,10)"},
        {"filter", "SELECT count(*) FILTER(WHERE n>0) AS total FROM $"},
        {"union", "SELECT id AS value FROM $ UNION ALL SELECT n FROM $"}
    };
    for (const auto& item : cases) {
        const std::string table = "t_" + item.first, view = "v_" + item.first;
        setup("CREATE TABLE " + table + "(first INT,id INT,n INT,unused INT)");
        std::string query = item.second;
        for (size_t at = query.find('$'); at != std::string::npos; at = query.find('$'))
            query.replace(at, 1, table);
        setup("CREATE VIEW " + view + " AS " + query);
        assert(g_engine.commentOnColumn(db, table, "n", "keep comment") == DBStatus::OK);
        const auto definition = g_engine.getViewSQL(db, view);
        restrictColumn(table, "n");
        check(g_engine.getViewSQL(db, view) == definition, table + " preserves view bytes");
        check(g_engine.getColumnComment(db, table, "n") == "keep comment", table + " preserves comment");
        check(!execute("ALTER TABLE " + table + " DROP COLUMN unused").first,
              table + " permits unreferenced column");
        check(!execute("ALTER TABLE " + table + " DROP COLUMN first").first,
              table + " permits earlier unreferenced column");
        restrictColumn(table, "n");
    }
    setup("CREATE TABLE star_rows(id INT,n INT)");
    setup("CREATE VIEW star_v AS SELECT * FROM star_rows");
    setup("ALTER TABLE star_rows ADD COLUMN appended INT");
    check(!execute("ALTER TABLE star_rows DROP COLUMN appended").first,
          "new expanded star does not acquire later added columns");
    restrictColumn("star_rows", "n");

    setup("CREATE TABLE replace_rows(id INT,n INT)");
    setup("CREATE VIEW replace_v AS SELECT n AS value FROM replace_rows");
    const auto viewOid = catalog().resolveRelationSnapshot("replace_v", {"public"})->oid;
    setup("CREATE OR REPLACE VIEW replace_v AS SELECT id AS value FROM replace_rows");
    check(catalog().getClass(viewOid).has_value(), "replacement preserves view identity");
    check(!execute("ALTER TABLE replace_rows DROP COLUMN n").first,
          "replacement removes obsolete column dependency");
    restrictColumn("replace_rows", "id");

    setup("CREATE TABLE whole_rows(id INT,n INT)");
    setup("CREATE VIEW whole_v AS SELECT 7 AS value FROM whole_rows");
    const auto wholeTableDrop = execute("DROP TABLE whole_rows");
    check(wholeTableDrop.first && wholeTableDrop.second.find("SQLSTATE 2BP01") != std::string::npos,
          "whole relation dependency reports 2BP01");
    check(g_engine.tableExists(db, "whole_rows") && g_engine.viewExists(db, "whole_v"),
          "whole relation restriction preserves table and view");
    setup("CREATE VIEW whole_child AS SELECT value FROM whole_v");
    const auto wholeViewDrop = execute("DROP VIEW whole_v");
    check(wholeViewDrop.first && wholeViewDrop.second.find("SQLSTATE 2BP01") != std::string::npos,
          "view dependency reports 2BP01");
    check(g_engine.viewExists(db, "whole_v") && g_engine.viewExists(db, "whole_child"),
          "view restriction preserves parent and child");

    // The compiled pg_class source exposes a smaller, reordered descriptor.
    // Its seventh source field is relowner, not catalog attribute number 7.
    const auto catalogDeclaration = execute("CREATE VIEW catalog_owner AS SELECT relowner FROM pg_catalog.pg_class");
    check(!catalogDeclaration.first, "compiled catalog is a real view read source");
    const auto catalogView = catalog().resolveRelationSnapshot("catalog_owner", {"public"});
    const auto ownerAttribute = catalog().getAttribute(PgClassOid_Class, "relowner");
    assert(ownerAttribute);
    bool ownerDependency = false;
    if (catalogView)
        for (const auto& dependency : catalog().findDepends(PgClassOid_Class, catalogView->oid))
            ownerDependency |= dependency.refclassid == PgClassOid_Class &&
                dependency.refobjid == PgClassOid_Class && dependency.refobjsubid == ownerAttribute->attnum;
    check(ownerDependency, "partial catalog source keeps actual column identity");
    const auto partialAlias = execute("CREATE VIEW partial_alias AS SELECT a.owner FROM pg_catalog.pg_class a(k,r,n,kind,atts,persist,owner)");
    check(!partialAlias.first, "aliased partial catalog declaration is preserved");
    const auto aliasView = catalog().resolveRelationSnapshot("partial_alias", {"public"});
    bool aliasDependency = false;
    if (aliasView)
        for (const auto& dependency : catalog().findDepends(PgClassOid_Class, aliasView->oid))
            aliasDependency |= dependency.refclassid == PgClassOid_Class &&
                dependency.refobjid == PgClassOid_Class && dependency.refobjsubid == ownerAttribute->attnum;
    check(aliasDependency, "partial catalog column alias retains original declaration identity");

    setup("CREATE TABLE chain_rows(id INT,n INT)");
    setup("CREATE VIEW chain_a AS SELECT n FROM chain_rows");
    setup("CREATE VIEW chain_b AS SELECT n+1 AS value FROM chain_a");
    setup("CREATE VIEW chain_c AS SELECT value FROM chain_b");
    setup("CREATE VIEW chain_kept AS SELECT id FROM chain_rows");
    const auto cascade = execute("ALTER TABLE chain_rows DROP COLUMN n CASCADE");
    check(!cascade.first, "column CASCADE succeeds");
    for (const auto* view : {"chain_a", "chain_b", "chain_c"}) {
        check(!g_engine.viewExists(db, view), std::string(view) + " physical CASCADE removal");
        check(!catalog().resolveRelationSnapshot(view, {"public"}), std::string(view) + " catalog CASCADE removal");
    }
    check(g_engine.viewExists(db, "chain_kept"), "CASCADE keeps unrelated view");
    check(columnExists("chain_rows", "id") && !columnExists("chain_rows", "n"),
          "CASCADE preserves unrelated column");

    setup("CREATE TABLE rollback_rows(id INT,n INT)");
    setup("CREATE VIEW rollback_v AS SELECT n FROM rollback_rows");
    const auto rollbackDefinition = g_engine.getViewSQL(db, "rollback_v");
    const auto rejected = execute("ALTER TABLE rollback_rows DROP COLUMN n CASCADE,DROP COLUMN missing");
    check(rejected.first && rejected.second.find("SQLSTATE 42703") != std::string::npos,
          "later subcommand failure remains 42703");
    check(columnExists("rollback_rows", "n") && g_engine.viewExists(db, "rollback_v"),
          "failed multi-action DDL restores column and physical view");
    check(g_engine.getViewSQL(db, "rollback_v") == rollbackDefinition,
          "failed multi-action DDL restores exact view definition");
    restrictColumn("rollback_rows", "n");
    check(!execute("ALTER TABLE rollback_rows DROP COLUMN IF EXISTS missing CASCADE").first,
          "missing IF EXISTS column does not cascade");
    check(g_engine.viewExists(db, "rollback_v"), "IF EXISTS preserves view");

    // Reproduce the old persisted relation-only representation, including a
    // complex view that had no relation edge. Actual old-program/cold image
    // compatibility is additionally exercised by the protocol evidence.
    setup("CREATE TABLE legacy_rows(first INT,id INT,n INT,unused INT)");
    setup("CREATE VIEW legacy_a AS SELECT id FROM legacy_rows WHERE n>0");
    setup("CREATE VIEW legacy_b AS SELECT a.id FROM legacy_a a JOIN rhs b ON a.id=b.n");
    for (const auto* view : {"legacy_a", "legacy_b"}) {
        const auto oid = catalog().resolveRelationSnapshot(view, {"public"})->oid;
        for (const auto& dependency : catalog().findDepends(PgClassOid_Class, oid))
            if (dependency.refclassid == PgClassOid_Class &&
                (dependency.refobjsubid > 0 || std::string(view) == "legacy_b"))
                assert(catalog().removeDepend(dependency.classid, dependency.objid, dependency.objsubid,
                    dependency.refclassid, dependency.refobjid, dependency.refobjsubid));
    }
    assert(catalog().persistAll());
    g_engine.catalogService().evict(db);
    restrictColumn("legacy_rows", "n");
    check(!execute("ALTER TABLE legacy_rows DROP COLUMN unused").first,
          "legacy metadata permits unused column");
    check(!execute("ALTER TABLE legacy_rows DROP COLUMN first").first,
          "legacy metadata survives ordinal compaction");
    restrictColumn("legacy_rows", "n");
    check(!execute("ALTER TABLE legacy_rows DROP COLUMN n CASCADE").first,
          "legacy column CASCADE succeeds");
    check(!g_engine.viewExists(db, "legacy_a") && !g_engine.viewExists(db, "legacy_b"),
          "legacy CASCADE follows derived relation closure");

    setup("CREATE TABLE legacy_star_rows(id INT,n INT)");
    setup("CREATE VIEW legacy_star AS SELECT * FROM legacy_star_rows");
    setup("CREATE TABLE legacy_literal_rows(id INT,n INT,unused INT)");
    setup("CREATE VIEW legacy_literal AS SELECT id,'\nBASE_TABLE:datum' AS marker FROM legacy_literal_rows WHERE n>0");
    for (const auto* view : {"legacy_star", "legacy_literal"}) {
        const auto oid = catalog().resolveRelationSnapshot(view, {"public"})->oid;
        for (const auto& dependency : catalog().findDepends(PgClassOid_Class, oid))
            if (dependency.refclassid == PgClassOid_Class && dependency.refobjsubid > 0)
                assert(catalog().removeDepend(dependency.classid, dependency.objid, dependency.objsubid,
                    dependency.refclassid, dependency.refobjid, dependency.refobjsubid));
    }
    assert(catalog().persistAll());
    g_engine.catalogService().evict(db);
    setup("ALTER TABLE legacy_star_rows ADD COLUMN appended INT");
    check(!execute("ALTER TABLE legacy_star_rows DROP COLUMN appended").first,
          "legacy star keeps its witnessed historical source prefix");
    restrictColumn("legacy_star_rows", "n");
    check(!execute("ALTER TABLE legacy_literal_rows DROP COLUMN unused").first,
          "legacy dependency parsing preserves a metadata-looking literal");
    restrictColumn("legacy_literal_rows", "n");

    setup("CREATE SEQUENCE calls");
    setup("CREATE TABLE pure_rows(id INT,n INT,unused INT)");
    setup("CREATE VIEW pure_v AS SELECT nextval('calls')+n AS value,1/0 AS danger FROM pure_rows");
    check(g_engine.nextval(db, "calls") == 1, "view declaration never evaluates volatile target");
    restrictColumn("pure_rows", "n");
    check(!execute("ALTER TABLE pure_rows DROP COLUMN unused").first, "pure unused drop succeeds");
    check(g_engine.nextval(db, "calls") == 2, "dependency checking never evaluates view expressions");
    g_engine.catalogService().evict(db);
    restrictColumn("pure_rows", "n");
    check(g_engine.nextval(db, "calls") == 3, "cold dependency checking remains pure");
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "VIEW_COLUMN_DEPENDENCY checks=" << checks << " failures=" << failures << '\n';
    return failures ? 1 : 0;
}
