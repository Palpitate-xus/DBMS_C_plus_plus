#include "catalog_generation_test_view.h"
#include "Session.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::string name = "view_replacement_contract";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    dbms_test::CatalogGenerationView catalog(g_engine, db);
    assert(!ddl.executeSql("CREATE TABLE rows(id INT,t TEXT,i INT,v VARCHAR(10),w VARCHAR(20))", session));
    assert(!ddl.executeSql("CREATE VIEW v AS SELECT id,t FROM rows", session));
    assert(!ddl.executeSql("CREATE VIEW UPPER_NATIVE AS SELECT ID FROM rows", session));
    assert(catalog().resolveRelationSnapshot("upper_native", {"public"}));
    assert(!catalog().resolveRelationSnapshot("UPPER_NATIVE", {"public"}));
    const auto original = catalog().resolveRelationSnapshot("v", {"public"});
    assert(original && original->relkind == 'v' && original->relnatts == 2);
    const auto oid = original->oid;

    const auto attributeState = [&](Oid relation) {
        std::string result;
        for (const auto& attribute : catalog().findAttributesByNum(relation))
            result += attribute.toString() + '\n';
        return result;
    };
    const auto dependencyState = [&](Oid relation) {
        std::string result;
        for (const auto& dependency : catalog().findDepends(PgClassOid_Class, relation))
            result += dependency.toString() + '\n';
        return result;
    };
    size_t rejected = 0;
    const auto reject = [&](const std::string& view, const std::string& sql, const std::string& state) {
        const auto before = catalog().resolveRelationSnapshot(view, {"public"});
        assert(before);
        const auto definition = g_engine.getViewSQL(db, view);
        const auto attributes = attributeState(before->oid);
        const auto dependencies = dependencyState(before->oid);
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
        std::cout << "VIEW_REPLACEMENT_NATIVE " << sql << ' ' << output.str();
        assert(failed && output.str().find("SQLSTATE " + state) != std::string::npos);
        const auto after = catalog().resolveRelationSnapshot(view, {"public"});
        assert(after && after->oid == before->oid && after->relnatts == before->relnatts);
        assert(g_engine.getViewSQL(db, view) == definition);
        assert(attributeState(after->oid) == attributes);
        assert(dependencyState(after->oid) == dependencies);
        assert(!g_engine.inTransaction());
        ++rejected;
    };
    for (const auto* projection : {"id,i", "id,i AS t", "id::BIGINT,t", "id AS renamed,t", "id"})
        reject("v", std::string("CREATE OR REPLACE VIEW v AS SELECT ") + projection + " FROM rows", "42P16");
    reject("v", "CREATE OR REPLACE VIEW v AS SELECT absent,t FROM rows", "42703");
    const auto* retired = &catalog();
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW v AS SELECT id+1 AS id,t,i FROM rows", session));
    const auto appended = catalog().resolveRelationSnapshot("v", {"public"});
    assert(appended && appended->oid == oid && appended->relnatts == 3);
    assert(retired->getClass(oid)->relnatts == 2);
    assert(retired->findAttributesByNum(oid).size() == 2);
    reject("v", "CREATE OR REPLACE VIEW v AS SELECT id,t FROM rows", "42P16");
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW v(id) AS SELECT id,t,i FROM rows", session));
    assert(catalog().findAttributesByNum(oid)[0].attname == "id");

    assert(!ddl.executeSql("CREATE VIEW width_v AS SELECT v FROM rows", session));
    const auto width = catalog().resolveRelationSnapshot("width_v", {"public"});
    assert(width);
    assert(catalog().findAttributesByNum(width->oid)[0].atttypmod == 14);
    reject("width_v", "CREATE OR REPLACE VIEW width_v AS SELECT w AS v FROM rows", "42P16");
    reject("width_v", "CREATE OR REPLACE VIEW width_v AS SELECT CAST(v AS VARCHAR(20)) AS v FROM rows", "42P16");
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW width_v AS SELECT CAST(w AS VARCHAR(10)) AS v FROM rows", session));
    assert(catalog().findAttributesByNum(width->oid)[0].atttypmod == 14);

    assert(!ddl.executeSql("CREATE VIEW constant_v AS SELECT 'constant' AS label", session));
    const auto constant = catalog().resolveRelationSnapshot("constant_v", {"public"});
    assert(constant && constant->relnatts == 1);
    assert(catalog().findAttributesByNum(constant->oid)[0].atttypid == 25);
    reject("constant_v", "CREATE OR REPLACE VIEW constant_v AS SELECT 7 AS label", "42P16");
    assert(!ddl.executeSql("CREATE VIEW pure_v AS SELECT 1/0 AS danger", session));
    assert(!ddl.executeSql("CREATE SEQUENCE calls", session));
    assert(!ddl.executeSql("CREATE VIEW volatile_v AS SELECT nextval('calls') AS id", session));
    assert(g_engine.nextval(db, "calls") == 1);

    // Historical constant/expression views were stored with no attributes.
    // Replacing one must still validate its old query, without publishing a
    // repaired descriptor as a side effect of a rejected replacement.
    assert(!ddl.executeSql("CREATE VIEW legacy_v AS SELECT 'kept' AS label", session));
    auto legacy = catalog().resolveRelationSnapshot("legacy_v", {"public"});
    assert(legacy);
    legacy->relnatts = 0;
    assert(catalog().updateClass(legacy->oid, *legacy));
    assert(catalog().replaceAttributes(legacy->oid, {}));
    assert(catalog().persistAll());
    reject("legacy_v", "CREATE OR REPLACE VIEW legacy_v AS SELECT 7 AS label", "42P16");
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW legacy_v AS SELECT 'new' AS label", session));

    assert(!ddl.executeSql("CREATE TABLE array_rows(a INT[],b TEXT[])", session));
    assert(!ddl.executeSql("CREATE VIEW array_v AS SELECT a FROM array_rows", session));
    const auto array = catalog().resolveRelationSnapshot("array_v", {"public"});
    assert(array);
    auto arrayAttributes = catalog().findAttributesByNum(array->oid);
    assert(arrayAttributes.size() == 1 && arrayAttributes[0].atttypid == 1007 && arrayAttributes[0].attndims == 1);
    reject("array_v", "CREATE OR REPLACE VIEW array_v AS SELECT b AS a FROM array_rows", "42P16");
    // Old view catalogs can retain element OID + attndims instead of the
    // genuine array OID. Compare its real array type, not its old encoding.
    arrayAttributes[0].atttypid = 23;
    assert(catalog().replaceAttributes(array->oid, arrayAttributes));
    assert(catalog().persistAll());
    assert(!ddl.executeSql("CREATE OR REPLACE VIEW array_v AS SELECT ARRAY[3,4] AS a FROM array_rows", session));

    g_engine.catalogService().evict(db);
    dbms_test::CatalogGenerationView cold(g_engine, db);
    assert(cold().getClass(oid)->relnatts == 3);
    assert(cold().findAttributesByNum(oid).size() == 3);
    assert(cold().findAttributesByNum(width->oid)[0].atttypmod == 14);
    assert(cold().findAttributesByNum(constant->oid)[0].atttypid == 25);
    assert(cold().findAttributesByNum(legacy->oid)[0].atttypid == 25);
    assert(cold().findAttributesByNum(array->oid)[0].atttypid == 1007);
    assert(cold().findAttributesByNum(array->oid)[0].attndims == 1);
    assert(rejected == 12);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[VIEW REPLACEMENT CONTRACT] 12 strict failures preserve OID/definition/attributes/dependencies; legal append, modifiers, purity, legacy, array identity and cold catalog read passed\n";
}
