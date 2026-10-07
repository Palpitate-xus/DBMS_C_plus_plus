#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("domain_ancestry_storage");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session; session.currentDB = db; session.username = "testuser"; session.permission = 1;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE DOMAIN domain_parent AS TEXT DEFAULT 'seed' NOT NULL CHECK(VALUE <> 'VALUE')", session));
    assert(!ddl.executeSql("CREATE DOMAIN domain_middle AS domain_parent CHECK(length(VALUE) < 6)", session));
    assert(!ddl.executeSql("CREATE DOMAIN domain_outer AS domain_middle DEFAULT 'near' CHECK(VALUE <> 'bad')", session));
    assert(!ddl.executeSql("CREATE TABLE domain_rows(id INT,x domain_outer CHECK(x <> 'col'))", session));
    assert(g_engine.insert(db,"domain_rows",{{"id","1"}}) == DBStatus::OK);
    assert(g_engine.insert(db,"domain_rows",{{"id","2"},{"x","VALUE"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.insert(db,"domain_rows",{{"id","2"},{"x","longer"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.insert(db,"domain_rows",{{"id","2"},{"x","bad"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.insert(db,"domain_rows",{{"id","2"},{"x","col"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.insertRow(db,"domain_rows",{{"id","2"},{"x",std::nullopt}}) == DBStatus::NULL_NOT_ALLOWED);
    assert(!ddl.executeSql("CREATE DOMAIN domain_null AS domain_middle DEFAULT NULL", session));
    assert(!ddl.executeSql("CREATE TABLE domain_null_rows(x domain_null)", session));
    assert(g_engine.insertDefaultValues(db,"domain_null_rows",g_engine.getTableSchema(db,"domain_null_rows")) == DBStatus::NULL_NOT_ALLOWED);
    const auto schema = g_engine.getTableSchema(db,"domain_rows");
    assert(schema.cols[1].domainName == "\"public\".\"domain_outer\"");
    assert(schema.cols[1].isNull); // domain NOT NULL is not pg_attribute NOT NULL
    assert(schema.cols[1].defaultValue == "'near'");
    assert(schema.cols[1].checkExpr.find("'VALUE'") != std::string::npos);
    auto& catalog = g_engine.catalogService().get(db);
    const auto* ns = catalog.findNamespaceByName("public"); assert(ns);
    const auto* parent = catalog.findTypeByName("domain_parent",ns->oid); assert(parent);
    const Oid parentOid = parent->oid;
    const auto* middle = catalog.findTypeByName("domain_middle",ns->oid); assert(middle);
    const Oid middleOid = middle->oid;
    const auto* outer = catalog.findTypeByName("domain_outer",ns->oid); assert(outer);
    const Oid outerOid = outer->oid;
    assert(parent->typbasetype == 25 && parent->typnotnull);
    assert(middle->typbasetype == parentOid && !middle->typnotnull);
    assert(outer->typbasetype == middleOid);
    const auto* relation = catalog.resolveRelation("domain_rows",{"public"}); assert(relation);
    const auto attributes = catalog.findAttributes(relation->oid);
    bool found = false;
    for (const auto& attr:attributes) if(attr.attname=="x") {
        found = true; assert(attr.atttypid == outerOid && !attr.attnotnull && attr.atttypmod == -1);
    }
    assert(found && catalog.persistAll());
    const auto snapshot = CatalogManager::readMetadataSnapshot((g_engine.dbPath(db)/"pg_catalog").string());
    bool reopened = false;
    for (const auto& type:snapshot.types) if(type.oid==outerOid) {
        reopened = true; assert(type.typtype == 'd' && type.typbasetype == middleOid);
    }
    assert(reopened);
    StorageEngine reader;
    std::cerr << "DOMAIN_NATIVE_COLD_COLUMNS\n";
    const auto identities = reader.catalogService().domainColumns(db,"domain_rows");
    assert(identities.size() == 1 && identities.at("x") == "\"public\".\"domain_outer\"");
    const auto ancestry = reader.resolveDomainAncestry(db,"domain_outer");
    std::cerr << "DOMAIN_NATIVE_ANCESTRY\n";
    assert(ancestry.domains.size() == 3 && ancestry.notNull && ancestry.defaultValue == "'near'");
    const auto prepared = reader.prepareBoundQuery(db,"SELECT x FROM domain_rows WHERE false");
    std::cerr << "DOMAIN_NATIVE_PREPARED\n";
    assert(prepared.output.size() == 1 && prepared.output[0].type == "text");
    for (int i=0;i<3;++i)
    {
        std::cerr << "DOMAIN_NATIVE_SCHEMA_CACHE " << i << '\n';
        assert(reader.getTableSchema(db,"domain_rows").cols[1].domainName == identities.at("x"));
    }
    std::cerr << "DOMAIN_NATIVE_READ_METADATA\n";
    const auto metadataPath = g_engine.dbPath(db)/".domains";
    std::ifstream input(metadataPath,std::ios::binary);
    const std::string metadata{std::istreambuf_iterator<char>(input),{}};
    input.close();
    const auto locksBefore = g_engine.getLockManager().lockedTables();
    std::cerr << "DOMAIN_NATIVE_CORRUPT_GUARD\n";
    { std::ofstream corrupt(metadataPath,std::ios::app); corrupt << "@D2|00|00|00|00|00|2|0\n"; }
    bool failed = false;
    try { (void)g_engine.insert(db,"domain_rows",{{"id","2"},{"x","ok"}}); }
    catch (const DbError& error) { failed = error.sqlState() == "XX001"; }
    std::cerr << "DOMAIN_NATIVE_AFTER_ERROR_TRANSACTION " << g_engine.inTransaction() << '\n';
    assert(failed && g_engine.getLockManager().lockedTables() == locksBefore);
    assert(!g_engine.inTransaction());
    std::cerr << "DOMAIN_NATIVE_RESTORE\n";
    { std::ofstream restored(metadataPath,std::ios::binary|std::ios::trunc); restored << metadata; }
    assert(g_engine.insert(db,"domain_rows",{{"id","2"},{"x","ok"}}) == DBStatus::OK);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db,"domain_rows",{{"id","3"},{"x","pre"}}) == DBStatus::OK);
    assert(g_engine.savepoint("user_before_error") == DBStatus::OK);
    { std::ofstream corrupt(metadataPath,std::ios::app); corrupt << "@D2|00|00|00|00|00|2|0\n"; }
    failed = false;
    std::vector<StorageEngine::SqlRow> returned{{{"id", "prior"}}};
    try { (void)g_engine.insertRow(db,"domain_rows",{{"id","4"},{"x","ok"}},&returned); }
    catch (const DbError& error) { failed = error.sqlState() == "XX001"; }
    assert(failed && g_engine.inTransaction() && returned.size() == 1);
    { std::ofstream restored(metadataPath,std::ios::binary|std::ios::trunc); restored << metadata; }
    assert(g_engine.rollbackToSavepoint("user_before_error") == DBStatus::OK);
    assert(g_engine.releaseSavepoint("user_before_error") == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.query(db,"domain_rows",{"=id 3"},{"id"}).size() == 1);
    assert(g_engine.query(db,"domain_rows",{"=id 4"},{"id"}).empty());
    setCurrentSession(nullptr);
    std::cout << "[DOMAIN ANCESTRY STORAGE] all checks/default/NULL/catalog OIDs/reopen/pure value descriptor passed\n";
}
