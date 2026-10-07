#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::StorageEngine engine;
    const auto db = testDbPath("domain_ancestry_metadata");
    assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    const auto path = std::filesystem::path(db) / ".domains";
    { std::ofstream legacy(path); legacy << "legacy|VARCHAR(4)|'base'|length(VALUE) < 5|legacy_check\n"; }
    auto old = engine.getDomain(db, "legacy");
    assert(old.name == "legacy" && old.hasDefault && !old.notNull);
    dbms::StorageEngine::DomainInfo inner;
    inner.name = "\"public\".\"inner\""; inner.baseType = "legacy";
    inner.notNull = true; inner.checkExpr = "VALUE <> 'VALUE|text'";
    assert(engine.createDomain(db, inner) == dbms::DBStatus::OK);
    dbms::StorageEngine::DomainInfo outer;
    outer.name = "\"public\".\"outer\""; outer.baseType = inner.name;
    outer.hasDefault = true; outer.defaultValue = "NULL";
    assert(engine.createDomain(db, outer) == dbms::DBStatus::OK);
    auto ancestry = engine.resolveDomainAncestry(db, "outer");
    assert(ancestry.domains.size() == 3 && ancestry.notNull && ancestry.hasDefault);
    assert(ancestry.defaultValue == "NULL" && ancestry.baseType == "VARCHAR(4)");
    dbms::StorageEngine reloaded;
    assert(reloaded.getDomain(db, "inner").checkExpr == inner.checkExpr);
    assert(reloaded.getDomain(db, "outer").hasDefault);
    outer.hasDefault = false; outer.defaultValue.clear();
    assert(engine.alterDomain(db, "outer", outer) == dbms::DBStatus::OK);
    // D3 has its own resolved default. DROP does not revert to a live parent.
    assert(!engine.resolveDomainAncestry(db, "outer").hasDefault);
    // Only actual old metadata retains the historical live fallback. A new
    // caller struct with its defaultResolved flag unset cannot downgrade D3.
    { std::ofstream legacyChild(path,std::ios::app);
      legacyChild << "legacy_outer|\"public\".\"inner\"|||\n"; }
    assert(engine.resolveDomainAncestry(db,"legacy_outer").defaultValue=="'base'");
    const auto quoted = dbms::SQLParser::parseTypeSpecification("\"S\" . \"Mixed\"");
    assert(quoted.typeName == "\"S\".\"Mixed\"");
    const auto numeric = dbms::SQLParser::parseTypeSpecification("NUMERIC(8,2)");
    assert(numeric.typeMods == std::vector<std::string>({"8", "2"}));
    const auto time = dbms::SQLParser::parseTypeSpecification("TIME(3) WITH TIME ZONE[]");
    assert(time.typeName == "timetz" && time.isArray && time.typeMods[0] == "3");
    outer.baseType = outer.name;
    assert(engine.alterDomain(db, "outer", outer) == dbms::DBStatus::OK);
    bool cycle = false;
    try { engine.resolveDomainAncestry(db, "outer"); }
    catch (const dbms::DbError& error) { cycle = error.sqlState() == "XX001"; }
    assert(cycle);
    dbms::StorageEngine::DomainInfo deep;
    for (int i=0;i<65;++i) {
        deep.name = "deep_" + std::to_string(i);
        deep.baseType = i == 0 ? "INT" : "deep_" + std::to_string(i-1);
        assert(engine.createDomain(db,deep) == dbms::DBStatus::OK);
    }
    bool depth = false;
    try { engine.resolveDomainAncestry(db,"deep_64"); }
    catch (const dbms::DbError& error) { depth = error.sqlState() == "54001"; }
    assert(depth);
    { std::ofstream broken(path, std::ios::app); broken << "@D2|00|00|00|00|00|2|0\n"; }
    bool corrupt = false;
    try { reloaded.getDomain(db, "legacy"); }
    catch (const dbms::DbError& error) { corrupt = error.sqlState() == "XX001"; }
    assert(corrupt);
    std::cout << "[DOMAIN ANCESTRY METADATA] old/v2/reopen/NULL/check/cycle/corrupt/type-envelope passed\n";
}
