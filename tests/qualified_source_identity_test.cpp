#include "parser/query_binding.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    size_t metadataReads = 0;
    QueryBindingMetadata metadata;
    metadata.relation = [&](const std::string& input) {
        ++metadataReads;
        CatalogManager::QualifiedName name;
        assert(CatalogManager::parseQualifiedName(input, name, true));
        if (name.name != "t") throw DbError("42P01", "missing relation");
        const std::string schema = name.schema.empty() || name.schema == "pg_temp" ? "pg_temp_42" : name.schema;
        return QueryRelationMetadata{schema,"t",{{"id","integer"},{"ID","text"}}, {}};
    };
    for (const auto* sql : {
            "SELECT pg_temp.t.id FROM pg_temp.t WHERE FALSE",
            "SELECT pg_temp.t.id FROM t WHERE FALSE",
            "SELECT pg_temp_42.t.id FROM pg_temp.t WHERE FALSE",
            "SELECT pg_temp.t.\"ID\" FROM t WHERE FALSE",
            "SELECT pg_temp.t.* FROM pg_temp.t WHERE FALSE"}) {
        auto prepared = prepareQuery(sql, {}, metadata);
        const auto* select = dynamic_cast<SelectStmt*>(prepared.ast.get());
        assert(select && prepared.sourceRanges.size() == 1);
        assert(prepared.sourceRanges[0].relationSchema == "pg_temp_42");
        const auto& bindings = prepared.projectionBindings.at(select);
        assert(bindings.size() == prepared.output.size());
        for (size_t i = 0; i < bindings.size(); ++i) {
            assert(bindings[i].column && bindings[i].column->scopeDepth == 0);
            assert(bindings[i].column->sourceOrdinal == prepared.sourceRanges[0].ordinal);
        }
    }
    for (const auto* sql : {
            "SELECT other.t.id FROM t WHERE FALSE",
            "SELECT pg_temp.t.id FROM t AS t WHERE FALSE",
            "SELECT pg_temp.t.* FROM t AS t WHERE FALSE"}) {
        bool failed = false;
        try { (void)prepareQuery(sql, {}, metadata); }
        catch (const DbError& error) { failed = true; assert(error.sqlState() == "42P01"); }
        assert(failed);
    }
    assert(metadataReads != 0); // There is no executor callback in this fixture.
    std::cout << "[QUALIFIED SOURCE IDENTITY] passed\n";
}
