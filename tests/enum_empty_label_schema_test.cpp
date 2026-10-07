#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "Session.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("enum_empty_label_schema");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    const std::vector<std::string> labels{"zeta", "", "alpha", "NULL", "it's"};
    assert(!ddl.executeSql("CREATE TYPE rank_type AS ENUM ('zeta','','alpha','NULL','it''s')", session));
    assert(!ddl.executeSql("CREATE TABLE ranks (id INT PRIMARY KEY, r rank_type)", session));
    const auto schema = g_engine.getTableSchema(db, "ranks");
    std::cout << "ENUM_EMPTY_SCHEMA_LABEL_COUNT=" << schema.cols[1].enumValues.size() << std::endl;
    assert(schema.len == 2 && schema.cols[1].enumValues == labels);
    assert(schema.cols[1].dataType == "rank_type" && schema.cols[1].isNull);
    const auto& catalog = g_engine.catalogService().get(db);
    const auto* ns = catalog.findNamespaceByName("public");
    assert(ns);
    const auto* type = catalog.findTypeByName("rank_type", ns->oid);
    assert(type && type->typtype == 'e' && type->typcategory == 'E');
    const Oid typeOid = type->oid;
    const auto catalogLabels = catalog.findEnumLabels(typeOid);
    assert(catalogLabels.size() == labels.size());
    for (size_t i = 0; i < labels.size(); ++i) assert(catalogLabels[i].enumlabel == labels[i]);
    const auto* relation = catalog.findClassByName("ranks", ns->oid);
    assert(relation && catalog.findAttribute(relation->oid, "r")->atttypid == typeOid);
    assert(g_engine.insertRow(db, "ranks", {{"id", "1"}, {"r", "zeta"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "2"}, {"r", ""}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "3"}, {"r", "alpha"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "4"}, {"r", "NULL"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", std::map<std::string, std::optional<std::string>>{{"id", "5"}, {"r", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "6"}, {"r", "it's"}}) == DBStatus::OK);
    StorageEngine::OrderBySpec byId; byId.colName = "id";
    assert((g_engine.query(db, "ranks", {"=r "}, {"id"}, {byId}) == std::vector<std::string>{"2 "}));
    assert((g_engine.query(db, "ranks", {"<r alpha"}, {"id"}, {byId}) == std::vector<std::string>{"1 ", "2 "}));
    assert(g_engine.createIndex(db, "ranks", "r") == DBStatus::OK);
    assert((g_engine.query(db, "ranks", {"=r "}, {"id"}, {byId}) == std::vector<std::string>{"2 "}));
    {
        StorageEngine reopened;
        const auto cold = reopened.getTableSchema(db, "ranks");
        assert(cold.len == 2 && cold.cols[1].dataType == "rank_type" && cold.cols[1].enumValues == labels);
        auto prepared = reopened.prepareBoundQuery(db, "SELECT id,r,r IS NULL FROM ranks ORDER BY id");
        auto plan = QueryPlanner::buildPreparedSelectPlan(&reopened, db, "ranks", std::move(prepared));
        const auto result = QueryPlanner::executePlanChecked(std::move(plan)); result.throwIfFailed();
        assert(result.structuredRowsAvailable);
        assert((result.structuredRows == std::vector<std::vector<std::string>>{
            {"1","zeta","f"},{"2","","f"},{"3","alpha","f"},{"4","NULL","f"},{"5","","t"},{"6","it's","f"}}));
        assert(result.structuredNulls.size() == 6);
        for (size_t i = 0; i < 6; ++i) assert((result.structuredNulls[i] == std::vector<bool>{false,i == 4,false}));
    }
    std::cout << "[ENUM EMPTY LABEL SCHEMA] counted declaration labels, values, NULL, catalog identity, SQL equality/index fallback and cold storage passed\n";
}
