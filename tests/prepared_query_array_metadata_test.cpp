#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "expression/prepared_query_execution.h"
#include "expression/expr_helper.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    for (const auto& alias : {"int[]", "int4[]", "INT4 []", "integer[][]"})
        assert(ExprHelper::canonicalResultTypeName(alias) == "integer[]");
    assert(ExprHelper::canonicalResultTypeName("numeric(10,2)[]") == "numeric[]");
    assert(ExprHelper::canonicalResultTypeName("int4") == "integer");
    StorageEngine owner;
    const auto database = testDbPath("prepared_query_array_metadata");
    assert(!owner.databaseExists(database));
    assert(owner.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema schema; schema.len = 1; schema.cols[0].dataName = "items";
    assert(TypeRegistry::instance().resolveColumnType(schema.cols[0], "integer", {}, true).empty());
    auto& catalog = owner.catalogService().get(database);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace);
    const auto publicOid = publicNamespace->oid;
    const auto addRelation = [&](const std::string& name, Oid type, int dimensions) {
        assert(owner.createTable(database, name, schema) == DBStatus::OK);
        PgClassRow relation; relation.relname = name;
        relation.relnamespace = publicOid; relation.relnatts = 1;
        const auto relationOid = catalog.createClass(relation);
        assert(relationOid != INVALID_OID);
        PgAttributeRow attribute; attribute.attrelid = relationOid;
        attribute.attname = "items"; attribute.attnum = 1;
        attribute.atttypid = type; attribute.attndims = dimensions;
        catalog.addAttribute(attribute);
    };
    // This project's DDL publishes the base OID plus attndims. Do not lose
    // its array marker merely because the catalog path precedes .schema.
    addRelation("dimension_array", 23, 1);
    // PostgreSQL-shaped array OIDs also retain their element declaration,
    // even if attndims is zero: the pg_type category/element is authoritative.
    PgTypeRow arrayType; arrayType.typname = "_explain_integer_array";
    arrayType.typnamespace = publicOid; arrayType.typcategory = 'A';
    arrayType.typtype = 'b'; arrayType.typelem = 23; arrayType.typlen = -1;
    const auto arrayOid = catalog.createType(arrayType);
    assert(arrayOid != INVALID_OID);
    addRelation("oid_array", arrayOid, 0);
    for (const auto& name : {"dimension_array", "oid_array"}) {
        auto prepared = std::make_shared<PreparedQuery>(owner.prepareBoundQuery(database,
            "SELECT items,array_length(items,1) FROM " + std::string(name)));
        assert(prepared->sourceRanges.size() == 1);
        const auto& range = prepared->sourceRanges.front();
        std::cerr << "ARRAY_DESCRIPTOR " << name << " columns=" << range.columns.size();
        if (!range.columns.empty()) std::cerr << " type=" << range.columns[0].type;
        std::cerr << '\n';
        assert(range.columns.size() == 1 && range.columns[0].type == "integer[]");
        auto* select = dynamic_cast<SelectStmt*>(prepared->ast.get());
        assert(select && select->selectList.size() == 2);
        const auto* column = dynamic_cast<const ColumnRefExpr*>(select->selectList[0].expr.get());
        assert(column && column->binding && column->binding->declaredType == "integer[]");
        PreparedQueryExecution execution(prepared, &owner, database);
        for (auto& target : select->selectList) execution.prepareExpression(target.expr.get());
        auto row = execution.context();
        execution.setSourceRow(row, range.ordinal, {ExprValue("integer[]", "{1,2}", false)});
        const auto value = execution.evaluate(select->selectList[0].expr.get(), row);
        assert(value.typeName == "integer[]" && value.value == "{1,2}" && !value.isNull);
        const auto length = execution.evaluate(select->selectList[1].expr.get(), row);
        assert(length.value == "2" && !length.isNull);
        bool mismatchedCellRejected = false;
        try { execution.setSourceRow(row, range.ordinal, {ExprValue("integer", "2", false)}); }
        catch (const DbError& error) { mismatchedCellRejected = error.sqlState() == "XX000"; }
        assert(mismatchedCellRejected);
    }
    assert(!owner.inTransaction());
    assert(owner.dropDatabase(database) == DBStatus::OK);
    std::cout << "[PREPARED QUERY ARRAY METADATA] passed\n";
}
