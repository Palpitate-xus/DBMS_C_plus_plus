#include "catalog/CatalogService.h"
#include "catalog/DatumMetadataFactory.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/TypedSortSpill.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("array_domain_declaration");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    Session session;
    session.currentDB = database;
    session.username = "testuser";
    session.permission = 1;
    setCurrentSession(&session);
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE DOMAIN reference_grid AS regtype[][] "
                           "CHECK (cardinality(VALUE) >= 2)", session));
    assert(!ddl.executeSql("CREATE DOMAIN reference_child AS reference_grid", session));
    assert(!ddl.executeSql("CREATE DOMAIN short_labels AS varchar(2)[]", session));
    assert(!ddl.executeSql("CREATE TABLE items (refs reference_child, labels short_labels)", session));

    const auto catalog = g_engine.catalogService().metadataSnapshot(database);
    const auto findType = [&](const std::string& name) -> const PgTypeRow& {
        const auto found = std::find_if(catalog.types.begin(), catalog.types.end(),
            [&](const auto& type) { return type.typname == name; });
        assert(found != catalog.types.end());
        return *found;
    };
    const auto& grid = findType("reference_grid");
    const auto& child = findType("reference_child");
    const auto& labels = findType("short_labels");
    assert(grid.typtype == 'd' && grid.typbasetype == 2211 && grid.typndims == 2);
    assert(grid.typlen == -1 && !grid.typbyval && grid.typelem == INVALID_OID);
    assert(child.typbasetype == grid.oid && child.typndims == 0);
    assert(labels.typbasetype == 1015 && labels.typtypmod == 6);
    const auto relation = std::find_if(catalog.relations.begin(), catalog.relations.end(),
        [](const auto& row) { return row.relname == "items"; });
    assert(relation != catalog.relations.end());
    const auto refs = std::find_if(catalog.attributes.begin(), catalog.attributes.end(),
        [&](const auto& attr) { return attr.attrelid == relation->oid && attr.attname == "refs"; });
    assert(refs != catalog.attributes.end());
    assert(refs->atttypid == child.oid && refs->attndims == 0 && refs->atttypmod == -1);

    DatumMetadataFactory factory(catalog);
    const auto declaration = factory.builtinArrayDomain(child.oid, 2211);
    assert(declaration && declaration->kind == DatumKind::Array && declaration->actualOid == child.oid);
    assert(declaration->arrayElement && declaration->arrayElement->actualOid == 2206);
    assert(factory.attribute(*refs)->actualOid == child.oid);
    auto ambiguous = *refs;
    ambiguous.attndims = 1;
    assert(!factory.attribute(ambiguous));
    auto oldCatalog = catalog;
    auto oldGrid = std::find_if(oldCatalog.types.begin(), oldCatalog.types.end(),
        [&](const auto& type) { return type.oid == grid.oid; });
    oldGrid->typbasetype = 2206;
    DatumMetadataFactory oldFactory(oldCatalog);
    try {
        (void)oldFactory.builtinArrayDomain(child.oid, 2211);
        assert(false && "legacy scalar base was accepted as an array");
    } catch (const DbError& error) {
        assert(error.sqlState() == "XX001");
    }

    ExprEvaluator evaluator;
    evaluator.setCurrentDB(database);
    const auto evaluate = [&](const std::string& sql) {
        auto query = g_engine.prepareBoundQuery(database, sql);
        auto* select = dynamic_cast<SelectStmt*>(query.ast.get());
        assert(select && select->selectList.size() == 1);
        auto* expression = select->selectList.front().expr.get();
        evaluator.bindScalarFunctions(expression, &g_engine);
        return evaluator.eval(expression, RowContext{});
    };
    auto value = evaluate("SELECT '[0:2]={integer,NULL,-}'::reference_child");
    assert(value.value == "[0:2]={integer,NULL,-}" && !value.isNull);
    assert(value.compound && value.compound->type->actualOid == child.oid);
    assert(value.compound->type->codecOid == 2211);
    assert(value.compound->validatedArrayDomainOid == child.oid);
    assert(value.compound->dimensions.size() == 1 && value.compound->dimensions[0].lower == 0);
    assert(value.elementOids == (std::vector<std::optional<uint32_t>>{23, std::nullopt, 0}));
    auto nullValue = evaluate("SELECT NULL::reference_child");
    assert(nullValue.isNull && nullValue.compound && !nullValue.compound->arrayElements);
    assert(nullValue.compound->validatedArrayDomainOid == child.oid);
    const auto spillRoundTrip = [](const ExprValue& input) {
        sort_detail::File file;
        sort_detail::Writer writer(file.fd(), 128);
        typed_sort_detail::writeValue(writer, input);
        writer.flush();
        sort_detail::Reader reader(file.fd(), 0, writer.position(), 128);
        auto output = typed_sort_detail::readValue(reader);
        assert(reader.remaining() == 0);
        return output;
    };
    for (const auto* input : {&value, &nullValue}) {
        const auto output = spillRoundTrip(*input);
        assert(output.value == input->value && output.isNull == input->isNull);
        assert(output.elementOids == input->elementOids);
        assert(output.compound->validatedArrayDomainOid == child.oid);
        assert(output.compound->type->actualOid == child.oid);
    }
    auto invalidProof = value;
    auto invalidPayload = std::make_shared<CompoundDatum>(*invalidProof.compound);
    invalidPayload->validatedArrayDomainOid = grid.oid;
    invalidProof.compound = std::move(invalidPayload);
    try {
        (void)spillRoundTrip(invalidProof);
        assert(false && "sort codec accepted a different domain's validation");
    } catch (const DbError& error) {
        assert(error.sqlState() == "58030");
    }
    const auto expectError = [&](const std::string& sql, const std::string& state) {
        try {
            (void)evaluate(sql);
            assert(false && "expected domain conversion to fail");
        } catch (const DbError& error) {
            assert(error.sqlState() == state);
        }
    };
    expectError("SELECT '{integer}'::reference_child", "23514");
    expectError("SELECT '{abc}'::short_labels", "22001");
    assert(evaluate("SELECT '{ab,NULL}'::short_labels").value == "{ab,NULL}");
    setCurrentSession(nullptr);
    std::cout << "[ARRAY DOMAIN] declaration identity, legacy rejection and owning casts passed\n";
}
