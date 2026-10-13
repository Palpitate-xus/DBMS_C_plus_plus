#include "catalog/declared_type.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "parser/query_binding.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& name) {
        assert(name == "rows");
        return QueryRelationMetadata{"public", name, {{"id", "integer"}, {"b", "boolean"}}, {}};
    };
    size_t routineDescriptions = 0, checks = 0;
    metadata.functionType = [&](const FunctionCallExpr*) { ++routineDescriptions; return "bigint"; };
    metadata.declaredType = [](const std::string& spelling) {
        const auto type = resolveDeclaredTypeName(spelling, nullptr, nullptr);
        return QueryOutputColumn{"", type.typeName, false, 0, type.typeOid};
    };
    const auto reject = [&](const std::string& sql, const std::string& expected) {
        ++checks;
        std::string state;
        try { (void)prepareQuery(sql, {}, metadata); }
        catch (const DbError& error) { state = error.sqlState(); }
        std::cout << "HAVING_BOOLEAN_NATIVE " << sql << " STATE " << state << std::endl;
        assert(state == expected);
    };
    for (const auto& input : std::vector<std::pair<std::string, std::string>>{
             {"1", "42804"}, {"NULL::INT", "42804"}, {"'notboolean'", "22P02"},
             {"'true'::TEXT", "42804"}, {"NULL::TEXT", "42804"}, {"(SELECT 1)", "42804"}}) {
        reject("SELECT count(*) FROM rows WHERE FALSE HAVING " + input.first, input.second);
        reject("SELECT id FROM rows GROUP BY id HAVING " + input.first, input.second);
    }
    ExprEvaluator evaluator;
    for (const auto& input : std::vector<std::pair<std::string, std::string>>{
             {"TRUE", "t"}, {"FALSE", "f"}, {"NULL", "null"},
             {"'t'", "t"}, {"'f'", "f"}, {"'yes'", "t"}, {"'off'", "f"}}) {
        ++checks;
        auto query = prepareQuery("SELECT 1 HAVING " + input.first, {}, metadata);
        const auto* select = dynamic_cast<const SelectStmt*>(query.ast.get());
        assert(select && select->having);
        const auto value = evaluator.eval(select->having.get(), RowContext{});
        assert(value.typeName == "boolean");
        assert(input.second == "null" ? value.isNull : !value.isNull && value.asBool() == (input.second == "t"));
    }
    ++checks;
    auto query = prepareQuery("SELECT b FROM rows GROUP BY b HAVING b", {}, metadata);
    const auto* select = dynamic_cast<const SelectStmt*>(query.ast.get());
    const auto* column = dynamic_cast<const ColumnRefExpr*>(select->having.get());
    assert(column && column->binding && column->binding->declaredType == "boolean");
    assert(checks == 20);
    std::cout << "[HAVING BOOLEAN CONTEXT] 20 pure metadata/typed input controls passed; routine metadata descriptions="
              << routineDescriptions << '\n';
}
