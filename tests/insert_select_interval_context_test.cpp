#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "parser/query_binding.h"
#include "utils/interval_type.h"
#include <cassert>
#include <iostream>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls = 0;
    for (const auto& [suffix, expected] : std::vector<std::pair<std::string, std::string>>{
            {"YEAR", "4 years"}, {"MONTH", "4 mons"}, {"DAY", "4 days"},
            {"HOUR", "04:00:00"}, {"MINUTE", "00:04:00"}, {"SECOND(2)", "00:00:04"},
            {"YEAR TO MONTH", "4 mons"}, {"DAY TO HOUR", "04:00:00"},
            {"DAY TO MINUTE", "00:04:00"}, {"DAY TO SECOND(2)", "00:00:04"},
            {"HOUR TO MINUTE", "00:04:00"}, {"HOUR TO SECOND(2)", "00:00:04"},
            {"MINUTE TO SECOND(2)", "00:00:04"}}) {
        const auto declaration = SQLParser::parseTypeSpecification("INTERVAL " + suffix);
        const auto modifier = interval_type_detail::modifiers(declaration.typeMods).packed();
        QueryBindingMetadata metadata;
        metadata.relation = [&](const std::string& name) {
            assert(name == "target_rows");
            QueryRelationMetadata result;
            result.schema = "public";
            result.name = name;
            result.columns = {{"id", "integer"}, {"v", "interval", false, 0, 1186, modifier}};
            return result;
        };
        for (const auto* source : {"", " FROM (SELECT 1) q", " WHERE false"}) {
            for (const bool null : {false, true}) {
                auto query = prepareQuery(std::string("INSERT INTO target_rows SELECT 1,") +
                    (null ? "NULL" : "'4'") + source, {}, metadata);
                const auto* insert = dynamic_cast<const InsertStmt*>(query.ast.get());
                assert(insert);
                const auto* select = dynamic_cast<const SelectStmt*>(insert->selectSource.get());
                assert(select && select->selectList.size() == 2);
                const auto* cast = dynamic_cast<const CastExpr*>(select->selectList[1].expr.get());
                assert(cast && cast->implicit && cast->typeName == "interval");
                assert(cast->typeMods == declaration.typeMods);
                const auto& output = query.statementOutputs.at(select).at(1);
                assert(output.type == "interval" && output.typeMod == modifier);
                ExprEvaluator evaluator;
                const auto value = evaluator.eval(cast, RowContext{});
                assert(value.isNull == null);
                if (!null) assert(value.value == expected);
                ++controls;
            }
        }
    }
    std::cout << "[INSERT SELECT INTERVAL CONTEXT] " << controls
              << " pure declaration/NULL/empty-source controls passed\n";
}
