#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("between_result_type");
    if (g_engine.createDatabase(database, "utf8") != DBStatus::OK) return 2;
    size_t controls = 0, failures = 0;
    const auto require = [&](bool valid, const std::string& label) {
        ++controls; if (!valid) {++failures; std::cerr << "[BETWEEN TYPE FAIL] " << label << '\n';}
    };
    for (const auto& type : {"smallint", "integer", "bigint", "text", "varbit", "bit"})
        for (const auto& operation : {" BETWEEN ", " NOT BETWEEN "})
            for (const auto& prefix : {"NULL", "'1'"}) {
                const auto expression = std::string(prefix) + operation + "NULL AND NULL::" + type;
                require(ExprHelper::inferResultType(expression) == "boolean", "metadata-only whole grammar root " + expression);
                auto parsed = SQLParser().parse("SELECT " + expression);
                const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
                require(select && select->selectList.size() == 1 && ExprHelper::inferParsedResultType(select->selectList[0].expr.get()) == "boolean",
                    "actual parser-owned grammar type " + expression);
            }
    require(ExprHelper::inferResultType("1::smallint") == "smallint", "standalone genuine postfix cast remains smallint");
    require(ExprHelper::inferResultType("1::bigint") == "bigint", "standalone genuine postfix cast remains bigint");
    require(ExprHelper::inferResultType("(1 BETWEEN 0 AND 2)::text") == "text", "genuine outer cast owns its descriptor");
    require(ExprHelper::inferResultType("(1 NOT BETWEEN 0 AND 2)::integer") == "integer", "genuine outer integer cast keeps output type");
    for (const auto& target : {"text", "varchar", "char", "smallint", "bigint"}) {
        const auto expression = std::string("CAST((1 BETWEEN 0 AND 2) AS ") + target + ")";
        require(ExprHelper::canonicalResultTypeName(ExprHelper::inferResultType(expression)) == ExprHelper::canonicalResultTypeName(target),
            "actual outer CAST owns its result, not nested range text " + expression);
    }
    if (g_engine.createSchema(database, "type_roles") != DBStatus::OK) return 2;
    for (const auto& name : {"between", "not between"}) {
        if (g_engine.createUDF(database, name, {"a", "b", "c"}, {"integer", "integer", "integer"},
            "a", 'i', "sql", "integer", false, false, "type_roles") != DBStatus::OK) return 2;
        for (const auto& qualifier : {"type_roles.", "\"type_roles\"."}) {
            const auto expression = std::string(qualifier) + '"' + name + "\"(1,2,3)";
            require(ExprHelper::inferResultType(expression, {}, database, &g_engine) == "integer",
                "actual quoted/schema-qualified stored routine owns result metadata " + expression);
        }
    }
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "[BETWEEN RESULT TYPE] all " << controls << " parser-owned predicate/standalone cast/outer cast/real routine controls; failures=" << failures << '\n';
    return failures ? 1 : 0;
}
