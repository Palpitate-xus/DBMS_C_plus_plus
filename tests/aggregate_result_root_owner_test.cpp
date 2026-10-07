#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"
#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <vector>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    std::cout << std::unitbuf;
    const std::map<std::string, std::string> hints = {
        {"i", "integer"}, {"b", "bigint"}, {"f", "real"},
        {"t", "text"}, {"r", "rank_type"}};
    bool correct = true;
    for (const auto& control : std::vector<std::pair<std::string, std::string>>{
        {"sum(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN i IS NOT DISTINCT FROM 1 THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN i BETWEEN 1 AND 2 THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN t LIKE 'a%' THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN t ILIKE 'A%' THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN r IS DISTINCT FROM '' THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN i IS DISTINCT FROM 1 THEN b ELSE NULL END)", "numeric"},
        {"sum(CASE WHEN t LIKE 'a%' THEN f ELSE NULL END)", "real"},
        {"sum(CASE WHEN i IS DISTINCT FROM 1 THEN CAST(NULL AS SMALLINT) ELSE CAST(NULL AS SMALLINT) END)", "bigint"},
        {"sum(CASE WHEN i IS DISTINCT FROM 1 THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END)", "numeric"},
        {"avg(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)", "numeric"},
        {"min(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)", "integer"},
        {"max(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)", "integer"},
        {"count(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE NULL END)", "bigint"},
        {"bool_and(CASE WHEN i IS DISTINCT FROM 1 THEN true ELSE false END)", "boolean"},
        {"bool_or(CASE WHEN t LIKE 'a%' THEN true ELSE false END)", "boolean"},
        {"every(CASE WHEN t ILIKE 'A%' THEN true ELSE false END)", "boolean"},
        {"pg_catalog.sum(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)", "bigint"},
        {"\"sum\"(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN t=' is distinct from ' THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN t=' between ' THEN 1 ELSE 0 END)", "bigint"},
        {"sum(CASE WHEN t=' like ' THEN 1 ELSE 0 END)", "bigint"},
        {"sum(i) FILTER(WHERE t LIKE 'a%')", "bigint"},
        {"sum(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END) + CAST(0 AS BIGINT)", "bigint"},
        {"sum(CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END)>1", "boolean"},
        {"CASE WHEN i IS DISTINCT FROM 1 THEN 1 ELSE 0 END", "integer"},
        {"i IS DISTINCT FROM 1", "boolean"},
        {"i IS NOT DISTINCT FROM 1", "boolean"},
        {"i BETWEEN 1 AND 2", "boolean"},
        {"t LIKE 'a%'", "boolean"},
        {"t ILIKE 'A%'", "boolean"},
        {"sum(CAST(NULL AS REAL))", "real"},
        {"sum(CAST(NULL AS MONEY))", "money"},
        {"avg(CAST(NULL AS INTERVAL))", "interval"},
    }) {
        dbms::SQLParser parser;
        auto parsed = parser.parse("SELECT " + control.first);
        const auto* select = parsed.success
            ? dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get()) : nullptr;
        assert(select && select->selectList.size() == 1);
        const auto actual = dbms::ExprHelper::inferResultType(control.first, hints);
        std::cout << "AGGREGATE_RESULT_ROOT " << control.first << " type=" << actual
                  << " expected=" << control.second << '\n';
        correct = correct && actual == control.second;
    }
    assert(correct);
    std::cout << "[AGGREGATE RESULT ROOT OWNER] actual outer aggregate, predicate, cast, NULL and input overload controls passed\n";
}
