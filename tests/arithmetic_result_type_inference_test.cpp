#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>

static void expect(const std::string& expression, const std::string& expected,
                   const std::map<std::string, std::string>& hints = {}) {
    const auto type = dbms::ExprHelper::inferResultType(expression, hints);
    if (type != expected) {
        std::cerr << expression << ": " << type << ", expected " << expected << '\n';
        assert(false);
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    expect("(16777216::REAL+1::REAL) IS NOT DISTINCT FROM 16777216::REAL", "boolean");
    expect("CAST(1 AS REAL)+1", "double precision");
    expect("CAST(NULL AS REAL)+1", "double precision");
    expect("CAST(1 AS REAL)+CAST(1 AS NUMERIC)", "double precision");
    expect("CAST(1 AS REAL)+CAST(1 AS REAL)", "real");
    expect("1::REAL+1::INT", "double precision");
    expect("CAST(1 AS REAL)^CAST(2 AS REAL)", "double precision");
    expect("1^2", "double precision");
    expect("CAST(1 AS NUMERIC)^CAST(2 AS NUMERIC)", "numeric");
    expect("CAST(1 AS SMALLINT)+CAST(2 AS SMALLINT)", "smallint");
    expect("CAST(1 AS SMALLINT)+1", "integer");
    expect("CAST(1 AS BIGINT)+1", "bigint");
    expect("CAST((CAST(1 AS REAL)+1) AS REAL)", "real");
    expect("CASE WHEN CAST(1 AS REAL)>0 THEN 1 ELSE 2 END", "integer");
    expect("CASE WHEN TRUE THEN CAST(1 AS REAL) ELSE 2 END", "real");
    expect("l+r", "double precision", {{"l", "float"}, {"r", "int4"}});
    expect("l+r", "real", {{"l", "float"}, {"r", "float"}});
    expect("l+r", "bigint", {{"l", "int8"}, {"r", "integer"}});
    for (const std::string operation : {"+", "-", "*", "/", "^"}) {
        const auto result = dbms::ExprHelper::evalStringWithNulls(
            "l"+operation+"r", {{"l", "1"}, {"r", ""}}, {"r"}, {{"l", "integer"}, {"r", "numeric"}});
        assert(result.ok && result.isNull && result.typeName == "numeric");
        expect("l"+operation+"r", "numeric", {{"l", "integer"}, {"r", "numeric"}});
    }
    std::cout << "[ARITHMETIC RESULT TYPE INFERENCE] structural roots and shared operator types passed\n";
}
