#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <set>
#include <string>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::map<std::string, std::string> canonical = {
        {"int", "integer"}, {"int2", "smallint"}, {"smallint", "smallint"},
        {"smallserial", "smallint"}, {"int4", "integer"}, {"integer", "integer"},
        {"serial", "integer"}, {"int8", "bigint"}, {"bigint", "bigint"},
        {"bigserial", "bigint"}, {"float", "real"}, {"float4", "real"},
        {"real", "real"}, {"double", "double precision"},
        {"float8", "double precision"}, {"double precision", "double precision"},
    };
    for (const auto& [source, expected] : canonical) {
        const std::string value = expected == "bigint" ? "9223372036854775807" : "1";
        const std::map<std::string, std::string> hints{{"v", source}};
        auto result = dbms::ExprHelper::evalStringWithNulls("v", {{"v", value}}, {}, hints);
        if (!result.ok || result.isNull || result.value != value || result.typeName != expected) {
            std::cerr << source << " -> " << result.typeName << ": " << result.error << '\n';
            assert(false);
        }
        result = dbms::ExprHelper::evalStringWithNulls("v", {{"v", ""}}, {"v"}, hints);
        assert(result.ok && result.isNull && result.typeName == expected);
    }
    const auto empty = dbms::ExprHelper::evalStringWithNulls("v", {{"v", ""}}, {}, {{"v", "text"}});
    assert(empty.ok && !empty.isNull && empty.value.empty() && empty.typeName == "text");
    std::cout << "[EXPRESSION STORAGE TYPE IDENTITY] widths, aliases, NULL and empty text passed\n";
}
