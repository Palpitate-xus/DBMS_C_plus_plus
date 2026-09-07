#include "Config.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>

dbms::Config g_config;

int main() {
    const std::map<std::string, std::string> typeHints = {
        {"enabled", "boolean"},
    };
    std::string error;

    assert(dbms::ExprHelper::evalBool(
        "enabled = true", {{"enabled", "true"}}, typeHints, &error));
    assert(error.empty());
    assert(dbms::ExprHelper::evalBool(
        "enabled = true", {{"enabled", "t"}}, typeHints, &error));
    assert(!dbms::ExprHelper::evalBool(
        "enabled = true", {{"enabled", "false"}}, typeHints, &error));
    assert(dbms::ExprHelper::evalBool(
        "enabled = false", {{"enabled", "f"}}, typeHints, &error));
    assert(dbms::ExprHelper::evalBool(
        "enabled <> false", {{"enabled", "true"}}, typeHints, &error));
    assert(dbms::ExprHelper::evalBool(
        "enabled > false", {{"enabled", "true"}}, typeHints, &error));
    assert(error.empty());

    auto result = dbms::ExprHelper::evalString("NULL IS UNKNOWN", {});
    assert(result.ok && !result.isNull && result.value == "t");
    result = dbms::ExprHelper::evalString("TRUE IS UNKNOWN", {});
    assert(result.ok && !result.isNull && result.value == "f");
    result = dbms::ExprHelper::evalString("NULL IS NOT UNKNOWN", {});
    assert(result.ok && !result.isNull && result.value == "f");
    result = dbms::ExprHelper::evalString("FALSE IS NOT UNKNOWN", {});
    assert(result.ok && !result.isNull && result.value == "t");

    std::cout << "[BOOLEAN EXPRESSION] all passed\n";
    return 0;
}
