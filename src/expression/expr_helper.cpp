#include "expr_helper.h"
#include "ExprEvaluator.h"
#include "parser/parser.h"
#include "parser/ast.h"

#include <cctype>
#include <memory>
#include <sstream>

namespace dbms {

namespace {

std::string toLower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

bool looksLikeNumber(const std::string& s) {
    if (s.empty()) return false;
    size_t i = 0;
    if (s[0] == '+' || s[0] == '-') i = 1;
    bool hasDot = false, hasDigit = false;
    for (; i < s.size(); ++i) {
        if (s[i] >= '0' && s[i] <= '9') { hasDigit = true; continue; }
        if (s[i] == '.' && !hasDot) { hasDot = true; continue; }
        return false;
    }
    return hasDigit;
}

std::string inferType(const std::string& value) {
    if (value.empty()) return "text";
    if (looksLikeNumber(value)) {
        return value.find('.') != std::string::npos ? "double precision" : "integer";
    }
    return "text";
}

std::string canonicalTypeName(const std::string& storageType) {
    std::string t = toLower(storageType);
    if (t == "int2" || t == "int4" || t == "int8" ||
        t == "smallint" || t == "integer" || t == "bigint" ||
        t == "serial" || t == "bigserial") {
        return "integer";
    }
    if (t == "float4" || t == "real") return "real";
    if (t == "float8" || t == "double precision") return "double precision";
    if (t == "numeric" || t == "decimal") return "numeric";
    if (t == "varchar" || t == "character varying" ||
        t == "char" || t == "character" || t == "text" ||
        t == "bpchar") {
        return "text";
    }
    if (t == "bool" || t == "boolean") return "boolean";
    if (t == "timestamp" || t == "timestamptz" || t == "datetime") return "timestamp";
    if (t == "date") return "date";
    if (t == "time" || t == "timetz") return "time";
    if (t == "interval") return "interval";
    if (t == "uuid") return "uuid";
    return t;
}

} // namespace

ExprEvalResult ExprHelper::evalString(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints) {

    ExprEvalResult res;
    if (exprSql.empty()) {
        res.error = "empty expression";
        return res;
    }

    // Parse the expression by wrapping it in a SELECT.
    SQLParser parser;
    ParseResult pr = parser.parse("SELECT " + exprSql);
    if (!pr.success || !pr.stmt) {
        res.error = pr.error.empty() ? "failed to parse expression" : pr.error;
        return res;
    }

    auto* select = dynamic_cast<SelectStmt*>(pr.stmt.get());
    if (!select || select->selectList.empty() || !select->selectList[0].expr) {
        res.error = "expression did not parse to a SELECT item";
        return res;
    }

    // Build row context.
    RowContext ctx;
    for (const auto& [name, value] : row) {
        std::string typeName = inferType(value);
        auto it = typeHints.find(name);
        if (it != typeHints.end() && !it->second.empty()) {
            typeName = canonicalTypeName(it->second);
        }
        ctx.set(name, ExprValue(typeName, value, value.empty()));
    }

    ExprEvaluator evaluator;
    ExprValue v = evaluator.eval(select->selectList[0].expr.get(), ctx);

    res.ok = true;
    res.isNull = v.isNull;
    res.value = v.value;
    return res;
}

bool ExprHelper::evalBool(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    std::string* error) {

    ExprEvalResult r = evalString(exprSql, row, typeHints);
    if (!r.ok) {
        if (error) *error = r.error;
        return false;
    }
    if (r.isNull) return false;

    // ExprValue::asBool treats "t"/"true"/"1" as true; otherwise truthy.
    ExprValue tmp("boolean", r.value, false);
    return tmp.asBool();
}

} // namespace dbms
