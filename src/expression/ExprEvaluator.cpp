#include "ExprEvaluator.h"
#include "common/DateType.h"
#include "types/numeric.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace dbms {

// ============================================================================
// ExprValue helpers
// ============================================================================

static std::string toLower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

static bool isNumericTypeName(const std::string& s) {
    std::string t = toLower(s);
    return t == "numeric" || t == "decimal";
}

static std::optional<Numeric> tryParseNumeric(const std::string& s) {
    try {
        return Numeric(s);
    } catch (...) {
        return std::nullopt;
    }
}

bool ExprValue::asBool() const {
    if (isNull) return false;
    std::string v = toLower(value);
    if (v == "t" || v == "true" || v == "1" || v == "yes" || v == "on") return true;
    if (v == "f" || v == "false" || v == "0" || v == "no" || v == "off") return false;
    // Non-empty non-zero string/value is truthy
    return !value.empty() && value != "0" && value != "0.0";
}

int64_t ExprValue::asInt() const {
    if (isNull || value.empty()) return 0;
    try {
        size_t pos = 0;
        return std::stoll(value, &pos);
    } catch (...) {
        return 0;
    }
}

double ExprValue::asDouble() const {
    if (isNull || value.empty()) return 0.0;
    try {
        return std::stod(value);
    } catch (...) {
        return 0.0;
    }
}

// ============================================================================
// RowContext
// ============================================================================

std::string RowContext::normalize(const std::string& s) {
    return toLower(s);
}

std::optional<ExprValue> RowContext::get(const std::string& name) const {
    auto it = values_.find(normalize(name));
    if (it != values_.end()) return it->second;
    return std::nullopt;
}

// ============================================================================
// ExprEvaluator
// ============================================================================

ExprEvaluator::ExprEvaluator() {
    registerBuiltins();
}

ExprValue ExprEvaluator::eval(const Expr* expr, const RowContext& ctx) const {
    if (!expr) return ExprValue{};
    switch (expr->type) {
        case ExprType::Literal:      return evalLiteral(static_cast<const LiteralExpr*>(expr));
        case ExprType::ColumnRef:    return evalColumnRef(static_cast<const ColumnRefExpr*>(expr), ctx);
        case ExprType::UnaryOp:      return evalUnaryOp(static_cast<const UnaryOpExpr*>(expr), ctx);
        case ExprType::BinaryOp:     return evalBinaryOp(static_cast<const BinaryOpExpr*>(expr), ctx);
        case ExprType::FunctionCall: return evalFunctionCall(static_cast<const FunctionCallExpr*>(expr), ctx);
        case ExprType::CaseExpr:     return evalCase(static_cast<const CaseExpr*>(expr), ctx);
        case ExprType::CastExpr:     return evalCast(static_cast<const CastExpr*>(expr), ctx);
        case ExprType::ArrayExpr:    return evalArrayExpr(static_cast<const ArrayExpr*>(expr), ctx);
        case ExprType::RowExpr:      return evalRowExpr(static_cast<const RowExpr*>(expr), ctx);
        case ExprType::Subquery:     return ExprValue{}; // not supported in Wave 0
        case ExprType::Parameter:    return ExprValue{};
        case ExprType::A_Star:       return ExprValue{};
    }
    return ExprValue{};
}

// ----------------------------------------------------------------------------
// Literals
// ----------------------------------------------------------------------------

static bool isQuotedString(const std::string& s) {
    return s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                             (s.front() == '"' && s.back() == '"'));
}

static std::string unquote(const std::string& s) {
    if (isQuotedString(s)) return s.substr(1, s.size() - 2);
    return s;
}

static bool isNumericLiteral(const std::string& s) {
    if (s.empty()) return false;
    size_t i = 0;
    if (s[0] == '+' || s[0] == '-') i = 1;
    bool hasDigit = false, hasDot = false;
    for (; i < s.size(); ++i) {
        if (s[i] >= '0' && s[i] <= '9') { hasDigit = true; continue; }
        if (s[i] == '.') {
            if (hasDot) return false;
            hasDot = true;
            continue;
        }
        return false;
    }
    return hasDigit;
}

ExprValue ExprEvaluator::evalLiteral(const LiteralExpr* e) const {
    if (!e) return ExprValue{};
    const std::string& raw = e->value;
    std::string low = toLower(raw);

    if (low == "null") return ExprValue("unknown", "", true);
    if (low == "true") return ExprValue("boolean", "t", false);
    if (low == "false") return ExprValue("boolean", "f", false);

    if (!e->typeName.empty()) {
        return ExprValue(e->typeName, unquote(raw), false);
    }

    if (isQuotedString(raw)) {
        return ExprValue("character varying", unquote(raw), false);
    }

    if (isNumericLiteral(raw)) {
        if (raw.find('.') != std::string::npos) return ExprValue("double precision", raw, false);
        return ExprValue("integer", raw, false);
    }

    return ExprValue("character varying", raw, false);
}

// ----------------------------------------------------------------------------
// Column references
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalColumnRef(const ColumnRefExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    std::string name = e->column;
    auto v = ctx.get(name);
    if (v) return *v;
    // Try table.column form
    if (!e->table.empty()) {
        v = ctx.get(e->table + "." + e->column);
        if (v) return *v;
    }
    return ExprValue("unknown", "", true);
}

// ----------------------------------------------------------------------------
// Unary operators
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalUnaryOp(const UnaryOpExpr* e, const RowContext& ctx) const {
    if (!e || !e->operand) return ExprValue{};
    ExprValue v = eval(e->operand.get(), ctx);
    std::string op = toLower(e->op);

    if (op == "+") return v;
    if (op == "-") {
        if (v.isNull) return v;
        if (v.value.empty()) return ExprValue(v.typeName, "0", false);
        if (isNumericTypeName(v.typeName)) {
            auto n = tryParseNumeric(v.value);
            if (n) return ExprValue("numeric", (-(*n)).toString(), false);
        }
        if (v.value[0] == '-') return ExprValue(v.typeName, v.value.substr(1), false);
        return ExprValue(v.typeName, "-" + v.value, false);
    }
    if (op == "not") {
        return ExprValue("boolean", v.asBool() ? "f" : "t", false);
    }
    if (op == "is null") {
        return ExprValue("boolean", v.isNull ? "t" : "f", false);
    }
    if (op == "is not null") {
        return ExprValue("boolean", v.isNull ? "f" : "t", false);
    }
    if (op.find("is true") != std::string::npos) {
        bool r = !v.isNull && v.asBool();
        if (op.find("not") != std::string::npos) r = !r;
        return ExprValue("boolean", r ? "t" : "f", false);
    }
    if (op.find("is false") != std::string::npos) {
        bool r = !v.isNull && !v.asBool();
        if (op.find("not") != std::string::npos) r = !r;
        return ExprValue("boolean", r ? "t" : "f", false);
    }

    return ExprValue{};
}

// ----------------------------------------------------------------------------
// Value comparison
// ----------------------------------------------------------------------------

static bool looksLikeNumber(const std::string& s) {
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

int ExprEvaluator::compareValues(const ExprValue& a, const ExprValue& b) {
    if (a.isNull || b.isNull) return 0; // caller handles NULL

    std::string ta = toLower(a.typeName);
    std::string tb = toLower(b.typeName);

    // Exact numeric comparison for explicit numeric/decimal types.
    if (isNumericTypeName(a.typeName) || isNumericTypeName(b.typeName)) {
        auto na = tryParseNumeric(a.value);
        auto nb = tryParseNumeric(b.value);
        if (na && nb) {
            return (*na > *nb) - (*na < *nb);
        }
    }

    // Numeric comparison
    bool numA = looksLikeNumber(a.value);
    bool numB = looksLikeNumber(b.value);
    if (numA && numB) {
        bool floatA = a.value.find('.') != std::string::npos ||
                      ta == "double precision" || ta == "real" || ta == "numeric";
        bool floatB = b.value.find('.') != std::string::npos ||
                      tb == "double precision" || tb == "real" || tb == "numeric";
        if (floatA || floatB) {
            double da = a.asDouble(), db = b.asDouble();
            return (da > db) - (da < db);
        }
        int64_t ia = a.asInt(), ib = b.asInt();
        return (ia > ib) - (ia < ib);
    }

    // Date/timestamp comparison
    bool dateA = (ta == "date" || ta == "timestamp" || ta == "timestamptz");
    bool dateB = (tb == "date" || tb == "timestamp" || tb == "timestamptz");
    if (dateA || dateB) {
        if (ta == "date" && tb == "date") {
            Date da(a.value.c_str()), db(b.value.c_str());
            return (da > db) - (da < db);
        }
        int64_t sa = parseTimestampToSeconds(a.value);
        int64_t sb = parseTimestampToSeconds(b.value);
        return (sa > sb) - (sa < sb);
    }

    // Default string comparison
    return a.value < b.value ? -1 : (a.value > b.value ? 1 : 0);
}

ExprValue ExprEvaluator::applyComparison(const std::string& op,
                                         const ExprValue& l,
                                         const ExprValue& r) {
    if (l.isNull || r.isNull) return ExprValue("boolean", "", true);

    std::string cmp = op;
    if (cmp == "!=") cmp = "<>";

    int c = compareValues(l, r);
    bool result = false;
    if (cmp == "=")  result = c == 0;
    else if (cmp == "<>") result = c != 0;
    else if (cmp == "<")  result = c < 0;
    else if (cmp == ">")  result = c > 0;
    else if (cmp == "<=") result = c <= 0;
    else if (cmp == ">=") result = c >= 0;

    return ExprValue("boolean", result ? "t" : "f", false);
}

// ----------------------------------------------------------------------------
// Arithmetic
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::applyArithmetic(const std::string& op,
                                         const ExprValue& l,
                                         const ExprValue& r) {
    if (l.isNull || r.isNull) return ExprValue(l.typeName, "", true);

    // Exact arithmetic for explicit numeric/decimal types.
    if (isNumericTypeName(l.typeName) || isNumericTypeName(r.typeName)) {
        auto nl = tryParseNumeric(l.value);
        auto nr = tryParseNumeric(r.value);
        if (nl && nr) {
            Numeric res;
            if (op == "+") res = *nl + *nr;
            else if (op == "-") res = *nl - *nr;
            else if (op == "*") res = *nl * *nr;
            else if (op == "/") res = *nl / *nr;
            else return ExprValue("numeric", "", true);
            return ExprValue("numeric", res.toString(), false);
        }
    }

    bool floatResult = l.value.find('.') != std::string::npos ||
                       r.value.find('.') != std::string::npos ||
                       toLower(l.typeName) == "double precision" ||
                       toLower(l.typeName) == "real" ||
                       toLower(l.typeName) == "numeric";

    if (floatResult) {
        double a = l.asDouble(), b = r.asDouble(), res = 0;
        if (op == "+") res = a + b;
        else if (op == "-") res = a - b;
        else if (op == "*") res = a * b;
        else if (op == "/") res = (b == 0) ? 0 : a / b;
        else if (op == "%") res = (b == 0) ? 0 : std::fmod(a, b);
        else if (op == "^") res = std::pow(a, b);
        std::ostringstream oss;
        oss << res;
        return ExprValue("double precision", oss.str(), false);
    }

    int64_t a = l.asInt(), b = r.asInt(), res = 0;
    if (op == "+") res = a + b;
    else if (op == "-") res = a - b;
    else if (op == "*") res = a * b;
    else if (op == "/") res = (b == 0) ? 0 : a / b;
    else if (op == "%") res = (b == 0) ? 0 : a % b;
    else if (op == "^") res = static_cast<int64_t>(std::pow(static_cast<double>(a), static_cast<double>(b)));
    return ExprValue("integer", std::to_string(res), false);
}

// ----------------------------------------------------------------------------
// LIKE / SIMILAR TO
// ----------------------------------------------------------------------------

bool ExprEvaluator::likeMatch(const std::string& text, const std::string& pattern) {
    size_t ti = 0, pi = 0, star = std::string::npos, match = 0;
    while (ti < text.size()) {
        if (pi < pattern.size() && (pattern[pi] == '_' || pattern[pi] == text[ti])) {
            ++ti; ++pi;
        } else if (pi < pattern.size() && pattern[pi] == '%') {
            star = pi++;
            match = ti;
        } else if (star != std::string::npos) {
            pi = star + 1;
            ti = ++match;
        } else {
            return false;
        }
    }
    while (pi < pattern.size() && pattern[pi] == '%') ++pi;
    return pi == pattern.size();
}

bool ExprEvaluator::similarToMatch(const std::string& text, const std::string& pattern) {
    // SIMILAR TO is LIKE with regex-ish additions; approximate with LIKE for Wave 0
    return likeMatch(text, pattern);
}

// ----------------------------------------------------------------------------
// Binary operators
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalBinaryOp(const BinaryOpExpr* e, const RowContext& ctx) const {
    if (!e || !e->left || !e->right) return ExprValue{};
    std::string op = toLower(e->op);

    // Logical short-circuit
    if (op == "and") {
        ExprValue l = eval(e->left.get(), ctx);
        if (!l.asBool()) return ExprValue("boolean", "f", false);
        ExprValue r = eval(e->right.get(), ctx);
        return ExprValue("boolean", r.asBool() ? "t" : "f", false);
    }
    if (op == "or") {
        ExprValue l = eval(e->left.get(), ctx);
        if (l.asBool()) return ExprValue("boolean", "t", false);
        ExprValue r = eval(e->right.get(), ctx);
        return ExprValue("boolean", r.asBool() ? "t" : "f", false);
    }

    ExprValue l = eval(e->left.get(), ctx);
    ExprValue r = eval(e->right.get(), ctx);

    // Comparison
    static const std::set<std::string> cmpOps = {"=", "<>", "!=", "<", ">", "<=", ">="};
    if (cmpOps.count(op)) return applyComparison(op, l, r);

    // Arithmetic
    static const std::set<std::string> arithOps = {"+", "-", "*", "/", "%", "^"};
    if (arithOps.count(op)) return applyArithmetic(op, l, r);

    // String concatenation
    if (op == "||") {
        if (l.isNull || r.isNull) return ExprValue("text", "", true);
        return ExprValue("text", l.value + r.value, false);
    }

    // LIKE / ILIKE / SIMILAR TO
    if (op == "like" || op == "not like") {
        bool m = !l.isNull && !r.isNull && likeMatch(l.value, r.value);
        if (op == "not like") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }
    if (op == "ilike" || op == "not ilike") {
        std::string lt = toLower(l.value), rt = toLower(r.value);
        bool m = !l.isNull && !r.isNull && likeMatch(lt, rt);
        if (op == "not ilike") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }
    if (op == "similar to" || op == "not similar to") {
        bool m = !l.isNull && !r.isNull && similarToMatch(l.value, r.value);
        if (op == "not similar to") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }

    // IN list
    if (op == "in") {
        if (l.isNull) return ExprValue("boolean", "", true);
        // Parser currently stores IN list as raw literal text; split on space
        std::string listText = r.value;
        std::istringstream iss(listText);
        std::string tok;
        while (iss >> tok) {
            if (compareValues(l, ExprValue("character varying", unquote(tok), false)) == 0)
                return ExprValue("boolean", "t", false);
        }
        return ExprValue("boolean", "f", false);
    }

    // Cast (::)
    if (op == "::") {
        return evalCast(nullptr, ctx, l, r.value);
    }

    // Array subscript (expr[idx])
    if (op == "[]") {
        // Wave 0: not supported
        return ExprValue("unknown", "", true);
    }

    return ExprValue{};
}

// ----------------------------------------------------------------------------
// CASE expressions
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalCase(const CaseExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    for (const auto& wc : e->whenClauses) {
        bool match = false;
        if (e->switchExpr) {
            ExprValue sw = eval(e->switchExpr.get(), ctx);
            ExprValue cond = eval(wc.first.get(), ctx);
            match = compareValues(sw, cond) == 0;
        } else {
            match = eval(wc.first.get(), ctx).asBool();
        }
        if (match) return eval(wc.second.get(), ctx);
    }
    if (e->elseExpr) return eval(e->elseExpr.get(), ctx);
    return ExprValue("unknown", "", true);
}

// ----------------------------------------------------------------------------
// CAST
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalCast(const CastExpr* e, const RowContext& ctx) const {
    if (e) {
        ExprValue v = eval(e->operand.get(), ctx);
        return evalCast(nullptr, ctx, v, e->typeName);
    }
    return ExprValue{};
}

// Helper overload used by BinaryOpExpr "::"
ExprValue ExprEvaluator::evalCast(const Expr*, const RowContext&,
                                  const ExprValue& v, const std::string& targetTypeName) const {
    if (v.isNull) return ExprValue(targetTypeName, "", true);
    std::string target = toLower(targetTypeName);

    if (target == "boolean" || target == "bool") {
        return ExprValue("boolean", v.asBool() ? "t" : "f", false);
    }
    if (target == "integer" || target == "int" || target == "int4") {
        return ExprValue("integer", std::to_string(v.asInt()), false);
    }
    if (target == "bigint" || target == "int8") {
        return ExprValue("bigint", std::to_string(v.asInt()), false);
    }
    if (target == "smallint" || target == "int2") {
        return ExprValue("smallint", std::to_string(static_cast<int16_t>(v.asInt())), false);
    }
    if (target == "real" || target == "float4") {
        std::ostringstream oss; oss << static_cast<float>(v.asDouble());
        return ExprValue("real", oss.str(), false);
    }
    if (target == "double precision" || target == "float8") {
        std::ostringstream oss; oss << v.asDouble();
        return ExprValue("double precision", oss.str(), false);
    }
    if (target == "numeric" || target == "decimal") {
        auto n = tryParseNumeric(v.value);
        if (n) return ExprValue("numeric", n->toString(), false);
        return ExprValue("numeric", "", true);
    }
    if (target == "text" || target.find("char") != std::string::npos || target == "varchar") {
        return ExprValue(targetTypeName, v.value, false);
    }
    if (target == "date") {
        Date d(v.value.c_str());
        if (d.year == 0) return ExprValue("date", "", true);
        return ExprValue("date", str(d), false);
    }
    if (target == "timestamp" || target == "timestamptz") {
        int64_t ts = parseTimestampToSeconds(v.value);
        if (ts == 0 && v.value != "1970-01-01 00:00:00")
            return ExprValue(target, "", true);
        return ExprValue(target, formatTimestampSeconds(ts), false);
    }

    // Default passthrough
    return ExprValue(targetTypeName, v.value, false);
}

// ----------------------------------------------------------------------------
// Function calls
// ----------------------------------------------------------------------------

void ExprEvaluator::registerFunction(const std::string& name, ScalarFunction fn) {
    functions_[toLower(name)] = std::move(fn);
}

bool ExprEvaluator::hasFunction(const std::string& name) const {
    return functions_.find(toLower(name)) != functions_.end();
}

ExprValue ExprEvaluator::evalFunctionCall(const FunctionCallExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    std::string name = toLower(e->funcName);

    std::vector<ExprValue> args;
    for (const auto& a : e->args) args.push_back(eval(a.get(), ctx));

    auto it = functions_.find(name);
    if (it != functions_.end()) return it->second(args);

    // Built-in fallback for common functions even if not registered
    if (name == "coalesce") {
        for (const auto& a : args) if (!a.isNull) return a;
        return ExprValue("unknown", "", true);
    }
    if (name == "nullif") {
        if (args.size() < 2) return ExprValue("unknown", "", true);
        if (args[0].isNull || args[1].isNull) return args[0];
        return compareValues(args[0], args[1]) == 0
                   ? ExprValue(args[0].typeName, "", true)
                   : args[0];
    }
    if (name == "greatest") {
        ExprValue best("unknown", "", true);
        for (const auto& a : args) {
            if (a.isNull) continue;
            if (best.isNull || compareValues(a, best) > 0) best = a;
        }
        return best;
    }
    if (name == "least") {
        ExprValue best("unknown", "", true);
        for (const auto& a : args) {
            if (a.isNull) continue;
            if (best.isNull || compareValues(a, best) < 0) best = a;
        }
        return best;
    }
    if (name == "between") {
        if (args.size() != 3) return ExprValue("boolean", "f", false);
        ExprValue v = args[0], lo = args[1], hi = args[2];
        bool r = !v.isNull && !lo.isNull && !hi.isNull &&
                 compareValues(v, lo) >= 0 && compareValues(v, hi) <= 0;
        return ExprValue("boolean", r ? "t" : "f", false);
    }

    return ExprValue{};
}

// ----------------------------------------------------------------------------
// Array / Row expressions
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalArrayExpr(const ArrayExpr*, const RowContext&) const {
    return ExprValue("unknown", "", true);
}

ExprValue ExprEvaluator::evalRowExpr(const RowExpr*, const RowContext&) const {
    return ExprValue("unknown", "", true);
}

// ----------------------------------------------------------------------------
// Built-in scalar functions
// ----------------------------------------------------------------------------

void ExprEvaluator::registerBuiltins() {
    functions_["abs"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (isNumericTypeName(a[0].typeName)) {
            auto n = tryParseNumeric(a[0].value);
            if (n) return ExprValue("numeric", (n->sign() < 0 ? -(*n) : *n).toString(), false);
        }
        double v = std::abs(a[0].asDouble());
        return ExprValue("numeric", std::to_string(v), false);
    };
    functions_["length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(a[0].value.size()), false);
    };
    functions_["lower"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        return ExprValue("text", toLower(a[0].value), false);
    };
    functions_["upper"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string s = a[0].value;
        for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        return ExprValue("text", s, false);
    };
    functions_["substring"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string s = a[0].value;
        if (a.size() < 2) return ExprValue("text", s, false);
        size_t from = static_cast<size_t>(a[1].asInt());
        if (from > 0) --from;
        size_t len = (a.size() >= 3) ? static_cast<size_t>(a[2].asInt()) : std::string::npos;
        if (from >= s.size()) return ExprValue("text", "", false);
        return ExprValue("text", s.substr(from, len), false);
    };
    functions_["round"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (isNumericTypeName(a[0].typeName)) {
            auto n = tryParseNumeric(a[0].value);
            if (n) {
                int p = (a.size() >= 2) ? static_cast<int>(a[1].asInt()) : 0;
                return ExprValue("numeric", n->withScale(p).toString(), false);
            }
        }
        double v = a[0].asDouble();
        if (a.size() >= 2) {
            int p = static_cast<int>(a[1].asInt());
            double mult = std::pow(10.0, p);
            v = std::round(v * mult) / mult;
        } else {
            v = std::round(v);
        }
        return ExprValue("numeric", std::to_string(v), false);
    };
    functions_["now"] = [](const std::vector<ExprValue>&) {
        // Return current timestamp as string; Wave 0 uses a fixed reference
        return ExprValue("timestamp", "2026-06-20 12:00:00", false);
    };

    // ------------------------------------------------------------------------
    // Math functions
    // ------------------------------------------------------------------------
    auto unaryMath = [](const std::vector<ExprValue>& a, double (*fn)(double),
                        const std::string& outType = "double precision") {
        if (a.empty() || a[0].isNull) return ExprValue(outType, "", true);
        return ExprValue(outType, std::to_string(fn(a[0].asDouble())), false);
    };
    functions_["sin"]   = [&](const auto& a) { return unaryMath(a, std::sin); };
    functions_["cos"]   = [&](const auto& a) { return unaryMath(a, std::cos); };
    functions_["tan"]   = [&](const auto& a) { return unaryMath(a, std::tan); };
    functions_["asin"]  = [&](const auto& a) { return unaryMath(a, std::asin); };
    functions_["acos"]  = [&](const auto& a) { return unaryMath(a, std::acos); };
    functions_["atan"]  = [&](const auto& a) { return unaryMath(a, std::atan); };
    functions_["exp"]   = [&](const auto& a) { return unaryMath(a, std::exp); };
    functions_["ln"]    = [&](const auto& a) { return unaryMath(a, std::log); };
    functions_["log"]   = [&](const auto& a) { return unaryMath(a, std::log10); };
    functions_["sqrt"]  = [&](const auto& a) { return unaryMath(a, std::sqrt); };
    functions_["cbrt"]  = [&](const auto& a) { return unaryMath(a, std::cbrt); };
    functions_["ceil"]  = [&](const auto& a) { return unaryMath(a, std::ceil); };
    functions_["floor"] = [&](const auto& a) { return unaryMath(a, std::floor); };
    functions_["trunc"] = [&](const auto& a) { return unaryMath(a, std::trunc); };

    functions_["atan2"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         std::to_string(std::atan2(a[0].asDouble(), a[1].asDouble())), false);
    };
    functions_["power"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         std::to_string(std::pow(a[0].asDouble(), a[1].asDouble())), false);
    };
    functions_["mod"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("integer", "", true);
        int64_t b = a[1].asInt();
        if (b == 0) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(a[0].asInt() % b), false);
    };
    functions_["sign"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        double v = a[0].asDouble();
        return ExprValue("integer", std::to_string(v > 0 ? 1 : (v < 0 ? -1 : 0)), false);
    };
    functions_["pi"] = [](const std::vector<ExprValue>&) {
        return ExprValue("double precision", std::to_string(std::atan(1.0) * 4.0), false);
    };
    functions_["random"] = [](const std::vector<ExprValue>&) {
        return ExprValue("double precision", std::to_string(static_cast<double>(std::rand()) / RAND_MAX), false);
    };

    // ------------------------------------------------------------------------
    // String functions
    // ------------------------------------------------------------------------
    functions_["concat"] = [](const std::vector<ExprValue>& a) {
        std::string s;
        for (const auto& v : a) {
            if (v.isNull) return ExprValue("text", "", true);
            s += v.value;
        }
        return ExprValue("text", s, false);
    };
    functions_["trim"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        std::string chars = (a.size() >= 2 && !a[1].isNull) ? a[1].value : " \t\n\r\f\v";
        size_t b = 0, e = s.size();
        while (b < e && chars.find(s[b]) != std::string::npos) ++b;
        while (e > b && chars.find(s[e - 1]) != std::string::npos) --e;
        return ExprValue("text", s.substr(b, e - b), false);
    };
    functions_["ltrim"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        std::string chars = (a.size() >= 2 && !a[1].isNull) ? a[1].value : " \t\n\r\f\v";
        size_t b = 0;
        while (b < s.size() && chars.find(s[b]) != std::string::npos) ++b;
        return ExprValue("text", s.substr(b), false);
    };
    functions_["rtrim"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        std::string chars = (a.size() >= 2 && !a[1].isNull) ? a[1].value : " \t\n\r\f\v";
        if (s.empty()) return ExprValue("text", s, false);
        size_t e = s.size();
        while (e > 0 && chars.find(s[e - 1]) != std::string::npos) --e;
        return ExprValue("text", s.substr(0, e), false);
    };
    functions_["replace"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        std::string s = a[0].value;
        const std::string& from = a[1].value;
        const std::string& to = a[2].value;
        if (from.empty()) return ExprValue("text", s, false);
        size_t pos = 0;
        while ((pos = s.find(from, pos)) != std::string::npos) {
            s.replace(pos, from.size(), to);
            pos += to.size();
        }
        return ExprValue("text", s, false);
    };
    functions_["position"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("integer", "", true);
        size_t pos = a[1].value.find(a[0].value);
        return ExprValue("integer", std::to_string(pos == std::string::npos ? 0 : static_cast<int64_t>(pos + 1)), false);
    };
    functions_["left"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        int64_t n = a[1].asInt();
        if (n <= 0) return ExprValue("text", "", false);
        return ExprValue("text", a[0].value.substr(0, static_cast<size_t>(n)), false);
    };
    functions_["right"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        int64_t n = a[1].asInt();
        const std::string& s = a[0].value;
        if (n <= 0) return ExprValue("text", "", false);
        size_t start = static_cast<size_t>(n) > s.size() ? 0 : s.size() - static_cast<size_t>(n);
        return ExprValue("text", s.substr(start), false);
    };
    functions_["repeat"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        int64_t n = a[1].asInt();
        if (n <= 0) return ExprValue("text", "", false);
        std::string s;
        s.reserve(a[0].value.size() * static_cast<size_t>(n));
        for (int64_t i = 0; i < n; ++i) s += a[0].value;
        return ExprValue("text", s, false);
    };
    functions_["reverse"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string s = a[0].value;
        std::reverse(s.begin(), s.end());
        return ExprValue("text", s, false);
    };
    functions_["ascii"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull || a[0].value.empty()) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(static_cast<int>(static_cast<unsigned char>(a[0].value[0]))), false);
    };
    functions_["chr"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        int v = static_cast<int>(a[0].asInt());
        if (v < 0 || v > 255) return ExprValue("text", "", true);
        return ExprValue("text", std::string(1, static_cast<char>(v)), false);
    };
    // substr — PostgreSQL alias of substring(str, from[, len])
    functions_["substr"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string s = a[0].value;
        if (a.size() < 2) return ExprValue("text", s, false);
        int64_t from = a[1].asInt();
        int64_t len = (a.size() >= 3) ? a[2].asInt() : -1;
        // PG semantics: 1-based; clamp a non-positive start, adjusting length.
        int64_t end = (len >= 0) ? from + len : static_cast<int64_t>(s.size()) + 1;
        int64_t start = from < 1 ? 1 : from;
        if (end < start) end = start;
        if (start > static_cast<int64_t>(s.size())) return ExprValue("text", "", false);
        size_t b = static_cast<size_t>(start - 1);
        size_t e = std::min(static_cast<size_t>(end - 1), s.size());
        return ExprValue("text", s.substr(b, e - b), false);
    };
    // char_length / character_length — character count (bytes for our ASCII storage)
    functions_["char_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(a[0].value.size()), false);
    };
    functions_["character_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(a[0].value.size()), false);
    };
    functions_["octet_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(a[0].value.size()), false);
    };
    functions_["bit_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(a[0].value.size() * 8), false);
    };
    // lpad / rpad — pad (or truncate) a string to a target length with a fill string
    functions_["lpad"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        int64_t len = a[1].asInt();
        std::string fill = (a.size() >= 3 && !a[2].isNull) ? a[2].value : " ";
        if (len <= 0) return ExprValue("text", "", false);
        if (static_cast<int64_t>(s.size()) >= len)
            return ExprValue("text", s.substr(0, static_cast<size_t>(len)), false);
        if (fill.empty()) return ExprValue("text", s, false);
        std::string pad;
        while (static_cast<int64_t>(pad.size() + s.size()) < len) pad += fill;
        pad = pad.substr(0, static_cast<size_t>(len) - s.size());
        return ExprValue("text", pad + s, false);
    };
    functions_["rpad"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        int64_t len = a[1].asInt();
        std::string fill = (a.size() >= 3 && !a[2].isNull) ? a[2].value : " ";
        if (len <= 0) return ExprValue("text", "", false);
        if (static_cast<int64_t>(s.size()) >= len)
            return ExprValue("text", s.substr(0, static_cast<size_t>(len)), false);
        if (fill.empty()) return ExprValue("text", s, false);
        std::string out = s;
        size_t fi = 0;
        while (static_cast<int64_t>(out.size()) < len) {
            out += fill[fi % fill.size()];
            ++fi;
        }
        return ExprValue("text", out, false);
    };
    // btrim(str[, chars]) — trim matching characters (default whitespace) from both ends
    functions_["btrim"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        std::string chars = (a.size() >= 2 && !a[1].isNull) ? a[1].value : " \t\n\r\f\v";
        size_t b = 0, e = s.size();
        while (b < e && chars.find(s[b]) != std::string::npos) ++b;
        while (e > b && chars.find(s[e - 1]) != std::string::npos) --e;
        return ExprValue("text", s.substr(b, e - b), false);
    };
    // split_part(str, delim, n) — n-th field (1-based; negative counts from the end)
    functions_["split_part"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        const std::string& delim = a[1].value;
        int64_t n = a[2].asInt();
        std::vector<std::string> parts;
        if (delim.empty()) {
            parts.push_back(s);
        } else {
            size_t pos = 0, next;
            while ((next = s.find(delim, pos)) != std::string::npos) {
                parts.push_back(s.substr(pos, next - pos));
                pos = next + delim.size();
            }
            parts.push_back(s.substr(pos));
        }
        int64_t idx;
        if (n > 0) idx = n - 1;
        else if (n < 0) idx = static_cast<int64_t>(parts.size()) + n;
        else return ExprValue("text", "", false);
        if (idx < 0 || idx >= static_cast<int64_t>(parts.size()))
            return ExprValue("text", "", false);
        return ExprValue("text", parts[static_cast<size_t>(idx)], false);
    };
    // strpos(string, substring) — 1-based position of first match, 0 if absent
    functions_["strpos"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("integer", "", true);
        size_t pos = a[0].value.find(a[1].value);
        return ExprValue("integer",
                         std::to_string(pos == std::string::npos ? 0 : static_cast<int64_t>(pos + 1)),
                         false);
    };
    // initcap — capitalize the first letter of each word, lowercase the rest
    functions_["initcap"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string s = a[0].value;
        bool startWord = true;
        for (char& c : s) {
            unsigned char uc = static_cast<unsigned char>(c);
            if (std::isalnum(uc)) {
                c = startWord ? static_cast<char>(std::toupper(uc))
                              : static_cast<char>(std::tolower(uc));
                startWord = false;
            } else {
                startWord = true;
            }
        }
        return ExprValue("text", s, false);
    };
    // to_hex(int) — hexadecimal text of a (non-negative interpreted) integer
    functions_["to_hex"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        uint64_t v = static_cast<uint64_t>(a[0].asInt());
        if (v == 0) return ExprValue("text", "0", false);
        std::string out;
        const char* digits = "0123456789abcdef";
        while (v) { out.push_back(digits[v & 0xF]); v >>= 4; }
        std::reverse(out.begin(), out.end());
        return ExprValue("text", out, false);
    };
    // concat_ws(sep, ...) — join the non-NULL arguments with a separator
    functions_["concat_ws"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& sep = a[0].value;
        std::string out;
        bool first = true;
        for (size_t i = 1; i < a.size(); ++i) {
            if (a[i].isNull) continue;
            if (!first) out += sep;
            out += a[i].value;
            first = false;
        }
        return ExprValue("text", out, false);
    };
    // starts_with(str, prefix) — boolean prefix test
    functions_["starts_with"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("boolean", "", true);
        const std::string& s = a[0].value;
        const std::string& p = a[1].value;
        bool r = s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
        return ExprValue("boolean", r ? "t" : "f", false);
    };
    // translate(str, from, to) — map each "from" char to the matching "to" char,
    // deleting chars whose "from" index has no "to" counterpart
    functions_["translate"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        const std::string& from = a[1].value;
        const std::string& to = a[2].value;
        std::string out;
        for (char c : s) {
            size_t idx = from.find(c);
            if (idx == std::string::npos) out.push_back(c);
            else if (idx < to.size()) out.push_back(to[idx]);
            // else: char is deleted
        }
        return ExprValue("text", out, false);
    };
    // overlay(string, newsub, start[, count]) — replace count chars at 1-based start
    functions_["overlay"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        const std::string& repl = a[1].value;
        int64_t start = a[2].asInt();
        int64_t count = (a.size() >= 4 && !a[3].isNull)
                            ? a[3].asInt()
                            : static_cast<int64_t>(repl.size());
        if (start < 1) start = 1;
        if (count < 0) count = 0;
        size_t b = std::min(static_cast<size_t>(start - 1), s.size());
        size_t removed = std::min(static_cast<size_t>(count), s.size() - b);
        std::string out = s.substr(0, b) + repl + s.substr(b + removed);
        return ExprValue("text", out, false);
    };
    // quote_literal — single-quote a value, doubling embedded quotes
    functions_["quote_literal"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string out = "'";
        for (char c : a[0].value) {
            if (c == '\'') out += "''";
            else out.push_back(c);
        }
        out += "'";
        return ExprValue("text", out, false);
    };
    // quote_ident — double-quote an identifier when it is not a simple lower-case name
    functions_["quote_ident"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        bool simple = !s.empty();
        for (size_t i = 0; i < s.size() && simple; ++i) {
            unsigned char c = static_cast<unsigned char>(s[i]);
            bool ok = (c == '_') || (std::islower(c)) || (std::isdigit(c) && i > 0);
            if (!ok) simple = false;
        }
        if (simple) return ExprValue("text", s, false);
        std::string out = "\"";
        for (char c : s) {
            if (c == '"') out += "\"\"";
            else out.push_back(c);
        }
        out += "\"";
        return ExprValue("text", out, false);
    };

    // ------------------------------------------------------------------------
    // Date/time functions
    // ------------------------------------------------------------------------
    functions_["current_date"] = [](const std::vector<ExprValue>&) {
        return ExprValue("date", "2026-06-20", false);
    };
    functions_["extract"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("integer", "", true);
        std::string field = toLower(a[0].value);
        const std::string& src = a[1].value;
        int64_t result = 0;
        if (field == "year") {
            result = src.size() >= 4 ? std::stoll(src.substr(0, 4)) : 0;
        } else if (field == "month") {
            result = src.size() >= 7 ? std::stoll(src.substr(5, 2)) : 0;
        } else if (field == "day") {
            result = src.size() >= 10 ? std::stoll(src.substr(8, 2)) : 0;
        } else {
            return ExprValue("integer", "", true);
        }
        return ExprValue("integer", std::to_string(result), false);
    };
}

} // namespace dbms
