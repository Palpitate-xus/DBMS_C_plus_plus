#include "ExprEvaluator.h"
#include "common/DateType.h"
#include "types/numeric.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <regex>
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

// MD5 (RFC 1321) — compact self-contained implementation returning lowercase hex.
static std::string md5Hex(const std::string& msg) {
    static const uint32_t K[64] = {
        0xd76aa478,0xe8c7b756,0x242070db,0xc1bdceee,0xf57c0faf,0x4787c62a,0xa8304613,0xfd469501,
        0x698098d8,0x8b44f7af,0xffff5bb1,0x895cd7be,0x6b901122,0xfd987193,0xa679438e,0x49b40821,
        0xf61e2562,0xc040b340,0x265e5a51,0xe9b6c7aa,0xd62f105d,0x02441453,0xd8a1e681,0xe7d3fbc8,
        0x21e1cde6,0xc33707d6,0xf4d50d87,0x455a14ed,0xa9e3e905,0xfcefa3f8,0x676f02d9,0x8d2a4c8a,
        0xfffa3942,0x8771f681,0x6d9d6122,0xfde5380c,0xa4beea44,0x4bdecfa9,0xf6bb4b60,0xbebfbc70,
        0x289b7ec6,0xeaa127fa,0xd4ef3085,0x04881d05,0xd9d4d039,0xe6db99e5,0x1fa27cf8,0xc4ac5665,
        0xf4292244,0x432aff97,0xab9423a7,0xfc93a039,0x655b59c3,0x8f0ccc92,0xffeff47d,0x85845dd1,
        0x6fa87e4f,0xfe2ce6e0,0xa3014314,0x4e0811a1,0xf7537e82,0xbd3af235,0x2ad7d2bb,0xeb86d391};
    static const int S[64] = {
        7,12,17,22,7,12,17,22,7,12,17,22,7,12,17,22,
        5,9,14,20,5,9,14,20,5,9,14,20,5,9,14,20,
        4,11,16,23,4,11,16,23,4,11,16,23,4,11,16,23,
        6,10,15,21,6,10,15,21,6,10,15,21,6,10,15,21};
    auto rotl = [](uint32_t x, int c) { return (x << c) | (x >> (32 - c)); };

    std::vector<uint8_t> data(msg.begin(), msg.end());
    uint64_t bitlen = static_cast<uint64_t>(data.size()) * 8;
    data.push_back(0x80);
    while (data.size() % 64 != 56) data.push_back(0);
    for (int i = 0; i < 8; ++i) data.push_back(static_cast<uint8_t>(bitlen >> (8 * i)));

    uint32_t a0 = 0x67452301, b0 = 0xefcdab89, c0 = 0x98badcfe, d0 = 0x10325476;
    for (size_t off = 0; off < data.size(); off += 64) {
        uint32_t M[16];
        for (int i = 0; i < 16; ++i) {
            M[i] = static_cast<uint32_t>(data[off + i * 4]) |
                   (static_cast<uint32_t>(data[off + i * 4 + 1]) << 8) |
                   (static_cast<uint32_t>(data[off + i * 4 + 2]) << 16) |
                   (static_cast<uint32_t>(data[off + i * 4 + 3]) << 24);
        }
        uint32_t A = a0, B = b0, C = c0, D = d0;
        for (int i = 0; i < 64; ++i) {
            uint32_t F; int g;
            if (i < 16)      { F = (B & C) | (~B & D); g = i; }
            else if (i < 32) { F = (D & B) | (~D & C); g = (5 * i + 1) % 16; }
            else if (i < 48) { F = B ^ C ^ D;         g = (3 * i + 5) % 16; }
            else             { F = C ^ (B | ~D);      g = (7 * i) % 16; }
            F = F + A + K[i] + M[g];
            A = D; D = C; C = B;
            B = B + rotl(F, S[i]);
        }
        a0 += A; b0 += B; c0 += C; d0 += D;
    }
    uint32_t vals[4] = {a0, b0, c0, d0};
    static const char* hex = "0123456789abcdef";
    std::string out;
    for (int i = 0; i < 4; ++i)
        for (int j = 0; j < 4; ++j) {
            uint8_t byte = static_cast<uint8_t>(vals[i] >> (8 * j));
            out.push_back(hex[byte >> 4]);
            out.push_back(hex[byte & 0xF]);
        }
    return out;
}

// Base64 encode/decode over raw byte strings.
static std::string base64Encode(const std::string& in) {
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    size_t i = 0;
    while (i + 2 < in.size()) {
        uint32_t n = (static_cast<uint8_t>(in[i]) << 16) |
                     (static_cast<uint8_t>(in[i + 1]) << 8) |
                     static_cast<uint8_t>(in[i + 2]);
        out.push_back(tbl[(n >> 18) & 63]);
        out.push_back(tbl[(n >> 12) & 63]);
        out.push_back(tbl[(n >> 6) & 63]);
        out.push_back(tbl[n & 63]);
        i += 3;
    }
    size_t rem = in.size() - i;
    if (rem == 1) {
        uint32_t n = static_cast<uint8_t>(in[i]) << 16;
        out.push_back(tbl[(n >> 18) & 63]);
        out.push_back(tbl[(n >> 12) & 63]);
        out += "==";
    } else if (rem == 2) {
        uint32_t n = (static_cast<uint8_t>(in[i]) << 16) | (static_cast<uint8_t>(in[i + 1]) << 8);
        out.push_back(tbl[(n >> 18) & 63]);
        out.push_back(tbl[(n >> 12) & 63]);
        out.push_back(tbl[(n >> 6) & 63]);
        out.push_back('=');
    }
    return out;
}

static bool base64Decode(const std::string& in, std::string& out) {
    auto val = [](char c) -> int {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        if (c == '+') return 62;
        if (c == '/') return 63;
        return -1;
    };
    out.clear();
    int buf = 0, bits = 0;
    for (char c : in) {
        if (c == '=' ) break;
        if (std::isspace(static_cast<unsigned char>(c))) continue;
        int v = val(c);
        if (v < 0) return false;
        buf = (buf << 6) | v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out.push_back(static_cast<char>((buf >> bits) & 0xFF));
        }
    }
    return true;
}

static std::string hexEncode(const std::string& in) {
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(in.size() * 2);
    for (unsigned char c : in) {
        out.push_back(hex[c >> 4]);
        out.push_back(hex[c & 0xF]);
    }
    return out;
}

static bool hexDecode(const std::string& in, std::string& out) {
    auto nib = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    out.clear();
    std::string clean;
    for (char c : in) if (!std::isspace(static_cast<unsigned char>(c))) clean.push_back(c);
    if (clean.size() % 2 != 0) return false;
    for (size_t i = 0; i < clean.size(); i += 2) {
        int hi = nib(clean[i]), lo = nib(clean[i + 1]);
        if (hi < 0 || lo < 0) return false;
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return true;
}

static std::string trimStr(const std::string& s) {
    size_t b = 0, e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

// Split a PostgreSQL array literal '{a,b,...}' into its top-level element
// tokens (raw text, quotes preserved), respecting nested {} and "" quoting.
// Returns false if the text is not a brace-delimited array.
static bool parseArrayElements(const std::string& text, std::vector<std::string>& out) {
    out.clear();
    std::string s = trimStr(text);
    if (s.size() < 2 || s.front() != '{' || s.back() != '}') return false;
    std::string inner = s.substr(1, s.size() - 2);
    if (trimStr(inner).empty()) return true;  // empty array
    int depth = 0;
    bool inQ = false;
    std::string cur;
    for (size_t i = 0; i < inner.size(); ++i) {
        char c = inner[i];
        if (inQ) {
            cur.push_back(c);
            if (c == '\\' && i + 1 < inner.size()) { cur.push_back(inner[++i]); }
            else if (c == '"') inQ = false;
        } else if (c == '"') {
            inQ = true; cur.push_back(c);
        } else if (c == '{') {
            ++depth; cur.push_back(c);
        } else if (c == '}') {
            --depth; cur.push_back(c);
        } else if (c == ',' && depth == 0) {
            out.push_back(trimStr(cur)); cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(trimStr(cur));
    return true;
}

// Strip surrounding double-quotes from an array element token and unescape.
static std::string arrayElemUnquote(const std::string& tok) {
    if (tok.size() >= 2 && tok.front() == '"' && tok.back() == '"') {
        std::string out;
        for (size_t i = 1; i + 1 < tok.size(); ++i) {
            if (tok[i] == '\\' && i + 2 < tok.size()) { out.push_back(tok[++i]); }
            else out.push_back(tok[i]);
        }
        return out;
    }
    return tok;
}

// Quote an array element token if it needs quoting (contains delimiters, braces,
// quotes, leading/trailing space, or is empty / looks like NULL).
static std::string arrayElemQuote(const std::string& v) {
    bool needQuote = v.empty();
    std::string low;
    for (char c : v) low.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    if (low == "null") needQuote = true;
    for (char c : v) {
        if (c == ',' || c == '{' || c == '}' || c == '"' || c == '\\' ||
            std::isspace(static_cast<unsigned char>(c))) { needQuote = true; break; }
    }
    if (!needQuote) return v;
    std::string out = "\"";
    for (char c : v) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    out += "\"";
    return out;
}

// JSON value text -> normalized type name ('object'/'array'/'string'/'number'/
// 'boolean'/'null'); empty string for unrecognized input.
static std::string jsonTypeOf(const std::string& s) {
    std::string t = trimStr(s);
    if (t.empty()) return "";
    char c = t[0];
    if (c == '{') return "object";
    if (c == '[') return "array";
    if (c == '"') return "string";
    if (c == 't' || c == 'f') return "boolean";
    if (c == 'n') return "null";
    if (c == '-' || (c >= '0' && c <= '9')) return "number";
    return "";
}

// Split a JSON array/object body into top-level element strings, respecting
// nested {}/[] and "" quoting. open/close are the delimiter braces.
static bool jsonTopLevelSplit(const std::string& s, char open, char close,
                              std::vector<std::string>& out) {
    out.clear();
    std::string t = trimStr(s);
    if (t.size() < 2 || t.front() != open || t.back() != close) return false;
    std::string inner = t.substr(1, t.size() - 2);
    if (trimStr(inner).empty()) return true;
    int depth = 0;
    bool inQ = false;
    std::string cur;
    for (size_t i = 0; i < inner.size(); ++i) {
        char c = inner[i];
        if (inQ) {
            cur.push_back(c);
            if (c == '\\' && i + 1 < inner.size()) cur.push_back(inner[++i]);
            else if (c == '"') inQ = false;
        } else if (c == '"') {
            inQ = true; cur.push_back(c);
        } else if (c == '{' || c == '[') {
            ++depth; cur.push_back(c);
        } else if (c == '}' || c == ']') {
            --depth; cur.push_back(c);
        } else if (c == ',' && depth == 0) {
            out.push_back(trimStr(cur)); cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(trimStr(cur));
    return true;
}

static std::string jsonQuoteStr(const std::string& v) {
    std::string out = "\"";
    for (char c : v) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    out += "\"";
    return out;
}

// Render an ExprValue as a compact JSON value.
static std::string toJsonValue(const ExprValue& v) {
    if (v.isNull) return "null";
    std::string t = v.typeName;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (t == "boolean" || t == "bool") {
        std::string lv = v.value;
        for (char& c : lv) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return (lv == "t" || lv == "true" || lv == "1") ? "true" : "false";
    }
    bool numeric = (t.find("int") != std::string::npos) || t == "numeric" || t == "decimal" ||
                   t.find("double") != std::string::npos || t == "real" || t == "float" ||
                   t == "smallint" || t == "bigint";
    if (numeric && !v.value.empty()) return v.value;
    if (t == "json" || t == "jsonb") return v.value;  // already JSON
    return jsonQuoteStr(v.value);
}

// Navigate one level into a JSON value: by key for an object member, or by
// integer index for an array element. Returns false if the step does not apply.
static bool jsonStep(const std::string& cur, const std::string& key, std::string& out) {
    std::string t = trimStr(cur);
    if (t.empty()) return false;
    if (t.front() == '{') {
        std::vector<std::string> members;
        if (!jsonTopLevelSplit(t, '{', '}', members)) return false;
        for (const auto& m : members) {
            // Split "key": value at the first top-level colon (honoring quotes).
            bool inQ = false;
            size_t colon = std::string::npos;
            for (size_t i = 0; i < m.size(); ++i) {
                char c = m[i];
                if (inQ) { if (c == '\\' && i + 1 < m.size()) ++i; else if (c == '"') inQ = false; }
                else if (c == '"') inQ = true;
                else if (c == ':') { colon = i; break; }
            }
            if (colon == std::string::npos) continue;
            std::string k = trimStr(m.substr(0, colon));
            std::string ku = (k.size() >= 2 && k.front() == '"' && k.back() == '"')
                                 ? k.substr(1, k.size() - 2) : k;
            if (ku == key) { out = trimStr(m.substr(colon + 1)); return true; }
        }
        return false;
    }
    if (t.front() == '[') {
        std::vector<std::string> elems;
        if (!jsonTopLevelSplit(t, '[', ']', elems)) return false;
        long idx = 0;
        try { size_t pos = 0; idx = std::stol(key, &pos); if (pos != key.size()) return false; }
        catch (...) { return false; }
        if (idx < 0 || idx >= static_cast<long>(elems.size())) return false;
        out = elems[static_cast<size_t>(idx)];
        return true;
    }
    return false;
}

// Build a std::regex from a PostgreSQL-style pattern + flags ('i' case-insensitive,
// 'g' handled by the caller). Uses ECMAScript syntax (close to POSIX ERE for
// common patterns). Sets ok=false on a malformed pattern.
static std::regex buildRegex(const std::string& pattern, const std::string& flags, bool& ok) {
    auto f = std::regex::ECMAScript;
    for (char c : flags) {
        if (c == 'i') f |= std::regex::icase;
        else if (c == 'm') f |= std::regex::multiline;
    }
    ok = true;
    try {
        return std::regex(pattern, f);
    } catch (...) {
        ok = false;
        return std::regex();
    }
}

// Translate a PostgreSQL replacement string (\1..\9 backrefs, \& whole match,
// \\ literal backslash) into the std::regex_replace ($1, $&) form, escaping any
// literal '$'.
static std::string translateReplacement(const std::string& repl) {
    std::string out;
    for (size_t i = 0; i < repl.size(); ++i) {
        char c = repl[i];
        if (c == '\\' && i + 1 < repl.size()) {
            char n = repl[i + 1];
            if (n >= '0' && n <= '9') { out.push_back('$'); out.push_back(n); ++i; }
            else if (n == '&') { out += "$&"; ++i; }
            else if (n == '\\') { out.push_back('\\'); ++i; }
            else { out.push_back(n); ++i; }
        } else if (c == '$') {
            out += "$$";  // escape literal $ for std::regex_replace
        } else {
            out.push_back(c);
        }
    }
    return out;
}

// Parsed pieces of a range literal: 'empty' or '[lo,hi)' style.
struct RangeParts {
    bool valid = false;
    bool empty = false;
    bool loInc = false, hiInc = false;   // inclusive bound flags
    bool loInf = false, hiInf = false;   // unbounded (infinite) flags
    std::string lo, hi;                  // bound text (unquoted), empty if infinite
};

static RangeParts parseRangeLiteral(const std::string& text) {
    RangeParts r;
    std::string s = trimStr(text);
    std::string low = s;
    for (char& c : low) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (low == "empty") { r.valid = true; r.empty = true; return r; }
    if (s.size() < 3) return r;  // need at least "[,]"
    char f = s.front(), b = s.back();
    if ((f != '[' && f != '(') || (b != ']' && b != ')')) return r;
    r.loInc = (f == '[');
    r.hiInc = (b == ']');
    std::string inner = s.substr(1, s.size() - 2);
    // Split at the top-level comma, honoring double quotes.
    bool inQ = false;
    size_t comma = std::string::npos;
    for (size_t i = 0; i < inner.size(); ++i) {
        char c = inner[i];
        if (inQ) { if (c == '\\' && i + 1 < inner.size()) ++i; else if (c == '"') inQ = false; }
        else if (c == '"') inQ = true;
        else if (c == ',') { comma = i; break; }
    }
    if (comma == std::string::npos) return r;
    auto unq = [](std::string v) {
        v = trimStr(v);
        if (v.size() >= 2 && v.front() == '"' && v.back() == '"') {
            std::string o;
            for (size_t i = 1; i + 1 < v.size(); ++i) {
                if (v[i] == '\\' && i + 2 < v.size()) o.push_back(v[++i]);
                else o.push_back(v[i]);
            }
            return o;
        }
        return v;
    };
    r.lo = unq(inner.substr(0, comma));
    r.hi = unq(inner.substr(comma + 1));
    r.loInf = r.lo.empty();
    r.hiInf = r.hi.empty();
    r.valid = true;
    return r;
}

static bool typeIsRange(const std::string& typeName) {
    std::string t = typeName;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return t.find("range") != std::string::npos;
}

// SQL identifier quoting (quote_ident / format %I): only quote when not a simple
// lower-case identifier; double embedded quotes.
static std::string sqlQuoteIdent(const std::string& s) {
    bool simple = !s.empty();
    for (size_t i = 0; i < s.size() && simple; ++i) {
        unsigned char c = static_cast<unsigned char>(s[i]);
        bool ok = (c == '_') || std::islower(c) || (std::isdigit(c) && i > 0);
        if (!ok) simple = false;
    }
    if (simple) return s;
    std::string out = "\"";
    for (char c : s) { if (c == '"') out += "\"\""; else out.push_back(c); }
    out += "\"";
    return out;
}

// SQL string-literal quoting (quote_literal / format %L): single-quote, doubling
// embedded quotes.
static std::string sqlQuoteLiteral(const std::string& s) {
    std::string out = "'";
    for (char c : s) { if (c == '\'') out += "''"; else out.push_back(c); }
    out += "'";
    return out;
}

// to_char(date/timestamp, fmt): format an ISO 'YYYY-MM-DD[ HH:MM:SS]' (or a
// time-only 'HH:MM:SS') value per a PostgreSQL-style template. Supported tokens
// (case-insensitive match; letter tokens honour the casing of the template):
//   YYYY YYY YY Y · MM · MON Mon mon · MONTH Month month · DD · DDD · D ·
//   DAY Day day · DY Dy dy · HH HH12 HH24 · MI · SS · AM PM am pm · Q · WW
// Double-quoted runs are emitted verbatim; any other char passes through.
// Note: month/day names are NOT blank-padded to a fixed width (PostgreSQL pads
// MONTH/DAY to 9 chars by default); the natural-width form is returned.
static std::string formatDateTime(const std::string& src, const std::string& fmt) {
    auto num = [&](size_t off, size_t len) -> int {
        if (src.size() < off + len) return 0;
        int v = 0;
        for (size_t i = off; i < off + len; ++i) {
            char c = src[i];
            if (c < '0' || c > '9') return 0;
            v = v * 10 + (c - '0');
        }
        return v;
    };
    bool hasDate = src.size() >= 10 && src[4] == '-' && src[7] == '-';
    int y, mo, d, h, mi, se;
    if (hasDate) {
        y = num(0, 4); mo = num(5, 2); d = num(8, 2);
        h = num(11, 2); mi = num(14, 2); se = num(17, 2);
    } else {
        // Time-only 'HH:MM:SS'.
        y = mo = d = 0;
        h = num(0, 2); mi = num(3, 2); se = num(6, 2);
    }

    static const char* MON_FULL[] = {"January", "February", "March", "April",
        "May", "June", "July", "August", "September", "October", "November", "December"};
    static const char* MON_ABBR[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    static const char* DAY_FULL[] = {"Sunday", "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday"};
    static const char* DAY_ABBR[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

    // Sakamoto's day-of-week: 0=Sunday .. 6=Saturday.
    auto dow = [&]() -> int {
        static const int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        int yy = y - (mo < 3 ? 1 : 0);
        int w = (yy + yy / 4 - yy / 100 + yy / 400 + t[(mo > 0 ? mo : 1) - 1] + d) % 7;
        return (w < 0) ? w + 7 : w;
    };
    auto doy = [&]() -> int {
        Date cur(y, mo, d), jan1(y, 1, 1);
        return (cur.year != 0 && jan1.year != 0)
                   ? static_cast<int>(cur.convert() - jan1.convert() + 1) : 0;
    };
    // Casing style derived from a matched token: 1=UPPER, 2=Capitalized, 3=lower.
    auto styleOf = [&](size_t pos, size_t len) -> int {
        bool allUpper = true, allLower = true;
        for (size_t i = pos; i < pos + len && i < fmt.size(); ++i) {
            unsigned char c = static_cast<unsigned char>(fmt[i]);
            if (std::isalpha(c)) {
                if (std::islower(c)) allUpper = false;
                if (std::isupper(c)) allLower = false;
            }
        }
        if (allUpper) return 1;
        if (allLower) return 3;
        return 2;
    };
    auto recase = [](std::string s, int style) {
        if (style == 1)
            for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        else if (style == 3)
            for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        else if (style == 2)
            for (size_t i = 0; i < s.size(); ++i)
                s[i] = static_cast<char>(i == 0 ? std::toupper(static_cast<unsigned char>(s[i]))
                                                : std::tolower(static_cast<unsigned char>(s[i])));
        return s;
    };
    auto matches = [&](size_t pos, const char* kw) -> bool {
        size_t n = std::strlen(kw);
        if (pos + n > fmt.size()) return false;
        for (size_t i = 0; i < n; ++i)
            if (std::tolower(static_cast<unsigned char>(fmt[pos + i])) !=
                std::tolower(static_cast<unsigned char>(kw[i])))
                return false;
        return true;
    };
    auto hh12 = [&]() { int x = h % 12; return x == 0 ? 12 : x; };

    char buf[16];
    std::string out;
    size_t i = 0;
    while (i < fmt.size()) {
        char c = fmt[i];
        if (c == '"') {  // quoted literal run
            ++i;
            while (i < fmt.size() && fmt[i] != '"') out.push_back(fmt[i++]);
            if (i < fmt.size()) ++i;  // skip closing quote
            continue;
        }
        // Longest token first so HH24 beats HH, MONTH beats MON beats MM, etc.
        if (matches(i, "HH24")) { std::snprintf(buf, sizeof buf, "%02d", h); out += buf; i += 4; continue; }
        if (matches(i, "HH12")) { std::snprintf(buf, sizeof buf, "%02d", hh12()); out += buf; i += 4; continue; }
        if (matches(i, "YYYY")) { std::snprintf(buf, sizeof buf, "%04d", y); out += buf; i += 4; continue; }
        if (matches(i, "MONTH")) { out += recase(MON_FULL[(mo >= 1 && mo <= 12) ? mo - 1 : 0], styleOf(i, 5)); i += 5; continue; }
        if (matches(i, "MON")) { out += recase(MON_ABBR[(mo >= 1 && mo <= 12) ? mo - 1 : 0], styleOf(i, 3)); i += 3; continue; }
        if (matches(i, "DAY")) { out += recase(DAY_FULL[dow()], styleOf(i, 3)); i += 3; continue; }
        if (matches(i, "DDD")) { std::snprintf(buf, sizeof buf, "%03d", doy()); out += buf; i += 3; continue; }
        if (matches(i, "YYY")) { std::snprintf(buf, sizeof buf, "%03d", y % 1000); out += buf; i += 3; continue; }
        if (matches(i, "DY")) { out += recase(DAY_ABBR[dow()], styleOf(i, 2)); i += 2; continue; }
        if (matches(i, "YY")) { std::snprintf(buf, sizeof buf, "%02d", y % 100); out += buf; i += 2; continue; }
        if (matches(i, "MM")) { std::snprintf(buf, sizeof buf, "%02d", mo); out += buf; i += 2; continue; }
        if (matches(i, "DD")) { std::snprintf(buf, sizeof buf, "%02d", d); out += buf; i += 2; continue; }
        if (matches(i, "HH")) { std::snprintf(buf, sizeof buf, "%02d", hh12()); out += buf; i += 2; continue; }
        if (matches(i, "MI")) { std::snprintf(buf, sizeof buf, "%02d", mi); out += buf; i += 2; continue; }
        if (matches(i, "SS")) { std::snprintf(buf, sizeof buf, "%02d", se); out += buf; i += 2; continue; }
        if (matches(i, "AM") || matches(i, "PM")) { out += recase(h >= 12 ? "PM" : "AM", styleOf(i, 2)); i += 2; continue; }
        if (matches(i, "WW")) { std::snprintf(buf, sizeof buf, "%02d", (doy() - 1) / 7 + 1); out += buf; i += 2; continue; }
        if (c == 'Q' || c == 'q') { std::snprintf(buf, sizeof buf, "%d", mo > 0 ? (mo - 1) / 3 + 1 : 0); out += buf; ++i; continue; }
        if (c == 'D' || c == 'd') { std::snprintf(buf, sizeof buf, "%d", dow() + 1); out += buf; ++i; continue; }
        if (c == 'Y' || c == 'y') { std::snprintf(buf, sizeof buf, "%d", y % 10); out += buf; ++i; continue; }
        out.push_back(c);
        ++i;
    }
    return out;
}

// to_char(numeric, fmt): minimal number formatter supporting '9'/'0' digit
// placeholders, a '.' decimal point, and a leading 'FM' (fill mode: suppress
// the leading sign-position blank). '0' placeholders zero-pad the integer part.
// Grouping ('G'/','), currency, and sign templates are not implemented.
static std::string formatNumeric(double val, const std::string& fmtIn) {
    std::string fmt = fmtIn;
    bool fm = false;
    if (fmt.size() >= 2 && (fmt[0] == 'F' || fmt[0] == 'f') &&
        (fmt[1] == 'M' || fmt[1] == 'm')) { fm = true; fmt = fmt.substr(2); }
    size_t dot = fmt.find('.');
    int fracDigits = 0;
    if (dot != std::string::npos)
        for (size_t i = dot + 1; i < fmt.size(); ++i)
            if (fmt[i] == '9' || fmt[i] == '0') ++fracDigits;
    int intPlaces = 0; bool zeroPad = false;
    size_t intEnd = (dot == std::string::npos) ? fmt.size() : dot;
    for (size_t i = 0; i < intEnd; ++i) {
        if (fmt[i] == '9') ++intPlaces;
        else if (fmt[i] == '0') { ++intPlaces; zeroPad = true; }
    }
    bool neg = val < 0;
    char numbuf[64];
    std::snprintf(numbuf, sizeof numbuf, "%.*f", fracDigits, neg ? -val : val);
    std::string s = numbuf, ip = s, fp;
    size_t sp = s.find('.');
    if (sp != std::string::npos) { ip = s.substr(0, sp); fp = s.substr(sp + 1); }
    if (zeroPad && static_cast<int>(ip.size()) < intPlaces)
        ip = std::string(intPlaces - ip.size(), '0') + ip;
    std::string out = neg ? "-" : (fm ? "" : " ");
    out += ip;
    if (fracDigits > 0) out += "." + fp;
    return out;
}

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
        // Overload: lower(anyrange) returns the lower bound (NULL if unbounded).
        if (typeIsRange(a[0].typeName)) {
            RangeParts r = parseRangeLiteral(a[0].value);
            if (!r.valid || r.empty || r.loInf) return ExprValue(a[0].typeName, "", true);
            return ExprValue("numeric", r.lo, false);
        }
        return ExprValue("text", toLower(a[0].value), false);
    };
    functions_["upper"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        // Overload: upper(anyrange) returns the upper bound (NULL if unbounded).
        if (typeIsRange(a[0].typeName)) {
            RangeParts r = parseRangeLiteral(a[0].value);
            if (!r.valid || r.empty || r.hiInf) return ExprValue(a[0].typeName, "", true);
            return ExprValue("numeric", r.hi, false);
        }
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
    functions_["log"]   = [](const std::vector<ExprValue>& a) {
        // log(x) = base-10 log; log(b, x) = base-b log.
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        if (a.size() >= 2) {
            if (a[1].isNull) return ExprValue("double precision", "", true);
            double b = a[0].asDouble(), x = a[1].asDouble();
            return ExprValue("double precision", std::to_string(std::log(x) / std::log(b)), false);
        }
        return ExprValue("double precision", std::to_string(std::log10(a[0].asDouble())), false);
    };
    functions_["log10"] = [&](const auto& a) { return unaryMath(a, std::log10); };
    functions_["sqrt"]  = [&](const auto& a) { return unaryMath(a, std::sqrt); };
    functions_["cbrt"]  = [&](const auto& a) { return unaryMath(a, std::cbrt); };
    functions_["ceil"]  = [&](const auto& a) { return unaryMath(a, std::ceil); };
    functions_["floor"] = [&](const auto& a) { return unaryMath(a, std::floor); };
    functions_["trunc"] = [](const std::vector<ExprValue>& a) {
        // trunc(x) truncates toward zero; trunc(x, n) keeps n decimal places.
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        double v = a[0].asDouble();
        if (a.size() >= 2 && !a[1].isNull) {
            int n = static_cast<int>(a[1].asInt());
            double mult = std::pow(10.0, n);
            return ExprValue("numeric", std::to_string(std::trunc(v * mult) / mult), false);
        }
        return ExprValue("double precision", std::to_string(std::trunc(v)), false);
    };

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
    // pow — alias of power; ceiling — alias of ceil
    functions_["pow"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         std::to_string(std::pow(a[0].asDouble(), a[1].asDouble())), false);
    };
    functions_["ceiling"] = [&](const auto& a) { return unaryMath(a, std::ceil); };
    // degrees / radians
    functions_["degrees"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         std::to_string(a[0].asDouble() * 180.0 / (std::atan(1.0) * 4.0)), false);
    };
    functions_["radians"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         std::to_string(a[0].asDouble() * (std::atan(1.0) * 4.0) / 180.0), false);
    };
    // cot — cotangent
    functions_["cot"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        return ExprValue("double precision", std::to_string(1.0 / std::tan(a[0].asDouble())), false);
    };
    // Hyperbolic functions
    functions_["sinh"]  = [&](const auto& a) { return unaryMath(a, std::sinh); };
    functions_["cosh"]  = [&](const auto& a) { return unaryMath(a, std::cosh); };
    functions_["tanh"]  = [&](const auto& a) { return unaryMath(a, std::tanh); };
    functions_["asinh"] = [&](const auto& a) { return unaryMath(a, std::asinh); };
    functions_["acosh"] = [&](const auto& a) { return unaryMath(a, std::acosh); };
    functions_["atanh"] = [&](const auto& a) { return unaryMath(a, std::atanh); };
    // gcd / lcm — integer greatest common divisor / least common multiple
    functions_["gcd"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bigint", "", true);
        int64_t x = std::llabs(a[0].asInt()), y = std::llabs(a[1].asInt());
        while (y) { int64_t t = x % y; x = y; y = t; }
        return ExprValue("bigint", std::to_string(x), false);
    };
    functions_["lcm"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bigint", "", true);
        int64_t x = std::llabs(a[0].asInt()), y = std::llabs(a[1].asInt());
        if (x == 0 || y == 0) return ExprValue("bigint", "0", false);
        int64_t g = x, b = y;
        while (b) { int64_t t = g % b; g = b; b = t; }
        return ExprValue("bigint", std::to_string(x / g * y), false);
    };
    // div(y, x) — integer quotient of y / x, truncated toward zero
    functions_["div"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("numeric", "", true);
        int64_t x = a[1].asInt();
        if (x == 0) return ExprValue("numeric", "", true);
        return ExprValue("numeric", std::to_string(a[0].asInt() / x), false);
    };
    // factorial(n) — n! for small non-negative n
    functions_["factorial"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        int64_t n = a[0].asInt();
        if (n < 0) return ExprValue("numeric", "", true);
        int64_t r = 1;
        for (int64_t i = 2; i <= n; ++i) r *= i;
        return ExprValue("numeric", std::to_string(r), false);
    };
    // width_bucket(operand, low, high, count) — histogram bucket index (1..count)
    functions_["width_bucket"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 4 || a[0].isNull || a[1].isNull || a[2].isNull || a[3].isNull)
            return ExprValue("integer", "", true);
        double v = a[0].asDouble(), lo = a[1].asDouble(), hi = a[2].asDouble();
        int64_t count = a[3].asInt();
        if (count <= 0 || lo == hi) return ExprValue("integer", "", true);
        bool reversed = lo > hi;
        if (reversed) std::swap(lo, hi);
        int64_t bucket;
        if (v < lo) bucket = reversed ? count + 1 : 0;
        else if (v >= hi) bucket = reversed ? 0 : count + 1;
        else {
            int64_t b = static_cast<int64_t>((v - lo) / (hi - lo) * static_cast<double>(count)) + 1;
            bucket = reversed ? count - b + 1 : b;
        }
        return ExprValue("integer", std::to_string(bucket), false);
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
        return ExprValue("text", sqlQuoteLiteral(a[0].value), false);
    };
    // quote_ident — double-quote an identifier when it is not a simple lower-case name
    functions_["quote_ident"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        return ExprValue("text", sqlQuoteIdent(a[0].value), false);
    };
    // format(fmtstr, args...) — %s (string), %I (identifier), %L (literal), %% (percent)
    functions_["format"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string& fmt = a[0].value;
        size_t argi = 1;
        std::string out;
        for (size_t i = 0; i < fmt.size(); ++i) {
            if (fmt[i] != '%' || i + 1 >= fmt.size()) { out.push_back(fmt[i]); continue; }
            char spec = fmt[i + 1];
            if (spec == '%') { out.push_back('%'); ++i; continue; }
            if (spec == 's' || spec == 'I' || spec == 'L') {
                ++i;
                ExprValue arg = (argi < a.size()) ? a[argi++] : ExprValue("text", "", true);
                if (spec == 's') out += arg.isNull ? "" : arg.value;
                else if (spec == 'I') out += sqlQuoteIdent(arg.isNull ? "" : arg.value);
                else /* L */ out += arg.isNull ? "NULL" : sqlQuoteLiteral(arg.value);
            } else {
                out.push_back('%');  // unknown spec: keep the percent literally
            }
        }
        return ExprValue("text", out, false);
    };
    // nvl / ifnull — 2-arg null-coalescing (Oracle/MySQL compatibility)
    functions_["nvl"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2) return ExprValue("unknown", "", true);
        return a[0].isNull ? a[1] : a[0];
    };
    functions_["ifnull"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2) return ExprValue("unknown", "", true);
        return a[0].isNull ? a[1] : a[0];
    };
    // md5(text) — 32-char lowercase hex digest
    functions_["md5"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        return ExprValue("text", md5Hex(a[0].value), false);
    };
    // encode(data, format) — format is 'hex', 'base64', or 'escape'
    functions_["encode"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        std::string fmt = toLower(a[1].value);
        const std::string& data = a[0].value;
        if (fmt == "hex") return ExprValue("text", hexEncode(data), false);
        if (fmt == "base64") return ExprValue("text", base64Encode(data), false);
        if (fmt == "escape") {
            std::string out;
            for (unsigned char c : data) {
                if (c == '\\') out += "\\\\";
                else if (c < 0x20 || c > 0x7e) {
                    char buf[5];
                    std::snprintf(buf, sizeof(buf), "\\%03o", c);
                    out += buf;
                } else out.push_back(static_cast<char>(c));
            }
            return ExprValue("text", out, false);
        }
        return ExprValue("text", "", true);  // unknown format
    };
    // decode(text, format) — inverse of encode, returns the raw bytes as text
    functions_["decode"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bytea", "", true);
        std::string fmt = toLower(a[1].value);
        const std::string& text = a[0].value;
        std::string out;
        if (fmt == "hex") {
            if (!hexDecode(text, out)) return ExprValue("bytea", "", true);
            return ExprValue("bytea", out, false);
        }
        if (fmt == "base64") {
            if (!base64Decode(text, out)) return ExprValue("bytea", "", true);
            return ExprValue("bytea", out, false);
        }
        if (fmt == "escape") {
            for (size_t i = 0; i < text.size(); ++i) {
                if (text[i] == '\\' && i + 1 < text.size()) {
                    if (text[i + 1] == '\\') { out.push_back('\\'); i += 1; }
                    else if (i + 3 < text.size()) {
                        int v = 0; bool ok = true;
                        for (int k = 1; k <= 3; ++k) {
                            char c = text[i + k];
                            if (c < '0' || c > '7') { ok = false; break; }
                            v = v * 8 + (c - '0');
                        }
                        if (ok) { out.push_back(static_cast<char>(v)); i += 3; }
                        else out.push_back(text[i]);
                    } else out.push_back(text[i]);
                } else out.push_back(text[i]);
            }
            return ExprValue("bytea", out, false);
        }
        return ExprValue("bytea", "", true);
    };

    // ------------------------------------------------------------------------
    // Array functions (operate on the '{...}' array literal text)
    // ------------------------------------------------------------------------
    // array_length(arr, dim) — element count along dimension 1 (or 2); NULL if empty/unknown dim
    functions_["array_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        int dim = (a.size() >= 2 && !a[1].isNull) ? static_cast<int>(a[1].asInt()) : 1;
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems) || elems.empty())
            return ExprValue("integer", "", true);
        if (dim == 1) return ExprValue("integer", std::to_string(elems.size()), false);
        if (dim == 2) {
            std::vector<std::string> sub;
            if (!parseArrayElements(elems[0], sub) || sub.empty())
                return ExprValue("integer", "", true);
            return ExprValue("integer", std::to_string(sub.size()), false);
        }
        return ExprValue("integer", "", true);
    };
    // cardinality(arr) — total number of elements across all dimensions
    functions_["cardinality"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems)) return ExprValue("integer", "", true);
        // Count leaves recursively.
        std::function<int64_t(const std::vector<std::string>&)> count =
            [&](const std::vector<std::string>& es) -> int64_t {
            int64_t n = 0;
            for (const auto& e : es) {
                std::vector<std::string> sub;
                if (parseArrayElements(e, sub)) n += count(sub);
                else n += 1;
            }
            return n;
        };
        return ExprValue("integer", std::to_string(count(elems)), false);
    };
    // array_ndims(arr) — number of dimensions (leading '{' count)
    functions_["array_ndims"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        std::string s = a[0].value;
        size_t b = 0;
        while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
        int n = 0;
        while (b < s.size() && s[b] == '{') { ++n; ++b; }
        if (n == 0) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(n), false);
    };
    // array_lower(arr, dim) — PG arrays default to lower bound 1
    functions_["array_lower"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems) || elems.empty())
            return ExprValue("integer", "", true);
        return ExprValue("integer", "1", false);
    };
    // array_upper(arr, dim) — upper bound == length for the default lower bound 1
    functions_["array_upper"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        int dim = (a.size() >= 2 && !a[1].isNull) ? static_cast<int>(a[1].asInt()) : 1;
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems) || elems.empty())
            return ExprValue("integer", "", true);
        if (dim == 1) return ExprValue("integer", std::to_string(elems.size()), false);
        if (dim == 2) {
            std::vector<std::string> sub;
            if (!parseArrayElements(elems[0], sub) || sub.empty())
                return ExprValue("integer", "", true);
            return ExprValue("integer", std::to_string(sub.size()), false);
        }
        return ExprValue("integer", "", true);
    };
    // array_append(arr, elem) — append element, returning the new array literal
    functions_["array_append"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull) return ExprValue("ARRAY", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems)) return ExprValue("ARRAY", "", true);
        elems.push_back(a[1].isNull ? "NULL" : arrayElemQuote(a[1].value));
        std::string out = "{";
        for (size_t i = 0; i < elems.size(); ++i) { if (i) out += ","; out += elems[i]; }
        out += "}";
        return ExprValue("ARRAY", out, false);
    };
    // array_prepend(elem, arr) — prepend element
    functions_["array_prepend"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[1].isNull) return ExprValue("ARRAY", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[1].value, elems)) return ExprValue("ARRAY", "", true);
        elems.insert(elems.begin(), a[0].isNull ? "NULL" : arrayElemQuote(a[0].value));
        std::string out = "{";
        for (size_t i = 0; i < elems.size(); ++i) { if (i) out += ","; out += elems[i]; }
        out += "}";
        return ExprValue("ARRAY", out, false);
    };
    // array_cat(a, b) — concatenate two arrays
    functions_["array_cat"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2) return ExprValue("ARRAY", "", true);
        std::vector<std::string> ea, eb;
        if (a[0].isNull && !a[1].isNull) return ExprValue("ARRAY", a[1].value, false);
        if (a[1].isNull && !a[0].isNull) return ExprValue("ARRAY", a[0].value, false);
        if (!parseArrayElements(a[0].value, ea) || !parseArrayElements(a[1].value, eb))
            return ExprValue("ARRAY", "", true);
        ea.insert(ea.end(), eb.begin(), eb.end());
        std::string out = "{";
        for (size_t i = 0; i < ea.size(); ++i) { if (i) out += ","; out += ea[i]; }
        out += "}";
        return ExprValue("ARRAY", out, false);
    };
    // array_position(arr, elem) — 1-based index of first matching element, NULL if absent
    functions_["array_position"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull) return ExprValue("integer", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems)) return ExprValue("integer", "", true);
        for (size_t i = 0; i < elems.size(); ++i) {
            std::string v = arrayElemUnquote(elems[i]);
            if (!a[1].isNull && v == a[1].value)
                return ExprValue("integer", std::to_string(i + 1), false);
        }
        return ExprValue("integer", "", true);
    };
    // array_to_string(arr, delim [, null_string]) — join non-NULL elements
    functions_["array_to_string"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems)) return ExprValue("text", "", true);
        const std::string& delim = a[1].value;
        bool hasNullStr = (a.size() >= 3 && !a[2].isNull);
        std::string nullStr = hasNullStr ? a[2].value : "";
        std::string out;
        bool first = true;
        for (const auto& e : elems) {
            std::string low;
            for (char c : e) low.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
            bool isNull = (low == "null");
            if (isNull && !hasNullStr) continue;  // omit NULLs when no null_string given
            if (!first) out += delim;
            out += isNull ? nullStr : arrayElemUnquote(e);
            first = false;
        }
        return ExprValue("text", out, false);
    };
    // string_to_array(str, delim [, null_string]) — split into an array literal
    functions_["string_to_array"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull) return ExprValue("ARRAY", "", true);
        const std::string& s = a[0].value;
        bool hasNullStr = (a.size() >= 3 && !a[2].isNull);
        std::string nullStr = hasNullStr ? a[2].value : "";
        std::vector<std::string> parts;
        if (a[1].isNull) {
            // NULL delimiter: split into individual characters.
            for (char c : s) parts.push_back(std::string(1, c));
        } else {
            const std::string& delim = a[1].value;
            if (delim.empty()) { parts.push_back(s); }
            else {
                size_t pos = 0, next;
                while ((next = s.find(delim, pos)) != std::string::npos) {
                    parts.push_back(s.substr(pos, next - pos));
                    pos = next + delim.size();
                }
                parts.push_back(s.substr(pos));
            }
        }
        std::string out = "{";
        for (size_t i = 0; i < parts.size(); ++i) {
            if (i) out += ",";
            if (hasNullStr && parts[i] == nullStr) out += "NULL";
            else out += arrayElemQuote(parts[i]);
        }
        out += "}";
        return ExprValue("ARRAY", out, false);
    };

    // ------------------------------------------------------------------------
    // JSON functions (operate on JSON value text)
    // ------------------------------------------------------------------------
    auto jsonTypeofFn = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string t = jsonTypeOf(a[0].value);
        if (t.empty()) return ExprValue("text", "", true);
        return ExprValue("text", t, false);
    };
    functions_["json_typeof"] = jsonTypeofFn;
    functions_["jsonb_typeof"] = jsonTypeofFn;

    auto jsonArrayLenFn = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        std::vector<std::string> elems;
        if (!jsonTopLevelSplit(a[0].value, '[', ']', elems))
            return ExprValue("integer", "", true);  // not a JSON array
        return ExprValue("integer", std::to_string(elems.size()), false);
    };
    functions_["json_array_length"] = jsonArrayLenFn;
    functions_["jsonb_array_length"] = jsonArrayLenFn;

    // json_build_array(VARIADIC) -> compact JSON array
    auto jsonBuildArrayFn = [](const std::vector<ExprValue>& a) {
        std::string out = "[";
        for (size_t i = 0; i < a.size(); ++i) {
            if (i) out += ",";
            out += toJsonValue(a[i]);
        }
        out += "]";
        return ExprValue("json", out, false);
    };
    functions_["json_build_array"] = jsonBuildArrayFn;
    functions_["jsonb_build_array"] = jsonBuildArrayFn;

    // json_build_object(k1, v1, ...) -> compact JSON object (keys coerced to text)
    auto jsonBuildObjectFn = [](const std::vector<ExprValue>& a) {
        std::string out = "{";
        bool first = true;
        for (size_t i = 0; i + 1 < a.size(); i += 2) {
            if (!first) out += ",";
            out += jsonQuoteStr(a[i].isNull ? "" : a[i].value);
            out += ":";
            out += toJsonValue(a[i + 1]);
            first = false;
        }
        out += "}";
        return ExprValue("json", out, false);
    };
    functions_["json_build_object"] = jsonBuildObjectFn;
    functions_["jsonb_build_object"] = jsonBuildObjectFn;

    // to_json / to_jsonb -> JSON representation of the argument
    auto toJsonFn = [](const std::vector<ExprValue>& a) {
        if (a.empty()) return ExprValue("json", "null", false);
        return ExprValue("json", toJsonValue(a[0]), false);
    };
    functions_["to_json"] = toJsonFn;
    functions_["to_jsonb"] = toJsonFn;

    // json_extract_path(json, key, ...) -> the JSON sub-value at the key/index
    // path, or NULL if any step does not resolve.
    auto jsonExtractFn = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("json", "", true);
        std::string cur = a[0].value;
        for (size_t i = 1; i < a.size(); ++i) {
            if (a[i].isNull) return ExprValue("json", "", true);
            std::string next;
            if (!jsonStep(cur, a[i].value, next)) return ExprValue("json", "", true);
            cur = next;
        }
        return ExprValue("json", cur, false);
    };
    functions_["json_extract_path"] = jsonExtractFn;
    functions_["jsonb_extract_path"] = jsonExtractFn;

    // json_extract_path_text(json, key, ...) -> the resolved value as text
    // (JSON strings are unquoted; JSON null becomes SQL NULL).
    auto jsonExtractTextFn = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string cur = a[0].value;
        for (size_t i = 1; i < a.size(); ++i) {
            if (a[i].isNull) return ExprValue("text", "", true);
            std::string next;
            if (!jsonStep(cur, a[i].value, next)) return ExprValue("text", "", true);
            cur = next;
        }
        std::string t = trimStr(cur);
        if (t == "null") return ExprValue("text", "", true);
        if (t.size() >= 2 && t.front() == '"' && t.back() == '"') {
            std::string o;
            for (size_t i = 1; i + 1 < t.size(); ++i) {
                if (t[i] == '\\' && i + 2 < t.size()) o.push_back(t[++i]);
                else o.push_back(t[i]);
            }
            return ExprValue("text", o, false);
        }
        return ExprValue("text", t, false);
    };
    functions_["json_extract_path_text"] = jsonExtractTextFn;
    functions_["jsonb_extract_path_text"] = jsonExtractTextFn;

    // ------------------------------------------------------------------------
    // Regular expression functions (std::regex, ECMAScript dialect)
    // ------------------------------------------------------------------------
    // regexp_replace(source, pattern, replacement [, flags]) — 'g' = replace all
    functions_["regexp_replace"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        std::string flags = (a.size() >= 4 && !a[3].isNull) ? a[3].value : "";
        bool ok;
        std::regex re = buildRegex(a[1].value, flags, ok);
        if (!ok) return ExprValue("text", "", true);
        std::string repl = translateReplacement(a[2].value);
        auto fmtFlags = (flags.find('g') != std::string::npos)
                            ? std::regex_constants::format_default
                            : std::regex_constants::format_first_only;
        try {
            return ExprValue("text", std::regex_replace(a[0].value, re, repl, fmtFlags), false);
        } catch (...) {
            return ExprValue("text", "", true);
        }
    };
    // regexp_match(string, pattern [, flags]) — capture groups of first match as
    // a text array; whole match if the pattern has no groups; NULL if no match
    functions_["regexp_match"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("ARRAY", "", true);
        std::string flags = (a.size() >= 3 && !a[2].isNull) ? a[2].value : "";
        bool ok;
        std::regex re = buildRegex(a[1].value, flags, ok);
        if (!ok) return ExprValue("ARRAY", "", true);
        std::smatch m;
        if (!std::regex_search(a[0].value, m, re)) return ExprValue("ARRAY", "", true);  // NULL
        std::string out = "{";
        if (m.size() <= 1) {
            out += arrayElemQuote(m[0].str());
        } else {
            for (size_t i = 1; i < m.size(); ++i) {
                if (i > 1) out += ",";
                out += m[i].matched ? arrayElemQuote(m[i].str()) : "NULL";
            }
        }
        out += "}";
        return ExprValue("ARRAY", out, false);
    };
    // regexp_split_to_array(string, pattern [, flags]) -> array of the parts
    functions_["regexp_split_to_array"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("ARRAY", "", true);
        std::string flags = (a.size() >= 3 && !a[2].isNull) ? a[2].value : "";
        bool ok;
        std::regex re = buildRegex(a[1].value, flags, ok);
        if (!ok) return ExprValue("ARRAY", "", true);
        const std::string& s = a[0].value;
        std::string out = "{";
        bool first = true;
        try {
            std::sregex_token_iterator it(s.begin(), s.end(), re, -1), end;
            for (; it != end; ++it) {
                if (!first) out += ",";
                out += arrayElemQuote(*it);
                first = false;
            }
        } catch (...) {
            return ExprValue("ARRAY", "", true);
        }
        out += "}";
        return ExprValue("ARRAY", out, false);
    };
    // regexp_count(string, pattern [, start [, flags]]) -> number of matches
    functions_["regexp_count"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("integer", "", true);
        std::string flags = (a.size() >= 4 && !a[3].isNull) ? a[3].value : "";
        bool ok;
        std::regex re = buildRegex(a[1].value, flags, ok);
        if (!ok) return ExprValue("integer", "", true);
        const std::string& s = a[0].value;
        size_t start = 0;
        if (a.size() >= 3 && !a[2].isNull) {
            int64_t st = a[2].asInt();
            if (st > 1) start = std::min(static_cast<size_t>(st - 1), s.size());
        }
        std::string sub = s.substr(start);
        try {
            auto b = std::sregex_iterator(sub.begin(), sub.end(), re);
            auto e = std::sregex_iterator();
            return ExprValue("integer", std::to_string(std::distance(b, e)), false);
        } catch (...) {
            return ExprValue("integer", "", true);
        }
    };
    // regexp_substr(string, pattern [, start [, N [, flags]]]) -> N-th match substring
    functions_["regexp_substr"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        std::string flags = (a.size() >= 5 && !a[4].isNull) ? a[4].value : "";
        bool ok;
        std::regex re = buildRegex(a[1].value, flags, ok);
        if (!ok) return ExprValue("text", "", true);
        const std::string& s = a[0].value;
        size_t start = 0;
        if (a.size() >= 3 && !a[2].isNull) {
            int64_t st = a[2].asInt();
            if (st > 1) start = std::min(static_cast<size_t>(st - 1), s.size());
        }
        int64_t which = (a.size() >= 4 && !a[3].isNull) ? a[3].asInt() : 1;
        if (which < 1) which = 1;
        std::string sub = s.substr(start);
        try {
            auto it = std::sregex_iterator(sub.begin(), sub.end(), re);
            auto e = std::sregex_iterator();
            int64_t idx = 1;
            for (; it != e; ++it, ++idx)
                if (idx == which) return ExprValue("text", it->str(), false);
        } catch (...) {
            return ExprValue("text", "", true);
        }
        return ExprValue("text", "", true);  // no match -> NULL
    };

    // ------------------------------------------------------------------------
    // Range functions (operate on range literal text '[lo,hi)' / 'empty')
    // ------------------------------------------------------------------------
    functions_["isempty"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("boolean", "", true);
        RangeParts r = parseRangeLiteral(a[0].value);
        if (!r.valid) return ExprValue("boolean", "", true);
        return ExprValue("boolean", r.empty ? "t" : "f", false);
    };
    functions_["lower_inc"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("boolean", "", true);
        RangeParts r = parseRangeLiteral(a[0].value);
        if (!r.valid) return ExprValue("boolean", "", true);
        bool inc = !r.empty && !r.loInf && r.loInc;
        return ExprValue("boolean", inc ? "t" : "f", false);
    };
    functions_["upper_inc"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("boolean", "", true);
        RangeParts r = parseRangeLiteral(a[0].value);
        if (!r.valid) return ExprValue("boolean", "", true);
        bool inc = !r.empty && !r.hiInf && r.hiInc;
        return ExprValue("boolean", inc ? "t" : "f", false);
    };
    functions_["lower_inf"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("boolean", "", true);
        RangeParts r = parseRangeLiteral(a[0].value);
        if (!r.valid) return ExprValue("boolean", "", true);
        return ExprValue("boolean", (!r.empty && r.loInf) ? "t" : "f", false);
    };
    functions_["upper_inf"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("boolean", "", true);
        RangeParts r = parseRangeLiteral(a[0].value);
        if (!r.valid) return ExprValue("boolean", "", true);
        return ExprValue("boolean", (!r.empty && r.hiInf) ? "t" : "f", false);
    };

    // ------------------------------------------------------------------------
    // Date/time functions
    // ------------------------------------------------------------------------
    functions_["current_date"] = [](const std::vector<ExprValue>&) {
        return ExprValue("date", "2026-06-20", false);
    };
    // Reference "current" timestamps — fixed within a session like now(), so
    // expression evaluation stays deterministic.
    functions_["current_timestamp"] = [](const std::vector<ExprValue>&) {
        return ExprValue("timestamp with time zone", "2026-06-20 12:00:00", false);
    };
    functions_["localtimestamp"] = [](const std::vector<ExprValue>&) {
        return ExprValue("timestamp", "2026-06-20 12:00:00", false);
    };
    functions_["transaction_timestamp"] = [](const std::vector<ExprValue>&) {
        return ExprValue("timestamp with time zone", "2026-06-20 12:00:00", false);
    };
    functions_["statement_timestamp"] = [](const std::vector<ExprValue>&) {
        return ExprValue("timestamp with time zone", "2026-06-20 12:00:00", false);
    };
    functions_["clock_timestamp"] = [](const std::vector<ExprValue>&) {
        return ExprValue("timestamp with time zone", "2026-06-20 12:00:00", false);
    };
    functions_["current_time"] = [](const std::vector<ExprValue>&) {
        return ExprValue("time with time zone", "12:00:00", false);
    };
    functions_["localtime"] = [](const std::vector<ExprValue>&) {
        return ExprValue("time", "12:00:00", false);
    };

    // Shared field extractor for extract() / date_part(); src is an ISO date or
    // timestamp 'YYYY-MM-DD[ HH:MM:SS]'.
    auto extractImpl = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("numeric", "", true);
        std::string field = toLower(a[0].value);
        const std::string& src = a[1].value;
        auto num = [&](size_t off, size_t len) -> int {
            if (src.size() < off + len) return 0;
            int v = 0;
            for (size_t i = off; i < off + len; ++i) {
                char c = src[i];
                if (c < '0' || c > '9') return 0;
                v = v * 10 + (c - '0');
            }
            return v;
        };
        int y = num(0, 4), mo = num(5, 2), d = num(8, 2);
        int h = num(11, 2), mi = num(14, 2), se = num(17, 2);
        int64_t r = 0;
        if (field == "year") r = y;
        else if (field == "month") r = mo;
        else if (field == "day") r = d;
        else if (field == "hour") r = h;
        else if (field == "minute") r = mi;
        else if (field == "second") r = se;
        else if (field == "quarter") r = mo > 0 ? (mo - 1) / 3 + 1 : 0;
        else if (field == "decade") r = y / 10;
        else if (field == "century") r = y > 0 ? (y - 1) / 100 + 1 : 0;
        else if (field == "millennium") r = y > 0 ? (y - 1) / 1000 + 1 : 0;
        else if (field == "dow" || field == "isodow") {
            // Sakamoto's algorithm: w = 0 (Sunday) .. 6 (Saturday)
            static const int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
            int yy = y - (mo < 3 ? 1 : 0);
            int w = (yy + yy / 4 - yy / 100 + yy / 400 + t[(mo > 0 ? mo : 1) - 1] + d) % 7;
            if (field == "isodow") r = (w == 0) ? 7 : w;  // 1=Mon .. 7=Sun
            else r = w;                                    // 0=Sun .. 6=Sat
        } else if (field == "doy") {
            Date cur(y, mo, d), jan1(y, 1, 1);
            r = (cur.year != 0 && jan1.year != 0) ? cur.convert() - jan1.convert() + 1 : 0;
        } else if (field == "epoch") {
            // Seconds since 1970-01-01 00:00:00.
            r = parseTimestampToSeconds(src) - parseTimestampToSeconds("1970-01-01 00:00:00");
        } else {
            return ExprValue("numeric", "", true);
        }
        return ExprValue("numeric", std::to_string(r), false);
    };
    functions_["extract"] = extractImpl;
    functions_["date_part"] = extractImpl;

    // make_date(y, m, d) -> 'YYYY-MM-DD'
    functions_["make_date"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("date", "", true);
        int y = static_cast<int>(a[0].asInt());
        int m = static_cast<int>(a[1].asInt());
        int d = static_cast<int>(a[2].asInt());
        if (m < 1 || m > 12 || d < 1 || d > 31) return ExprValue("date", "", true);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
        return ExprValue("date", buf, false);
    };
    // make_time(h, m, s) -> 'HH:MM:SS'
    functions_["make_time"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("time", "", true);
        int h = static_cast<int>(a[0].asInt());
        int m = static_cast<int>(a[1].asInt());
        int s = static_cast<int>(a[2].asDouble());
        if (h < 0 || h > 23 || m < 0 || m > 59 || s < 0 || s > 59)
            return ExprValue("time", "", true);
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h, m, s);
        return ExprValue("time", buf, false);
    };
    // make_timestamp(y, m, d, h, mi, s) -> 'YYYY-MM-DD HH:MM:SS'
    functions_["make_timestamp"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 6) return ExprValue("timestamp", "", true);
        for (int i = 0; i < 6; ++i) if (a[i].isNull) return ExprValue("timestamp", "", true);
        int y = static_cast<int>(a[0].asInt());
        int mo = static_cast<int>(a[1].asInt());
        int d = static_cast<int>(a[2].asInt());
        int h = static_cast<int>(a[3].asInt());
        int mi = static_cast<int>(a[4].asInt());
        int s = static_cast<int>(a[5].asDouble());
        if (mo < 1 || mo > 12 || d < 1 || d > 31 || h < 0 || h > 23 ||
            mi < 0 || mi > 59 || s < 0 || s > 59)
            return ExprValue("timestamp", "", true);
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, s);
        return ExprValue("timestamp", buf, false);
    };
    // date_trunc(field, source) -> truncate timestamp to the given precision
    functions_["date_trunc"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("timestamp", "", true);
        std::string field = toLower(a[0].value);
        const std::string& src = a[1].value;
        auto num = [&](size_t off, size_t len) -> int {
            if (src.size() < off + len) return 0;
            int v = 0;
            for (size_t i = off; i < off + len; ++i) {
                char c = src[i];
                if (c < '0' || c > '9') return 0;
                v = v * 10 + (c - '0');
            }
            return v;
        };
        int y = num(0, 4), mo = num(5, 2), d = num(8, 2);
        int h = num(11, 2), mi = num(14, 2), se = num(17, 2);
        if (field == "year") { mo = 1; d = 1; h = mi = se = 0; }
        else if (field == "quarter") { mo = mo > 0 ? (mo - 1) / 3 * 3 + 1 : 1; d = 1; h = mi = se = 0; }
        else if (field == "month") { d = 1; h = mi = se = 0; }
        else if (field == "day") { h = mi = se = 0; }
        else if (field == "hour") { mi = se = 0; }
        else if (field == "minute") { se = 0; }
        else if (field == "second") { /* keep */ }
        else return ExprValue("timestamp", "", true);
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, se);
        return ExprValue("timestamp", buf, false);
    };
    // to_char(value, fmt): format a date/timestamp/time or number as text. The
    // input is treated as temporal when its declared type is date/time/timestamp
    // or its value looks like 'YYYY-MM-DD...' / 'HH:MM:SS'; otherwise numeric.
    functions_["to_char"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        const std::string& v = a[0].value;
        const std::string& fmt = a[1].value;
        std::string tn = toLower(a[0].typeName);
        bool temporal = tn.find("date") != std::string::npos ||
                        tn.find("timestamp") != std::string::npos ||
                        tn.find("time") != std::string::npos;
        if (!temporal) {
            bool looksDate = v.size() >= 10 && v[4] == '-' && v[7] == '-' &&
                             std::isdigit(static_cast<unsigned char>(v[0]));
            bool looksTime = v.size() >= 5 && v[2] == ':' &&
                             std::isdigit(static_cast<unsigned char>(v[0]));
            temporal = looksDate || looksTime;
        }
        if (temporal) return ExprValue("text", formatDateTime(v, fmt), false);
        return ExprValue("text", formatNumeric(a[0].asDouble(), fmt), false);
    };
}

} // namespace dbms
