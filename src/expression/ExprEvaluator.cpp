#include "ExprEvaluator.h"
#include "common/DateType.h"

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
        return ExprValue("numeric", std::to_string(v.asDouble()), false);
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
}

} // namespace dbms
