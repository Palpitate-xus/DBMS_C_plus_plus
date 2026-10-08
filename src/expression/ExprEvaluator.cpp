#include "ExprEvaluator.h"
#include "arithmetic_type.h"
#include "equality_type.h"
#include "expr_helper.h"
#include "SqlPattern.h"
#include "function_namespace.h"
#include "common/QueryHostProvider.h"
#include "utils/interval.h"
#include "commands/TableManage.h"
#include "catalog/collation.h"
#include "catalog/catalog.h"
#include "common/DateType.h"
#include "common/BooleanCodec.h"
#include "common/NetworkValue.h"
#include "common/GeometryValue.h"
#include "expression/geometric_input.h"
#include "expression/between_input.h"
#include "common/DbError.h"
#include "common/SqlArrayText.h"
#include "common/NotificationManager.h"
#include "common/sha256.h"
#include "common/sha2_extended.h"
#include "types/numeric.h"
#include "types/money.h"
#include "types/uuid.h"
#include "types/bytea.h"
#include "types/encoding_conversion.h"
#include "types/xml.h"
#include "utils/Session.h"
#include "common/TimeZoneRules.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <locale>
#include <regex>
#include <set>
#include <sstream>
#include <tuple>

// Global engine reference used by sequence builtins.
extern dbms::StorageEngine g_engine;

namespace dbms {
static bool parseArrayElements(const std::string&, std::vector<std::string>&);
static std::string arrayElemUnquote(const std::string&);
static float parseRealCastValue(const ExprValue& value);
static double parseDoubleCastValue(const ExprValue& value);
template <typename Floating>
static std::string formatFloatingCastValue(Floating value);

// ============================================================================
// ExprValue helpers
// ============================================================================

static std::string toLower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

static int compareTextWithCollation(const std::string& left,
                                    const std::string& right,
                                    const std::string& requested) {
    const std::string effective = requested.empty()
        ? "en_US.utf8" : requested;
    const int compared = collation::compare(left, right, effective);
    if (effective == "nocase") return compared;
    // PostgreSQL's deterministic libc collation distinguishes byte-distinct
    // strings even when the locale reports equal collation weights.
    return compared == 0 ? left.compare(right) : compared;
}

static bool isCollatableExprType(const std::string& typeName) {
    const std::string type = toLower(typeName);
    return type == "text" || type == "unknown" || type == "varchar" ||
           type == "character varying" || type == "char" ||
           type == "character" || type == "bpchar";
}

static bool isCollatableCastTarget(std::string typeName) {
    typeName = toLower(std::move(typeName));
    const size_t modifier = typeName.find('(');
    if (modifier != std::string::npos) typeName.resize(modifier);
    while (!typeName.empty() &&
           std::isspace(static_cast<unsigned char>(typeName.back())))
        typeName.pop_back();
    return isCollatableExprType(typeName);
}

static bool isTextResultBuiltin(const std::string& name) {
    static const std::set<std::string> textBuiltins = {
        "lower", "upper", "substring", "substr", "ltrim", "rtrim",
        "btrim", "replace", "left", "right", "repeat", "reverse",
        "concat", "concat_ws", "initcap", "translate", "overlay",
        "lpad", "rpad", "split_part", "trim"
    };
    return textBuiltins.count(name) != 0;
}

static std::string mergeExplicitCollations(const std::string& left,
                                           const std::string& right) {
    const std::string a = collation::normalizeName(left);
    const std::string b = collation::normalizeName(right);
    if (!a.empty() && !b.empty() && a != b)
        throw DbError("42P21", "collation mismatch between explicit collations");
    return a.empty() ? b : a;
}

// CASE and COALESCE choose a value lazily, but PostgreSQL resolves their
// result collation from every result arm before executing any arm.  Inspect
// only the result-bearing AST nodes here; evaluating an unused arm would
// violate their short-circuit and side-effect behavior.
static std::string explicitResultCollation(const Expr* expression,
                                           bool validateName = true) {
    if (!expression) return {};
    switch (expression->type) {
        case ExprType::UnaryOp: {
            const auto* unary = static_cast<const UnaryOpExpr*>(expression);
            if (toLower(unary->op).rfind("collate ", 0) == 0) {
                const std::string name =
                    collation::normalizeName(unary->op.substr(8));
                if (validateName && !collation::isValid(name))
                    throw DbError("42704", "collation does not exist: " + name);
                return name;
            }
            return {};
        }
        case ExprType::CastExpr: {
            const auto* cast = static_cast<const CastExpr*>(expression);
            std::string target=cast->typeName;
            if(target.size()>=2 && target.compare(target.size()-2,2,"[]")==0)target.resize(target.size()-2);
            return isCollatableCastTarget(target)
                ? explicitResultCollation(cast->operand.get(), validateName)
                : std::string{};
        }
        case ExprType::ArrayExpr: {
            std::string result;
            for(const auto& item:static_cast<const ArrayExpr*>(expression)->elements)
                result=mergeExplicitCollations(result,explicitResultCollation(item.get(),validateName));
            return result;
        }
        case ExprType::Literal: {
            // A SQL sublink carries its actual prepared child, not a scalar
            // datum or a token whose spelling can supply a collation.
            const auto* select=dynamic_cast<const SelectStmt*>(expression->preparedSubquery.get());
            if(!select || select->selectList.size()!=1)return {};
            return explicitResultCollation(select->selectList.front().expr.get(),validateName);
        }
        case ExprType::BinaryOp: {
            const auto* binary = static_cast<const BinaryOpExpr*>(expression);
            if (binary->op == "||")
                return mergeExplicitCollations(
                    explicitResultCollation(binary->left.get(), validateName),
                    explicitResultCollation(binary->right.get(), validateName));
            if (binary->op == "::") {
                const auto* target = binary->right &&
                    binary->right->type == ExprType::Literal
                    ? static_cast<const LiteralExpr*>(binary->right.get())
                    : nullptr;
                return target && isCollatableCastTarget(target->value)
                    ? explicitResultCollation(binary->left.get(), validateName)
                    : std::string{};
            }
            return {};
        }
        case ExprType::CaseExpr: {
            const auto* conditional = static_cast<const CaseExpr*>(expression);
            std::string result;
            for (const auto& arm : conditional->whenClauses)
                result = mergeExplicitCollations(
                    result, explicitResultCollation(arm.second.get(),
                                                    validateName));
            return mergeExplicitCollations(
                result, explicitResultCollation(
                    conditional->elseExpr.get(), validateName));
        }
        case ExprType::FunctionCall: {
            const auto* function = static_cast<const FunctionCallExpr*>(expression);
            const std::string name = toLower(function->funcName);
            if (name != "coalesce" && name != "greatest" &&
                name != "least" &&
                !((function->schema.empty() ||
                   toLower(function->schema) == "pg_catalog") &&
                  isTextResultBuiltin(name))) return {};
            std::string result;
            for (const auto& argument : function->args)
                result = mergeExplicitCollations(
                    result, explicitResultCollation(argument.get(),
                                                    validateName));
            return result;
        }
        default:
            return {};
    }
}

std::string ExprEvaluator::analyzeExplicitResultCollation(const Expr* expr) {
    // The storage layer resolves built-in and user-defined collation names
    // against the current database after this syntax-only conflict analysis.
    return explicitResultCollation(expr, false);
}

static std::string formatUtcClock(std::time_t value, const char* format) {
    std::tm utc{};
    if (::gmtime_r(&value, &utc) == nullptr) return "";
    char buffer[40];
    if (std::strftime(buffer, sizeof(buffer), format, &utc) == 0) return "";
    return buffer;
}

// Exact decimal types (PG rounds these half-up); float types take the
// double path (PG rounds them half-to-even).
static bool isNumericTypeName(const std::string& s) {
    std::string t = toLower(s);
    return t == "numeric" || t == "decimal" || t == "integer" ||
           t == "int" || t == "bigint" || t == "smallint";
}

static bool isIntegerTypeName(const std::string& s) {
    const std::string type = toLower(s);
    return type == "integer" || type == "int" || type == "int2" ||
           type == "int4" || type == "int8" || type == "bigint" ||
           type == "smallint";
}

static std::optional<Numeric> tryParseNumeric(const std::string& s) {
    try {
        return Numeric(s);
    } catch (...) {
        return std::nullopt;
    }
}

static Numeric numericTruncatedQuotient(const Numeric& dividend,
                                        const Numeric& divisor) {
    const Numeric rounded = dividend / divisor;
    if (!rounded.isFinite()) return rounded;
    std::string integral = rounded.toString();
    const size_t point = integral.find('.');
    if (point != std::string::npos) integral.resize(point);
    if (integral.empty() || integral == "-") integral += '0';
    Numeric quotient(integral);
    // Division rounds at its selected scale, possibly all the way to the
    // next integer. Dropping fractional digits alone cannot undo that carry.
    // Correct the at-most-one-unit overshoot using exact decimal products.
    const Numeric product = quotient * divisor;
    const Numeric productMagnitude = product.sign() < 0 ? -product : product;
    const Numeric dividendMagnitude = dividend.sign() < 0 ? -dividend : dividend;
    if (productMagnitude > dividendMagnitude)
        quotient -= Numeric(static_cast<int64_t>(quotient.sign()));
    return quotient;
}

static std::optional<Money> tryParseMoney(const std::string& value,
                                          bool decimalInput = false) {
    Money money;
    const std::string locale = StorageEngine::getMoneyLocale();
    const bool parsed = decimalInput
        ? Money::parseDecimal(value, money, locale)
        : Money::parse(value, money, locale);
    if (!parsed) return std::nullopt;
    return money;
}

static std::optional<UuidValue> tryParseUuid(const std::string& value) {
    UuidValue uuid;
    if (!UuidValue::parse(value, uuid)) return std::nullopt;
    return uuid;
}

static bool isCanonicalByteaType(const std::string& typeName) {
    const std::string type = toLower(typeName);
    return type == "bytea" || type == "blob";
}

static ByteaValue parseByteaOrThrow(const ExprValue& value) {
    ByteaValue bytes;
    if (!ByteaValue::parse(value.value, bytes)) {
        throw std::runtime_error(
            "invalid input syntax for type bytea (SQLSTATE 22P02)");
    }
    return bytes;
}

static BuiltinEncoding parseEncodingOrThrow(const ExprValue& value) {
    BuiltinEncoding encoding = BuiltinEncoding::Utf8;
    if (!parseBuiltinEncoding(value.value, encoding)) {
        throw std::runtime_error(
            "invalid encoding name: \"" + value.value +
            "\" (SQLSTATE 22023)");
    }
    return encoding;
}

static std::string convertEncodingOrThrow(const std::string& input,
                                          BuiltinEncoding source,
                                          BuiltinEncoding destination,
                                          size_t* characterCount = nullptr) {
    std::string output;
    if (!convertBuiltinEncoding(
            input, source, destination, output, characterCount)) {
        throw std::runtime_error(
            "character not in repertoire or invalid byte sequence "
            "(SQLSTATE 22021)");
    }
    return output;
}

template <size_t Size>
static ExprValue byteaDigestValue(const std::array<uint8_t, Size>& digest) {
    return ExprValue(
        "bytea",
        ByteaValue::fromBytes(std::string(
            reinterpret_cast<const char*>(digest.data()),
            digest.size())).toString(),
        false);
}

static std::string normalizeDecimalMagnitude(std::string value) {
    const size_t first = value.find_first_not_of('0');
    if (first == std::string::npos) return "0";
    value.erase(0, first);
    return value;
}

static int compareDecimalMagnitudes(const std::string& left,
                                    const std::string& right) {
    if (left.size() != right.size())
        return left.size() < right.size() ? -1 : 1;
    if (left == right) return 0;
    return left < right ? -1 : 1;
}

// Subtract two normalized unsigned decimal integers, with left >= right.
static std::string subtractDecimalMagnitudes(const std::string& left,
                                             const std::string& right) {
    std::string result(left.size(), '0');
    int borrow = 0;
    size_t rightIndex = right.size();
    for (size_t i = left.size(); i > 0; --i) {
        int digit = left[i - 1] - '0' - borrow;
        const int subtrahend = rightIndex > 0
            ? right[--rightIndex] - '0' : 0;
        if (digit < subtrahend) {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result[i - 1] = static_cast<char>('0' + digit - subtrahend);
    }
    return normalizeDecimalMagnitude(std::move(result));
}

static std::pair<std::string, std::string> divideDecimalMagnitudes(
    const std::string& dividend, const std::string& divisor) {
    std::string quotient;
    quotient.reserve(dividend.size());
    std::string remainder = "0";
    for (char digit : dividend) {
        if (remainder == "0") remainder.assign(1, digit);
        else remainder.push_back(digit);
        remainder = normalizeDecimalMagnitude(std::move(remainder));

        int quotientDigit = 0;
        while (compareDecimalMagnitudes(remainder, divisor) >= 0) {
            remainder = subtractDecimalMagnitudes(remainder, divisor);
            ++quotientDigit;
        }
        quotient.push_back(static_cast<char>('0' + quotientDigit));
    }
    return {normalizeDecimalMagnitude(std::move(quotient)), remainder};
}

static std::string gcdDecimalMagnitudes(std::string left,
                                        std::string right) {
    while (right != "0") {
        std::string remainder =
            divideDecimalMagnitudes(left, right).second;
        left = std::move(right);
        right = std::move(remainder);
    }
    return left;
}

static std::string multiplyDecimalMagnitudes(const std::string& left,
                                             const std::string& right) {
    if (left == "0" || right == "0") return "0";
    std::vector<int> digits(left.size() + right.size(), 0);
    for (size_t i = left.size(); i > 0; --i) {
        for (size_t j = right.size(); j > 0; --j) {
            digits[i + j - 1] +=
                (left[i - 1] - '0') * (right[j - 1] - '0');
        }
    }
    for (size_t i = digits.size(); i > 1; --i) {
        digits[i - 2] += digits[i - 1] / 10;
        digits[i - 1] %= 10;
    }
    std::string result;
    result.reserve(digits.size());
    for (int digit : digits)
        result.push_back(static_cast<char>('0' + digit));
    return normalizeDecimalMagnitude(std::move(result));
}

static int numericTextScale(const std::string& input,
                            const Numeric& parsed) {
    if (parsed.sign() != 0) return parsed.scale();

    size_t begin = 0;
    while (begin < input.size() &&
           std::isspace(static_cast<unsigned char>(input[begin]))) {
        ++begin;
    }
    size_t end = input.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    const size_t exponentPosition = input.find_first_of("eE", begin);
    const size_t mantissaEnd = exponentPosition == std::string::npos ||
            exponentPosition >= end
        ? end : exponentPosition;
    const size_t decimalPoint = input.find('.', begin);
    int64_t fractionalDigits = 0;
    if (decimalPoint != std::string::npos && decimalPoint < mantissaEnd) {
        fractionalDigits = static_cast<int64_t>(
            mantissaEnd - decimalPoint - 1);
    }
    int64_t exponent = 0;
    if (exponentPosition != std::string::npos &&
        exponentPosition + 1 < end) {
        try {
            exponent = std::stoll(input.substr(
                exponentPosition + 1, end - exponentPosition - 1));
        } catch (...) {
            return parsed.scale();
        }
    }
    const int64_t scale = std::max<int64_t>(0, fractionalDigits - exponent);
    return static_cast<int>(std::min<int64_t>(scale, Numeric::kMaxPrecision));
}

static std::string numericMagnitudeAtScale(const Numeric& value,
                                           int targetScale) {
    std::string text = value.toString();
    if (!text.empty() && (text.front() == '-' || text.front() == '+'))
        text.erase(text.begin());
    text.erase(std::remove(text.begin(), text.end(), '.'), text.end());
    text = normalizeDecimalMagnitude(std::move(text));
    if (text != "0" && targetScale > value.scale())
        text.append(static_cast<size_t>(targetScale - value.scale()), '0');
    return text;
}

static std::string formatScaledDecimalMagnitude(std::string digits,
                                                int scale) {
    digits = normalizeDecimalMagnitude(std::move(digits));
    if (scale <= 0) return digits;
    const size_t fractionalDigits = static_cast<size_t>(scale);
    if (digits.size() <= fractionalDigits) {
        return "0." + std::string(fractionalDigits - digits.size(), '0') +
               digits;
    }
    digits.insert(digits.size() - fractionalDigits, 1, '.');
    return digits;
}

bool ExprValue::asBool() const {
    if (isNull) return false;
    const auto parsed = parsePostgresBoolean(value);
    return parsed.value_or(false);
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

const ExprValue& RowContext::parameter(size_t slot) const {
    if (slot >= parameters_.size())
        throw DbError("42P02", "there is no parameter $" + std::to_string(slot + 1));
    return parameters_[slot];
}

const ExprValue& RowContext::boundColumn(size_t sourceOrdinal, size_t columnOrdinal) const {
    const auto found = boundColumns_.find({sourceOrdinal, columnOrdinal});
    if (found == boundColumns_.end())
        throw DbError("XX000", "prepared source column has no runtime cell");
    return found->second;
}

// ============================================================================
// ExprEvaluator
// ============================================================================

ExprEvaluator::ExprEvaluator() {
    registerBuiltins();
}

ExprValue ExprEvaluator::eval(const Expr* expr, const RowContext& ctx) const {
    if (!expr) return ExprValue{};
    if (expr->preparedSubquery) {
        if (!scalarSubqueryExecutor_)
            throw DbError("0A000", "prepared subqueries require a query execution context");
        return scalarSubqueryExecutor_(expr, ctx);
    }
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
        case ExprType::Parameter:    return ctx.parameter(static_cast<const ParameterExpr*>(expr)->slot);
        case ExprType::QuantifiedComparison: return evalQuantified(static_cast<const QuantifiedComparisonExpr*>(expr),ctx);
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

static constexpr size_t kMaxSupportedBitStringLength = 8388608;

static bool isBitStringTypeName(const std::string& typeName) {
    const std::string lowered = toLower(typeName);
    return lowered == "bit" || lowered == "bit varying" ||
           lowered == "varbit";
}

static bool isInetTypeName(const std::string& typeName) {
    const std::string type = toLower(typeName);
    return type == "inet" || type == "cidr";
}

static bool decodeBitStringInput(const std::string& input,
                                 std::string& bits) {
    const bool hexadecimal = !input.empty() && (input.front() == 'x' || input.front() == 'X');
    const bool prefixed = hexadecimal || (!input.empty() && (input.front() == 'b' || input.front() == 'B'));
    const std::string body = input.substr(prefixed ? 1 : 0);
    bits.clear();
    if (!hexadecimal) {
        if (body.size() > kMaxSupportedBitStringLength) return false;
        for (char bit : body) {
            if (bit != '0' && bit != '1') return false;
        }
        bits = body;
        return true;
    }
    if (body.size() > kMaxSupportedBitStringLength / 4) return false;
    bits.reserve(body.size() * 4);
    for (char digit : body) {
        unsigned value = 0;
        if (digit >= '0' && digit <= '9') value = digit - '0';
        else if (digit >= 'a' && digit <= 'f') value = digit - 'a' + 10;
        else if (digit >= 'A' && digit <= 'F') value = digit - 'A' + 10;
        else return false;
        for (int shift = 3; shift >= 0; --shift) {
            bits.push_back((value & (1U << shift)) ? '1' : '0');
        }
    }
    return true;
}

static bool decodeBitStringLiteral(const std::string& input,
                                   std::string& bits) {
    if (input.size() < 3 || input[1] != '\'' || input.back() != '\'' ||
        (input[0] != 'B' && input[0] != 'b' &&
         input[0] != 'X' && input[0] != 'x')) return false;
    return decodeBitStringInput(input.substr(0,1) + input.substr(2,input.size()-3),bits);
}

static std::string unquote(const std::string& s) {
    // SQL string literal: strip the outer quotes and collapse doubled quotes.
    if (isQuotedString(s)) {
        std::string inner = s.substr(1, s.size() - 2);
        std::string out;
        for (size_t i = 0; i < inner.size(); ++i) {
            out += inner[i];
            if (inner[i] == '\'' && i + 1 < inner.size() && inner[i + 1] == '\'') ++i;
        }
        return out;
    }
    return s;
}


static std::string trimStr(const std::string& s);
static size_t utf8CharCount(const std::string& s);
static size_t utf8ByteAt(const std::string& s, size_t charIdx);
static bool isBlankPaddedCharacterType(const std::string& typeName);
static size_t logicalCharacterByteLength(const ExprValue& value);

// ----------------------------------------------------------------------------
// Interval support
//
// Canonical text form (PostgreSQL "postgres" style, produced by the
// type layer):  [N years] [N mons] [N days] [HH:MM:SS[.ffffff]]
// Internally an interval is (months, days, microseconds) — months and days
// are applied calendar-wise, microseconds absolutely, exactly like PG.
// ----------------------------------------------------------------------------

using IntervalParts = IntervalInputParts;

static bool combineIntervalField(long long left, long long right,
                                 bool subtract, long long& result,
                                 bool calendarField = false) {
    const __int128 total = static_cast<__int128>(left) +
        (subtract ? -static_cast<__int128>(right)
                  : static_cast<__int128>(right));
    const __int128 lower = calendarField ? std::numeric_limits<int32_t>::lowest()
        : std::numeric_limits<int64_t>::lowest();
    const __int128 upper = calendarField ? std::numeric_limits<int32_t>::max()
        : std::numeric_limits<int64_t>::max();
    if (total < lower || total > upper) {
        return false;
    }
    result = static_cast<long long>(total);
    return true;
}

static bool addScaledIntervalField(long long& target, long long value,
                                   long long scale) {
    const __int128 total = static_cast<__int128>(target) +
        static_cast<__int128>(value) * scale;
    if (total <= std::numeric_limits<long long>::lowest() ||
        total > std::numeric_limits<long long>::max()) {
        return false;
    }
    target = static_cast<long long>(total);
    return true;
}

static bool addIntervalSeconds(long long& target,
                               const std::string& secondsText) {
    long double seconds = 0;
    try {
        size_t consumed = 0;
        seconds = std::stold(secondsText, &consumed);
        if (consumed != secondsText.size() || !std::isfinite(seconds))
            return false;
    } catch (...) {
        return false;
    }

    const long double roundedMicros =
        std::nearbyint(seconds * 1000000.0L);
    if (!std::isfinite(roundedMicros) ||
        roundedMicros <= static_cast<long double>(
                             std::numeric_limits<long long>::lowest()) ||
        roundedMicros > static_cast<long double>(
                            std::numeric_limits<long long>::max())) {
        return false;
    }
    return addScaledIntervalField(
        target, static_cast<long long>(roundedMicros), 1);
}

static std::string formatTimeFields(long long hours, long long minutes,
                                    const std::string& secondsText,
                                    int* dayCarry = nullptr) {
    if (dayCarry) *dayCarry = 0;
    if (hours < 0 || hours > 24 || minutes < 0 || minutes > 59)
        return "";

    long double seconds = 0;
    try {
        size_t consumed = 0;
        seconds = std::stold(secondsText, &consumed);
        if (consumed != secondsText.size() || !std::isfinite(seconds) ||
            seconds < 0 || seconds > 60) {
            return "";
        }
    } catch (...) {
        return "";
    }

    const long long secondMicros =
        static_cast<long long>(
            std::nearbyint(seconds * 1000000.0L));
    if (secondMicros < 0 || secondMicros > 60000000LL) return "";

    constexpr long long microsPerSecond = 1000000LL;
    constexpr long long microsPerMinute = 60 * microsPerSecond;
    constexpr long long microsPerHour = 60 * microsPerMinute;
    constexpr long long microsPerDay = 24 * microsPerHour;
    long long totalMicros = hours * microsPerHour +
                            minutes * microsPerMinute + secondMicros;
    if (totalMicros > microsPerDay) return "";
    if (dayCarry && totalMicros == microsPerDay) {
        *dayCarry = 1;
        totalMicros = 0;
    }

    const long long outputHours = totalMicros / microsPerHour;
    const long long outputMinutes =
        (totalMicros % microsPerHour) / microsPerMinute;
    const long long wholeSeconds =
        (totalMicros % microsPerMinute) / microsPerSecond;
    const long long fraction = totalMicros % microsPerSecond;
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%02lld:%02lld:%02lld",
                  outputHours, outputMinutes, wholeSeconds);
    std::string result = buffer;
    if (fraction != 0) {
        std::string digits = std::to_string(fraction);
        digits.insert(digits.begin(), 6 - digits.size(), '0');
        while (!digits.empty() && digits.back() == '0') digits.pop_back();
        result += "." + digits;
    }
    return result;
}

struct ExtractTimeParts {
    int hour = 0;
    int minute = 0;
    int64_t secondMicros = 0;
    int offsetMinutes = 0;
};

static std::optional<ExtractTimeParts> parseTimeForExtract(
    const std::string& input) {
    std::string text = trimStr(input);
    if (text.empty()) return std::nullopt;

    size_t zonePosition = std::string::npos;
    for (size_t i = 1; i < text.size(); ++i) {
        if (text[i] == '+' || text[i] == '-') {
            zonePosition = i;
            break;
        }
    }
    const bool hasZulu = !text.empty() &&
        (text.back() == 'Z' || text.back() == 'z');
    if (hasZulu && zonePosition != std::string::npos)
        return std::nullopt;

    std::string zone;
    if (hasZulu) {
        zone = text.substr(text.size() - 1);
        text.pop_back();
    } else if (zonePosition != std::string::npos) {
        zone = text.substr(zonePosition);
        text.resize(zonePosition);
    }

    int offsetMinutes = 0;
    if (!zone.empty() && zone != "Z" && zone != "z") {
        const int sign = zone.front() == '-' ? -1 : 1;
        const std::string displacement = zone.substr(1);
        const size_t colon = displacement.find(':');
        std::string hourText;
        std::string minuteText;
        if (colon != std::string::npos) {
            if (colon < 1 || colon > 2 ||
                displacement.find(':', colon + 1) != std::string::npos ||
                displacement.size() - colon - 1 != 2) {
                return std::nullopt;
            }
            hourText = displacement.substr(0, colon);
            minuteText = displacement.substr(colon + 1);
        } else if (displacement.size() == 4) {
            hourText = displacement.substr(0, 2);
            minuteText = displacement.substr(2);
        } else if (displacement.size() >= 1 &&
                   displacement.size() <= 2) {
            hourText = displacement;
            minuteText = "0";
        } else {
            return std::nullopt;
        }
        auto parseUnsigned = [](const std::string& value, int& result) {
            if (value.empty()) return false;
            result = 0;
            for (const unsigned char c : value) {
                if (!std::isdigit(c)) return false;
                result = result * 10 + (c - '0');
            }
            return true;
        };
        int offsetHour = 0;
        int offsetMinute = 0;
        if (!parseUnsigned(hourText, offsetHour) ||
            !parseUnsigned(minuteText, offsetMinute) ||
            offsetHour > 15 || offsetMinute > 59) {
            return std::nullopt;
        }
        offsetMinutes = sign * (offsetHour * 60 + offsetMinute);
    }

    const size_t firstColon = text.find(':');
    const size_t secondColon = firstColon == std::string::npos
        ? std::string::npos : text.find(':', firstColon + 1);
    if (firstColon == std::string::npos ||
        secondColon == std::string::npos ||
        text.find(':', secondColon + 1) != std::string::npos) {
        return std::nullopt;
    }
    auto parseUnsigned = [](const std::string& value, long long& result) {
        if (value.empty()) return false;
        result = 0;
        for (const unsigned char c : value) {
            if (!std::isdigit(c)) return false;
            result = result * 10 + (c - '0');
        }
        return true;
    };
    long long hour = 0;
    long long minute = 0;
    if (!parseUnsigned(text.substr(0, firstColon), hour) ||
        !parseUnsigned(text.substr(firstColon + 1,
                                   secondColon - firstColon - 1), minute)) {
        return std::nullopt;
    }
    const std::string normalized = formatTimeFields(
        hour, minute, text.substr(secondColon + 1));
    if (normalized.empty()) return std::nullopt;

    ExtractTimeParts result;
    result.hour = std::stoi(normalized.substr(0, 2));
    result.minute = std::stoi(normalized.substr(3, 2));
    result.secondMicros = std::stoll(normalized.substr(6, 2)) * 1000000LL;
    const size_t dot = normalized.find('.');
    if (dot != std::string::npos) {
        std::string fraction = normalized.substr(dot + 1);
        fraction.append(6 - fraction.size(), '0');
        result.secondMicros += std::stoll(fraction);
    }
    result.offsetMinutes = offsetMinutes;
    return result;
}

static bool scaleIntervalField(long long value, long double scale,
                               long long& result, bool calendarField = false) {
    const long double scaled = static_cast<long double>(value) * scale;
    const long double lower = calendarField ? std::numeric_limits<int32_t>::lowest()
        : static_cast<long double>(std::numeric_limits<int64_t>::lowest());
    const long double upperExclusive = calendarField
        ? static_cast<long double>(std::numeric_limits<int32_t>::max()) + 1.0L
        : -static_cast<long double>(std::numeric_limits<int64_t>::lowest());
    if (!std::isfinite(scaled) ||
        scaled < lower || scaled >= upperExclusive) {
        return false;
    }
    result = static_cast<long long>(scaled);
    return true;
}

static std::string formatMicrosNumeric(__int128 value) {
    const bool negative = value < 0;
    const unsigned __int128 magnitude = negative
        ? static_cast<unsigned __int128>(-(value + 1)) + 1
        : static_cast<unsigned __int128>(value);
    unsigned __int128 whole = magnitude / 1000000;
    const unsigned int fraction =
        static_cast<unsigned int>(magnitude % 1000000);

    std::string integer;
    do {
        integer.push_back(static_cast<char>('0' + whole % 10));
        whole /= 10;
    } while (whole != 0);
    if (negative) integer.push_back('-');
    std::reverse(integer.begin(), integer.end());

    std::string fractional = std::to_string(fraction);
    fractional.insert(fractional.begin(), 6 - fractional.size(), '0');
    return integer + "." + fractional;
}

static IntervalParts parseIntervalText(const std::string& value) {
    return parseIntervalInput(value);
}

static void validateTypedIntervalRange(const std::string& value) {
    const auto parsed = parseIntervalText(value);
    if (parsed.numericFieldTooLong)
        throw DbError("22007", "invalid input syntax for type interval");
    if (parsed.outOfRange)
        throw DbError("22015", "interval field value out of range");
    if (parsed.combinedOutOfRange)
        throw DbError("22008", "interval out of range");
}

// Render (months, days, micros) back to canonical PG text.
static std::string intervalToText(long long months, long long days, long long micros) {
    return formatIntervalInput(months, days, micros);
}

// Civil-date helpers (Howard Hinnant's algorithms, public domain).
static long long civilToDays(long long y, unsigned m, unsigned d) {
    y -= m <= 2;
    const long long era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<long long>(doe) - 719468;
}
static std::tuple<long long, unsigned, unsigned> daysToCivil(long long z) {
    z += 719468;
    const long long era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const long long y = static_cast<long long>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    const unsigned d = doy - (153 * mp + 2) / 5 + 1;
    const unsigned m = mp + (mp < 10 ? 3 : -9);
    return {y + (m <= 2), m, d};
}

static bool parseInt64Exact(const std::string& text, long long& value) {
    try {
        size_t consumed = 0;
        value = std::stoll(text, &consumed);
        return consumed == text.size();
    } catch (...) {
        return false;
    }
}

static int32_t parseInt32Argument(const ExprValue& argument) {
    long long parsed = 0;
    if (!parseInt64Exact(argument.value, parsed)) {
        throw std::runtime_error(
            "invalid input syntax for type integer (SQLSTATE 22P02)");
    }
    if (parsed < std::numeric_limits<int32_t>::lowest() ||
        parsed > std::numeric_limits<int32_t>::max()) {
        throw std::runtime_error(
            "integer out of range (SQLSTATE 22003)");
    }
    return static_cast<int32_t>(parsed);
}

constexpr size_t kMaxTextPayload = (size_t{1} << 30) - 4;

static std::string shiftDateByDays(const std::string& text, long long days,
                                   bool add) {
    Date input(text.c_str());
    if (input.year < 1 || input.year > 9999) return "";

    const long long minimumDay = civilToDays(1, 1, 1);
    const long long maximumDay = civilToDays(9999, 12, 31);
    const __int128 shifted =
        static_cast<__int128>(civilToDays(input.year, input.month, input.day)) +
        (add ? static_cast<__int128>(days)
             : -static_cast<__int128>(days));
    if (shifted < minimumDay || shifted > maximumDay) return "";

    auto [year, month, day] = daysToCivil(static_cast<long long>(shifted));
    char buffer[16];
    std::snprintf(buffer, sizeof(buffer), "%04lld-%02u-%02u", year, month,
                  day);
    return buffer;
}

// Apply an interval to 'YYYY-MM-DD[ HH:MM:SS]' and return a timestamp.
// DATE +/- INTERVAL promotes to timestamp, so a date-only input must retain
// any time introduced by the interval. Months roll the calendar date (day
// clamped to month length); days and microseconds shift with day carry.
static std::string timestampShift(const std::string& ts, const IntervalParts& iv, bool add) {
    const std::string input = trimStr(ts);
    const size_t separator = input.find_first_of(" Tt");
    const bool hasTime = separator != std::string::npos;
    const std::string dateText = hasTime
        ? input.substr(0, separator) : input;
    Date inputDate(dateText.c_str());
    if (inputDate.year == 0 || inputDate.year < 1 || inputDate.year > 9999)
        return "";

    long long h = 0, mi = 0, s = 0;
    long long inputFraction = 0;
    int inputDayCarry = 0;
    if (hasTime) {
        std::string timeText = trimStr(input.substr(separator + 1));
        size_t zonePosition = std::string::npos;
        for (size_t i = 1; i < timeText.size(); ++i) {
            if (timeText[i] == '+' || timeText[i] == '-') {
                zonePosition = i;
                break;
            }
        }
        if (zonePosition != std::string::npos) timeText.resize(zonePosition);
        else if (!timeText.empty() &&
                 (timeText.back() == 'Z' || timeText.back() == 'z')) {
            timeText.pop_back();
        }

        const size_t firstColon = timeText.find(':');
        const size_t secondColon = firstColon == std::string::npos
            ? std::string::npos : timeText.find(':', firstColon + 1);
        if (firstColon == std::string::npos ||
            secondColon == std::string::npos ||
            timeText.find(':', secondColon + 1) != std::string::npos) {
            return "";
        }
        const auto parseUnsigned = [](const std::string& field,
                                      long long& value) {
            if (field.empty()) return false;
            value = 0;
            for (const unsigned char c : field) {
                if (!std::isdigit(c)) return false;
                value = value * 10 + (c - '0');
            }
            return true;
        };
        if (!parseUnsigned(timeText.substr(0, firstColon), h) ||
            !parseUnsigned(timeText.substr(
                firstColon + 1, secondColon - firstColon - 1), mi)) {
            return "";
        }
        const std::string normalizedTime = formatTimeFields(
            h, mi, timeText.substr(secondColon + 1), &inputDayCarry);
        if (normalizedTime.empty() ||
            std::sscanf(normalizedTime.c_str(), "%lld:%lld:%lld",
                        &h, &mi, &s) != 3) {
            return "";
        }
        const size_t dot = normalizedTime.find('.');
        if (dot != std::string::npos) {
            const std::string digits = normalizedTime.substr(dot + 1);
            if (!parseInt64Exact(digits, inputFraction)) return "";
            for (size_t i = digits.size(); i < 6; ++i)
                inputFraction *= 10;
        }
    }

    const __int128 direction = add ? 1 : -1;
    const long long minimumDay = civilToDays(1, 1, 1);
    const long long maximumDay = civilToDays(9999, 12, 31);
    auto dayInDomain = [&](const __int128 value) {
        return value >= minimumDay && value <= maximumDay;
    };

    __int128 totalDays = static_cast<__int128>(civilToDays(
        inputDate.year, inputDate.month, inputDate.day)) + inputDayCarry +
        direction * iv.days;
    if (!dayInDomain(totalDays)) return "";
    if (iv.months != 0) {
        auto [y2, m2, d2] =
            daysToCivil(static_cast<long long>(totalDays));
        const __int128 monthIndex = static_cast<__int128>(y2) * 12 +
            (m2 - 1) + direction * iv.months;
        const __int128 minimumMonth = 12;  // 0001-01
        const __int128 maximumMonth =
            static_cast<__int128>(9999) * 12 + 11;
        if (monthIndex < minimumMonth || monthIndex > maximumMonth)
            return "";
        const long long ny = static_cast<long long>(monthIndex / 12);
        const long long nm = static_cast<long long>(monthIndex % 12);
        static const int mdays[] = {31,28,31,30,31,30,31,31,30,31,30,31};
        int ml = mdays[nm];
        if (nm == 1 && ((ny % 4 == 0 && ny % 100 != 0) || ny % 400 == 0)) ml = 29;
        if (d2 > static_cast<unsigned>(ml)) d2 = static_cast<unsigned>(ml);
        totalDays = civilToDays(ny, static_cast<unsigned>(nm + 1), d2);
    }

    constexpr long long MICROS_PER_DAY = 86400000000LL;
    __int128 totalMicros =
        (static_cast<__int128>(h) * 3600 + mi * 60 + s) * 1000000 +
        inputFraction +
        direction * iv.micros;
    __int128 carryDays = totalMicros / MICROS_PER_DAY;
    totalMicros %= MICROS_PER_DAY;
    if (totalMicros < 0) {
        totalMicros += MICROS_PER_DAY;
        --carryDays;
    }
    totalDays += carryDays;
    if (!dayInDomain(totalDays)) return "";
    auto [Y2, M2, D2] =
        daysToCivil(static_cast<long long>(totalDays));
    const long long dayMicros = static_cast<long long>(totalMicros);
    long long hh = dayMicros / 3600000000LL;
    long long mm = (dayMicros / 60000000LL) % 60;
    long long ss = (dayMicros / 1000000LL) % 60;
    long long fs = dayMicros % 1000000LL;
    char buf[80];
    if (fs) std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u %02lld:%02lld:%02lld.%06lld",
                          Y2, M2, D2, hh, mm, ss, fs);
    else std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u %02lld:%02lld:%02lld",
                       Y2, M2, D2, hh, mm, ss);
    return buf;
}

static bool parseIsoTimestampMicros(const std::string& timestamp,
                                     int64_t& result) {
    if (timestamp.size() < 19) return false;
    Date date(timestamp.substr(0, 10).c_str());
    if (date.year == 0) return false;
    long long hour = 0;
    long long minute = 0;
    long long second = 0;
    if (std::sscanf(timestamp.c_str() + 11, "%lld:%lld:%lld",
                    &hour, &minute, &second) != 3 ||
        hour > 23 || minute > 59 || second > 59) {
        return false;
    }
    long long fraction = 0;
    const size_t dot = timestamp.find('.', 19);
    if (dot != std::string::npos) {
        size_t digits = 0;
        for (size_t i = dot + 1; i < timestamp.size(); ++i) {
            const unsigned char character = timestamp[i];
            if (!std::isdigit(character) || digits == 6) return false;
            fraction = fraction * 10 + (character - '0');
            ++digits;
        }
        while (digits++ < 6) fraction *= 10;
    }
    const __int128 micros =
        (static_cast<__int128>(civilToDays(
             date.year, static_cast<unsigned>(date.month),
             static_cast<unsigned>(date.day))) * 86400 +
         hour * 3600 + minute * 60 + second) * 1000000 + fraction;
    if (micros < std::numeric_limits<int64_t>::min() ||
        micros > std::numeric_limits<int64_t>::max()) {
        return false;
    }
    result = static_cast<int64_t>(micros);
    return true;
}

static bool shiftUuidTimestamp(int64_t baseMicros,
                               const IntervalParts& interval,
                               int64_t& result) {
    long long seconds = baseMicros / 1000000;
    long long fraction = baseMicros % 1000000;
    if (fraction < 0) {
        fraction += 1000000;
        --seconds;
    }
    long long days = seconds / 86400;
    long long secondsOfDay = seconds % 86400;
    if (secondsOfDay < 0) {
        secondsOfDay += 86400;
        --days;
    }
    const auto [year, month, day] = daysToCivil(days);
    if (year < 1 || year > 9999) return false;
    char buffer[80];
    std::snprintf(
        buffer, sizeof(buffer), "%04lld-%02u-%02u %02lld:%02lld:%02lld.%06lld",
        year, month, day, secondsOfDay / 3600,
        (secondsOfDay % 3600) / 60, secondsOfDay % 60, fraction);
    const std::string shifted = timestampShift(buffer, interval, true);
    return !shifted.empty() && parseIsoTimestampMicros(shifted, result);
}

static std::string formatUuidTimestamp(int64_t unixMicros) {
    long long days = unixMicros / 86400000000LL;
    long long dayMicros = unixMicros % 86400000000LL;
    if (dayMicros < 0) {
        dayMicros += 86400000000LL;
        --days;
    }
    const auto [year, month, day] = daysToCivil(days);
    if (year < 1 || year > 9999) return {};
    const long long hour = dayMicros / 3600000000LL;
    const long long minute = (dayMicros / 60000000LL) % 60;
    const long long second = (dayMicros / 1000000LL) % 60;
    const long long fraction = dayMicros % 1000000LL;
    char buffer[80];
    std::snprintf(buffer, sizeof(buffer),
                  "%04lld-%02u-%02u %02lld:%02lld:%02lld",
                  year, month, day, hour, minute, second);
    std::string result = buffer;
    if (fraction != 0) {
        std::string digits = std::to_string(fraction);
        digits.insert(digits.begin(), 6 - digits.size(), '0');
        while (!digits.empty() && digits.back() == '0') digits.pop_back();
        result += "." + digits;
    }
    return result + "+00";
}

// Resolve fixed-offset timezone spellings. Named zones use IANA rules at the
// input instant in timezoneOffsetAt() instead of a timeless lookup table.
static bool parseTimeZoneOffset(const std::string& name, long long& offsetMinutes) {
    std::string s = trimStr(name);
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') s = trimStr(s.substr(1, s.size() - 2));
    if (s.empty()) return false;
    std::string low;
    for (char c : s) low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (low == "utc" || low == "gmt" || low == "z") { offsetMinutes = 0; return true; }
    if (low.rfind("utc", 0) == 0 || low.rfind("gmt", 0) == 0) s = s.substr(3);
    // [+-]HH[:MM] or [+-]HHMM
    if (s.size() < 2 || (s[0] != '+' && s[0] != '-')) return false;
    const int sign = s[0] == '-' ? -1 : 1;
    const std::string rest = s.substr(1);
    const auto parseDigits = [](const std::string& text,
                                long long& value) {
        if (text.empty()) return false;
        value = 0;
        for (const unsigned char ch : text) {
            if (!std::isdigit(ch)) return false;
            value = value * 10 + static_cast<long long>(ch - '0');
        }
        return true;
    };
    long long hh = 0, mm = 0;
    const size_t colon = rest.find(':');
    if (colon != std::string::npos) {
        if (colon < 1 || colon > 2 ||
            rest.find(':', colon + 1) != std::string::npos ||
            rest.size() - colon - 1 != 2 ||
            !parseDigits(rest.substr(0, colon), hh) ||
            !parseDigits(rest.substr(colon + 1), mm)) {
            return false;
        }
    } else {
        if (rest.size() == 4) {
            if (!parseDigits(rest.substr(0, 2), hh) ||
                !parseDigits(rest.substr(2), mm)) {
                return false;
            }
        } else if (rest.size() <= 2 && parseDigits(rest, hh)) {
            mm = 0;
        } else {
            return false;
        }
    }
    if (hh > 15 || mm > 59) return false;
    // POSIX-style numeric zones invert the sign (UTC+8 means UTC-8),
    // while IANA named zones use their date-specific natural displacement.
    offsetMinutes = -sign * (hh * 60 + mm);
    return true;
}

static long long timezoneOffsetAt(const std::string& name,
                                  const std::string& input,
                                  bool localTime) {
    std::string zone = trimStr(name);
    if (zone.size() >= 2 && zone.front() == '\'' && zone.back() == '\'')
        zone = zone.substr(1, zone.size() - 2);
    const int64_t seconds = parseTimestampToSeconds(input);
    if (const auto namedOffset = dbms::ianaTimezoneOffsetMinutes(
            zone, seconds, localTime))
        return *namedOffset;
    long long fixedOffset = 0;
    if (parseTimeZoneOffset(zone, fixedOffset)) return fixedOffset;
    throw DbError("22023", "time zone \"" + zone + "\" not recognized");
}

// JSON helpers defined later in this file; forward-declared for the JSON
// operator evaluation (-> / ->> / #> / #>> / @> / <@) higher up.
static std::string trimStr(const std::string& s);
static std::string jsonTypeOf(const std::string& s);
static bool jsonStep(const std::string& cur, const std::string& key, std::string& out);
static bool jsonTopLevelSplit(const std::string& s, char open, char close,
                              std::vector<std::string>& out);
static bool jsonUnquoteString(const std::string& token, std::string& out);
static bool typeIsRange(const std::string& typeName);

// Split a SQL array literal '{e1,e2,...}' (or a bare non-array scalar,
// which yields one element) into its element texts. Handles nested arrays
// and quoted elements with escaped quotes. Returns false on malformed input.
static bool splitSqlArrayElems(const std::string& in, std::vector<std::string>& out) {
    std::string s = trimStr(in);
    if (s.empty()) return false;
    if (s.front() != '{') {
        // Not an array literal: treat as a single scalar element only when
        // callers passed something scalar; array ops then fail cleanly.
        out.push_back(s);
        return true;
    }
    if (s.back() != '}') return false;
    std::string body = s.substr(1, s.size() - 2);
    std::string cur;
    bool inQuote = false;
    int depth = 0;
    for (size_t i = 0; i < body.size(); ++i) {
        char c = body[i];
        if (inQuote) {
            cur += c;
            if (c == '\\' && i + 1 < body.size()) cur += body[++i];
            else if (c == '"') inQuote = false;
            continue;
        }
        if (c == '"') { inQuote = true; cur += c; continue; }
        if (c == '{') { ++depth; cur += c; continue; }
        if (c == '}') { --depth; cur += c; continue; }
        if (c == ',' && depth == 0) { out.push_back(cur); cur.clear(); continue; }
        cur += c;
    }
    if (inQuote || depth != 0) return false;
    if (!cur.empty() || !out.empty()) out.push_back(cur);
    return true;
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
        if ((s[i] == 'e' || s[i] == 'E') && hasDigit) {
            ++i;
            if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
            if (i == s.size()) return false;
            for (; i < s.size(); ++i) {
                if (s[i] < '0' || s[i] > '9') return false;
            }
            return true;
        }
        return false;
    }
    return hasDigit;
}

ExprValue ExprEvaluator::evalLiteral(const LiteralExpr* e) const {
    if (!e) return ExprValue{};
    if (e->preparedSubquery)
        throw DbError("0A000", "prepared subqueries require a query execution context");
    const std::string& raw = e->value;
    std::string low = toLower(raw);

    if (low == "null") return ExprValue("unknown", "", true);
    if (low == "true") return ExprValue("boolean", "t", false);
    if (low == "false") return ExprValue("boolean", "f", false);

    if (raw.size() >= 3 && raw[1] == '\'' && raw.back() == '\'' &&
        (raw[0] == 'B' || raw[0] == 'b' ||
         raw[0] == 'X' || raw[0] == 'x')) {
        std::string bits;
        if (!decodeBitStringLiteral(raw, bits)) {
            throw DbError("22P02", "invalid bit string literal");
        }
        return ExprValue("bit", std::move(bits), false);
    }

    if (!e->typeName.empty()) {
        if (isGeometryTypeName(e->typeName)) {
            std::string value;
            if (!normalizeGeometryText(unquote(raw), e->typeName, value))
                throw DbError("22P02", "invalid input syntax for type " + e->typeName);
            return ExprValue(e->typeName, std::move(value), false);
        }
        if (toLower(e->typeName) == "interval") validateTypedIntervalRange(unquote(raw));
        if (toLower(e->typeName) == "xml") {
            const std::string value = unquote(raw);
            const auto validation = validateXml(value, XmlParseMode::Content);
            if (!validation.ok) {
                throw DbError("2200N", "invalid XML content: " +
                                         validation.message);
            }
            return ExprValue("xml", value, false);
        }
        return ExprValue(e->typeName, unquote(raw), false);
    }

    if (isQuotedString(raw)) {
        return ExprValue("character varying", unquote(raw), false);
    }

    if (isNumericLiteral(raw)) {
        if (raw.find('.') != std::string::npos ||
            raw.find_first_of("eE") != std::string::npos)
            return ExprValue("numeric", raw, false);
        return ExprValue(ExprHelper::inferValuesResultType(raw), raw, false);
    }

    return ExprValue("character varying", raw, false);
}

// ----------------------------------------------------------------------------
// Column references
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalColumnRef(const ColumnRefExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    if (e->binding)
        return ctx.boundColumn(e->binding->sourceOrdinal, e->binding->columnOrdinal);
    // A qualified reference must win over an unqualified value with the same
    // column name. This is essential for joins and UPDATE ... FROM, where
    // the target and source relations commonly share column names.
    if (!e->table.empty()) {
        auto v = ctx.get(e->table + "." + e->column);
        if (v) return *v;
    }
    auto v = ctx.get(e->column);
    if (v) return *v;
    return ExprValue("unknown", "", true);
}

// ----------------------------------------------------------------------------
// Unary operators
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalUnaryOp(const UnaryOpExpr* e, const RowContext& ctx) const {
    if (!e || !e->operand) return ExprValue{};
    ExprValue v = eval(e->operand.get(), ctx);
    std::string op = toLower(e->op);

    if (op.rfind("collate ", 0) == 0) {
        const std::string name = collation::normalizeName(
            trimStr(e->op.substr(std::string("COLLATE ").size())));
        if (!collation::isValid(name))
            throw DbError("42704", "collation does not exist: " + name);
        if (!isCollatableExprType(v.typeName))
            throw DbError("42804", "collations are not supported by type " +
                                    v.typeName);
        v.collation = name;
        return v;
    }

    if (op == "+") return v;
    if (op == "-") {
        if (v.isNull) return v;
        if (toLower(v.typeName) == "interval") {
            const auto interval = parseIntervalInput(v.value);
            const auto state = intervalInputSqlState(interval);
            if (!state.empty())
                throw DbError(state, state == "22007"
                    ? "invalid input syntax for type interval"
                    : "interval out of range");
            // Calendar months/days are signed int32; time is signed int64
            // microseconds. Check each minimum before integer negation.
            if (interval.months == std::numeric_limits<int32_t>::lowest() ||
                interval.days == std::numeric_limits<int32_t>::lowest() ||
                interval.micros == std::numeric_limits<int64_t>::lowest())
                throw DbError("22008", "interval out of range");
            const auto months = -interval.months;
            const auto days = -interval.days;
            const auto micros = -interval.micros;
            // PostgreSQL reserves this triple for positive infinity. A
            // finite negation must not manufacture that representation.
            if (months == std::numeric_limits<int32_t>::max() &&
                days == std::numeric_limits<int32_t>::max() &&
                micros == std::numeric_limits<int64_t>::max())
                throw DbError("22008", "interval out of range");
            return ExprValue("interval", formatIntervalInput(months, days, micros, true), false);
        }
        if (v.value.empty()) return ExprValue(v.typeName, "0", false);
        // The positive spelling of INT_MIN needs a wider type, but the
        // negative literal itself fits int4/int8. Resolve the signed token
        // before applying ordinary integer negation and range checks.
        if (const auto* literal =
                dynamic_cast<const LiteralExpr*>(e->operand.get());
            literal && literal->typeName.empty() &&
            isNumericLiteral(literal->value) &&
            literal->value.find_first_of(".eE") == std::string::npos) {
            const std::string signedValue = "-" + literal->value;
            const std::string type =
                ExprHelper::inferValuesResultType(signedValue);
            const bool zero =
                literal->value.find_first_not_of('0') == std::string::npos;
            return ExprValue(type, zero ? "0" : signedValue, false);
        }
        if (toLower(v.typeName) == "money") {
            const auto money = tryParseMoney(v.value);
            if (!money || money->minorUnits() ==
                              std::numeric_limits<int64_t>::min()) {
                throw DbError("22003", "money out of range");
            }
            return ExprValue(
                "money", Money(-money->minorUnits()).format(
                             StorageEngine::getMoneyLocale()), false);
        }
        if (isIntegerTypeName(v.typeName)) {
            long long integer = 0;
            if (!parseInt64Exact(v.value, integer) ||
                integer == std::numeric_limits<int64_t>::lowest()) {
                throw DbError("22003", "integer out of range");
            }
            const std::string type = toLower(v.typeName);
            const bool narrowOverflow = (type == "smallint" || type == "int2")
                ? -integer < std::numeric_limits<int16_t>::lowest() ||
                    -integer > std::numeric_limits<int16_t>::max()
                : (type == "integer" || type == "int" || type == "int4") &&
                    (-integer < std::numeric_limits<int32_t>::lowest() ||
                     -integer > std::numeric_limits<int32_t>::max());
            if (narrowOverflow)
                throw DbError("22003", "integer out of range");
            return ExprValue(v.typeName, std::to_string(-integer), false);
        }
        if (isNumericTypeName(v.typeName)) {
            auto n = tryParseNumeric(v.value);
            // Keep the operand's type: like PG, -int4 stays int4 and only
            // numeric/decimal inputs stay exact-decimal (a numeric result
            // here would flip integer division into decimal division).
            if (n) {
                std::string outType = v.typeName;
                std::string tl = toLower(outType);
                if (tl == "numeric" || tl == "decimal") outType = "numeric";
                return ExprValue(outType, (-(*n)).toString(), false);
            }
        }
        if (v.value[0] == '-') return ExprValue(v.typeName, v.value.substr(1), false);
        return ExprValue(v.typeName, "-" + v.value, false);
    }
    if (op == "not") {
        // SQL three-valued logic: NOT NULL is NULL.
        if (v.isNull) return ExprValue("boolean", "", true);
        return ExprValue("boolean", v.asBool() ? "f" : "t", false);
    }
    if (op == "~") {
        if (!isBitStringTypeName(v.typeName)) {
            throw DbError("42883", "operator does not exist: ~ " + v.typeName);
        }
        if (v.isNull) return ExprValue(v.typeName, "", true);
        std::string result = v.value;
        for (char& bit : result) bit = bit == '0' ? '1' : '0';
        return ExprValue(v.typeName, std::move(result), false);
    }
    if (op.rfind("at time zone", 0) == 0) {
        // AT TIME ZONE <zone>: timestamp input is a wall clock in the named
        // zone and becomes a UTC timestamptz; timestamptz input is a UTC
        // instant rendered as a local timestamp.
        std::string zone = trimStr(e->op.substr(std::string("at time zone").size()));
        if (v.isNull)
            return ExprValue("timestamp", "", true);
        const std::string inputType = toLower(v.typeName);
        const bool tzIn = inputType == "timestamptz" ||
                          inputType == "timestamp with time zone";
        const long long offMin = timezoneOffsetAt(zone, v.value, !tzIn);
        IntervalParts shift;
        shift.micros = (tzIn ? offMin : -offMin) * 60000000LL;
        std::string out = timestampShift(v.value, shift, true);
        if (!tzIn && !out.empty()) out += "+00";
        if (out.empty()) return ExprValue("timestamp", "", true);
        return ExprValue(tzIn ? "timestamp" : "timestamptz", out, false);
    }
    if (toLower(op) == "is null") {
        return ExprValue("boolean", v.isNull ? "t" : "f", false);
    }
    if (toLower(op) == "is not null") {
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
    if (op == "is unknown" || op == "is not unknown") {
        bool r = v.isNull;
        if (op == "is not unknown") r = !r;
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

struct ComparableTimestamp {
    int infinity = 0;  // -1 = -infinity, 0 = finite, 1 = infinity
    int64_t micros = 0;
};

static std::optional<ComparableTimestamp> parseComparableTimestamp(
    const std::string& input, bool withTimeZone) {
    std::string text = trimStr(input);
    const std::string lowered = toLower(text);
    if (lowered == "infinity") return ComparableTimestamp{1, 0};
    if (lowered == "-infinity") return ComparableTimestamp{-1, 0};

    size_t separator = text.find_first_of(" Tt");
    if (separator != std::string::npos && text[separator] != ' ')
        text[separator] = ' ';
    const size_t timeStart = separator == std::string::npos
        ? text.size() : separator + 1;
    size_t zonePosition = std::string::npos;
    for (size_t i = timeStart; i < text.size(); ++i) {
        if (text[i] == '+' || text[i] == '-') {
            zonePosition = i;
            break;
        }
    }
    const bool hasZulu = !text.empty() &&
        (text.back() == 'Z' || text.back() == 'z');
    const size_t fractionEnd = zonePosition != std::string::npos
        ? zonePosition : (hasZulu ? text.size() - 1 : text.size());
    const size_t dot = text.find('.', timeStart);

    int64_t micros = 0;
    if (dot != std::string::npos) {
        if (dot >= fractionEnd || dot + 1 == fractionEnd)
            return std::nullopt;
        size_t digits = 0;
        for (size_t i = dot + 1; i < fractionEnd; ++i) {
            if (text[i] < '0' || text[i] > '9') return std::nullopt;
            if (digits < 6) micros = micros * 10 + (text[i] - '0');
            ++digits;
        }
        while (digits < 6) {
            micros *= 10;
            ++digits;
        }
        if (digits > 6) {
            bool trailingNonzero = false;
            for (size_t i = dot + 8; i < fractionEnd; ++i) {
                if (text[i] != '0') {
                    trailingNonzero = true;
                    break;
                }
            }
            const bool roundUp = text[dot + 7] > '5' ||
                (text[dot + 7] == '5' &&
                 (trailingNonzero || micros % 2 != 0));
            if (roundUp) ++micros;
        }
        text.erase(dot, fractionEnd - dot);
    }

    if (!withTimeZone) {
        zonePosition = std::string::npos;
        for (size_t i = timeStart; i < text.size(); ++i) {
            if (text[i] == '+' || text[i] == '-') {
                zonePosition = i;
                break;
            }
        }
        if (zonePosition != std::string::npos) text.erase(zonePosition);
        else if (!text.empty() &&
                 (text.back() == 'Z' || text.back() == 'z')) text.pop_back();
    }

    int64_t seconds = parseTimestampToSeconds(text);
    if (seconds == 0 || isInfiniteTimestamp(seconds)) return std::nullopt;
    if (micros == 1000000) {
        ++seconds;
        micros = 0;
    }
    return ComparableTimestamp{0, seconds * 1000000LL + micros};
}

static __int128 preparedIntervalValue(const ExprValue& value) {
    const auto parsed=parseIntervalInput(value.value);
    const auto state=intervalInputSqlState(parsed);
    if(!state.empty())throw DbError(state,"invalid input for interval comparison");
    // PostgreSQL's interval ordering treats a month as thirty days. The
    // valid int32 month/day fields can overflow int64 once expressed in us.
    return (static_cast<__int128>(parsed.months)*30+parsed.days)*86400000000LL+parsed.micros;
}

int ExprEvaluator::compareValues(const ExprValue& a, const ExprValue& b) {
    if (a.isNull || b.isNull) return 0; // caller handles NULL

    std::string ta = toLower(a.typeName);
    std::string tb = toLower(b.typeName);
    if(common_type_detail::array(ta) && common_type_detail::array(tb)) {
        const auto left=sql_array_text::parse(a.value),right=sql_array_text::parse(b.value);
        const auto first=arrayElements(a),second=arrayElements(b);
        for(size_t i=0;i<std::min(first.size(),second.size());++i) {
            if(first[i].isNull!=second[i].isNull)return first[i].isNull?1:-1;
            if(first[i].isNull)continue;
            const int comparison=compareValues(first[i],second[i]);
            if(comparison)return comparison;
        }
        if(first.size()!=second.size())return first.size()>second.size()?1:-1;
        if(left.dimensions.size()!=right.dimensions.size())return left.dimensions.size()>right.dimensions.size()?1:-1;
        for(size_t i=0;i<left.dimensions.size();++i)
            if(left.dimensions[i].length!=right.dimensions[i].length)return left.dimensions[i].length>right.dimensions[i].length?1:-1;
        for(size_t i=0;i<left.dimensions.size();++i)
            if(left.dimensions[i].lower!=right.dimensions[i].lower)return left.dimensions[i].lower>right.dimensions[i].lower?1:-1;
        return 0;
    }
    if(ta=="interval" && tb=="interval") {
        const auto left=preparedIntervalValue(a),right=preparedIntervalValue(b);
        return (left>right)-(left<right);
    }
    if (isBitStringTypeName(ta) && isBitStringTypeName(tb)) {
        // Ordered bits compare lexicographically, then by length. Leading
        // zeroes and empty values are significant, not decimal decoration.
        return a.value < b.value ? -1 : (a.value > b.value ? 1 : 0);
    }

    // Boolean columns are commonly supplied by storage as "true"/"false",
    // while SQL boolean literals are represented internally as "t"/"f".
    // Compare their logical values instead of their different spellings.
    if (ta == "boolean" && tb == "boolean") {
        const bool ba = a.asBool();
        const bool bb = b.asBool();
        return (ba > bb) - (ba < bb);
    }

    if (ta == "uuid" || tb == "uuid") {
        const auto left = tryParseUuid(a.value);
        const auto right = tryParseUuid(b.value);
        if (!left || !right) {
            throw std::runtime_error(
                "invalid input syntax for type uuid (SQLSTATE 22P02)");
        }
        if (*left == *right) return 0;
        return *left < *right ? -1 : 1;
    }

    if (isCanonicalByteaType(ta) || isCanonicalByteaType(tb)) {
        const ByteaValue left = parseByteaOrThrow(a);
        const ByteaValue right = parseByteaOrThrow(b);
        if (left == right) return 0;
        return left < right ? -1 : 1;
    }

    if (isInetTypeName(ta) && isInetTypeName(tb)) {
        NetworkAddressValue left;
        NetworkAddressValue right;
        if (!parseNetworkAddress(a.value, left, ta == "cidr") ||
            !parseNetworkAddress(b.value, right, tb == "cidr")) {
            throw DbError("22P02", "invalid input syntax for network address");
        }
        return compareNetworkAddresses(left, right);
    }
    const bool macA = ta == "macaddr" || ta == "macaddr8";
    const bool macB = tb == "macaddr" || tb == "macaddr8";
    if (macA && macB) {
        const size_t leftLength = ta == "macaddr" ? 6 : 8;
        const size_t rightLength = tb == "macaddr" ? 6 : 8;
        std::array<uint8_t, 8> left{};
        std::array<uint8_t, 8> right{};
        if (!parseMacAddress(a.value, leftLength, left) ||
            !parseMacAddress(b.value, rightLength, right)) {
            throw DbError("22P02", "invalid input syntax for MAC address");
        }
        const int compared = std::lexicographical_compare(
            left.begin(), left.begin() + leftLength,
            right.begin(), right.begin() + rightLength) ? -1 :
            std::lexicographical_compare(
                right.begin(), right.begin() + rightLength,
                left.begin(), left.begin() + leftLength) ? 1 : 0;
        return compared;
    }

    const bool blankPaddedA = isBlankPaddedCharacterType(ta);
    const bool blankPaddedB = isBlankPaddedCharacterType(tb);
    auto isVaryingCharacter = [](const std::string& type) {
        return type == "varchar" || type == "character varying" ||
               type == "unknown" || type.rfind("varchar(", 0) == 0 ||
               type.rfind("character varying(", 0) == 0;
    };
    const bool varyingA = isVaryingCharacter(ta);
    const bool varyingB = isVaryingCharacter(tb);
    const bool textualA = blankPaddedA || varyingA || ta == "text" || ta == "name";
    const bool textualB = blankPaddedB || varyingB || tb == "text" || tb == "name";
    std::string textCollation;
    if (textualA && textualB) {
        const std::string leftCollation =
            collation::normalizeName(a.collation);
        const std::string rightCollation =
            collation::normalizeName(b.collation);
        if (!leftCollation.empty() && !rightCollation.empty() &&
            leftCollation != rightCollation) {
            throw DbError("42P21", "collation mismatch between explicit collations");
        }
        textCollation = leftCollation.empty()
            ? rightCollation : leftCollation;
        // NAME's implicit type collation is C, not the database's default
        // text locale. A supplied expression/source collation still wins.
        if(textCollation.empty() && (ta=="name" || tb=="name"))textCollation="C";
    }
    if ((blankPaddedA || blankPaddedB) && textualA && textualB) {
        std::string left = a.value;
        std::string right = b.value;
        auto trimPadding = [](std::string& value) {
            while (!value.empty() && value.back() == ' ') value.pop_back();
        };
        if (blankPaddedA || (blankPaddedB && varyingA)) trimPadding(left);
        if (blankPaddedB || (blankPaddedA && varyingB)) trimPadding(right);
        return compareTextWithCollation(left, right, textCollation);
    }

    // A pair of text values stays textual even when both strings contain
    // digits. Numeric coercion here changes ORDER BY, comparisons and
    // GREATEST/LEAST (for example, text '10' must precede text '2').
    if (textualA && textualB) {
        return compareTextWithCollation(a.value, b.value, textCollation);
    }

    if (ta == "money" || tb == "money") {
        const auto left = tryParseMoney(a.value, ta != "money");
        const auto right = tryParseMoney(b.value, tb != "money");
        if (left && right) {
            return (left->minorUnits() > right->minorUnits()) -
                   (left->minorUnits() < right->minorUnits());
        }
    }

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
        const bool zonedA = ta == "timestamptz" ||
                            ta == "timestamp with time zone";
        const bool zonedB = tb == "timestamptz" ||
                            tb == "timestamp with time zone";
        const auto parsedA = parseComparableTimestamp(a.value, zonedA);
        const auto parsedB = parseComparableTimestamp(b.value, zonedB);
        if (parsedA && parsedB) {
            if (parsedA->infinity != parsedB->infinity)
                return parsedA->infinity < parsedB->infinity ? -1 : 1;
            if (parsedA->infinity != 0) return 0;
            return (parsedA->micros > parsedB->micros) -
                   (parsedA->micros < parsedB->micros);
        }
        // Invalid values normally cannot reach comparison after cast/storage
        // validation; keep deterministic behavior for manually supplied data.
        return a.value < b.value ? -1 : (a.value > b.value ? 1 : 0);
    }

    // Default string comparison
    return a.value < b.value ? -1 : (a.value > b.value ? 1 : 0);
}

static double geometricFloatOperation(double left, double right, bool divide = false) {
    if (divide && right == 0.0) throw DbError("22012", "division by zero");
    const double result = divide ? left / right : left * right;
    if (std::isinf(result) && std::isfinite(left) && std::isfinite(right))
        throw DbError("22003", "value out of range: overflow");
    if (result == 0.0 && left != 0.0 && (divide ? !std::isinf(right) : right != 0.0))
        throw DbError("22003", "value out of range: underflow");
    return result;
}

// PostgreSQL's geometric '=' operators are not display-string equality or
// a generic ordering equivalence. In particular LSEG ordering is by length
// but equality is by the two ordered endpoints, and PATH '=' counts points.
static std::optional<bool> compareGeometricEquality(const std::string& op,
                                                    const ExprValue& left,
                                                    const ExprValue& right) {
    const auto type = toLower(left.typeName);
    if (type != toLower(right.typeName) ||
        (type != "path" && type != "circle" && type != "line" && type != "lseg"))
        return std::nullopt;
    if (op != "=" && !(op == "<>" && (type == "circle" || type == "lseg")))
        return std::nullopt;
    GeometryValue a, b;
    if (!parseGeometryValue(left.value, type, a) || !parseGeometryValue(right.value, type, b))
        throw DbError("22P02", "invalid input syntax for type " + type);
    const auto fuzzyEqual = [](double x, double y) {
        return x == y || std::fabs(x-y) <= 1.0e-6;
    };
    const auto exactEqual = [](double x, double y) {
        return x == y || (std::isnan(x) && std::isnan(y));
    };
    if (type == "path") return a.coordinates.size() == b.coordinates.size();
    if (type == "circle") {
        const auto area = [](double radius) {
            return geometricFloatOperation(geometricFloatOperation(radius, radius), std::acos(-1.0));
        };
        const double x=area(a.coordinates[2]), y=area(b.coordinates[2]);
        // FPne is deliberately not !FPeq for NaN, matching circle_ne.
        return op == "=" ? fuzzyEqual(x,y) : x != y && std::fabs(x-y) > 1.0e-6;
    }
    bool equal = true;
    if (type == "line") {
        const bool nan = std::any_of(a.coordinates.begin(),a.coordinates.end(),[](double v){return std::isnan(v);}) ||
                         std::any_of(b.coordinates.begin(),b.coordinates.end(),[](double v){return std::isnan(v);});
        if (nan) {
            for (size_t i=0;i<3;++i) equal = equal && exactEqual(a.coordinates[i],b.coordinates[i]);
        } else {
            double ratio=1.0;
            for (size_t i=0;i<3;++i) {
                if (std::fabs(b.coordinates[i]) > 1.0e-6) {
                    ratio=geometricFloatOperation(a.coordinates[i],b.coordinates[i],true);break;
                }
            }
            for (size_t i=0;i<3;++i)
                equal = equal && fuzzyEqual(a.coordinates[i],geometricFloatOperation(ratio,b.coordinates[i]));
        }
    } else {
        for (size_t offset : {size_t(0),size_t(2)}) {
            const bool nan=std::isnan(a.coordinates[offset]) || std::isnan(a.coordinates[offset+1]) ||
                           std::isnan(b.coordinates[offset]) || std::isnan(b.coordinates[offset+1]);
            for (size_t i=offset;i<offset+2;++i)
                equal = equal && (nan ? exactEqual(a.coordinates[i],b.coordinates[i]) :
                                         fuzzyEqual(a.coordinates[i],b.coordinates[i]));
        }
    }
    return op == "=" ? equal : !equal;
}

ExprValue ExprEvaluator::applyComparison(const std::string& op,
                                         const ExprValue& l,
                                         const ExprValue& r) {
    if(isBitStringTypeName(l.typeName) || isBitStringTypeName(r.typeName))
        (void)resolveComparison(op,l.typeName,r.typeName);
    if (l.isNull || r.isNull) return ExprValue("boolean", "", true);

    std::string cmp = op;
    if (cmp == "!=") cmp = "<>";
    if (const auto equal = compareGeometricEquality(cmp,l,r))
        return ExprValue("boolean", *equal ? "t" : "f", false);

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

QueryComparisonBinding ExprEvaluator::resolveComparison(const std::string& rawOp,
    const std::string& rawLeft, const std::string& rawRight) {
    QueryComparisonBinding binding;
    binding.op = rawOp == "!=" ? "<>" : rawOp;
    const auto types=resolveBuiltinEquality(rawLeft,rawRight);
    binding.leftType=types.first;binding.rightType=types.second;
    static const std::set<std::string> operations={"=","<>","<",">","<=",">="};
    static const std::set<std::string> numeric={"smallint","integer","bigint","real","double precision","numeric"};
    static const std::set<std::string> textual={"text","bpchar","name"};
    static const std::set<std::string> temporal={"date","timestamp","timestamptz"};
    const bool numbers=numeric.count(binding.leftType) && numeric.count(binding.rightType);
    const bool strings=textual.count(binding.leftType) && textual.count(binding.rightType);
    const bool timestamps=temporal.count(binding.leftType) && temporal.count(binding.rightType);
    const bool same=binding.leftType==binding.rightType &&
        (binding.leftType=="boolean" || binding.leftType=="uuid" || binding.leftType=="bytea" ||
         binding.leftType=="time" || binding.leftType=="interval" ||
         isBitStringTypeName(binding.leftType) || common_type_detail::array(binding.leftType));
    if (!operations.count(binding.op) || (!numbers && !strings && !timestamps && !same))
        throw DbError("42883","operator does not exist: " + binding.leftType + " " + rawOp + " " + binding.rightType);
    binding.identity="builtin-comparison:"+binding.op+"("+binding.leftType+","+binding.rightType+")";
    binding.strict=true;
    // Capability is attached to the resolved typed implementation. Unknown
    // operators/custom datatypes never reach an equality/hash fallback.
    binding.hashable=binding.op=="=" && (numbers || strings || binding.leftType=="boolean" ||
        binding.leftType=="interval" || isBitStringTypeName(binding.leftType));
    return binding;
}

ExprValue ExprEvaluator::coerceComparison(const QueryComparisonBinding& binding,
    const ExprValue& value, bool left) const {
    if (binding.identity.empty() || !binding.strict) throw DbError("XX000","comparison has no prepared implementation");
    const auto& target=left?binding.leftType:binding.rightType;
    ExprValue result=value;
    const auto source=common_type_detail::canonical(value.typeName);
    if(source!=target) {
        if(value.isNull)result=ExprValue(target,"",true);
        else if(source=="real" && target=="double precision")
            result=ExprValue(target,formatFloatingCastValue(static_cast<double>(parseRealCastValue(value))));
        else if(target=="bpchar")result.typeName=target; // implicit, unconstrained CHAR
        else if(target=="bit") {
            // Operator input conversion is unconstrained, not explicit
            // ::BIT's default BIT(1). Validate through the real VARBIT
            // codec and retain every bit, including empty/leading zeroes.
            result=evalCast(nullptr,RowContext{},value,"bit varying");
        }
        else result=evalCast(nullptr,RowContext{},value,target);
    }
    result.typeName=target;
    if (!binding.collation.empty()) result.collation=binding.collation;
    return result;
}
ExprValue ExprEvaluator::comparePrepared(const QueryComparisonBinding& binding,
    const ExprValue& left, const ExprValue& right) const {
    const auto a=coerceComparison(binding,left,true),b=coerceComparison(binding,right,false);
    if(a.isNull || b.isNull)return ExprValue("boolean","",true);
    if(binding.enumTypeOid) {
        const auto rank=[&](const ExprValue& value) {
            const auto found=std::find(binding.enumLabels.begin(),binding.enumLabels.end(),value.value);
            if(found==binding.enumLabels.end())
                throw DbError("22P02","invalid input value for enum "+binding.leftType+": "+value.value);
            return found-binding.enumLabels.begin();
        };
        const auto x=rank(a),y=rank(b);
        const bool truth=binding.op=="="?x==y:binding.op=="<>"?x!=y:
            binding.op=="<"?x<y:binding.op==">"?x>y:binding.op=="<="?x<=y:x>=y;
        return ExprValue("boolean",truth?"t":"f");
    }
    const auto floating=[](const std::string& type){return type=="real" || type=="double precision";};
    if(floating(binding.leftType) && floating(binding.rightType)) {
        const double x=binding.leftType=="real"?static_cast<double>(parseRealCastValue(a)):parseDoubleCastValue(a);
        const double y=binding.rightType=="real"?static_cast<double>(parseRealCastValue(b)):parseDoubleCastValue(b);
        // PostgreSQL orders NaN above non-NaN and treats two NaNs as equal.
        const int comparison=std::isnan(x)?(std::isnan(y)?0:1):std::isnan(y)?-1:(x>y)-(x<y);
        const bool truth=binding.op=="="?comparison==0:binding.op=="<>"?comparison!=0:
            binding.op=="<"?comparison<0:binding.op==">"?comparison>0:
            binding.op=="<="?comparison<=0:comparison>=0;
        return ExprValue("boolean",truth?"t":"f");
    }
    return applyComparison(binding.op,a,b);
}
std::string ExprEvaluator::comparisonHashKey(const QueryComparisonBinding& binding,
    const ExprValue& value, bool left) {
    if (!binding.hashable || !binding.strict || binding.identity.empty() || value.isNull)
        throw DbError("XX000","comparison has no non-NULL hash implementation");
    const auto type=common_type_detail::canonical(value.typeName);
    if (type!=(left?binding.leftType:binding.rightType)) throw DbError("XX000","comparison hash received an uncoerced cell");
    static const std::set<std::string> numeric={"smallint","integer","bigint","real","double precision","numeric"};
    if(type=="real" || type=="double precision") {
        double number=type=="real"?static_cast<double>(parseRealCastValue(value)):parseDoubleCastValue(value);
        if(number==0)number=0; // signed zeros are operator-equal
        return "float:"+formatFloatingCastValue(number);
    }
    if(type=="interval") {
        __int128 number=preparedIntervalValue(value);
        const bool negative=number<0;if(negative)number=-number;
        std::string key;
        do{key.push_back('0'+number%10);number/=10;}while(number);
        if(negative)key.push_back('-');std::reverse(key.begin(),key.end());return "interval:"+key;
    }
    Column column;
    const auto hashType=numeric.count(type)?"numeric":type=="boolean"?"boolean":"text";
    const auto error=TypeRegistry::instance().resolveColumnType(column,hashType,{},false);
    if (!error.empty()) throw DbError("XX000",error);
    column.collation=binding.collation.empty()?value.collation:binding.collation;
    std::string text=value.value;
    if(type=="bpchar")while(!text.empty() && text.back()==' ')text.pop_back();
    return StorageEngine::groupingValueKey(column,text,false);
}

// ----------------------------------------------------------------------------
// Arithmetic
// ----------------------------------------------------------------------------

static bool isIntegralPowerText(const std::string& text) {
    if (text.empty()) return false;
    size_t index = (text.front() == '+' || text.front() == '-') ? 1 : 0;
    if (index == text.size()) return false;
    for (; index < text.size(); ++index) {
        if (text[index] < '0' || text[index] > '9') return false;
    }
    return true;
}

static int numericPowerDisplayScale(long double value) {
    if (value == 0) return 16;
    if (!std::isfinite(value)) return 0;
    const int integerDigits = static_cast<int>(
        std::floor(std::log10(std::fabs(value)))) + 1;
    return std::max(0, 17 - integerDigits);
}

static ExprValue evaluateNumericPowerOperator(const ExprValue& left,
                                              const ExprValue& right) {
    const auto base = tryParseNumeric(left.value);
    const auto exponent = tryParseNumeric(right.value);
    if (!base || !exponent) return ExprValue("numeric", "", true);

    try {
        long long integerExponent = 0;
        if (parseInt64Exact(right.value, integerExponent) &&
            integerExponent >= -1000 && integerExponent <= 1000) {
            if (base->sign() == 0 && integerExponent < 0) {
                throw std::runtime_error(
                    "zero raised to a negative power is undefined "
                    "(SQLSTATE 2201F)");
            }

            uint64_t magnitude = integerExponent < 0
                ? static_cast<uint64_t>(-(integerExponent + 1)) + 1
                : static_cast<uint64_t>(integerExponent);
            Numeric result(1);
            Numeric factor = *base;
            while (magnitude != 0) {
                if ((magnitude & 1U) != 0) result = result * factor;
                magnitude >>= 1U;
                if (magnitude != 0) factor = factor * factor;
            }
            if (integerExponent < 0) result = Numeric(1) / result;
            if (!result.isFinite())
                return ExprValue("numeric", result.toString(), false);
            const std::string resultText = result.toString();
            if (isIntegralPowerText(left.value) &&
                isIntegralPowerText(right.value) &&
                resultText.find('.') == std::string::npos) {
                return ExprValue("numeric", resultText, false);
            }
            const long double approximate =
                std::strtold(resultText.c_str(), nullptr);
            return ExprValue(
                "numeric",
                result.withScale(
                    numericPowerDisplayScale(approximate)).toString(),
                false);
        }

        const long double baseValue =
            std::strtold(left.value.c_str(), nullptr);
        const long double exponentValue =
            std::strtold(right.value.c_str(), nullptr);
        if (baseValue == 0 && exponentValue < 0) {
            throw std::runtime_error(
                "zero raised to a negative power is undefined "
                "(SQLSTATE 2201F)");
        }
        if (baseValue < 0 && std::isfinite(exponentValue) &&
            std::trunc(exponentValue) != exponentValue) {
            throw std::runtime_error(
                "a negative number raised to a non-integer power yields "
                "a complex result (SQLSTATE 2201F)");
        }

        const long double result = std::pow(baseValue, exponentValue);
        if (std::isnan(result)) return ExprValue("numeric", "NaN", false);
        if (std::isinf(result)) {
            if (std::isfinite(baseValue) && std::isfinite(exponentValue)) {
                throw std::runtime_error(
                    "numeric value out of range (SQLSTATE 22003)");
            }
            return ExprValue(
                "numeric", std::signbit(result) ? "-Infinity" : "Infinity",
                false);
        }

        std::ostringstream output;
        output << std::fixed
               << std::setprecision(numericPowerDisplayScale(result))
               << result;
        return ExprValue("numeric", output.str(), false);
    } catch (const std::invalid_argument&) {
        throw std::runtime_error(
            "numeric value out of range (SQLSTATE 22003)");
    }
}

static float parseRealCastValue(const ExprValue& value);
static double parseDoubleCastValue(const ExprValue& value);
template <typename Floating>
static std::string formatFloatingCastValue(Floating value);

ExprValue ExprEvaluator::applyArithmetic(const std::string& op,
                                         const ExprValue& l,
                                         const ExprValue& r) {
    const auto integerWidth = arithmetic_detail::integerWidth;
    const int leftIntegerWidth = integerWidth(l.typeName);
    const int rightIntegerWidth = integerWidth(r.typeName);
    const int resultIntegerWidth = std::max(leftIntegerWidth, rightIntegerWidth);
    const auto integerType = arithmetic_detail::integerTypeName;
    const auto floatingWidth = arithmetic_detail::floatingWidth;
    const auto unknownFloatingOperand = [](const std::string& type) {
        const std::string name = toLower(type);
        return name.empty() || name == "unknown" || name == "character varying";
    };
    const auto numericFloatingOperand = [&](const std::string& type) {
        const std::string name = toLower(type);
        return floatingWidth(type) || integerWidth(type) ||
            name == "numeric" || name == "decimal" || unknownFloatingOperand(type);
    };
    const int leftFloatingWidth = floatingWidth(l.typeName);
    const int rightFloatingWidth = floatingWidth(r.typeName);
    const bool floatingOperands = (leftFloatingWidth || rightFloatingWidth) &&
        numericFloatingOperand(l.typeName) && numericFloatingOperand(r.typeName);
    const auto numericResultType = arithmetic_detail::resultType(op, l.typeName, r.typeName, true);
    const bool singlePrecision = numericResultType && *numericResultType == "real";
    if ((leftFloatingWidth || rightFloatingWidth) && op == "%")
        throw DbError("42883", "operator does not exist for floating operands");
    if (l.isNull || r.isNull) {
        if (numericResultType) return ExprValue(*numericResultType, "", true);
        return ExprValue(l.typeName, "", true);
    }

    // Interval arithmetic (PostgreSQL semantics):
    //   timestamp/date ± interval -> timestamp/date (months/days calendar-wise)
    //   interval ± interval       -> interval
    //   interval * n, n * interval, interval / n -> interval
    auto isTsLike = [](const ExprValue& v) {
        std::string t = toLower(v.typeName);
        if (t == "timestamp" || t == "timestamptz" || t == "date" ||
            t == "datetime")
            return true;
        if (!t.empty()) return false;
        // Untyped: does it parse as 'YYYY-M-D[ H:M:S]'?
        int Y = 0, Mo = 0, D = 0;
        return std::sscanf(v.value.c_str(), "%d-%d-%d", &Y, &Mo, &D) == 3 &&
               Y >= 1 && Mo >= 1 && Mo <= 12 && D >= 1 && D <= 31;
    };
    std::string lt = toLower(l.typeName), rt = toLower(r.typeName);
    // date +/- integer days (PG: date '2024-03-15' + 7 -> 2024-03-22) and
    // date - date -> integer day count (PG: 14).
    {
        auto isPureInt = [](const ExprValue& v) {
            return !v.value.empty() &&
                   v.value.find_first_not_of("0123456789-+") == std::string::npos &&
                   v.value != "-" && v.value != "+";
        };
        const bool lDate = (lt == "date");
        const bool rDate = (rt == "date");
        const bool lInt = isPureInt(l) && !lDate;
        const bool rInt = isPureInt(r) && !rDate;
        if (op == "+" && ((lDate && rInt) || (lInt && rDate))) {
            const ExprValue& dv = lDate ? l : r;
            long long days = 0;
            const bool parsed =
                parseInt64Exact((lDate ? r : l).value, days);
            const std::string shifted =
                parsed ? shiftDateByDays(dv.value, days, true) : "";
            return ExprValue("date", shifted, shifted.empty());
        }
        if (op == "-" && lDate && rDate) {
            int Y1 = 0, M1 = 0, D1 = 0, Y2 = 0, M2 = 0, D2 = 0;
            if (std::sscanf(l.value.substr(0, 10).c_str(), "%d-%d-%d", &Y1, &M1, &D1) == 3 &&
                std::sscanf(r.value.substr(0, 10).c_str(), "%d-%d-%d", &Y2, &M2, &D2) == 3) {
                long long diff = civilToDays(Y1, M1, D1) -
                                 civilToDays(Y2, M2, D2);
                return ExprValue("int4", std::to_string(diff), false);
            }
        }
        // timestamp - timestamp -> interval (PG: "1 day 00:30:00").
        // Only when both sides are timestamp-typed (cast or column), so
        // unknown/unknown text keeps the 42725 ambiguity error.
        if (op == "-" && (lt == "timestamp" || lt == "timestamptz" || lt == "datetime") &&
            (rt == "timestamp" || rt == "timestamptz" || rt == "datetime")) {
            const auto leftTimestamp = parseComparableTimestamp(
                l.value, lt == "timestamptz");
            const auto rightTimestamp = parseComparableTimestamp(
                r.value, rt == "timestamptz");
            if (!leftTimestamp || !rightTimestamp ||
                leftTimestamp->infinity != 0 ||
                rightTimestamp->infinity != 0) {
                return ExprValue("interval", "", true);
            }
            constexpr long long microsPerDay = 86400000000LL;
            const long long difference = leftTimestamp->micros -
                                         rightTimestamp->micros;
            const long long days = difference / microsPerDay;
            const long long micros = difference % microsPerDay;
            return ExprValue(
                "interval", intervalToText(0, days, micros), false);
        }
        if (op == "-" && lDate && rInt) {
            long long days = 0;
            const bool parsed = parseInt64Exact(r.value, days);
            const std::string shifted =
                parsed ? shiftDateByDays(l.value, days, false) : "";
            return ExprValue("date", shifted, shifted.empty());
        }
    }
    bool lIv = (lt == "interval"), rIv = (rt == "interval");    if (!lIv && !rIv && (op == "+" || op == "-") && isTsLike(l)) {
        // 'timestamp' + '1 day' style: the untyped operand is an interval
        // literal quoted as a string.
        IntervalParts iv = parseIntervalText(r.value);
        rIv = iv.ok;
    } else if (!lIv && !rIv && op == "+" && isTsLike(r)) {
        IntervalParts iv = parseIntervalText(l.value);
        lIv = iv.ok;
    }
    if (lIv || rIv) {
        const auto shiftedTimestampValue = [](const ExprValue& source,
                                              std::string shifted) {
            const std::string sourceType = toLower(source.typeName);
            const bool withTimeZone = sourceType == "timestamptz" ||
                sourceType == "timestamp with time zone";
            const std::string resultType = withTimeZone
                ? "timestamptz" : "timestamp";
            if (shifted.empty()) return ExprValue(resultType, "", true);
            if (withTimeZone) shifted += "+00";
            return ExprValue(resultType, std::move(shifted), false);
        };
        if (op == "*" || op == "/") {
            IntervalParts iv;
            double k = 0;
            if (lIv && !rIv) {
                iv = parseIntervalText(l.value);
                k = r.asDouble();
            } else if (rIv && !lIv && op == "*") {
                iv = parseIntervalText(r.value);
                k = l.asDouble();
            } else {
                return ExprValue("interval", "", true);
            }
            if (!iv.ok) return ExprValue("interval", "", true);
            if (op == "/" && k == 0)
                throw DbError("22012",
                    "division by zero");
            const long double scale = op == "*"
                ? static_cast<long double>(k)
                : 1.0L / static_cast<long double>(k);
            long long months = 0;
            long long days = 0;
            long long micros = 0;
            if (!scaleIntervalField(iv.months, scale, months, true) ||
                !scaleIntervalField(iv.days, scale, days, true) ||
                !scaleIntervalField(iv.micros, scale, micros)) {
                throw DbError("22008", "interval out of range");
            }
            return ExprValue("interval",
                             intervalToText(months, days, micros), false);
        }
        if (lIv && rIv) {
            IntervalParts a = parseIntervalText(l.value);
            IntervalParts b = parseIntervalText(r.value);
            if (!a.ok || !b.ok) return ExprValue("interval", "", true);
            const bool subtract = op == "-";
            long long mm = 0;
            long long dd = 0;
            long long us = 0;
            if (!combineIntervalField(a.months, b.months, subtract, mm, true) ||
                !combineIntervalField(a.days, b.days, subtract, dd, true) ||
                !combineIntervalField(a.micros, b.micros, subtract, us)) {
                throw DbError("22008", "interval out of range");
            }
            return ExprValue("interval", intervalToText(mm, dd, us), false);
        }
        if (lIv && op == "+") {
            IntervalParts iv = parseIntervalText(l.value);
            if (!iv.ok || !isTsLike(r)) return ExprValue("timestamp", "", true);
            std::string shifted = timestampShift(r.value, iv, true);
            return shiftedTimestampValue(r, std::move(shifted));
        }
        if (rIv) {
            IntervalParts iv = parseIntervalText(r.value);
            if (!iv.ok || !isTsLike(l)) return ExprValue("timestamp", "", true);
            std::string shifted = timestampShift(l.value, iv, op == "+");
            return shiftedTimestampValue(l, std::move(shifted));
        }
        return ExprValue("timestamp", "", true);
    }

    // PG operator resolution for string/number mixes (SQLSTATE-correct):
    //   unknown + unknown        -> 42725 operator is not unique
    //   int + unknown            -> strict int parse of the unknown (22P02)
    //   numeric + unknown        -> numeric parse of the unknown (22P02)
    auto isTextyType = [](const std::string& t) {
        std::string tl = toLower(t);
        return tl.empty() || tl == "unknown" || tl == "character varying" ||
               tl == "varchar" || tl.rfind("character", 0) == 0 || tl == "text";
    };
    if (isTextyType(l.typeName) && isTextyType(r.typeName)) {
        throw DbError("42725", "operator is not unique: unknown " + op +
                                 " unknown");
    }
    auto strictIntErr = [](const std::string& v) {
        throw DbError("22P02", std::string("invalid input syntax for type integer: ") +
                                 std::string(1, 34) + v + std::string(1, 34));
    };
    auto isIntTyped = [](const std::string& t) {
        std::string tl = toLower(t);
        return tl == "integer" || tl == "int" || tl == "int2" || tl == "int4" ||
               tl == "int8" || tl == "bigint" || tl == "smallint";
    };
    if (isTextyType(l.typeName) != isTextyType(r.typeName)) {
        const ExprValue& tv = isTextyType(l.typeName) ? l : r;
        const ExprValue& ov = isTextyType(l.typeName) ? r : l;
        if (isIntTyped(ov.typeName)) {
            auto c2 = [](const std::string& s) {
                if (s.empty()) return false;
                size_t i2 = (s[0] == 45 || s[0] == 43) ? 1 : 0;
                if (i2 == s.size()) return false;
                for (size_t k2 = i2; k2 < s.size(); ++k2)
                    if (s[k2] < 48 || s[k2] > 57) return false;
                return true;
            };
            if (!c2(tv.value)) strictIntErr(tv.value);
        }
    }
    // Exact arithmetic when either side carries a decimal-capable type
    // (numeric/decimal/float).  Integer op integer stays integer-typed, as
    // in PostgreSQL ("id + 10" on int4 returns int4, not numeric).
    auto isDecimalTyped = [](const std::string& t) {
        std::string tl = toLower(t);
        return tl == "numeric" || tl == "decimal" ||
               tl == "double precision" || tl == "float" || tl == "float8" ||
               tl == "real" || tl == "float4";
    };
    auto isFloatingTyped = [](const std::string& type) {
        const std::string lowered = toLower(type);
        return lowered == "double precision" || lowered == "float" ||
               lowered == "double" || lowered == "float8" || lowered == "real" ||
               lowered == "float4";
    };
    const bool floatingPower =
        op == "^" &&
        (isFloatingTyped(l.typeName) || isFloatingTyped(r.typeName));
    const bool leftMoney = toLower(l.typeName) == "money";
    const bool rightMoney = toLower(r.typeName) == "money";
    if (leftMoney || rightMoney) {
        const auto leftCash = leftMoney ? tryParseMoney(l.value)
                                        : std::optional<Money>{};
        const auto rightCash = rightMoney ? tryParseMoney(r.value)
                                          : std::optional<Money>{};
        if ((leftMoney && !leftCash) || (rightMoney && !rightCash)) {
            throw DbError("22P02",
                "invalid input syntax for type money");
        }
        const std::string locale = StorageEngine::getMoneyLocale();
        if ((op == "+" || op == "-") && leftMoney && rightMoney) {
            const __int128 result = op == "+"
                ? static_cast<__int128>(leftCash->minorUnits()) +
                      rightCash->minorUnits()
                : static_cast<__int128>(leftCash->minorUnits()) -
                      rightCash->minorUnits();
            if (result < std::numeric_limits<int64_t>::min() ||
                result > std::numeric_limits<int64_t>::max()) {
                throw DbError("22003",
                    "money out of range");
            }
            return ExprValue(
                "money", Money(static_cast<int64_t>(result)).format(locale),
                false);
        }
        if (op == "/" && leftMoney && rightMoney) {
            if (rightCash->minorUnits() == 0) {
                throw DbError("22012",
                    "division by zero");
            }
            const double quotient =
                static_cast<double>(leftCash->minorUnits()) /
                static_cast<double>(rightCash->minorUnits());
            char text[64];
            const auto converted = std::to_chars(
                text, text + sizeof(text), quotient,
                std::chars_format::general);
            if (converted.ec != std::errc()) {
                throw DbError("XX000",
                    "money division failed");
            }
            return ExprValue(
                "double precision", std::string(text, converted.ptr), false);
        }
        if ((op == "*" || op == "/") && leftMoney != rightMoney &&
            (leftMoney || op == "*")) {
            const ExprValue& numericOperand = leftMoney ? r : l;
            auto numeric = tryParseNumeric(numericOperand.value);
            if (!numeric) {
                throw DbError("22P02",
                    "invalid input syntax for type numeric");
            }
            if (op == "/" && numeric->isFinite() && numeric->sign() == 0) {
                throw DbError("22012",
                    "division by zero");
            }
            Numeric cashValue(
                (leftMoney ? *leftCash : *rightCash).decimalString(locale));
            Numeric result = op == "*" ? cashValue * *numeric
                                        : cashValue / *numeric;
            Money rounded;
            if (!result.isFinite() ||
                !Money::parseDecimal(result.toString(), rounded, locale)) {
                throw DbError("22003",
                    "money out of range");
            }
            return ExprValue("money", rounded.format(locale), false);
        }
        throw DbError("42883",
            "operator does not exist for money operands");
    }
    if ((leftFloatingWidth || rightFloatingWidth) &&
        (op == "+" || op == "-" || op == "*" || op == "/")) {
        if (!floatingOperands)
            throw DbError("42883", "operator does not exist for floating operands");
        // REAL+REAL uses float4. Mixed REAL with integer/numeric selects
        // float8, but first restore each REAL's binary float4 datum before
        // widening; its shortest decimal text is not the same double value.
        const auto applyFloating = [&](auto a, auto b) -> ExprValue {
            using Floating = decltype(a);
            Floating result = 0;
            if (op == "+") result = a + b;
            else if (op == "-") result = a - b;
            else if (op == "*") result = a * b;
            else {
                if (b == 0)
                    throw DbError("22012", "division by zero");
                result = a / b;
            }
            if ((std::isinf(result) && std::isfinite(a) && std::isfinite(b)) ||
                ((op == "*" || op == "/") && result == 0 && a != 0 && b != 0 &&
                 std::isfinite(a) && std::isfinite(b)))
                throw DbError("22003", "floating value out of range");
            return ExprValue(singlePrecision ? "real" : "double precision",
                             formatFloatingCastValue(result), false);
        };
        if (singlePrecision)
            return applyFloating(parseRealCastValue(l), parseRealCastValue(r));
        const auto doubleOperand = [&](const ExprValue& value) {
            return floatingWidth(value.typeName) == 1
                ? static_cast<double>(parseRealCastValue(value))
                : parseDoubleCastValue(value);
        };
        return applyFloating(doubleOperand(l), doubleOperand(r));
    }
    if ((isDecimalTyped(l.typeName) || isDecimalTyped(r.typeName)) &&
        !floatingPower) {
        auto nl = tryParseNumeric(l.value);
        auto nr = tryParseNumeric(r.value);
        if (nl && nr) {
            if (op == "^") return evaluateNumericPowerOperator(l, r);
            Numeric res;
            if (op == "+") res = *nl + *nr;
            else if (op == "-") res = *nl - *nr;
            else if (op == "*") res = *nl * *nr;
            else if (op == "/") {
                if (nr->isFinite() && nr->sign() == 0)
                    throw DbError("22012", "division by zero");
                res = *nl / *nr;
            }
            else if (op == "%") {
                if (nl->isNaN() || nr->isNaN()) {
                    res = Numeric::nan();
                } else if (nr->isFinite() && nr->sign() == 0) {
                    throw DbError("22012", "division by zero");
                } else if (nl->isInfinite()) {
                    res = Numeric::nan();
                } else if (nr->isInfinite()) {
                    res = *nl;
                } else {
                    res = *nl - numericTruncatedQuotient(*nl, *nr) * *nr;
                }
            }
            else return ExprValue("numeric", "", true);
            // PG display scale from the operand TEXTS: +/- max,
            // * sum, / division scale.
            auto textScale = [](const std::string& s) {
                size_t d = s.find('.');
                return (d == std::string::npos) ? 0 : (int)(s.size() - d - 1);
            };
            int tsL = textScale(l.value), tsR = textScale(r.value);
            if (op == "/")
                return ExprValue("numeric", res.toString(), false);
            int target = std::max(tsL, tsR);
            if (op == "*") target = tsL + tsR;
            Numeric rs2 = res.withScale(target);
            std::string s = rs2.toString();
            int cur = 0;
            size_t dot = s.find('.');
            if (dot != std::string::npos) cur = (int)(s.size() - dot - 1);
            if (cur < target) {
                if (dot == std::string::npos) { s += '.'; }
                s += std::string(target - cur, '0');
            }
            return ExprValue("numeric", s, false);
        }
    }

    bool floatResult = l.value.find('.') != std::string::npos ||
                       r.value.find('.') != std::string::npos ||
                       isFloatingTyped(l.typeName) || isFloatingTyped(r.typeName) ||
                       toLower(l.typeName) == "numeric" || op == "^";

    // A bare decimal literal ("1.5") is NUMERIC in PG even when untyped
    // here: route decimal-point values through exact Numeric arithmetic
    // (select 1.5/1 -> 1.50000000000000000000 via select_div_scale).
    if (floatResult && op != "^" &&
        !isDecimalTyped(l.typeName) && !isDecimalTyped(r.typeName)) {
        auto nl2 = tryParseNumeric(l.value);
        auto nr2 = tryParseNumeric(r.value);
        if (nl2 && nr2) {
            Numeric res;
            if (op == "+") res = *nl2 + *nr2;
            else if (op == "-") res = *nl2 - *nr2;
            else if (op == "*") res = *nl2 * *nr2;
            else if (op == "/") {
                if (nr2->isFinite() && nr2->sign() == 0)
                    throw DbError("22012", "division by zero");
                res = *nl2 / *nr2;
            } else return ExprValue("numeric", "", true);
            return ExprValue("numeric", res.toString(), false);
        }
    }

    if (floatResult) {
        const auto powerOperand = [&](const ExprValue& value) {
            return floatingWidth(value.typeName) == 1
                ? static_cast<double>(parseRealCastValue(value))
                : parseDoubleCastValue(value);
        };
        double a = floatingOperands ? powerOperand(l) : l.asDouble();
        double b = floatingOperands ? powerOperand(r) : r.asDouble();
        double res = 0;
        if (op == "+") res = a + b;
        else if (op == "-") res = a - b;
        else if (op == "*") res = a * b;
        else if (op == "/") {
            if (b == 0) throw DbError("22012", "division by zero");
            res = a / b;
        }
        else if (op == "%") {
            if (b == 0)
                throw DbError("22012",
                    "division by zero");
            res = std::fmod(a, b);
        }
        else if (op == "^") {
            if ((a == 0 && b < 0) ||
                (a < 0 && std::isfinite(b) && std::trunc(b) != b)) {
                throw DbError("2201F",
                    "invalid argument for power function");
            }
            res = std::pow(a, b);
            if (std::isnan(res) && std::isfinite(a) && std::isfinite(b)) {
                throw DbError("2201F",
                    "invalid argument for power function");
            }
            if (std::isinf(res) && std::isfinite(a) && std::isfinite(b)) {
                throw DbError("22003",
                    "numeric value out of range");
            }
        }
        // PostgreSQL float8 output: shortest decimal string that round-trips
        // to the same double (extra_float_digits >= 1 semantics).  A plain
        // ostringstream insert would truncate to 6 significant digits.
        if (std::floor(res) == res && std::isfinite(res) &&
            res >= -9.007199254740992e15 && res <= 9.007199254740992e15) {
            // Integral values print without a fractional part, like PG.
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.0f", res);
            return ExprValue("double precision", buf, false);
        }
        for (int prec = 15; prec <= 17; ++prec) {
            std::ostringstream oss;
            oss << std::setprecision(prec) << res;
            double back = 0;
            std::istringstream iss(oss.str());
            iss >> back;
            if (back == res) {
                return ExprValue("double precision", oss.str(), false);
            }
        }
        std::ostringstream oss;
        oss << std::setprecision(17) << res;
        return ExprValue("double precision", oss.str(), false);
    }

    // PG int coercion is strict: an operand that does not fully parse as an
    // integer (untyped date-like or fraction text) raises 22P02 instead of
    // being atoi-truncated to a prefix.  Decimal-looking operands were
    // already routed to the numeric branches above.
    auto cleanInt = [](const std::string& s) {
        if (s.empty()) return false;
        size_t i2 = (s[0] == 45 || s[0] == 43) ? 1 : 0;
        if (i2 == s.size()) return false;
        for (size_t k2 = i2; k2 < s.size(); ++k2)
            if (s[k2] < 48 || s[k2] > 57) return false;
        return true;
    };
    if (!cleanInt(l.value))
        throw DbError("22P02", std::string("invalid input syntax for type integer: \"") + l.value + "\"");
    if (!cleanInt(r.value))
        throw DbError("22P02", std::string("invalid input syntax for type integer: \"") + r.value + "\"");
    auto integerOutOfRange = []() {
        throw DbError("22003", "integer out of range");
    };
    long long a = 0;
    long long b = 0;
    if (!parseInt64Exact(l.value, a) || !parseInt64Exact(r.value, b))
        integerOutOfRange();

    // PostgreSQL selects an int2/int4/int8 operator from the declared operand
    // types. Mixed integer widths widen, but same-width arithmetic must not
    // silently gain int8 range just because our textual carrier uses int64_t.
    const int resolvedWidth = resultIntegerWidth ? resultIntegerWidth : 2;
    const auto withinIntegerWidth = [](const __int128 value, int width) {
        if (width == 1)
            return value >= std::numeric_limits<int16_t>::lowest() &&
                   value <= std::numeric_limits<int16_t>::max();
        if (width == 2)
            return value >= std::numeric_limits<int32_t>::lowest() &&
                   value <= std::numeric_limits<int32_t>::max();
        return value >= std::numeric_limits<int64_t>::lowest() &&
               value <= std::numeric_limits<int64_t>::max();
    };
    if (!withinIntegerWidth(a, leftIntegerWidth ? leftIntegerWidth : resolvedWidth) ||
        !withinIntegerWidth(b, rightIntegerWidth ? rightIntegerWidth : resolvedWidth))
        integerOutOfRange();

    int64_t res = 0;
    if (op == "+" || op == "-" || op == "*") {
        __int128 wide = 0;
        if (op == "+")
            wide = static_cast<__int128>(a) + b;
        else if (op == "-")
            wide = static_cast<__int128>(a) - b;
        else
            wide = static_cast<__int128>(a) * b;
        if (!withinIntegerWidth(wide, resolvedWidth)) {
            integerOutOfRange();
        }
        res = static_cast<int64_t>(wide);
    }
    else if (op == "/") {
        if (b == 0) throw DbError("22012", "division by zero");
        if (a == std::numeric_limits<int64_t>::lowest() && b == -1)
            integerOutOfRange();
        res = a / b;
        if (!withinIntegerWidth(res, resolvedWidth)) integerOutOfRange();
    }
    else if (op == "%") {
        if (b == 0)
            throw DbError("22012", "division by zero");
        res = (a == std::numeric_limits<int64_t>::lowest() && b == -1)
            ? 0 : a % b;
    }
    return ExprValue(integerType(resolvedWidth), std::to_string(res), false);
}

// ----------------------------------------------------------------------------
// LIKE / SIMILAR TO
// ----------------------------------------------------------------------------

[[noreturn]] static void throwInvalidRegularExpression();

static bool likeMatchWithEscape(const std::string& text,
                                const std::string& pattern,
                                const std::string& escape,
                                bool foldCase = false,
                                bool byteMode = false,
                                bool asciiOnly = false) {
    return sql_pattern::like(text,pattern,escape,foldCase,byteMode,asciiOnly);
}

bool ExprEvaluator::likeMatch(const std::string& text, const std::string& pattern) {
    return likeMatchWithEscape(text, pattern, "\\");
}

bool ExprEvaluator::similarToMatch(const std::string& text, const std::string& pattern) {
    return sql_pattern::similar(text,pattern);
}

static bool similarToMatchEscape(const std::string& text, const std::string& pattern,
                                const std::string& escape, bool asciiOnly = false) {
    return sql_pattern::similar(text,pattern,escape,asciiOnly);
}

static void coercePatternTextArgument(ExprValue& value) {
    // SQL pattern/escape arguments accept TEXT, so a CHAR datum loses its
    // trailing padding there. The left bpchar datum keeps its own padding.
    if (ExprHelper::canonicalResultTypeName(value.typeName)=="character") {
        while (!value.value.empty() && value.value.back()==' ') value.value.pop_back();
        value.typeName="text";
    }
}

void ExprEvaluator::validatePatternEscapeInput(const ExprValue& escape) {
    if (escape.isNull) return;
    const bool bytes = ExprHelper::canonicalResultTypeName(escape.typeName) == "bytea";
    const size_t length = bytes ? parseByteaOrThrow(escape).bytes().size()
                                : utf8CharCount(escape.value);
    if (length > 1)
        throw DbError("22025", "invalid escape string: escape string must be empty or one character");
}

// ----------------------------------------------------------------------------
// Binary operators
// ----------------------------------------------------------------------------

static ExprValue tsMatch(const std::string& vecText, const std::string& query);
static bool parseArrayElements(const std::string&, std::vector<std::string>&);
static std::string arrayElemUnquote(const std::string&);
static std::string arrayElemQuote(const std::string&);
static std::optional<std::vector<size_t>> arrayShapeOf(const std::string&);

static std::string arrayExpressionType(const Expr* expression, const RowContext& row,
                                       const std::string& database, const ExprEvaluator* evaluator = nullptr) {
    if (const auto* literal=dynamic_cast<const LiteralExpr*>(expression);
        literal && literal->typeName.empty() && !literal->preparedSubquery &&
        (isQuotedString(literal->value) || toLower(literal->value)=="null")) return "unknown";
    if(const auto* function=dynamic_cast<const FunctionCallExpr*>(expression);function && evaluator){
        const auto type=evaluator->scalarFunctionResultType(function);
        if(!type.empty())return ExprHelper::canonicalResultTypeName(type);
    }
    std::map<std::string,std::string> hints;
    std::function<void(const Expr*)> collect = [&](const Expr* node) {
        if (!node || node->preparedSubquery) return;
        if (const auto* column = dynamic_cast<const ColumnRefExpr*>(node)) {
            if (!column->binding) {
                auto cell = row.get(column->toString());
                if (!cell) cell = row.get(column->column);
                if (cell) hints[column->toString()] = cell->typeName;
            }
        } else if (const auto* array = dynamic_cast<const ArrayExpr*>(node)) for (const auto& value : array->elements) collect(value.get());
        else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node)) { collect(binary->left.get()); if(binary->op!="::")collect(binary->right.get()); }
        else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(node)) collect(unary->operand.get());
        else if (const auto* cast = dynamic_cast<const CastExpr*>(node)) collect(cast->operand.get());
        else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(node)) for(const auto& value:call->args)collect(value.get());
        else if (const auto* conditional = dynamic_cast<const CaseExpr*>(node)) {
            for(const auto& arm:conditional->whenClauses){collect(arm.first.get());collect(arm.second.get());} collect(conditional->elseExpr.get());
        }
    };
    collect(expression);
    return ExprHelper::inferParsedResultType(expression,hints,database);
}

ExprValue ExprEvaluator::evalBinaryOp(const BinaryOpExpr* e, const RowContext& ctx) const {
    if (!e || !e->left || !e->right) return ExprValue{};
    std::string op = toLower(e->op);
    if(op=="[]") {
        // A multidimensional subscript is one receiver with N indexes, not
        // N independent one-dimensional array fetches. Preserve the original
        // datum/NULL and every dimension's actual lower bound.
        std::vector<const Expr*> indexes;
        const Expr* receiver=e;
        while(const auto* item=dynamic_cast<const BinaryOpExpr*>(receiver)) {
            if(item->op!="[]")break;
            indexes.push_back(item->right.get());receiver=item->left.get();
        }
        std::reverse(indexes.begin(),indexes.end());
        const auto array=eval(receiver,ctx);
        const auto type=ExprHelper::canonicalResultTypeName(array.typeName);
        const auto element=common_type_detail::array(type)?type.substr(0,type.size()-2):"unknown";
        std::vector<int32_t> subscripts;bool null=array.isNull;
        for(const auto* index:indexes) {
            const auto value=eval(index,ctx);
            if(value.isNull)null=true;
            else subscripts.push_back(parseInt32Argument(value));
        }
        if(null)return ExprValue(element,"",true);
        const auto literal=sql_array_text::parse(array.value);
        if(subscripts.size()!=literal.dimensions.size())return ExprValue(element,"",true);
        std::string current=literal.body;
        for(size_t dimension=0;dimension<subscripts.size();++dimension) {
            const auto& metadata=literal.dimensions[dimension];
            const int64_t offset=int64_t(subscripts[dimension])-metadata.lower;
            if(offset<0 || offset>=metadata.length)return ExprValue(element,"",true);
            const auto fields=sql_array_text::parse(current).elements;
            current=fields.at(static_cast<size_t>(offset));
        }
        const bool isNull=current=="NULL";
        return ExprValue(element,isNull?std::string{}:arrayElemUnquote(current),isNull);
    }
    auto arrayConcat = e->arrayConcat;
    if (op=="||" && !arrayConcat)
        arrayConcat = ExprHelper::resolveArrayConcatTypes(
            arrayExpressionType(e->left.get(),ctx,currentDB_,this),
            arrayExpressionType(e->right.get(),ctx,currentDB_,this));

    // Logical short-circuit with SQL three-valued logic:
    //   NULL AND false = false,  NULL AND true  = NULL
    //   NULL OR true   = true,   NULL OR false  = NULL
    if (op == "and") {
        ExprValue l = eval(e->left.get(), ctx);
        if (!l.isNull && !l.asBool()) return ExprValue("boolean", "f", false);
        ExprValue r = eval(e->right.get(), ctx);
        if (!r.isNull && !r.asBool()) return ExprValue("boolean", "f", false);
        if (l.isNull || r.isNull) return ExprValue("boolean", "", true);
        return ExprValue("boolean", r.asBool() ? "t" : "f", false);
    }
    if (op == "or") {
        ExprValue l = eval(e->left.get(), ctx);
        if (!l.isNull && l.asBool()) return ExprValue("boolean", "t", false);
        ExprValue r = eval(e->right.get(), ctx);
        if (!r.isNull && r.asBool()) return ExprValue("boolean", "t", false);
        if (l.isNull || r.isNull) return ExprValue("boolean", "", true);
        return ExprValue("boolean", r.asBool() ? "t" : "f", false);
    }

    ExprValue l = eval(e->left.get(), ctx);

    // A scalar IN list is represented as a RowExpr so each member retains its
    // own type, NULL bit, and expression tree. IN is an OR of equality
    // comparisons; NOT IN negates that three-valued result.
    if ((op == "in" || op == "not in") &&
        dynamic_cast<const RowExpr*>(e->right.get())) {
        const auto* list = static_cast<const RowExpr*>(e->right.get());
        const auto* literal=dynamic_cast<const LiteralExpr*>(e->left.get());
        const bool unknownLiteral=literal && literal->typeName.empty() && !literal->preparedSubquery &&
            (toLower(literal->value)=="null" || (!literal->value.empty() && literal->value.front()=='\''));
        bool sawUnknown = l.isNull;
        for (const auto& element : list->elements) {
            const ExprValue candidate = eval(element.get(), ctx);
            ExprValue left=l;
            if(unknownLiteral && isBitStringTypeName(candidate.typeName))left.typeName="unknown";
            const ExprValue equal = isBitStringTypeName(left.typeName) || isBitStringTypeName(candidate.typeName)
                ? comparePrepared(resolveComparison("=",left.typeName,candidate.typeName),left,candidate)
                : applyComparison("=",left,candidate);
            if (!equal.isNull && equal.asBool()) {
                return ExprValue("boolean", op == "in" ? "t" : "f", false);
            }
            if (equal.isNull) sawUnknown = true;
        }
        if (sawUnknown) return ExprValue("boolean", "", true);
        return ExprValue("boolean", op == "in" ? "f" : "t", false);
    }

    ExprValue r = eval(e->right.get(), ctx);

    if (arrayConcat) {
        const auto& binding = *arrayConcat;
        l = evalCast(nullptr,ctx,l,binding.leftType);
        r = evalCast(nullptr,ctx,r,binding.rightType);
        if (binding.leftArray && binding.rightArray) {
            if (l.isNull && r.isNull) return ExprValue(binding.elementType+"[]","",true);
            if (l.isNull) return r;
            if (r.isNull) return l;
            return ExprValue(binding.elementType+"[]",sql_array_text::concatenate(l.value,r.value),false);
        }
        std::vector<std::string> left, right;
        if (binding.leftArray) {
            if (!l.isNull && !parseArrayElements(l.value,left)) throw DbError("22P02","malformed array literal");
        } else left.push_back(l.isNull ? "NULL" : arrayElemQuote(l.value));
        if (binding.rightArray) {
            if (!r.isNull && !parseArrayElements(r.value,right)) throw DbError("22P02","malformed array literal");
        } else right.push_back(r.isNull ? "NULL" : arrayElemQuote(r.value));
        const auto shape=arrayShapeOf(binding.leftArray?l.value:r.value);
        if(shape && shape->size()>1)throw DbError("22000","argument must be empty or one-dimensional array");
        left.insert(left.end(),right.begin(),right.end());
        const auto& receiver=binding.leftArray?l:r;
        auto dimensions=receiver.isNull?std::vector<sql_array_text::Dimension>{}:sql_array_text::parse(receiver.value).dimensions;
        if(dimensions.empty())dimensions.push_back({1,0});
        dimensions.front().length=static_cast<int32_t>(left.size());
        return ExprValue(binding.elementType+"[]",sql_array_text::compose(left,dimensions),false);
    }

    if (op == "<<" || op == "<<=" || op == ">>" || op == ">>=" ||
        op == "&&") {
        const bool leftNetwork = isInetTypeName(l.typeName);
        const bool rightNetwork = isInetTypeName(r.typeName);
        if (leftNetwork || rightNetwork) {
            if (!leftNetwork || !rightNetwork) {
                throw DbError("42883", "operator does not exist for network and non-network operands");
            }
            if (l.isNull || r.isNull)
                return ExprValue("boolean", "", true);
            NetworkAddressValue left;
            NetworkAddressValue right;
            if (!parseNetworkAddress(l.value, left,
                                     toLower(l.typeName) == "cidr") ||
                !parseNetworkAddress(r.value, right,
                                     toLower(r.typeName) == "cidr")) {
                throw DbError("22P02", "invalid input syntax for network address");
            }
            bool result = false;
            if (op == "&&") {
                result = networkOverlaps(left, right);
            } else if (op == "<<" || op == "<<=") {
                result = networkContains(right, left, op == "<<=");
            } else {
                result = networkContains(left, right, op == ">>=");
            }
            return ExprValue("boolean", result ? "t" : "f", false);
        }
    }

    if (op == "&" || op == "|" || op == "#") {
        if (!isBitStringTypeName(l.typeName) ||
            !isBitStringTypeName(r.typeName)) {
            throw DbError("42883", "operator does not exist for non-bit operands");
        }
        if (l.isNull || r.isNull) return ExprValue("bit", "", true);
        if (l.value.size() != r.value.size()) {
            throw DbError("22026", "cannot bitwise operate bit strings of different sizes");
        }
        std::string result(l.value.size(), '0');
        for (size_t i = 0; i < result.size(); ++i) {
            const bool left = l.value[i] == '1';
            const bool right = r.value[i] == '1';
            const bool value = op == "&" ? left && right
                : op == "|" ? left || right : left != right;
            result[i] = value ? '1' : '0';
        }
        return ExprValue("bit", std::move(result), false);
    }
    if (op == "<<" || op == ">>") {
        if (!isBitStringTypeName(l.typeName) || !isIntegerTypeName(r.typeName)) {
            throw DbError("42883", "operator does not exist for bit shift operands");
        }
        if (l.isNull || r.isNull) return ExprValue(l.typeName, "", true);
        long long count = 0;
        if (!parseInt64Exact(r.value, count)) {
            throw DbError("22P02", "invalid input syntax for type integer");
        }
        bool shiftLeft = op == "<<";
        uint64_t amount = 0;
        if (count < 0) {
            shiftLeft = !shiftLeft;
            amount = count == std::numeric_limits<long long>::min()
                ? uint64_t{1} << 63
                : static_cast<uint64_t>(-count);
        } else {
            amount = static_cast<uint64_t>(count);
        }
        std::string result(l.value.size(), '0');
        if (amount < l.value.size()) {
            const size_t n = static_cast<size_t>(amount);
            if (shiftLeft) {
                std::copy(l.value.begin() + n, l.value.end(), result.begin());
            } else {
                std::copy(l.value.begin(), l.value.end() - n,
                          result.begin() + n);
            }
        }
        return ExprValue(l.typeName, std::move(result), false);
    }

    // Comparison
    // IS [NOT] DISTINCT FROM: equality that treats NULLs as comparable
    // (never returns NULL).
    if (op == "is distinct from" || op == "is not distinct from") {
        bool distinct;
        if (l.isNull || r.isNull) {
            distinct = (l.isNull != r.isNull);
        } else {
            // DISTINCT selects '=' and negates its Boolean result. '<>' is
            // neither guaranteed to exist nor its inverse for geometric NaN.
            distinct = !(e->comparison?comparePrepared(*e->comparison,l,r):applyComparison("=",l,r)).asBool();
        }
        if (op == "is not distinct from") distinct = !distinct;
        return ExprValue("boolean", distinct ? "t" : "f", false);
    }
    // POSIX-ish regex match operators: ~, ~* (case-insensitive), !~, !~*.
    if (op == "~" || op == "~*" || op == "!~" || op == "!~*") {
        if (l.isNull || r.isNull) return ExprValue("boolean", "", true);
        auto flags = std::regex::ECMAScript;
        if (op == "~*" || op == "!~*") flags |= std::regex::icase;
        bool m;
        try {
            std::regex re(r.value, flags);
            m = std::regex_search(l.value, re);
        } catch (const std::regex_error&) {
            throwInvalidRegularExpression();
        }
        if (op == "!~" || op == "!~*") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }
    static const std::set<std::string> cmpOps = {"=", "<>", "!=", "<", ">", "<=", ">="};
    if (cmpOps.count(op)) return e->comparison?comparePrepared(*e->comparison,l,r):applyComparison(op,l,r);

    // Arithmetic
    static const std::set<std::string> arithOps = {"+", "-", "*", "/", "%", "^"};
    if (arithOps.count(op)) return applyArithmetic(op, l, r);

    // String concatenation; SQL arrays concatenate as arrays (PG ||).
    if (op == "||") {
        const bool leftBit = isBitStringTypeName(l.typeName);
        const bool rightBit = isBitStringTypeName(r.typeName);
        if (leftBit || rightBit) {
            if (!leftBit || !rightBit) {
                throw DbError("42883", "operator does not exist for bit and non-bit operands");
            }
            if (l.isNull || r.isNull) return ExprValue("bit", "", true);
            if (l.value.size() > kMaxSupportedBitStringLength ||
                r.value.size() >
                    kMaxSupportedBitStringLength - l.value.size()) {
                throw DbError("54000", "bit string is too long");
            }
            return ExprValue("bit", l.value + r.value, false);
        }
        const bool byteaResult = isCanonicalByteaType(l.typeName) &&
                                 isCanonicalByteaType(r.typeName);
        std::string resultCollation;
        if (!byteaResult) {
            const std::string leftCollation =
                collation::normalizeName(l.collation);
            const std::string rightCollation =
                collation::normalizeName(r.collation);
            if (!leftCollation.empty() && !rightCollation.empty() &&
                leftCollation != rightCollation) {
                throw DbError("42P21",
                              "collation mismatch between explicit collations");
            }
            resultCollation = leftCollation.empty()
                ? rightCollation : leftCollation;
        }
        if (l.isNull || r.isNull) {
            ExprValue result(byteaResult ? "bytea" : "text", "", true);
            result.collation = std::move(resultCollation);
            return result;
        }
        if (byteaResult) {
            std::string joined = parseByteaOrThrow(l).bytes();
            joined += parseByteaOrThrow(r).bytes();
            return ExprValue(
                "bytea", ByteaValue::fromBytes(std::move(joined)).toString(),
                false);
        }
        // The text concatenation operator casts bpchar operands to text.
        // That cast discards the blank padding, unlike concat(), which
        // preserves the original character datum's visible spaces.
        ExprValue result(
            "text", l.value.substr(0, logicalCharacterByteLength(l)) +
                        r.value.substr(0, logicalCharacterByteLength(r)),
            false);
        result.collation = std::move(resultCollation);
        return result;
    }

    // JSON access operators (PostgreSQL):
    //   ->  field/array index as JSON    ->>  same but text (unquoted)
    //   #>  path 'a,b,0' as JSON        #>>  same but text
    //   @>  containment                  <@  contained-by (swapped @>)
    if (op == "->" || op == "->>" || op == "#>" || op == "#>>") {
        if (l.isNull || r.isNull) return ExprValue("text", "", true);
        std::string cur = l.value;
        if (op == "#>" || op == "#>>") {
            // Path text 'k1,k2,0' — split on commas, step through each.
            std::string path = r.value;
            if (path.size() >= 2 && path.front() == '\'' && path.back() == '\'')
                path = path.substr(1, path.size() - 2);
            // PG accepts the array-literal path form {a,b} as well as
            // bare a,b; strip the braces.
            if (path.size() >= 2 && path.front() == '{' && path.back() == '}')
                path = path.substr(1, path.size() - 2);
            std::vector<std::string> steps;
            std::string curStep;
            for (char pc : path) {
                if (pc == ',') { steps.push_back(curStep); curStep.clear(); }
                else curStep += pc;
            }
            if (!curStep.empty() || !steps.empty()) steps.push_back(curStep);
            for (const auto& st : steps) {
                if (st.empty()) continue;
                std::string next;
                if (!jsonStep(cur, st, next)) return ExprValue("text", "", true);
                cur = next;
            }
        } else {
            std::string next;
            if (!jsonStep(cur, r.value, next)) return ExprValue("text", "", true);
            cur = next;
        }
        if (op == "->" || op == "#>") return ExprValue("json", cur, false);
        // ->> / #>>: text form — unquote strings, JSON null -> SQL NULL.
        std::string t = trimStr(cur);
        if (t == "null" || t.empty()) return ExprValue("text", "", true);
        if (t.size() >= 2 && t.front() == '"' && t.back() == '"') {
            std::string o;
            if (!jsonUnquoteString(t, o))
                return ExprValue("text", "", true);
            return ExprValue("text", o, false);
        }
        return ExprValue("text", t, false);
    }
    if (op == "@@") {
        // Full-text match: tsvector @@ tsquery. The left side may be a
        // stored tsvector literal ('l':1,3 ...) or plain text; the right
        // side is a tsquery string.
        if (l.isNull || r.isNull) return ExprValue("boolean", "", true);
        return tsMatch(l.value, r.value);
    }
    if (op == "&&") {
        if (typeIsRange(l.typeName) && typeIsRange(r.typeName)) {
            const auto overlaps = rangesOverlap(l, r);
            if (!overlaps) return ExprValue("boolean", "", true);
            return ExprValue("boolean", *overlaps ? "t" : "f", false);
        }
        // SQL array overlap: any element (as a set) shared by both sides.
        if (l.isNull || r.isNull) return ExprValue("boolean", "", true);
        auto splitElems = [](const std::string& v) {
            std::vector<std::string> out;
            std::string t = trimStr(v);
            if (t.size() >= 2 && t.front() == 0x7B && t.back() == 0x7D) t = t.substr(1, t.size() - 2);
            std::string cur;
            int d = 0;
            for (char c : t) {
                if (c == 0x7B || c == 0x5B) ++d;
                else if (c == 0x7D || c == 0x5D) --d;
                if (c == 44 && d == 0) { out.push_back(trimStr(cur)); cur.clear(); }
                else cur += c;
            }
            if (!trimStr(cur).empty()) out.push_back(trimStr(cur));
            return out;
        };
        auto A = splitElems(l.value);
        auto B = splitElems(r.value);
        for (const auto& x : A)
            for (const auto& y : B)
                if (x == y) return ExprValue("boolean", "t", false);
        return ExprValue("boolean", "f", false);
    }

    if (op == "@>" || op == "<@") {
        // Two families share these operators:
        //   SQL arrays '{e1,e2}'  — element containment, recursive
        //   JSON '{"/["...'       — PostgreSQL JSON containment
        // SQL arrays are recognized by the '{' opener with ',' or '}' next
        // (a JSON object's first payload char is always '"'); JSON objects
        // always have quoted keys.
        const ExprValue& cont = (op == "@>") ? l : r;   // container
        const ExprValue& item = (op == "@>") ? r : l;   // contained
        if (typeIsRange(cont.typeName)) {
            const auto contains = rangeContains(cont, item);
            if (!contains) return ExprValue("boolean", "", true);
            return ExprValue("boolean", *contains ? "t" : "f", false);
        }
        if (cont.isNull || item.isNull) return ExprValue("boolean", "", true);
        auto looksSqlArray = [](const std::string& v) -> bool {
            std::string t = trimStr(v);
            if (t.size() < 2 || t.front() != '{' || t.back() == 0) return false;
            if (t.back() != '}') return false;
            // A JSON object always contains a top-level `":` (quoted key
            // followed by a colon) before any other structural char; a SQL
            // array's quoted elements are never followed by colons.
            bool inQ = false;
            for (size_t i = 1; i + 1 < t.size(); ++i) {
                char c = t[i];
                if (inQ) {
                    if (c == '\\') ++i;
                    else if (c == '"') inQ = false;
                    continue;
                }
                if (c == '"') { inQ = true; continue; }
                if (c == ':') return false; // JSON object
            }
            return true; // no key-colon shape: SQL array
        };
        if (looksSqlArray(cont.value) && looksSqlArray(item.value)) {
            std::vector<std::string> ce, ie;
            if (!splitSqlArrayElems(cont.value, ce) || !splitSqlArrayElems(item.value, ie))
                return ExprValue("boolean", "", true);
            // Every RHS element must appear in LHS (element-wise text match;
            // nested arrays match recursively by canonical text).
            bool res = true;
            for (const auto& e : ie) {
                bool found = false;
                for (const auto& c2 : ce) {
                    if (trimStr(c2) == trimStr(e)) { found = true; break; }
                }
                if (!found) { res = false; break; }
            }
            return ExprValue("boolean", res ? "t" : "f", false);
        }
        // JSON containment (recursive PostgreSQL semantics):
        std::function<bool(const std::string&, const std::string&)> contains =
            [&](const std::string& c, const std::string& i) -> bool {
            std::string ct = trimStr(c), it = trimStr(i);
            if (ct.empty() || it.empty()) return false;
            if (ct.front() == '{' && it.front() == '{') {
                std::vector<std::string> cm, im;
                if (!jsonTopLevelSplit(ct, '{', '}', cm)) return false;
                if (!jsonTopLevelSplit(it, '{', '}', im)) return false;
                for (const auto& m : im) {
                    size_t colon = std::string::npos; bool inQ = false;
                    for (size_t k = 0; k < m.size(); ++k) {
                        char c2 = m[k];
                        if (inQ) { if (c2 == '\\' && k + 1 < m.size()) ++k; else if (c2 == '"') inQ = false; }
                        else if (c2 == '"') inQ = true;
                        else if (c2 == ':') { colon = k; break; }
                    }
                    if (colon == std::string::npos) return false;
                    std::string k2 = trimStr(m.substr(0, colon));
                    std::string ku = k2;
                    if (k2.size() >= 2 && k2.front() == '"' &&
                        k2.back() == '"' &&
                        !jsonUnquoteString(k2, ku)) {
                        return false;
                    }
                    std::string want = trimStr(m.substr(colon + 1));
                    std::string got;
                    if (!jsonStep(ct, ku, got)) return false;
                    if (!contains(trimStr(got), want)) return false;
                }
                return true;
            }
            if (ct.front() == '[' && it.front() != '[' &&
                it.front() != '{') {
                std::vector<std::string> elements;
                if (!jsonTopLevelSplit(ct, '[', ']', elements)) return false;
                for (const auto& element : elements) {
                    const std::string candidate = trimStr(element);
                    if (!candidate.empty() && candidate.front() != '[' &&
                        candidate.front() != '{' &&
                        contains(candidate, it)) {
                        return true;
                    }
                }
                return false;
            }
            if (ct.front() == '[' && it.front() == '[') {
                std::vector<std::string> ce, ie;
                if (!jsonTopLevelSplit(ct, '[', ']', ce)) return false;
                if (!jsonTopLevelSplit(it, '[', ']', ie)) return false;
                for (const auto& e : ie) {
                    bool found = false;
                    for (const auto& c3 : ce) {
                        if (contains(trimStr(c3), trimStr(e))) { found = true; break; }
                    }
                    if (!found) return false;
                }
                return true;
            }
            // JSONB scalar equality is semantic rather than lexical: numeric
            // scale/exponent spelling and escaped string spelling do not
            // affect the value.
            if (ct.front() == '"' && it.front() == '"') {
                std::string containerString;
                std::string itemString;
                return jsonUnquoteString(ct, containerString) &&
                       jsonUnquoteString(it, itemString) &&
                       containerString == itemString;
            }
            if (jsonTypeOf(ct) == "number" &&
                jsonTypeOf(it) == "number") {
                const auto containerNumber = tryParseNumeric(ct);
                const auto itemNumber = tryParseNumeric(it);
                return containerNumber && itemNumber &&
                       *containerNumber == *itemNumber;
            }
            // Other scalars (or mixed shapes) have canonical spellings.
            return ct == it;
        };
        const bool res = contains(cont.value, item.value);
        return ExprValue("boolean", res ? "t" : "f", false);
    }

    // LIKE / ILIKE / SIMILAR TO
    if (op == "like" || op == "not like" || op == "ilike" || op == "not ilike" ||
        op == "similar to" || op == "not similar to") coercePatternTextArgument(r);
    if ((op == "like" || op == "not like" ||
         op == "ilike" || op == "not ilike" ||
         op == "similar to" || op == "not similar to") &&
        (l.isNull || r.isNull)) {
        return ExprValue("boolean", "", true);
    }
    if (op == "like" || op == "not like") {
        const bool bytes = ExprHelper::canonicalResultTypeName(l.typeName) == "bytea";
        bool m = bytes ? likeMatchWithEscape(parseByteaOrThrow(l).bytes(),
            parseByteaOrThrow(r).bytes(), "\\", false, true)
            : likeMatch(l.value, r.value);
        if (op == "not like") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }
    if (op == "ilike" || op == "not ilike") {
        const std::string locale = collation::normalizeName(l.collation.empty() ? r.collation : l.collation);
        bool m = likeMatchWithEscape(l.value,r.value,"\\",true,false,
                                     locale == "c" || locale == "posix");
        if (op == "not ilike") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }
    if (op == "similar to" || op == "not similar to") {
        const std::string locale = collation::normalizeName(l.collation.empty() ? r.collation : l.collation);
        bool m = similarToMatchEscape(l.value,r.value,"\\",locale == "c" || locale == "posix");
        if (op == "not similar to") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }

    // IN / NOT IN list
    if (op == "in" || op == "not in") {
        if (l.isNull) return ExprValue("boolean", "", true);
        // Parser currently stores IN list as raw literal text; split on space
        std::string listText = r.value;
        std::istringstream iss(listText);
        std::string tok;
        while (iss >> tok) {
            if (compareValues(l, ExprValue("character varying", unquote(tok), false)) == 0)
                return ExprValue("boolean", op == "in" ? "t" : "f", false);
        }
        return ExprValue("boolean", op == "in" ? "f" : "t", false);
    }

    // Cast (::)
    if (op == "::") {
        return evalCast(nullptr, ctx, l, r.value);
    }

    // Array slice (expr[lower:upper]) — PostgreSQL inclusive bounds,
    // 1-based; empty side = open bound; result is an array literal.
    if (op == "[:]") {
        if (l.isNull) return ExprValue("text", "", true);
        std::vector<std::string> elems;
        const auto literal=sql_array_text::parse(l.value);elems=literal.elements;
        if(literal.dimensions.empty())return ExprValue(l.typeName,"{}",false);
        // Bounds literal "lower:upper" (either side may be empty).
        const std::string& b = r.value;
        size_t colon = b.find(':');
        std::string loS = colon == std::string::npos ? b : b.substr(0, colon);
        std::string hiS = colon == std::string::npos ? "" : b.substr(colon + 1);
        const int64_t lower=literal.dimensions.front().lower;
        const int64_t upper=lower+literal.dimensions.front().length-1;
        long lo = lower, hi = upper;
        auto parseBound = [](const std::string& s, long def) -> long {
            if (s.empty()) return def;
            try {
                // Unary expressions are serialized by the parser as "- 1"
                // or "+ 1".  Compact only that separator so strict integer
                // parsing still rejects arbitrary embedded whitespace.
                std::string value = s;
                if (value.size() > 1 && (value[0] == '-' || value[0] == '+')) {
                    size_t digits = 1;
                    while (digits < value.size() &&
                           std::isspace(static_cast<unsigned char>(value[digits]))) {
                        ++digits;
                    }
                    value = value.substr(0, 1) + value.substr(digits);
                }
                size_t cp = 0;
                long v = std::stol(value, &cp);
                if (cp != value.size()) return def;
                return v;
            } catch (...) {
                return def;
            }
        };
        lo = parseBound(loS, lower);
        hi = parseBound(hiS, upper);
        if (lo < lower) lo = lower;
        if (hi > upper) hi = upper;
        std::string out = "{";
        for (long i = lo; i <= hi; ++i) {
            if (i > lo) out += ",";
            out += elems[static_cast<size_t>(i - lower)];
        }
        out += "}";
        return ExprValue(l.typeName, out, false);
    }

    return ExprValue{};
}

// ----------------------------------------------------------------------------
// CASE expressions
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalCase(const CaseExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    const std::string resultCollation = explicitResultCollation(e);
    auto withResultCollation = [&](ExprValue value) {
        value.collation = mergeExplicitCollations(value.collation,
                                                    resultCollation);
        return value;
    };
    const bool simpleCase = static_cast<bool>(e->switchExpr);
    if(!e->simpleEnumComparisons.empty() && e->simpleEnumComparisons.size()!=e->whenClauses.size())
        throw DbError("XX000","simple CASE has incomplete enum operator bindings");
    const ExprValue switchValue = simpleCase
        ? eval(e->switchExpr.get(), ctx) : ExprValue{};
    size_t clauseIndex = 0;
    for (const auto& wc : e->whenClauses) {
        bool match = false;
        if (simpleCase) {
            ExprValue condition = eval(wc.first.get(), ctx);
            ExprValue left = switchValue;
            ExprValue equal;
            if (!e->simpleEnumComparisons.empty() && e->simpleEnumComparisons.at(clauseIndex)) {
                equal=comparePrepared(*e->simpleEnumComparisons[clauseIndex],left,condition);
            } else if (!e->simpleComparisonTypes.empty()) {
                if (e->simpleComparisonTypes.size() != e->whenClauses.size())
                    throw DbError("XX000", "simple CASE has incomplete operator bindings");
                const auto& types = e->simpleComparisonTypes[clauseIndex];
                const auto coerce = [&](const ExprValue& value, const std::string& target) {
                    if (ExprHelper::canonicalResultTypeName(value.typeName) == target) return value;
                    if (target == "bpchar" || target == "bit") {
                        // An implicit unconstrained binary cast must not use
                        // explicit CHAR/BIT's default typmod of one.
                        ExprValue result = value; result.typeName = target;
                        return result;
                    }
                    return evalCast(nullptr, ctx, value, target);
                };
                left = coerce(left, types.first);
                condition = coerce(condition, types.second);
                const bool floating = types.first == "real" || types.first == "double precision";
                if (floating && (types.second == "real" || types.second == "double precision") &&
                    !left.isNull && !condition.isNull) {
                    // REAL's shortest round-trip text represents a float4,
                    // not an independently parsed float8. PostgreSQL's cross
                    // float operator promotes the already-rounded float4.
                    const double a = types.first == "real"
                        ? static_cast<double>(parseRealCastValue(left)) : parseDoubleCastValue(left);
                    const double b = types.second == "real"
                        ? static_cast<double>(parseRealCastValue(condition)) : parseDoubleCastValue(condition);
                    equal = ExprValue("boolean", (a == b || (std::isnan(a) && std::isnan(b))) ? "t" : "f", false);
                } else equal = applyComparison("=", left, condition);
            } else equal = applyComparison("=", left, condition);
            match = !equal.isNull && equal.asBool();
        } else {
            match = eval(wc.first.get(), ctx).asBool();
        }
        if (match) return withResultCollation(eval(wc.second.get(), ctx));
        ++clauseIndex;
    }
    if (e->elseExpr)
        return withResultCollation(eval(e->elseExpr.get(), ctx));
    return withResultCollation(ExprValue("unknown", "", true));
}

// ----------------------------------------------------------------------------
// CAST
// ----------------------------------------------------------------------------

enum class IntegerCastTarget { SmallInt, Integer, BigInt };

static ExprValue castToIntegerRange(const ExprValue& value,
                                    const std::string& targetType);
static ExprValue castToNumericRange(const ExprValue& value);
static ExprValue castToDateRange(const ExprValue& value);
static ExprValue castToTimestampRange(const ExprValue& value,
                                      const std::string& targetType);

static const char* integerCastTypeName(IntegerCastTarget target) {
    switch (target) {
        case IntegerCastTarget::SmallInt: return "smallint";
        case IntegerCastTarget::Integer:  return "integer";
        case IntegerCastTarget::BigInt:   return "bigint";
    }
    return "integer";
}

[[noreturn]] static void throwIntegerCastRangeError(IntegerCastTarget target) {
    throw DbError("22003",
        std::string(integerCastTypeName(target)) +
        " out of range");
}

[[noreturn]] static void throwIntegerCastSyntaxError(
    IntegerCastTarget target, const std::string& value) {
    throw DbError("22P02",
        "invalid input syntax for type " +
        std::string(integerCastTypeName(target)) + ": '" + value +
        "'");
}

enum class SignedIntegerParseResult { Ok, Invalid, OutOfRange };

static SignedIntegerParseResult parseSignedInteger(
    const std::string& input, int64_t& result) {
    const std::string text = trimStr(input);
    if (text.empty()) return SignedIntegerParseResult::Invalid;
    try {
        size_t consumed = 0;
        result = std::stoll(text, &consumed, 10);
        return consumed == text.size() ? SignedIntegerParseResult::Ok
                                       : SignedIntegerParseResult::Invalid;
    } catch (const std::invalid_argument&) {
        return SignedIntegerParseResult::Invalid;
    } catch (const std::out_of_range&) {
        return SignedIntegerParseResult::OutOfRange;
    }
}

static bool integerFitsTarget(int64_t value, IntegerCastTarget target) {
    switch (target) {
        case IntegerCastTarget::SmallInt:
            return value >= std::numeric_limits<int16_t>::min() &&
                   value <= std::numeric_limits<int16_t>::max();
        case IntegerCastTarget::Integer:
            return value >= std::numeric_limits<int32_t>::min() &&
                   value <= std::numeric_limits<int32_t>::max();
        case IntegerCastTarget::BigInt:
            return true;
    }
    return false;
}

static ExprValue castToInteger(const ExprValue& value,
                               IntegerCastTarget target) {
    const std::string sourceType = toLower(value.typeName);

    if (isCanonicalByteaType(sourceType)) {
        const ByteaValue parsedBytes = parseByteaOrThrow(value);
        const std::string& bytes = parsedBytes.bytes();
        const size_t width = target == IntegerCastTarget::SmallInt ? 2U :
                             target == IntegerCastTarget::Integer ? 4U : 8U;
        if (bytes.size() > width) {
            throw DbError("22003",
                "invalid byte sequence for encoding integer");
        }
        uint64_t bits = 0;
        for (const unsigned char byte : bytes)
            bits = (bits << 8) | byte;
        int64_t converted = 0;
        if (bytes.size() == width &&
            (bits & (uint64_t{1} << (width * 8 - 1))) != 0) {
            if (width == 8) {
                converted = bits == (uint64_t{1} << 63)
                    ? std::numeric_limits<int64_t>::min()
                    : -static_cast<int64_t>((~bits) + 1U);
            } else {
                const uint64_t mask =
                    ~((uint64_t{1} << (width * 8)) - 1);
                converted = static_cast<int64_t>(bits | mask);
            }
        } else {
            converted = static_cast<int64_t>(bits);
        }
        return ExprValue(integerCastTypeName(target),
                         std::to_string(converted), false);
    }

    if (sourceType == "boolean" || sourceType == "bool") {
        if (target != IntegerCastTarget::Integer) {
            throw DbError("42846",
                "cannot cast type boolean to " +
                std::string(integerCastTypeName(target)));
        }
        return ExprValue("integer", value.asBool() ? "1" : "0", false);
    }

    const bool numericSource =
        sourceType == "numeric" || sourceType == "decimal" ||
        sourceType.rfind("numeric(", 0) == 0 ||
        sourceType.rfind("decimal(", 0) == 0;
    const bool floatingSource =
        sourceType == "real" || sourceType == "float4" ||
        sourceType == "float" || sourceType == "float8" ||
        sourceType == "double" || sourceType == "double precision";

    int64_t converted = 0;
    if (floatingSource) {
        const std::string text = trimStr(value.value);
        double parsed = 0.0;
        try {
            size_t consumed = 0;
            parsed = std::stod(text, &consumed);
            if (consumed != text.size())
                throwIntegerCastSyntaxError(target, text);
        } catch (const std::invalid_argument&) {
            throwIntegerCastSyntaxError(target, text);
        } catch (const std::out_of_range&) {
            throwIntegerCastRangeError(target);
        }

        const long double rounded =
            std::nearbyint(static_cast<long double>(parsed));
        const int bitWidth = target == IntegerCastTarget::SmallInt ? 16 :
                             target == IntegerCastTarget::Integer ? 32 : 64;
        const long double upperExclusive = std::ldexp(1.0L, bitWidth - 1);
        if (!std::isfinite(rounded) || rounded < -upperExclusive ||
            rounded >= upperExclusive) {
            throwIntegerCastRangeError(target);
        }
        converted = static_cast<int64_t>(rounded);
    } else {
        std::string integerText = value.value;
        if (numericSource) {
            auto numeric = tryParseNumeric(value.value);
            if (!numeric)
                throwIntegerCastSyntaxError(target, trimStr(value.value));
            if (!numeric->isFinite()) throwIntegerCastRangeError(target);
            integerText = numeric->withScale(0).toString();
        }

        const SignedIntegerParseResult parsed =
            parseSignedInteger(integerText, converted);
        if (parsed == SignedIntegerParseResult::Invalid)
            throwIntegerCastSyntaxError(target, trimStr(value.value));
        if (parsed == SignedIntegerParseResult::OutOfRange)
            throwIntegerCastRangeError(target);
        if (!integerFitsTarget(converted, target))
            throwIntegerCastRangeError(target);
    }

    return ExprValue(integerCastTypeName(target), std::to_string(converted),
                     false);
}

static ExprValue castToBytea(const ExprValue& value) {
    const std::string sourceType = toLower(value.typeName);
    size_t width = 0;
    if (sourceType == "smallint" || sourceType == "int2") width = 2;
    else if (sourceType == "integer" || sourceType == "int" ||
             sourceType == "int4") width = 4;
    else if (sourceType == "bigint" || sourceType == "int8") width = 8;

    if (width != 0) {
        int64_t signedValue = 0;
        const SignedIntegerParseResult parsed =
            parseSignedInteger(value.value, signedValue);
        if (parsed != SignedIntegerParseResult::Ok) {
            throwIntegerCastSyntaxError(
                width == 2 ? IntegerCastTarget::SmallInt :
                width == 4 ? IntegerCastTarget::Integer :
                             IntegerCastTarget::BigInt,
                trimStr(value.value));
        }
        uint64_t bits = static_cast<uint64_t>(signedValue);
        std::string bytes(width, '\0');
        for (size_t index = width; index > 0; --index) {
            bytes[index - 1] = static_cast<char>(bits & 0xffU);
            bits >>= 8;
        }
        return ExprValue(
            "bytea", ByteaValue::fromBytes(std::move(bytes)).toString(),
            false);
    }

    ByteaValue bytes;
    if (!ByteaValue::parse(value.value, bytes)) {
        throw DbError("22P02",
            "invalid input syntax for type bytea");
    }
    return ExprValue("bytea", bytes.toString(), false);
}

[[noreturn]] static void throwFloatingCastSyntaxError(
    const std::string& targetType, const std::string& value) {
    throw DbError("22P02",
        "invalid input syntax for type " + targetType + ": '" + value +
        "'");
}

[[noreturn]] static void throwFloatingCastRangeError(
    const std::string& targetType) {
    throw DbError("22003",
        "value out of range for type " + targetType);
}

static void rejectBooleanFloatingCast(const ExprValue& value,
                                      const std::string& targetType) {
    const std::string sourceType = toLower(value.typeName);
    if (sourceType == "boolean" || sourceType == "bool") {
        throw DbError("42846",
            "cannot cast type boolean to " + targetType);
    }
}

static float parseRealCastValue(const ExprValue& value) {
    rejectBooleanFloatingCast(value, "real");
    const std::string text = trimStr(value.value);
    if (text.empty()) throwFloatingCastSyntaxError("real", text);

    errno = 0;
    char* end = nullptr;
    const float parsed = std::strtof(text.c_str(), &end);
    const int parseError = errno;
    if (end == text.c_str() || !end || *end != '\0')
        throwFloatingCastSyntaxError("real", text);
    // ERANGE is also reported for representable subnormal values. PostgreSQL
    // preserves those, but rejects finite input that overflows to infinity or
    // underflows all the way to zero.
    if (parseError == ERANGE &&
        (parsed == 0.0f || std::isinf(parsed))) {
        throwFloatingCastRangeError("real");
    }
    return parsed;
}

static double parseDoubleCastValue(const ExprValue& value) {
    rejectBooleanFloatingCast(value, "double precision");
    const std::string text = trimStr(value.value);
    if (text.empty())
        throwFloatingCastSyntaxError("double precision", text);

    errno = 0;
    char* end = nullptr;
    const double parsed = std::strtod(text.c_str(), &end);
    const int parseError = errno;
    if (end == text.c_str() || !end || *end != '\0')
        throwFloatingCastSyntaxError("double precision", text);
    if (parseError == ERANGE &&
        (parsed == 0.0 || std::isinf(parsed))) {
        throwFloatingCastRangeError("double precision");
    }
    return parsed;
}

template <typename Floating>
static std::string formatFloatingCastValue(Floating value) {
    if (std::isnan(value)) return "NaN";
    if (std::isinf(value))
        return std::signbit(value) ? "-Infinity" : "Infinity";

    char buffer[64];
    const auto converted = std::to_chars(
        buffer, buffer + sizeof(buffer), value, std::chars_format::general);
    if (converted.ec == std::errc())
        return std::string(buffer, converted.ptr);

    std::ostringstream output;
    output << std::setprecision(std::numeric_limits<Floating>::max_digits10)
           << value;
    return output.str();
}

struct NumericCastSpec {
    bool matches = false;
    bool hasTypmod = false;
    int precision = 0;
    int scale = 0;
};

[[noreturn]] static void throwNumericCastSyntaxError(
    const std::string& value) {
    throw DbError("22P02",
        "invalid input syntax for type numeric: '" + value +
        "'");
}

[[noreturn]] static void throwNumericCastOverflow() {
    throw DbError("22003",
        "numeric field overflow");
}

[[noreturn]] static void throwNumericTypmodError(
    const std::string& message) {
    throw DbError("22023",
        message);
}

static int parseNumericTypmodInteger(const std::string& text,
                                     const std::string& field) {
    int64_t parsed = 0;
    if (parseSignedInteger(text, parsed) != SignedIntegerParseResult::Ok ||
        parsed < std::numeric_limits<int>::min() ||
        parsed > std::numeric_limits<int>::max()) {
        throwNumericTypmodError("invalid NUMERIC " + field);
    }
    return static_cast<int>(parsed);
}

static NumericCastSpec parseNumericCastSpec(const std::string& target) {
    NumericCastSpec spec;
    std::string base;
    if (target == "numeric" || target == "decimal") {
        spec.matches = true;
        return spec;
    }
    if (target.rfind("numeric(", 0) == 0) {
        base = "numeric";
    } else if (target.rfind("decimal(", 0) == 0) {
        base = "decimal";
    } else {
        return spec;
    }

    spec.matches = true;
    spec.hasTypmod = true;
    const size_t open = base.size();
    const size_t close = target.rfind(')');
    if (close == std::string::npos || close != target.size() - 1 ||
        close <= open + 1) {
        throwNumericTypmodError("invalid NUMERIC type modifier");
    }

    const std::string body = target.substr(open + 1, close - open - 1);
    const size_t comma = body.find(',');
    if (comma != std::string::npos && body.find(',', comma + 1) !=
                                         std::string::npos) {
        throwNumericTypmodError("invalid NUMERIC type modifier");
    }
    const std::string precisionText =
        comma == std::string::npos ? body : body.substr(0, comma);
    const std::string scaleText =
        comma == std::string::npos ? "0" : body.substr(comma + 1);
    spec.precision = parseNumericTypmodInteger(precisionText, "precision");
    spec.scale = parseNumericTypmodInteger(scaleText, "scale");
    if (spec.precision < 1 || spec.precision > Numeric::kMaxPrecision) {
        throwNumericTypmodError(
            "NUMERIC precision " + std::to_string(spec.precision) +
            " must be between 1 and " +
            std::to_string(Numeric::kMaxPrecision));
    }
    if (spec.scale < -Numeric::kMaxPrecision ||
        spec.scale > Numeric::kMaxPrecision) {
        throwNumericTypmodError(
            "NUMERIC scale " + std::to_string(spec.scale) +
            " must be between -" + std::to_string(Numeric::kMaxPrecision) +
            " and " + std::to_string(Numeric::kMaxPrecision));
    }
    return spec;
}

static std::string formatNumericCastValue(const Numeric& numeric,
                                          int scale) {
    std::string result = numeric.toString();
    if (!numeric.isFinite() || scale <= 0) return result;

    int currentScale = 0;
    const size_t dot = result.find('.');
    if (dot != std::string::npos)
        currentScale = static_cast<int>(result.size() - dot - 1);
    if (currentScale < scale) {
        if (dot == std::string::npos) result.push_back('.');
        result.append(static_cast<size_t>(scale - currentScale), '0');
    }
    return result;
}

static ExprValue castToNumeric(const ExprValue& value,
                               const NumericCastSpec& spec) {
    const std::string sourceType = toLower(value.typeName);
    if (sourceType == "boolean" || sourceType == "bool") {
        throw DbError("42846",
            "cannot cast type boolean to numeric");
    }
    std::optional<Numeric> numeric;
    if (sourceType == "money") {
        const auto money = tryParseMoney(value.value);
        if (!money) throwNumericCastSyntaxError(trimStr(value.value));
        numeric.emplace(money->decimalString(
            StorageEngine::getMoneyLocale()));
    }
    try {
        if (!numeric) numeric.emplace(value.value);
    } catch (const std::invalid_argument& error) {
        const std::string message = error.what();
        if (message.find("exceeds maximum") != std::string::npos ||
            message.find("out of range") != std::string::npos) {
            throwNumericCastOverflow();
        }
        throwNumericCastSyntaxError(trimStr(value.value));
    }
    if (!spec.hasTypmod)
        return ExprValue("numeric", numeric->toString(), false);
    if (numeric->isNaN())
        return ExprValue("numeric", "NaN", false);
    if (numeric->isInfinite()) throwNumericCastOverflow();

    Numeric rounded;
    try {
        rounded = numeric->withScale(spec.scale);
    } catch (const std::invalid_argument&) {
        throwNumericCastOverflow();
    }
    const int allowedDigitsBeforeDecimal = spec.precision - spec.scale;
    const int actualDigitsBeforeDecimal =
        rounded.precision() - rounded.scale();
    if (rounded.sign() != 0 &&
        actualDigitsBeforeDecimal > allowedDigitsBeforeDecimal) {
        throwNumericCastOverflow();
    }
    return ExprValue("numeric",
                     formatNumericCastValue(rounded, spec.scale), false);
}

static bool isTextCastSourceType(const std::string& sourceType);

static ExprValue castToMoney(const ExprValue& value) {
    const std::string sourceType = toLower(value.typeName);
    if (sourceType == "boolean" || sourceType == "bool") {
        throw DbError("42846",
            "cannot cast type boolean to money");
    }
    const bool textual = isTextCastSourceType(sourceType);
    const bool numeric = isNumericTypeName(sourceType) ||
        sourceType == "real" || sourceType == "float" ||
        sourceType == "float4" || sourceType == "float8" ||
        sourceType == "double" || sourceType == "double precision";
    if (sourceType != "money" && !textual && !numeric) {
        throw DbError("42846",
            "cannot cast type " + sourceType +
            " to money");
    }
    std::string input = value.value;
    if (numeric) {
        try {
            const Numeric normalized(input);
            if (!normalized.isFinite()) throw std::invalid_argument("non-finite");
            input = normalized.toString();
        } catch (const std::invalid_argument&) {
            throw DbError("22P02",
                "invalid input syntax for type money: '" +
                trimStr(value.value) + "'");
        }
    }
    Money money;
    const std::string locale = StorageEngine::getMoneyLocale();
    const bool parsed = sourceType == "money" || textual
        ? Money::parse(input, money, locale)
        : Money::parseDecimal(input, money, locale);
    if (!parsed) {
        throw DbError("22P02",
            "invalid input syntax for type money: '" +
            trimStr(value.value) + "'");
    }
    return ExprValue("money", money.format(locale), false);
}

static std::optional<bool> parseBooleanCastText(const std::string& input) {
    return parsePostgresBoolean(input);
}

static ExprValue castToBoolean(const ExprValue& value) {
    const std::string sourceType = toLower(value.typeName);
    const bool booleanSource =
        sourceType == "boolean" || sourceType == "bool";
    const bool integerSource =
        sourceType == "integer" || sourceType == "int" ||
        sourceType == "int4";
    const bool textSource =
        sourceType.empty() || sourceType == "unknown" ||
        sourceType == "text" || sourceType == "varchar" ||
        sourceType == "character varying" || sourceType == "char" ||
        sourceType == "character" || sourceType == "bpchar" ||
        sourceType.rfind("varchar(", 0) == 0 ||
        sourceType.rfind("character(", 0) == 0 ||
        sourceType.rfind("character varying(", 0) == 0;

    if (integerSource) {
        int64_t integer = 0;
        const SignedIntegerParseResult parsed =
            parseSignedInteger(value.value, integer);
        if (parsed == SignedIntegerParseResult::OutOfRange)
            throwIntegerCastRangeError(IntegerCastTarget::Integer);
        if (parsed != SignedIntegerParseResult::Ok) {
            throw DbError("22P02",
                "invalid input syntax for type integer: '" +
                trimStr(value.value) + "'");
        }
        return ExprValue("boolean", integer == 0 ? "f" : "t", false);
    }

    if (!booleanSource && !textSource) {
        throw DbError("42846",
            "cannot cast type " + sourceType +
            " to boolean");
    }
    const auto parsed = parseBooleanCastText(value.value);
    if (!parsed) {
        throw DbError("22P02",
            "invalid input syntax for type boolean: '" +
            trimStr(value.value) + "'");
    }
    return ExprValue("boolean", *parsed ? "t" : "f", false);
}

static bool isTextCastSourceType(const std::string& sourceType) {
    return sourceType.empty() || sourceType == "unknown" ||
           sourceType == "text" || sourceType == "varchar" ||
           sourceType == "character varying" || sourceType == "char" ||
           sourceType == "character" || sourceType == "bpchar" ||
           sourceType.rfind("varchar(", 0) == 0 ||
           sourceType.rfind("character(", 0) == 0 ||
           sourceType.rfind("character varying(", 0) == 0;
}

static ExprValue castToUuid(const ExprValue& value) {
    const std::string sourceType = toLower(value.typeName);
    if (sourceType != "uuid" && !isTextCastSourceType(sourceType)) {
        throw DbError("42846",
            "cannot cast type " + sourceType +
            " to uuid");
    }
    UuidValue uuid;
    if (!UuidValue::parse(value.value, uuid)) {
        throw DbError("22P02",
            "invalid input syntax for type uuid: '" +
            trimStr(value.value) + "'");
    }
    return ExprValue("uuid", uuid.toString(), false);
}

static bool isTimestampCastSourceType(const std::string& sourceType) {
    return sourceType == "timestamp" || sourceType == "timestamptz" ||
           sourceType == "timestamp without time zone" ||
           sourceType == "timestamp with time zone";
}

static bool resemblesTemporalFieldText(const std::string& text) {
    bool hasDigit = false;
    for (const unsigned char ch : text) {
        if (std::isdigit(ch)) {
            hasDigit = true;
            continue;
        }
        if (std::isspace(ch) || ch == '-' || ch == '+' || ch == ':' ||
            ch == '.' || ch == 'T' || ch == 't' || ch == 'Z' || ch == 'z') {
            continue;
        }
        return false;
    }
    return hasDigit;
}

[[noreturn]] static void throwTemporalCastError(
    const std::string& targetType, const std::string& value) {
    if (resemblesTemporalFieldText(value)) {
        throw DbError("22008",
            "date/time field value out of range: '" + value +
            "'");
    }
    throw DbError("22007",
        "invalid input syntax for type " + targetType + ": '" + value +
        "'");
}

[[noreturn]] static void throwUnsupportedTemporalCast(
    const std::string& sourceType, const std::string& targetType) {
    throw DbError("42846",
        "cannot cast type " + sourceType + " to " + targetType);
}

static ExprValue castToDate(const ExprValue& value) {
    const std::string sourceType = toLower(value.typeName);
    const bool dateSource = sourceType == "date";
    const bool timestampSource = isTimestampCastSourceType(sourceType);
    if (!dateSource && !timestampSource &&
        !isTextCastSourceType(sourceType)) {
        throwUnsupportedTemporalCast(sourceType, "date");
    }

    std::string text = trimStr(value.value);
    const std::string lowered = toLower(text);
    if (lowered == "infinity" || lowered == "-infinity")
        return ExprValue("date", lowered, false);
    if (timestampSource) {
        const size_t separator = text.find_first_of(" T");
        if (separator != std::string::npos) text.resize(separator);
    }

    const Date date(text.c_str());
    if (date.year == 0) throwTemporalCastError("date", trimStr(value.value));
    return ExprValue("date", str(date), false);
}

static ExprValue castToTimestamp(const ExprValue& value,
                                 const std::string& targetType) {
    const std::string sourceType = toLower(value.typeName);
    const bool dateSource = sourceType == "date";
    if (!dateSource && !isTimestampCastSourceType(sourceType) &&
        !isTextCastSourceType(sourceType)) {
        throwUnsupportedTemporalCast(sourceType, targetType);
    }

    std::string text = trimStr(value.value);
    const std::string lowered = toLower(text);
    if (lowered == "infinity" || lowered == "-infinity")
        return ExprValue(targetType, lowered, false);
    if (dateSource) {
        const Date date(text.c_str());
        if (date.year == 0) throwTemporalCastError(targetType, text);
        text = str(date) + " 00:00:00";
    }

    const size_t separator = text.find_first_of(" Tt");
    std::string dateText = separator == std::string::npos
        ? text : text.substr(0, separator);
    std::string timeAndZone = separator == std::string::npos
        ? "00:00:00" : trimStr(text.substr(separator + 1));
    const Date parsedDate(dateText.c_str());
    if (parsedDate.year == 0 || timeAndZone.empty() ||
        timeAndZone.find_first_of(" \t\r\n") != std::string::npos) {
        throwTemporalCastError(targetType, text);
    }

    size_t zonePosition = std::string::npos;
    for (size_t i = 1; i < timeAndZone.size(); ++i) {
        if (timeAndZone[i] == '+' || timeAndZone[i] == '-') {
            zonePosition = i;
            break;
        }
    }
    std::string zone;
    if (zonePosition != std::string::npos) {
        zone = timeAndZone.substr(zonePosition);
        timeAndZone.resize(zonePosition);
    } else if (!timeAndZone.empty() &&
               (timeAndZone.back() == 'Z' || timeAndZone.back() == 'z')) {
        zone = timeAndZone.substr(timeAndZone.size() - 1);
        timeAndZone.pop_back();
    }

    const size_t firstColon = timeAndZone.find(':');
    const size_t secondColon = firstColon == std::string::npos
        ? std::string::npos : timeAndZone.find(':', firstColon + 1);
    if (firstColon == std::string::npos || secondColon == std::string::npos ||
        timeAndZone.find(':', secondColon + 1) != std::string::npos) {
        throwTemporalCastError(targetType, text);
    }
    const auto parseUnsignedField = [](const std::string& field,
                                       long long& output) {
        if (field.empty()) return false;
        output = 0;
        for (const unsigned char c : field) {
            if (!std::isdigit(c)) return false;
            output = output * 10 + (c - '0');
        }
        return true;
    };
    long long hours = 0;
    long long minutes = 0;
    if (!parseUnsignedField(timeAndZone.substr(0, firstColon), hours) ||
        !parseUnsignedField(
            timeAndZone.substr(firstColon + 1,
                               secondColon - firstColon - 1), minutes)) {
        throwTemporalCastError(targetType, text);
    }

    int dayCarry = 0;
    const std::string formattedTime = formatTimeFields(
        hours, minutes, timeAndZone.substr(secondColon + 1), &dayCarry);
    if (formattedTime.empty()) throwTemporalCastError(targetType, text);
    Date normalizedDate = parsedDate;
    if (dayCarry != 0) normalizedDate = parsedDate + dayCarry;
    if (normalizedDate.year == 0) throwTemporalCastError(targetType, text);

    const size_t fractionPosition = formattedTime.find('.');
    const std::string wholeTime = fractionPosition == std::string::npos
        ? formattedTime : formattedTime.substr(0, fractionPosition);
    const std::string fraction = fractionPosition == std::string::npos
        ? "" : formattedTime.substr(fractionPosition);
    const std::string localTimestamp = str(normalizedDate) + " " + wholeTime;

    const bool sourceHasTimeZone =
        sourceType == "timestamptz" ||
        sourceType == "timestamp with time zone";
    // Validate an explicit displacement even when a timestamp-without-zone
    // target will ignore it, matching PostgreSQL's input rules.  An unzoned
    // value cast to timestamptz is local wall time in the current session.
    int64_t zonedTimestamp = parseTimestampToSeconds(localTimestamp + zone);
    if (zonedTimestamp == 0) throwTemporalCastError(targetType, text);
    if (targetType == "timestamptz" && zone.empty() && !sourceHasTimeZone) {
        const Session* session = currentSession();
        if (session) {
            int inputOffset = session->timezoneOffsetMinutes;
            if (const auto namedOffset = dbms::ianaTimezoneOffsetMinutes(
                    session->timeZone, zonedTimestamp, true))
                inputOffset = *namedOffset;
            zonedTimestamp -= static_cast<int64_t>(inputOffset) * 60;
        }
    }
    const bool convertToUtc = targetType == "timestamptz" ||
                              sourceHasTimeZone;
    std::string formatted = localTimestamp;
    if (convertToUtc) {
        formatted = formatTimestampSeconds(zonedTimestamp);
        if (formatted.empty()) throwTemporalCastError(targetType, text);
    }
    formatted += fraction;
    if (targetType == "timestamptz") formatted += "+00";
    return ExprValue(targetType, formatted, false);
}

enum class CharacterCastKind { None, Text, Varchar, Char };

struct CharacterCastSpec {
    CharacterCastKind kind = CharacterCastKind::None;
    bool hasLength = false;
    size_t length = 0;
};

[[noreturn]] static void throwCharacterTypmodError(
    const std::string& message) {
    throw DbError("22023",
        message);
}

static CharacterCastSpec parseCharacterCastSpec(const std::string& target) {
    CharacterCastSpec spec;
    if (target == "text") {
        spec.kind = CharacterCastKind::Text;
        return spec;
    }
    if (target.rfind("text(", 0) == 0) {
        throw DbError("42601",
            "type modifier is not allowed for type text");
    }

    std::string base;
    if (target == "varchar" || target == "character varying") {
        spec.kind = CharacterCastKind::Varchar;
        return spec;
    }
    if (target == "char" || target == "character" || target == "bpchar") {
        spec.kind = CharacterCastKind::Char;
        spec.hasLength = true;
        spec.length = 1;
        return spec;
    }
    if (target.rfind("varchar(", 0) == 0) {
        base = "varchar";
        spec.kind = CharacterCastKind::Varchar;
    } else if (target.rfind("character varying(", 0) == 0) {
        base = "character varying";
        spec.kind = CharacterCastKind::Varchar;
    } else if (target.rfind("char(", 0) == 0) {
        base = "char";
        spec.kind = CharacterCastKind::Char;
    } else if (target.rfind("character(", 0) == 0) {
        base = "character";
        spec.kind = CharacterCastKind::Char;
    } else if (target.rfind("bpchar(", 0) == 0) {
        base = "bpchar";
        spec.kind = CharacterCastKind::Char;
    } else {
        return spec;
    }

    const size_t close = target.rfind(')');
    if (close == std::string::npos || close != target.size() - 1 ||
        close <= base.size() + 1) {
        throwCharacterTypmodError("invalid length for type " + base);
    }
    const std::string lengthText =
        target.substr(base.size() + 1, close - base.size() - 1);
    int64_t length = 0;
    if (parseSignedInteger(lengthText, length) !=
        SignedIntegerParseResult::Ok) {
        throwCharacterTypmodError("invalid length for type " + base);
    }
    constexpr int64_t maximumLength = 10485760;
    if (length < 1) {
        throwCharacterTypmodError(
            "length for type " + base + " must be at least 1");
    }
    if (length > maximumLength) {
        throwCharacterTypmodError(
            "length for type " + base + " cannot exceed " +
            std::to_string(maximumLength));
    }
    spec.hasLength = true;
    spec.length = static_cast<size_t>(length);
    return spec;
}

static ExprValue castToCharacter(const ExprValue& value,
                                 const CharacterCastSpec& spec) {
    std::string converted = value.value;
    if (spec.kind != CharacterCastKind::Char &&
        isBlankPaddedCharacterType(value.typeName)) {
        converted.resize(logicalCharacterByteLength(value));
    }
    if (spec.hasLength) {
        const size_t characters = utf8CharCount(converted);
        if (characters > spec.length) {
            converted.resize(utf8ByteAt(converted, spec.length));
        } else if (spec.kind == CharacterCastKind::Char &&
                   characters < spec.length) {
            converted.append(spec.length - characters, ' ');
        }
    }

    const char* resultType = spec.kind == CharacterCastKind::Text
        ? "text" : spec.kind == CharacterCastKind::Varchar
        ? "character varying" : "character";
    return ExprValue(resultType, std::move(converted), false);
}

ExprValue ExprEvaluator::evalCast(const CastExpr* e, const RowContext& ctx) const {
    if (e) {
        ExprValue v = eval(e->operand.get(), ctx);
        if (e->implicit && e->typeMods.empty()) {
            const auto target = ExprHelper::canonicalResultTypeName(e->typeName);
            if (target == "character" || target == "bpchar" || target == "bit") {
                if (target == "bit")
                    v = evalCast(nullptr,ctx,v,"bit varying");
                v.typeName = target;
                return v;
            }
        }
        std::string fullT = e->typeName;
        const bool arrayTarget=fullT.size()>=2 && fullT.compare(fullT.size()-2,2,"[]")==0;
        if (!e->typeMods.empty()) {
            if(arrayTarget)fullT.resize(fullT.size()-2);
            std::vector<std::string> modifiers;
            for (size_t i = 0; i < e->typeMods.size(); ++i) {
                std::string modifier = e->typeMods[i];
                if ((modifier == "+" || modifier == "-") &&
                    i + 1 < e->typeMods.size()) {
                    modifier += e->typeMods[++i];
                }
                if (modifier != ",") modifiers.push_back(std::move(modifier));
            }
            fullT += "(";
            for (size_t i = 0; i < modifiers.size(); ++i) {
                if (i) fullT += ",";
                fullT += modifiers[i];
            }
            fullT += ")";
            if(arrayTarget)fullT+="[]";
        }
        return evalCast(nullptr, ctx, v, fullT);
    }
    return ExprValue{};
}

struct BitCastSpec {
    bool matches = false;
    bool varying = false;
    bool hasLength = false;
    size_t length = 0;
};

static BitCastSpec parseBitCastSpec(const std::string& target) {
    BitCastSpec spec;
    std::string base = target;
    std::string modifier;
    const size_t open = target.find('(');
    if (open != std::string::npos) base = trimStr(target.substr(0, open));
    base = toLower(trimStr(base));
    if (base == "bit") {
        spec.matches = true;
    } else if (base == "bit varying" || base == "varbit") {
        spec.matches = true;
        spec.varying = true;
    } else {
        return spec;
    }
    if (open != std::string::npos) {
        if (target.back() != ')') {
            throw DbError("42601", "invalid bit type modifier");
        }
        modifier = trimStr(target.substr(open + 1,
                                         target.size() - open - 2));
        if (modifier.empty()) {
            throw DbError("42601", "invalid bit type modifier");
        }
        uint64_t parsed = 0;
        const auto result = std::from_chars(
            modifier.data(), modifier.data() + modifier.size(), parsed);
        if (result.ec != std::errc{} ||
            result.ptr != modifier.data() + modifier.size() || parsed == 0 ||
            parsed > kMaxSupportedBitStringLength) {
            throw DbError("22023", "length for type bit must be between 1 and " +
                                     std::to_string(kMaxSupportedBitStringLength));
        }
        spec.hasLength = true;
        spec.length = static_cast<size_t>(parsed);
    }
    return spec;
}

static ExprValue castToBitString(const ExprValue& value,
                                 const BitCastSpec& spec) {
    size_t targetLength = spec.length;
    if (!spec.varying && !spec.hasLength) targetLength = 1;

    std::string bits;
    if (isIntegerTypeName(value.typeName)) {
        long long parsed = 0;
        if (!parseInt64Exact(value.value, parsed)) {
            throw DbError("22P02", "invalid input syntax for type bit");
        }
        size_t sourceWidth = 64;
        const std::string sourceType = toLower(value.typeName);
        if (sourceType == "smallint" || sourceType == "int2") sourceWidth = 16;
        else if (sourceType == "integer" || sourceType == "int" ||
                 sourceType == "int4") sourceWidth = 32;
        if (!spec.hasLength && spec.varying) targetLength = sourceWidth;
        const uint64_t raw = static_cast<uint64_t>(parsed);
        bits.reserve(targetLength);
        for (size_t position = targetLength; position > 0; --position) {
            const size_t bitIndex = position - 1;
            const bool one = bitIndex < 64
                ? ((raw >> bitIndex) & 1U) != 0
                : parsed < 0;
            bits.push_back(one ? '1' : '0');
        }
    } else {
        if (isBitStringTypeName(value.typeName)) bits = value.value;
        else if (!decodeBitStringInput(value.value,bits))
            throw DbError("22P02", "invalid input syntax for type bit");
        if (bits.size() > kMaxSupportedBitStringLength ||
            std::any_of(bits.begin(), bits.end(),
                        [](char bit) { return bit != '0' && bit != '1'; })) {
            throw DbError("22P02", "invalid input syntax for type bit");
        }
        if (spec.hasLength || !spec.varying) {
            if (bits.size() > targetLength) bits.resize(targetLength);
            if (!spec.varying && bits.size() < targetLength) {
                bits.append(targetLength - bits.size(), '0');
            }
        }
    }
    return ExprValue(spec.varying ? "bit varying" : "bit",
                     std::move(bits), false);
}

// Helper overload used by BinaryOpExpr "::"
ExprValue ExprEvaluator::evalCast(const Expr*, const RowContext&,
                                  const ExprValue& v, const std::string& targetTypeName) const {
    std::string target = toLower(targetTypeName);
    {
        // :: type modifier lists arrive space-joined without an opening
        // parenthesis ("numeric 4 , 2)"). CAST nodes already contain the
        // opening parenthesis. Canonicalize both forms while preserving a
        // separated sign token in negative scales.
        const size_t close = target.rfind(')');
        if (close != std::string::npos) {
            const size_t open = target.find('(');
            const size_t modifierStart = open != std::string::npos
                ? open + 1 : target.find_first_of("0123456789+-");
            if (modifierStart != std::string::npos && close > modifierStart) {
                std::string base = trimStr(target.substr(
                    0, open != std::string::npos ? open : modifierStart));
                std::string modifiers;
                for (size_t i = modifierStart; i < close; ++i) {
                    if (!std::isspace(
                            static_cast<unsigned char>(target[i]))) {
                        modifiers.push_back(target[i]);
                    }
                }
                target = base + "(" + modifiers + ")" + trimStr(target.substr(close+1));
            }
        }
    }
    const auto geometryTarget=geometric_input_detail::builtinType(targetTypeName);
    const auto sourceType=ExprHelper::canonicalResultTypeName(v.typeName);
    if(!geometryTarget.empty() &&
        (v.isNull || sourceType==geometryTarget || sourceType=="unknown" || sourceType=="text" ||
         sourceType=="character varying" || sourceType=="varchar" || sourceType=="character" ||
         sourceType=="bpchar" || sourceType=="name")) {
        if(v.isNull)return ExprValue(geometryTarget,"",true);
        std::string normalized;
        if(!normalizeGeometryText(v.value,geometryTarget,normalized))
            throw DbError("22P02","invalid input syntax for type "+geometryTarget);
        return ExprValue(geometryTarget,std::move(normalized),false);
    }
    if (v.isNull) {
        ExprValue result(targetTypeName, "", true);
        if (parseCharacterCastSpec(target).kind != CharacterCastKind::None)
            result.collation = v.collation;
        return result;
    }
    if (target.size()>=2 && target.compare(target.size()-2,2,"[]")==0) {
        const auto elementTarget = target.substr(0,target.size()-2);
        const auto elementType = ExprHelper::canonicalResultTypeName(elementTarget);
        auto literal = sql_array_text::parse(v.value);
        std::function<std::string(const std::string&)> convert = [&](const std::string& source) {
            std::vector<std::string> elements;
            if (!parseArrayElements(source,elements)) throw DbError("22P02","malformed array literal: " + source);
            std::string output="{";
            for(size_t i=0;i<elements.size();++i){
                if(i)output+=',';
                const auto& token=elements[i];
                if(!token.empty() && token.front()=='{') output+=convert(token);
                else if(toLower(token)=="null") output+="NULL";
                else output+=arrayElemQuote(evalCast(nullptr,RowContext{},ExprValue("unknown",arrayElemUnquote(token),false),elementTarget).value);
            }
            return output+'}';
        };
        literal.body=convert(literal.body);
        return ExprValue(elementType+"[]",sql_array_text::render(literal),false);
    }

    if (target == "boolean" || target == "bool") return castToBoolean(v);
    if (target == "integer" || target == "int" || target == "int4")
        return castToInteger(v, IntegerCastTarget::Integer);
    if (target == "bigint" || target == "int8") {
        return castToInteger(v, IntegerCastTarget::BigInt);
    }
    if (target == "smallint" || target == "int2") {
        return castToInteger(v, IntegerCastTarget::SmallInt);
    }
    if (target == "real" || target == "float4") {
        const float converted = parseRealCastValue(v);
        return ExprValue("real", formatFloatingCastValue(converted), false);
    }
    if (target == "double precision" || target == "float8") {
        const double converted = parseDoubleCastValue(v);
        return ExprValue("double precision",
                         formatFloatingCastValue(converted), false);
    }
    const NumericCastSpec numericSpec = parseNumericCastSpec(target);
    if (numericSpec.matches) return castToNumeric(v, numericSpec);
    if (target == "money") return castToMoney(v);
    if (target == "interval") {
        validateTypedIntervalRange(v.value);
        return ExprValue("interval", v.value, false);
    }
    if (target == "uuid") return castToUuid(v);
    if (target == "bytea" || target == "blob") return castToBytea(v);
    if (target == "inet" || target == "cidr") {
        NetworkAddressValue address;
        const bool cidr = target == "cidr";
        if (!parseNetworkAddress(v.value, address, cidr)) {
            throw DbError("22P02", "invalid input syntax for type " + target);
        }
        return ExprValue(target, address.toString(cidr), false);
    }
    if (target == "macaddr" || target == "macaddr8") {
        const size_t length = target == "macaddr" ? 6 : 8;
        std::array<uint8_t, 8> address{};
        if (!parseMacAddress(v.value, length, address)) {
            throw DbError("22P02", "invalid input syntax for type " + target);
        }
        return ExprValue(target, formatMacAddress(address.data(), length), false);
    }
    if (target == "xml") {
        const auto validation = validateXml(v.value, XmlParseMode::Content);
        if (!validation.ok) {
            throw DbError("2200N", "invalid XML content: " +
                                     validation.message);
        }
        return ExprValue("xml", v.value, false);
    }
    const BitCastSpec bitSpec = parseBitCastSpec(target);
    if (bitSpec.matches) return castToBitString(v, bitSpec);
    const CharacterCastSpec characterSpec = parseCharacterCastSpec(target);
    if (characterSpec.kind != CharacterCastKind::None) {
        ExprValue result = castToCharacter(v, characterSpec);
        result.collation = v.collation;
        return result;
    }
    if (target == "date") return castToDate(v);
    if (target == "timestamp" || target == "timestamp without time zone")
        return castToTimestamp(v, "timestamp");
    if (target == "timestamptz" || target == "timestamp with time zone")
        return castToTimestamp(v, "timestamptz");
    if (target == "int4range" || target == "int8range")
        return castToIntegerRange(v, target);
    if (target == "numrange") return castToNumericRange(v);
    if (target == "daterange") return castToDateRange(v);
    if (target == "tsrange" || target == "tstzrange")
        return castToTimestampRange(v, target);

    // Default passthrough
    return ExprValue(targetTypeName, v.value, false);
}

// ----------------------------------------------------------------------------
// Function calls
// ----------------------------------------------------------------------------

void ExprEvaluator::registerFunction(const std::string& name, ScalarFunction fn) {
    std::string n = toLower(name);
    functions_[n] = std::move(fn);
    volatility_[n] = 'v';
}

void ExprEvaluator::registerFunction(const std::string& name, ScalarFunction fn, char volatility) {
    std::string n = toLower(name);
    functions_[n] = std::move(fn);
    volatility_[n] = volatility;
}

bool ExprEvaluator::hasFunction(const std::string& name) const {
    return functions_.find(toLower(name)) != functions_.end();
}

char ExprEvaluator::volatility(const std::string& name) const {
    auto it = volatility_.find(toLower(name));
    return it != volatility_.end() ? it->second : 'v';
}

namespace {
struct ResolvedScalarFunction {
    bool found = false;
    bool stored = false;
    std::string name;
    std::string schema;
    const QueryHostSetReturningProvider* provider=nullptr;
    StorageEngine::UDFInfo routine;
};

// A named callable keeps declaration metadata available after an AST call
// has been rebound to its private key. Reading its type never invokes it.
struct BoundScalarRoutine {
    StorageEngine* engine;
    std::string database;
    ResolvedScalarFunction resolved;
    const FunctionCallExpr* site;

    ExprValue operator()(const std::vector<ExprValue>& args) const {
        std::vector<std::string> values;
        std::vector<bool> nulls;
        for (const auto& arg : args) {
            values.push_back(arg.value);
            nulls.push_back(arg.isNull);
        }
        std::string value;
        bool isNull = false;
        if (!engine->callUDF(database, resolved.name, values, value, &isNull, &nulls,resolved.schema))
            throw DbError("22023", "stored-function evaluation failed: " + resolved.name);
        return ExprValue(ExprHelper::canonicalResultTypeName(resolved.routine.returnType),
                         value, isNull);
    }
};

ResolvedScalarFunction resolveScalarFunction(
    const ExprEvaluator& evaluator, const FunctionCallExpr* call,
    const std::string& database, StorageEngine* engine,
    const std::map<std::string,ScalarFunction,std::less<>>& registry) {
    if (!call) return {};
    // A bound callback is owned by this actual AST occurrence, not by an
    // SQL-visible name. Rebinding the same site preserves its engine/routine
    // metadata; another legal SQL routine with that spelling remains distinct.
    const auto registered=registry.find(toLower(call->funcName));
    if(call->schema.empty() && registered!=registry.end()) {
        const auto* bound=registered->second.target<BoundScalarRoutine>();
        if(bound && bound->site==call) {
            ResolvedScalarFunction owned;owned.found=true;
            owned.name=registered->first;owned.schema="pg_catalog";return owned;
        }
    }
    static const std::set<std::string> syntaxFunctions = {
        "coalesce", "nullif", "greatest", "least", "make_interval",
        "between", "not between", "like escape", "not like escape",
        "ilike escape", "not ilike escape", "similar to escape",
        "not similar to escape"
    };
    ResolvedScalarFunction result;
    // SQL syntax wrappers have spaces and are not routine identifiers.
    const std::string syntax = toLower(call->funcName);
    if (call->schema.empty() && syntaxFunctions.count(syntax)) {
        result.found = true;
        result.name = syntax;
        return result;
    }
    CatalogManager::QualifiedName name;
    if (!CatalogManager::parseQualifiedName(call->funcName, name, true) ||
        !name.schema.empty()) return result;
    std::string schema;
    if (!call->schema.empty()) {
        CatalogManager::QualifiedName decoded;
        if (!CatalogManager::parseQualifiedName(call->schema, decoded, true) ||
            !decoded.schema.empty()) return result;
        schema = decoded.name;
    }
    result.name = name.name;
    engine = engine ? engine : &g_engine;
    const auto path=functionNamespaceSearchPath(*engine,database,schema);
    const QueryHostSetReturningProvider* provider=nullptr;
    for(const auto& candidateSchema:path) {
        if(candidateSchema=="pg_catalog") {
            const auto callback=registry.find(toLower(result.name));
            const bool internalCallback=callback!=registry.end() && callback->second.target<BoundScalarRoutine>();
            if(result.name==toLower(result.name) && evaluator.hasFunction(result.name) && !internalCallback) {
                result.found=true;result.schema=candidateSchema;return result;
            }
            provider=queryHostSetReturningProvider(result.name);
            if(provider && provider->fixedSignature && call->args.size()==provider->arity && call->namedArgs.empty()) {
                result.provider=provider;result.schema=candidateSchema;return result;
            }
            continue;
        }
        if(database.empty())continue;
        auto routine=engine->getUDF(database,result.name,candidateSchema);
        if (!routine.expression.empty()) {
            size_t arity = routine.paramNames.size();
            if (arity == 1 && routine.paramNames.front().empty()) arity = 0;
            if (call->namedArgs.empty() && call->args.size() == arity) {
                // The registered array SRF has a polymorphic candidate.
                // An actual typed scalar overload is a better exact match
                // even when catalog occurs first. UNKNOWN chooses a string
                // category candidate when present; an array-only unknown
                // remains ambiguous. Never inspect an evaluated datum.
                const auto* polymorphic=queryHostSetReturningProvider(result.name);
                bool matches=true;
                if(schema.empty() && polymorphic && !polymorphic->fixedSignature) {
                    for(size_t i=0;i<call->args.size();++i) {
                        const auto type=ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedInputType(
                            call->args[i].get(),{},database,engine));
                        const auto declared=i<routine.paramTypes.size()
                            ?ExprHelper::canonicalResultTypeName(routine.paramTypes[i]):std::string{};
                        const auto* entry=TypeRegistry::instance().findType(declared);
                        const bool unknownString=type=="unknown" && entry && entry->category==TypeCategory::String;
                        if(!unknownString && type!=declared)matches=false;
                    }
                }
                if(matches) {
                    result.routine=std::move(routine);result.schema=candidateSchema;
                    result.found = result.stored = true;return result;
                }
            }
        }
    }
    result.provider=provider;
    // SUM/AVG's existing evaluator diagnostic concerns aggregate overload
    // resolution, not a missing scalar callback. Keep it at preparation so
    // an unused COALESCE/CASE arm still fails without evaluating any routine.
    // Quoted mixed-case names and explicit non-catalog schemas are distinct.
    if ((schema.empty() || schema == "pg_catalog") &&
        (result.name == "sum" || result.name == "avg") &&
        call->namedArgs.empty() && call->args.size() == 1) {
        const auto* literal = dynamic_cast<const LiteralExpr*>(call->args.front().get());
        if (literal && literal->typeName.empty() &&
            (isQuotedString(literal->value) || toLower(literal->value) == "null"))
            throw DbError("42725", "function " + result.name + "(unknown) is not unique");
    }
    return result;
}
} // namespace

bool ExprEvaluator::hasScalarFunction(const FunctionCallExpr* call,
                                      StorageEngine* engine) const {
    return resolveScalarFunction(*this, call, currentDB_, engine,functions_).found;
}

const QueryHostSetReturningProvider* ExprEvaluator::queryHostSetReturningRole(
    const FunctionCallExpr* call,StorageEngine* engine) const {
    return resolveScalarFunction(*this,call,currentDB_,engine,functions_).provider;
}

char ExprEvaluator::scalarFunctionVolatility(const FunctionCallExpr* call,
                                             StorageEngine* engine) const {
    const auto resolved = resolveScalarFunction(*this, call, currentDB_, engine,functions_);
    if (!resolved.found)
        throw DbError("42883", "function does not exist: " +
            (call ? call->funcName : std::string{}));
    if (resolved.stored) return resolved.routine.provolatile;
    // Syntax wrappers have no callback of their own; their volatility is
    // entirely that of their operand expressions, analyzed by the caller.
    return hasFunction(resolved.name) ? volatility(resolved.name) : 'i';
}

std::string ExprEvaluator::scalarFunctionResultType(const FunctionCallExpr* call,
                                                   StorageEngine* engine) const {
    const auto resolved = resolveScalarFunction(*this, call, currentDB_, engine,functions_);
    if (!resolved.found)
        throw DbError("42883", "function does not exist: " +
            (call ? call->funcName : std::string{}));
    if (resolved.stored)
        return ExprHelper::canonicalResultTypeName(resolved.routine.returnType);
    const auto callback = functions_.find(toLower(resolved.name));
    if (callback != functions_.end()) {
        if (const auto* bound = callback->second.target<BoundScalarRoutine>())
            return ExprHelper::canonicalResultTypeName(bound->resolved.routine.returnType);
    }
    return {};
}

std::string ExprEvaluator::scalarFunctionIdentity(const FunctionCallExpr* call,
                                                 StorageEngine* engine) const {
    auto resolved = resolveScalarFunction(*this, call, currentDB_, engine,functions_);
    if (!resolved.found)
        throw DbError("42883", "function does not exist: " +
            (call ? call->funcName : std::string{}));
    std::string database = currentDB_;
    engine = engine ? engine : &g_engine;
    if (!resolved.stored) {
        const auto callback = functions_.find(toLower(resolved.name));
        if (callback != functions_.end()) {
            if (const auto* bound = callback->second.target<BoundScalarRoutine>()) {
                resolved = bound->resolved;
                database = bound->database;
                engine = bound->engine;
            }
        }
    }
    const auto field = [](const std::string& value) {
        return std::to_string(value.size()) + ":" + value;
    };
    std::string key = resolved.stored ? "stored" : "builtin";
    key += field(std::to_string(reinterpret_cast<uintptr_t>(engine)));
    key += field(database) + field(resolved.schema) + field(resolved.name);
    key += field(std::to_string(call->args.size())) + field(std::to_string(call->namedArgs.size()));
    if (resolved.stored) {
        for (const auto& type : resolved.routine.paramTypes)
            key += field(ExprHelper::canonicalResultTypeName(type));
        key += field(ExprHelper::canonicalResultTypeName(resolved.routine.returnType));
    }
    return key;
}

void ExprEvaluator::bindScalarFunctions(Expr* expression, StorageEngine* engine) {
    engine = engine ? engine : &g_engine;
    std::function<void(Expr*)> visit = [&](Expr* node) {
        if (!node) return;
        if (auto* function = dynamic_cast<FunctionCallExpr*>(node)) {
            const auto resolved = resolveScalarFunction(*this, function, currentDB_, engine,functions_);
            if (!resolved.found && !function->setReturning)
                throw DbError("42883", "function does not exist: " + function->funcName);
            if (resolved.stored) {
                if (function->hasOver || function->distinct || function->filter ||
                    !function->orderBy.empty())
                    throw DbError("0A000", "scalar routine used as an aggregate or window function");
                std::string key = "__dbms_bound_routine_" + std::to_string(functions_.size());
                while (hasFunction(key)) key += '_';
                registerFunction(key, BoundScalarRoutine{engine, currentDB_, resolved,function},
                                 resolved.routine.provolatile);
                function->funcName = key;
            } else if(!function->setReturning) {
                function->funcName = resolved.name;
            }
            if(!function->setReturning)function->schema.clear();
            for (auto& arg : function->args) visit(arg.get());
            for (auto& arg : function->namedArgs) visit(arg.value.get());
            visit(function->filter.get());
            for (auto& item : function->over.partitionBy) visit(item.get());
            for (auto& item : function->over.orderBy) visit(item.first.get());
            visit(function->over.frameStart.get());
            visit(function->over.frameEnd.get());
        } else if (auto* unary = dynamic_cast<UnaryOpExpr*>(node)) {
            visit(unary->operand.get());
        } else if (auto* binary = dynamic_cast<BinaryOpExpr*>(node)) {
            visit(binary->left.get());
            if (binary->op != "::") visit(binary->right.get());
        } else if (auto* quantified = dynamic_cast<QuantifiedComparisonExpr*>(node)) {
            visit(quantified->left.get()); visit(quantified->right.get());
        } else if (auto* cast = dynamic_cast<CastExpr*>(node)) {
            visit(cast->operand.get());
        } else if (auto* conditional = dynamic_cast<CaseExpr*>(node)) {
            visit(conditional->switchExpr.get());
            for (auto& arm : conditional->whenClauses) {
                visit(arm.first.get()); visit(arm.second.get());
            }
            visit(conditional->elseExpr.get());
        } else if (auto* array = dynamic_cast<ArrayExpr*>(node)) {
            for (auto& item : array->elements) visit(item.get());
        } else if (auto* row = dynamic_cast<RowExpr*>(node)) {
            for (auto& item : row->elements) visit(item.get());
        }
        // Subquery expressions have an independent query namespace and are
        // prepared by the query host, never evaluated as scalar AST nodes.
    };
    visit(expression);
}

ExprValue ExprEvaluator::evalFunctionCall(const FunctionCallExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    if(e->setReturning)throw DbError("0A000","set-returning routine requires its query-host receiver");
    std::string name = toLower(e->funcName);
    // Keep polymorphic array builtin results typed at the same boundary as
    // ARRAY constructors. The descriptor is read before any argument runs;
    // a brace-looking TEXT value never determines the result type.
    const auto declaredResult=arrayExpressionType(e,ctx,currentDB_,this);

    // COALESCE is syntax-like in SQL: stop as soon as the first non-NULL
    // argument is found, so unused arguments are never evaluated.
    if (name == "coalesce") {
        const std::string resultCollation = explicitResultCollation(e);
        for (const auto& argument : e->args) {
            ExprValue value = eval(argument.get(), ctx);
            if (!value.isNull) {
                value.collation = mergeExplicitCollations(
                    value.collation, resultCollation);
                return value;
            }
        }
        ExprValue result("unknown", "", true);
        result.collation = resultCollation;
        return result;
    }

    if (e->schema.empty() && (name == "between" || name == "not between") &&
        functions_.find(name) == functions_.end()) {
        if (e->args.size() != 3 && e->args.size() != 4) return ExprValue("boolean", "f", false);
        const bool negate = name == "not between";
        for (const auto& argument : e->args)
            between_input_detail::validateLiteralCasts(argument.get());
        const std::function<bool(const Expr*)> nullInput = [&](const Expr* expression) {
            if (!expression || expression->preparedSubquery) return false;
            if (between_input_detail::nullConstant(expression)) return true;
            // Only actual statement inputs are immutable after Bind. Runtime
            // rows/aggregate cells and metadata placeholders keep peer demand;
            // never execute an expression to infer preparation-time NULL.
            if (const auto* parameter = dynamic_cast<const ParameterExpr*>(expression))
                return parameter->origin == ParameterOrigin::StatementInput &&
                    ctx.parameter(parameter->slot).isNull;
            if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
                return cast->typeMods.empty() && between_input_detail::primitiveType(cast->typeName) &&
                    nullInput(cast->operand.get());
            if (const auto* cast = dynamic_cast<const BinaryOpExpr*>(expression); cast && cast->op == "::") {
                const auto* target = dynamic_cast<const LiteralExpr*>(cast->right.get());
                return target && between_input_detail::primitiveType(target->value) && nullInput(cast->left.get());
            }
            return false;
        };
        const auto input = [&](size_t position) {
            ExprValue value = eval(e->args[position].get(), ctx);
            const auto* literal = dynamic_cast<const LiteralExpr*>(e->args[position].get());
            if (literal && literal->typeName.empty() && !literal->preparedSubquery &&
                (isQuotedString(literal->value) || toLower(literal->value) == "null"))
                value.typeName = "unknown";
            return value;
        };
        const auto preparedType = [](const std::string& raw) {
            const auto type = ExprHelper::canonicalResultTypeName(raw);
            return isBitStringTypeName(type) || type == "smallint" || type == "integer" || type == "bigint";
        };
        const auto comparison = [&](const std::string& operation, size_t bound) {
            // A genuine constant NULL makes a strict comparison NULL during
            // planning. Do not invoke its otherwise volatile peer just to
            // rediscover that fact; row/provider NULLs are not constants.
            if (nullInput(e->args[0].get()) || nullInput(e->args[bound].get())) {
                // Pure peers retain planning errors even under a strict NULL
                // comparison. No row/parameter/routine/child is evaluated.
                ExprEvaluator pure;
                if (between_input_detail::constant(e->args[0].get()))
                    (void)pure.eval(e->args[0].get(), RowContext{});
                if (between_input_detail::constant(e->args[bound].get()))
                    (void)pure.eval(e->args[bound].get(), RowContext{});
                const auto leftType = arrayExpressionType(e->args[0].get(), ctx, currentDB_, this);
                const auto rightType = arrayExpressionType(e->args[bound].get(), ctx, currentDB_, this);
                if (preparedType(leftType) || preparedType(rightType))
                    (void)resolveComparison(operation, leftType, rightType);
                return ExprValue("boolean", "", true);
            }
            // Each logical comparison owns its actual left occurrence. A
            // demanded second pair repeats the left expression, not its value.
            const auto left = input(bound == 2 && e->args.size() == 4 ? 3 : 0), right = input(bound);
            return preparedType(left.typeName) || preparedType(right.typeName)
                ? comparePrepared(resolveComparison(operation, left.typeName, right.typeName), left, right)
                : applyComparison(operation, left, right);
        };
        const auto lower = comparison(negate ? "<" : ">=", 1);
        if (!lower.isNull && lower.asBool() == negate)
            return ExprValue("boolean", negate ? "t" : "f", false);
        const auto upper = comparison(negate ? ">" : "<=", 2);
        if (!upper.isNull && upper.asBool() == negate)
            return ExprValue("boolean", negate ? "t" : "f", false);
        if (lower.isNull || upper.isNull) return ExprValue("boolean", "", true);
        return ExprValue("boolean", negate ? "f" : "t", false);
    }

    std::vector<ExprValue> args;
    for (const auto& a : e->args) args.push_back(eval(a.get(), ctx));

    if (name == "timezone" && args.size() == 2) {
        // Unknown SQL literals select the preferred timestamptz overload;
        // an explicitly typed TEXT value must not gain that implicit cast.
        const auto* literal = dynamic_cast<const LiteralExpr*>(e->args[1].get());
        const bool unknownLiteral = literal && literal->typeName.empty() &&
            (isQuotedString(literal->value) || toLower(literal->value) == "null");
        if (unknownLiteral || toLower(args[1].typeName) == "date")
            args[1] = evalCast(nullptr, ctx, args[1], "timestamptz");
    }

    // SUM and AVG have several numeric overloads.  PostgreSQL cannot select
    // one when their sole argument is an untyped string or NULL literal; a
    // cast makes the call unambiguous (and may then produce 42883 for an
    // unsupported type).  Preserve that distinction instead of treating the
    // literal as text before function resolution.
    if ((name == "sum" || name == "avg") && e->args.size() == 1 &&
        e->args.front() && e->args.front()->type == ExprType::Literal) {
        const auto* literal =
            static_cast<const LiteralExpr*>(e->args.front().get());
        const std::string literalName = toLower(literal->value);
        if (literal->typeName.empty() &&
            (isQuotedString(literal->value) || literalName == "null")) {
            throw std::runtime_error(
                "function " + name +
                "(unknown) is not unique (SQLSTATE 42725)");
        }
    }

    // SIMILAR TO ... ESCAPE / NOT SIMILAR TO ... ESCAPE (parser wraps the
    // three-operand form into a FunctionCallExpr, mirroring LIKE ESCAPE).
    // LIKE ... ESCAPE / NOT LIKE ... ESCAPE (parser wraps the three-operand
    // form into a FunctionCallExpr). The shared SQL matcher preserves explicit
    // escape literals and determines trailing-escape demand during matching.
    if (name == "like escape" || name == "not like escape" ||
        name == "ilike escape" || name == "not ilike escape") {
        if (args.size()==3) { coercePatternTextArgument(args[1]);coercePatternTextArgument(args[2]); }
        if (args.size() < 3 || args[1].isNull || args[2].isNull) {
            return ExprValue("boolean", "", true);
        }
        validatePatternEscapeInput(args[2]);
        if (args[0].isNull) return ExprValue("boolean", "", true);
        const bool foldCase = name == "ilike escape" ||
                              name == "not ilike escape";
        const bool bytes = ExprHelper::canonicalResultTypeName(args[0].typeName) == "bytea";
        const std::string locale = collation::normalizeName(args[0].collation.empty()
            ? args[1].collation : args[0].collation);
        bool m = bytes ? likeMatchWithEscape(parseByteaOrThrow(args[0]).bytes(),
            parseByteaOrThrow(args[1]).bytes(), parseByteaOrThrow(args[2]).bytes(), false, true)
            : likeMatchWithEscape(args[0].value, args[1].value, args[2].value, foldCase,
                                  false,locale == "c" || locale == "posix");
        if (name.rfind("not ", 0) == 0) m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }

    if (name == "similar to escape" || name == "not similar to escape") {
        if (args.size()==3) { coercePatternTextArgument(args[1]);coercePatternTextArgument(args[2]); }
        if (args.size() < 3 || args[1].isNull || args[2].isNull) {
            return ExprValue("boolean", "", true);
        }
        validatePatternEscapeInput(args[2]);
        if (args[0].isNull) return ExprValue("boolean", "", true);
        const std::string locale = collation::normalizeName(args[0].collation.empty()
            ? args[1].collation : args[0].collation);
        bool m = similarToMatchEscape(args[0].value,args[1].value,args[2].value,
                                     locale == "c" || locale == "posix");
        if (name[0] == 110) m = !m;  // "not ..."
        return ExprValue("boolean", m ? "t" : "f", false);
    }

    if (name == "make_interval") {
        long long mi_months = 0, mi_days = 0, mi_micros = 0;
        auto addIntegerField = [&](const std::string& key,
                                   const long long value) {
            if (key == "years")
                return addScaledIntervalField(mi_months, value, 12);
            if (key == "months")
                return addScaledIntervalField(mi_months, value, 1);
            if (key == "weeks")
                return addScaledIntervalField(mi_days, value, 7);
            if (key == "days")
                return addScaledIntervalField(mi_days, value, 1);
            if (key == "hours")
                return addScaledIntervalField(
                    mi_micros, value, 3600000000LL);
            if (key == "mins")
                return addScaledIntervalField(
                    mi_micros, value, 60000000LL);
            return true;
        };
        auto addIntegerArgument = [&](const std::string& key,
                                      const ExprValue& argument) {
            long long value = 0;
            return parseInt64Exact(argument.value, value) &&
                   addIntegerField(key, value);
        };

        static const char* positionalNames[] = {
            "years", "months", "weeks", "days", "hours", "mins",
            "secs"};
        if (args.size() > std::size(positionalNames))
            return ExprValue("interval", "", true);
        for (size_t i = 0; i < args.size(); ++i) {
            const bool valid = !args[i].isNull &&
                (i == 6
                     ? addIntervalSeconds(mi_micros, args[i].value)
                     : addIntegerArgument(positionalNames[i], args[i]));
            if (!valid) {
                return ExprValue("interval", "", true);
            }
        }

        for (const auto& na : e->namedArgs) {
            ExprValue nv = eval(na.value.get(), ctx);
            if (nv.isNull)
                return ExprValue("interval", "", true);
            std::string k = toLower(na.name);
            if (k != "years" && k != "months" && k != "weeks" &&
                k != "days" && k != "hours" && k != "mins" &&
                k != "secs") {
                throw std::runtime_error(
                    "function make_interval has no argument named \"" + k +
                    "\" (SQLSTATE 42883)");
            }
            const bool valid = k == "secs"
                ? addIntervalSeconds(mi_micros, nv.value)
                : addIntegerArgument(k, nv);
            if (!valid)
                return ExprValue("interval", "", true);
        }
        return ExprValue("interval", intervalToText(mi_months, mi_days, mi_micros), false);
    }
    auto it = functions_.find(name);
    if (it != functions_.end()) {
        std::string argumentCollation;
        for (const ExprValue& argument : args) {
            if (isCollatableExprType(argument.typeName))
                argumentCollation = mergeExplicitCollations(
                    argumentCollation, argument.collation);
        }
        ExprValue result = it->second(args);
        if(declaredResult.size()>=2 && declaredResult.compare(declaredResult.size()-2,2,"[]")==0)
            result=evalCast(nullptr,ctx,result,declaredResult);
        if (isCollatableExprType(result.typeName))
            result.collation = mergeExplicitCollations(
                result.collation, argumentCollation);
        return result;
    }

    // Built-in fallback for common functions even if not registered
    if (name == "nullif") {
        if (args.size() < 2) return ExprValue("unknown", "", true);
        ExprValue first = args[0];
        first.collation = mergeExplicitCollations(first.collation,
                                                   args[1].collation);
        if (first.isNull || args[1].isNull) return first;
        if (applyComparison("=", first, args[1]).asBool()) {
            first.value.clear();
            first.isNull = true;
        }
        return first;
    }
    if (name == "greatest") {
        const std::string resultCollation = explicitResultCollation(e);
        ExprValue best("unknown", "", true);
        for (auto a : args) {
            if (isCollatableExprType(a.typeName))
                a.collation = mergeExplicitCollations(a.collation,
                                                        resultCollation);
            if (a.isNull) continue;
            if (best.isNull || compareValues(a, best) > 0) best = a;
        }
        if (best.isNull) best.collation = resultCollation;
        return best;
    }
    if (name == "least") {
        const std::string resultCollation = explicitResultCollation(e);
        ExprValue best("unknown", "", true);
        for (auto a : args) {
            if (isCollatableExprType(a.typeName))
                a.collation = mergeExplicitCollations(a.collation,
                                                        resultCollation);
            if (a.isNull) continue;
            if (best.isNull || compareValues(a, best) < 0) best = a;
        }
        if (best.isNull) best.collation = resultCollation;
        return best;
    }
    // PG 42883: function <name>(<argtypes>) does not exist.
    {
        auto pgTypeName = [](const ExprValue& v) -> std::string {
            std::string t = toLower(v.typeName);
            if (t.empty() || t == "unknown") {
                if (!v.value.empty() &&
                    v.value.find_first_not_of("0123456789-+") == std::string::npos &&
                    v.value != "-" && v.value != "+")
                    return "integer";
                return "unknown";
            }
            if (t == "int" || t == "int4" || t == "int2" || t == "int8" ||
                t == "bigint" || t == "smallint")
                return "integer";
            if (t == "numeric" || t == "decimal") return "numeric";
            if (t == "float" || t == "double" || t == "float8" || t == "float4" ||
                t == "real")
                return "double precision";
            if (t == "varchar" || t == "character varying" || t == "char" ||
                t == "bpchar" || t == "text")
                return "text";
            return t;
        };
        std::string sig;
        for (size_t ai = 0; ai < args.size(); ++ai) {
            if (ai) sig += ", ";
            sig += pgTypeName(args[ai]);
        }
        throw std::runtime_error(
            "function " + name + "(" + sig + ") does not exist (SQLSTATE 42883)");
    }
    return ExprValue{};
}

// ----------------------------------------------------------------------------
// Array / Row expressions
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalArrayExpr(const ArrayExpr* array, const RowContext& row) const {
    auto elementType=array->elementType;
    if(elementType.empty()){
        auto type=arrayExpressionType(array,row,currentDB_,this);elementType=type.substr(0,type.size()-2);
    }
    std::string output="{";
    bool nested=array->nestedElements;
    if(array->elementType.empty())for(const auto& item:array->elements){
        const auto type=arrayExpressionType(item.get(),row,currentDB_,this);
        if(type.size()>=2 && type.compare(type.size()-2,2,"[]")==0)nested=true;
    }
    size_t nestedWidth=0;
    std::vector<sql_array_text::Dimension> childDimensions;
    for(size_t i=0;i<array->elements.size();++i){
        ExprValue value=eval(array->elements[i].get(),row);
        if(i)output+=',';
        if(nested){
            if(value.isNull)throw DbError("2202E","multidimensional arrays must have matching dimensions");
            if (elementType != "bit" || value.typeName != "bit[]")
                value=evalCast(nullptr,row,value,elementType+"[]");
            std::vector<std::string> elements;
            if(!parseArrayElements(value.value,elements))throw DbError("22P02","malformed array literal");
            if(i && nestedWidth!=elements.size())throw DbError("2202E","multidimensional arrays must have matching dimensions");
            const auto child=sql_array_text::parse(value.value);
            if(i && child.dimensions!=childDimensions)throw DbError("2202E","multidimensional arrays must have matching dimensions");
            childDimensions=child.dimensions;
            nestedWidth=elements.size();output+=child.body;
        } else {
            if (elementType == "bit") {
                // ARRAY chooses a common element type without a typmod.
                // The scalar explicit ::bit cast instead defaults to bit(1)
                // and must not truncate already typed constructor elements.
                value=evalCast(nullptr,row,value,"bit varying");
                value.typeName="bit";
            } else {
                value=evalCast(nullptr,row,value,elementType);
            }
            output+=value.isNull?"NULL":arrayElemQuote(value.value);
        }
    }
    const auto result=[&](std::string value){ExprValue cell(elementType+"[]",std::move(value));cell.collation=explicitResultCollation(array);return cell;};
    if(nested && nestedWidth==0)return result("{}");
    if(!arrayShapeOf(output+'}'))throw DbError("2202E","multidimensional arrays must have matching dimensions");
    if(nested) {
        auto literal=sql_array_text::parse(output+'}');
        for(size_t i=0;i<childDimensions.size();++i)literal.dimensions.at(i+1).lower=childDimensions[i].lower;
        return result(sql_array_text::render(literal));
    }
    return result(output+'}');
}

ExprValue ExprEvaluator::evalQuantified(const QuantifiedComparisonExpr* expression,const RowContext& row) const {
    if (!expression || !expression->comparison) throw DbError("XX000","quantified comparison was not prepared");
    if (expression->right && expression->right->preparedSubquery) {
        if (!quantifiedExecutor_) throw DbError("0A000","quantified SQL requires its prepared query cursor");
        return quantifiedExecutor_(expression,row);
    }
    // Scalar-array arguments are evaluated once, completely, in their
    // ordinary left-to-right order before element comparison may stop.
    const auto left=eval(expression->left.get(),row);
    const auto array=eval(expression->right.get(),row);
    if (array.isNull) return ExprValue("boolean","",true);
    std::vector<std::string> elements;
    std::function<void(const std::string&)> flatten=[&](const std::string& input) {
        std::vector<std::string> fields;
        if(!parseArrayElements(input,fields))throw DbError("22P02","malformed array literal");
        for(const auto& field:fields) {
            if(!field.empty() && field.front()=='{')flatten(field);
            else elements.push_back(field);
        }
    };
    flatten(array.value);
    const bool all=expression->quantifier==QuantifiedComparisonExpr::Quantifier::All;
    bool unknown=false;
    for (const auto& token:elements) {
        const auto text=trimStr(token);
        const bool quoted=text.size()>=2 && text.front()=='"' && text.back()=='"';
        const std::string arrayType=common_type_detail::canonical(array.typeName);
        if(!common_type_detail::array(arrayType))throw DbError("XX000","quantified ARRAY lost its typed datum");
        ExprValue right(arrayType.substr(0,arrayType.size()-2),arrayElemUnquote(text),!quoted && toLower(text)=="null");
        right.collation=array.collation;
        const auto truth=comparePrepared(*expression->comparison,left,right);
        if (truth.isNull) unknown=true;
        else if (truth.asBool()!=all) return ExprValue("boolean",all?"f":"t");
    }
    return unknown?ExprValue("boolean","",true):ExprValue("boolean",all?"t":"f");
}

std::vector<ExprValue> ExprEvaluator::arrayElements(const ExprValue& array) {
    const std::string type=common_type_detail::canonical(array.typeName);
    if(!common_type_detail::array(type))throw DbError("42809","array receiver requires a declared array datum");
    std::vector<ExprValue> elements;
    if(array.isNull)return elements;
    const auto elementType=type.substr(0,type.size()-2);
    std::function<void(const std::string&)> flatten=[&](const std::string& input) {
        std::vector<std::string> fields;
        if(!parseArrayElements(input,fields))throw DbError("22P02","malformed array literal");
        for(const auto& field:fields) {
            if(!field.empty() && field.front()=='{')flatten(field);
            else {
                const auto text=trimStr(field);
                const bool quoted=text.size()>=2 && text.front()=='"' && text.back()=='"';
                ExprValue value(elementType,arrayElemUnquote(text),!quoted && toLower(text)=="null");
                value.collation=array.collation;elements.push_back(std::move(value));
            }
        }
    };
    flatten(array.value);return elements;
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
    auto fail = [&]() {
        out.clear();
        return false;
    };
    uint32_t buffer = 0;
    int position = 0;
    int padding = 0;
    for (char c : in) {
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') continue;

        int decoded = 0;
        if (c == '=') {
            if (padding == 0) {
                if (position == 2)
                    padding = 1;
                else if (position == 3)
                    padding = 2;
                else
                    return fail();
            }
        } else {
            decoded = val(c);
            if (decoded < 0) return fail();
        }

        buffer = (buffer << 6) | static_cast<uint32_t>(decoded);
        if (++position == 4) {
            out.push_back(static_cast<char>((buffer >> 16) & 0xff));
            if (padding == 0 || padding > 1)
                out.push_back(static_cast<char>((buffer >> 8) & 0xff));
            if (padding == 0 || padding > 2)
                out.push_back(static_cast<char>(buffer & 0xff));
            buffer = 0;
            position = 0;
        }
    }
    if (position != 0) return fail();
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

static uint32_t byteaCrc(const std::string& bytes, uint32_t polynomial) {
    uint32_t crc = 0xffffffffU;
    for (const unsigned char byte : bytes) {
        crc ^= byte;
        for (unsigned bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ ((crc & 1U) ? polynomial : 0U);
    }
    return crc ^ 0xffffffffU;
}

// UTF-8 aware helpers: total characters and char-index -> byte offset.
static size_t utf8CharCount(const std::string& s) {
    size_t n = 0;
    for (unsigned char c : s) if ((c & 0xC0) != 0x80) ++n;
    return n;
}

static bool isBlankPaddedCharacterType(const std::string& typeName) {
    const std::string type = toLower(typeName);
    return type == "char" || type == "character" || type == "bpchar" ||
           type.rfind("char(", 0) == 0 ||
           type.rfind("character(", 0) == 0 ||
           type.rfind("bpchar(", 0) == 0;
}

static size_t logicalCharacterByteLength(const ExprValue& value) {
    size_t length = value.value.size();
    if (isBlankPaddedCharacterType(value.typeName)) {
        while (length > 0 && value.value[length - 1] == ' ') --length;
    }
    return length;
}

static std::string textArgumentValue(const ExprValue& value) {
    return value.value.substr(0, logicalCharacterByteLength(value));
}

static size_t utf8ByteAt(const std::string& s, size_t charIdx) {
    size_t n = 0, b = 0;
    while (b < s.size()) {
        if (n == charIdx) return b;
        unsigned char c = static_cast<unsigned char>(s[b]);
        b += (c < 0x80) ? 1 : ((c & 0xE0) == 0xC0) ? 2 : ((c & 0xF0) == 0xE0) ? 3 : 4;
        ++n;
    }
    return s.size();
}

static size_t utf8StartByte(const std::string& text, int64_t oneBasedStart) {
    if (oneBasedStart <= 1) return 0;
    const uint64_t characterIndex =
        static_cast<uint64_t>(oneBasedStart - 1);
    const size_t characters = utf8CharCount(text);
    if (characterIndex >= characters) return text.size();
    return utf8ByteAt(text, static_cast<size_t>(characterIndex));
}

static bool decodeFirstUtf8CodePoint(const std::string& text,
                                     uint32_t& codePoint) {
    if (text.empty()) return false;
    const auto byte = [&](size_t index) {
        return static_cast<unsigned char>(text[index]);
    };
    const unsigned char first = byte(0);
    if (first < 0x80) {
        codePoint = first;
        return true;
    }
    auto continuation = [&](size_t index) {
        return index < text.size() && (byte(index) & 0xc0) == 0x80;
    };
    if (first >= 0xc2 && first <= 0xdf && continuation(1)) {
        codePoint = ((first & 0x1f) << 6) | (byte(1) & 0x3f);
        return true;
    }
    if (first >= 0xe0 && first <= 0xef && continuation(1) &&
        continuation(2) && !(first == 0xe0 && byte(1) < 0xa0) &&
        !(first == 0xed && byte(1) >= 0xa0)) {
        codePoint = ((first & 0x0f) << 12) |
                    ((byte(1) & 0x3f) << 6) | (byte(2) & 0x3f);
        return true;
    }
    if (first >= 0xf0 && first <= 0xf4 && continuation(1) &&
        continuation(2) && continuation(3) &&
        !(first == 0xf0 && byte(1) < 0x90) &&
        !(first == 0xf4 && byte(1) > 0x8f)) {
        codePoint = ((first & 0x07) << 18) |
                    ((byte(1) & 0x3f) << 12) |
                    ((byte(2) & 0x3f) << 6) | (byte(3) & 0x3f);
        return true;
    }
    return false;
}

static std::string encodeUtf8CodePoint(uint32_t codePoint) {
    if (codePoint == 0 || codePoint > 0x10ffff ||
        (codePoint >= 0xd800 && codePoint <= 0xdfff)) {
        return "";
    }
    std::string result;
    if (codePoint <= 0x7f) {
        result.push_back(static_cast<char>(codePoint));
    } else if (codePoint <= 0x7ff) {
        result.push_back(static_cast<char>(0xc0 | (codePoint >> 6)));
        result.push_back(static_cast<char>(0x80 | (codePoint & 0x3f)));
    } else if (codePoint <= 0xffff) {
        result.push_back(static_cast<char>(0xe0 | (codePoint >> 12)));
        result.push_back(
            static_cast<char>(0x80 | ((codePoint >> 6) & 0x3f)));
        result.push_back(static_cast<char>(0x80 | (codePoint & 0x3f)));
    } else {
        result.push_back(static_cast<char>(0xf0 | (codePoint >> 18)));
        result.push_back(
            static_cast<char>(0x80 | ((codePoint >> 12) & 0x3f)));
        result.push_back(
            static_cast<char>(0x80 | ((codePoint >> 6) & 0x3f)));
        result.push_back(static_cast<char>(0x80 | (codePoint & 0x3f)));
    }
    return result;
}

static std::vector<std::string> splitUtf8Characters(
    const std::string& text) {
    std::vector<std::string> result;
    const size_t count = utf8CharCount(text);
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const size_t begin = utf8ByteAt(text, i);
        const size_t end = utf8ByteAt(text, i + 1);
        result.push_back(text.substr(begin, end - begin));
    }
    return result;
}

static std::string trimUtf8Characters(const std::string& text,
                                      const std::string& trimCharacters,
                                      bool trimLeading,
                                      bool trimTrailing) {
    const std::vector<std::string> input = splitUtf8Characters(text);
    const std::vector<std::string> trimSet =
        splitUtf8Characters(trimCharacters);
    const auto shouldTrim = [&](const std::string& character) {
        return std::find(trimSet.begin(), trimSet.end(), character) !=
               trimSet.end();
    };
    size_t begin = 0;
    size_t end = input.size();
    if (trimLeading)
        while (begin < end && shouldTrim(input[begin])) ++begin;
    if (trimTrailing)
        while (end > begin && shouldTrim(input[end - 1])) --end;
    const size_t beginByte = utf8ByteAt(text, begin);
    const size_t endByte = utf8ByteAt(text, end);
    return text.substr(beginByte, endByte - beginByte);
}

static ExprValue evaluateTextSubstring(const std::vector<ExprValue>& args) {
    if (args.empty()) return ExprValue("text", "", true);
    const bool byteaInput = isCanonicalByteaType(args[0].typeName);
    const bool bitInput = isBitStringTypeName(args[0].typeName);
    const char* resultType = byteaInput ? "bytea" : bitInput ? "bit" : "text";
    if (args[0].isNull)
        return ExprValue(resultType, "", true);
    const std::string byteInput = byteaInput
        ? parseByteaOrThrow(args[0]).bytes() : std::string();
    const std::string input = textArgumentValue(args[0]);
    if (args.size() < 2) {
        if (byteaInput) {
            return ExprValue(
                "bytea", ByteaValue::fromBytes(byteInput).toString(), false);
        }
        return ExprValue(bitInput ? "bit" : "text", input, false);
    }
    if (args[1].isNull || (args.size() >= 3 && args[2].isNull))
        return ExprValue(resultType, "", true);

    long long from = 0;
    if (!parseInt64Exact(args[1].value, from))
        return ExprValue("text", "", true);
    const bool hasLength = args.size() >= 3;
    long long length = 0;
    if (hasLength) {
        if (!parseInt64Exact(args[2].value, length))
            return ExprValue("text", "", true);
        if (length < 0) {
            throw std::runtime_error(
                "negative substring length not allowed (SQLSTATE 22011)");
        }
    }

    const size_t total = byteaInput ? byteInput.size()
        : bitInput ? input.size() : utf8CharCount(input);
    const __int128 requestedEnd = hasLength
        ? static_cast<__int128>(from) + length
        : static_cast<__int128>(total) + 1;
    const __int128 requestedStart = std::max<__int128>(from, 1);
    if (requestedStart > static_cast<__int128>(total))
        return ExprValue(resultType,
                         byteaInput ? "\\x" : "", false);

    const size_t beginCharacter =
        static_cast<size_t>(requestedStart - 1);
    __int128 endCharacterWide = requestedEnd - 1;
    if (endCharacterWide < static_cast<__int128>(beginCharacter))
        endCharacterWide = beginCharacter;
    if (endCharacterWide > static_cast<__int128>(total))
        endCharacterWide = total;
    const size_t endCharacter = static_cast<size_t>(endCharacterWide);
    const size_t beginByte = byteaInput || bitInput
        ? beginCharacter : utf8ByteAt(input, beginCharacter);
    const size_t endByte = byteaInput || bitInput
        ? endCharacter : utf8ByteAt(input, endCharacter);
    if (byteaInput) {
        return ExprValue(
            "bytea",
            ByteaValue::fromBytes(
                byteInput.substr(beginByte, endByte - beginByte)).toString(),
            false);
    }
    return ExprValue(
        bitInput ? "bit" : "text",
        input.substr(beginByte, endByte - beginByte), false);
}

static ExprValue evaluateByteaTrim(const std::vector<ExprValue>& args,
                                   bool trimLeading,
                                   bool trimTrailing) {
    if (args.size() < 2) {
        throw std::runtime_error(
            "function does not exist for bytea without a byte set "
            "(SQLSTATE 42883)");
    }
    if (args[0].isNull || args[1].isNull)
        return ExprValue("bytea", "", true);
    std::string bytes = parseByteaOrThrow(args[0]).bytes();
    const std::string trimBytes = parseByteaOrThrow(args[1]).bytes();
    const auto shouldTrim = [&](unsigned char byte) {
        return std::find_if(
                   trimBytes.begin(), trimBytes.end(),
                   [&](unsigned char candidate) { return candidate == byte; }) !=
               trimBytes.end();
    };
    size_t begin = 0;
    size_t end = bytes.size();
    if (trimLeading)
        while (begin < end && shouldTrim(
               static_cast<unsigned char>(bytes[begin]))) ++begin;
    if (trimTrailing)
        while (end > begin && shouldTrim(
               static_cast<unsigned char>(bytes[end - 1]))) --end;
    return ExprValue(
        "bytea",
        ByteaValue::fromBytes(bytes.substr(begin, end - begin)).toString(),
        false);
}

static ExprValue evaluateTextPad(const std::vector<ExprValue>& args,
                                 bool padLeft) {
    if (args.size() < 2 || args[0].isNull || args[1].isNull ||
        (args.size() >= 3 && args[2].isNull)) {
        return ExprValue("text", "", true);
    }
    const int32_t requestedLength = parseInt32Argument(args[1]);
    if (requestedLength <= 0) return ExprValue("text", "", false);

    const size_t targetLength = static_cast<size_t>(requestedLength);
    const std::string input = textArgumentValue(args[0]);
    const size_t inputLength = utf8CharCount(input);
    if (inputLength >= targetLength) {
        return ExprValue(
            "text", input.substr(0, utf8ByteAt(input, targetLength)),
            false);
    }

    const std::string fill = args.size() >= 3
        ? textArgumentValue(args[2]) : " ";
    if (fill.empty()) return ExprValue("text", input, false);
    std::vector<std::string> fillCharacters;
    const size_t fillLength = utf8CharCount(fill);
    fillCharacters.reserve(fillLength);
    for (size_t i = 0; i < fillLength; ++i) {
        const size_t begin = utf8ByteAt(fill, i);
        const size_t end = utf8ByteAt(fill, i + 1);
        fillCharacters.push_back(fill.substr(begin, end - begin));
    }
    if (fillCharacters.empty())
        return ExprValue("text", input, false);

    const size_t needed = targetLength - inputLength;
    const size_t completeCycles = needed / fillCharacters.size();
    const size_t remainder = needed % fillCharacters.size();
    __int128 paddingBytes =
        static_cast<__int128>(completeCycles) * fill.size();
    for (size_t i = 0; i < remainder; ++i)
        paddingBytes += fillCharacters[i].size();
    if (paddingBytes + input.size() > kMaxTextPayload) {
        throw std::runtime_error(
            "requested text length exceeds the limit (SQLSTATE 54000)");
    }

    std::string padding;
    padding.reserve(static_cast<size_t>(paddingBytes));
    for (size_t i = 0; i < needed; ++i)
        padding += fillCharacters[i % fillCharacters.size()];
    return ExprValue(
        "text", padLeft ? padding + input : input + padding,
        false);
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
    const auto value=trimStr(text);
    if(value.empty() || (value.front()!='{' && value.front()!='['))return false;
    try{out=sql_array_text::parse(value).elements;return true;}
    catch(const DbError&){return false;}
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

static std::optional<std::vector<size_t>> arrayShapeOf(const std::string& value) {
    std::vector<std::string> elements;
    if(!parseArrayElements(value,elements))return {};
    std::vector<size_t> shape{elements.size()};
    if(elements.empty())return shape;
    const auto first=arrayShapeOf(elements.front());
    for(size_t i=1;i<elements.size();++i){
        const auto child=arrayShapeOf(elements[i]);
        if(child.has_value()!=first.has_value() || (child && *child!=*first))return {};
    }
    if(first)shape.insert(shape.end(),first->begin(),first->end());
    return shape;
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
    static constexpr char hex[] = "0123456789abcdef";
    for (unsigned char c : v) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    out += "\\u00";
                    out.push_back(hex[c >> 4]);
                    out.push_back(hex[c & 0x0f]);
                } else {
                    out.push_back(static_cast<char>(c));
                }
                break;
        }
    }
    out += "\"";
    return out;
}

static bool jsonUnquoteString(const std::string& token, std::string& out) {
    const std::string text = trimStr(token);
    if (text.size() < 2 || text.front() != '"' || text.back() != '"')
        return false;

    auto parseHexUnit = [&](size_t begin, uint32_t& value) {
        if (begin + 4 > text.size() - 1) return false;
        value = 0;
        for (size_t i = begin; i < begin + 4; ++i) {
            const char c = text[i];
            uint32_t digit = 0;
            if (c >= '0' && c <= '9') digit = static_cast<uint32_t>(c - '0');
            else if (c >= 'a' && c <= 'f') digit = static_cast<uint32_t>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') digit = static_cast<uint32_t>(c - 'A' + 10);
            else return false;
            value = (value << 4) | digit;
        }
        return true;
    };

    out.clear();
    for (size_t i = 1; i + 1 < text.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(text[i]);
        if (c != '\\') {
            if (c < 0x20) return false;
            out.push_back(static_cast<char>(c));
            continue;
        }
        if (++i >= text.size() - 1) return false;
        switch (text[i]) {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'n': out.push_back('\n'); break;
            case 'r': out.push_back('\r'); break;
            case 't': out.push_back('\t'); break;
            case 'u': {
                uint32_t codePoint = 0;
                if (!parseHexUnit(i + 1, codePoint)) return false;
                i += 4;
                if (codePoint >= 0xd800 && codePoint <= 0xdbff) {
                    if (i + 6 >= text.size() || text[i + 1] != '\\' ||
                        text[i + 2] != 'u') {
                        return false;
                    }
                    uint32_t low = 0;
                    if (!parseHexUnit(i + 3, low) || low < 0xdc00 ||
                        low > 0xdfff) {
                        return false;
                    }
                    codePoint = 0x10000 + ((codePoint - 0xd800) << 10) +
                                (low - 0xdc00);
                    i += 6;
                } else if (codePoint >= 0xdc00 && codePoint <= 0xdfff) {
                    return false;
                }
                const std::string encoded = encodeUtf8CodePoint(codePoint);
                if (encoded.empty()) return false;
                out += encoded;
                break;
            }
            default: return false;
        }
    }
    return true;
}

// Render an ExprValue as a compact JSON value.
static std::string toJsonValue(const ExprValue& v) {
    if (v.isNull) return "null";
    std::string t = v.typeName;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    const bool arrayType = t == "array" ||
        (t.size() >= 2 && t.compare(t.size() - 2, 2, "[]") == 0);
    if (arrayType) {
        std::string elementType = "text";
        if (t != "array") {
            elementType = t;
            while (elementType.size() >= 2 &&
                   elementType.compare(elementType.size() - 2, 2, "[]") == 0) {
                elementType.resize(elementType.size() - 2);
            }
        }
        std::function<std::optional<std::string>(const std::string&)> render =
            [&](const std::string& array) -> std::optional<std::string> {
                std::vector<std::string> elements;
                if (!parseArrayElements(array, elements)) return std::nullopt;
                std::string result = "[";
                for (size_t i = 0; i < elements.size(); ++i) {
                    if (i) result += ",";
                    std::vector<std::string> nested;
                    if (parseArrayElements(elements[i], nested)) {
                        const auto nestedJson = render(elements[i]);
                        if (!nestedJson) return std::nullopt;
                        result += *nestedJson;
                        continue;
                    }
                    const std::string token = trimStr(elements[i]);
                    const bool quoted = token.size() >= 2 &&
                        token.front() == '"' && token.back() == '"';
                    const bool elementIsNull =
                        !quoted && toLower(token) == "null";
                    result += toJsonValue(ExprValue(
                        elementType, arrayElemUnquote(token), elementIsNull));
                }
                result += "]";
                return result;
            };
        const auto result = render(v.value);
        return result ? *result : jsonQuoteStr(v.value);
    }
    if (t == "boolean" || t == "bool") {
        std::string lv = v.value;
        for (char& c : lv) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return (lv == "t" || lv == "true" || lv == "1") ? "true" : "false";
    }
    bool numeric = (t.find("int") != std::string::npos) || t == "numeric" || t == "decimal" ||
                   t.find("double") != std::string::npos || t == "real" || t == "float" ||
                   t == "smallint" || t == "bigint";
    if (numeric && !v.value.empty()) {
        const auto parsed = tryParseNumeric(v.value);
        if (parsed && !parsed->isFinite())
            return jsonQuoteStr(parsed->toString());
        return v.value;
    }
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
        bool found = false;
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
            std::string ku = k;
            if (k.size() >= 2 && k.front() == '"' && k.back() == '"' &&
                !jsonUnquoteString(k, ku)) {
                continue;
            }
            if (ku == key) {
                out = trimStr(m.substr(colon + 1));
                found = true;
            }
        }
        return found;
    }
    if (t.front() == '[') {
        std::vector<std::string> elems;
        if (!jsonTopLevelSplit(t, '[', ']', elems)) return false;
        long idx = 0;
        try { size_t pos = 0; idx = std::stol(key, &pos); if (pos != key.size()) return false; }
        catch (...) { return false; }
        size_t actualIndex = 0;
        if (idx < 0) {
            const auto distanceFromEnd =
                static_cast<unsigned long long>(-(idx + 1)) + 1;
            if (distanceFromEnd > elems.size()) return false;
            actualIndex = elems.size() -
                          static_cast<size_t>(distanceFromEnd);
        } else {
            if (static_cast<unsigned long long>(idx) >= elems.size())
                return false;
            actualIndex = static_cast<size_t>(idx);
        }
        out = elems[actualIndex];
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

[[noreturn]] static void throwInvalidRegularExpression() {
    throw std::runtime_error(
        "invalid regular expression (SQLSTATE 2201B)");
}

static int64_t parsePositiveRegexParameter(const ExprValue& value,
                                           const std::string& name) {
    long long parsed = 0;
    if (!parseInt64Exact(value.value, parsed) || parsed < 1) {
        throw std::runtime_error(
            "invalid value for parameter \"" + name + "\": " +
            value.value + " (SQLSTATE 22023)");
    }
    return parsed;
}

static void validateRegexOptions(const std::string& options,
                                 bool allowGlobal,
                                 const std::string& function) {
    static const std::string validOptions = "bceimnpqstwx";
    for (char option : options) {
        if (option == 'g') {
            if (!allowGlobal) {
                throw std::runtime_error(
                    function + "() does not support the global option "
                    "(SQLSTATE 22023)");
            }
            continue;
        }
        if (validOptions.find(option) == std::string::npos) {
            throw std::runtime_error(
                "invalid regular expression option: \"" +
                std::string(1, option) + "\" (SQLSTATE 22023)");
        }
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

static ExprValue castToIntegerRange(const ExprValue& value,
                                    const std::string& targetType) {
    const RangeParts range = parseRangeLiteral(value.value);
    if (!range.valid) {
        throw std::runtime_error(
            "invalid input syntax for type " + targetType + ": '" +
            value.value + "' (SQLSTATE 22P02)");
    }
    if (range.empty) return ExprValue(targetType, "empty", false);

    const int64_t minimum = targetType == "int4range"
        ? std::numeric_limits<int32_t>::min()
        : std::numeric_limits<int64_t>::min();
    const int64_t maximum = targetType == "int4range"
        ? std::numeric_limits<int32_t>::max()
        : std::numeric_limits<int64_t>::max();
    const auto parseBound = [&](const std::string& text) {
        int64_t parsed = 0;
        const SignedIntegerParseResult status =
            parseSignedInteger(text, parsed);
        if (status == SignedIntegerParseResult::Invalid) {
            throw std::runtime_error(
                "invalid input syntax for type " + targetType + ": '" +
                value.value + "' (SQLSTATE 22P02)");
        }
        if (status == SignedIntegerParseResult::OutOfRange ||
            parsed < minimum || parsed > maximum) {
            throw std::runtime_error(
                std::string(targetType == "int4range" ? "integer" : "bigint") +
                " out of range (SQLSTATE 22003)");
        }
        return parsed;
    };

    int64_t lower = 0;
    int64_t upper = 0;
    if (!range.loInf) lower = parseBound(range.lo);
    if (!range.hiInf) upper = parseBound(range.hi);
    if (!range.loInf && !range.hiInf) {
        if (lower > upper) {
            throw std::runtime_error(
                "range lower bound must be less than or equal to range "
                "upper bound (SQLSTATE 22000)");
        }
        if (lower == upper && !(range.loInc && range.hiInc))
            return ExprValue(targetType, "empty", false);
    }

    if (!range.loInf && !range.loInc) {
        if (lower == maximum) {
            throw std::runtime_error(
                std::string(targetType == "int4range" ? "integer" : "bigint") +
                " out of range (SQLSTATE 22003)");
        }
        ++lower;
    }
    if (!range.hiInf && range.hiInc) {
        if (upper == maximum) {
            throw std::runtime_error(
                std::string(targetType == "int4range" ? "integer" : "bigint") +
                " out of range (SQLSTATE 22003)");
        }
        ++upper;
    }
    if (!range.loInf && !range.hiInf && lower >= upper)
        return ExprValue(targetType, "empty", false);

    const std::string result =
        std::string(range.loInf ? "(" : "[") +
        (range.loInf ? "" : std::to_string(lower)) + "," +
        (range.hiInf ? "" : std::to_string(upper)) + ")";
    return ExprValue(targetType, result, false);
}

static ExprValue castToNumericRange(const ExprValue& value) {
    const RangeParts range = parseRangeLiteral(value.value);
    if (!range.valid) {
        throw std::runtime_error(
            "invalid input syntax for type numrange: '" + value.value +
            "' (SQLSTATE 22P02)");
    }
    if (range.empty) return ExprValue("numrange", "empty", false);

    std::optional<Numeric> lower;
    std::optional<Numeric> upper;
    try {
        if (!range.loInf) lower.emplace(range.lo);
        if (!range.hiInf) upper.emplace(range.hi);
    } catch (const std::invalid_argument& error) {
        const std::string message = error.what();
        if (message.find("exceeds maximum") != std::string::npos ||
            message.find("out of range") != std::string::npos) {
            throw std::runtime_error(
                "numeric value out of range (SQLSTATE 22003)");
        }
        throw std::runtime_error(
            "invalid input syntax for type numeric (SQLSTATE 22P02)");
    }

    if (lower && upper) {
        if (*lower > *upper) {
            throw std::runtime_error(
                "range lower bound must be less than or equal to range "
                "upper bound (SQLSTATE 22000)");
        }
        if (*lower == *upper && !(range.loInc && range.hiInc))
            return ExprValue("numrange", "empty", false);
    }

    const std::string result =
        std::string(range.loInf ? "(" : (range.loInc ? "[" : "(")) +
        (lower ? lower->toString() : "") + "," +
        (upper ? upper->toString() : "") +
        (range.hiInf ? ")" : (range.hiInc ? "]" : ")"));
    return ExprValue("numrange", result, false);
}

static ExprValue castToDateRange(const ExprValue& value) {
    const RangeParts range = parseRangeLiteral(value.value);
    if (!range.valid) {
        throw std::runtime_error(
            "invalid input syntax for type daterange: '" + value.value +
            "' (SQLSTATE 22P02)");
    }
    if (range.empty) return ExprValue("daterange", "empty", false);

    Date lower;
    Date upper;
    if (!range.loInf) {
        lower = Date(range.lo.c_str());
        if (lower.year == 0) {
            throw std::runtime_error(
                "date/time field value out of range: '" + range.lo +
                "' (SQLSTATE 22008)");
        }
    }
    if (!range.hiInf) {
        upper = Date(range.hi.c_str());
        if (upper.year == 0) {
            throw std::runtime_error(
                "date/time field value out of range: '" + range.hi +
                "' (SQLSTATE 22008)");
        }
    }
    if (!range.loInf && !range.hiInf) {
        if (lower > upper) {
            throw std::runtime_error(
                "range lower bound must be less than or equal to range "
                "upper bound (SQLSTATE 22000)");
        }
        if (lower == upper && !(range.loInc && range.hiInc))
            return ExprValue("daterange", "empty", false);
    }

    bool upperInfinite = range.hiInf;
    if (!range.loInf && !range.loInc) {
        lower = lower + 1;
        if (lower.year == 0)
            return ExprValue("daterange", "empty", false);
    }
    if (!range.hiInf && range.hiInc) {
        upper = upper + 1;
        if (upper.year == 0) upperInfinite = true;
    }
    if (!range.loInf && !upperInfinite && lower >= upper)
        return ExprValue("daterange", "empty", false);

    const std::string result =
        std::string(range.loInf ? "(" : "[") +
        (range.loInf ? "" : str(lower)) + "," +
        (upperInfinite ? "" : str(upper)) + ")";
    return ExprValue("daterange", result, false);
}

static ExprValue castToTimestampRange(const ExprValue& value,
                                      const std::string& targetType) {
    const RangeParts range = parseRangeLiteral(value.value);
    if (!range.valid) {
        throw std::runtime_error(
            "invalid input syntax for type " + targetType + ": '" +
            value.value + "' (SQLSTATE 22P02)");
    }
    if (range.empty) return ExprValue(targetType, "empty", false);

    const std::string subtype = targetType == "tstzrange"
        ? "timestamptz" : "timestamp";
    std::optional<std::string> lower;
    std::optional<std::string> upper;
    if (!range.loInf) {
        lower = castToTimestamp(
            ExprValue("text", range.lo, false), subtype).value;
    }
    if (!range.hiInf) {
        upper = castToTimestamp(
            ExprValue("text", range.hi, false), subtype).value;
    }

    const auto compareBounds = [](std::string left, std::string right) {
        const std::string leftLower = toLower(left);
        const std::string rightLower = toLower(right);
        if (leftLower == rightLower) return 0;
        if (leftLower == "-infinity" || rightLower == "infinity") return -1;
        if (leftLower == "infinity" || rightLower == "-infinity") return 1;

        const auto fixedPrecision = [](std::string timestamp) {
            if (timestamp.size() >= 3 &&
                timestamp.compare(timestamp.size() - 3, 3, "+00") == 0) {
                timestamp.resize(timestamp.size() - 3);
            }
            const size_t dot = timestamp.find('.', 11);
            if (dot == std::string::npos) {
                timestamp += ".000000";
            } else {
                const size_t digits = timestamp.size() - dot - 1;
                if (digits < 6) timestamp.append(6 - digits, '0');
            }
            return timestamp;
        };
        left = fixedPrecision(std::move(left));
        right = fixedPrecision(std::move(right));
        return left < right ? -1 : (left > right ? 1 : 0);
    };

    if (lower && upper) {
        const int comparison = compareBounds(*lower, *upper);
        if (comparison > 0) {
            throw std::runtime_error(
                "range lower bound must be less than or equal to range "
                "upper bound (SQLSTATE 22000)");
        }
        if (comparison == 0 && !(range.loInc && range.hiInc))
            return ExprValue(targetType, "empty", false);
    }

    const auto emitBound = [](const std::string& bound) {
        if (bound.find_first_of(" ,\\\"") == std::string::npos)
            return bound;
        std::string output = "\"";
        for (const char c : bound) {
            if (c == '\\' || c == '"') output.push_back('\\');
            output.push_back(c);
        }
        output.push_back('"');
        return output;
    };
    const std::string result =
        std::string(range.loInf ? "(" : (range.loInc ? "[" : "(")) +
        (lower ? emitBound(*lower) : "") + "," +
        (upper ? emitBound(*upper) : "") +
        (range.hiInf ? ")" : (range.hiInc ? "]" : ")"));
    return ExprValue(targetType, result, false);
}

static bool typeIsRange(const std::string& typeName) {
    std::string t = typeName;
    for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return t.find("range") != std::string::npos;
}

static std::string rangeBoundType(const std::string& typeName) {
    const std::string type = toLower(typeName);
    if (type == "int4range") return "integer";
    if (type == "int8range") return "bigint";
    if (type == "daterange") return "date";
    if (type == "tsrange") return "timestamp";
    if (type == "tstzrange") return "timestamptz";
    // numrange and user-defined ranges historically use numeric bounds in
    // this evaluator when subtype metadata is unavailable.
    return "numeric";
}

std::optional<bool> ExprEvaluator::rangesOverlap(const ExprValue& left,
                                                 const ExprValue& right) {
    if (left.isNull || right.isNull) return std::nullopt;
    if (toLower(left.typeName) != toLower(right.typeName)) return std::nullopt;

    const RangeParts lhs = parseRangeLiteral(left.value);
    const RangeParts rhs = parseRangeLiteral(right.value);
    if (!lhs.valid || !rhs.valid) return std::nullopt;
    if (lhs.empty || rhs.empty) return false;

    const std::string boundType = rangeBoundType(left.typeName);
    const auto compareBounds = [&](const std::string& a,
                                   const std::string& b) {
        return compareValues(ExprValue(boundType, a, false),
                             ExprValue(boundType, b, false));
    };

    if (!lhs.hiInf && !rhs.loInf) {
        const int comparison = compareBounds(lhs.hi, rhs.lo);
        if (comparison < 0 ||
            (comparison == 0 && !(lhs.hiInc && rhs.loInc))) {
            return false;
        }
    }
    if (!rhs.hiInf && !lhs.loInf) {
        const int comparison = compareBounds(rhs.hi, lhs.lo);
        if (comparison < 0 ||
            (comparison == 0 && !(rhs.hiInc && lhs.loInc))) {
            return false;
        }
    }
    return true;
}

std::optional<bool> ExprEvaluator::rangeContains(const ExprValue& container,
                                                 const ExprValue& contained) {
    if (container.isNull || contained.isNull) return std::nullopt;

    const RangeParts outer = parseRangeLiteral(container.value);
    if (!outer.valid) return std::nullopt;
    const std::string boundType = rangeBoundType(container.typeName);
    const auto compareBoundTo = [&](const std::string& bound,
                                    const ExprValue& value) {
        return compareValues(ExprValue(boundType, bound, false), value);
    };

    if (!typeIsRange(contained.typeName)) {
        if (outer.empty) return false;
        if (!outer.loInf) {
            const int comparison = compareBoundTo(outer.lo, contained);
            if (comparison > 0 || (comparison == 0 && !outer.loInc))
                return false;
        }
        if (!outer.hiInf) {
            const int comparison = compareBoundTo(outer.hi, contained);
            if (comparison < 0 || (comparison == 0 && !outer.hiInc))
                return false;
        }
        return true;
    }

    if (toLower(container.typeName) != toLower(contained.typeName))
        return std::nullopt;
    const RangeParts inner = parseRangeLiteral(contained.value);
    if (!inner.valid) return std::nullopt;
    if (inner.empty) return true;
    if (outer.empty) return false;

    if (!outer.loInf) {
        if (inner.loInf) return false;
        const int comparison = compareBoundTo(
            outer.lo, ExprValue(boundType, inner.lo, false));
        if (comparison > 0 ||
            (comparison == 0 && inner.loInc && !outer.loInc)) {
            return false;
        }
    }
    if (!outer.hiInf) {
        if (inner.hiInf) return false;
        const int comparison = compareBoundTo(
            outer.hi, ExprValue(boundType, inner.hi, false));
        if (comparison < 0 ||
            (comparison == 0 && inner.hiInc && !outer.hiInc)) {
            return false;
        }
    }
    return true;
}

// SQL identifier quoting (quote_ident / format %I): only quote when not a simple
// lower-case identifier; double embedded quotes.
static std::string sqlQuoteIdent(const std::string& s) {
    // quote_ident leaves only unreserved keywords bare. PostgreSQL also
    // quotes column-name and type/function-name keywords because their use is
    // context-sensitive and would not be safe in arbitrary generated SQL.
    static const std::set<std::string> keywordsRequiringQuotes = {
        "all", "analyse", "analyze", "and", "any", "array", "as",
        "asc", "asymmetric", "authorization", "between", "bigint",
        "binary", "bit", "boolean", "both", "case", "cast", "char",
        "character", "check", "coalesce", "collate", "collation",
        "column", "concurrently", "constraint", "create", "cross",
        "current_catalog", "current_date", "current_role",
        "current_schema", "current_time", "current_timestamp",
        "current_user", "dec", "decimal", "default", "deferrable",
        "desc", "distinct", "do", "else", "end", "except", "exists",
        "extract", "false", "fetch", "float", "for", "foreign",
        "freeze", "from", "full", "grant", "greatest", "group",
        "grouping", "having", "ilike", "in", "initially", "inner",
        "inout", "int", "integer", "intersect", "interval", "into",
        "is", "isnull", "join", "json", "json_array",
        "json_arrayagg", "json_exists", "json_object",
        "json_objectagg", "json_query", "json_scalar",
        "json_serialize", "json_table", "json_value", "lateral",
        "leading", "least", "left", "like", "limit", "localtime",
        "localtimestamp", "merge_action", "national", "natural",
        "nchar", "none", "normalize", "not", "notnull", "null",
        "nullif", "numeric", "offset", "on", "only", "or", "order",
        "out", "outer", "overlaps", "overlay", "placing", "position",
        "precision", "primary", "real", "references", "returning",
        "right", "row", "select", "session_user", "setof", "similar",
        "smallint", "some", "substring", "symmetric", "system_user",
        "table", "tablesample", "then", "time", "timestamp", "to",
        "trailing", "treat", "trim", "true", "union", "unique",
        "user", "using", "values", "varchar", "variadic", "verbose",
        "when", "where", "window", "with", "xmlattributes",
        "xmlconcat", "xmlelement", "xmlexists", "xmlforest",
        "xmlnamespaces", "xmlparse", "xmlpi", "xmlroot",
        "xmlserialize", "xmltable"
    };
    bool simple = !s.empty();
    for (size_t i = 0; i < s.size() && simple; ++i) {
        unsigned char c = static_cast<unsigned char>(s[i]);
        bool ok = (c == '_') || std::islower(c) || (std::isdigit(c) && i > 0);
        if (!ok) simple = false;
    }
    if (simple && keywordsRequiringQuotes.count(s) != 0) simple = false;
    if (simple) return s;
    std::string out = "\"";
    for (char c : s) { if (c == '"') out += "\"\""; else out.push_back(c); }
    out += "\"";
    return out;
}

// SQL string-literal quoting (quote_literal / format %L): single-quote, doubling
// embedded quotes.
static std::string sqlQuoteLiteral(const std::string& s) {
    const bool escapeSyntax = s.find('\\') != std::string::npos;
    std::string out = escapeSyntax ? "E'" : "'";
    for (char c : s) {
        if (c == '\'') out += "''";
        else if (c == '\\') out += "\\\\";
        else out.push_back(c);
    }
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
        if (c == 'J') { // Julian day number
            long mp = mo - 3; if (mp < 0) mp += 12; long yp = y - (mo < 3 ? 1 : 0);
            long jd = d + (153 * mp + 2) / 5 + 365 * yp + yp / 4 - yp / 100 + yp / 400 + 1721119;
            std::snprintf(buf, sizeof buf, "%ld", jd); out += buf; ++i; continue;
        }
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
static std::string formatNumeric(double val, const std::string& fmtIn,
                                 const std::string& exactInput = std::string()) {
    std::string fmt = fmtIn;
    bool fm = false;
    if (fmt.size() >= 2 && (fmt[0] == 'F' || fmt[0] == 'f') &&
        (fmt[1] == 'M' || fmt[1] == 'm')) { fm = true; fmt = fmt.substr(2); }
    // Treat D as the decimal point (PG locale-independent form).
    size_t dot = fmt.find('.');
    size_t dot0pos = dot;
    if (dot == std::string::npos) dot = fmt.find('D');
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
    bool hasPL = fmt.find("PL") != std::string::npos;
    bool hasPR = fmt.find("PR") != std::string::npos;
    bool hasMI = fmt.find("MI") != std::string::npos;
    bool hasTH = fmt.find("TH") != std::string::npos || fmt.find("th") != std::string::npos;
    bool hasV = fmt.find('V') != std::string::npos;
    bool hasEEEE = fmt.find("EEEE") != std::string::npos;
    bool hasRN = fmt.find("RN") != std::string::npos || fmt.find("rn") != std::string::npos;
    bool hasS = false;    {
        size_t sp2 = fmt.find('S');        while (sp2 != std::string::npos) {            if (sp2 + 1 >= fmt.size() || fmt[sp2 + 1] != 'G') { hasS = true; break; }            sp2 = fmt.find('S', sp2 + 2);        }    }
    const size_t currencyPosition = fmt.find_first_of("Ll");
    bool hasL = currencyPosition != std::string::npos;
    const size_t sgPosition = fmt.find("SG");
    bool hasG = false;
    for (size_t i = 0; i < fmt.size(); ++i)
        if (fmt[i] == 'G' && (i == 0 || fmt[i - 1] != 'S')) hasG = true;
    if (hasRN) {
        // Roman numerals, right-aligned to width 15 (FMRN unpads).
        long n2 = (long)((val < 0) ? -val : val);
        if (n2 < 1 || n2 > 3999) return "";
        static const char* h2[] = {"","I","II","III","IV","V","VI","VII","VIII","IX"};
        static const char* t2[] = {"","X","XX","XXX","XL","L","LX","LXX","LXXX","XC"};
        static const char* h3[] = {"","C","CC","CCC","CD","D","DC","DCC","DCCC","CM"};
        static const char* h4[] = {"","M","MM","MMM"};
        std::string r2 = std::string(h4[n2 / 1000]) + h3[(n2 / 100) % 10] + t2[(n2 / 10) % 10] + h2[n2 % 10];
        std::string rnOut = ((int)r2.size() < 15 && !fm) ? std::string(15 - r2.size(), ' ') + r2 : r2;
        if (fmtIn.find("rn") != std::string::npos) for (auto& rc : rnOut) rc = static_cast<char>(std::tolower((unsigned char)rc));
        return rnOut;
    }
    const double valTH = (val < 0) ? -val : val;
    bool neg = val < 0;
    if (hasEEEE) {
        int sig = intPlaces + fracDigits;
        if (sig < 1) sig = 1;
        char eb[64];
        std::snprintf(eb, sizeof eb, "%.*e", sig - 1, val);
        std::string es(eb);
        size_t ep2 = es.find('e');
        if (ep2 == std::string::npos) return es;
        std::string mant = es.substr(0, ep2);
        std::string expt = es.substr(ep2 + 1);
        char xs = '+';
        if (!expt.empty() && (expt[0] == '+' || expt[0] == '-')) { xs = expt[0]; expt = expt.substr(1); }
        while (expt.size() > 2 && expt[0] == '0') expt = expt.substr(1);
        std::string mout;
        if (!mant.empty() && mant[0] == '-') mout = mant;
        else mout = (fm ? "" : " ") + mant;
        return mout + "e" + xs + expt;
    }
    int scaleShift = 0;
    if (hasV) {
        // V shifts the decimal point: digits after V are scale shifts.
        size_t vPos = fmt.find('V');
        int shift = 0;
        for (size_t i = vPos + 1; i < fmt.size(); ++i)
            if (fmt[i] == '9' || fmt[i] == '0') ++shift;
        scaleShift = shift;
        double av2 = neg ? -val : val;
        for (int k2 = 0; k2 < shift; ++k2) av2 *= 10.0;
        val = av2; neg = false; fracDigits = 0; dot = std::string::npos;
        // intPlaces spans digits on both sides of V.
        intPlaces = 0; zeroPad = false;
        for (size_t i = 0; i < fmt.size(); ++i)
            if (fmt[i] == '9') ++intPlaces;
            else if (fmt[i] == '0') { ++intPlaces; zeroPad = true; }
    }
    std::string roundedText;
    if (!exactInput.empty()) {
        Numeric decimal(exactInput);
        if (decimal.isFinite()) {
            if (scaleShift > 0) {
                decimal *= Numeric("1" + std::string(static_cast<size_t>(scaleShift), '0'));
            }
            decimal = decimal.withScale(fracDigits);
            neg = decimal.sign() < 0;
            roundedText = (neg ? -decimal : decimal).toString();
            if (fracDigits > 0) {
                size_t point = roundedText.find('.');
                if (point == std::string::npos) {
                    roundedText += '.';
                    point = roundedText.size() - 1;
                }
                const size_t existing = roundedText.size() - point - 1;
                if (existing < static_cast<size_t>(fracDigits)) {
                    roundedText.append(static_cast<size_t>(fracDigits) - existing, '0');
                }
            }
        }
    }
    if (roundedText.empty()) {
        char numbuf[64];
        std::snprintf(numbuf, sizeof numbuf, "%.*f", fracDigits, neg ? -val : val);
        roundedText = numbuf;
    }
    std::string s = roundedText, ip = s, fp;
    // This also covers rounded zero in the floating-point fallback.
    if (s.find_first_not_of("0.") == std::string::npos) neg = false;
    size_t sp = s.find('.');
    if (sp != std::string::npos) { ip = s.substr(0, sp); fp = s.substr(sp + 1); }
    if (fm && fracDigits > 0) {
        size_t minimumFractionDigits = 0;
        size_t digitPosition = 0;
        for (size_t i = dot + 1; i < fmt.size(); ++i) {
            if (fmt[i] != '9' && fmt[i] != '0') continue;
            ++digitPosition;
            if (fmt[i] == '0') minimumFractionDigits = digitPosition;
        }
        while (fp.size() > minimumFractionDigits && fp.back() == '0') fp.pop_back();
    }
    if (zeroPad && static_cast<int>(ip.size()) < intPlaces)
        ip = std::string(intPlaces - ip.size(), '0') + ip;
    else if (!zeroPad && !fm) {
        // Optional integer zero disappears only before a fractional field;
        // integer-only templates still emit the final zero digit.
        if (ip == "0" && fracDigits > 0) ip = std::string(intPlaces, ' ');
        else if (static_cast<int>(ip.size()) < intPlaces)
            ip = std::string(intPlaces - ip.size(), ' ') + ip;
    }
    if (fm && !zeroPad && ip == "0" && !fp.empty()) ip.clear();
    // PG overflow: more integer digits than 9/0 positions render #.
    if (!ip.empty() && ip.find_first_not_of(" 0123456789") == std::string::npos &&
        static_cast<int>(ip.size()) > intPlaces) {
        ip = std::string(intPlaces, '#');
        fp = std::string(fracDigits, '#');
    }
    if (hasG) {
        // Insert commas every three digits, leaving leading blanks in place.
        size_t firstDig = ip.find_first_not_of(' ');
        if (firstDig == std::string::npos) firstDig = 0;
        std::string digits = ip.substr(firstDig);
        std::string lead = ip.substr(0, firstDig);
        std::string grouped;
        for (size_t k = 0; k < digits.size(); ++k) {
            if (k > 0 && (digits.size() - k) % 3 == 0) grouped += ",";
            grouped += digits[k];
        }
        ip = lead + grouped;
    }
    std::string out;
    std::string currencySuffix;
    std::string signSuffix;
    if (hasPR && neg) {
        // PR: negatives in angle brackets occupying the sign + digit region.
        std::string trimIp = ip;
    size_t nz = trimIp.find_first_not_of(' ');
    if (nz != std::string::npos) trimIp = trimIp.substr(nz);
    std::string body = "<" + trimIp + ">";
        int width = intPlaces + 2;
        if (static_cast<int>(body.size()) < width)
            body = std::string(width - body.size(), ' ') + body;
        out = body;
    } else if (hasPR) {
        out = fm ? "" : " ";
        out += ip;
        out += " ";
    } else if (hasPL && !neg) {
        out = std::string("+") + (fm ? "" : " ") + ip;
    } else if (hasPL && neg) {
        // PL on negative: blank sign slot, minus adjacent to the digits.
        std::string trimIp2 = ip;
        size_t nz2 = trimIp2.find_first_not_of(' ');
        if (nz2 != std::string::npos) trimIp2 = trimIp2.substr(nz2);
        std::string body = "-" + trimIp2;
        int width = intPlaces + 2;
        if (static_cast<int>(body.size()) < width)
            body = std::string(width - body.size(), ' ') + body;
        out = body;
    } else if (hasMI) {
        // MI marks the sign POSITION: minus for negatives,
        // blank otherwise (FM suppresses the blank).
        size_t miPos = fmt.find("MI");
        std::string sgn = neg ? "-" : (fm ? "" : " ");
        size_t firstDig = fmt.find_first_of("90");        
        if (miPos != std::string::npos && firstDig != std::string::npos && miPos < firstDig) {
            out = sgn + ip;
        } else {
            out = ip + sgn;
        }
    } else if (sgPosition != std::string::npos) {
        // SG keeps its template position, including before integer padding;
        // its G belongs to the sign token, not to a grouping directive.
        const std::string sign = neg ? "-" : "+";
        if (sgPosition < fmt.find_first_of("90")) out = sign + ip;
        else {
            out = ip;
            signSuffix = sign;
        }
    } else if (hasS) {
        // S: explicit sign (+/-) anchored at its position;
        // FM keeps the sign, only blanks are suppressed.
        size_t sPos = fmt.find('S');
        std::string sg2 = neg ? "-" : "+";
        size_t fd2 = fmt.find_first_of("90");
        if (sPos != std::string::npos && fd2 != std::string::npos && sPos < fd2) {
            // Sign sits ADJACENT to the digits, inside the pad region
            // (PG: S9999 -42 -> "  -42", width intPlaces+1).
            size_t fz = ip.find_first_not_of(' ');
            if (fz == std::string::npos) fz = 0;
            out = ip.substr(0, fz) + sg2 + ip.substr(fz);
        } else {
            out = ip + sg2;
        }
    } else if (hasL) {
        // Numeric formatting uses the locale's literal currency symbol;
        // unlike MONEY I/O it has no historical dollar fallback in C.
        const std::locale locale(StorageEngine::getMoneyLocale().c_str());
        std::string symbol = std::use_facet<std::moneypunct<char, false>>(
            locale).curr_symbol();
        if (symbol.empty()) symbol = " ";
        if (neg) {
            const size_t firstDigit = ip.find_first_not_of(' ');
            const size_t signPosition = firstDigit == std::string::npos ? ip.size() : firstDigit;
            out = ip.substr(0, signPosition) + "-" + ip.substr(signPosition);
        } else {
            out = (fm ? "" : " ") + ip;
        }
        if (currencyPosition < fmt.find_first_of("90")) out = symbol + out;
        else currencySuffix = std::move(symbol);
    } else {
        if (neg) {
            const size_t firstDigit = ip.find_first_not_of(' ');
            const size_t signPosition = firstDigit == std::string::npos
                ? ip.size() : firstDigit;
            out = ip.substr(0, signPosition) + "-" + ip.substr(signPosition);
        } else {
            out = (fm ? "" : " ") + ip;
        }
    }
    if (fracDigits > 0) out += "." + fp;
    out += signSuffix;
    out += currencySuffix;
    if (hasTH) {
        long iv = (long)std::llround(valTH);
        long a11 = iv % 100; long d1 = iv % 10;
        std::string sfxS = "th";
        if (a11 == 11 || a11 == 12 || a11 == 13) sfxS = "th";
        else if (d1 == 1) sfxS = "st";
        else if (d1 == 2) sfxS = "nd";
        else if (d1 == 3) sfxS = "rd";
        if (fmtIn.find("th") != std::string::npos) for (auto& sc : sfxS) sc = static_cast<char>(std::tolower((unsigned char)sc));
        else for (auto& sc : sfxS) sc = static_cast<char>(std::toupper((unsigned char)sc));
        out += sfxS;
    }
    return out;
}

// ============================================================================
// Full-text search evaluation
//   to_tsvector(text): tokenize (alnum runs, lowercased, trailing
//     punctuation stripped), assign 1-based positions, canonicalize via
//     the engine's tsvector normalizer.
//   to_tsquery(text) / plainto_tsquery(text): AND (&) the query words.
//   @@: does the tsvector (left) satisfy the tsquery (right)?
//   ts_rank(tsvector, tsquery): frequency-weighted match score in [0,1].
// ============================================================================
static std::vector<std::string> tsTokenize(const std::string& text) {
    std::vector<std::string> out;
    std::string cur;
    for (char ch : text) {
        if (std::isalnum(static_cast<unsigned char>(ch))) {
            cur += static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        } else if (!cur.empty()) {
            out.push_back(cur);
            cur.clear();
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

static std::string tsBuildVector(const std::string& text) {
    std::vector<std::string> toks = tsTokenize(text);
    std::string raw;
    for (size_t i = 0; i < toks.size(); ++i) {
        if (!raw.empty()) raw += ' ';
        raw += "'" + toks[i] + "':" + std::to_string(i + 1);
    }
    std::string canon;
    if (!raw.empty() && g_engine.normalizeTsVectorText(raw, canon)) return canon;
    return raw;
}

// Lexeme positions of a stored tsvector literal ('l1':1,2A 'l2':3 ...) —
// tolerant of plain text input (treated as its token set) so @@ composes
// with any text column.  Each occurrence is (position, weight) with weight
// one of 'A'..'D' ('D' when unlabeled, as PostgreSQL implies).
struct TsVectorLex {
    // lexeme -> (position, weight) occurrences
    std::map<std::string, std::vector<std::pair<int, char>>> occ;
};
static TsVectorLex tsLexemesW(const std::string& v) {
    TsVectorLex out;
    size_t i = 0, n = v.size();
    while (i < n) {
        // skip to next lexeme start: bare word or quoted
        while (i < n && (v[i] == ' ' || v[i] == ',')) ++i;
        if (i >= n) break;
        std::string lex;
        if (v[i] == '\'') {
            ++i;
            while (i < n) {
                if (v[i] == '\'' && i + 1 < n && v[i + 1] == '\'') { lex += '\''; i += 2; }
                else if (v[i] == '\'') { ++i; break; }
                else { lex += v[i]; ++i; }
            }
        } else {
            while (i < n && v[i] != ' ' && v[i] != ':') { lex += v[i]; ++i; }
        }
        if (lex.empty()) { if (i < n && v[i] == ':') ++i; continue; }
        std::vector<std::pair<int, char>> positions;
        if (i < n && v[i] == ':') {
            ++i;
            while (i < n && std::isdigit(static_cast<unsigned char>(v[i]))) {
                int p = 0;
                while (i < n && std::isdigit(static_cast<unsigned char>(v[i]))) {
                    p = p * 10 + (v[i] - '0'); ++i;
                }
                char w = 'D';
                if (i < n && v[i] >= 'A' && v[i] <= 'D') { w = v[i]; ++i; }
                positions.push_back({p, w});
                if (i < n && v[i] == ',') { ++i; continue; }
                break;
            }
        }
        if (positions.empty()) positions.push_back({0, 'D'});
        out.occ[lex] = positions;
    }
    return out;
}
// Position-only view (keeps older call sites simple).
static std::map<std::string, std::vector<int>> tsLexemes(const std::string& v) {
    std::map<std::string, std::vector<int>> out;
    TsVectorLex w = tsLexemesW(v);
    for (const auto& kv : w.occ) {
        std::vector<int> ps;
        for (const auto& pw : kv.second) ps.push_back(pw.first);
        out[kv.first] = ps;
    }
    return out;
}

// tsquery parsing with PostgreSQL operator precedence
//   !  (NOT, highest)
//   <-> (phrase / adjacency)
//   &  (AND)
//   |  (OR, lowest)
// Leaf: quoted or bare lexeme.  <-> matches when the two sides occur at
// adjacent positions (left position + 1 == right position), which is the
// default (distance-1) meaning of <-> in PostgreSQL.
struct TsQueryNode {
    enum Kind { Lexeme, Not, Phrase, And, Or } kind = Lexeme;
    std::string lexeme;
    std::unique_ptr<TsQueryNode> l, r;
};
struct TsQueryParser {
    const std::string& q;
    size_t i = 0;
    explicit TsQueryParser(const std::string& s) : q(s) {}
    void skipWs() { while (i < q.size() && std::isspace(static_cast<unsigned char>(q[i]))) ++i; }
    bool eat(const char* tok) {
        skipWs();
        size_t j = i;
        for (const char* p = tok; *p; ++p) {
            if (j >= q.size() || q[j] != *p) return false;
            ++j;
        }
        i = j;
        return true;
    }
    std::unique_ptr<TsQueryNode> parseLeaf() {
        skipWs();
        if (i >= q.size()) return nullptr;
        if (q[i] == '(') {
            ++i;
            auto node = parseOr();
            skipWs();
            if (i < q.size() && q[i] == ')') ++i;
            return node;
        }
        std::string lex;
        if (q[i] == '\'') {
            ++i;
            while (i < q.size()) {
                if (q[i] == '\'' && i + 1 < q.size() && q[i + 1] == '\'') { lex += '\''; i += 2; }
                else if (q[i] == '\'') { ++i; break; }
                else { lex += q[i]; ++i; }
            }
        } else {
            while (i < q.size() && std::isalnum(static_cast<unsigned char>(q[i]))) {
                lex += static_cast<char>(std::tolower(static_cast<unsigned char>(q[i])));
                ++i;
            }
        }
        if (lex.empty()) return nullptr;
        auto n = std::make_unique<TsQueryNode>();
        n->kind = TsQueryNode::Lexeme;
        n->lexeme = lex;
        return n;
    }
    std::unique_ptr<TsQueryNode> parseNot() {
        skipWs();
        if (i < q.size() && q[i] == '!') {
            ++i;
            auto n = std::make_unique<TsQueryNode>();
            n->kind = TsQueryNode::Not;
            n->l = parseNot();
            return n;
        }
        return parsePhraseOperand();
    }
    std::unique_ptr<TsQueryNode> parsePhraseOperand() {
        auto left = parseLeaf();
        if (!left) return nullptr;
        while (eat("<->")) {
            auto right = parseLeaf();
            if (!right) break;
            auto n = std::make_unique<TsQueryNode>();
            n->kind = TsQueryNode::Phrase;
            n->l = std::move(left);
            n->r = std::move(right);
            left = std::move(n);
        }
        return left;
    }
    std::unique_ptr<TsQueryNode> parseAnd() {
        auto left = parseNot();
        if (!left) return nullptr;
        while (true) {
            size_t save = i;
            if (!eat("&")) { i = save; break; }
            auto right = parseNot();
            if (!right) { i = save; break; }
            auto n = std::make_unique<TsQueryNode>();
            n->kind = TsQueryNode::And;
            n->l = std::move(left);
            n->r = std::move(right);
            left = std::move(n);
        }
        return left;
    }
    std::unique_ptr<TsQueryNode> parseOr() {
        auto left = parseAnd();
        if (!left) return nullptr;
        while (true) {
            size_t save = i;
            if (!eat("|")) { i = save; break; }
            auto right = parseAnd();
            if (!right) { i = save; break; }
            auto n = std::make_unique<TsQueryNode>();
            n->kind = TsQueryNode::Or;
            n->l = std::move(left);
            n->r = std::move(right);
            left = std::move(n);
        }
        return left;
    }
};

// Evaluate a query node against the vector, producing the matching
// occurrences; returns whether the node matches at all.
static bool tsEvalNode(const TsQueryNode* n, const TsVectorLex& vec,
                       std::vector<std::pair<int, char>>& out) {
    out.clear();
    if (!n) return false;
    switch (n->kind) {
    case TsQueryNode::Lexeme: {
        auto it = vec.occ.find(n->lexeme);
        if (it == vec.occ.end()) return false;
        out = it->second;
        return true;
    }
    case TsQueryNode::Not: {
        std::vector<std::pair<int, char>> sub;
        if (!tsEvalNode(n->l.get(), vec, sub)) {
            // negation of a non-match: vacuously true at a sentinel position
            out.push_back({0, 'D'});
            return true;
        }
        return false;
    }
    case TsQueryNode::And: {
        std::vector<std::pair<int, char>> a, b;
        if (!tsEvalNode(n->l.get(), vec, a)) return false;
        if (!tsEvalNode(n->r.get(), vec, b)) return false;
        out = a;
        out.insert(out.end(), b.begin(), b.end());
        return true;
    }
    case TsQueryNode::Or: {
        std::vector<std::pair<int, char>> a, b;
        bool ma = tsEvalNode(n->l.get(), vec, a);
        bool mb = tsEvalNode(n->r.get(), vec, b);
        out = a;
        out.insert(out.end(), b.begin(), b.end());
        return ma || mb;
    }
    case TsQueryNode::Phrase: {
        // left <-> right: some position of left is exactly one before a
        // position of right (PostgreSQL distance-1 phrase semantics).
        std::vector<std::pair<int, char>> a, b;
        if (!tsEvalNode(n->l.get(), vec, a)) return false;
        if (!tsEvalNode(n->r.get(), vec, b)) return false;
        bool hasZero = false;
        for (const auto& pw : a) if (pw.first == 0) hasZero = true;
        for (const auto& pw : b) if (pw.first == 0) hasZero = true;
        if (hasZero) {
            // Position information unavailable on a side: degrade to plain
            // co-occurrence (both sides present).
            out = a;
            out.insert(out.end(), b.begin(), b.end());
            return true;
        }
        for (const auto& la : a) {
            for (const auto& rb : b) {
                if (rb.first == la.first + 1) {
                    out.push_back(la);
                    out.push_back(rb);
                    return true;
                }
            }
        }
        return false;
    }
    }
    return false;
}

static bool tsQueryMatch(const TsVectorLex& vec, const std::string& query) {
    TsQueryParser p(query);
    auto root = p.parseOr();
    if (!root) return false;
    std::vector<std::pair<int, char>> hits;
    return tsEvalNode(root.get(), vec, hits);
}

// Every literal lexeme of a query (for ts_rank coverage accounting);
// includes lexemes under <-> phrase nodes.
struct TsQueryTerms {
    bool allAnd = true;               // no '|' encountered
    std::vector<std::string> terms;   // every literal lexeme
};
static TsQueryTerms tsQueryTerms(const std::string& q) {
    TsQueryTerms out;
    std::string cur;
    for (size_t i = 0; i < q.size(); ++i) {
        char ch = q[i];
        if (std::isalnum(static_cast<unsigned char>(ch))) {
            cur += static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        } else if (!cur.empty()) {
            out.terms.push_back(cur);
            cur.clear();
            if (ch == '|') out.allAnd = false;
        } else if (ch == '|') {
            out.allAnd = false;
        }
    }
    if (!cur.empty()) out.terms.push_back(cur);
    return out;
}

static ExprValue tsMatch(const std::string& vecText, const std::string& query) {
    TsVectorLex lex = tsLexemesW(vecText);
    if (!tsQueryMatch(lex, query)) return ExprValue("boolean", "f", false);
    return ExprValue("boolean", "t", false);
}

void ExprEvaluator::registerBuiltins() {
    const std::time_t stableClock = std::time(nullptr);
    const std::string stableDate =
        formatUtcClock(stableClock, "%Y-%m-%d");
    const std::string stableTimestamp =
        formatUtcClock(stableClock, "%Y-%m-%d %H:%M:%S");
    const std::string stableTime =
        formatUtcClock(stableClock, "%H:%M:%S");

    auto requireXmlArity = [](const std::vector<ExprValue>& arguments,
                              size_t expected,
                              const char* signature) {
        if (arguments.size() != expected) {
            throw DbError("42883", std::string("function ") + signature +
                                      " does not exist");
        }
    };
    auto xmlWellFormed = [requireXmlArity](XmlParseMode mode,
                                           const char* signature,
                                           const std::vector<ExprValue>& a) {
        requireXmlArity(a, 1, signature);
        if (a[0].isNull) return ExprValue("boolean", "", true);
        return ExprValue("boolean",
                         validateXml(a[0].value, mode).ok ? "t" : "f",
                         false);
    };
    functions_["xml_is_well_formed_content"] =
        [xmlWellFormed](const std::vector<ExprValue>& a) {
            return xmlWellFormed(XmlParseMode::Content,
                                 "xml_is_well_formed_content(text)", a);
        };
    functions_["xml_is_well_formed_document"] =
        [xmlWellFormed](const std::vector<ExprValue>& a) {
            return xmlWellFormed(XmlParseMode::Document,
                                 "xml_is_well_formed_document(text)", a);
        };
    // xmloption defaults to CONTENT in PostgreSQL and this engine does not
    // expose a mutable xmloption setting yet.
    functions_["xml_is_well_formed"] =
        [xmlWellFormed](const std::vector<ExprValue>& a) {
            return xmlWellFormed(XmlParseMode::Content,
                                 "xml_is_well_formed(text)", a);
        };
    functions_["xml_is_document"] =
        [xmlWellFormed](const std::vector<ExprValue>& a) {
            return xmlWellFormed(XmlParseMode::Document,
                                 "xml_is_document(xml)", a);
        };
    functions_["xmlconcat"] = [](const std::vector<ExprValue>& a) {
        if (a.empty()) {
            throw DbError("42601", "XMLCONCAT requires at least one argument");
        }
        std::string result;
        bool sawValue = false;
        for (const ExprValue& argument : a) {
            if (argument.isNull) continue;
            const auto validation =
                validateXml(argument.value, XmlParseMode::Content);
            if (!validation.ok) {
                throw DbError("2200N", "invalid XML content: " +
                                         validation.message);
            }
            result += stripXmlDeclaration(argument.value);
            sawValue = true;
        }
        return sawValue ? ExprValue("xml", std::move(result), false)
                        : ExprValue("xml", "", true);
    };
    functions_["xmlcomment"] = [requireXmlArity](
        const std::vector<ExprValue>& a) {
        requireXmlArity(a, 1, "xmlcomment(text)");
        if (a[0].isNull) return ExprValue("xml", "", true);
        if (a[0].value.find("--") != std::string::npos ||
            (!a[0].value.empty() && a[0].value.back() == '-')) {
            throw DbError("2200S", "invalid XML comment");
        }
        const std::string value = "<!--" + a[0].value + "-->";
        const auto validation = validateXml(value, XmlParseMode::Content);
        if (!validation.ok) {
            throw DbError("2200S", "invalid XML comment: " +
                                     validation.message);
        }
        return ExprValue("xml", value, false);
    };

    // pg_notify(text, text) is the expression form of NOTIFY. It is volatile
    // and returns PostgreSQL's void pseudo-type; delivery is staged by the
    // same per-session notification transaction as the utility command.
    functions_["pg_notify"] = [](const std::vector<ExprValue>& a) {
        if (a.size() != 2) {
            throw std::runtime_error(
                "function pg_notify requires exactly two arguments "
                "(SQLSTATE 42883)");
        }
        Session* session = currentSession();
        if (!session) {
            throw std::runtime_error(
                "pg_notify has no active session (SQLSTATE XX000)");
        }
        const std::string channel = a[0].isNull ? "" : a[0].value;
        const std::string payload = a[1].isNull ? "" : a[1].value;
        if (channel.empty()) {
            throw std::runtime_error(
                "channel name cannot be empty (SQLSTATE 22023)");
        }
        if (channel.size() >= 64) {
            throw std::runtime_error(
                "channel name too long (SQLSTATE 22023)");
        }
        if (!notificationManager().publish(
                session->pid, session->currentDB, channel, payload)) {
            throw std::runtime_error(
                "notification payload is too long or contains a zero byte "
                "(SQLSTATE 22023)");
        }
        return ExprValue("void", "", false);
    };
    volatility_["pg_notify"] = 'v';

    functions_["pg_notification_queue_usage"] = [](
            const std::vector<ExprValue>& a) {
        if (!a.empty()) {
            throw std::runtime_error(
                "function pg_notification_queue_usage takes no arguments "
                "(SQLSTATE 42883)");
        }
        std::ostringstream value;
        value << std::setprecision(17)
              << notificationManager().queueUsage();
        return ExprValue("double precision", value.str(), false);
    };
    volatility_["pg_notification_queue_usage"] = 'v';

    // ARRAY[...] constructor (emitted by the parser as a function call so it
    // composes with the expression grammar). Renders the canonical
    // {e1,e2,...} literal text; NULL elements render as NULL.
    functions_["__array_construct"] = [](const std::vector<ExprValue>& a) {
        std::string out = "{";
        for (size_t i = 0; i < a.size(); ++i) {
            if (i > 0) out += ",";
            std::string low;
            for (char ch : a[i].value) low += static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (a[i].isNull || low == "null") {
                out += "NULL";
            } else {
                // Quote when the element contains whitespace, braces, commas
                // or quotes (PostgreSQL array output rules).
                const std::string& v = a[i].value;
                // Nested array literals ({...}) stay bare so subscripting and
                // containment compare canonical text; other elements quote on
                // the PostgreSQL triggers (whitespace/braces/commas/quotes).
                bool needQuote = v.empty() ||
                    (v.front() != '{' &&
                     v.find_first_of(" {},\"") != std::string::npos);
                if (!needQuote) {
                    out += v;
                } else {
                    out += '"';
                    for (char c : v) {
                        if (c == '"' || c == '\\') out += '\\';
                        out += c;
                    }
                    out += '"';
                }
            }
        }
        out += "}";
        return ExprValue("text", out, false);
    };

    // timezone(zone, timestamp) — AT TIME ZONE in function form
    functions_["overlaps"] = [](const std::vector<ExprValue>& a) {
        // (s1, e1) OVERLAPS (s2, e2): ISO-format text compares lexicographically.
        // Swap each pair so start <= end, apply the PG point/interval rules.
        // With one NULL endpoint, the non-NULL endpoint is the only known
        // boundary.  A result is true only when it is strictly inside the
        // other period; otherwise the missing endpoint leaves it unknown.
        if (a.size() != 4) return ExprValue("boolean", "", true);
        std::vector<ExprValue> period = a;
        auto materializeIntervalEnd = [&](size_t start, size_t end) {
            if (toLower(period[end].typeName) != "interval") return;
            if (period[start].isNull || period[end].isNull) {
                period[end] = ExprValue("timestamp", "", true);
                return;
            }
            const IntervalParts interval = parseIntervalText(period[end].value);
            const std::string endpoint = interval.ok
                ? timestampShift(period[start].value, interval, true)
                : "";
            period[end] = ExprValue("timestamp", endpoint, endpoint.empty());
            if (toLower(period[start].typeName) == "date" &&
                period[start].value.size() == 10) {
                period[start].value += " 00:00:00";
                period[start].typeName = "timestamp";
            }
        };
        materializeIntervalEnd(0, 1);
        materializeIntervalEnd(2, 3);

        if ((period[0].isNull && period[1].isNull) ||
            (period[2].isNull && period[3].isNull)) {
            return ExprValue("boolean", "", true);
        }
        size_t s1 = 0, e1 = 1, s2 = 2, e2 = 3;
        if (period[s1].isNull ||
            (!period[e1].isNull && period[s1].value > period[e1].value)) {
            std::swap(s1, e1);
        }
        if (period[s2].isNull ||
            (!period[e2].isNull && period[s2].value > period[e2].value)) {
            std::swap(s2, e2);
        }

        if (period[s1].value > period[s2].value) {
            if (period[e2].isNull) return ExprValue("boolean", "", true);
            if (period[s1].value < period[e2].value)
                return ExprValue("boolean", "t", false);
            if (period[e1].isNull) return ExprValue("boolean", "", true);
            return ExprValue("boolean", "f", false);
        }
        if (period[s1].value < period[s2].value) {
            if (period[e1].isNull) return ExprValue("boolean", "", true);
            if (period[s2].value < period[e1].value)
                return ExprValue("boolean", "t", false);
            if (period[e2].isNull) return ExprValue("boolean", "", true);
            return ExprValue("boolean", "f", false);
        }
        if (period[e1].isNull || period[e2].isNull)
            return ExprValue("boolean", "", true);
        return ExprValue("boolean", "t", false);
    };

    functions_["timezone"] = [](const std::vector<ExprValue>& a) {
        if (a.size() != 2)
            throw DbError("42883", "timezone requires two arguments");
        const std::string inTn = toLower(a[1].typeName);
        const bool timestampIn = inTn == "timestamp" ||
                                 inTn == "timestamp without time zone";
        const bool timestamptzIn = inTn == "timestamptz" ||
                                  inTn == "timestamp with time zone";
        const std::string zoneType = toLower(a[0].typeName);
        const bool intervalZone = zoneType == "interval";
        const bool textZone = zoneType == "text" || zoneType == "varchar" ||
            zoneType == "character varying" || zoneType == "char" ||
            zoneType == "character" || zoneType == "bpchar" ||
            zoneType == "name" || zoneType == "unknown";
        if ((!timestampIn && !timestamptzIn) || (!textZone && !intervalZone))
            throw DbError("42883", "function timezone(" + zoneType + ", " + inTn + ") does not exist");
        const std::string resultType = timestampIn ? "timestamptz" : "timestamp";
        // Resolve the overload before NULL propagation, so NULL::integer is
        // still a type error instead of a successful NULL timestamp.
        if (a[0].isNull || a[1].isNull) return ExprValue(resultType, "", true);
        IntervalParts shift;
        if (intervalZone) {
            shift = parseIntervalText(a[0].value);
            if (!shift.ok || shift.months || shift.days)
                throw DbError("22023", "time zone interval must not include months or days");
            if (timestampIn) {
                if (shift.micros == std::numeric_limits<long long>::lowest())
                    throw DbError("22015", "time zone interval is out of range");
                shift.micros = -shift.micros;
            }
        } else {
            const long long offMin = timezoneOffsetAt(a[0].value, a[1].value, timestampIn);
            shift.micros = (timestampIn ? -offMin : offMin) * 60000000LL;
        }
        std::string out = timestampShift(a[1].value, shift, true);
        if (out.empty()) return ExprValue(resultType, "", true);
        // PG: timezone(zone, timestamptz) -> timestamp (local wall time);
        //     timezone(zone, timestamp)  -> timestamptz (UTC instant).
        // Untyped literals resolve to the timestamptz overload here, as in
        // PostgreSQL, and therefore keep the plain local rendering.
        if (!timestampIn) {
            return ExprValue("timestamp", out, false);
        }
        return ExprValue("timestamptz", out + "+00", false);
    };

    // ------------------ full-text search ------------------
    // to_tsvector([config,] text): tokens -> 'lex':pos entries, canonical.
    functions_["to_tsvector"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a.back().isNull) return ExprValue("tsvector", "", true);
        return ExprValue("tsvector", tsBuildVector(a.back().value), false);
    };
    // plainto_tsquery([config,] text): plain words ANDed together.
    functions_["plainto_tsquery"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a.back().isNull) return ExprValue("tsquery", "", true);
        auto toks = tsTokenize(a.back().value);
        std::string q;
        for (const auto& t : toks) {
            if (!q.empty()) q += " & ";
            q += "'" + t + "'";
        }
        return ExprValue("tsquery", q, false);
    };
    // to_tsquery([config,] text): pass through (already &/|/!/<->-shaped),
    // or synthesize &'d terms for plain words.
    functions_["to_tsquery"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a.back().isNull) return ExprValue("tsquery", "", true);
        const std::string& v = a.back().value;
        if (v.find('&') != std::string::npos || v.find('|') != std::string::npos ||
            v.find('!') != std::string::npos || v.find("<->") != std::string::npos) {
            return ExprValue("tsquery", v, false);
        }
        auto toks = tsTokenize(v);
        std::string q;
        for (const auto& t : toks) {
            if (!q.empty()) q += " & ";
            q += "'" + t + "'";
        }
        return ExprValue("tsquery", q, false);
    };
    // setweight(tsvector, 'A'..'D'): tag every lexeme occurrence of the
    // vector with the given weight letter (PostgreSQL semantics; the
    // canonical output keeps only non-D labels).
    functions_["setweight"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) {
            return ExprValue("tsvector", "", true);
        }
        char w = static_cast<char>(std::toupper(static_cast<unsigned char>(
            !a[1].value.empty() ? a[1].value[0] : 'D')));
        if (w < 'A' || w > 'D') return ExprValue("tsvector", "", true);
        TsVectorLex lex = tsLexemesW(a[0].value);
        std::vector<std::pair<size_t, std::string>> items;  // (sortKey..., lexeme)
        (void)items;
        // Serialize canonically: lexemes sorted length-first then bytewise
        // (the engine normalizer's order), positions merged and sorted.
        std::vector<std::string> keys;
        for (const auto& kv : lex.occ) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end(), [](const std::string& x, const std::string& y) {
            if (x.size() != y.size()) return x.size() < y.size();
            return x < y;
        });
        std::string out;
        for (const auto& k : keys) {
            auto it = lex.occ.find(k);
            if (it == lex.occ.end()) continue;
            std::map<int, char> pos;  // dedupe by position
            for (const auto& pw : it->second) pos[pw.first] = w;
            if (!out.empty()) out += ' ';
            out += '\'' + k + '\'';
            bool any = false;
            for (const auto& pp : pos) {
                if (pp.first <= 0) continue;  // positionless lexeme stays bare
                out += (any ? "," : ":");
                any = true;
                out += std::to_string(pp.first);
                if (w != 'D') out += w;
            }
        }
        std::string canon;
        if (!out.empty() && g_engine.normalizeTsVectorText(out, canon)) return ExprValue("tsvector", canon, false);
        return ExprValue("tsvector", out, false);
    };
    // ts_rank(tsvector, tsquery [, weights float4[]]): matched-term
    // coverage weighted by the query's term count and positional density
    // of matches.  The optional weights array follows PostgreSQL's
    // {D,C,B,A} ordering (default {0.1,0.2,0.4,1.0}): each occurrence
    // contributes its weight letter's value.
    functions_["ts_rank"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) {
            return ExprValue("double precision", "", true);
        }
        // weights: array literal {d,c,b,a}
        double w[4] = {0.1, 0.2, 0.4, 1.0};
        if (a.size() >= 3 && !a[2].isNull) {
            const std::string& wv = a[2].value;
            std::vector<double> parsed;
            std::string cur;
            for (char ch : wv) {
                if (ch == '{' || ch == ' ') continue;
                if (ch == ',' || ch == '}') {
                    if (!cur.empty()) {
                        try { parsed.push_back(std::stod(cur)); }
                        catch (...) { parsed.push_back(0.0); }
                        cur.clear();
                    }
                    if (ch == '}') break;
                } else {
                    cur += ch;
                }
            }
            if (parsed.size() >= 4) {
                // PostgreSQL order: {D-weight, C, B, A}
                w[0] = parsed[0]; w[1] = parsed[1]; w[2] = parsed[2]; w[3] = parsed[3];
            }
        }
        TsVectorLex lex = tsLexemesW(a[0].value);
        TsQueryTerms qt = tsQueryTerms(a[1].value);
        if (qt.terms.empty()) {
            return ExprValue("double precision", "0", false);
        }
        size_t matchedTerms = 0;
        double weightedDensity = 0;
        for (const auto& t : qt.terms) {
            auto it = lex.occ.find(t);
            if (it == lex.occ.end()) continue;
            ++matchedTerms;
            for (const auto& pw : it->second) {
                // weights[] is in PostgreSQL's {D,C,B,A} order, so letter A
                // maps to slot 3 and letter D to slot 0.
                int idx = (pw.second >= 'A' && pw.second <= 'D')
                              ? ('D' - pw.second)
                              : 0;
                weightedDensity += w[idx] * (pw.first > 0 ? 1.0 / pw.first : 1.0);
            }
        }
        // Term coverage stays monotone in matched terms; the weights array
        // scales each occurrence's positional-density contribution by its
        // weight letter (PostgreSQL {D,C,B,A} ordering).
        double coverage = static_cast<double>(matchedTerms) /
                          static_cast<double>(qt.terms.size());
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.4f",
                      0.05 + coverage * 0.4 +
                          (coverage > 0 ? std::min(0.5, weightedDensity * 0.1) : 0.0));
        return ExprValue("double precision", buf, false);
    };

    functions_["abs"] = [](const std::vector<ExprValue>& a) {
        if (a.empty()) return ExprValue("numeric", "", true);
        const std::string type = toLower(a[0].typeName);
        if (a[0].isNull) {
            const std::string resultType =
                type == "real" || type == "float4" ? "real" :
                type == "double precision" || type == "double" ||
                type == "float8" ? "double precision" : a[0].typeName;
            return ExprValue(resultType, "", true);
        }
        if (isIntegerTypeName(a[0].typeName)) {
            long long value = 0;
            if (!parseInt64Exact(a[0].value, value) ||
                value == std::numeric_limits<int64_t>::lowest()) {
                throw std::runtime_error(
                    "integer out of range (SQLSTATE 22003)");
            }
            return ExprValue(
                a[0].typeName,
                std::to_string(value < 0 ? -value : value), false);
        }
        if (isNumericTypeName(a[0].typeName)) {
            auto n = tryParseNumeric(a[0].value);
            if (n) {
                if (n->isNaN())
                    return ExprValue("numeric", "NaN", false);
                if (n->isInfinite())
                    return ExprValue("numeric", "Infinity", false);
                return ExprValue(
                    "numeric", (n->sign() < 0 ? -(*n) : *n).toString(), false);
            }
        }
        if (type == "real" || type == "float4") {
            const float value = std::fabs(
                static_cast<float>(a[0].asDouble()));
            return ExprValue(
                "real", formatFloatingCastValue(value), false);
        }
        const double value = std::fabs(a[0].asDouble());
        return ExprValue(
            "double precision", formatFloatingCastValue(value), false);
    };
    functions_["length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("integer", "", true);
        }
        const std::string type = toLower(a[0].typeName);
        const bool canonicalBytea = isCanonicalByteaType(type);
        const bool byteLength = canonicalBytea || type == "binary" ||
                                type == "varbinary";
        size_t length = 0;
        if (canonicalBytea && a.size() >= 2) {
            const std::string bytes = parseByteaOrThrow(a[0]).bytes();
            (void)convertEncodingOrThrow(
                bytes, parseEncodingOrThrow(a[1]), BuiltinEncoding::Utf8,
                &length);
        } else if (canonicalBytea) {
            length = parseByteaOrThrow(a[0]).bytes().size();
        } else if (byteLength) {
            length = a[0].value.size();
        } else {
            length = utf8CharCount(a[0].value.substr(
                0, logicalCharacterByteLength(a[0])));
        }
        return ExprValue("integer", std::to_string(length), false);
    };
    functions_["lower"] = [](const std::vector<ExprValue>& a) {
        if (a.empty()) return ExprValue("text", "", true);
        // Overload: lower(anyrange) returns the lower bound (NULL if unbounded).
        if (typeIsRange(a[0].typeName)) {
            const std::string boundType = rangeBoundType(a[0].typeName);
            if (a[0].isNull) return ExprValue(boundType, "", true);
            RangeParts r = parseRangeLiteral(a[0].value);
            if (!r.valid || r.empty || r.loInf)
                return ExprValue(boundType, "", true);
            return ExprValue(boundType, r.lo, false);
        }
        ExprValue result("text", a[0].isNull
            ? "" : toLower(textArgumentValue(a[0])), a[0].isNull);
        result.collation = a[0].collation;
        return result;
    };
    functions_["upper"] = [](const std::vector<ExprValue>& a) {
        if (a.empty()) return ExprValue("text", "", true);
        // Overload: upper(anyrange) returns the upper bound (NULL if unbounded).
        if (typeIsRange(a[0].typeName)) {
            const std::string boundType = rangeBoundType(a[0].typeName);
            if (a[0].isNull) return ExprValue(boundType, "", true);
            RangeParts r = parseRangeLiteral(a[0].value);
            if (!r.valid || r.empty || r.hiInf)
                return ExprValue(boundType, "", true);
            return ExprValue(boundType, r.hi, false);
        }
        std::string s = a[0].isNull ? "" : textArgumentValue(a[0]);
        for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        ExprValue result("text", std::move(s), a[0].isNull);
        result.collation = a[0].collation;
        return result;
    };
    functions_["substring"] = evaluateTextSubstring;
    auto scaleFloatingDecimal = [](double value, int scale,
                                   bool roundToNearest) {
        if (!std::isfinite(value) || value == 0.0) return value;

        // A double can represent decimal exponents down to roughly -324
        // (including subnormals) and finite values only through 1e308.
        // Outside that window the correctly scaled double is known without
        // constructing a zero or infinite pow(10, scale) multiplier.
        if (scale >= 324) return value;
        if (scale < -std::numeric_limits<double>::max_exponent10)
            return std::copysign(0.0, value);

        const double quantum = std::pow(10.0, -scale);
        if (quantum == 0.0) return value;
        if (!std::isfinite(quantum)) return std::copysign(0.0, value);

        const double scaled = value / quantum;
        if (!std::isfinite(scaled)) return value;
        const double integral = roundToNearest
            ? std::nearbyint(scaled) : std::trunc(scaled);
        return integral * quantum;
    };
    functions_["round"] = [scaleFloatingDecimal](const std::vector<ExprValue>& a) {
        if (a.empty()) return ExprValue("double precision", "", true);
        const std::string inputType = toLower(a[0].typeName);
        const bool exactNumeric = inputType == "numeric" ||
                                  inputType == "decimal";
        if (a[0].isNull || (a.size() >= 2 && a[1].isNull)) {
            return ExprValue(
                exactNumeric ? "numeric" : "double precision", "", true);
        }
        if (exactNumeric) {
            auto n = tryParseNumeric(a[0].value);
            if (n) {
                int scale = 0;
                if (a.size() >= 2) {
                    long long requestedScale = 0;
                    if (!parseInt64Exact(a[1].value, requestedScale) ||
                        requestedScale < std::numeric_limits<int>::lowest() ||
                        requestedScale > std::numeric_limits<int>::max()) {
                        throw std::runtime_error(
                            "integer out of range (SQLSTATE 22003)");
                    }
                    scale = static_cast<int>(requestedScale);
                }
                try {
                    return ExprValue(
                        "numeric", n->withScale(scale).toString(), false);
                } catch (const std::invalid_argument&) {
                    throw std::runtime_error(
                        "numeric value out of range (SQLSTATE 22003)");
                }
            }
        }
        // float8 round: half-to-even (rint), like PG float8.
        double v = a[0].asDouble();
        if (a.size() >= 2) {
            long long requestedScale = 0;
            if (!parseInt64Exact(a[1].value, requestedScale) ||
                requestedScale < std::numeric_limits<int>::lowest() ||
                requestedScale > std::numeric_limits<int>::max()) {
                throw std::runtime_error(
                    "integer out of range (SQLSTATE 22003)");
            }
            const int p = static_cast<int>(requestedScale);
            v = scaleFloatingDecimal(v, p, true);
        } else {
            v = std::nearbyint(v);
        }
        return ExprValue(
            "double precision", formatFloatingCastValue(v), false);
    };
    // Internal IS NULL / IS NOT NULL forms (rewritten from postfix syntax
    // by the projection router and the scalar executor).
    functions_["is_null"] = [](const std::vector<ExprValue>& a) {
        bool n = a.empty() || a[0].isNull;
        return ExprValue("boolean", n ? "t" : "f", false);
    };
    functions_["is_not_null"] = [](const std::vector<ExprValue>& a) {
        bool n = a.empty() || a[0].isNull;
        return ExprValue("boolean", n ? "f" : "t", false);
    };
    functions_["now"] = [stableTimestamp](const std::vector<ExprValue>&) {
        return ExprValue(
            "timestamptz", stableTimestamp + "+00", stableTimestamp.empty());
    };

    // ------------------------------------------------------------------------
    // Math functions
    // ------------------------------------------------------------------------
    // PG float8 text output: shortest decimal that round-trips (Ryu-style
    // dtoa), tried from the fewest significant digits upward.
    auto float8Text = [](double v) {
        if (v != v) return std::string("NaN");
        if (std::isinf(v))
            return std::string(std::signbit(v) ? "-Infinity" : "Infinity");
        char buf[64];
        for (int prec = 15; prec <= 17; ++prec) {
            std::snprintf(buf, sizeof buf, "%.*g", prec, v);
            if (std::strtod(buf, nullptr) == v) break;
        }
        return std::string(buf);
    };
    auto float8Unary = [float8Text](const std::vector<ExprValue>& a,
                                    double (*fn)(double)) {
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        return ExprValue("double precision", float8Text(fn(a[0].asDouble())), false);
    };
    auto boundedFloat8 = [float8Text](const std::vector<ExprValue>& a,
                                      double (*fn)(double), double lower,
                                      double upper) {
        if (a.empty() || a[0].isNull)
            return ExprValue("double precision", "", true);
        const double argument = a[0].asDouble();
        if (argument < lower || argument > upper) {
            throw std::runtime_error(
                "input is out of range (SQLSTATE 22003)");
        }
        return ExprValue(
            "double precision", float8Text(fn(argument)), false);
    };
    auto finiteFloat8 = [boundedFloat8](const std::vector<ExprValue>& a,
                                        double (*fn)(double)) {
        const double maximum = std::numeric_limits<double>::max();
        return boundedFloat8(a, fn, -maximum, maximum);
    };
    functions_["sin"]   = [finiteFloat8](const auto& a) { return finiteFloat8(a, std::sin); };
    functions_["cos"]   = [finiteFloat8](const auto& a) { return finiteFloat8(a, std::cos); };
    functions_["tan"]   = [finiteFloat8](const auto& a) { return finiteFloat8(a, std::tan); };
    functions_["asin"]  = [boundedFloat8](const auto& a) {
        return boundedFloat8(a, std::asin, -1.0, 1.0);
    };
    functions_["acos"]  = [boundedFloat8](const auto& a) {
        return boundedFloat8(a, std::acos, -1.0, 1.0);
    };
    functions_["atan"]  = [float8Unary](const auto& a) { return float8Unary(a, std::atan); };
    // PG presents exp/ln/log/sqrt as numeric with fixed display scales:
    //   exp: 15 frac digits for integer input, 16 for fractional input;
    //   ln/log/log10: 16 for fractional or (ln) any input, bare for exact int log of int;
    //   sqrt: 15 for fractional input, bare for integer input.
    // Values are computed in long double to reproduce PG's 16th digit.
    auto numericFixed = [](long double v, int frac) {
        std::ostringstream out;
        out << std::fixed << std::setprecision(frac) << v;
        return ExprValue("numeric", out.str(), false);
    };
    auto argHasDot = [](const std::vector<ExprValue>& a) -> bool {
        return !a.empty() && !a[0].isNull && a[0].value.find('.') != std::string::npos;
    };
    auto requireLogarithmArgument = [](long double value,
                                       bool allowOne = true) {
        if (value <= 0 || (!allowOne && value == 1)) {
            throw std::runtime_error(
                "invalid argument for logarithm (SQLSTATE 2201E)");
        }
    };
    functions_["exp"] = [numericFixed, argHasDot](const auto& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (const auto exact = tryParseNumeric(a[0].value)) {
            if (exact->isNaN()) return ExprValue("numeric", "NaN", false);
            if (exact->isInfinite()) {
                return ExprValue(
                    "numeric",
                    exact->toString().front() == '-' ? "0" : "Infinity",
                    false);
            }
        }
        const long double result = expl(a[0].asDouble());
        if (!std::isfinite(result)) {
            throw std::runtime_error(
                "numeric value out of range (SQLSTATE 22003)");
        }
        return numericFixed(result, argHasDot(a) ? 16 : 15);
    };
    functions_["ln"] =
        [numericFixed, requireLogarithmArgument](const auto& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (const auto exact = tryParseNumeric(a[0].value)) {
            if (exact->isNaN()) return ExprValue("numeric", "NaN", false);
            if (exact->isInfinite() && exact->toString().front() != '-')
                return ExprValue("numeric", "Infinity", false);
        }
        const long double value = a[0].asDouble();
        requireLogarithmArgument(value);
        return numericFixed(logl(value), 16);
    };
    functions_["sqrt"] = [numericFixed, argHasDot](const auto& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (const auto exact = tryParseNumeric(a[0].value)) {
            if (exact->isNaN()) return ExprValue("numeric", "NaN", false);
            if (exact->isInfinite() && exact->toString().front() != '-')
                return ExprValue("numeric", "Infinity", false);
        }
        const long double input = a[0].asDouble();
        if (input < 0) {
            throw std::runtime_error(
                "cannot take square root of a negative number "
                "(SQLSTATE 2201F)");
        }
        long double v = sqrtl(input);
        if (!argHasDot(a)) {
            if (v == floorl(v)) return ExprValue("numeric", std::to_string(static_cast<long long>(v)), false);
            return numericFixed(v, 15);
        }
        return numericFixed(v, 15);
    };
    functions_["log"] = [numericFixed, argHasDot,
                          requireLogarithmArgument](
                            const std::vector<ExprValue>& a) {
        // log(x) = base-10 log; log(b, x) = base-b log.
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (a.size() >= 2) {
            if (a[1].isNull) return ExprValue("numeric", "", true);
            const auto exactBase = tryParseNumeric(a[0].value);
            const auto exactValue = tryParseNumeric(a[1].value);
            if ((exactBase && exactBase->isNaN()) ||
                (exactValue && exactValue->isNaN())) {
                return ExprValue("numeric", "NaN", false);
            }
            long double b = a[0].asDouble(), x = a[1].asDouble();
            requireLogarithmArgument(b, false);
            requireLogarithmArgument(x);
            if (exactBase && exactBase->isInfinite()) {
                if (exactValue && exactValue->isInfinite())
                    return ExprValue("numeric", "NaN", false);
                return ExprValue("numeric", "0", false);
            }
            if (exactValue && exactValue->isInfinite()) {
                return ExprValue(
                    "numeric", b > 1 ? "Infinity" : "-Infinity", false);
            }
            return numericFixed(logl(x) / logl(b), 16);
        }
        if (const auto exact = tryParseNumeric(a[0].value)) {
            if (exact->isNaN()) return ExprValue("numeric", "NaN", false);
            if (exact->isInfinite() && exact->toString().front() != '-')
                return ExprValue("numeric", "Infinity", false);
        }
        const long double value = a[0].asDouble();
        requireLogarithmArgument(value);
        long double v = log10l(value);
        if (!argHasDot(a) && v == floorl(v))
            return ExprValue("numeric", std::to_string(static_cast<long long>(v)), false);
        return numericFixed(v, 16);
    };
    functions_["log10"] = [numericFixed, argHasDot,
                             requireLogarithmArgument](const auto& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        if (const auto exact = tryParseNumeric(a[0].value)) {
            if (exact->isNaN()) return ExprValue("numeric", "NaN", false);
            if (exact->isInfinite() && exact->toString().front() != '-')
                return ExprValue("numeric", "Infinity", false);
        }
        const long double value = a[0].asDouble();
        requireLogarithmArgument(value);
        long double v = log10l(value);
        if (!argHasDot(a) && v == floorl(v))
            return ExprValue("numeric", std::to_string(static_cast<long long>(v)), false);
        return numericFixed(v, 16);
    };
    auto integralRound = [float8Text](const std::vector<ExprValue>& a,
                                      bool towardPositiveInfinity) {
        if (a.empty())
            return ExprValue("double precision", "", true);
        const std::string type = toLower(a[0].typeName);
        const bool exactNumeric = type == "numeric" || type == "decimal";
        if (a[0].isNull) {
            return ExprValue(
                exactNumeric ? "numeric" : "double precision", "", true);
        }
        if (exactNumeric) {
            const auto value = tryParseNumeric(a[0].value);
            if (!value) return ExprValue("numeric", "", true);
            if (!value->isFinite())
                return ExprValue("numeric", value->toString(), false);

            const std::string text = value->toString();
            const size_t decimalPoint = text.find('.');
            if (decimalPoint == std::string::npos)
                return ExprValue("numeric", text, false);
            const bool hasFraction = std::any_of(
                text.begin() + static_cast<std::ptrdiff_t>(decimalPoint + 1),
                text.end(), [](char digit) { return digit != '0'; });
            std::string integerText = text.substr(0, decimalPoint);
            if (integerText.empty() || integerText == "-") integerText += '0';
            Numeric result(integerText);
            if (hasFraction && towardPositiveInfinity && value->sign() > 0)
                result += Numeric(1);
            if (hasFraction && !towardPositiveInfinity && value->sign() < 0)
                result -= Numeric(1);
            return ExprValue("numeric", result.toString(), false);
        }

        const double result = towardPositiveInfinity
            ? std::ceil(a[0].asDouble()) : std::floor(a[0].asDouble());
        return ExprValue("double precision", float8Text(result), false);
    };
    functions_["cbrt"]  = [float8Unary](const auto& a) { return float8Unary(a, std::cbrt); };
    functions_["ceil"]  = [integralRound](const auto& a) { return integralRound(a, true); };
    functions_["floor"] = [integralRound](const auto& a) { return integralRound(a, false); };
    functions_["trunc"] = [scaleFloatingDecimal,
                            float8Text](const std::vector<ExprValue>& a) {
        // trunc(x) truncates toward zero; trunc(x, n) keeps n decimal places.
        if (a.empty()) return ExprValue("double precision", "", true);
        const std::string inputType = toLower(a[0].typeName);
        const bool exactNumeric = inputType == "numeric" ||
                                  inputType == "decimal";
        if (a[0].isNull || (a.size() >= 2 && a[1].isNull)) {
            return ExprValue(
                exactNumeric ? "numeric" : "double precision", "", true);
        }
        if (exactNumeric) {
            auto value = tryParseNumeric(a[0].value);
            if (value) {
                long long requestedScale = 0;
                if (a.size() >= 2 &&
                    (!parseInt64Exact(a[1].value, requestedScale) ||
                     requestedScale < std::numeric_limits<int>::lowest() ||
                     requestedScale > std::numeric_limits<int>::max())) {
                    throw std::runtime_error(
                        "integer out of range (SQLSTATE 22003)");
                }

                if (!value->isFinite())
                    return ExprValue("numeric", value->toString(), false);

                std::string text = value->toString();
                if (requestedScale >= 0) {
                    const int scale = static_cast<int>(requestedScale);
                    if (scale >= value->scale()) {
                        try {
                            return ExprValue(
                                "numeric", value->withScale(scale).toString(),
                                false);
                        } catch (const std::invalid_argument&) {
                            throw std::runtime_error(
                                "numeric value out of range (SQLSTATE 22003)");
                        }
                    }

                    const size_t decimalPoint = text.find('.');
                    text.resize(decimalPoint + (scale == 0 ? 0 : 1 + scale));
                    return ExprValue(
                        "numeric", Numeric(text).toString(), false);
                }

                const bool negative = !text.empty() && text.front() == '-';
                const size_t integerStart = negative ? 1 : 0;
                const size_t decimalPoint = text.find('.');
                std::string integerPart = text.substr(
                    integerStart, decimalPoint - integerStart);
                const uint64_t places =
                    static_cast<uint64_t>(-(requestedScale + 1)) + 1;
                if (places >= integerPart.size())
                    return ExprValue("numeric", "0", false);

                integerPart.replace(integerPart.size() - places,
                                    static_cast<size_t>(places),
                                    static_cast<size_t>(places), '0');
                const bool isZero = std::all_of(
                    integerPart.begin(), integerPart.end(),
                    [](char digit) { return digit == '0'; });
                return ExprValue(
                    "numeric",
                    negative && !isZero ? "-" + integerPart : integerPart,
                    false);
            }
        }

        double v = a[0].asDouble();
        if (a.size() >= 2) {
            long long requestedScale = 0;
            if (!parseInt64Exact(a[1].value, requestedScale) ||
                requestedScale < std::numeric_limits<int>::lowest() ||
                requestedScale > std::numeric_limits<int>::max()) {
                throw std::runtime_error(
                    "integer out of range (SQLSTATE 22003)");
            }
            const int n = static_cast<int>(requestedScale);
            return ExprValue(
                "double precision",
                float8Text(scaleFloatingDecimal(v, n, false)), false);
        }
        return ExprValue(
            "double precision", float8Text(std::trunc(v)), false);
    };

    functions_["atan2"] = [float8Text](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         float8Text(std::atan2(a[0].asDouble(), a[1].asDouble())), false);
    };
    // power(a, b): PG numeric-power semantics. Both args integral and the
    // result exact -> bare integer; otherwise 16 fractional digits (PG's
    // numeric exp/ln presentation, e.g. power(2.5,2) -> 6.2500000000000000).
    functions_["power"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2)
            return ExprValue("double precision", "", true);
        const std::string leftType = toLower(a[0].typeName);
        const std::string rightType = toLower(a[1].typeName);
        const bool exactNumeric = leftType == "numeric" ||
                                  leftType == "decimal" ||
                                  rightType == "numeric" ||
                                  rightType == "decimal";
        const char* resultType = exactNumeric ? "numeric" : "double precision";
        if (a[0].isNull || a[1].isNull)
            return ExprValue(resultType, "", true);
        const long double baseValue = a[0].asDouble();
        const long double exponentValue = a[1].asDouble();
        if ((baseValue == 0 && exponentValue < 0) ||
            (baseValue < 0 && std::isfinite(exponentValue) &&
             std::trunc(exponentValue) != exponentValue)) {
            throw std::runtime_error(
                "invalid argument for power function (SQLSTATE 2201F)");
        }
        auto isIntVal = [](const ExprValue& e) {
            if (e.isNull) return false;
            std::string s = e.value;
            return s.find('.') == std::string::npos;
        };
        // PG power presents 17 significant digits: display scale =
        // 17 - integerDigits(result) (6.25 -> 16 frac, 15.625 -> 15,
        // 1801.00081924 -> 13).
        auto displayScale = [](long double val) -> int {
            int intDigits = (val == 0) ? 1 : static_cast<int>(std::floor(std::log10(std::fabs(val > 0 ? val : -val)))) + 1;
            int sc = 17 - intDigits;
            return sc < 0 ? 0 : sc;
        };
        // Small integer exponent on a decimal base: PG multiplies exactly
        // in numeric arithmetic (power_var_int).
        long long integerExponent = 0;
        if (isIntVal(a[1]) &&
            parseInt64Exact(a[1].value, integerExponent)) {
            const long long e = integerExponent;
            auto nb = tryParseNumeric(a[0].value);
            if (nb && e >= -1000 && e <= 1000) {
                try {
                    Numeric r(1);
                    Numeric base = (e < 0) ? (Numeric(1) / *nb) : *nb;
                    const long long exponentMagnitude = e < 0 ? -e : e;
                    for (long long i = 0; i < exponentMagnitude; ++i)
                        r = r * base;
                    if (!r.isFinite() && !exactNumeric) {
                        throw std::runtime_error(
                            "numeric value out of range (SQLSTATE 22003)");
                    }
                    if (!r.isFinite())
                        return ExprValue("numeric", r.toString(), false);
                    if (!exactNumeric && isIntVal(a[0]) && isIntVal(a[1])) {
                        std::string rs = r.toString();
                        if (rs.find('.') == std::string::npos)
                            return ExprValue("double precision", rs, false);
                    }
                    long double rl =
                        std::strtold(r.toString().c_str(), nullptr);
                    return ExprValue(
                        resultType,
                        r.withScale(displayScale(rl)).toString(), false);
                } catch (const std::invalid_argument&) {
                    throw std::runtime_error(
                        "numeric value out of range (SQLSTATE 22003)");
                }
            }
        }
        // PG numeric power computes exp/ln in extended precision; long double matches its 16-digit output.
        long double lv = powl(a[0].asDouble(), a[1].asDouble());
        if (!std::isfinite(lv) && !exactNumeric) {
            throw std::runtime_error(
                "numeric value out of range (SQLSTATE 22003)");
        }
        if (!std::isfinite(lv)) {
            return ExprValue(
                "numeric", std::isnan(lv) ? "NaN" :
                std::signbit(lv) ? "-Infinity" : "Infinity", false);
        }
        double v = static_cast<double>(lv);
        if (!exactNumeric && isIntVal(a[0]) && isIntVal(a[1]) &&
            v == std::floor(v) && std::fabs(v) < 1e15)
            return ExprValue("double precision", std::to_string(static_cast<long long>(v)), false);
        std::ostringstream out;
        out << std::fixed << std::setprecision(displayScale(lv)) << lv;
        return ExprValue(resultType, out.str(), false);
    };
    functions_["mod"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("integer", "", true);
        auto isDecimal = [](const ExprValue& value) {
            const std::string type = toLower(value.typeName);
            return type == "numeric" || type == "decimal" ||
                   value.value.find('.') != std::string::npos;
        };
        if (isDecimal(a[0]) || isDecimal(a[1])) {
            const auto left = tryParseNumeric(a[0].value);
            const auto right = tryParseNumeric(a[1].value);
            if (!left || !right)
                return ExprValue("numeric", "", true);
            if (!left->isNaN() && !right->isNaN() && right->isFinite() &&
                right->sign() == 0) {
                throw std::runtime_error(
                    "division by zero (SQLSTATE 22012)");
            }
            try {
                Numeric remainder;
                if (left->isNaN() || right->isNaN() ||
                    left->isInfinite()) {
                    remainder = Numeric::nan();
                } else if (right->isInfinite()) {
                    remainder = *left;
                } else {
                    remainder = *left -
                        numericTruncatedQuotient(*left, *right) * *right;
                }
                auto textScale = [](const std::string& value) {
                    const size_t point = value.find('.');
                    return point == std::string::npos
                        ? 0 : static_cast<int>(value.size() - point - 1);
                };
                const int scale = std::max(
                    textScale(a[0].value), textScale(a[1].value));
                return ExprValue(
                    "numeric", remainder.withScale(scale).toString(), false);
            } catch (const std::invalid_argument&) {
                throw std::runtime_error(
                    "numeric value out of range (SQLSTATE 22003)");
            }
        }
        long long left = 0;
        long long right = 0;
        if (!parseInt64Exact(a[0].value, left) ||
            !parseInt64Exact(a[1].value, right)) {
            throw std::runtime_error(
                "integer out of range (SQLSTATE 22003)");
        }
        if (right == 0)
            throw std::runtime_error(
                "division by zero (SQLSTATE 22012)");
        const long long remainder =
            left == std::numeric_limits<int64_t>::lowest() && right == -1
                ? 0 : left % right;
        return ExprValue("integer", std::to_string(remainder), false);
    };
    functions_["sign"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull)
            return ExprValue("double precision", "", true);
        const std::string type = toLower(a[0].typeName);
        if (type == "numeric" || type == "decimal") {
            const auto value = tryParseNumeric(a[0].value);
            if (!value) return ExprValue("numeric", "", true);
            if (value->isNaN())
                return ExprValue("numeric", "NaN", false);
            int result = value->sign();
            if (value->isInfinite())
                result = value->toString().front() == '-' ? -1 : 1;
            return ExprValue("numeric", std::to_string(result), false);
        }
        const double value = a[0].asDouble();
        return ExprValue(
            "double precision",
            std::to_string(value > 0 ? 1 : (value < 0 ? -1 : 0)), false);
    };
    functions_["pi"] = [float8Text](const std::vector<ExprValue>&) {
        return ExprValue("double precision", float8Text(std::atan(1.0) * 4.0), false);
    };
    functions_["random"] = [](const std::vector<ExprValue>&) {
        return ExprValue("double precision", std::to_string(static_cast<double>(std::rand()) / RAND_MAX), false);
    };
    functions_["gen_random_uuid"] = [](const std::vector<ExprValue>& a) {
        if (!a.empty()) {
            throw std::runtime_error(
                "function gen_random_uuid() does not accept arguments "
                "(SQLSTATE 42883)");
        }
        return ExprValue("uuid", UuidValue::generateV4().toString(), false);
    };
    functions_["uuidv4"] = [](const std::vector<ExprValue>& a) {
        if (!a.empty()) {
            throw std::runtime_error(
                "function uuidv4() does not accept arguments "
                "(SQLSTATE 42883)");
        }
        return ExprValue("uuid", UuidValue::generateV4().toString(), false);
    };
    functions_["uuidv7"] = [](const std::vector<ExprValue>& a) {
        if (a.size() > 1) {
            throw std::runtime_error(
                "function uuidv7(interval) accepts at most one argument "
                "(SQLSTATE 42883)");
        }
        if (!a.empty() && a[0].isNull)
            return ExprValue("uuid", "", true);
        const auto now = std::chrono::time_point_cast<
            std::chrono::microseconds>(std::chrono::system_clock::now());
        int64_t unixMicros = now.time_since_epoch().count();
        if (!a.empty()) {
            const IntervalParts shift = parseIntervalText(a[0].value);
            if (!shift.ok || !shiftUuidTimestamp(
                                 unixMicros, shift, unixMicros)) {
                throw std::runtime_error(
                    "UUIDv7 timestamp is out of range (SQLSTATE 22008)");
            }
        }
        UuidValue uuid;
        if (!UuidValue::generateV7At(unixMicros, a.empty(), uuid)) {
            throw std::runtime_error(
                "UUIDv7 timestamp is out of range (SQLSTATE 22008)");
        }
        return ExprValue("uuid", uuid.toString(), false);
    };
    functions_["uuid_extract_version"] = [](const std::vector<ExprValue>& a) {
        if (a.size() != 1) {
            throw std::runtime_error(
                "function uuid_extract_version(uuid) requires one argument "
                "(SQLSTATE 42883)");
        }
        if (a[0].isNull) return ExprValue("smallint", "", true);
        UuidValue uuid;
        if (!UuidValue::parse(a[0].value, uuid)) {
            throw std::runtime_error(
                "invalid input syntax for type uuid (SQLSTATE 22P02)");
        }
        const int version = uuid.version();
        return version < 0
            ? ExprValue("smallint", "", true)
            : ExprValue("smallint", std::to_string(version), false);
    };
    functions_["uuid_extract_timestamp"] = [](const std::vector<ExprValue>& a) {
        if (a.size() != 1) {
            throw std::runtime_error(
                "function uuid_extract_timestamp(uuid) requires one argument "
                "(SQLSTATE 42883)");
        }
        if (a[0].isNull) return ExprValue("timestamptz", "", true);
        UuidValue uuid;
        if (!UuidValue::parse(a[0].value, uuid)) {
            throw std::runtime_error(
                "invalid input syntax for type uuid (SQLSTATE 22P02)");
        }
        int64_t unixMicros = 0;
        if (!uuid.extractUnixMicros(unixMicros))
            return ExprValue("timestamptz", "", true);
        const std::string timestamp = formatUuidTimestamp(unixMicros);
        return ExprValue(
            "timestamptz", timestamp, timestamp.empty());
    };
    volatility_["uuid_extract_version"] = 'i';
    volatility_["uuid_extract_timestamp"] = 'i';
    // pow — alias of power; ceiling — alias of ceil
    functions_["pow"] = functions_["power"];
    functions_["ceiling"] = [integralRound](const auto& a) { return integralRound(a, true); };
    // degrees / radians
    functions_["degrees"] = [float8Text](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         float8Text(a[0].asDouble() * 180.0 / (std::atan(1.0) * 4.0)), false);
    };
    functions_["radians"] = [float8Text](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("double precision", "", true);
        return ExprValue("double precision",
                         float8Text(a[0].asDouble() * (std::atan(1.0) * 4.0) / 180.0), false);
    };
    // cot — cotangent
    functions_["cot"] = [finiteFloat8](const std::vector<ExprValue>& a) {
        return finiteFloat8(a, [](double value) {
            return 1.0 / std::tan(value);
        });
    };
    // Hyperbolic functions
    functions_["sinh"]  = [float8Unary](const auto& a) { return float8Unary(a, std::sinh); };
    functions_["cosh"]  = [float8Unary](const auto& a) { return float8Unary(a, std::cosh); };
    functions_["tanh"]  = [float8Unary](const auto& a) { return float8Unary(a, std::tanh); };
    functions_["asinh"] = [float8Unary](const auto& a) { return float8Unary(a, std::asinh); };
    functions_["acosh"] = [boundedFloat8](const auto& a) {
        return boundedFloat8(
            a, std::acosh, 1.0,
            std::numeric_limits<double>::infinity());
    };
    functions_["atanh"] = [boundedFloat8](const auto& a) {
        return boundedFloat8(a, std::atanh, -1.0, 1.0);
    };
    // gcd / lcm — integer greatest common divisor / least common multiple
    auto integerBinaryResultType = [](const std::vector<ExprValue>& a) {
        for (size_t i = 0; i < std::min<size_t>(2, a.size()); ++i) {
            const std::string type = toLower(a[i].typeName);
            if (type == "bigint" || type == "int8")
                return std::string("bigint");
        }
        return std::string("integer");
    };
    auto decimalGcdLcm = [](const std::vector<ExprValue>& a,
                            bool leastCommonMultiple)
        -> std::optional<ExprValue> {
        bool numericOverload = false;
        for (size_t i = 0; i < std::min<size_t>(2, a.size()); ++i) {
            const std::string type = toLower(a[i].typeName);
            if (type == "numeric" || type == "decimal" ||
                type.rfind("numeric(", 0) == 0 ||
                type.rfind("decimal(", 0) == 0) {
                numericOverload = true;
            }
        }
        if (!numericOverload) return std::nullopt;
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("numeric", "", true);

        const auto left = tryParseNumeric(a[0].value);
        const auto right = tryParseNumeric(a[1].value);
        if (!left || !right) {
            throw std::runtime_error(
                "invalid input syntax for type numeric (SQLSTATE 22P02)");
        }
        if (!left->isFinite() || !right->isFinite())
            return ExprValue("numeric", "NaN", false);

        const int scale = std::max(numericTextScale(a[0].value, *left),
                                   numericTextScale(a[1].value, *right));
        const std::string leftMagnitude =
            numericMagnitudeAtScale(*left, scale);
        const std::string rightMagnitude =
            numericMagnitudeAtScale(*right, scale);
        const std::string divisor =
            gcdDecimalMagnitudes(leftMagnitude, rightMagnitude);
        std::string result = divisor;
        if (leastCommonMultiple) {
            if (leftMagnitude == "0" || rightMagnitude == "0") {
                result = "0";
            } else {
                const auto quotient =
                    divideDecimalMagnitudes(leftMagnitude, divisor);
                result = multiplyDecimalMagnitudes(
                    quotient.first, rightMagnitude);
                if (result.size() >
                    static_cast<size_t>(Numeric::kMaxPrecision)) {
                    throw std::runtime_error(
                        "numeric value out of range (SQLSTATE 22003)");
                }
            }
        }
        return ExprValue(
            "numeric", formatScaledDecimalMagnitude(result, scale), false);
    };
    functions_["gcd"] = [integerBinaryResultType, decimalGcdLcm](const std::vector<ExprValue>& a) {
        if (const auto numeric = decimalGcdLcm(a, false)) return *numeric;
        const std::string resultType = integerBinaryResultType(a);
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue(resultType, "", true);
        long long left = 0;
        long long right = 0;
        if (!parseInt64Exact(a[0].value, left) ||
            !parseInt64Exact(a[1].value, right)) {
            throw std::runtime_error(
                "integer out of range (SQLSTATE 22003)");
        }
        auto magnitude = [](const long long value) -> uint64_t {
            return value < 0
                ? static_cast<uint64_t>(-(value + 1)) + 1
                : static_cast<uint64_t>(value);
        };
        uint64_t x = magnitude(left);
        uint64_t y = magnitude(right);
        while (y) { uint64_t t = x % y; x = y; y = t; }
        if (x > static_cast<uint64_t>(
                    std::numeric_limits<int64_t>::max())) {
            throw std::runtime_error(
                "integer out of range (SQLSTATE 22003)");
        }
        return ExprValue(resultType, std::to_string(x), false);
    };
    functions_["lcm"] = [integerBinaryResultType, decimalGcdLcm](const std::vector<ExprValue>& a) {
        if (const auto numeric = decimalGcdLcm(a, true)) return *numeric;
        const std::string resultType = integerBinaryResultType(a);
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue(resultType, "", true);
        long long left = 0;
        long long right = 0;
        if (!parseInt64Exact(a[0].value, left) ||
            !parseInt64Exact(a[1].value, right)) {
            throw std::runtime_error(
                "integer out of range (SQLSTATE 22003)");
        }
        auto magnitude = [](const long long value) -> uint64_t {
            return value < 0
                ? static_cast<uint64_t>(-(value + 1)) + 1
                : static_cast<uint64_t>(value);
        };
        const uint64_t x = magnitude(left);
        const uint64_t y = magnitude(right);
        if (x == 0 || y == 0) return ExprValue(resultType, "0", false);
        uint64_t gcd = x;
        uint64_t divisor = y;
        while (divisor) {
            const uint64_t remainder = gcd % divisor;
            gcd = divisor;
            divisor = remainder;
        }
        const unsigned __int128 result =
            static_cast<unsigned __int128>(x / gcd) * y;
        if (result > static_cast<unsigned __int128>(
                         std::numeric_limits<int64_t>::max())) {
            throw std::runtime_error(
                "integer out of range (SQLSTATE 22003)");
        }
        return ExprValue(
            resultType, std::to_string(static_cast<uint64_t>(result)), false);
    };
    // div(y, x) — integer quotient of y / x, truncated toward zero
    functions_["div"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("numeric", "", true);
        const auto dividend = tryParseNumeric(a[0].value);
        const auto divisor = tryParseNumeric(a[1].value);
        if (!dividend || !divisor)
            return ExprValue("numeric", "", true);
        if (divisor->isFinite() && divisor->sign() == 0)
            throw std::runtime_error(
                "division by zero (SQLSTATE 22012)");

        return ExprValue("numeric",
            numericTruncatedQuotient(*dividend, *divisor).toString(), false);
    };
    // factorial(n) — n! for small non-negative n
    functions_["factorial"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        long long n = 0;
        if (!parseInt64Exact(a[0].value, n))
            return ExprValue("numeric", "", true);
        if (n < 0)
            throw std::runtime_error(
                "factorial of a negative number is undefined "
                "(SQLSTATE 22003)");
        Numeric result(1);
        for (long long i = 2; i <= n; ++i) {
            try {
                result *= Numeric(i);
            } catch (const std::invalid_argument&) {
                throw std::runtime_error(
                    "numeric value out of range (SQLSTATE 22003)");
            }
            if (result.precision() > Numeric::kMaxPrecision) {
                throw std::runtime_error(
                    "numeric value out of range (SQLSTATE 22003)");
            }
        }
        return ExprValue("numeric", result.toString(), false);
    };
    // width_bucket(operand, low, high, count) — histogram bucket index (1..count)
    functions_["width_bucket"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 4 || a[0].isNull || a[1].isNull || a[2].isNull || a[3].isNull)
            return ExprValue("integer", "", true);
        double v = a[0].asDouble(), lo = a[1].asDouble(), hi = a[2].asDouble();
        const int64_t count = parseInt32Argument(a[3]);
        if (count <= 0) {
            throw std::runtime_error(
                "count must be greater than zero (SQLSTATE 2201G)");
        }
        if (std::isnan(v) || std::isnan(lo) || std::isnan(hi)) {
            throw std::runtime_error(
                "operand, lower bound, and upper bound cannot be NaN "
                "(SQLSTATE 2201G)");
        }
        if (!std::isfinite(lo) || !std::isfinite(hi)) {
            throw std::runtime_error(
                "lower and upper bounds must be finite (SQLSTATE 2201G)");
        }
        if (lo == hi) {
            throw std::runtime_error(
                "lower bound cannot equal upper bound (SQLSTATE 2201G)");
        }
        bool reversed = lo > hi;
        if (reversed) std::swap(lo, hi);
        auto pastLastBucket = [count]() -> int64_t {
            if (count == std::numeric_limits<int64_t>::max()) {
                throw std::runtime_error(
                    "integer out of range (SQLSTATE 22003)");
            }
            return count + 1;
        };
        int64_t bucket;
        if (v < lo) bucket = reversed ? pastLastBucket() : 0;
        else if (v >= hi) bucket = reversed ? 0 : pastLastBucket();
        else {
            long double rawBucket = std::floor(
                (static_cast<long double>(v) - lo) /
                (static_cast<long double>(hi) - lo) * count) + 1;
            if (!std::isfinite(rawBucket))
                return ExprValue("integer", "", true);
            rawBucket = std::max(
                1.0L, std::min(rawBucket, static_cast<long double>(count)));
            int64_t b = static_cast<int64_t>(rawBucket);
            bucket = reversed ? count - b + 1 : b;
        }
        return ExprValue("integer", std::to_string(bucket), false);
    };

    // ------------------------------------------------------------------------
    // String functions
    // ------------------------------------------------------------------------
    functions_["if"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3) return ExprValue("unknown", "", true);
        return !a[0].isNull && a[0].asBool() ? a[1] : a[2];
    };
    functions_["iif"] = functions_["if"];
    functions_["concat"] = [](const std::vector<ExprValue>& a) {
        // PG concat() ignores NULL arguments rather than returning NULL.
        std::string s;
        for (const auto& v : a) {
            if (v.isNull) continue;
            s += v.value;
        }
        return ExprValue("text", s, false);
    };
    functions_["trim"] = [](const std::vector<ExprValue>& a) {
        // Keyword form: trim([both|leading|trailing] [chars] from s).
        std::string dir = "both";
        size_t idx = 0;
        if (!a.empty() && (a[0].value == "both" || a[0].value == "leading" || a[0].value == "trailing")) { dir = a[0].value; idx = 1; }
        if (a.size() <= idx || a[idx].isNull) return ExprValue("text", "", true);
        if ((idx == 0 && a.size() > 1 && a[1].isNull) ||
            (idx == 1 && a.size() > 2 && a[2].isNull)) {
            return ExprValue("text", "", true);
        }
        std::string s, chars = " ";
        if (idx == 1 && a.size() > 2) {
            chars = textArgumentValue(a[1]);
            s = textArgumentValue(a[2]);
        } else if (idx == 0 && a.size() > 1) {
            s = textArgumentValue(a[0]);
            chars = textArgumentValue(a[1]);
        } else {
            s = textArgumentValue(a[idx]);
        }
        if (dir == "leading") {
            return ExprValue(
                "text", trimUtf8Characters(s, chars, true, false), false);
        }
        if (dir == "trailing") {
            return ExprValue(
                "text", trimUtf8Characters(s, chars, false, true), false);
        }
        return ExprValue(
            "text", trimUtf8Characters(s, chars, true, true), false);
    };
    functions_["ltrim"] = [](const std::vector<ExprValue>& a) {
        if (!a.empty() && isCanonicalByteaType(a[0].typeName))
            return evaluateByteaTrim(a, true, false);
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        std::string chars = (a.size() >= 2 && !a[1].isNull)
            ? textArgumentValue(a[1]) : " ";
        return ExprValue(
            "text", trimUtf8Characters(s, chars, true, false), false);
    };
    functions_["rtrim"] = [](const std::vector<ExprValue>& a) {
        if (!a.empty() && isCanonicalByteaType(a[0].typeName))
            return evaluateByteaTrim(a, false, true);
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        std::string chars = (a.size() >= 2 && !a[1].isNull)
            ? textArgumentValue(a[1]) : " ";
        return ExprValue(
            "text", trimUtf8Characters(s, chars, false, true), false);
    };
    functions_["replace"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        std::string s = textArgumentValue(a[0]);
        const std::string from = textArgumentValue(a[1]);
        const std::string to = textArgumentValue(a[2]);
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
        if (isCanonicalByteaType(a[0].typeName) ||
            isCanonicalByteaType(a[1].typeName)) {
            const std::string needle = parseByteaOrThrow(a[0]).bytes();
            const std::string haystack = parseByteaOrThrow(a[1]).bytes();
            const size_t position = haystack.find(needle);
            return ExprValue(
                "integer",
                position == std::string::npos ? "0" :
                    std::to_string(position + 1),
                false);
        }
        const std::string needle = textArgumentValue(a[0]);
        const std::string haystack = textArgumentValue(a[1]);
        size_t pos = haystack.find(needle);
        if (pos == std::string::npos) return ExprValue("integer", "0", false);
        return ExprValue(
            "integer", std::to_string(utf8CharCount(haystack.substr(0, pos)) + 1),
            false);
    };
    functions_["left"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        int64_t n = parseInt32Argument(a[1]);
        const std::string s = textArgumentValue(a[0]);
        size_t total = utf8CharCount(s);
        const uint64_t magnitude = n < 0
            ? uint64_t{0} - static_cast<uint64_t>(n)
            : static_cast<uint64_t>(n);
        size_t take = n < 0
            ? (magnitude >= total ? 0 : total - static_cast<size_t>(magnitude))
            : (magnitude >= total ? total : static_cast<size_t>(magnitude));
        return ExprValue("text", s.substr(0, utf8ByteAt(s, take)), false);
    };
    functions_["right"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        int64_t n = parseInt32Argument(a[1]);
        const std::string s = textArgumentValue(a[0]);
        size_t total = utf8CharCount(s);
        const uint64_t magnitude = n < 0
            ? uint64_t{0} - static_cast<uint64_t>(n)
            : static_cast<uint64_t>(n);
        size_t skip = n < 0
            ? (magnitude >= total ? total : static_cast<size_t>(magnitude))
            : (magnitude >= total ? 0 : total - static_cast<size_t>(magnitude));
        return ExprValue("text", s.substr(utf8ByteAt(s, skip)), false);
    };
    functions_["repeat"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        const int32_t n = parseInt32Argument(a[1]);
        if (n <= 0) return ExprValue("text", "", false);
        const std::string input = textArgumentValue(a[0]);
        if (!input.empty() &&
            static_cast<size_t>(n) > kMaxTextPayload / input.size()) {
            throw std::runtime_error(
                "requested text length exceeds the limit (SQLSTATE 54000)");
        }
        std::string s;
        s.reserve(input.size() * static_cast<size_t>(n));
        for (int32_t i = 0; i < n; ++i) s += input;
        return ExprValue("text", s, false);
    };
    functions_["reverse"] = [](const std::vector<ExprValue>& a) {
        const bool byteaInput = !a.empty() &&
                                isCanonicalByteaType(a[0].typeName);
        if (a.empty() || a[0].isNull)
            return ExprValue(byteaInput ? "bytea" : "text", "", true);
        if (byteaInput) {
            std::string bytes = parseByteaOrThrow(a[0]).bytes();
            std::reverse(bytes.begin(), bytes.end());
            return ExprValue(
                "bytea", ByteaValue::fromBytes(std::move(bytes)).toString(),
                false);
        }
        const std::string input = textArgumentValue(a[0]);
        std::string result;
        result.reserve(input.size());
        const size_t characters = utf8CharCount(input);
        for (size_t i = characters; i > 0; --i) {
            const size_t begin = utf8ByteAt(input, i - 1);
            const size_t end = utf8ByteAt(input, i);
            result.append(input, begin, end - begin);
        }
        return ExprValue("text", result, false);
    };
    functions_["ascii"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        const std::string input = textArgumentValue(a[0]);
        if (input.empty()) return ExprValue("integer", "0", false);
        uint32_t codePoint = 0;
        if (!decodeFirstUtf8CodePoint(input, codePoint))
            return ExprValue("integer", "", true);
        return ExprValue(
            "integer", std::to_string(codePoint), false);
    };
    functions_["chr"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        long long value = 0;
        if (!parseInt64Exact(a[0].value, value))
            return ExprValue("text", "", true);
        if (value < 0) {
            throw std::runtime_error(
                "character number must be positive (SQLSTATE 22023)");
        }
        if (value == 0) {
            throw std::runtime_error(
                "null character not permitted (SQLSTATE 54000)");
        }
        if (value > 0x10ffff ||
            (value >= 0xd800 && value <= 0xdfff)) {
            throw std::runtime_error(
                "requested character not valid for encoding (SQLSTATE 54000)");
        }
        const std::string result =
            encodeUtf8CodePoint(static_cast<uint32_t>(value));
        return ExprValue("text", result, false);
    };
    // substr — PostgreSQL alias of substring(str, from[, len])
    functions_["substr"] = evaluateTextSubstring;
    // char_length / character_length — UTF-8 character count
    functions_["char_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer",
                         std::to_string(utf8CharCount(a[0].value.substr(
                             0, logicalCharacterByteLength(a[0])))),
                         false);
    };
    functions_["character_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        return ExprValue("integer",
                         std::to_string(utf8CharCount(a[0].value.substr(
                             0, logicalCharacterByteLength(a[0])))),
                         false);
    };
    functions_["octet_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        const size_t length = isBitStringTypeName(a[0].typeName)
            ? (a[0].value.size() + 7) / 8
            : isCanonicalByteaType(a[0].typeName)
                ? parseByteaOrThrow(a[0]).bytes().size()
                : a[0].value.size();
        return ExprValue("integer", std::to_string(length), false);
    };
    functions_["bit_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        if (isCanonicalByteaType(a[0].typeName)) {
            return ExprValue(
                "integer",
                std::to_string(parseByteaOrThrow(a[0]).bytes().size() * 8),
                false);
        }
        if (isBitStringTypeName(a[0].typeName)) {
            return ExprValue(
                "integer", std::to_string(a[0].value.size()), false);
        }
        return ExprValue(
            "integer", std::to_string(logicalCharacterByteLength(a[0]) * 8),
            false);
    };
    // lpad / rpad — pad (or truncate) a string to a target length with a fill string
    functions_["lpad"] = [](const std::vector<ExprValue>& a) {
        return evaluateTextPad(a, true);
    };
    functions_["rpad"] = [](const std::vector<ExprValue>& a) {
        return evaluateTextPad(a, false);
    };
    // btrim(str[, chars]) — trim matching characters (default whitespace) from both ends
    functions_["btrim"] = [](const std::vector<ExprValue>& a) {
        if (!a.empty() && isCanonicalByteaType(a[0].typeName))
            return evaluateByteaTrim(a, true, true);
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        std::string chars = (a.size() >= 2 && !a[1].isNull)
            ? textArgumentValue(a[1]) : " ";
        return ExprValue(
            "text", trimUtf8Characters(s, chars, true, true), false);
    };
    // split_part(str, delim, n) — n-th field (1-based; negative counts from the end)
    functions_["split_part"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        const std::string s = textArgumentValue(a[0]);
        const std::string delim = textArgumentValue(a[1]);
        const int64_t n = parseInt32Argument(a[2]);
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
        else {
            throw std::runtime_error(
                "field position must not be zero (SQLSTATE 22023)");
        }
        if (idx < 0 || idx >= static_cast<int64_t>(parts.size()))
            return ExprValue("text", "", false);
        return ExprValue("text", parts[static_cast<size_t>(idx)], false);
    };
    // strpos(string, substring) — 1-based position of first match, 0 if absent
    functions_["strpos"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("integer", "", true);
        const std::string input = textArgumentValue(a[0]);
        const std::string substring = textArgumentValue(a[1]);
        size_t pos = input.find(substring);
        if (pos == std::string::npos) return ExprValue("integer", "0", false);
        return ExprValue(
            "integer", std::to_string(utf8CharCount(input.substr(0, pos)) + 1),
            false);
    };
    // initcap — capitalize the first letter of each word, lowercase the rest
    functions_["initcap"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string s = textArgumentValue(a[0]);
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
    // to_hex(int) — render the two's-complement width of the selected int4
    // or int8 overload, matching PostgreSQL for negative values.
    functions_["to_hex"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        long long signedValue = 0;
        if (!parseInt64Exact(a[0].value, signedValue)) {
            throw std::runtime_error(
                "invalid input syntax for type integer (SQLSTATE 22P02)");
        }
        const std::string type = toLower(a[0].typeName);
        const bool int8 = type == "bigint" || type == "int8";
        if (!int8 &&
            (signedValue < std::numeric_limits<int32_t>::lowest() ||
             signedValue > std::numeric_limits<int32_t>::max())) {
            throw std::runtime_error(
                "integer out of range (SQLSTATE 22003)");
        }
        uint64_t v = int8
            ? static_cast<uint64_t>(signedValue)
            : static_cast<uint32_t>(signedValue);
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
        const std::string s = textArgumentValue(a[0]);
        const std::string p = textArgumentValue(a[1]);
        bool r = s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
        return ExprValue("boolean", r ? "t" : "f", false);
    };
    // translate(str, from, to) — map each "from" char to the matching "to" char,
    // deleting chars whose "from" index has no "to" counterpart
    functions_["translate"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        const std::string s = textArgumentValue(a[0]);
        const std::string from = textArgumentValue(a[1]);
        const std::string to = textArgumentValue(a[2]);
        auto characters = [](const std::string& text) {
            std::vector<std::string> result;
            const size_t count = utf8CharCount(text);
            result.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                const size_t begin = utf8ByteAt(text, i);
                const size_t end = utf8ByteAt(text, i + 1);
                result.push_back(text.substr(begin, end - begin));
            }
            return result;
        };
        const std::vector<std::string> inputCharacters = characters(s);
        const std::vector<std::string> fromCharacters = characters(from);
        const std::vector<std::string> toCharacters = characters(to);
        std::string out;
        for (const std::string& character : inputCharacters) {
            const auto match = std::find(
                fromCharacters.begin(), fromCharacters.end(), character);
            if (match == fromCharacters.end()) {
                out += character;
                continue;
            }
            const size_t index = static_cast<size_t>(
                std::distance(fromCharacters.begin(), match));
            if (index < toCharacters.size()) out += toCharacters[index];
            // else: char is deleted
        }
        return ExprValue("text", out, false);
    };
    // to_date(text, fmt): parse per the pattern (YYYY/MM/DD widths, literal
    // separators); render the ISO date.
    functions_["to_date"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull || (a.size() >= 2 && a[1].isNull))
            return ExprValue("date", "", true);
        const std::string& s = a[0].value;
        const std::string fmt =
            a.size() >= 2 ? a[1].value : "YYYY-MM-DD";
        Date date;
        int32_t timeSeconds = 0;
        if (!parseTemporalFormatValue(s, fmt, date, timeSeconds))
            return ExprValue("date", "", true);
        return ExprValue("date", str(date), false);
    };

    // to_timestamp(text, fmt) interprets parsed wall time in the session zone
    // and stores a UTC instant, which the result boundary renders locally.
    functions_["to_timestamp"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull || (a.size() >= 2 && a[1].isNull))
            return ExprValue("timestamptz", "", true);
        const std::string& s = a[0].value;
        const std::string fmt = a.size() >= 2
            ? a[1].value : "YYYY-MM-DD HH24:MI:SS";
        Date date;
        int32_t timeSeconds = 0;
        if (!parseTemporalFormatValue(s, fmt, date, timeSeconds))
            return ExprValue("timestamptz", "", true);
        int64_t utcSeconds = parseTimestampToSeconds(
            str(date) + " " + formatTimeSeconds(timeSeconds));
        const Session* session = currentSession();
        if (session) {
            int inputOffset = session->timezoneOffsetMinutes;
            if (const auto namedOffset = dbms::ianaTimezoneOffsetMinutes(
                    session->timeZone, utcSeconds, true))
                inputOffset = *namedOffset;
            utcSeconds -= static_cast<int64_t>(inputOffset) * 60;
        }
        const std::string utcText = formatTimestampSeconds(utcSeconds);
        return ExprValue("timestamptz", utcText + "+00", utcText.empty());
    };

    // to_number(text, fmt): extract the numeric literal; pattern characters
    // (9/0 digits, D decimal point, G grouping, S sign, blanks) guide parsing.
    functions_["to_number"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
        const std::string& s = a[0].value;
        std::string digits;
        bool seenDot = false, seenSign = false;
        for (char c : s) {
            if ((c >= '0' && c <= '9')) digits += c;
            else if (c == '.' && !seenDot) { digits += c; seenDot = true; }
            else if ((c == '-' || c == '+') && !seenSign && digits.empty()) {
                if (c == '-') digits += c;
                seenSign = true;
            }
            // grouping separators / blanks / template letters are skipped
        }
        if (digits.empty() || digits == "-") return ExprValue("numeric", "0", false);
        return ExprValue("numeric", digits, false);
    };

    // overlay(string, newsub, start[, count]) — replace count chars at 1-based start
    functions_["overlay"] = [](const std::vector<ExprValue>& a) {
        const bool byteaInput = !a.empty() &&
                                isCanonicalByteaType(a[0].typeName);
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull ||
            (a.size() >= 4 && a[3].isNull)) {
            return ExprValue(byteaInput ? "bytea" : "text", "", true);
        }
        const std::string s = byteaInput
            ? parseByteaOrThrow(a[0]).bytes() : textArgumentValue(a[0]);
        const std::string repl = byteaInput
            ? parseByteaOrThrow(a[1]).bytes() : textArgumentValue(a[1]);
        const long long start = parseInt32Argument(a[2]);
        long long count = 0;
        if (a.size() >= 4) {
            count = parseInt32Argument(a[3]);
        } else {
            count = static_cast<long long>(
                byteaInput ? repl.size() : utf8CharCount(repl));
        }
        if (start < 1) {
            throw std::runtime_error(
                "negative substring length not allowed (SQLSTATE 22011)");
        }
        const size_t characters = byteaInput ? s.size() : utf8CharCount(s);
        const uint64_t requestedBegin = static_cast<uint64_t>(start - 1);
        const size_t prefixCharacters = requestedBegin >= characters
            ? characters : static_cast<size_t>(requestedBegin);
        const __int128 suffixStart =
            static_cast<__int128>(start) + count;
        size_t suffixCharacter = 0;
        if (suffixStart > 1) {
            const __int128 zeroBased = suffixStart - 1;
            suffixCharacter = zeroBased >= static_cast<__int128>(characters)
                ? characters : static_cast<size_t>(zeroBased);
        }
        const size_t prefixByte = byteaInput
            ? prefixCharacters : utf8ByteAt(s, prefixCharacters);
        const size_t suffixByte = byteaInput
            ? suffixCharacter : utf8ByteAt(s, suffixCharacter);
        std::string out = s.substr(0, prefixByte) + repl + s.substr(suffixByte);
        if (byteaInput) {
            return ExprValue(
                "bytea", ByteaValue::fromBytes(std::move(out)).toString(),
                false);
        }
        return ExprValue("text", out, false);
    };
    // quote_literal — single-quote a value, doubling embedded quotes
    functions_["quote_literal"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        return ExprValue(
            "text", sqlQuoteLiteral(textArgumentValue(a[0])), false);
    };
    // quote_nullable — quote non-NULL values like quote_literal, but render a
    // SQL NULL as the non-NULL text "NULL" for safe dynamic SQL assembly.
    functions_["quote_nullable"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull)
            return ExprValue("text", "NULL", false);
        return ExprValue(
            "text", sqlQuoteLiteral(textArgumentValue(a[0])), false);
    };
    // quote_ident — double-quote an identifier when it is not a simple lower-case name
    functions_["quote_ident"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        return ExprValue(
            "text", sqlQuoteIdent(textArgumentValue(a[0])), false);
    };
    // format(fmtstr, args...) — %s (string), %I (identifier), %L (literal), %% (percent)
    // regexp_matches(text, pattern[, flags]) — PG set-returning form used
    // as a scalar: first match as a text array; capture groups when present.
    functions_["regexp_matches"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull ||
            (a.size() >= 3 && a[2].isNull))
            return ExprValue("text", "", true);
        const std::string options = a.size() >= 3
            ? textArgumentValue(a[2]) : "";
        validateRegexOptions(options, true, "regexp_matches");
        auto fl = std::regex::ECMAScript;
        for (char option : options) {
            if (option == 'i') fl |= std::regex::icase;
        }
        try {
            std::regex re(textArgumentValue(a[1]), fl);
            const std::string input = textArgumentValue(a[0]);
            std::smatch m;
            if (!std::regex_search(input, m, re))
                return ExprValue("text", "", true);
            std::string out = "{";
            if (m.size() > 1) {
                for (size_t k = 1; k < m.size(); ++k) {
                    if (k > 1) out += ",";
                    out += m[k].str();
                }
            } else {
                out += m[0].str();
            }
            out += "}";
            return ExprValue("text", out, false);
        } catch (const std::regex_error&) {
            throwInvalidRegularExpression();
        }
    };

    functions_["format"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string fmt = textArgumentValue(a[0]);
        size_t argi = 1;
        std::string out;
        for (size_t i = 0; i < fmt.size(); ++i) {
            if (fmt[i] != '%') {
                out.push_back(fmt[i]);
                continue;
            }
            if (i + 1 >= fmt.size()) {
                throw std::runtime_error(
                    "unterminated format() type specifier (SQLSTATE 22023)");
            }
            size_t specifierIndex = i + 1;
            size_t argumentIndex = 0;
            bool explicitPosition = false;
            if (std::isdigit(static_cast<unsigned char>(
                    fmt[specifierIndex]))) {
                size_t position = 0;
                const size_t positionStart = specifierIndex;
                while (specifierIndex < fmt.size() &&
                       std::isdigit(static_cast<unsigned char>(
                           fmt[specifierIndex]))) {
                    const size_t digit = static_cast<size_t>(
                        fmt[specifierIndex] - '0');
                    if (position >
                        (std::numeric_limits<size_t>::max() - digit) / 10) {
                        throw std::runtime_error(
                            "format() argument position is out of range "
                            "(SQLSTATE 22023)");
                    }
                    position = position * 10 + digit;
                    ++specifierIndex;
                }
                if (specifierIndex < fmt.size() &&
                    fmt[specifierIndex] == '$') {
                    if (position == 0) {
                        throw std::runtime_error(
                            "format() arguments are numbered from 1 "
                            "(SQLSTATE 22023)");
                    }
                    explicitPosition = true;
                    argumentIndex = position;
                    ++specifierIndex;
                } else {
                    // Digits without '$' are width syntax, handled by the
                    // width parser rather than as a position.
                    specifierIndex = positionStart;
                }
            }
            if (specifierIndex >= fmt.size()) {
                throw std::runtime_error(
                    "unterminated format() type specifier (SQLSTATE 22023)");
            }
            bool leftJustify = false;
            if (fmt[specifierIndex] == '-') {
                leftJustify = true;
                ++specifierIndex;
            }
            size_t width = 0;
            bool hasWidth = false;
            bool dynamicWidth = false;
            bool explicitWidthPosition = false;
            size_t widthArgumentIndex = 0;
            if (specifierIndex < fmt.size() &&
                fmt[specifierIndex] == '*') {
                hasWidth = true;
                dynamicWidth = true;
                ++specifierIndex;
                if (specifierIndex < fmt.size() &&
                    std::isdigit(static_cast<unsigned char>(
                        fmt[specifierIndex]))) {
                    size_t position = 0;
                    while (specifierIndex < fmt.size() &&
                           std::isdigit(static_cast<unsigned char>(
                               fmt[specifierIndex]))) {
                        const size_t digit = static_cast<size_t>(
                            fmt[specifierIndex] - '0');
                        if (position >
                            (std::numeric_limits<size_t>::max() - digit) /
                                10) {
                            throw std::runtime_error(
                                "format() argument position is out of range "
                                "(SQLSTATE 22023)");
                        }
                        position = position * 10 + digit;
                        ++specifierIndex;
                    }
                    if (specifierIndex >= fmt.size() ||
                        fmt[specifierIndex] != '$') {
                        throw std::runtime_error(
                            "invalid format() width specification "
                            "(SQLSTATE 22023)");
                    }
                    if (position == 0) {
                        throw std::runtime_error(
                            "format() arguments are numbered from 1 "
                            "(SQLSTATE 22023)");
                    }
                    explicitWidthPosition = true;
                    widthArgumentIndex = position;
                    ++specifierIndex;
                }
            } else {
                while (specifierIndex < fmt.size() &&
                       std::isdigit(static_cast<unsigned char>(
                           fmt[specifierIndex]))) {
                    hasWidth = true;
                    const size_t digit = static_cast<size_t>(
                        fmt[specifierIndex] - '0');
                    if (width >
                        (std::numeric_limits<size_t>::max() - digit) / 10) {
                        throw std::runtime_error(
                            "format() width is out of range (SQLSTATE 22023)");
                    }
                    width = width * 10 + digit;
                    ++specifierIndex;
                }
            }
            if (specifierIndex >= fmt.size()) {
                throw std::runtime_error(
                    "unterminated format() type specifier (SQLSTATE 22023)");
            }
            char spec = fmt[specifierIndex];
            if (spec == '%') {
                if (explicitPosition || leftJustify || hasWidth) {
                    throw std::runtime_error(
                        "unrecognized format() type specifier \"%\" "
                        "(SQLSTATE 22023)");
                }
                out.push_back('%');
                i = specifierIndex;
                continue;
            }
            if (spec == 's' || spec == 'I' || spec == 'L') {
                if (dynamicWidth) {
                    const size_t dynamicWidthIndex = explicitWidthPosition
                        ? widthArgumentIndex : argi;
                    if (dynamicWidthIndex >= a.size()) {
                        throw std::runtime_error(
                            "too few arguments for format() (SQLSTATE 22023)");
                    }
                    argi = dynamicWidthIndex + 1;
                    if (!a[dynamicWidthIndex].isNull) {
                        int64_t parsedWidth = 0;
                        const SignedIntegerParseResult parsed =
                            parseSignedInteger(
                                a[dynamicWidthIndex].value, parsedWidth);
                        if (parsed == SignedIntegerParseResult::OutOfRange ||
                            parsedWidth < std::numeric_limits<int32_t>::min() ||
                            parsedWidth > std::numeric_limits<int32_t>::max()) {
                            throw std::runtime_error(
                                "format() width is out of range "
                                "(SQLSTATE 22003)");
                        }
                        if (parsed != SignedIntegerParseResult::Ok) {
                            throw std::runtime_error(
                                "invalid input syntax for type integer: '" +
                                a[dynamicWidthIndex].value +
                                "' (SQLSTATE 22P02)");
                        }
                        if (parsedWidth < 0) {
                            leftJustify = true;
                            width = static_cast<size_t>(-parsedWidth);
                        } else {
                            width = static_cast<size_t>(parsedWidth);
                        }
                    }
                }
                if (!explicitPosition) argumentIndex = argi;
                if (argumentIndex >= a.size()) {
                    throw std::runtime_error(
                        "too few arguments for format() (SQLSTATE 22023)");
                }
                argi = argumentIndex + 1;
                const ExprValue& arg = a[argumentIndex];
                std::string rendered;
                if (spec == 's') rendered = arg.isNull ? "" : arg.value;
                else if (spec == 'I') {
                    if (arg.isNull) {
                        throw std::runtime_error(
                            "null values cannot be formatted as an SQL "
                            "identifier (SQLSTATE 22004)");
                    }
                    rendered = sqlQuoteIdent(arg.value);
                }
                else /* L */ rendered = arg.isNull
                    ? "NULL" : sqlQuoteLiteral(arg.value);
                const size_t renderedWidth = utf8CharCount(rendered);
                const size_t padding = hasWidth && width > renderedWidth
                    ? width - renderedWidth : 0;
                if (!leftJustify) out.append(padding, ' ');
                out += rendered;
                if (leftJustify) out.append(padding, ' ');
                i = specifierIndex;
            } else {
                throw std::runtime_error(
                    "unrecognized format() type specifier \"" +
                    std::string(1, spec) + "\" (SQLSTATE 22023)");
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
    // md5(text/bytea) — 32-char lowercase hex digest
    functions_["md5"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        const std::string input = isCanonicalByteaType(a[0].typeName)
            ? parseByteaOrThrow(a[0]).bytes() : textArgumentValue(a[0]);
        return ExprValue("text", md5Hex(input), false);
    };
    functions_["sha224"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bytea", "", true);
        return byteaDigestValue(
            SHA224::digestBytes(parseByteaOrThrow(a[0]).bytes()));
    };
    functions_["sha256"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bytea", "", true);
        return byteaDigestValue(
            SHA256::digestBytes(parseByteaOrThrow(a[0]).bytes()));
    };
    functions_["sha384"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bytea", "", true);
        return byteaDigestValue(
            SHA384::digestBytes(parseByteaOrThrow(a[0]).bytes()));
    };
    functions_["sha512"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bytea", "", true);
        return byteaDigestValue(
            SHA512::digestBytes(parseByteaOrThrow(a[0]).bytes()));
    };
    // encode(data, format) — format is 'hex', 'base64', or 'escape'
    functions_["encode"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        std::string fmt = toLower(textArgumentValue(a[1]));
        const std::string data = isCanonicalByteaType(a[0].typeName)
            ? parseByteaOrThrow(a[0]).bytes() : a[0].value;
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
        throw std::runtime_error(
            "unrecognized encoding: \"" + a[1].value +
            "\" (SQLSTATE 22023)");
    };
    // decode(text, format) — inverse of encode, returns canonical bytea.
    functions_["decode"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bytea", "", true);
        std::string fmt = toLower(textArgumentValue(a[1]));
        const std::string text = textArgumentValue(a[0]);
        std::string out;
        if (fmt == "hex") {
            if (!hexDecode(text, out))
                throw std::runtime_error(
                    "invalid hexadecimal data (SQLSTATE 22023)");
            return ExprValue(
                "bytea", ByteaValue::fromBytes(std::move(out)).toString(),
                false);
        }
        if (fmt == "base64") {
            if (!base64Decode(text, out))
                throw std::runtime_error(
                    "invalid base64 data (SQLSTATE 22023)");
            return ExprValue(
                "bytea", ByteaValue::fromBytes(std::move(out)).toString(),
                false);
        }
        if (fmt == "escape") {
            for (size_t i = 0; i < text.size(); ++i) {
                if (text[i] != '\\') {
                    out.push_back(text[i]);
                    continue;
                }
                if (i + 1 < text.size() && text[i + 1] == '\\') {
                    out.push_back('\\');
                    ++i;
                    continue;
                }
                if (i + 3 >= text.size() || text[i + 1] < '0' ||
                    text[i + 1] > '3' || text[i + 2] < '0' ||
                    text[i + 2] > '7' || text[i + 3] < '0' ||
                    text[i + 3] > '7') {
                    throw std::runtime_error(
                        "invalid input syntax for type bytea "
                        "(SQLSTATE 22P02)");
                }
                const int value = (text[i + 1] - '0') * 64 +
                                  (text[i + 2] - '0') * 8 +
                                  (text[i + 3] - '0');
                out.push_back(static_cast<char>(value));
                i += 3;
            }
            return ExprValue(
                "bytea", ByteaValue::fromBytes(std::move(out)).toString(),
                false);
        }
        throw std::runtime_error(
            "unrecognized encoding: \"" + a[1].value +
            "\" (SQLSTATE 22023)");
    };
    functions_["convert"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("bytea", "", true);
        const std::string converted = convertEncodingOrThrow(
            parseByteaOrThrow(a[0]).bytes(), parseEncodingOrThrow(a[1]),
            parseEncodingOrThrow(a[2]));
        return ExprValue(
            "bytea", ByteaValue::fromBytes(converted).toString(), false);
    };
    functions_["convert_from"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("text", "", true);
        return ExprValue(
            "text",
            convertEncodingOrThrow(
                parseByteaOrThrow(a[0]).bytes(),
                parseEncodingOrThrow(a[1]), BuiltinEncoding::Utf8),
            false);
    };
    functions_["convert_to"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("bytea", "", true);
        const std::string converted = convertEncodingOrThrow(
            textArgumentValue(a[0]), BuiltinEncoding::Utf8,
            parseEncodingOrThrow(a[1]));
        return ExprValue(
            "bytea", ByteaValue::fromBytes(converted).toString(), false);
    };
    functions_["get_byte"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("integer", "", true);
        const std::string bytes = parseByteaOrThrow(a[0]).bytes();
        const int64_t offset = parseInt32Argument(a[1]);
        if (offset < 0 || static_cast<uint64_t>(offset) >= bytes.size()) {
            throw std::runtime_error(
                "index out of valid range (SQLSTATE 2202E)");
        }
        return ExprValue(
            "integer",
            std::to_string(static_cast<unsigned char>(
                bytes[static_cast<size_t>(offset)])),
            false);
    };
    functions_["set_byte"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("bytea", "", true);
        std::string bytes = parseByteaOrThrow(a[0]).bytes();
        const int64_t offset = parseInt32Argument(a[1]);
        const int64_t replacement = parseInt32Argument(a[2]);
        if (offset < 0 || static_cast<uint64_t>(offset) >= bytes.size()) {
            throw std::runtime_error(
                "index out of valid range (SQLSTATE 2202E)");
        }
        if (replacement < 0 || replacement > 255) {
            throw std::runtime_error(
                "new byte must be between 0 and 255 "
                "(SQLSTATE 22023)");
        }
        bytes[static_cast<size_t>(offset)] =
            static_cast<char>(replacement);
        return ExprValue(
            "bytea", ByteaValue::fromBytes(std::move(bytes)).toString(),
            false);
    };
    functions_["get_bit"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("integer", "", true);
        const int64_t offset = parseInt32Argument(a[1]);
        if (isBitStringTypeName(a[0].typeName)) {
            if (offset < 0 ||
                static_cast<uint64_t>(offset) >= a[0].value.size()) {
                throw std::runtime_error(
                    "index out of valid range (SQLSTATE 2202E)");
            }
            return ExprValue(
                "integer",
                a[0].value[static_cast<size_t>(offset)] == '1' ? "1" : "0",
                false);
        }
        const std::string bytes = parseByteaOrThrow(a[0]).bytes();
        if (offset < 0 || static_cast<uint64_t>(offset) >= bytes.size() * 8U) {
            throw std::runtime_error(
                "index out of valid range (SQLSTATE 2202E)");
        }
        const unsigned char byte = static_cast<unsigned char>(
            bytes[static_cast<size_t>(offset) / 8U]);
        const unsigned bit = static_cast<unsigned>(offset) & 7U;
        return ExprValue(
            "integer", ((byte >> bit) & 1U) != 0 ? "1" : "0", false);
    };
    functions_["set_bit"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue(
                !a.empty() && isBitStringTypeName(a[0].typeName)
                    ? a[0].typeName : "bytea", "", true);
        const int64_t offset = parseInt32Argument(a[1]);
        const int64_t replacement = parseInt32Argument(a[2]);
        if (isBitStringTypeName(a[0].typeName)) {
            if (offset < 0 ||
                static_cast<uint64_t>(offset) >= a[0].value.size()) {
                throw std::runtime_error(
                    "index out of valid range (SQLSTATE 2202E)");
            }
            if (replacement != 0 && replacement != 1) {
                throw std::runtime_error(
                    "new bit must be 0 or 1 (SQLSTATE 22023)");
            }
            std::string bits = a[0].value;
            bits[static_cast<size_t>(offset)] =
                replacement == 0 ? '0' : '1';
            return ExprValue(a[0].typeName, std::move(bits), false);
        }
        std::string bytes = parseByteaOrThrow(a[0]).bytes();
        if (offset < 0 || static_cast<uint64_t>(offset) >= bytes.size() * 8U) {
            throw std::runtime_error(
                "index out of valid range (SQLSTATE 2202E)");
        }
        if (replacement != 0 && replacement != 1) {
            throw std::runtime_error(
                "new bit must be 0 or 1 (SQLSTATE 22023)");
        }
        unsigned char byte = static_cast<unsigned char>(
            bytes[static_cast<size_t>(offset) / 8U]);
        const unsigned char mask = static_cast<unsigned char>(
            1U << (static_cast<unsigned>(offset) & 7U));
        byte = replacement == 0 ? static_cast<unsigned char>(byte & ~mask)
                                : static_cast<unsigned char>(byte | mask);
        bytes[static_cast<size_t>(offset) / 8U] = static_cast<char>(byte);
        return ExprValue(
            "bytea", ByteaValue::fromBytes(std::move(bytes)).toString(),
            false);
    };
    functions_["bit_count"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bigint", "", true);
        if (isBitStringTypeName(a[0].typeName)) {
            return ExprValue(
                "bigint",
                std::to_string(std::count(
                    a[0].value.begin(), a[0].value.end(), '1')),
                false);
        }
        const std::string bytes = parseByteaOrThrow(a[0]).bytes();
        uint64_t count = 0;
        for (unsigned char byte : bytes) {
            while (byte != 0) {
                byte &= static_cast<unsigned char>(byte - 1);
                ++count;
            }
        }
        return ExprValue("bigint", std::to_string(count), false);
    };
    functions_["crc32"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bigint", "", true);
        return ExprValue(
            "bigint",
            std::to_string(byteaCrc(
                parseByteaOrThrow(a[0]).bytes(), 0xedb88320U)),
            false);
    };
    functions_["crc32c"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("bigint", "", true);
        return ExprValue(
            "bigint",
            std::to_string(byteaCrc(
                parseByteaOrThrow(a[0]).bytes(), 0x82f63b78U)),
            false);
    };

    // ------------------------------------------------------------------------
    // Array functions (operate on the '{...}' array literal text)
    // ------------------------------------------------------------------------
    auto parseArrayDimension = [](const std::vector<ExprValue>& arguments,
                                  long long& dimension) {
        if (arguments.size() < 2 || arguments[1].isNull) return false;
        dimension = parseInt32Argument(arguments[1]);
        return true;
    };
    auto arrayExtent = [](const std::string& array,
                          long long dimension) -> std::optional<size_t> {
        if (dimension <= 0) return std::nullopt;
        std::string current = array;
        while (dimension-- > 0) {
            std::vector<std::string> elements;
            if (!parseArrayElements(current, elements) || elements.empty())
                return std::nullopt;
            if (dimension == 0) return elements.size();
            current = elements.front();
        }
        return std::nullopt;
    };
    // array_length(arr, dim) — element count along the requested dimension
    functions_["array_length"] =
        [parseArrayDimension, arrayExtent](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        long long dim = 0;
        if (!parseArrayDimension(a, dim))
            return ExprValue("integer", "", true);
        const auto extent = arrayExtent(a[0].value, dim);
        return extent
            ? ExprValue("integer", std::to_string(*extent), false)
            : ExprValue("integer", "", true);
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
    // array_ndims(arr) — number of non-empty nested array dimensions
    functions_["array_ndims"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        std::string current = a[0].value;
        int n = 0;
        while (true) {
            std::vector<std::string> elements;
            if (!parseArrayElements(current, elements) || elements.empty())
                break;
            ++n;
            current = elements.front();
        }
        if (n == 0) return ExprValue("integer", "", true);
        return ExprValue("integer", std::to_string(n), false);
    };
    // Array bounds are part of the value, not inferred from element count.
    functions_["array_lower"] =
        [parseArrayDimension](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        long long dim = 0;
        if (!parseArrayDimension(a, dim))
            return ExprValue("integer", "", true);
        const auto literal=sql_array_text::parse(a[0].value);
        return dim>0 && static_cast<uint64_t>(dim)<=literal.dimensions.size()
            ? ExprValue("integer",std::to_string(literal.dimensions[static_cast<size_t>(dim-1)].lower),false)
            : ExprValue("integer","",true);
    };
    // array_upper(arr, dim) — upper bound == length for the default lower bound 1
    functions_["array_upper"] =
        [parseArrayDimension](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        long long dim = 0;
        if (!parseArrayDimension(a, dim))
            return ExprValue("integer", "", true);
        const auto literal=sql_array_text::parse(a[0].value);
        if(dim<=0 || static_cast<uint64_t>(dim)>literal.dimensions.size())return ExprValue("integer","",true);
        const auto& metadata=literal.dimensions[static_cast<size_t>(dim-1)];
        return ExprValue("integer",std::to_string(int64_t(metadata.lower)+metadata.length-1),false);
    };
    // array_append(arr, elem) — append element, returning the new array literal
    functions_["array_append"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2) return ExprValue("ARRAY", "", true);
        std::vector<std::string> elems;
        if (!a[0].isNull && !parseArrayElements(a[0].value, elems))
            return ExprValue("ARRAY", "", true);
        auto dimensions=a[0].isNull?std::vector<sql_array_text::Dimension>{}:sql_array_text::parse(a[0].value).dimensions;
        if(dimensions.size()>1)throw DbError("22000","argument must be empty or one-dimensional array");
        if(dimensions.empty())dimensions.push_back({1,0});
        elems.push_back(a[1].isNull ? "NULL" : arrayElemQuote(a[1].value));
        std::string out = "{";
        for (size_t i = 0; i < elems.size(); ++i) { if (i) out += ","; out += elems[i]; }
        out += "}";
        dimensions.front().length=static_cast<int32_t>(elems.size());
        return ExprValue("ARRAY",sql_array_text::prefix(dimensions)+out,false);
    };
    // array_prepend(elem, arr) — prepend element
    functions_["array_prepend"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2) return ExprValue("ARRAY", "", true);
        std::vector<std::string> elems;
        if (!a[1].isNull && !parseArrayElements(a[1].value, elems))
            return ExprValue("ARRAY", "", true);
        auto dimensions=a[1].isNull?std::vector<sql_array_text::Dimension>{}:sql_array_text::parse(a[1].value).dimensions;
        if(dimensions.size()>1)throw DbError("22000","argument must be empty or one-dimensional array");
        if(dimensions.empty())dimensions.push_back({1,0});
        elems.insert(elems.begin(), a[0].isNull ? "NULL" : arrayElemQuote(a[0].value));
        std::string out = "{";
        for (size_t i = 0; i < elems.size(); ++i) { if (i) out += ","; out += elems[i]; }
        out += "}";
        dimensions.front().length=static_cast<int32_t>(elems.size());
        return ExprValue("ARRAY",sql_array_text::prefix(dimensions)+out,false);
    };
    // array_cat(a, b) — concatenate two arrays
    functions_["array_cat"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2) return ExprValue("ARRAY", "", true);
        if(a[0].isNull && a[1].isNull)return ExprValue("ARRAY","",true);
        if (a[0].isNull && !a[1].isNull) return ExprValue("ARRAY", a[1].value, false);
        if (a[1].isNull && !a[0].isNull) return ExprValue("ARRAY", a[0].value, false);
        return ExprValue("ARRAY",sql_array_text::concatenate(a[0].value,a[1].value),false);
    };
    // array_position(arr, elem) — 1-based index of first matching element, NULL if absent
    // array_dims(arr) — dimensions as PG text, e.g. [1:3].
    functions_["array_dims"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string dimensions;
        const auto literal=sql_array_text::parse(a[0].value);
        for(const auto& metadata:literal.dimensions)dimensions+='['+std::to_string(metadata.lower)+':'+std::to_string(int64_t(metadata.lower)+metadata.length-1)+']';
        if (dimensions.empty()) return ExprValue("text", "", true);
        return ExprValue("text", dimensions, false);
    };

    functions_["array_position"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull) return ExprValue("integer", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems)) return ExprValue("integer", "", true);
        const auto literal=sql_array_text::parse(a[0].value);
        const int64_t lower=literal.dimensions.empty()?1:literal.dimensions.front().lower;
        for (const auto& elem : elems) {
            std::vector<std::string> nested;
            if (parseArrayElements(elem, nested)) {
                throw std::runtime_error(
                    "searching for elements in multidimensional arrays is "
                    "not supported (SQLSTATE 0A000)");
            }
        }
        long long initialPosition = lower;
        if (a.size() >= 3) {
            if (a[2].isNull) {
                throw std::runtime_error(
                    "initial position must not be null (SQLSTATE 22004)");
            }
            initialPosition = parseInt32Argument(a[2]);
        }
        size_t first = 0;
        if (initialPosition > lower) {
            if (static_cast<unsigned long long>(initialPosition-lower) >= elems.size())
                return ExprValue("integer", "", true);
            first = static_cast<size_t>(initialPosition - lower);
        }
        for (size_t i = first; i < elems.size(); ++i) {
            const std::string token = trimStr(elems[i]);
            const bool quoted = token.size() >= 2 &&
                token.front() == '"' && token.back() == '"';
            const bool elementIsNull =
                !quoted && toLower(token) == "null";
            const std::string value = arrayElemUnquote(token);
            if ((a[1].isNull && elementIsNull) ||
                (!a[1].isNull && !elementIsNull &&
                 value == a[1].value)) {
                return ExprValue("integer", std::to_string(static_cast<int64_t>(i)+lower), false);
            }
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
        std::function<void(const std::vector<std::string>&)> appendElements =
            [&](const std::vector<std::string>& values) {
                for (const auto& raw : values) {
                    std::vector<std::string> nested;
                    if (parseArrayElements(raw, nested)) {
                        appendElements(nested);
                        continue;
                    }
                    const std::string token = trimStr(raw);
                    const bool quoted = token.size() >= 2 &&
                        token.front() == '"' && token.back() == '"';
                    const bool isNull = !quoted && toLower(token) == "null";
                    if (isNull && !hasNullStr) continue;
                    if (!first) out += delim;
                    out += isNull ? nullStr : arrayElemUnquote(token);
                    first = false;
                }
            };
        appendElements(elems);
        return ExprValue("text", out, false);
    };
    // string_to_array(str, delim [, null_string]) — split into an array literal
    functions_["string_to_array"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull) return ExprValue("ARRAY", "", true);
        const std::string& s = a[0].value;
        bool hasNullStr = (a.size() >= 3 && !a[2].isNull);
        std::string nullStr = hasNullStr ? a[2].value : "";
        std::vector<std::string> parts;
        if (s.empty()) {
            // PostgreSQL represents an empty input as an empty array, not an
            // array containing one empty string.
        } else if (a[1].isNull) {
            // NULL delimiter: split into individual characters, preserving
            // complete UTF-8 code points.
            const size_t characters = utf8CharCount(s);
            for (size_t i = 0; i < characters; ++i) {
                const size_t begin = utf8ByteAt(s, i);
                const size_t end = utf8ByteAt(s, i + 1);
                parts.push_back(s.substr(begin, end - begin));
            }
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
        if (!jsonTopLevelSplit(a[0].value, '[', ']', elems)) {
            const std::string kind = jsonTypeOf(a[0].value);
            throw std::runtime_error(
                kind == "object"
                    ? "cannot get array length of a non-array "
                      "(SQLSTATE 22023)"
                    : "cannot get array length of a scalar "
                      "(SQLSTATE 22023)");
        }
        return ExprValue("integer", std::to_string(elems.size()), false);
    };
    functions_["json_array_length"] = jsonArrayLenFn;
    functions_["jsonb_array_length"] = jsonArrayLenFn;

    // json_build_array(VARIADIC) -> compact JSON array
    auto jsonBuildArrayFn = [](const std::vector<ExprValue>& a,
                               const std::string& resultType) {
        std::string out = "[";
        for (size_t i = 0; i < a.size(); ++i) {
            if (i) out += ",";
            out += toJsonValue(a[i]);
        }
        out += "]";
        return ExprValue(resultType, out, false);
    };
    functions_["json_build_array"] = [jsonBuildArrayFn](
        const std::vector<ExprValue>& a) {
        return jsonBuildArrayFn(a, "json");
    };
    functions_["jsonb_build_array"] = [jsonBuildArrayFn](
        const std::vector<ExprValue>& a) {
        return jsonBuildArrayFn(a, "jsonb");
    };

    // json_build_object(k1, v1, ...) -> compact JSON object (keys coerced to text)
    auto jsonBuildObjectFn = [](const std::vector<ExprValue>& a,
                                bool binary) {
        if (a.size() % 2 != 0) {
            throw std::runtime_error(
                "argument list must have even number of elements "
                "(SQLSTATE 22023)");
        }
        std::string out = "{";
        bool first = true;
        for (size_t i = 0; i + 1 < a.size(); i += 2) {
            if (a[i].isNull) {
                throw std::runtime_error(
                    binary
                        ? "key must not be null (SQLSTATE 22023)"
                        : "null value not allowed for object key "
                          "(SQLSTATE 22004)");
            }
            if (!first) out += ",";
            out += jsonQuoteStr(a[i].value);
            out += ":";
            out += toJsonValue(a[i + 1]);
            first = false;
        }
        out += "}";
        return ExprValue(binary ? "jsonb" : "json", out, false);
    };
    functions_["json_build_object"] = [jsonBuildObjectFn](
        const std::vector<ExprValue>& a) {
        return jsonBuildObjectFn(a, false);
    };
    functions_["jsonb_build_object"] = [jsonBuildObjectFn](
        const std::vector<ExprValue>& a) {
        return jsonBuildObjectFn(a, true);
    };

    // to_json / to_jsonb -> JSON representation of the argument
    auto toJsonFn = [](const std::vector<ExprValue>& a,
                       const std::string& resultType) {
        if (a.empty()) return ExprValue(resultType, "null", false);
        return ExprValue(resultType, toJsonValue(a[0]), false);
    };
    functions_["to_json"] = [toJsonFn](const std::vector<ExprValue>& a) {
        return toJsonFn(a, "json");
    };
    functions_["to_jsonb"] = [toJsonFn](const std::vector<ExprValue>& a) {
        return toJsonFn(a, "jsonb");
    };

    // json_extract_path(json, key, ...) -> the JSON sub-value at the key/index
    // path, or NULL if any step does not resolve.
    auto jsonExtractFn = [](const std::vector<ExprValue>& a,
                            const std::string& resultType) {
        if (a.empty() || a[0].isNull) return ExprValue(resultType, "", true);
        std::string cur = a[0].value;
        for (size_t i = 1; i < a.size(); ++i) {
            if (a[i].isNull) return ExprValue(resultType, "", true);
            std::string next;
            if (!jsonStep(cur, a[i].value, next))
                return ExprValue(resultType, "", true);
            cur = next;
        }
        return ExprValue(resultType, cur, false);
    };
    functions_["json_extract_path"] = [jsonExtractFn](
        const std::vector<ExprValue>& a) {
        return jsonExtractFn(a, "json");
    };
    functions_["jsonb_extract_path"] = [jsonExtractFn](
        const std::vector<ExprValue>& a) {
        return jsonExtractFn(a, "jsonb");
    };

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
            if (!jsonUnquoteString(t, o))
                return ExprValue("text", "", true);
            return ExprValue("text", o, false);
        }
        return ExprValue("text", t, false);
    };
    functions_["json_extract_path_text"] = jsonExtractTextFn;
    functions_["jsonb_extract_path_text"] = jsonExtractTextFn;

    // Operator forms: json -> key / json -> idx (rewritten from the
    // arrow syntax in expr_helper) share the path machinery.
    functions_["json_get"] = [jsonExtractFn](
        const std::vector<ExprValue>& a) {
        const bool binary = !a.empty() && toLower(a[0].typeName) == "jsonb";
        return jsonExtractFn(a, binary ? "jsonb" : "json");
    };
    functions_["json_get_text"] = jsonExtractTextFn;

    // ------------------------------------------------------------------------
    // Regular expression functions (std::regex, ECMAScript dialect)
    // ------------------------------------------------------------------------
    // regexp_replace(source, pattern, replacement [, flags]) — 'g' = replace all
    functions_["regexp_replace"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull ||
            (a.size() >= 4 && a[3].isNull))
            return ExprValue("text", "", true);
        std::string flags = (a.size() >= 4 && !a[3].isNull)
            ? textArgumentValue(a[3]) : "";
        validateRegexOptions(flags, true, "regexp_replace");
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) throwInvalidRegularExpression();
        std::string repl = translateReplacement(textArgumentValue(a[2]));
        const std::string input = textArgumentValue(a[0]);
        auto fmtFlags = (flags.find('g') != std::string::npos)
                            ? std::regex_constants::format_default
                            : std::regex_constants::format_first_only;
        try {
            return ExprValue(
                "text", std::regex_replace(input, re, repl, fmtFlags), false);
        } catch (...) {
            return ExprValue("text", "", true);
        }
    };
    // regexp_match(string, pattern [, flags]) — capture groups of first match as
    // a text array; whole match if the pattern has no groups; NULL if no match
    functions_["regexp_match"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull ||
            (a.size() >= 3 && a[2].isNull))
            return ExprValue("ARRAY", "", true);
        std::string flags = (a.size() >= 3 && !a[2].isNull)
            ? textArgumentValue(a[2]) : "";
        validateRegexOptions(flags, false, "regexp_match");
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) throwInvalidRegularExpression();
        const std::string input = textArgumentValue(a[0]);
        std::smatch m;
        if (!std::regex_search(input, m, re))
            return ExprValue("ARRAY", "", true);  // NULL
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull ||
            (a.size() >= 3 && a[2].isNull))
            return ExprValue("ARRAY", "", true);
        std::string flags = (a.size() >= 3 && !a[2].isNull)
            ? textArgumentValue(a[2]) : "";
        validateRegexOptions(flags, false, "regexp_split_to_array");
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) throwInvalidRegularExpression();
        const std::string s = textArgumentValue(a[0]);
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull ||
            (a.size() >= 3 && a[2].isNull) ||
            (a.size() >= 4 && a[3].isNull))
            return ExprValue("integer", "", true);
        std::string flags = (a.size() >= 4 && !a[3].isNull)
            ? textArgumentValue(a[3]) : "";
        validateRegexOptions(flags, false, "regexp_count");
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) throwInvalidRegularExpression();
        const std::string s = textArgumentValue(a[0]);
        size_t start = 0;
        if (a.size() >= 3 && !a[2].isNull) {
            const int64_t st = parsePositiveRegexParameter(a[2], "start");
            start = utf8StartByte(s, st);
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull ||
            (a.size() >= 3 && a[2].isNull) ||
            (a.size() >= 4 && a[3].isNull) ||
            (a.size() >= 5 && a[4].isNull))
            return ExprValue("text", "", true);
        std::string flags = (a.size() >= 5 && !a[4].isNull)
            ? textArgumentValue(a[4]) : "";
        validateRegexOptions(flags, false, "regexp_substr");
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) throwInvalidRegularExpression();
        const std::string s = textArgumentValue(a[0]);
        size_t start = 0;
        if (a.size() >= 3 && !a[2].isNull) {
            const int64_t st = parsePositiveRegexParameter(a[2], "start");
            start = utf8StartByte(s, st);
        }
        const int64_t which = (a.size() >= 4 && !a[3].isNull)
            ? parsePositiveRegexParameter(a[3], "n") : 1;
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
    functions_["current_date"] = [stableDate](const std::vector<ExprValue>&) {
        return ExprValue("date", stableDate, stableDate.empty());
    };
    // Stable clock functions share the evaluator's creation-time snapshot.
    // The engine currently has a fixed UTC session timezone.
    functions_["current_timestamp"] = [stableTimestamp](const std::vector<ExprValue>&) {
        return ExprValue("timestamptz", stableTimestamp + "+00",
                         stableTimestamp.empty());
    };
    functions_["localtimestamp"] = [stableTimestamp](const std::vector<ExprValue>&) {
        return ExprValue(
            "timestamp", stableTimestamp, stableTimestamp.empty());
    };
    functions_["transaction_timestamp"] = [stableTimestamp](const std::vector<ExprValue>&) {
        return ExprValue("timestamptz", stableTimestamp + "+00",
                         stableTimestamp.empty());
    };
    functions_["statement_timestamp"] = [stableTimestamp](const std::vector<ExprValue>&) {
        return ExprValue("timestamptz", stableTimestamp + "+00",
                         stableTimestamp.empty());
    };
    functions_["clock_timestamp"] = [](const std::vector<ExprValue>&) {
        const std::string timestamp = formatUtcClock(
            std::time(nullptr), "%Y-%m-%d %H:%M:%S");
        return ExprValue(
            "timestamptz", timestamp + "+00", timestamp.empty());
    };
    functions_["current_time"] = [stableTime](const std::vector<ExprValue>&) {
        return ExprValue("time with time zone", stableTime + "+00",
                         stableTime.empty());
    };
    functions_["localtime"] = [stableTime](const std::vector<ExprValue>&) {
        return ExprValue("time", stableTime, stableTime.empty());
    };

    // Shared field extractor for extract() / date_part(); src is an ISO date or
    // timestamp 'YYYY-MM-DD[ HH:MM:SS]'.
    auto extractImpl = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("numeric", "", true);
        std::string field = toLower(a[0].value);
        const std::string& src = a[1].value;
        const bool declaredInterval =
            toLower(a[1].typeName).find("interval") != std::string::npos;
        if (declaredInterval) {
            const IntervalParts interval = parseIntervalText(src);
            if (!interval.ok)
                return ExprValue("numeric", "", true);
            if (field == "epoch") {
                const __int128 micros =
                    static_cast<__int128>(interval.months) * 30 *
                        86400000000LL +
                    static_cast<__int128>(interval.days) * 86400000000LL +
                    interval.micros;
                return ExprValue("numeric", formatMicrosNumeric(micros),
                                 false);
            }
            if (field == "year")
                return ExprValue("numeric",
                                 std::to_string(interval.months / 12), false);
            if (field == "month")
                return ExprValue("numeric",
                                 std::to_string(interval.months % 12), false);
            if (field == "quarter")
                return ExprValue(
                    "numeric",
                    std::to_string((interval.months % 12) / 3 + 1), false);
            if (field == "decade")
                return ExprValue(
                    "numeric", std::to_string(interval.months / 120), false);
            if (field == "century")
                return ExprValue(
                    "numeric", std::to_string(interval.months / 1200), false);
            if (field == "millennium")
                return ExprValue(
                    "numeric", std::to_string(interval.months / 12000), false);
            if (field == "day")
                return ExprValue("numeric", std::to_string(interval.days),
                                 false);
            if (field == "hour")
                return ExprValue(
                    "numeric",
                    std::to_string(interval.micros / 3600000000LL), false);
            if (field == "minute")
                return ExprValue(
                    "numeric",
                    std::to_string(
                        (interval.micros / 60000000LL) % 60), false);
            if (field == "second") {
                const long long secondMicros =
                    interval.micros % 60000000LL;
                if (secondMicros % 1000000LL == 0) {
                    return ExprValue(
                        "numeric",
                        std::to_string(secondMicros / 1000000LL), false);
                }
                return ExprValue(
                    "numeric", formatMicrosNumeric(secondMicros), false);
            }
            if (field == "milliseconds") {
                const long long secondMicros =
                    interval.micros % 60000000LL;
                const bool negative = secondMicros < 0;
                const unsigned long long magnitude = negative
                    ? static_cast<unsigned long long>(-secondMicros)
                    : static_cast<unsigned long long>(secondMicros);
                std::string fraction = std::to_string(magnitude % 1000);
                fraction.insert(fraction.begin(), 3 - fraction.size(), '0');
                return ExprValue(
                    "numeric",
                    (negative ? "-" : "") +
                        std::to_string(magnitude / 1000) + "." + fraction,
                    false);
            }
            if (field == "microseconds") {
                return ExprValue(
                    "numeric",
                    std::to_string(interval.micros % 60000000LL), false);
            }
            static const std::set<std::string> unsupportedIntervalUnits = {
                "dow", "isodow", "doy", "week", "isoyear", "julian",
                "timezone", "timezone_hour", "timezone_minute"
            };
            if (unsupportedIntervalUnits.count(field)) {
                throw std::runtime_error(
                    "unit \"" + field +
                    "\" not supported for type interval (SQLSTATE 0A000)");
            }
            throw std::runtime_error(
                "unit \"" + field +
                "\" not recognized for type interval (SQLSTATE 22023)");
        }

        static const std::set<std::string> temporalUnits = {
            "year", "month", "day", "hour", "minute", "second",
            "milliseconds", "microseconds", "quarter", "decade",
            "century", "millennium", "dow", "isodow", "doy", "week",
            "isoyear", "julian", "epoch", "timezone", "timezone_hour",
            "timezone_minute"
        };
        if (!temporalUnits.count(field)) {
            throw std::runtime_error(
                "unit \"" + field +
                "\" not recognized for temporal type (SQLSTATE 22023)");
        }
        const std::string sourceType = toLower(a[1].typeName);
        if (sourceType == "date") {
            static const std::set<std::string> unsupportedDateUnits = {
                "hour", "minute", "second", "milliseconds",
                "microseconds", "timezone", "timezone_hour",
                "timezone_minute"
            };
            if (unsupportedDateUnits.count(field)) {
                throw std::runtime_error(
                    "unit \"" + field +
                    "\" not supported for type date (SQLSTATE 0A000)");
            }
        }
        const bool timeInput = sourceType == "time" ||
            sourceType == "time without time zone" ||
            sourceType == "timetz" ||
            sourceType == "time with time zone";
        if (timeInput) {
            const bool zonedTime = sourceType == "timetz" ||
                sourceType == "time with time zone";
            static const std::set<std::string> supportedTimeUnits = {
                "hour", "minute", "second", "milliseconds",
                "microseconds", "epoch", "timezone", "timezone_hour",
                "timezone_minute"
            };
            if (!supportedTimeUnits.count(field) ||
                (!zonedTime && (field == "timezone" ||
                                field == "timezone_hour" ||
                                field == "timezone_minute"))) {
                throw std::runtime_error(
                    "unit \"" + field + "\" not supported for type " +
                    (zonedTime ? "time with time zone" :
                                 "time without time zone") +
                    " (SQLSTATE 0A000)");
            }
            const auto parsedTime = parseTimeForExtract(a[1].value);
            if (!parsedTime)
                return ExprValue("numeric", "", true);
            if (field == "hour") {
                return ExprValue(
                    "numeric", std::to_string(parsedTime->hour), false);
            }
            if (field == "minute") {
                return ExprValue(
                    "numeric", std::to_string(parsedTime->minute), false);
            }
            if (field == "second") {
                if (parsedTime->secondMicros % 1000000LL == 0) {
                    return ExprValue(
                        "numeric",
                        std::to_string(
                            parsedTime->secondMicros / 1000000LL),
                        false);
                }
                return ExprValue(
                    "numeric",
                    formatMicrosNumeric(parsedTime->secondMicros), false);
            }
            if (field == "milliseconds") {
                const uint64_t micros =
                    static_cast<uint64_t>(parsedTime->secondMicros);
                std::string fraction = std::to_string(micros % 1000);
                fraction.insert(fraction.begin(), 3 - fraction.size(), '0');
                return ExprValue(
                    "numeric", std::to_string(micros / 1000) + "." +
                        fraction, false);
            }
            if (field == "microseconds") {
                return ExprValue(
                    "numeric", std::to_string(parsedTime->secondMicros),
                    false);
            }
            if (field == "timezone") {
                return ExprValue(
                    "numeric",
                    std::to_string(parsedTime->offsetMinutes * 60), false);
            }
            if (field == "timezone_hour") {
                return ExprValue(
                    "numeric",
                    std::to_string(parsedTime->offsetMinutes / 60), false);
            }
            if (field == "timezone_minute") {
                return ExprValue(
                    "numeric",
                    std::to_string(parsedTime->offsetMinutes % 60), false);
            }
            const __int128 localMicros =
                (static_cast<__int128>(parsedTime->hour) * 3600 +
                 parsedTime->minute * 60) * 1000000 +
                parsedTime->secondMicros;
            const __int128 epochMicros = localMicros -
                (zonedTime
                     ? static_cast<__int128>(parsedTime->offsetMinutes) *
                           60000000
                     : 0);
            return ExprValue(
                "numeric", formatMicrosNumeric(epochMicros), false);
        }
        if (field == "timezone" || field == "timezone_hour" ||
            field == "timezone_minute") {
            const bool withTimeZone = sourceType == "timestamptz" ||
                sourceType == "timestamp with time zone";
            if (!withTimeZone) {
                throw std::runtime_error(
                    "unit \"" + field +
                    "\" not supported for timestamp without time zone "
                    "(SQLSTATE 0A000)");
            }
            // This evaluator's session/time-zone model is UTC-only.
            return ExprValue("numeric", "0", false);
        }

        const bool withTimeZone = sourceType == "timestamptz" ||
            sourceType == "timestamp with time zone";
        const auto parsedTimestamp =
            parseComparableTimestamp(src, withTimeZone);
        if (!parsedTimestamp)
            return ExprValue("numeric", "", true);
        if (parsedTimestamp->infinity != 0) {
            const bool monotonicField =
                field == "epoch" || field == "year" ||
                field == "decade" || field == "century" ||
                field == "millennium";
            if (!monotonicField)
                return ExprValue("numeric", "", true);
            return ExprValue(
                "numeric",
                parsedTimestamp->infinity > 0 ? "Infinity" : "-Infinity",
                false);
        }
        const int64_t secondMicros =
            parsedTimestamp->micros % 60000000LL;
        const std::string calendarSource = formatTimestampSeconds(
            parsedTimestamp->micros / 1000000LL);
        if (calendarSource.empty())
            return ExprValue("numeric", "", true);
        auto num = [&](size_t off, size_t len) -> int {
            if (calendarSource.size() < off + len) return 0;
            int v = 0;
            for (size_t i = off; i < off + len; ++i) {
                char c = calendarSource[i];
                if (c < '0' || c > '9') return 0;
                v = v * 10 + (c - '0');
            }
            return v;
        };
        int y = num(0, 4), mo = num(5, 2), d = num(8, 2);
        int h = num(11, 2), mi = num(14, 2);
        auto sundayBasedWeekday = [](int year, int month, int day) {
            static const int offsets[] = {
                0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4
            };
            const int adjustedYear = year - (month < 3 ? 1 : 0);
            return (adjustedYear + adjustedYear / 4 -
                    adjustedYear / 100 + adjustedYear / 400 +
                    offsets[month - 1] + day) % 7;
        };
        auto isoWeeksInYear = [&](int year) {
            const int januaryFirst = sundayBasedWeekday(year, 1, 1);
            const int isoJanuaryFirst = januaryFirst == 0
                ? 7 : januaryFirst;
            const bool leap =
                (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            return isoJanuaryFirst == 4 ||
                   (isoJanuaryFirst == 3 && leap) ? 53 : 52;
        };
        int64_t r = 0;
        if (field == "year") r = y;
        else if (field == "month") r = mo;
        else if (field == "day") r = d;
        else if (field == "hour") r = h;
        else if (field == "minute") r = mi;
        else if (field == "second") {
            if (secondMicros % 1000000LL == 0) {
                r = secondMicros / 1000000LL;
            } else {
                return ExprValue(
                    "numeric", formatMicrosNumeric(secondMicros), false);
            }
        }
        else if (field == "milliseconds") {
            std::string fraction = std::to_string(secondMicros % 1000);
            fraction.insert(fraction.begin(), 3 - fraction.size(), '0');
            return ExprValue(
                "numeric", std::to_string(secondMicros / 1000) + "." +
                    fraction, false);
        }
        else if (field == "microseconds") r = secondMicros;
        else if (field == "quarter") r = mo > 0 ? (mo - 1) / 3 + 1 : 0;
        else if (field == "decade") r = y / 10;
        else if (field == "century") r = y > 0 ? (y - 1) / 100 + 1 : 0;
        else if (field == "millennium") r = y > 0 ? (y - 1) / 1000 + 1 : 0;
        else if (field == "week" || field == "isoyear") {
            const Date current(y, mo, d);
            const Date firstDay(y, 1, 1);
            if (current.year == 0 || firstDay.year == 0)
                return ExprValue("numeric", "", true);
            const int dayOfYear = static_cast<int>(
                current.convert() - firstDay.convert() + 1);
            const int weekday = sundayBasedWeekday(y, mo, d);
            const int isoWeekday = weekday == 0 ? 7 : weekday;
            int isoYear = y;
            int isoWeek = (dayOfYear - isoWeekday + 10) / 7;
            if (isoWeek < 1) {
                if (isoYear == 1)
                    return ExprValue("numeric", "", true); // BC unsupported
                --isoYear;
                isoWeek = isoWeeksInYear(isoYear);
            } else if (isoWeek > isoWeeksInYear(isoYear)) {
                ++isoYear;
                isoWeek = 1;
            }
            r = field == "week" ? isoWeek : isoYear;
        }
        else if (field == "dow" || field == "isodow") {
            const int w = sundayBasedWeekday(y, mo, d);
            if (field == "isodow") r = (w == 0) ? 7 : w;  // 1=Mon .. 7=Sun
            else r = w;                                    // 0=Sun .. 6=Sat
        } else if (field == "doy") {
            Date cur(y, mo, d), jan1(y, 1, 1);
            r = (cur.year != 0 && jan1.year != 0) ? cur.convert() - jan1.convert() + 1 : 0;
        } else if (field == "julian") {
            const int calendarOffset = (14 - mo) / 12;
            const int shiftedYear = y + 4800 - calendarOffset;
            const int shiftedMonth = mo + 12 * calendarOffset - 3;
            const int64_t julianDay =
                d + (153LL * shiftedMonth + 2) / 5 +
                365LL * shiftedYear + shiftedYear / 4 -
                shiftedYear / 100 + shiftedYear / 400 - 32045;
            const long double dayFraction =
                (static_cast<long double>(h * 3600 + mi * 60) *
                     1000000.0L +
                 static_cast<long double>(secondMicros)) /
                86400000000.0L;
            std::ostringstream out;
            out << std::fixed << std::setprecision(6)
                << static_cast<long double>(julianDay) + dayFraction;
            return ExprValue("numeric", out.str(), false);
        } else if (field == "epoch") {
            // Timestamp: seconds since 1970-01-01 00:00:00, numeric
            // scale 6 (86400.000000).
            const __int128 epochMicros =
                static_cast<__int128>(parsedTimestamp->micros) -
                static_cast<__int128>(parseTimestampToSeconds(
                    "1970-01-01 00:00:00")) * 1000000;
            return ExprValue(
                "numeric", formatMicrosNumeric(epochMicros), false);
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
        const int64_t y = a[0].asInt();
        const int64_t m = a[1].asInt();
        const int64_t d = a[2].asInt();
        if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1 || d > 31) {
            throw std::runtime_error(
                "date field value out of range (SQLSTATE 22008)");
        }
        Date date(static_cast<int>(y), static_cast<int>(m),
                  static_cast<int>(d));
        if (date.year == 0) {
            throw std::runtime_error(
                "date field value out of range (SQLSTATE 22008)");
        }
        return ExprValue("date", str(date), false);
    };
    // make_time(h, m, s) -> 'HH:MM:SS'
    functions_["make_time"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("time", "", true);
        const int64_t h = a[0].asInt();
        const int64_t m = a[1].asInt();
        const std::string result = formatTimeFields(h, m, a[2].value);
        if (result.empty()) {
            throw std::runtime_error(
                "time field value out of range (SQLSTATE 22008)");
        }
        return ExprValue("time", result, false);
    };
    // make_timestamp(y, m, d, h, mi, s) -> 'YYYY-MM-DD HH:MM:SS'
    functions_["make_timestamp"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 6) return ExprValue("timestamp", "", true);
        for (int i = 0; i < 6; ++i) if (a[i].isNull) return ExprValue("timestamp", "", true);
        const int64_t y = a[0].asInt();
        const int64_t mo = a[1].asInt();
        const int64_t d = a[2].asInt();
        const int64_t h = a[3].asInt();
        const int64_t mi = a[4].asInt();
        if (y < 1 || y > 9999 || mo < 1 || mo > 12 || d < 1 || d > 31) {
            throw std::runtime_error(
                "timestamp field value out of range (SQLSTATE 22008)");
        }
        Date date(static_cast<int>(y), static_cast<int>(mo),
                  static_cast<int>(d));
        if (date.year == 0) {
            throw std::runtime_error(
                "timestamp field value out of range (SQLSTATE 22008)");
        }
        int dayCarry = 0;
        const std::string time =
            formatTimeFields(h, mi, a[5].value, &dayCarry);
        if (time.empty()) {
            throw std::runtime_error(
                "timestamp field value out of range (SQLSTATE 22008)");
        }
        if (dayCarry != 0) {
            Date advanced(date.year, date.month, date.day + 1);
            if (advanced.year == 0) {
                int year = date.year;
                int month = date.month + 1;
                if (month > 12) {
                    month = 1;
                    ++year;
                }
                if (year > 9999) {
                    throw std::runtime_error(
                        "timestamp field value out of range "
                        "(SQLSTATE 22008)");
                }
                advanced = Date(year, month, 1);
            }
            if (advanced.year == 0) {
                throw std::runtime_error(
                    "timestamp field value out of range "
                    "(SQLSTATE 22008)");
            }
            date = advanced;
        }
        return ExprValue("timestamp", str(date) + " " + time, false);
    };
    // date_trunc(field, source) -> truncate timestamp to the given precision
    functions_["date_trunc"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2)
            return ExprValue("timestamp", "", true);
        const std::string sourceType = toLower(a[1].typeName);
        const bool dateInput = sourceType == "date";
        const bool intervalInput =
            sourceType.find("interval") != std::string::npos;
        const bool withTimeZone = sourceType == "timestamptz" ||
            sourceType == "timestamp with time zone";
        const std::string resultType = intervalInput ? "interval" :
            (dateInput || withTimeZone ? "timestamptz" : "timestamp");
        if (a[0].isNull || a[1].isNull)
            return ExprValue(resultType, "", true);
        std::string field = toLower(a[0].value);
        const std::string& src = a[1].value;
        if (intervalInput) {
            const IntervalParts parsed = parseIntervalText(src);
            if (!parsed.ok) return ExprValue("interval", "", true);
            long long months = parsed.months;
            long long days = parsed.days;
            long long micros = parsed.micros;
            if (field == "millennium") {
                months = months / 12000 * 12000;
                days = micros = 0;
            } else if (field == "century") {
                months = months / 1200 * 1200;
                days = micros = 0;
            } else if (field == "decade") {
                months = months / 120 * 120;
                days = micros = 0;
            } else if (field == "year") {
                months = months / 12 * 12;
                days = micros = 0;
            } else if (field == "quarter") {
                months = months / 3 * 3;
                days = micros = 0;
            } else if (field == "month") {
                days = micros = 0;
            } else if (field == "day") {
                micros = 0;
            } else if (field == "hour") {
                micros = micros / 3600000000LL * 3600000000LL;
            } else if (field == "minute") {
                micros = micros / 60000000LL * 60000000LL;
            } else if (field == "second") {
                micros = micros / 1000000LL * 1000000LL;
            } else if (field == "milliseconds") {
                micros = micros / 1000LL * 1000LL;
            } else if (field != "microseconds") {
                if (field == "week" || field == "timezone" ||
                    field == "timezone_hour" ||
                    field == "timezone_minute") {
                    throw std::runtime_error(
                        "unit \"" + field +
                        "\" not supported for type interval "
                        "(SQLSTATE 0A000)");
                }
                throw std::runtime_error(
                    "unit \"" + field +
                    "\" not recognized for type interval "
                    "(SQLSTATE 22023)");
            }
            return ExprValue(
                "interval", intervalToText(months, days, micros), false);
        }
        const auto parsedTimestamp =
            parseComparableTimestamp(src, withTimeZone);
        if (!parsedTimestamp)
            return ExprValue(resultType, "", true);
        if (parsedTimestamp->infinity != 0) {
            return ExprValue(resultType,
                             parsedTimestamp->infinity > 0
                                 ? "infinity" : "-infinity",
                             false);
        }
        int64_t fractionalMicros =
            parsedTimestamp->micros % 1000000LL;
        const std::string calendarSource = formatTimestampSeconds(
            parsedTimestamp->micros / 1000000LL);
        if (calendarSource.empty())
            return ExprValue(resultType, "", true);
        auto num = [&](size_t off, size_t len) -> int {
            if (calendarSource.size() < off + len) return 0;
            int v = 0;
            for (size_t i = off; i < off + len; ++i) {
                char c = calendarSource[i];
                if (c < '0' || c > '9') return 0;
                v = v * 10 + (c - '0');
            }
            return v;
        };
        int y = num(0, 4), mo = num(5, 2), d = num(8, 2);
        int h = num(11, 2), mi = num(14, 2), se = num(17, 2);
        if (field == "millennium") {
            y = (y - 1) / 1000 * 1000 + 1;
            mo = 1; d = 1; h = mi = se = 0;
        }
        else if (field == "century") {
            y = (y - 1) / 100 * 100 + 1;
            mo = 1; d = 1; h = mi = se = 0;
        }
        else if (field == "decade") {
            y = y / 10 * 10;
            if (y == 0)
                return ExprValue(resultType, "", true); // BC unsupported
            mo = 1; d = 1; h = mi = se = 0;
        }
        else if (field == "year") { mo = 1; d = 1; h = mi = se = 0; }
        else if (field == "quarter") { mo = mo > 0 ? (mo - 1) / 3 * 3 + 1 : 1; d = 1; h = mi = se = 0; }
        else if (field == "month") { d = 1; h = mi = se = 0; }
        else if (field == "week") {
            static const int monthOffsets[] = {
                0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4
            };
            const int adjustedYear = y - (mo < 3 ? 1 : 0);
            const int sundayBased =
                (adjustedYear + adjustedYear / 4 - adjustedYear / 100 +
                 adjustedYear / 400 + monthOffsets[mo - 1] + d) % 7;
            int daysToMonday = (sundayBased + 6) % 7;
            while (daysToMonday-- > 0) {
                if (d > 1) {
                    --d;
                    continue;
                }
                if (--mo == 0) {
                    mo = 12;
                    if (--y == 0)
                        return ExprValue(resultType, "", true);
                }
                d = 31;
                while (Date(y, mo, d).year == 0) --d;
            }
            h = mi = se = 0;
        }
        else if (field == "day") { h = mi = se = 0; }
        else if (field == "hour") { mi = se = 0; }
        else if (field == "minute") { se = 0; }
        else if (field == "second" || field == "milliseconds" ||
                 field == "microseconds") { /* keep */ }
        else {
            throw std::runtime_error(
                "unit \"" + field +
                "\" not recognized for date_trunc (SQLSTATE 22023)");
        }
        if (field == "milliseconds") {
            fractionalMicros = fractionalMicros / 1000 * 1000;
        } else if (field != "microseconds") {
            fractionalMicros = 0;
        }
        char buf[40];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, se);
        // PG date_trunc over a date input promotes to timestamptz and
        // renders with the zone suffix (+00); timestamp input stays plain.
        std::string res = buf;
        if (fractionalMicros != 0) {
            std::string fraction = std::to_string(fractionalMicros);
            fraction.insert(fraction.begin(), 6 - fraction.size(), '0');
            while (!fraction.empty() && fraction.back() == '0')
                fraction.pop_back();
            res += "." + fraction;
        }
        if (dateInput || withTimeZone) res += "+00";
        return ExprValue(resultType, res, false);
    };
    // to_char(value, fmt): format a date/timestamp/time or number as text. The
    // input is treated as temporal when its declared type is date/time/timestamp
    // or its value looks like 'YYYY-MM-DD...' / 'HH:MM:SS'; otherwise numeric.
    // justify_hours/days/interval: PG interval normalization.
    auto intervalToTextPg = [](long long months, long long days, long long micros) -> std::string {
        std::string o;
        auto part = [&](long long v, const char* one, const char* many) {
            if (!v) return;
            if (!o.empty()) o += " ";
            o += std::to_string(v) + " " + ((v == 1) ? one : many);
        };
        long long yy = months / 12, mm = months % 12;
        part(yy, "year", "years"); part(mm, "mon", "mons"); part(days, "day", "days");
        if (micros || o.empty()) {
            bool tn = micros < 0; long long au = tn ? -micros : micros;
            long long hh = au / 3600000000LL; au %= 3600000000LL;
            long long mi = au / 60000000LL; au %= 60000000LL;
            long long se = au / 1000000LL;
            long long frac = au % 1000000LL;
            char tb[64];
            if (frac) {
                std::snprintf(tb, sizeof tb, "%s%02lld:%02lld:%02lld.%06lld",
                              tn ? "-" : "", hh, mi, se, frac);
            } else {
                std::snprintf(tb, sizeof tb, "%s%02lld:%02lld:%02lld",
                              tn ? "-" : "", hh, mi, se);
            }
            if (!o.empty()) o += " ";
            o += tb;
        }
        return o;
    };
    auto justifyCommon = [intervalToTextPg](
                             const std::string& in,
                             int mode) -> std::optional<std::string> {
        IntervalParts p = parseIntervalText(in);
        if (!p.ok) return std::nullopt;
        long long months = p.months, days = p.days, micros = p.micros;
        auto assignRepresentable = [](const __int128 value,
                                      long long& output) {
            if (value <= std::numeric_limits<long long>::lowest() ||
                value > std::numeric_limits<long long>::max()) {
                return false;
            }
            output = static_cast<long long>(value);
            return true;
        };
        if (mode == 2) {
            // Normalize so a single sign dominates: decompose |days+time|,
            // attach the overall sign, fold whole 30-day groups into months.
            const __int128 totalMicros =
                static_cast<__int128>(days) * 86400000000LL + micros;
            const __int128 sign = months != 0
                ? (months < 0 ? -1 : 1)
                : (totalMicros < 0 ? -1 : 1);
            const __int128 magnitude =
                totalMicros < 0 ? -totalMicros : totalMicros;
            const __int128 normalizedDays = magnitude / 86400000000LL;
            if (!assignRepresentable(
                    static_cast<__int128>(months) +
                        sign * (normalizedDays / 30),
                    months) ||
                !assignRepresentable(sign * (normalizedDays % 30), days) ||
                !assignRepresentable(
                    sign * (magnitude % 86400000000LL), micros)) {
                return std::nullopt;
            }
        } else if (mode == 1) {
            if (!assignRepresentable(
                    static_cast<__int128>(months) + days / 30, months)) {
                return std::nullopt;
            }
            days %= 30;
        } else {
            const long long normalizedDays = micros / 86400000000LL;
            if (!assignRepresentable(
                    static_cast<__int128>(days) + normalizedDays, days)) {
                return std::nullopt;
            }
            micros %= 86400000000LL;
        }
        return intervalToTextPg(months, days, micros);
    };
    // isfinite(interval/date/timestamp): false for infinity/NaN.
    functions_["isfinite"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull) return ExprValue("bool", "", true);
        std::string v = a[0].value, lv;
        for (char c : v)
            lv += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        bool finite = lv.find("infinity") == std::string::npos &&
                      lv.find("nan") == std::string::npos;
        return ExprValue("bool", finite ? "t" : "f", false);
    };
    functions_["justify_hours"] = [justifyCommon](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull)
            return ExprValue("interval", "", true);
        const auto result = justifyCommon(a[0].value, 0);
        return ExprValue("interval", result.value_or(""), !result);
    };
    functions_["justify_days"] = [justifyCommon](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull)
            return ExprValue("interval", "", true);
        const auto result = justifyCommon(a[0].value, 1);
        return ExprValue("interval", result.value_or(""), !result);
    };
    // age(ts [, ts]): with one argument PostgreSQL subtracts the timestamp
    // from current_date at midnight; two arguments subtract right from left.
    functions_["age"] = [intervalToTextPg, stableDate](
                            const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull) || stableDate.empty())
            return ExprValue("interval", "", true);
        const std::string lhs = a.size() == 1
            ? stableDate + " 00:00:00"
            : a[0].value;
        const std::string& rhs = a.size() == 1 ? a[0].value : a[1].value;
        auto splitTs = [](const std::string& v, long long& Y, long long& Mo,
                          long long& D, long long& us) {
            int y = 0, mo = 0, d = 0;
            if (std::sscanf(v.substr(0, 10).c_str(), "%d-%d-%d", &y, &mo, &d) != 3)
                return false;
            Y = y; Mo = mo; D = d;
            long long h = 0, mi = 0, se = 0, fr = 0;
            if (v.size() > 11) {
                std::sscanf(v.substr(11).c_str(), "%lld:%lld:%lld", &h, &mi, &se);
                size_t dot = v.find(46, 11);
                if (dot != std::string::npos) {
                    size_t digits = 0;
                    for (size_t i = dot + 1;
                         i < v.size() && v[i] >= '0' && v[i] <= '9' &&
                         digits < 6;
                         ++i, ++digits) {
                        fr = fr * 10 + (v[i] - '0');
                    }
                    if (digits == 0) return false;
                    while (digits++ < 6) fr *= 10;
                }
            }
            us = ((h * 3600 + mi * 60 + se) * 1000000LL) + fr;
            return true;
        };
        long long y1 = 0, m1 = 0, d1 = 0, us1 = 0, y2 = 0, m2 = 0, d2 = 0, us2 = 0;
        if (!splitTs(lhs, y1, m1, d1, us1) ||
            !splitTs(rhs, y2, m2, d2, us2))
            return ExprValue("interval", "", true);
        // PG renders a reversed age (earlier first) as the negated
        // swap: age(2020-01-01, 2026-05-06) = -6 years -4 mons -5 days.
        // Swap the operands and negate every field at the end.
        bool swapped = false;
        auto tsLess = [](long long y, long long mo, long long d, long long us2,
                        long long yB, long long moB, long long dB, long long usB) {
            if (y != yB) return y < yB;
            if (mo != moB) return mo < moB;
            if (d != dB) return d < dB;
            return us2 < usB;
        };
        if (tsLess(y1, m1, d1, us1, y2, m2, d2, us2)) {
            std::swap(y1, y2); std::swap(m1, m2); std::swap(d1, d2);
            std::swap(us1, us2);
            swapped = true;
        }
        long long months = (y1 * 12 + m1) - (y2 * 12 + m2);
        long long days = d1 - d2;
        long long micros = us1 - us2;
        if (micros < 0) { micros += 86400000000LL; days -= 1; }
        if (days < 0) {
            // PG timestamp_age borrow: the day field borrows the
            // length of the EARLIER timestamp's own month (dt2.mon
            // in dt2.year), not the preceding month of dt1.  Derived
            // empirically against reference PG 17 and verified on 16
            // samples including leap-February and year-wrap borrows:
            // age(2026-05-06, 2000-01-15) = 26y 3m 22d (Jan=31).
            long long plen = civilToDays((int)(y2 + (m2 == 12 ? 1 : 0)),
                                         (unsigned)(m2 == 12 ? 1 : m2 + 1), 1) -
                             civilToDays((int)y2, (unsigned)m2, 1);
            days += plen; months -= 1;
        }
        if (months < 0) { months += 12; y1 -= 1; }
        if (swapped) { months = -months; days = -days; micros = -micros; }
        return ExprValue("interval", intervalToTextPg(months, days, micros), false);
    };
    // EXISTS (SELECT ... FROM t [WHERE ...]) as a SELECT-list item:
    // PG returns boolean true/false.  The subquery text arrives as
    // the single argument; parse the table and conditions, then ask
    // the storage engine whether any row satisfies them.
    functions_["exists"] = [this](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull) return ExprValue("boolean", "", true);
        std::string sub = a[0].value;
        // the argument arrives wrapped by the caller: strip one
        // balanced outer paren pair if present
        while (sub.size() >= 2 && sub.front() == '(' && sub.back() == ')') {
            int depth = 0; bool bal = true;
            for (size_t i = 0; i < sub.size(); ++i) {
                if (sub[i] == '(') ++depth;
                else if (sub[i] == ')') { --depth; if (depth == 0 && i + 1 != sub.size()) { bal = false; break; } }
            }
            if (!bal || depth != 0) break;
            sub = sub.substr(1, sub.size() - 2);
        }
        std::string low;
        for (char c : sub) low += static_cast<char>(tolower(static_cast<unsigned char>(c)));
        size_t fp = low.find(" from ");
        if (fp == std::string::npos) return ExprValue("boolean", "f", false);
        size_t wp = low.find(" where ");
        std::string table = trimStr(sub.substr(fp + 6, (wp == std::string::npos ? sub.size() : wp) - fp - 6));
        size_t sp = table.find_first_of(" ,;)");
        if (sp != std::string::npos) table = table.substr(0, sp);
        std::string condRaw;
        if (wp != std::string::npos) {
            condRaw = trimStr(sub.substr(wp + 7));
            // parseConditions expects whitespace-separated tokens:
            // pad comparison operators with spaces when missing.
            std::string padded;
            for (size_t i = 0; i < condRaw.size(); ++i) {
                char c = condRaw[i];
                char prev = padded.empty() ? ' ' : padded.back();
                bool isOp = (c == '=' || c == '<' || c == '>');
                bool prevIsOp = (prev == '=' || prev == '<' || prev == '>' || prev == '!');
                if (isOp && prev != ' ' && !prevIsOp) padded += ' ';
                padded += c;
                char next = (i + 1 < condRaw.size()) ? condRaw[i + 1] : ' ';
                bool nextIsOp = (next == '=' || next == '<' || next == '>');
                if (isOp && next != ' ' && !nextIsOp) padded += ' ';
            }
            condRaw = padded;
        }
        std::vector<std::string> condTexts;
        if (!condRaw.empty()) {
            // parseConditions expects the operator FIRST, glued to
            // the column name: "id = 1" -> "=id 1".  Split the
            // (already space-padded) text into three tokens and
            // re-emit in engine order.
            std::istringstream iss(condRaw);
            std::string lhs, op, rhs;
            iss >> lhs >> op >> rhs;
            if (!lhs.empty() && !op.empty() && !rhs.empty()) {
                std::string rest;
                std::getline(iss, rest);
                std::string tail = trimStr(rest);
                if (!tail.empty()) rhs += ' ' + tail;
                condTexts.push_back(op + lhs + ' ' + rhs);
            }
        }

        auto conds = dbms::StorageEngine::parseConditions(condTexts);
        bool scanFailed = false;
        bool indexReadFailed = false;
        bool any = g_engine.anyRowMatches(
            currentDB_, table, conds, &scanFailed, &indexReadFailed);
        if (indexReadFailed) {
            throw DbError("XX001", "B-tree index read failed for relation \"" +
                table + "\"");
        }
        (void)scanFailed;
        return ExprValue("boolean", any ? "t" : "f", false);
    };
    functions_["justify_interval"] = [justifyCommon](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull)
            return ExprValue("interval", "", true);
        const auto result = justifyCommon(a[0].value, 2);
        return ExprValue("interval", result.value_or(""), !result);
    };
    functions_["to_char"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        // Unwrap typed literals: date '...' / timestamp '...' / numeric '...'
        std::string vval = a[0].value;
        {
            static const char* kws[] = {"date ", "timestamp ", "timestamptz ", "interval ", "boolean ", "time ", "numeric ", "int ", "text "};
            for (const char* kw : kws) {
                std::string kwd(kw);
                if (vval.size() > kwd.size()) {
                    std::string low = vval.substr(0, kwd.size());
                    for (auto& lc : low) lc = static_cast<char>(std::tolower((unsigned char)lc));
                    if (low == kwd) {
                        vval = vval.substr(kwd.size());
                        if (vval.size() >= 2 && vval.front() == (char)39 && vval.back() == (char)39)
                            vval = vval.substr(1, vval.size() - 2);
                        break;
                    }
                }
            }
        }
        const std::string& v = vval;
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
        // Interval input: format HH24/MI/SS from the interval parts
        // (PG renders interval time-of-day fields).
        if (tn == "interval") {
            IntervalParts ip = parseIntervalText(vval);
            if (!ip.ok) return ExprValue("text", "", true);
            {
                long long totalSecs = ip.micros / 1000000LL;
                long long hh = totalSecs / 3600;
                long long mm = (totalSecs % 3600) / 60;
                long long ss = totalSecs % 60;
                auto formatIntervalField = [](long long value) {
                    std::string field = std::to_string(value);
                    if (field.size() < 2) field.insert(field.begin(), '0');
                    return field;
                };
                std::string out;
                for (size_t fi = 0; fi < fmt.size(); ++fi) {
                    if (fmt.compare(fi, 4, "HH24") == 0) {
                        out += formatIntervalField(hh); fi += 3; continue;
                    }
                    if (fmt.compare(fi, 2, "MI") == 0) {
                        out += formatIntervalField(mm); fi += 1; continue;
                    }
                    if (fmt.compare(fi, 2, "SS") == 0) {
                        out += formatIntervalField(ss); fi += 1; continue;
                    }
                    out += fmt[fi];
                }
                return ExprValue("text", out, false);
            }
        }
        if (temporal) return ExprValue("text", formatDateTime(v, fmt), false);
        const bool exactNumeric = tn == "numeric" || tn == "decimal" ||
            tn == "integer" || tn == "int" || tn == "smallint" ||
            tn == "bigint" || tn == "int2" || tn == "int4" || tn == "int8";
        return ExprValue("text", formatNumeric(a[0].asDouble(), fmt,
            exactNumeric ? vval : std::string()), false);
    };

    // ------------------------------------------------------------------------
    // Sequence functions (delegate to the global StorageEngine)
    // ------------------------------------------------------------------------
    auto requireSequenceArity = [](const std::vector<ExprValue>& arguments,
                                   size_t expected,
                                   const char* signature) {
        if (arguments.size() != expected) {
            throw DbError("42883", std::string("function ") + signature +
                                       " does not exist");
        }
    };
    functions_["nextval"] = [this, requireSequenceArity](
        const std::vector<ExprValue>& a) -> ExprValue {
        requireSequenceArity(a, 1, "nextval(regclass)");
        if (a[0].isNull) return ExprValue("bigint", "", true);
        int64_t v = g_engine.nextval(currentDB_, a[0].value);
        return ExprValue("bigint", std::to_string(v), false);
    };
    functions_["currval"] = [this, requireSequenceArity](
        const std::vector<ExprValue>& a) -> ExprValue {
        requireSequenceArity(a, 1, "currval(regclass)");
        if (a[0].isNull) return ExprValue("bigint", "", true);
        int64_t v = g_engine.currval(currentDB_, a[0].value);
        return ExprValue("bigint", std::to_string(v), false);
    };
    functions_["lastval"] = [this, requireSequenceArity](
        const std::vector<ExprValue>& a) -> ExprValue {
        requireSequenceArity(a, 0, "lastval()");
        int64_t v = g_engine.lastval();
        return ExprValue("bigint", std::to_string(v), false);
    };
    functions_["setval"] = [this](
        const std::vector<ExprValue>& a) -> ExprValue {
        if (a.size() != 2 && a.size() != 3) {
            throw DbError("42883",
                          "function setval(regclass, bigint [, boolean]) "
                          "does not exist");
        }
        for (const auto& argument : a) {
            if (argument.isNull) return ExprValue("bigint", "", true);
        }
        long long parsedValue = 0;
        if (!parseInt64Exact(a[1].value, parsedValue)) {
            throw DbError("22P02", "invalid input syntax for type bigint");
        }
        bool isCalled = true;
        if (a.size() == 3) {
            const auto parsedBoolean = parsePostgresBoolean(a[2].value);
            if (!parsedBoolean) {
                throw DbError("22P02", "invalid input syntax for type boolean");
            }
            isCalled = *parsedBoolean;
        }
        const int64_t v = g_engine.setval(
            currentDB_, a[0].value, static_cast<int64_t>(parsedValue),
            isCalled);
        return ExprValue("bigint", std::to_string(v), false);
    };

    // Volatility metadata for builtins (safe default is 'v' set by registerFunction).
    volatility_["abs"] = 'i';
    volatility_["length"] = 'i';
    volatility_["lower"] = 'i';
    volatility_["upper"] = 'i';
    volatility_["substring"] = 'i';
    volatility_["round"] = 'i';
    volatility_["sin"] = 'i';
    volatility_["cos"] = 'i';
    volatility_["tan"] = 'i';
    volatility_["asin"] = 'i';
    volatility_["acos"] = 'i';
    volatility_["atan"] = 'i';
    volatility_["exp"] = 'i';
    volatility_["ln"] = 'i';
    volatility_["log"] = 'i';
    volatility_["log10"] = 'i';
    volatility_["sqrt"] = 'i';
    volatility_["cbrt"] = 'i';
    volatility_["ceil"] = 'i';
    volatility_["floor"] = 'i';
    volatility_["trunc"] = 'i';
    volatility_["atan2"] = 'i';
    volatility_["now"] = 's';
    volatility_["current_date"] = 's';
    volatility_["current_timestamp"] = 's';
    volatility_["localtimestamp"] = 's';
    volatility_["transaction_timestamp"] = 's';
    volatility_["statement_timestamp"] = 's';
    volatility_["current_time"] = 's';
    volatility_["localtime"] = 's';
    volatility_["clock_timestamp"] = 'v';
    volatility_["current_user"] = 's';
    volatility_["session_user"] = 's';
    volatility_["nextval"] = 'v';
    volatility_["currval"] = 'v';
    volatility_["lastval"] = 'v';
    volatility_["setval"] = 'v';
    volatility_["random"] = 'v';
    volatility_["xml_is_well_formed"] = 's';
    volatility_["xml_is_well_formed_content"] = 'i';
    volatility_["xml_is_well_formed_document"] = 'i';
    volatility_["xml_is_document"] = 'i';
    volatility_["xmlconcat"] = 'i';
    volatility_["xmlcomment"] = 'i';
}

} // namespace dbms
