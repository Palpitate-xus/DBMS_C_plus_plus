#include "ExprEvaluator.h"
#include "commands/TableManage.h"
#include "common/DateType.h"
#include "types/numeric.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <regex>
#include <sstream>
#include <tuple>

// Global engine reference used by sequence builtins.
extern dbms::StorageEngine g_engine;

namespace dbms {

// ============================================================================
// ExprValue helpers
// ============================================================================

static std::string toLower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
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

struct IntervalParts {
    long long months = 0;
    long long days = 0;
    long long micros = 0;
    bool ok = false;
};

static bool combineIntervalField(long long left, long long right,
                                 bool subtract, long long& result) {
    const __int128 total = static_cast<__int128>(left) +
        (subtract ? -static_cast<__int128>(right)
                  : static_cast<__int128>(right));
    if (total <= std::numeric_limits<long long>::lowest() ||
        total > std::numeric_limits<long long>::max()) {
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
        std::round(seconds * 1000000.0L);
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
                                    const std::string& secondsText) {
    if (hours < 0 || hours > 23 || minutes < 0 || minutes > 59)
        return "";

    long double seconds = 0;
    try {
        size_t consumed = 0;
        seconds = std::stold(secondsText, &consumed);
        if (consumed != secondsText.size() || !std::isfinite(seconds) ||
            seconds < 0 || seconds >= 60) {
            return "";
        }
    } catch (...) {
        return "";
    }

    const long long secondMicros =
        static_cast<long long>(std::round(seconds * 1000000.0L));
    if (secondMicros < 0 || secondMicros >= 60000000LL) return "";

    const long long wholeSeconds = secondMicros / 1000000LL;
    const long long fraction = secondMicros % 1000000LL;
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%02lld:%02lld:%02lld", hours,
                  minutes, wholeSeconds);
    std::string result = buffer;
    if (fraction != 0) {
        std::string digits = std::to_string(fraction);
        digits.insert(digits.begin(), 6 - digits.size(), '0');
        while (!digits.empty() && digits.back() == '0') digits.pop_back();
        result += "." + digits;
    }
    return result;
}

static bool scaleIntervalField(long long value, long double scale,
                               long long& result) {
    const long double scaled = static_cast<long double>(value) * scale;
    if (!std::isfinite(scaled) ||
        scaled <= static_cast<long double>(
                      std::numeric_limits<long long>::lowest()) ||
        scaled > static_cast<long double>(
                     std::numeric_limits<long long>::max())) {
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

// Parse the canonical output form or the human input form ("2 years",
// "14 months", "90 minutes", "1-2", "04:05:06", "1.5 days").
static IntervalParts parseIntervalText(const std::string& in) {
    IntervalParts r;
    std::string s = trimStr(in);
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') s = trimStr(s.substr(1, s.size() - 2));
    if (s.empty()) return r;
    bool negate = false;
    std::string low;
    for (char c : s) low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (low.size() > 4 && low.substr(low.size() - 4) == " ago") {
        negate = true;
        s = trimStr(s.substr(0, s.size() - 4));
    }
    auto parseInteger = [](const std::string& text, long long& value) {
        try {
            size_t consumed = 0;
            value = std::stoll(text, &consumed);
            return consumed == text.size();
        } catch (...) {
            return false;
        }
    };
    auto addScaled = [](long long& target, long long value,
                        long long scale) {
        const __int128 total = static_cast<__int128>(target) +
            static_cast<__int128>(value) * scale;
        // LLONG_MIN cannot be safely negated by the canonical formatter or
        // by a trailing "ago", so it is outside this text representation.
        if (total <= std::numeric_limits<long long>::lowest() ||
            total > std::numeric_limits<long long>::max()) {
            return false;
        }
        target = static_cast<long long>(total);
        return true;
    };
    auto parseDecimal = [](const std::string& text, long double& value) {
        try {
            size_t consumed = 0;
            value = std::stold(text, &consumed);
            return consumed == text.size() && std::isfinite(value);
        } catch (...) {
            return false;
        }
    };
    auto truncateToInteger = [](long double value, long long& result) {
        if (!std::isfinite(value) ||
            value <= static_cast<long double>(
                         std::numeric_limits<long long>::lowest()) ||
            value > static_cast<long double>(
                        std::numeric_limits<long long>::max())) {
            return false;
        }
        result = static_cast<long long>(value);
        return true;
    };
    auto addScaledDecimal = [&](long long& target, long double value,
                                long double scale) {
        long long delta = 0;
        return truncateToInteger(value * scale, delta) &&
               addScaled(target, delta, 1);
    };
    // SQL year-month shorthand "N-M"
    {
        bool shorthand = true;
        size_t dash = std::string::npos;
        for (size_t i = 0; i < s.size(); ++i) {
            char c = s[i];
            if (c == '-') { if (dash != std::string::npos) { shorthand = false; break; } dash = i; }
            else if (!std::isdigit(static_cast<unsigned char>(c))) { shorthand = false; break; }
        }
        if (shorthand && dash != std::string::npos && dash > 0 && dash + 1 < s.size()) {
            long long years = 0;
            long long months = 0;
            if (!parseInteger(s.substr(0, dash), years) ||
                !parseInteger(s.substr(dash + 1), months) ||
                !addScaled(r.months, years, 12) ||
                !addScaled(r.months, months, 1)) {
                return IntervalParts{};
            }
            if (negate) r.months = -r.months;
            r.ok = true;
            return r;
        }
    }
    r.ok = true;
    auto addClockMicros = [&](long long hours, long long minutes,
                              long long seconds, long long fraction,
                              bool negative) {
        if (minutes < 0 || seconds < 0 || fraction < 0) return false;
        __int128 hourMagnitude = static_cast<__int128>(hours);
        if (hourMagnitude < 0) hourMagnitude = -hourMagnitude;
        __int128 delta =
            (hourMagnitude * 3600 + static_cast<__int128>(minutes) * 60 +
             seconds) * 1000000 + fraction;
        if (negative) delta = -delta;
        const __int128 total = static_cast<__int128>(r.micros) + delta;
        if (total <= std::numeric_limits<long long>::lowest() ||
            total > std::numeric_limits<long long>::max()) {
            return false;
        }
        r.micros = static_cast<long long>(total);
        return true;
    };
    auto parseClockToken = [&](const std::string& token) {
        const size_t firstColon = token.find(':');
        if (firstColon == std::string::npos || firstColon == 0)
            return false;
        const size_t secondColon = token.find(':', firstColon + 1);
        if (secondColon != std::string::npos &&
            token.find(':', secondColon + 1) != std::string::npos) {
            return false;
        }
        auto parseClockInteger = [&](const std::string& text,
                                     long long& value) {
            return !text.empty() &&
                text.find_first_not_of("0123456789") == std::string::npos &&
                parseInteger(text, value);
        };

        const std::string hourText = token.substr(0, firstColon);
        long long hours = 0;
        if (!parseInteger(hourText, hours)) return false;
        const size_t minuteEnd = secondColon == std::string::npos
            ? token.size() : secondColon;
        long long minutes = 0;
        if (!parseClockInteger(
                token.substr(firstColon + 1,
                             minuteEnd - firstColon - 1),
                minutes)) {
            return false;
        }

        long long seconds = 0;
        long long fraction = 0;
        if (secondColon != std::string::npos) {
            const std::string secondsText = token.substr(secondColon + 1);
            const size_t dot = secondsText.find('.');
            if (dot != std::string::npos &&
                secondsText.find('.', dot + 1) != std::string::npos) {
                return false;
            }
            const std::string whole = secondsText.substr(0, dot);
            if (!parseClockInteger(whole, seconds)) return false;
            if (dot != std::string::npos) {
                const std::string digits = secondsText.substr(dot + 1);
                if (digits.empty() ||
                    digits.find_first_not_of("0123456789") !=
                        std::string::npos) {
                    return false;
                }
                const size_t kept = std::min<size_t>(digits.size(), 6);
                for (size_t i = 0; i < kept; ++i)
                    fraction = fraction * 10 + (digits[i] - '0');
                for (size_t i = kept; i < 6; ++i) fraction *= 10;
            }
        }
        return addClockMicros(hours, minutes, seconds, fraction,
                              hourText.front() == '-');
    };
    auto applyUnit = [&](long long n, const std::string& unit) {
        std::string u;
        for (char c : unit) u += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (u == "year" || u == "years" || u == "y")
            return addScaled(r.months, n, 12);
        if (u == "mon" || u == "mons" || u == "month" || u == "months")
            return addScaled(r.months, n, 1);
        if (u == "week" || u == "weeks" || u == "w")
            return addScaled(r.days, n, 7);
        if (u == "day" || u == "days" || u == "d")
            return addScaled(r.days, n, 1);
        if (u == "hour" || u == "hours" || u == "h")
            return addScaled(r.micros, n, 3600000000LL);
        if (u == "min" || u == "mins" || u == "minute" || u == "minutes")
            return addScaled(r.micros, n, 60000000LL);
        if (u == "sec" || u == "secs" || u == "second" || u == "seconds" || u == "s")
            return addScaled(r.micros, n, 1000000LL);
        if (u == "millisec" || u == "millisecs" || u == "milliseconds")
            return addScaled(r.micros, n, 1000LL);
        if (u == "microsec" || u == "microsecs" || u == "microseconds")
            return addScaled(r.micros, n, 1);
        return false;
    };
    std::istringstream iss(s);
    std::string tok;
    while (iss >> tok) {
        if (tok.find(':') != std::string::npos) {
            if (!parseClockToken(tok)) r.ok = false;
            if (!r.ok) break;
            continue;
        }
        bool allDigit = !tok.empty() &&
            tok.find_first_not_of("0123456789") == std::string::npos;
        if (allDigit) {
            long long integer = 0;
            if (!parseInteger(tok, integer)) {
                r.ok = false;
                break;
            }
            // A bare number is seconds — unless the next token is a unit
            // ("1 day"), in which case it applies to that unit.
            std::string peek;
            auto pos0 = iss.tellg();
            if (pos0 != decltype(iss)::pos_type(-1) && (iss >> peek)) {
                bool unitish = !peek.empty() &&
                    std::isalpha(static_cast<unsigned char>(peek[0]));
                if (unitish) {
                    if (!applyUnit(integer, peek)) r.ok = false;
                    continue;
                }
                // put the token back
                iss.clear();
                iss.seekg(pos0);
            }
            if (!addScaled(r.micros, integer, 1000000LL)) {
                r.ok = false;
                break;
            }
            continue;
        }
        // number[.fraction][unit] with unit possibly in the next token
        size_t endNum = 0;
        if (!tok.empty() && (tok[0] == '-' || tok[0] == '+')) ++endNum;
        bool sawDigit = false;
        while (endNum < tok.size() &&
               std::isdigit(static_cast<unsigned char>(tok[endNum]))) {
            sawDigit = true;
            ++endNum;
        }
        if (endNum < tok.size() && tok[endNum] == '.') {
            ++endNum;
            while (endNum < tok.size() &&
                   std::isdigit(static_cast<unsigned char>(tok[endNum]))) {
                sawDigit = true;
                ++endNum;
            }
        }
        if (!sawDigit) { r.ok = false; break; }
        long double n = 0;
        if (!parseDecimal(tok.substr(0, endNum), n)) {
            r.ok = false;
            break;
        }
        std::string unit = (endNum <= tok.size()) ? tok.substr(endNum) : std::string();
        if (unit.empty()) {
            if (!(iss >> unit)) {
                if (!addScaledDecimal(r.micros, n, 1000000.0L))
                    r.ok = false;
                continue;
            }
        }
        // lower-case the unit
        for (char& c : unit) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (unit == "days" || unit == "day") {
            long long whole = 0;
            if (!truncateToInteger(n, whole) ||
                !addScaled(r.days, whole, 1) ||
                !addScaledDecimal(r.micros, n - whole,
                                  86400000000.0L)) {
                r.ok = false;
            }
        } else if (unit == "hours" || unit == "hour") {
            if (!addScaledDecimal(r.micros, n, 3600000000.0L))
                r.ok = false;
        } else if (unit == "minutes" || unit == "minute") {
            if (!addScaledDecimal(r.micros, n, 60000000.0L))
                r.ok = false;
        } else if (unit == "seconds" || unit == "second") {
            if (!addScaledDecimal(r.micros, n, 1000000.0L))
                r.ok = false;
        } else {
            long long integer = 0;
            if (!truncateToInteger(n, integer) ||
                !applyUnit(integer, unit)) {
                r.ok = false;
            }
        }
        if (!r.ok) break;
    }
    if (r.ok && negate) {
        r.months = -r.months;
        r.days = -r.days;
        r.micros = -r.micros;
    }
    return r;
}

// Render (months, days, micros) back to canonical PG text.
static std::string intervalToText(long long months, long long days, long long micros) {
    std::string out;
    auto appendPart = [&](long long value, const char* singular,
                          const char* plural) {
        if (value == 0) return;
        if (!out.empty()) out += " ";
        out += std::to_string(value);
        out += (value == 1 || value == -1) ? singular : plural;
    };
    appendPart(months / 12, " year", " years");
    appendPart(months % 12, " mon", " mons");
    appendPart(days, " day", " days");
    if (micros || out.empty()) {
        if (!out.empty()) out += " ";
        const bool negative = micros < 0;
        long long us = negative ? -micros : micros;
        long long hh = us / 3600000000LL; us %= 3600000000LL;
        long long mm = us / 60000000LL; us %= 60000000LL;
        long long ss = us / 1000000LL; long long frac = us % 1000000LL;
        char buf[64];
        if (frac) {
            std::snprintf(buf, sizeof(buf), "%s%02lld:%02lld:%02lld.%06lld",
                          negative ? "-" : "", hh, mm, ss, frac);
        } else {
            std::snprintf(buf, sizeof(buf), "%s%02lld:%02lld:%02lld",
                          negative ? "-" : "", hh, mm, ss);
        }
        out += buf;
    }
    return out;
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
    int Y = 1970, Mo = 1, D = 1, h = 0, mi = 0;
    long long s = 0;
    const int parsed = std::sscanf(ts.c_str(), "%d-%d-%d %d:%d:%lld",
                                   &Y, &Mo, &D, &h, &mi, &s);
    const bool hasTime = ts.find(' ') != std::string::npos;
    Date inputDate(Y, Mo, D);
    if (parsed < 3 || (hasTime && parsed < 6) || inputDate.year == 0 ||
        Y < 1 || Y > 9999 || h < 0 || h > 23 || mi < 0 || mi > 59 ||
        s < 0 || s > 59) {
        return "";
    }

    const __int128 direction = add ? 1 : -1;
    const long long minimumDay = civilToDays(1, 1, 1);
    const long long maximumDay = civilToDays(9999, 12, 31);
    auto dayInDomain = [&](const __int128 value) {
        return value >= minimumDay && value <= maximumDay;
    };

    __int128 totalDays = static_cast<__int128>(civilToDays(Y, Mo, D)) +
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

// Resolve a timezone name to a UTC offset in minutes. Supports UTC-style
// fixed offsets ("UTC", "UTC+8", "UTC-05:30") and POSIX abbreviated forms
// ("+08", "-0530"). Full IANA tzdata is out of scope for this layer.
static bool parseTimeZoneOffset(const std::string& name, long long& offsetMinutes) {
    std::string s = trimStr(name);
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') s = trimStr(s.substr(1, s.size() - 2));
    if (s.empty()) return false;
    std::string low;
    for (char c : s) low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    // Common named zones (fixed standard-time offsets; no DST model).
    static const std::map<std::string, long long> namedZones = {
        {"asia/tokyo", 540}, {"asia/shanghai", 480}, {"asia/kolkata", 330},
        {"asia/seoul", 540}, {"asia/dubai", 240}, {"asia/singapore", 480},
        {"europe/london", 0}, {"europe/paris", 60}, {"europe/berlin", 60},
        {"europe/moscow", 180}, {"america/new_york", -300}, {"america/chicago", -360},
        {"america/denver", -420}, {"america/los_angeles", -480},
        {"australia/sydney", 600}, {"pacific/auckland", 720},
    };
    auto it = namedZones.find(low);
    if (it != namedZones.end()) { offsetMinutes = it->second; return true; }
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
    // while IANA named zones above keep the natural sign.
    offsetMinutes = -sign * (hh * 60 + mm);
    return true;
}

static std::string formatTimeZoneOffset(long long offsetMinutes) {
    const long long absoluteMinutes = std::llabs(offsetMinutes);
    const long long hours = absoluteMinutes / 60;
    const long long minutes = absoluteMinutes % 60;
    std::ostringstream out;
    out << (offsetMinutes < 0 ? '-' : '+') << std::setfill('0')
        << std::setw(2) << hours;
    if (minutes != 0) {
        out << ':' << std::setw(2) << minutes;
    }
    return out.str();
}

// JSON helpers defined later in this file; forward-declared for the JSON
// operator evaluation (-> / ->> / #> / #>> / @> / <@) higher up.
static std::string trimStr(const std::string& s);
static bool jsonStep(const std::string& cur, const std::string& key, std::string& out);
static bool jsonTopLevelSplit(const std::string& s, char open, char close,
                              std::vector<std::string>& out);

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
        if (raw.find('.') != std::string::npos ||
            raw.find_first_of("eE") != std::string::npos)
            return ExprValue("numeric", raw, false);
        return ExprValue("integer", raw, false);
    }

    return ExprValue("character varying", raw, false);
}

// ----------------------------------------------------------------------------
// Column references
// ----------------------------------------------------------------------------

ExprValue ExprEvaluator::evalColumnRef(const ColumnRefExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
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

    if (op == "+") return v;
    if (op == "-") {
        if (v.isNull) return v;
        if (v.value.empty()) return ExprValue(v.typeName, "0", false);
        if (isIntegerTypeName(v.typeName)) {
            long long integer = 0;
            if (!parseInt64Exact(v.value, integer) ||
                integer == std::numeric_limits<int64_t>::lowest()) {
                throw std::runtime_error(
                    "integer out of range (SQLSTATE 22003)");
            }
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
    if (op.rfind("at time zone", 0) == 0) {
        // AT TIME ZONE <zone>: with a timestamp input, re-interpret the
        // wall clock in that zone (shift by the zone offset to UTC and keep
        // the naive rendering); with a timestamptz input, render the UTC
        // instant at the zone's local wall clock. Offset-only zone model.
        std::string zone = trimStr(e->op.substr(std::string("at time zone").size()));
        long long offMin = 0;
        if (v.isNull || !parseTimeZoneOffset(zone, offMin))
            return ExprValue("timestamp", "", true);
        // Naive zone model: the input wall clock is read as UTC and
        // rendered at the zone's local wall clock (local = utc + offset).
        IntervalParts shift;
        // Read the wall clock AS the zone: UTC = local - offset.
        shift.micros = -offMin * 60000000LL;
        std::string out = timestampShift(v.value, shift, true);
        // Naive timestamp input becomes timestamptz; render with the UTC offset suffix.
        bool tzIn = v.typeName.find("tz") != std::string::npos;
        if (!tzIn && !out.empty()) out += "+00";
        if (out.empty()) return ExprValue("timestamp", "", true);
        return ExprValue("timestamp", out, false);
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

    // Boolean columns are commonly supplied by storage as "true"/"false",
    // while SQL boolean literals are represented internally as "t"/"f".
    // Compare their logical values instead of their different spellings.
    if (ta == "boolean" && tb == "boolean") {
        const bool ba = a.asBool();
        const bool bb = b.asBool();
        return (ba > bb) - (ba < bb);
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
    const bool textualA = blankPaddedA || varyingA || ta == "text";
    const bool textualB = blankPaddedB || varyingB || tb == "text";
    if ((blankPaddedA || blankPaddedB) && textualA && textualB) {
        std::string left = a.value;
        std::string right = b.value;
        auto trimPadding = [](std::string& value) {
            while (!value.empty() && value.back() == ' ') value.pop_back();
        };
        if (blankPaddedA || (blankPaddedB && varyingA)) trimPadding(left);
        if (blankPaddedB || (blankPaddedA && varyingB)) trimPadding(right);
        return left < right ? -1 : (left > right ? 1 : 0);
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

ExprValue ExprEvaluator::applyArithmetic(const std::string& op,
                                         const ExprValue& l,
                                         const ExprValue& r) {
    if (l.isNull || r.isNull) return ExprValue(l.typeName, "", true);

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
            long long ls = parseTimestampToSeconds(l.value);
            long long rs = parseTimestampToSeconds(r.value);
            if (isInfiniteTimestamp(ls) || isInfiniteTimestamp(rs))
                return ExprValue("interval", "", true);
            long long diff = ls - rs;
            // PG renders the sign on each component:
            // -1 days -00:30:00
            long long sgn = (diff < 0) ? -1 : 1;
            long long au = diff < 0 ? -diff : diff;
            long long dd = sgn * (au / 86400);
            long long us = sgn * ((au % 86400) * 1000000LL);
            if (sgn < 0) {
                // PG style: sign on each component (-1 days -00:30:00)
                const char* dunit = "days";
                char nb[56];
                std::snprintf(nb, sizeof(nb), "-%lld %s -%02lld:%02lld:%02lld",
                              au / 86400, dunit, (au % 86400) / 3600,
                              ((au % 86400) % 3600) / 60, (au % 86400) % 60);
                std::string s = nb;
                if (au / 86400 == 0) {
                    char tb[32];
                    std::snprintf(tb, sizeof(tb), "-%02lld:%02lld:%02lld",
                                  (au % 86400) / 3600, ((au % 86400) % 3600) / 60,
                                  (au % 86400) % 60);
                    s = tb;
                }
                return ExprValue("interval", s, false);
            }
            return ExprValue("interval", intervalToText(0, dd, us), false);
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
                return ExprValue("interval", "", true);
            const long double scale = op == "*"
                ? static_cast<long double>(k)
                : 1.0L / static_cast<long double>(k);
            long long months = 0;
            long long days = 0;
            long long micros = 0;
            if (!scaleIntervalField(iv.months, scale, months) ||
                !scaleIntervalField(iv.days, scale, days) ||
                !scaleIntervalField(iv.micros, scale, micros)) {
                return ExprValue("interval", "", true);
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
            if (!combineIntervalField(a.months, b.months, subtract, mm) ||
                !combineIntervalField(a.days, b.days, subtract, dd) ||
                !combineIntervalField(a.micros, b.micros, subtract, us)) {
                return ExprValue("interval", "", true);
            }
            return ExprValue("interval", intervalToText(mm, dd, us), false);
        }
        if (lIv && op == "+") {
            IntervalParts iv = parseIntervalText(l.value);
            if (!iv.ok || !isTsLike(r)) return ExprValue("timestamp", "", true);
            std::string shifted = timestampShift(r.value, iv, true);
            if (shifted.empty()) return ExprValue("timestamp", "", true);
            return ExprValue("timestamp", shifted, false);
        }
        if (rIv) {
            IntervalParts iv = parseIntervalText(r.value);
            if (!iv.ok || !isTsLike(l)) return ExprValue("timestamp", "", true);
            std::string shifted = timestampShift(l.value, iv, op == "+");
            if (shifted.empty()) return ExprValue("timestamp", "", true);
            return ExprValue("timestamp", shifted, false);
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
        throw std::runtime_error("operator is not unique: unknown " + op +
                                 " unknown (SQLSTATE 42725)");
    }
    auto strictIntErr = [](const std::string& v) {
        throw std::runtime_error(std::string("invalid input syntax for type integer: ") +
                                 std::string(1, 34) + v + std::string(1, 34) +
                                 " (SQLSTATE 22P02)");
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
               lowered == "float8" || lowered == "real" ||
               lowered == "float4";
    };
    const bool floatingPower =
        op == "^" &&
        (isFloatingTyped(l.typeName) || isFloatingTyped(r.typeName));
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
                if (nr->sign() == 0)
                    throw std::runtime_error("division by zero (SQLSTATE 22012)");
                res = *nl / *nr;
            }
            else if (op == "%") {
                if (nr->sign() == 0)
                    throw std::runtime_error("division by zero (SQLSTATE 22012)");
                if (!nl->isFinite() || !nr->isFinite())
                    return ExprValue("numeric", "", true);
                const Numeric quotient = *nl / *nr;
                std::string integralQuotient = quotient.toString();
                const size_t decimalPoint = integralQuotient.find('.');
                if (decimalPoint != std::string::npos)
                    integralQuotient.resize(decimalPoint);
                if (integralQuotient.empty() || integralQuotient == "-")
                    integralQuotient += "0";
                res = *nl - Numeric(integralQuotient) * *nr;
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
                       toLower(l.typeName) == "double precision" ||
                       toLower(l.typeName) == "real" ||
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
                if (nr2->sign() == 0)
                    throw std::runtime_error("division by zero (SQLSTATE 22012)");
                res = *nl2 / *nr2;
            } else return ExprValue("numeric", "", true);
            return ExprValue("numeric", res.toString(), false);
        }
    }

    if (floatResult) {
        double a = l.asDouble(), b = r.asDouble(), res = 0;
        if (op == "+") res = a + b;
        else if (op == "-") res = a - b;
        else if (op == "*") res = a * b;
        else if (op == "/") {
            if (b == 0) throw std::runtime_error("division by zero (SQLSTATE 22012)");
            res = a / b;
        }
        else if (op == "%") {
            if (b == 0)
                throw std::runtime_error(
                    "division by zero (SQLSTATE 22012)");
            res = std::fmod(a, b);
        }
        else if (op == "^") {
            if ((a == 0 && b < 0) ||
                (a < 0 && std::isfinite(b) && std::trunc(b) != b)) {
                throw std::runtime_error(
                    "invalid argument for power function (SQLSTATE 2201F)");
            }
            res = std::pow(a, b);
            if (std::isnan(res) && std::isfinite(a) && std::isfinite(b)) {
                throw std::runtime_error(
                    "invalid argument for power function (SQLSTATE 2201F)");
            }
            if (std::isinf(res) && std::isfinite(a) && std::isfinite(b)) {
                throw std::runtime_error(
                    "numeric value out of range (SQLSTATE 22003)");
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
        throw std::runtime_error(std::string("invalid input syntax for type integer: \"") + l.value + "\" (SQLSTATE 22P02)");
    if (!cleanInt(r.value))
        throw std::runtime_error(std::string("invalid input syntax for type integer: \"") + r.value + "\" (SQLSTATE 22P02)");
    auto integerOutOfRange = []() {
        throw std::runtime_error("integer out of range (SQLSTATE 22003)");
    };
    long long a = 0;
    long long b = 0;
    if (!parseInt64Exact(l.value, a) || !parseInt64Exact(r.value, b))
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
        if (wide < std::numeric_limits<int64_t>::lowest() ||
            wide > std::numeric_limits<int64_t>::max()) {
            integerOutOfRange();
        }
        res = static_cast<int64_t>(wide);
    }
    else if (op == "/") {
        if (b == 0) throw std::runtime_error("division by zero (SQLSTATE 22012)");
        if (a == std::numeric_limits<int64_t>::lowest() && b == -1)
            integerOutOfRange();
        res = a / b;
    }
    else if (op == "%") {
        if (b == 0)
            throw std::runtime_error("division by zero (SQLSTATE 22012)");
        res = (a == std::numeric_limits<int64_t>::lowest() && b == -1)
            ? 0 : a % b;
    }
    return ExprValue("integer", std::to_string(res), false);
}

// ----------------------------------------------------------------------------
// LIKE / SIMILAR TO
// ----------------------------------------------------------------------------

bool ExprEvaluator::likeMatch(const std::string& text, const std::string& pattern) {
    size_t ti = 0, pi = 0, star = std::string::npos, match = 0;
    while (ti < text.size()) {
        if (pi < pattern.size() && pattern[pi] == '%') {
            // wildcard takes precedence over a literal '%' in the TEXT
            star = pi++;
            match = ti;
        } else if (pi < pattern.size() && (pattern[pi] == '_' || pattern[pi] == text[ti])) {
            ++ti; ++pi;
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
    // PG SIMILAR TO: the pattern is a SQL-similar pattern where % and _ are
    // wildcards and the rest is POSIX-regex, matched against the WHOLE string.
    // Translate % -> .*, _ -> . (backslash escapes preserved), anchor ^(...)$.
    std::string tr;
    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (c == 92 && i + 1 < pattern.size()) { tr += c; tr += pattern[++i]; continue; }
        if (c == 37) { tr += ".*"; continue; }
        if (c == 95) { tr += 46; continue; }
        tr += c;
    }
    std::string anchored = "^(" + tr + ")$";
    try {
        std::regex re(anchored, std::regex::ECMAScript);
        return std::regex_search(text, re);
    } catch (...) {
        return false;
    }
}

static bool likeMatchEscaped(const std::string& text, const std::string& pattern) {
    // likeMatch plus 0x01<literal> escape markers (see LIKE ESCAPE).
    size_t ti = 0, pi = 0, star = std::string::npos, match = 0;
    while (ti < text.size()) {
        if (pi + 1 < pattern.size() && pattern[pi] == 1) {
            if (pattern[pi + 1] != text[ti]) return false;
            ++ti; pi += 2;
        } else if (pi < pattern.size() && pattern[pi] == 37) {
            star = pi++; match = ti;
        } else if (pi < pattern.size() &&
                   (pattern[pi] == 95 || pattern[pi] == text[ti])) {
            ++ti; ++pi;
        } else if (star != std::string::npos) {
            pi = star + 1; ti = ++match;
        } else {
            return false;
        }
    }
    while (pi < pattern.size() && pattern[pi] == 37) ++pi;
    return pi == pattern.size();
}
static bool similarToMatchEscape(const std::string& text, const std::string& pattern, char esc) {
    // Like similarToMatch but with an explicit SQL ESCAPE character:
    // esc followed by % or _ denotes the literal character.
    std::string tr;
    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (c == esc && i + 1 < pattern.size() &&
            (pattern[i + 1] == 37 || pattern[i + 1] == 95 || pattern[i + 1] == esc)) {
            char nxt = pattern[++i];
            tr += 91; tr += nxt; tr += 93;  // character class [x]
            continue;
        }
        if (c == 92 && i + 1 < pattern.size()) { tr += c; tr += pattern[++i]; continue; }
        if (c == 37) { tr += ".*"; continue; }
        if (c == 95) { tr += 46; continue; }
        tr += c;
    }
    try {
        std::regex re("^(" + tr + ")$", std::regex::ECMAScript);
        return std::regex_search(text, re);
    } catch (...) {
        return false;
    }
}

// ----------------------------------------------------------------------------
// Binary operators
// ----------------------------------------------------------------------------

static ExprValue tsMatch(const std::string& vecText, const std::string& query);

ExprValue ExprEvaluator::evalBinaryOp(const BinaryOpExpr* e, const RowContext& ctx) const {
    if (!e || !e->left || !e->right) return ExprValue{};
    std::string op = toLower(e->op);

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
    ExprValue r = eval(e->right.get(), ctx);

    // Comparison
    // IS [NOT] DISTINCT FROM: equality that treats NULLs as comparable
    // (never returns NULL).
    if (op == "is distinct from" || op == "is not distinct from") {
        bool distinct;
        if (l.isNull || r.isNull) {
            distinct = (l.isNull != r.isNull);
        } else {
            distinct = applyComparison("<>", l, r).asBool();
        }
        if (op == "is not distinct from") distinct = !distinct;
        return ExprValue("boolean", distinct ? "t" : "f", false);
    }
    // POSIX-ish regex match operators: ~, ~* (case-insensitive), !~, !~*.
    if (op == "~" || op == "~*" || op == "!~" || op == "!~*") {
        if (l.isNull || r.isNull) return ExprValue("boolean", "", true);
        bool m = false;
        try {
            std::regex re(r.value);
            m = std::regex_search(l.value, re);
        } catch (const std::regex_error&) {
            return ExprValue("boolean", "f", false);
        }
        if (op == "~*" || op == "!~*") {
            try {
                std::regex re2(r.value, std::regex::icase);
                m = std::regex_search(l.value, re2);
            } catch (const std::regex_error&) {
                return ExprValue("boolean", "f", false);
            }
        }
        if (op == "!~" || op == "!~*") m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }
    static const std::set<std::string> cmpOps = {"=", "<>", "!=", "<", ">", "<=", ">="};
    if (cmpOps.count(op)) return applyComparison(op, l, r);

    // Arithmetic
    static const std::set<std::string> arithOps = {"+", "-", "*", "/", "%", "^"};
    if (arithOps.count(op)) return applyArithmetic(op, l, r);

    // String concatenation; SQL arrays concatenate as arrays (PG ||).
    if (op == "||") {
        if (l.isNull || r.isNull) return ExprValue("text", "", true);
        auto isArrayTxt = [](const std::string& v) {
            std::string s = trimStr(v);
            return s.size() >= 2 && s.front() == 0x7B && s.back() == 0x7D;
        };
        if (isArrayTxt(l.value) && isArrayTxt(r.value)) {
            std::string a = trimStr(l.value), b = trimStr(r.value);
            std::string inner = a.substr(1, a.size() - 2);
            std::string add = b.substr(1, b.size() - 2);
            std::string out = inner;
            if (!add.empty()) out += (inner.empty() ? "" : ",") + add;
            return ExprValue("text", std::string(1, 0x7B) + out + std::string(1, 0x7D), false);
        }
        return ExprValue("text", l.value + r.value, false);
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
            for (size_t i = 1; i + 1 < t.size(); ++i) {
                if (t[i] == '\\' && i + 2 < t.size()) o.push_back(t[++i]);
                else o.push_back(t[i]);
            }
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
                    std::string ku = (k2.size() >= 2 && k2.front() == '"' && k2.back() == '"')
                                         ? k2.substr(1, k2.size() - 2) : k2;
                    std::string want = trimStr(m.substr(colon + 1));
                    std::string got;
                    if (!jsonStep(ct, ku, got)) return false;
                    if (!contains(trimStr(got), want)) return false;
                }
                return true;
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
            // Scalars (or mixed shapes) compare by trimmed text.
            return ct == it;
        };
        const bool res = contains(cont.value, item.value);
        return ExprValue("boolean", res ? "t" : "f", false);
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

    // Array subscript (expr[idx]) — SQL array {e1,e2,...} text form.
    if (op == "[]") {
        if (l.isNull || r.isNull) return ExprValue("unknown", "", true);
        std::vector<std::string> elems;
        if (!splitSqlArrayElems(l.value, elems)) return ExprValue("unknown", "", true);
        long idx = 0;
        try {
            size_t cpos = 0;
            idx = std::stol(r.value, &cpos);
            if (cpos != r.value.size()) return ExprValue("unknown", "", true);
        } catch (...) {
            return ExprValue("unknown", "", true);
        }
        // PostgreSQL arrays are 1-based; negative = from the end.
        if (idx < 0) idx = static_cast<long>(elems.size()) + idx + 1;
        if (idx < 1 || idx > static_cast<long>(elems.size()))
            return ExprValue("unknown", "", true); // out of range -> NULL (PG)
        return ExprValue("text", elems[static_cast<size_t>(idx - 1)], false);
    }

    // Array slice (expr[lower:upper]) — PostgreSQL inclusive bounds,
    // 1-based; empty side = open bound; result is an array literal.
    if (op == "[:]") {
        if (l.isNull) return ExprValue("text", "", true);
        std::vector<std::string> elems;
        if (!splitSqlArrayElems(l.value, elems)) return ExprValue("unknown", "", true);
        // Bounds literal "lower:upper" (either side may be empty).
        const std::string& b = r.value;
        size_t colon = b.find(':');
        std::string loS = colon == std::string::npos ? b : b.substr(0, colon);
        std::string hiS = colon == std::string::npos ? "" : b.substr(colon + 1);
        long n = static_cast<long>(elems.size());
        long lo = 1, hi = n;
        auto parseBound = [](const std::string& s, long def, long nElem) -> long {
            if (s.empty()) return def;
            try {
                size_t cp = 0;
                long v = std::stol(s, &cp);
                if (cp != s.size()) return def;
                if (v < 0) v = nElem + v + 1; // negative = from the end
                return v;
            } catch (...) {
                return def;
            }
        };
        lo = parseBound(loS, 1, n);
        hi = parseBound(hiS, n, n);
        if (lo < 1) lo = 1;
        if (hi > n) hi = n;
        std::string out = "{";
        for (long i = lo; i <= hi; ++i) {
            if (i > lo) out += ",";
            out += elems[static_cast<size_t>(i - 1)];
        }
        out += "}";
        return ExprValue("text", out, false);
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

enum class IntegerCastTarget { SmallInt, Integer, BigInt };

static const char* integerCastTypeName(IntegerCastTarget target) {
    switch (target) {
        case IntegerCastTarget::SmallInt: return "smallint";
        case IntegerCastTarget::Integer:  return "integer";
        case IntegerCastTarget::BigInt:   return "bigint";
    }
    return "integer";
}

[[noreturn]] static void throwIntegerCastRangeError(IntegerCastTarget target) {
    throw std::runtime_error(
        std::string(integerCastTypeName(target)) +
        " out of range (SQLSTATE 22003)");
}

[[noreturn]] static void throwIntegerCastSyntaxError(
    IntegerCastTarget target, const std::string& value) {
    throw std::runtime_error(
        "invalid input syntax for type " +
        std::string(integerCastTypeName(target)) + ": '" + value +
        "' (SQLSTATE 22P02)");
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

    if (sourceType == "boolean" || sourceType == "bool") {
        if (target != IntegerCastTarget::Integer) {
            throw std::runtime_error(
                "cannot cast type boolean to " +
                std::string(integerCastTypeName(target)) +
                " (SQLSTATE 42846)");
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

[[noreturn]] static void throwFloatingCastSyntaxError(
    const std::string& targetType, const std::string& value) {
    throw std::runtime_error(
        "invalid input syntax for type " + targetType + ": '" + value +
        "' (SQLSTATE 22P02)");
}

[[noreturn]] static void throwFloatingCastRangeError(
    const std::string& targetType) {
    throw std::runtime_error(
        "value out of range for type " + targetType +
        " (SQLSTATE 22003)");
}

static void rejectBooleanFloatingCast(const ExprValue& value,
                                      const std::string& targetType) {
    const std::string sourceType = toLower(value.typeName);
    if (sourceType == "boolean" || sourceType == "bool") {
        throw std::runtime_error(
            "cannot cast type boolean to " + targetType +
            " (SQLSTATE 42846)");
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
    throw std::runtime_error(
        "invalid input syntax for type numeric: '" + value +
        "' (SQLSTATE 22P02)");
}

[[noreturn]] static void throwNumericCastOverflow() {
    throw std::runtime_error(
        "numeric field overflow (SQLSTATE 22003)");
}

[[noreturn]] static void throwNumericTypmodError(
    const std::string& message) {
    throw std::runtime_error(message + " (SQLSTATE 22023)");
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
        throw std::runtime_error(
            "cannot cast type boolean to numeric (SQLSTATE 42846)");
    }
    std::optional<Numeric> numeric;
    try {
        numeric.emplace(value.value);
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

static std::optional<bool> parseBooleanCastText(const std::string& input) {
    const std::string text = toLower(trimStr(input));
    if (text == "1") return true;
    if (text == "0") return false;
    if (text.empty()) return std::nullopt;

    const std::pair<const char*, bool> names[] = {
        {"true", true}, {"false", false}, {"yes", true},
        {"no", false}, {"on", true}, {"off", false}};
    std::optional<bool> result;
    size_t matches = 0;
    for (const auto& name : names) {
        const std::string candidate = name.first;
        if (text.size() <= candidate.size() &&
            candidate.compare(0, text.size(), text) == 0) {
            result = name.second;
            ++matches;
        }
    }
    return matches == 1 ? result : std::nullopt;
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
            throw std::runtime_error(
                "invalid input syntax for type integer: '" +
                trimStr(value.value) + "' (SQLSTATE 22P02)");
        }
        return ExprValue("boolean", integer == 0 ? "f" : "t", false);
    }

    if (!booleanSource && !textSource) {
        throw std::runtime_error(
            "cannot cast type " + sourceType +
            " to boolean (SQLSTATE 42846)");
    }
    const auto parsed = parseBooleanCastText(value.value);
    if (!parsed) {
        throw std::runtime_error(
            "invalid input syntax for type boolean: '" +
            trimStr(value.value) + "' (SQLSTATE 22P02)");
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
        throw std::runtime_error(
            "date/time field value out of range: '" + value +
            "' (SQLSTATE 22008)");
    }
    throw std::runtime_error(
        "invalid input syntax for type " + targetType + ": '" + value +
        "' (SQLSTATE 22007)");
}

[[noreturn]] static void throwUnsupportedTemporalCast(
    const std::string& sourceType, const std::string& targetType) {
    throw std::runtime_error(
        "cannot cast type " + sourceType + " to " + targetType +
        " (SQLSTATE 42846)");
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

    const int64_t timestamp = parseTimestampToSeconds(text);
    if (timestamp == 0) throwTemporalCastError(targetType, text);
    const std::string formatted = formatTimestampSeconds(timestamp);
    if (formatted.empty()) throwTemporalCastError(targetType, text);
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
    throw std::runtime_error(message + " (SQLSTATE 22023)");
}

static CharacterCastSpec parseCharacterCastSpec(const std::string& target) {
    CharacterCastSpec spec;
    if (target == "text") {
        spec.kind = CharacterCastKind::Text;
        return spec;
    }
    if (target.rfind("text(", 0) == 0) {
        throw std::runtime_error(
            "type modifier is not allowed for type text (SQLSTATE 42601)");
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
        std::string fullT = e->typeName;
        if (!e->typeMods.empty()) {
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
        }
        return evalCast(nullptr, ctx, v, fullT);
    }
    return ExprValue{};
}

// Helper overload used by BinaryOpExpr "::"
ExprValue ExprEvaluator::evalCast(const Expr*, const RowContext&,
                                  const ExprValue& v, const std::string& targetTypeName) const {
    if (v.isNull) return ExprValue(targetTypeName, "", true);
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
                target = base + "(" + modifiers + ")";
            }
        }
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
    const CharacterCastSpec characterSpec = parseCharacterCastSpec(target);
    if (characterSpec.kind != CharacterCastKind::None)
        return castToCharacter(v, characterSpec);
    if (target == "date") return castToDate(v);
    if (target == "timestamp" || target == "timestamp without time zone")
        return castToTimestamp(v, "timestamp");
    if (target == "timestamptz" || target == "timestamp with time zone")
        return castToTimestamp(v, "timestamptz");

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

ExprValue ExprEvaluator::evalFunctionCall(const FunctionCallExpr* e, const RowContext& ctx) const {
    if (!e) return ExprValue{};
    std::string name = toLower(e->funcName);

    std::vector<ExprValue> args;
    for (const auto& a : e->args) args.push_back(eval(a.get(), ctx));

    // SIMILAR TO ... ESCAPE / NOT SIMILAR TO ... ESCAPE (parser wraps the
    // three-operand form into a FunctionCallExpr, mirroring LIKE ESCAPE).
    // LIKE ... ESCAPE / NOT LIKE ... ESCAPE (parser wraps the three-operand
    // form into a FunctionCallExpr).  esc + wildcard denotes the literal
    // character; the pattern is normalized into 0x01<char> markers that
    // likeMatchEscaped handles as exact literals.
    if (name == "like escape" || name == "not like escape" ||
        name == "ilike escape" || name == "not ilike escape") {
        if (args.size() < 3 || args[0].isNull || args[1].isNull) {
            return ExprValue("boolean", "", true);
        }
        char esc = (!args[2].isNull && !args[2].value.empty()) ? args[2].value[0] : 92;
        std::string pat;
        const std::string& src = args[1].value;
        for (size_t i = 0; i < src.size(); ++i) {
            if (src[i] == esc && i + 1 < src.size() &&
                (src[i + 1] == 37 || src[i + 1] == 95 || src[i + 1] == esc)) {
                pat += static_cast<char>(1);
                pat += src[++i];
                continue;
            }
            pat += src[i];
        }
        bool m = likeMatchEscaped(args[0].value, pat);
        if (name.rfind("not ", 0) == 0) m = !m;
        return ExprValue("boolean", m ? "t" : "f", false);
    }

    if (name == "similar to escape" || name == "not similar to escape") {
        if (args.size() < 3 || args[0].isNull || args[1].isNull) {
            return ExprValue("boolean", "", true);
        }
        char esc = (!args[2].isNull && !args[2].value.empty()) ? args[2].value[0] : 92;
        bool m = similarToMatchEscape(args[0].value, args[1].value, esc);
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
    if (name == "between" || name == "not between") {
        if (args.size() != 3) return ExprValue("boolean", "f", false);
        ExprValue v = args[0], lo = args[1], hi = args[2];
        bool r = !v.isNull && !lo.isNull && !hi.isNull &&
                 compareValues(v, lo) >= 0 && compareValues(v, hi) <= 0;
        if (name == "not between") r = !r;
        return ExprValue("boolean", r ? "t" : "f", false);
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
    if (args.empty() || args[0].isNull)
        return ExprValue("text", "", true);
    const std::string input = textArgumentValue(args[0]);
    if (args.size() < 2) return ExprValue("text", input, false);
    if (args[1].isNull || (args.size() >= 3 && args[2].isNull))
        return ExprValue("text", "", true);

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

    const size_t total = utf8CharCount(input);
    const __int128 requestedEnd = hasLength
        ? static_cast<__int128>(from) + length
        : static_cast<__int128>(total) + 1;
    const __int128 requestedStart = std::max<__int128>(from, 1);
    if (requestedStart > static_cast<__int128>(total))
        return ExprValue("text", "", false);

    const size_t beginCharacter =
        static_cast<size_t>(requestedStart - 1);
    __int128 endCharacterWide = requestedEnd - 1;
    if (endCharacterWide < static_cast<__int128>(beginCharacter))
        endCharacterWide = beginCharacter;
    if (endCharacterWide > static_cast<__int128>(total))
        endCharacterWide = total;
    const size_t endCharacter = static_cast<size_t>(endCharacterWide);
    const size_t beginByte = utf8ByteAt(input, beginCharacter);
    const size_t endByte = utf8ByteAt(input, endCharacter);
    return ExprValue(
        "text", input.substr(beginByte, endByte - beginByte), false);
}

static ExprValue evaluateTextPad(const std::vector<ExprValue>& args,
                                 bool padLeft) {
    if (args.size() < 2 || args[0].isNull || args[1].isNull ||
        (args.size() >= 3 && args[2].isNull)) {
        return ExprValue("text", "", true);
    }
    long long requestedLength = 0;
    if (!parseInt64Exact(args[1].value, requestedLength))
        return ExprValue("text", "", true);
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

    std::string padding;
    const size_t needed = targetLength - inputLength;
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
static std::string formatNumeric(double val, const std::string& fmtIn) {
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
    bool hasL = fmt.find('L') != std::string::npos;
    bool hasG = fmt.find('G') != std::string::npos;
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
    if (hasV) {
        // V shifts the decimal point: digits after V are scale shifts.
        size_t vPos = fmt.find('V');
        int shift = 0;
        for (size_t i = vPos + 1; i < fmt.size(); ++i)
            if (fmt[i] == '9' || fmt[i] == '0') ++shift;
        double av2 = neg ? -val : val;
        for (int k2 = 0; k2 < shift; ++k2) av2 *= 10.0;
        val = av2; neg = false; fracDigits = 0; dot = std::string::npos;
        // intPlaces spans digits on both sides of V.
        intPlaces = 0; zeroPad = false;
        for (size_t i = 0; i < fmt.size(); ++i)
            if (fmt[i] == '9') ++intPlaces;
            else if (fmt[i] == '0') { ++intPlaces; zeroPad = true; }
    }
    char numbuf[64];
    std::snprintf(numbuf, sizeof numbuf, "%.*f", fracDigits, neg ? -val : val);
    std::string s = numbuf, ip = s, fp;
    size_t sp = s.find('.');
    if (sp != std::string::npos) { ip = s.substr(0, sp); fp = s.substr(sp + 1); }
    if (zeroPad && static_cast<int>(ip.size()) < intPlaces)
        ip = std::string(intPlaces - ip.size(), '0') + ip;
    else if (!zeroPad && !fm) {
        // PG: unused leading 9-positions render as blanks; an
        // all-zero integer part with no 0-pattern renders blank.
        if (ip == "0") ip = std::string(intPlaces, ' ');
        else if (static_cast<int>(ip.size()) < intPlaces)
            ip = std::string(intPlaces - ip.size(), ' ') + ip;
    }
    // PG overflow: more integer digits than 9/0 positions render #.
    if (!ip.empty() && ip.find_first_not_of(" 0123456789") == std::string::npos &&
        static_cast<int>(ip.size()) > intPlaces) {
        ip = std::string(intPlaces, '#');
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
        out = std::string("$") + (fm ? "" : " ") + ip;
    } else {
        out = neg ? "-" : (fm ? "" : " ");
        out += ip;
    }
    if (fracDigits > 0) out += "." + fp;
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
        if (a.size() != 4) return ExprValue("boolean", "", true);
        for (const auto& v : a) if (v.isNull) return ExprValue("boolean", "", true);
        size_t s1 = 0, e1 = 1, s2 = 2, e2 = 3;
        if (a[s1].value > a[e1].value) std::swap(s1, e1);
        if (a[s2].value > a[e2].value) std::swap(s2, e2);
        bool p1 = a[s1].value == a[e1].value;
        bool p2 = a[s2].value == a[e2].value;
        bool m;
        if (p1 && p2) m = a[s1].value == a[s2].value;
        else if (p1)  m = a[s2].value <= a[s1].value && a[s1].value <= a[e2].value;
        else if (p2)  m = a[s1].value <= a[s2].value && a[s2].value <= a[e1].value;
        else          m = a[s1].value < a[e2].value && a[s2].value < a[e1].value;
        return ExprValue("boolean", m ? "t" : "f", false);
    };

    functions_["timezone"] = [](const std::vector<ExprValue>& a) {
        if (a.size() != 2 || a[0].isNull || a[1].isNull) return ExprValue("timestamp", "", true);
        long long offMin = 0;
        if (!parseTimeZoneOffset(a[0].value, offMin)) return ExprValue("timestamp", "", true);
        const std::string inTn = toLower(a[1].typeName);
        IntervalParts shift;
        shift.micros = offMin * 60000000LL;
        std::string out = timestampShift(a[1].value, shift, true);
        if (out.empty()) return ExprValue("timestamp", "", true);
        // PG: timezone(zone, timestamptz) -> timestamp (local wall time);
        //     timezone(zone, timestamp)  -> timestamptz (attach the zone
        //     offset to the result render).  Untyped literals keep the
        //     plain render (PG misc behavior).
        if (inTn.find("timestamptz") != std::string::npos ||
            inTn != "timestamp") {
            return ExprValue("timestamp", out, false);
        }
        // PG renders whole-hour offsets without minutes (+00, +09).
        return ExprValue(
            "timestamptz", out + formatTimeZoneOffset(offMin), false);
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
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
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
            if (n) return ExprValue("numeric", (n->sign() < 0 ? -(*n) : *n).toString(), false);
        }
        double v = std::abs(a[0].asDouble());
        return ExprValue("numeric", std::to_string(v), false);
    };
    functions_["length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        const std::string type = toLower(a[0].typeName);
        const bool byteLength = type == "bytea" || type == "binary" ||
                                type == "varbinary";
        const size_t length = byteLength
            ? a[0].value.size()
            : utf8CharCount(a[0].value.substr(
                  0, logicalCharacterByteLength(a[0])));
        return ExprValue("integer", std::to_string(length), false);
    };
    functions_["lower"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        // Overload: lower(anyrange) returns the lower bound (NULL if unbounded).
        if (typeIsRange(a[0].typeName)) {
            RangeParts r = parseRangeLiteral(a[0].value);
            if (!r.valid || r.empty || r.loInf) return ExprValue(a[0].typeName, "", true);
            return ExprValue("numeric", r.lo, false);
        }
        return ExprValue("text", toLower(textArgumentValue(a[0])), false);
    };
    functions_["upper"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        // Overload: upper(anyrange) returns the upper bound (NULL if unbounded).
        if (typeIsRange(a[0].typeName)) {
            RangeParts r = parseRangeLiteral(a[0].value);
            if (!r.valid || r.empty || r.hiInf) return ExprValue(a[0].typeName, "", true);
            return ExprValue("numeric", r.hi, false);
        }
        std::string s = textArgumentValue(a[0]);
        for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        return ExprValue("text", s, false);
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
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("numeric", "", true);
        }
        if (isNumericTypeName(a[0].typeName)) {
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
        if (v == std::floor(v) && std::fabs(v) < 1e15)
            return ExprValue("numeric", std::to_string(static_cast<long long>(v)), false);
        return ExprValue("numeric", std::to_string(v), false);
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
        double v = fn(a[0].asDouble());
        if (v == std::floor(v) && std::fabs(v) < 1e15)
            return ExprValue(outType, std::to_string(static_cast<long long>(v)), false);
        return ExprValue(outType, std::to_string(v), false);
    };
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
        const long double value = a[0].asDouble();
        requireLogarithmArgument(value);
        return numericFixed(logl(value), 16);
    };
    functions_["sqrt"] = [numericFixed, argHasDot](const auto& a) {
        if (a.empty() || a[0].isNull) return ExprValue("numeric", "", true);
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
            long double b = a[0].asDouble(), x = a[1].asDouble();
            requireLogarithmArgument(b, false);
            requireLogarithmArgument(x);
            return numericFixed(logl(x) / logl(b), 16);
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
        const long double value = a[0].asDouble();
        requireLogarithmArgument(value);
        long double v = log10l(value);
        if (!argHasDot(a) && v == floorl(v))
            return ExprValue("numeric", std::to_string(static_cast<long long>(v)), false);
        return numericFixed(v, 16);
    };
    functions_["cbrt"]  = [float8Unary](const auto& a) { return float8Unary(a, std::cbrt); };
    functions_["ceil"]  = [unaryMath](const auto& a) { return unaryMath(a, std::ceil); };
    functions_["floor"] = [unaryMath](const auto& a) { return unaryMath(a, std::floor); };
    functions_["trunc"] = [scaleFloatingDecimal,
                            float8Text](const std::vector<ExprValue>& a) {
        // trunc(x) truncates toward zero; trunc(x, n) keeps n decimal places.
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("double precision", "", true);
        }
        if (isNumericTypeName(a[0].typeName)) {
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
            std::string ts2 = std::to_string(
                scaleFloatingDecimal(v, n, false));
            while (!ts2.empty() && ts2.back() == '0') ts2.pop_back();
            if (!ts2.empty() && ts2.back() == '.') ts2.pop_back();
            return ExprValue("numeric", ts2, false);
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("double precision", "", true);
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
                    if (!r.isFinite()) {
                        throw std::runtime_error(
                            "numeric value out of range (SQLSTATE 22003)");
                    }
                    if (isIntVal(a[0]) && isIntVal(a[1])) {
                        std::string rs = r.toString();
                        if (rs.find('.') == std::string::npos)
                            return ExprValue("double precision", rs, false);
                    }
                    long double rl =
                        std::strtold(r.toString().c_str(), nullptr);
                    return ExprValue(
                        "double precision",
                        r.withScale(displayScale(rl)).toString(), false);
                } catch (const std::invalid_argument&) {
                    throw std::runtime_error(
                        "numeric value out of range (SQLSTATE 22003)");
                }
            }
        }
        // PG numeric power computes exp/ln in extended precision; long double matches its 16-digit output.
        long double lv = powl(a[0].asDouble(), a[1].asDouble());
        if (!std::isfinite(lv)) {
            throw std::runtime_error(
                "numeric value out of range (SQLSTATE 22003)");
        }
        double v = static_cast<double>(lv);
        if (isIntVal(a[0]) && isIntVal(a[1]) && v == std::floor(v) && std::fabs(v) < 1e15)
            return ExprValue("double precision", std::to_string(static_cast<long long>(v)), false);
        std::ostringstream out;
        out << std::fixed << std::setprecision(displayScale(lv)) << lv;
        return ExprValue("double precision", out.str(), false);
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
            if (!left || !right || !left->isFinite() || !right->isFinite())
                return ExprValue("numeric", "", true);
            if (right->sign() == 0)
                throw std::runtime_error(
                    "division by zero (SQLSTATE 22012)");
            try {
                const Numeric quotient = *left / *right;
                std::string integralQuotient = quotient.toString();
                const size_t decimalPoint = integralQuotient.find('.');
                if (decimalPoint != std::string::npos)
                    integralQuotient.resize(decimalPoint);
                if (integralQuotient.empty() || integralQuotient == "-")
                    integralQuotient += '0';
                Numeric remainder =
                    *left - Numeric(integralQuotient) * *right;
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
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        double v = a[0].asDouble();
        return ExprValue("integer", std::to_string(v > 0 ? 1 : (v < 0 ? -1 : 0)), false);
    };
    functions_["pi"] = [float8Text](const std::vector<ExprValue>&) {
        return ExprValue("double precision", float8Text(std::atan(1.0) * 4.0), false);
    };
    functions_["random"] = [](const std::vector<ExprValue>&) {
        return ExprValue("double precision", std::to_string(static_cast<double>(std::rand()) / RAND_MAX), false);
    };
    // pow — alias of power; ceiling — alias of ceil
    functions_["pow"] = functions_["power"];
    functions_["ceiling"] = [unaryMath](const auto& a) { return unaryMath(a, std::ceil); };
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
    functions_["gcd"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bigint", "", true);
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
        return ExprValue("bigint", std::to_string(x), false);
    };
    functions_["lcm"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bigint", "", true);
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
        if (x == 0 || y == 0) return ExprValue("bigint", "0", false);
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
            "bigint", std::to_string(static_cast<uint64_t>(result)), false);
    };
    // div(y, x) — integer quotient of y / x, truncated toward zero
    functions_["div"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("numeric", "", true);
        const auto dividend = tryParseNumeric(a[0].value);
        const auto divisor = tryParseNumeric(a[1].value);
        if (!dividend || !divisor || !divisor->isFinite())
            return ExprValue("numeric", "", true);
        if (divisor->sign() == 0)
            throw std::runtime_error(
                "division by zero (SQLSTATE 22012)");

        const Numeric quotient = *dividend / *divisor;
        std::string result = quotient.toString();
        if (quotient.isFinite()) {
            const size_t decimalPoint = result.find('.');
            if (decimalPoint != std::string::npos)
                result.resize(decimalPoint);
            if (result.empty() || result == "-") result += "0";
        }
        return ExprValue("numeric", result, false);
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
        int64_t count = a[3].asInt();
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
        std::string s, chars = " \t\n\r\f\v";
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
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        std::string chars = (a.size() >= 2 && !a[1].isNull)
            ? textArgumentValue(a[1]) : " \t\n\r\f\v";
        return ExprValue(
            "text", trimUtf8Characters(s, chars, true, false), false);
    };
    functions_["rtrim"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        std::string chars = (a.size() >= 2 && !a[1].isNull)
            ? textArgumentValue(a[1]) : " \t\n\r\f\v";
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
        int64_t n = a[1].asInt();
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
        int64_t n = a[1].asInt();
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
        int64_t n = a[1].asInt();
        if (n <= 0) return ExprValue("text", "", false);
        const std::string input = textArgumentValue(a[0]);
        std::string s;
        s.reserve(input.size() * static_cast<size_t>(n));
        for (int64_t i = 0; i < n; ++i) s += input;
        return ExprValue("text", s, false);
    };
    functions_["reverse"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
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
        return ExprValue("integer", std::to_string(a[0].value.size()), false);
    };
    functions_["bit_length"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
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
        if (a.empty() || a[0].isNull ||
            (a.size() >= 2 && a[1].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        std::string chars = (a.size() >= 2 && !a[1].isNull)
            ? textArgumentValue(a[1]) : " \t\n\r\f\v";
        return ExprValue(
            "text", trimUtf8Characters(s, chars, true, true), false);
    };
    // split_part(str, delim, n) — n-th field (1-based; negative counts from the end)
    functions_["split_part"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        const std::string s = textArgumentValue(a[0]);
        const std::string delim = textArgumentValue(a[1]);
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

    // to_timestamp(text, fmt): pattern parse like to_date plus HH24/MI/SS;
    // renders "YYYY-MM-DD HH:MM:SS+00" (UTC offset like PG).
    functions_["to_timestamp"] = [](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.empty() || a[0].isNull || (a.size() >= 2 && a[1].isNull))
            return ExprValue("timestamp", "", true);
        const std::string& s = a[0].value;
        const std::string fmt = a.size() >= 2
            ? a[1].value : "YYYY-MM-DD HH24:MI:SS";
        Date date;
        int32_t timeSeconds = 0;
        if (!parseTemporalFormatValue(s, fmt, date, timeSeconds))
            return ExprValue("timestamp", "", true);
        return ExprValue("timestamp",
                         str(date) + " " + formatTimeSeconds(timeSeconds) +
                             "+00",
                         false);
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
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull ||
            (a.size() >= 4 && a[3].isNull)) {
            return ExprValue("text", "", true);
        }
        const std::string s = textArgumentValue(a[0]);
        const std::string repl = textArgumentValue(a[1]);
        long long start = 0;
        if (!parseInt64Exact(a[2].value, start))
            return ExprValue("text", "", true);
        long long count = 0;
        if (a.size() >= 4) {
            if (!parseInt64Exact(a[3].value, count))
                return ExprValue("text", "", true);
        } else {
            count = static_cast<long long>(utf8CharCount(repl));
        }
        if (start < 1) {
            throw std::runtime_error(
                "negative substring length not allowed (SQLSTATE 22011)");
        }
        const size_t characters = utf8CharCount(s);
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
        const size_t prefixByte = utf8ByteAt(s, prefixCharacters);
        const size_t suffixByte = utf8ByteAt(s, suffixCharacter);
        std::string out = s.substr(0, prefixByte) + repl + s.substr(suffixByte);
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
    // regexp_matches(text, pattern[, flags]) — PG set-returning form used
    // as a scalar: first match as a text array; capture groups when present.
    functions_["regexp_matches"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("text", "", true);
        auto fl = std::regex::ECMAScript;
        if (a.size() >= 3 && !a[2].isNull) {
            for (char c : textArgumentValue(a[2])) {
                if (c == 0x69) fl |= std::regex::icase;  // i
            }
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
            return ExprValue("text", "", true);
        }
    };

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
        throw std::runtime_error(
            "unrecognized encoding: \"" + a[1].value +
            "\" (SQLSTATE 22023)");
    };
    // decode(text, format) — inverse of encode, returns the raw bytes as text
    functions_["decode"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("bytea", "", true);
        std::string fmt = toLower(a[1].value);
        const std::string& text = a[0].value;
        std::string out;
        if (fmt == "hex") {
            if (!hexDecode(text, out))
                throw std::runtime_error(
                    "invalid hexadecimal data (SQLSTATE 22023)");
            return ExprValue("bytea", out, false);
        }
        if (fmt == "base64") {
            if (!base64Decode(text, out))
                throw std::runtime_error(
                    "invalid base64 data (SQLSTATE 22023)");
            return ExprValue("bytea", out, false);
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
            return ExprValue("bytea", out, false);
        }
        throw std::runtime_error(
            "unrecognized encoding: \"" + a[1].value +
            "\" (SQLSTATE 22023)");
    };

    // ------------------------------------------------------------------------
    // Array functions (operate on the '{...}' array literal text)
    // ------------------------------------------------------------------------
    auto parseArrayDimension = [](const std::vector<ExprValue>& arguments,
                                  long long& dimension) {
        return arguments.size() >= 2 && !arguments[1].isNull &&
               parseInt64Exact(arguments[1].value, dimension);
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
    // array_lower(arr, dim) — PG arrays default to lower bound 1
    functions_["array_lower"] =
        [parseArrayDimension, arrayExtent](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("integer", "", true);
        long long dim = 0;
        if (!parseArrayDimension(a, dim))
            return ExprValue("integer", "", true);
        return arrayExtent(a[0].value, dim)
            ? ExprValue("integer", "1", false)
            : ExprValue("integer", "", true);
    };
    // array_upper(arr, dim) — upper bound == length for the default lower bound 1
    functions_["array_upper"] =
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
    // array_dims(arr) — dimensions as PG text, e.g. [1:3].
    functions_["array_dims"] = [](const std::vector<ExprValue>& a) {
        if (a.empty() || a[0].isNull) return ExprValue("text", "", true);
        std::string current = a[0].value;
        std::string dimensions;
        while (true) {
            std::vector<std::string> elements;
            if (!parseArrayElements(current, elements) || elements.empty())
                break;
            dimensions +=
                "[1:" + std::to_string(elements.size()) + "]";
            current = elements.front();
        }
        if (dimensions.empty()) return ExprValue("text", "", true);
        return ExprValue("text", dimensions, false);
    };

    functions_["array_position"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull) return ExprValue("integer", "", true);
        std::vector<std::string> elems;
        if (!parseArrayElements(a[0].value, elems)) return ExprValue("integer", "", true);
        for (size_t i = 0; i < elems.size(); ++i) {
            const std::string token = trimStr(elems[i]);
            const bool quoted = token.size() >= 2 &&
                token.front() == '"' && token.back() == '"';
            const bool elementIsNull =
                !quoted && toLower(token) == "null";
            const std::string value = arrayElemUnquote(token);
            if ((a[1].isNull && elementIsNull) ||
                (!a[1].isNull && !elementIsNull &&
                 value == a[1].value)) {
                return ExprValue("integer", std::to_string(i + 1), false);
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

    // Operator forms: json -> key / json -> idx (rewritten from the
    // arrow syntax in expr_helper) share the path machinery.
    functions_["json_get"] = jsonExtractFn;
    functions_["json_get_text"] = jsonExtractTextFn;

    // ------------------------------------------------------------------------
    // Regular expression functions (std::regex, ECMAScript dialect)
    // ------------------------------------------------------------------------
    // regexp_replace(source, pattern, replacement [, flags]) — 'g' = replace all
    functions_["regexp_replace"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("text", "", true);
        std::string flags = (a.size() >= 4 && !a[3].isNull)
            ? textArgumentValue(a[3]) : "";
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) return ExprValue("text", "", true);
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("ARRAY", "", true);
        std::string flags = (a.size() >= 3 && !a[2].isNull)
            ? textArgumentValue(a[2]) : "";
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) return ExprValue("ARRAY", "", true);
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("ARRAY", "", true);
        std::string flags = (a.size() >= 3 && !a[2].isNull)
            ? textArgumentValue(a[2]) : "";
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) return ExprValue("ARRAY", "", true);
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
        if (a.size() < 2 || a[0].isNull || a[1].isNull) return ExprValue("integer", "", true);
        std::string flags = (a.size() >= 4 && !a[3].isNull)
            ? textArgumentValue(a[3]) : "";
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) return ExprValue("integer", "", true);
        const std::string s = textArgumentValue(a[0]);
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
        std::string flags = (a.size() >= 5 && !a[4].isNull)
            ? textArgumentValue(a[4]) : "";
        bool ok;
        std::regex re = buildRegex(textArgumentValue(a[1]), flags, ok);
        if (!ok) return ExprValue("text", "", true);
        const std::string s = textArgumentValue(a[0]);
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
            return ExprValue("numeric", "", true);
        }

        const int64_t timestamp = parseTimestampToSeconds(src);
        if (isInfiniteTimestamp(timestamp)) {
            const bool monotonicField =
                field == "epoch" || field == "year" ||
                field == "decade" || field == "century" ||
                field == "millennium";
            if (!monotonicField)
                return ExprValue("numeric", "", true);
            return ExprValue(
                "numeric",
                timestamp == TIMESTAMP_POSITIVE_INFINITY
                    ? "Infinity" : "-Infinity",
                false);
        }
        if (timestamp == 0)
            return ExprValue("numeric", "", true);
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
            // Timestamp: seconds since 1970-01-01 00:00:00, numeric
            // scale 6 (86400.000000).
            r = parseTimestampToSeconds(src) - parseTimestampToSeconds("1970-01-01 00:00:00");
            char eb[64];
            std::snprintf(eb, sizeof(eb), "%.6f", static_cast<double>(r));
            return ExprValue("numeric", eb, false);
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
        if (y < 1 || y > 9999 || m < 1 || m > 12 || d < 1 || d > 31)
            return ExprValue("date", "", true);
        Date date(static_cast<int>(y), static_cast<int>(m),
                  static_cast<int>(d));
        if (date.year == 0) return ExprValue("date", "", true);
        return ExprValue("date", str(date), false);
    };
    // make_time(h, m, s) -> 'HH:MM:SS'
    functions_["make_time"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 3 || a[0].isNull || a[1].isNull || a[2].isNull)
            return ExprValue("time", "", true);
        const int64_t h = a[0].asInt();
        const int64_t m = a[1].asInt();
        const std::string result = formatTimeFields(h, m, a[2].value);
        return ExprValue("time", result, result.empty());
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
        if (y < 1 || y > 9999 || mo < 1 || mo > 12 || d < 1 || d > 31 ||
            h < 0 || h > 23 || mi < 0 || mi > 59) {
            return ExprValue("timestamp", "", true);
        }
        Date date(static_cast<int>(y), static_cast<int>(mo),
                  static_cast<int>(d));
        if (date.year == 0) return ExprValue("timestamp", "", true);
        const std::string time = formatTimeFields(h, mi, a[5].value);
        if (time.empty()) return ExprValue("timestamp", "", true);
        return ExprValue("timestamp", str(date) + " " + time, false);
    };
    // date_trunc(field, source) -> truncate timestamp to the given precision
    functions_["date_trunc"] = [](const std::vector<ExprValue>& a) {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("timestamp", "", true);
        std::string field = toLower(a[0].value);
        const std::string& src = a[1].value;
        const int64_t parsedTimestamp = parseTimestampToSeconds(src);
        if (isInfiniteTimestamp(parsedTimestamp)) {
            return ExprValue("timestamp",
                             formatTimestampSeconds(parsedTimestamp), false);
        }
        if (parsedTimestamp == 0)
            return ExprValue("timestamp", "", true);
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
        char buf[40];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, se);
        // PG date_trunc over a date input promotes to timestamptz and
        // renders with the zone suffix (+00); timestamp input stays plain.
        std::string res = buf;
        if (toLower(a[1].typeName) == "date") res += "+00";
        return ExprValue("timestamp", res, false);
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
            char tb[64];
            if (tn) std::snprintf(tb, sizeof tb, "-%02lld:%02lld:%02lld", hh, mi, se);
            else std::snprintf(tb, sizeof tb, "%02lld:%02lld:%02lld", hh, mi, se);
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
    // age(ts, ts): PG calendar difference as interval.
    functions_["age"] = [intervalToTextPg](const std::vector<ExprValue>& a) -> ExprValue {
        if (a.size() < 2 || a[0].isNull || a[1].isNull)
            return ExprValue("interval", "", true);
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
                if (dot != std::string::npos)
                    fr = std::strtoll(v.c_str() + dot + 1, nullptr, 10);
            }
            us = ((h * 3600 + mi * 60 + se) * 1000000LL) + fr;
            return true;
        };
        long long y1 = 0, m1 = 0, d1 = 0, us1 = 0, y2 = 0, m2 = 0, d2 = 0, us2 = 0;
        if (!splitTs(a[0].value, y1, m1, d1, us1) ||
            !splitTs(a[1].value, y2, m2, d2, us2))
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
        bool any = g_engine.anyRowMatches(currentDB_, table, conds, &scanFailed);
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
        return ExprValue("text", formatNumeric(a[0].asDouble(), fmt), false);
    };

    // ------------------------------------------------------------------------
    // Sequence functions (delegate to the global StorageEngine)
    // ------------------------------------------------------------------------
    auto seqNameArg = [](const std::vector<ExprValue>& a) -> std::string {
        if (a.empty() || a[0].isNull) return "";
        std::string s = a[0].value;
        // Strip surrounding quotes if present.
        if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
            s = s.substr(1, s.size() - 2);
        }
        return s;
    };
    functions_["nextval"] = [this, seqNameArg](const std::vector<ExprValue>& a) -> ExprValue {
        std::string seq = seqNameArg(a);
        if (seq.empty() || currentDB_.empty()) return ExprValue("bigint", "", true);
        int64_t v = g_engine.nextval(currentDB_, seq);
        return ExprValue("bigint", std::to_string(v), false);
    };
    functions_["currval"] = [this, seqNameArg](const std::vector<ExprValue>& a) -> ExprValue {
        std::string seq = seqNameArg(a);
        if (seq.empty() || currentDB_.empty()) return ExprValue("bigint", "", true);
        int64_t v = g_engine.currval(currentDB_, seq);
        return ExprValue("bigint", std::to_string(v), false);
    };
    functions_["lastval"] = [this](const std::vector<ExprValue>&) -> ExprValue {
        if (currentDB_.empty()) return ExprValue("bigint", "", true);
        int64_t v = g_engine.lastval();
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
    volatility_["current_user"] = 's';
    volatility_["session_user"] = 's';
    volatility_["nextval"] = 'v';
    volatility_["currval"] = 'v';
    volatility_["lastval"] = 'v';
    volatility_["random"] = 'v';
}

} // namespace dbms
