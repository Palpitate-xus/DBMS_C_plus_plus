#include "utils/interval.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace dbms {
namespace {
std::string trimIntervalText(const std::string& value) {
    const auto first = value.find_first_not_of(" \t\r\n\f\v");
    if (first == std::string::npos) return {};
    const auto last = value.find_last_not_of(" \t\r\n\f\v");
    return value.substr(first, last - first + 1);
}
}

// Parse the canonical output form or the human input form ("2 years",
// "14 months", "90 minutes", "1-2", "04:05:06", "1.5 days").
IntervalInputParts parseIntervalInput(const std::string& in) {
    IntervalInputParts r;
    long long years = 0;
    std::string s = trimIntervalText(in);
    if (!s.empty() && s.front() == '@') s = trimIntervalText(s.substr(1));
    if (s.empty()) return r;
    // ISO designators select the same bounded calendar/time fields as the
    // verbose input form; retain the quantity spelling until checked parsing.
    if (s.front() == 'P' || s.front() == 'p') {
        std::string verbose;
        bool timePart = false, any = false;
        size_t position = 1;
        while (position < s.size()) {
            if (s[position] == 'T' || s[position] == 't') {
                if (timePart) return r;
                timePart = true; ++position; continue;
            }
            const size_t begin = position;
            if (s[position] == '+' || s[position] == '-') ++position;
            bool digits = false;
            while (position < s.size() && (std::isdigit(static_cast<unsigned char>(s[position])) || s[position] == '.')) {
                digits = digits || std::isdigit(static_cast<unsigned char>(s[position]));
                ++position;
            }
            if (!digits || position == s.size()) return r;
            const auto quantity = s.substr(begin, position - begin);
            const char unit = static_cast<char>(std::toupper(static_cast<unsigned char>(s[position++])));
            std::string name;
            if (!timePart && unit == 'Y') name = "years";
            else if (!timePart && unit == 'M') name = "months";
            else if (!timePart && unit == 'W') name = "weeks";
            else if (!timePart && unit == 'D') name = "days";
            else if (timePart && unit == 'H') name = "hours";
            else if (timePart && unit == 'M') name = "minutes";
            else if (timePart && unit == 'S') name = "seconds";
            else return r;
            if (!verbose.empty()) verbose += ' ';
            verbose += quantity + ' ' + name;
            any = true;
        }
        if (!any) return r;
        s = std::move(verbose);
    }
    bool negate = false;
    std::string low;
    for (char c : s) low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (low.size() > 4 && low.substr(low.size() - 4) == " ago") {
        negate = true;
        s = trimIntervalText(s.substr(0, s.size() - 4));
    }
    auto parseInteger = [&](const std::string& text, long long& value) {
        // A single field this long cannot fit PostgreSQL interval input's
        // 256-byte token workspace, irrespective of its numeric value.
        if (text.size() >= 255) {
            r.numericFieldTooLong = true;
            return false;
        }
        try {
            size_t consumed = 0;
            value = std::stoll(text, &consumed);
            return consumed == text.size();
        } catch (const std::out_of_range&) {
            r.outOfRange = true;
            return false;
        } catch (...) {
            return false;
        }
    };
    auto addScaled = [&](long long& target, long long value,
                        long long scale) {
        const __int128 total = static_cast<__int128>(target) +
            static_cast<__int128>(value) * scale;
        const bool calendarField = &target == &years || &target == &r.months || &target == &r.days;
        const __int128 lower = calendarField ? std::numeric_limits<int32_t>::lowest()
            : std::numeric_limits<int64_t>::lowest();
        const __int128 upper = calendarField ? std::numeric_limits<int32_t>::max()
            : std::numeric_limits<int64_t>::max();
        if (total < lower || total > upper) {
            r.outOfRange = true;
            return false;
        }
        target = static_cast<long long>(total);
        return true;
    };
    auto parseDecimal = [&](const std::string& text, long double& value) {
        if (text.size() >= 255) {
            r.numericFieldTooLong = true;
            return false;
        }
        try {
            size_t consumed = 0;
            value = std::stold(text, &consumed);
            return consumed == text.size() && std::isfinite(value);
        } catch (const std::out_of_range&) {
            r.outOfRange = true;
            return false;
        } catch (...) {
            return false;
        }
    };
    auto truncateToInteger = [&](long double value, long long& result) {
        if (!std::isfinite(value) ||
            value < static_cast<long double>(
                         std::numeric_limits<long long>::lowest()) ||
            value > static_cast<long double>(
                        std::numeric_limits<long long>::max())) {
            r.outOfRange = true;
            return false;
        }
        result = static_cast<long long>(value);
        return true;
    };
    auto addScaledDecimal = [&](long long& target, long double value,
                                long double scale) {
        // Interval fractional fields use binary64 and round a strictly
        // greater-than-half residual microsecond, independently of the
        // exact signed integer part. Never convert that integer to double.
        const double scaled = static_cast<double>(value) * static_cast<double>(scale);
        long long delta = 0;
        if (!truncateToInteger(std::trunc(scaled), delta)) return false;
        const double residual = scaled - static_cast<double>(delta);
        const long long adjustment = residual > 0.5 ? 1 : residual < -0.5 ? -1 : 0;
        return addScaled(target, delta, 1) && addScaled(target, adjustment, 1);
    };
    auto addFractionDays = [&](long double days) {
        const double binaryDays = static_cast<double>(days);
        long long whole = 0;
        return truncateToInteger(binaryDays, whole) && addScaled(r.days, whole, 1) &&
            addScaledDecimal(r.micros, binaryDays - whole, 86400000000.0L);
    };
    auto finish = [&]() {
        if (!r.ok) return r;
        if (negate) {
            if (years == std::numeric_limits<int32_t>::lowest() ||
                r.months == std::numeric_limits<int32_t>::lowest() ||
                r.days == std::numeric_limits<int32_t>::lowest() ||
                r.micros == std::numeric_limits<int64_t>::lowest()) {
                r.outOfRange = true;
                r.ok = false;
                return r;
            }
            years = -years;
            r.months = -r.months;
            r.days = -r.days;
            r.micros = -r.micros;
        }
        const __int128 months = static_cast<__int128>(years) * 12 + r.months;
        if (months < std::numeric_limits<int32_t>::lowest() ||
            months > std::numeric_limits<int32_t>::max()) {
            r.combinedOutOfRange = true;
            r.ok = false;
        } else {
            r.months = static_cast<long long>(months);
        }
        return r;
    };
    // SQL year-month shorthand "[+-]N-M": the leading sign covers both fields.
    {
        bool shorthand = true;
        size_t dash = std::string::npos;
        const size_t start = s.front() == '-' || s.front() == '+' ? 1 : 0;
        for (size_t i = start; i < s.size(); ++i) {
            char c = s[i];
            if (c == '-') { if (dash != std::string::npos) { shorthand = false; break; } dash = i; }
            else if (!std::isdigit(static_cast<unsigned char>(c))) { shorthand = false; break; }
        }
        if (shorthand && dash != std::string::npos && dash > start && dash + 1 < s.size()) {
            long long yearPart = 0;
            long long months = 0;
            if (!parseInteger(s.substr(0, dash), yearPart) ||
                !parseInteger(s.substr(dash + 1), months)) {
                return r;
            }
            if (months > 11) { r.outOfRange = true; return r; }
            if (s.front() == '-') months = -months;
            if (!addScaled(years, yearPart, 1) || !addScaled(r.months, months, 1)) return r;
            r.ok = true;
            return finish();
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
        if (total < std::numeric_limits<long long>::lowest() ||
            total > std::numeric_limits<long long>::max()) {
            r.outOfRange = true;
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
                if (digits.size() > 6) {
                    const bool trailingNonzero =
                        digits.find_first_not_of('0', 7) !=
                        std::string::npos;
                    const bool roundUp = digits[6] > '5' ||
                        (digits[6] == '5' &&
                         (trailingNonzero || fraction % 2 != 0));
                    if (roundUp) ++fraction;
                }
            }
        }
        return addClockMicros(hours, minutes, seconds, fraction,
                              hourText.front() == '-');
    };
    auto applyUnit = [&](long long n, const std::string& unit) {
        std::string u;
        for (char c : unit) u += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (u == "year" || u == "years" || u == "y" || u == "yr" || u == "yrs")
            return addScaled(years, n, 1);
        if (u == "mon" || u == "mons" || u == "month" || u == "months")
            return addScaled(r.months, n, 1);
        if (u == "week" || u == "weeks" || u == "w")
            return addScaled(r.days, n, 7);
        if (u == "day" || u == "days" || u == "d")
            return addScaled(r.days, n, 1);
        if (u == "hour" || u == "hours" || u == "h" || u == "hr" || u == "hrs")
            return addScaled(r.micros, n, 3600000000LL);
        if (u == "min" || u == "mins" || u == "minute" || u == "minutes")
            return addScaled(r.micros, n, 60000000LL);
        if (u == "sec" || u == "secs" || u == "second" || u == "seconds" || u == "s")
            return addScaled(r.micros, n, 1000000LL);
        if (u == "ms" || u == "millisec" || u == "millisecs" || u == "millisecond" || u == "milliseconds")
            return addScaled(r.micros, n, 1000LL);
        if (u == "us" || u == "microsec" || u == "microsecs" || u == "microsecond" || u == "microseconds")
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
                    if (!applyUnit(integer, peek)) { r.ok = false; break; }
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
        const std::string number = tok.substr(0, endNum);
        if (number.size() >= 255) {
            r.numericFieldTooLong = true;
            r.ok = false;
            break;
        }
        const size_t dot = number.find('.');
        const auto integerText = number.substr(0, dot);
        long long whole = 0;
        if (!integerText.empty() && integerText != "-" && integerText != "+" &&
            !parseInteger(integerText, whole)) {
            r.ok = false;
            break;
        }
        long double fraction = 0;
        if (dot != std::string::npos && dot + 1 < number.size()) {
            if (!parseDecimal("0" + number.substr(dot), fraction)) {
                r.ok = false;
                break;
            }
            if (number.front() == '-') fraction = -fraction;
        }
        std::string unit = (endNum <= tok.size()) ? tok.substr(endNum) : std::string();
        if (unit.empty()) {
            const auto position = iss.tellg();
            if (!(iss >> unit) || !std::isalpha(static_cast<unsigned char>(unit.front()))) {
                iss.clear();
                if (position != decltype(iss)::pos_type(-1)) iss.seekg(position);
                unit = "seconds";
            }
        }
        // lower-case the unit
        for (char& c : unit) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (unit == "year" || unit == "years" || unit == "y" || unit == "yr" || unit == "yrs") {
            long long extraMonths = 0;
            r.ok = addScaled(years, whole, 1) &&
                truncateToInteger(std::nearbyint(static_cast<double>(fraction) * 12.0), extraMonths) &&
                addScaled(r.months, extraMonths, 1);
        } else if (unit == "month" || unit == "months" || unit == "mon" || unit == "mons") {
            r.ok = addScaled(r.months, whole, 1) && addFractionDays(static_cast<double>(fraction) * 30.0);
        } else if (unit == "week" || unit == "weeks" || unit == "w") {
            r.ok = addScaled(r.days, whole, 7) && addFractionDays(static_cast<double>(fraction) * 7.0);
        } else if (unit == "day" || unit == "days" || unit == "d") {
            r.ok = addScaled(r.days, whole, 1) && addScaledDecimal(r.micros, fraction, 86400000000.0L);
        } else {
            long long scale = 0;
            if (unit == "hour" || unit == "hours" || unit == "h" || unit == "hr" || unit == "hrs") scale = 3600000000LL;
            else if (unit == "minute" || unit == "minutes" || unit == "min" || unit == "mins") scale = 60000000LL;
            else if (unit == "second" || unit == "seconds" || unit == "s" || unit == "sec" || unit == "secs") scale = 1000000LL;
            else if (unit == "ms" || unit == "millisec" || unit == "millisecs" || unit == "millisecond" || unit == "milliseconds") scale = 1000LL;
            else if (unit == "us" || unit == "microsec" || unit == "microsecs" || unit == "microsecond" || unit == "microseconds") scale = 1;
            r.ok = scale != 0 && addScaled(r.micros, whole, scale) &&
                addScaledDecimal(r.micros, fraction, static_cast<long double>(scale));
        }
        if (!r.ok) break;
    }
    return finish();
}

std::string formatIntervalInput(long long months, long long days, long long micros,
                                bool trimFractionZeros) {
    std::string out;
    auto appendPart = [&](long long value, const char* singular,
                          const char* plural) {
        if (value == 0) return;
        if (!out.empty()) out += " ";
        out += std::to_string(value);
        // PostgreSQL pluralizes negative interval fields: -1 days, -1 mons,
        // and -1 years.  Only the positive value one uses the singular form.
        out += (value == 1) ? singular : plural;
    };
    appendPart(months / 12, " year", " years");
    appendPart(months % 12, " mon", " mons");
    appendPart(days, " day", " days");
    if (micros || out.empty()) {
        if (!out.empty()) out += " ";
        const bool negative = micros < 0;
        uint64_t us = negative ? static_cast<uint64_t>(-(micros + 1)) + 1
            : static_cast<uint64_t>(micros);
        const long long hh = static_cast<long long>(us / 3600000000ULL); us %= 3600000000ULL;
        const long long mm = static_cast<long long>(us / 60000000ULL); us %= 60000000ULL;
        const long long ss = static_cast<long long>(us / 1000000ULL);
        const long long frac = static_cast<long long>(us % 1000000ULL);
        char buf[64];
        if (frac) {
            std::snprintf(buf, sizeof(buf), "%s%02lld:%02lld:%02lld.%06lld",
                          negative ? "-" : "", hh, mm, ss, frac);
        } else {
            std::snprintf(buf, sizeof(buf), "%s%02lld:%02lld:%02lld",
                          negative ? "-" : "", hh, mm, ss);
        }
        std::string clock = buf;
        if (trimFractionZeros && frac) {
            while (clock.back() == '0') clock.pop_back();
        }
        out += clock;
    }
    return out;
}

std::string intervalInputSqlState(const IntervalInputParts& parsed) {
    if (parsed.numericFieldTooLong) return "22007";
    if (parsed.outOfRange) return "22015";
    if (parsed.combinedOutOfRange) return "22008";
    return parsed.ok ? "" : "22007";
}
}
