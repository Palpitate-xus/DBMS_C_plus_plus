#pragma once

#include <algorithm>
#include <climits>
#include <cstdint>
#include <limits>
#include <locale>
#include <string>

namespace dbms {

// PostgreSQL's money datum is a signed 64-bit count of the smallest monetary
// unit.  Keep parsing and rendering decimal-only so values never pass through
// a binary floating-point approximation.
class Money {
public:
    static constexpr int kFallbackFractionDigits = 2;

    Money() = default;
    explicit Money(int64_t minorUnits) : minorUnits_(minorUnits) {}

    int64_t minorUnits() const { return minorUnits_; }

    static bool localeAvailable(const std::string& localeName) {
        if (localeName.empty()) return false;
        try {
            (void)std::locale(localeName.c_str());
            return true;
        } catch (...) {
            return false;
        }
    }

    static int fractionDigits(const std::string& localeName = std::string()) {
        return localeProfile(localeName).fractionDigits;
    }

    // Numeric casts use a locale-independent decimal point but retain the
    // target monetary scale.  This is distinct from user-facing cash input,
    // which accepts the locale's currency symbol and separators.
    static bool parseDecimal(const std::string& input, Money& output,
                             const std::string& localeName = std::string()) {
        const Profile profile = localeProfile(localeName);
        std::string text = trim(input);
        bool negative = false;
        if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
            negative = text.front() == '-';
            text.erase(text.begin());
        }
        if (text.empty()) return false;
        std::string integral;
        std::string fractional;
        bool decimal = false;
        bool digit = false;
        for (const char ch : text) {
            if (ch >= '0' && ch <= '9') {
                (decimal ? fractional : integral).push_back(ch);
                digit = true;
            } else if (ch == '.' && !decimal) {
                decimal = true;
            } else {
                return false;
            }
        }
        if (!digit) return false;
        if (integral.empty()) integral = "0";
        return fromParts(integral, fractional, negative, profile, output);
    }

    static bool parse(const std::string& input, Money& output,
                      const std::string& localeName = std::string()) {
        const Profile profile = localeProfile(localeName);
        std::string text = trim(input);
        if (text.empty() || text.find('\0') != std::string::npos) return false;

        bool negative = false;
        if (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
            negative = true;
            text = trim(text.substr(1, text.size() - 2));
        }

        if (!profile.symbol.empty()) eraseOnce(text, profile.symbol);
        text = trim(text);

        if (!profile.negativeSign.empty() &&
            eraseOnce(text, profile.negativeSign)) {
            if (negative) return false;
            negative = true;
        } else if (!profile.positiveSign.empty()) {
            (void)eraseOnce(text, profile.positiveSign);
        }
        text = trim(text);
        if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
            if (text.front() == '-') {
                if (negative) return false;
                negative = true;
            }
            text.erase(text.begin());
        }
        text = trim(text);
        if (!text.empty() && (text.back() == '+' || text.back() == '-')) {
            if (text.back() == '-') {
                if (negative) return false;
                negative = true;
            }
            text.pop_back();
        }
        text = trim(text);
        if (text.empty()) return false;

        std::string integral;
        std::string fractional;
        bool sawDecimal = false;
        bool sawDigit = false;
        for (const char ch : text) {
            if (ch >= '0' && ch <= '9') {
                (sawDecimal ? fractional : integral).push_back(ch);
                sawDigit = true;
                continue;
            }
            if (ch == profile.decimalPoint && !sawDecimal) {
                sawDecimal = true;
                continue;
            }
            if (ch == profile.thousandsSep && !sawDecimal) continue;
            if (ch == ' ' || ch == '\t') continue;
            return false;
        }
        if (!sawDigit || (integral.empty() && fractional.empty())) return false;
        if (integral.empty()) integral = "0";
        return fromParts(integral, fractional, negative, profile, output);
    }

    std::string decimalString(
            const std::string& localeName = std::string()) const {
        const Profile profile = localeProfile(localeName);
        const bool negative = minorUnits_ < 0;
        const uint64_t magnitude = negative
            ? uint64_t{0} - static_cast<uint64_t>(minorUnits_)
            : static_cast<uint64_t>(minorUnits_);
        uint64_t factor = 1;
        for (int i = 0; i < profile.fractionDigits; ++i) factor *= 10;
        std::string result = negative ? "-" : "";
        result += std::to_string(magnitude / factor);
        if (profile.fractionDigits > 0) {
            std::string fraction = std::to_string(magnitude % factor);
            fraction.insert(0, static_cast<size_t>(profile.fractionDigits) -
                                   fraction.size(), '0');
            result.push_back('.');
            result += fraction;
        }
        return result;
    }

    std::string format(const std::string& localeName = std::string()) const {
        const Profile profile = localeProfile(localeName);
        const bool negative = minorUnits_ < 0;
        const uint64_t magnitude = negative
            ? uint64_t{0} - static_cast<uint64_t>(minorUnits_)
            : static_cast<uint64_t>(minorUnits_);
        uint64_t factor = 1;
        for (int i = 0; i < profile.fractionDigits; ++i) factor *= 10;

        std::string whole = std::to_string(magnitude / factor);
        whole = groupDigits(whole, profile);
        std::string value = whole;
        if (profile.fractionDigits > 0) {
            std::string fraction = std::to_string(magnitude % factor);
            if (fraction.size() < static_cast<size_t>(profile.fractionDigits))
                fraction.insert(0, static_cast<size_t>(profile.fractionDigits) -
                                       fraction.size(), '0');
            value.push_back(profile.decimalPoint);
            value += fraction;
        }

        const std::string sign = negative ? profile.negativeSign
                                          : profile.positiveSign;
        const std::money_base::pattern pattern = negative
            ? profile.negativePattern : profile.positivePattern;
        std::string rendered;
        for (const auto field : pattern.field) {
            if (field == std::money_base::symbol) rendered += profile.symbol;
            else if (field == std::money_base::sign) rendered += sign;
            else if (field == std::money_base::value) rendered += value;
            else if (field == std::money_base::space && !rendered.empty() &&
                     rendered.back() != ' ') rendered.push_back(' ');
        }
        if (rendered.empty()) rendered = sign + profile.symbol + value;
        return trim(rendered);
    }

private:
    struct Profile {
        char decimalPoint = '.';
        char thousandsSep = ',';
        std::string grouping = "\3";
        std::string symbol = "$";
        std::string positiveSign;
        std::string negativeSign = "-";
        int fractionDigits = kFallbackFractionDigits;
        std::money_base::pattern positivePattern = {
            {std::money_base::symbol, std::money_base::sign,
             std::money_base::value, std::money_base::none}};
        std::money_base::pattern negativePattern = {
            {std::money_base::sign, std::money_base::symbol,
             std::money_base::value, std::money_base::none}};
    };

    static bool fromParts(const std::string& integral,
                          const std::string& fractional, bool negative,
                          const Profile& profile, Money& output) {
        const uint64_t magnitudeLimit = negative
            ? (uint64_t{1} << 63)
            : static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        uint64_t whole = 0;
        for (const char digit : integral) {
            const unsigned value = static_cast<unsigned>(digit - '0');
            if (whole > (magnitudeLimit - value) / 10) return false;
            whole = whole * 10 + value;
        }

        uint64_t factor = 1;
        for (int i = 0; i < profile.fractionDigits; ++i) {
            if (factor > magnitudeLimit / 10) return false;
            factor *= 10;
        }
        if (whole > magnitudeLimit / factor) return false;
        uint64_t magnitude = whole * factor;

        uint64_t fraction = 0;
        for (int i = 0; i < profile.fractionDigits; ++i) {
            fraction *= 10;
            if (static_cast<size_t>(i) < fractional.size())
                fraction += static_cast<unsigned>(fractional[i] - '0');
        }
        if (magnitude > magnitudeLimit - fraction) return false;
        magnitude += fraction;

        // cash_in rounds excess fractional digits to the locale's monetary
        // scale.  Do the rounding on digits so large values remain exact.
        if (fractional.size() > static_cast<size_t>(profile.fractionDigits) &&
            fractional[profile.fractionDigits] >= '5') {
            if (magnitude == magnitudeLimit) return false;
            ++magnitude;
        }

        if (negative) {
            output.minorUnits_ = magnitude == (uint64_t{1} << 63)
                ? std::numeric_limits<int64_t>::min()
                : -static_cast<int64_t>(magnitude);
        } else {
            output.minorUnits_ = static_cast<int64_t>(magnitude);
        }
        return true;
    }

    static std::string trim(const std::string& value) {
        size_t first = 0;
        size_t last = value.size();
        while (first < last && (value[first] == ' ' || value[first] == '\t' ||
                                value[first] == '\r' || value[first] == '\n'))
            ++first;
        while (last > first && (value[last - 1] == ' ' || value[last - 1] == '\t' ||
                                value[last - 1] == '\r' || value[last - 1] == '\n'))
            --last;
        return value.substr(first, last - first);
    }

    static bool eraseOnce(std::string& value, const std::string& token) {
        if (token.empty()) return false;
        const size_t position = value.find(token);
        if (position == std::string::npos) return false;
        if (value.find(token, position + token.size()) != std::string::npos)
            return false;
        value.erase(position, token.size());
        return true;
    }

    static Profile localeProfile(const std::string& localeName) {
        Profile profile;
        try {
            const std::locale locale = localeName.empty()
                ? std::locale("") : std::locale(localeName.c_str());
            const auto& punct = std::use_facet<std::moneypunct<char, false>>(locale);
            const int digits = static_cast<unsigned char>(punct.frac_digits());
            // The C locale exposes empty monetary punctuation. PostgreSQL
            // deliberately substitutes its historical $/./,/2 defaults in
            // that case instead of treating money as a zero-scale integer.
            if (punct.curr_symbol().empty() || punct.decimal_point() == '\0' ||
                digits == CHAR_MAX) return profile;
            if (digits <= 18) profile.fractionDigits = digits;
            if (punct.decimal_point() != '\0') profile.decimalPoint = punct.decimal_point();
            if (punct.thousands_sep() != '\0') profile.thousandsSep = punct.thousands_sep();
            if (!punct.grouping().empty() &&
                static_cast<unsigned char>(punct.grouping().front()) != CHAR_MAX)
                profile.grouping = punct.grouping();
            if (!punct.curr_symbol().empty()) profile.symbol = punct.curr_symbol();
            profile.positiveSign = punct.positive_sign();
            if (!punct.negative_sign().empty()) profile.negativeSign = punct.negative_sign();
            profile.positivePattern = punct.pos_format();
            profile.negativePattern = punct.neg_format();
        } catch (...) {
            // Invalid/uninstalled locales fail closed at GUC assignment.  The
            // codec itself retains C-locale PostgreSQL fallback semantics.
        }
        return profile;
    }

    static std::string groupDigits(const std::string& digits,
                                   const Profile& profile) {
        if (digits.size() <= 3 || profile.grouping.empty()) return digits;
        std::string reversed;
        reversed.reserve(digits.size() + digits.size() / 3);
        size_t groupingIndex = 0;
        unsigned group = static_cast<unsigned char>(profile.grouping[0]);
        unsigned used = 0;
        for (auto it = digits.rbegin(); it != digits.rend(); ++it) {
            if (used == group && group > 0 && group != CHAR_MAX) {
                reversed.push_back(profile.thousandsSep);
                used = 0;
                if (groupingIndex + 1 < profile.grouping.size()) {
                    const unsigned next = static_cast<unsigned char>(
                        profile.grouping[++groupingIndex]);
                    if (next != 0) group = next;
                }
            }
            reversed.push_back(*it);
            ++used;
        }
        std::reverse(reversed.begin(), reversed.end());
        return reversed;
    }

    int64_t minorUnits_ = 0;
};

}  // namespace dbms
