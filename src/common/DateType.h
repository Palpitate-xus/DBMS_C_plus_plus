#pragma once

#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>

constexpr int DAYS[14] = {0, 365, 334, 306, 275, 245, 214, 184, 153, 122, 92, 61, 31, 0};

// INT64_MIN is already the legacy fixed-width NULL marker (INF).  Keep the
// negative-infinity value distinct so a stored timestamp remains observable
// even in code paths that do not have a heap null bitmap bound.
inline constexpr int64_t TIMESTAMP_POSITIVE_INFINITY = INT64_MAX;
inline constexpr int64_t TIMESTAMP_NEGATIVE_INFINITY = INT64_MIN + int64_t{1};

inline bool isInfiniteTimestamp(int64_t value) {
    return value == TIMESTAMP_POSITIVE_INFINITY ||
           value == TIMESTAMP_NEGATIVE_INFINITY;
}

inline bool parseTemporalUnsigned(std::string_view text, size_t maxDigits,
                                  int& value) {
    if (text.empty() || text.size() > maxDigits) return false;
    int parsed = 0;
    for (const unsigned char c : text) {
        if (!std::isdigit(c)) return false;
        parsed = parsed * 10 + static_cast<int>(c - '0');
    }
    value = parsed;
    return true;
}

struct Date {
    Date() : year(0), month(0), day(0) {}
    Date(int y, int m, int d) {
        if (m < 1 || m > 12 || d < 1) return;
        const bool leap = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0);
        const int maximumDay =
            DAYS[m] - DAYS[m + 1] + (m == 2 && leap ? 1 : 0);
        if (d > maximumDay) return;
        year = y;
        month = m;
        day = d;
    }
    Date(const char* s) {
        if (!s) return;
        const std::string_view input(s);
        const size_t firstDash = input.find('-');
        if (firstDash == std::string_view::npos) return;
        const size_t secondDash = input.find('-', firstDash + 1);
        if (secondDash == std::string_view::npos ||
            input.find('-', secondDash + 1) != std::string_view::npos) {
            return;
        }
        int parsedYear = 0;
        int parsedMonth = 0;
        int parsedDay = 0;
        if (!parseTemporalUnsigned(input.substr(0, firstDash), 4,
                                   parsedYear) ||
            !parseTemporalUnsigned(
                input.substr(firstDash + 1,
                             secondDash - firstDash - 1),
                2, parsedMonth) ||
            !parseTemporalUnsigned(input.substr(secondDash + 1), 2,
                                   parsedDay) ||
            parsedYear == 0 || parsedMonth < 1 || parsedMonth > 12 ||
            parsedDay < 1) {
            return;
        }
        const bool leap =
            ((parsedYear % 4 == 0 && parsedYear % 100 != 0) ||
             parsedYear % 400 == 0);
        const int maximumDay = DAYS[parsedMonth] - DAYS[parsedMonth + 1] +
            (parsedMonth == 2 && leap ? 1 : 0);
        if (parsedDay > maximumDay) return;
        year = parsedYear;
        month = parsedMonth;
        day = parsedDay;
    }

    int year = 0, month = 0, day = 0;

    bool isleap() const {
        return ((!(year % 4) && year % 100) || !(year % 400));
    }
    void print() const {
        std::cout << year << '-' << month << '-' << day << std::endl;
    }
    int64_t convert() const {
        return year * 365LL + year / 4 - year / 100 + year / 400
               - DAYS[month] - (month <= 2) * isleap() + day;
    }
    int operator[](int st) const {
        if (st == 0) return year;
        if (st == 1) return month;
        if (st == 2) return day;
        return -1;
    }
};

inline Date DISCONV(int64_t num) {
    Date t = {0, 12, 31};
    if (num <= 0 || num > 3652059) return t;
    if (num >= 146097) t.year += num / 146097 * 400;
    num %= 146097;
    while (num > 0) t.year++, num -= (365 + t.isleap());
    while (num + DAYS[t.month] <= 0) t.month--;
    t.day = num + DAYS[t.month] + (t.month <= 2) * t.isleap();
    return t;
}

inline int64_t CONVERT(Date t) {
    return t.year * 365 + t.year / 4 - t.year / 100 + t.year / 400
           - DAYS[t.month] - (t.month <= 2) * t.isleap() + t.day;
}

inline bool operator>(Date a, Date b) {
    if (a.year != b.year) return a.year > b.year;
    if (a.month != b.month) return a.month > b.month;
    return a.day > b.day;
}
inline bool operator<(Date a, Date b) {
    if (a.year != b.year) return a.year < b.year;
    if (a.month != b.month) return a.month < b.month;
    return a.day < b.day;
}
inline bool operator==(Date a, Date b) {
    return a.year == b.year && a.month == b.month && a.day == b.day;
}
inline bool operator!=(Date a, Date b) {
    return a.year != b.year || a.month != b.month || a.day != b.day;
}
inline bool operator>=(Date a, Date b) {
    if (a.year != b.year) return a.year > b.year;
    if (a.month != b.month) return a.month > b.month;
    return a.day >= b.day;
}
inline bool operator<=(Date a, Date b) {
    if (a.year != b.year) return a.year < b.year;
    if (a.month != b.month) return a.month < b.month;
    return a.day <= b.day;
}
inline Date operator+(Date a, int64_t b) {
    return DISCONV(a.convert() + b);
}
inline Date operator-(Date a, int64_t b) {
    return DISCONV(a.convert() - b);
}
inline int64_t operator-(Date a, Date b) {
    return a.convert() - b.convert();
}

inline Date dateAddMonths(Date d, int months) {
    int m = d.month + months;
    int y = d.year;
    while (m > 12) { m -= 12; y++; }
    while (m < 1) { m += 12; y--; }
    int maxDay = DAYS[m] - DAYS[m + 1] + (m == 2) * ((!(y % 4) && y % 100) || !(y % 400));
    if (d.day > maxDay) d.day = maxDay;
    return Date(y, m, d.day);
}
inline Date dateAddYears(Date d, int years) {
    return dateAddMonths(d, years * 12);
}

inline std::string transstr(int64_t t) {
    if (t == 0) return "0";
    std::string tem;
    bool neg = t < 0;
    if (neg) t = -t;
    while (t) {
        tem = static_cast<char>(t % 10 + '0') + tem;
        t /= 10;
    }
    return neg ? "-" + tem : tem;
}

inline std::string str(Date t) {
    // Input parsing and on-disk dates use a four-digit civil year. Refuse an
    // invalid/out-of-domain Date instead of truncating a large integer into a
    // fixed stack buffer and returning a different, malformed value.
    if (t.year < 1 || t.year > 9999 || t.month < 1 || t.month > 12 ||
        t.day < 1) {
        return "";
    }
    const bool leap =
        ((t.year % 4 == 0 && t.year % 100 != 0) || t.year % 400 == 0);
    const int maximumDay = DAYS[t.month] - DAYS[t.month + 1] +
        (t.month == 2 && leap ? 1 : 0);
    if (t.day > maximumDay) return "";

    std::string result = std::to_string(t.year);
    result.insert(result.begin(), 4 - result.size(), '0');
    result.push_back('-');
    if (t.month < 10) result.push_back('0');
    result += std::to_string(t.month);
    result.push_back('-');
    if (t.day < 10) result.push_back('0');
    result += std::to_string(t.day);
    return result;
}

inline std::ostream& operator<<(std::ostream& ost, Date a) {
    ost << a.year << '-' << a.month << '-' << a.day;
    return ost;
}

// ========================================================================
// Time helpers: store as int32_t seconds since 00:00:00
// ========================================================================
inline int32_t parseTimeToSeconds(const std::string& s) {
    const std::string_view input(s);
    const size_t firstColon = input.find(':');
    if (firstColon == std::string_view::npos) return -1;
    const size_t secondColon = input.find(':', firstColon + 1);
    if (secondColon == std::string_view::npos ||
        input.find(':', secondColon + 1) != std::string_view::npos) {
        return -1;
    }
    int h = 0;
    int m = 0;
    int sec = 0;
    if (!parseTemporalUnsigned(input.substr(0, firstColon), 2, h) ||
        !parseTemporalUnsigned(
            input.substr(firstColon + 1, secondColon - firstColon - 1),
            2, m) ||
        !parseTemporalUnsigned(input.substr(secondColon + 1), 2, sec) ||
        h > 23 || m > 59 || sec > 59) {
        return -1;
    }
    return h * 3600 + m * 60 + sec;
}

inline std::string formatTimeSeconds(int32_t secs) {
    if (secs < 0 || secs > 86399) return "";
    int h = secs / 3600;
    int m = (secs % 3600) / 60;
    int s = secs % 60;
    std::string res;
    if (h < 10) res += "0";
    res += transstr(h) + ":";
    if (m < 10) res += "0";
    res += transstr(m) + ":";
    if (s < 10) res += "0";
    res += transstr(s);
    return res;
}

// ========================================================================
// Timestamp helpers: store as int64_t seconds since Date epoch
// ========================================================================
inline int64_t parseTimestampToSeconds(const std::string& s) {
    // Support PostgreSQL infinity / -infinity sentinels.
    std::string lower = s;
    for (auto& c : lower) c = std::tolower(static_cast<unsigned char>(c));
    if (lower == "infinity") return TIMESTAMP_POSITIVE_INFINITY;
    if (lower == "-infinity") return TIMESTAMP_NEGATIVE_INFINITY;
    if (s.empty()) return 0;
    int tzOffsetMinutes = 0;  // +08:00 => +480, -05:00 => -300
    const size_t sp = s.find(' ');
    if (sp != std::string::npos && s.find(' ', sp + 1) != std::string::npos)
        return 0;
    std::string datePart = (sp == std::string::npos) ? s : s.substr(0, sp);
    std::string timePart = (sp == std::string::npos) ? "00:00:00" : s.substr(sp + 1);
    if (datePart.empty() || timePart.empty()) return 0;
    // Parse timezone offset from timePart if present: [+-]HH or [+-]HH:MM
    size_t tzPos = std::string::npos;
    for (size_t i = 0; i < timePart.size(); ++i) {
        if ((timePart[i] == '+' || timePart[i] == '-') && i > 0) {
            tzPos = i;
            break;
        }
    }
    std::string tzStr;
    const bool hasZulu = !timePart.empty() &&
        (timePart.back() == 'Z' || timePart.back() == 'z');
    if (hasZulu) {
        if (tzPos != std::string::npos) return 0;
        timePart.pop_back();
        if (timePart.empty()) return 0;
    }
    if (tzPos != std::string::npos) {
        tzStr = timePart.substr(tzPos);
        timePart = timePart.substr(0, tzPos);
        if (timePart.empty() || tzStr.size() < 2) return 0;
        bool tzNegative = (tzStr[0] == '-');
        int tzh = 0, tzm = 0;
        size_t tzColon = tzStr.find(':');
        if (tzColon != std::string::npos) {
            if (tzStr.find(':', tzColon + 1) != std::string::npos ||
                !parseTemporalUnsigned(
                    std::string_view(tzStr).substr(1, tzColon - 1), 2,
                    tzh) ||
                !parseTemporalUnsigned(
                    std::string_view(tzStr).substr(tzColon + 1), 2,
                    tzm)) {
                return 0;
            }
        } else {
            if (!parseTemporalUnsigned(
                    std::string_view(tzStr).substr(1), 2, tzh)) {
                return 0;
            }
        }
        if (tzh > 15 || tzm > 59) return 0;
        tzOffsetMinutes = (tzNegative ? -1 : 1) * (tzh * 60 + tzm);
    }
    Date dt(datePart.c_str());
    if (dt.year == 0) return 0;
    const int32_t timeSeconds = parseTimeToSeconds(timePart);
    if (timeSeconds < 0) return 0;
    // For TIMESTAMPTZ: store as UTC (subtract timezone offset)
    return dt.convert() * 86400LL + timeSeconds - tzOffsetMinutes * 60LL;
}

inline std::string formatTimestampSeconds(int64_t ts) {
    if (ts == TIMESTAMP_POSITIVE_INFINITY) return "infinity";
    if (ts == TIMESTAMP_NEGATIVE_INFINITY) return "-infinity";
    if (ts < 0) return "";
    int64_t dayNum = ts / 86400;
    int64_t sod = ts % 86400;
    // dayNum is days since 1970-01-01 (Unix epoch); DISCONV uses a
    // year-0-based count, so convert epoch days to civil date directly
    // (Howard Hinnant civil_from_days).
    int64_t z = dayNum - 719163 + 719468;  // convert() days -> epoch days
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const uint32_t doe = static_cast<uint32_t>(z - era * 146097);
    const uint32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t yy = static_cast<int64_t>(yoe) + era * 400;
    const uint32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const uint32_t mp = (5 * doy + 2) / 153;
    uint32_t dd = doy - (153 * mp + 2) / 5 + 1;
    uint32_t mm = mp + (mp < 10 ? 3 : -9);
    if (mm <= 2) ++yy;
    if (yy < 1 || yy > 9999) return "";
    Date d;
    d.year = static_cast<int>(yy);
    d.month = static_cast<int>(mm);
    d.day = static_cast<int>(dd);
    if (d.year == 0) return "";
    int h = static_cast<int>(sod / 3600);
    int mn = static_cast<int>((sod % 3600) / 60);
    int s = static_cast<int>(sod % 60);
    std::string res = str(d) + " ";
    if (h < 10) res += "0";
    res += transstr(h) + ":";
    if (mn < 10) res += "0";
    res += transstr(mn) + ":";
    if (s < 10) res += "0";
    res += transstr(s);
    return res;
}

// Format timestamp seconds with timezone offset (e.g. +480 min = Asia/Shanghai)
inline std::string formatTimestampWithTz(int64_t utcSeconds, int tzOffsetMinutes) {
    if (isInfiniteTimestamp(utcSeconds))
        return formatTimestampSeconds(utcSeconds);
    if (tzOffsetMinutes < -(15 * 60 + 59) ||
        tzOffsetMinutes > 15 * 60 + 59) {
        return "";
    }
    __int128 adjusted = static_cast<__int128>(utcSeconds) +
        static_cast<__int128>(tzOffsetMinutes) * 60;
    if (adjusted < 0) adjusted = 0;
    // A finite timestamp plus an offset must not overflow or turn into the
    // positive-infinity sentinel by coincidence.
    if (adjusted >= TIMESTAMP_POSITIVE_INFINITY) return "";
    std::string base =
        formatTimestampSeconds(static_cast<int64_t>(adjusted));
    if (base.empty()) return "";
    // Append timezone offset: [+-]HH:MM
    int absOff = std::abs(tzOffsetMinutes);
    int tzh = absOff / 60;
    int tzm = absOff % 60;
    base += (tzOffsetMinutes >= 0) ? " +" : " -";
    if (tzh < 10) base += "0";
    base += transstr(tzh) + ":";
    if (tzm < 10) base += "0";
    base += transstr(tzm);
    return base;
}

// Parse timezone name or offset string to minutes offset from UTC
// Supports: "+08:00", "-05:30", "UTC", "Asia/Shanghai", "America/New_York", etc.
inline int parseTimezoneOffset(const std::string& tzStr) {
    std::string s = tzStr;
    // Trim whitespace and quotes
    {
        size_t a = 0;
        while (a < s.size() && (std::isspace(static_cast<unsigned char>(s[a])) || s[a] == '\'' || s[a] == '"')) ++a;
        size_t b = s.size();
        while (b > a && (std::isspace(static_cast<unsigned char>(s[b-1])) || s[b-1] == '\'' || s[b-1] == '"')) --b;
        s = s.substr(a, b - a);
    }
    // Normalize to lowercase for case-insensitive comparison
    for (char& c : s) c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
    if (s.empty() || s == "utc" || s == "gmt" || s == "z") return 0;
    // Parse [+-]HH:MM or [+-]HH
    if (s[0] == '+' || s[0] == '-') {
        bool negative = (s[0] == '-');
        int tzh = 0, tzm = 0;
        size_t colon = s.find(':');
        if (colon != std::string::npos) {
            for (size_t i = 1; i < colon; ++i) if (s[i] >= '0' && s[i] <= '9') tzh = tzh * 10 + s[i] - '0';
            for (size_t i = colon + 1; i < s.size(); ++i) if (s[i] >= '0' && s[i] <= '9') tzm = tzm * 10 + s[i] - '0';
        } else {
            for (size_t i = 1; i < s.size(); ++i) if (s[i] >= '0' && s[i] <= '9') tzh = tzh * 10 + s[i] - '0';
        }
        return (negative ? -1 : 1) * (tzh * 60 + tzm);
    }
    // Named timezone mapping (common zones) — all lowercase
    if (s == "asia/shanghai" || s == "asia/hong_kong" || s == "asia/singapore" || s == "asia/taipei") return 480;
    if (s == "asia/tokyo" || s == "asia/seoul" || s == "asia/osaka") return 540;
    if (s == "asia/bangkok" || s == "asia/jakarta" || s == "asia/ho_chi_minh") return 420;
    if (s == "asia/dubai") return 240;
    if (s == "asia/kolkata" || s == "asia/calcutta") return 330;
    if (s == "europe/london") return 0;
    if (s == "europe/paris" || s == "europe/berlin" || s == "europe/madrid" || s == "europe/rome" || s == "europe/amsterdam" || s == "europe/vienna") return 60;
    if (s == "europe/athens" || s == "europe/helsinki" || s == "europe/bucharest") return 120;
    if (s == "europe/moscow" || s == "europe/istanbul") return 180;
    if (s == "america/new_york" || s == "america/toronto" || s == "america/miami" || s == "america/detroit") return -300;
    if (s == "america/chicago" || s == "america/mexico_city" || s == "america/denver") return -360;
    if (s == "america/los_angeles" || s == "america/vancouver" || s == "america/seattle") return -480;
    if (s == "america/anchorage") return -540;
    if (s == "pacific/honolulu") return -600;
    if (s == "australia/sydney" || s == "australia/melbourne") return 600;
    if (s == "australia/perth") return 480;
    if (s == "australia/adelaide") return 570;
    if (s == "australia/darwin") return 570;
    if (s == "africa/cairo") return 120;
    if (s == "africa/johannesburg") return 120;
    if (s == "america/sao_paulo" || s == "america/buenos_aires") return -180;
    if (s == "pacific/auckland") return 720;
    return 0; // Default to UTC for unknown zones
}
