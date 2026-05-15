#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

constexpr int DAYS[14] = {0, 365, 334, 306, 275, 245, 214, 184, 153, 122, 92, 61, 31, 0};

struct Date {
    Date() : year(0), month(0), day(0) {}
    Date(int y, int m, int d) : year(y), month(m), day(d) {
        if (month > 12 || day > DAYS[month] - DAYS[month + 1] + (month == 2) * isleap()) {
            month = 0; year = 0; day = 0;
        }
    }
    Date(const char* s) {
        int pos[2] = {0, 0};
        int k = 0;
        for (int i = 0; s[i] && k < 2; i++) {
            if (s[i] == '-') pos[k++] = i;
        }
        if (!pos[0] || !pos[1] || pos[1] == pos[0] + 1 ||
            pos[1] - pos[0] > 3 || static_cast<int>(strlen(s)) - pos[1] > 3 || pos[0] > 4) {
            return;
        }
        for (int i = 0; i < pos[0]; i++) year = year * 10 + s[i] - '0';
        for (int i = pos[0] + 1; i < pos[1]; i++) month = month * 10 + s[i] - '0';
        for (int i = pos[1] + 1; s[i]; i++) day = day * 10 + s[i] - '0';
        if (month > 12 || day > DAYS[month] - DAYS[month + 1] + (month == 2) * isleap()) {
            month = 0; year = 0; day = 0;
        }
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
    return transstr(t.year) + '-' + transstr(t.month) + '-' + transstr(t.day);
}

inline std::ostream& operator<<(std::ostream& ost, Date a) {
    ost << a.year << '-' << a.month << '-' << a.day;
    return ost;
}

// ========================================================================
// Timestamp helpers: store as int64_t seconds since Date epoch
// ========================================================================
inline int64_t parseTimestampToSeconds(const std::string& s) {
    int y = 0, m = 0, d = 0, h = 0, mn = 0, sec = 0;
    auto sp = s.find(' ');
    std::string datePart = (sp == std::string::npos) ? s : s.substr(0, sp);
    std::string timePart = (sp == std::string::npos) ? "00:00:00" : s.substr(sp + 1);
    // Parse date YYYY-MM-DD
    int pos[2] = {0, 0};
    int k = 0;
    for (size_t i = 0; i < datePart.size() && k < 2; i++) {
        if (datePart[i] == '-') pos[k++] = static_cast<int>(i);
    }
    if (pos[0] && pos[1]) {
        for (int i = 0; i < pos[0]; i++) y = y * 10 + datePart[i] - '0';
        for (int i = pos[0] + 1; i < pos[1]; i++) m = m * 10 + datePart[i] - '0';
        for (size_t i = pos[1] + 1; i < datePart.size(); i++) d = d * 10 + datePart[i] - '0';
    }
    // Parse time HH:MM:SS
    int tpos[2] = {0, 0};
    k = 0;
    for (size_t i = 0; i < timePart.size() && k < 2; i++) {
        if (timePart[i] == ':') tpos[k++] = static_cast<int>(i);
    }
    if (tpos[0] && tpos[1]) {
        for (int i = 0; i < tpos[0]; i++) h = h * 10 + timePart[i] - '0';
        for (int i = tpos[0] + 1; i < tpos[1]; i++) mn = mn * 10 + timePart[i] - '0';
        for (size_t i = tpos[1] + 1; i < timePart.size(); i++) sec = sec * 10 + timePart[i] - '0';
    }
    Date dt(y, m, d);
    if (dt.year == 0) return 0;
    return dt.convert() * 86400LL + h * 3600LL + mn * 60LL + sec;
}

inline std::string formatTimestampSeconds(int64_t ts) {
    if (ts < 0) return "";
    int64_t dayNum = ts / 86400;
    int64_t sod = ts % 86400;
    Date d = DISCONV(dayNum);
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
