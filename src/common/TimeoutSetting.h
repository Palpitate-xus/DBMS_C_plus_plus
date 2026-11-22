#pragma once

#include "SessionSetting.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <optional>
#include <string_view>

namespace dbms {

enum class TimeoutParameter { Statement, Lock, Deadlock };

inline constexpr std::array<std::string_view, 3> timeoutNames = {
    "statement_timeout", "lock_timeout", "deadlock_timeout"
};

inline std::optional<TimeoutParameter> findTimeoutParameter(std::string_view name) {
    if (name.size() > 3 && name.substr(name.size() - 3) == "_ms")
        name.remove_suffix(3);
    const auto found = std::find(timeoutNames.begin(), timeoutNames.end(), name);
    if (found == timeoutNames.end()) return std::nullopt;
    return static_cast<TimeoutParameter>(found - timeoutNames.begin());
}

struct TimeoutUnit { std::string_view name; double milliseconds; };
inline constexpr TimeoutUnit timeoutUnits[] = {
    {"d", 86400000}, {"h", 3600000}, {"min", 60000},
    {"s", 1000}, {"ms", 1}, {"us", 0.001},
};

inline std::optional<int> parseTimeoutMs(const std::string& input, int minimum) {
    if (input.find('\0') != std::string::npos) return std::nullopt;
    const char* start = input.c_str();
    char* end = nullptr;
    errno = 0;
    double value = std::strtol(start, &end, 0);
    if (*end == '.' || *end == 'e' || *end == 'E' || errno == ERANGE) {
        errno = 0;
        value = std::strtod(start, &end);
    }
    if (end == start || errno == ERANGE || !std::isfinite(value)) return std::nullopt;
    while (std::isspace(static_cast<unsigned char>(*end))) ++end;
    if (*end) {
        std::string_view unit(end);
        while (!unit.empty() && std::isspace(static_cast<unsigned char>(unit.back())))
            unit.remove_suffix(1);
        const auto match = std::find_if(std::begin(timeoutUnits), std::end(timeoutUnits),
            [&](const TimeoutUnit& candidate) { return candidate.name == unit; });
        if (match == std::end(timeoutUnits)) return std::nullopt;
        value *= match->milliseconds;
        if (match + 1 != std::end(timeoutUnits)) {
            const double smaller = (match + 1)->milliseconds;
            value = std::rint(value / smaller) * smaller;
        }
    }
    value = std::rint(value);
    if (!std::isfinite(value) || value < minimum || value > INT_MAX) return std::nullopt;
    return static_cast<int>(value);
}

inline int timeoutMinimum(TimeoutParameter parameter, std::string_view name) {
    return parameter == TimeoutParameter::Deadlock && name == "deadlock_timeout" ? 1 : 0;
}

inline std::string formatTimeoutMs(int value) {
    if (value == 0) return "0";
    for (const auto& unit : timeoutUnits) {
        const int divisor = static_cast<int>(unit.milliseconds);
        if (divisor && value % divisor == 0)
            return std::to_string(value / divisor) + std::string(unit.name);
    }
    return std::to_string(value) + "ms";
}

class LockManager;

void publishTimeoutDefaults(int statement, int lock, int deadlock);

class SessionTimeouts {
    using Overrides = std::array<std::optional<int>, timeoutNames.size()>;
    std::array<int, timeoutNames.size()> instanceDefaults_{0, 0, 1000};
    std::uint64_t defaultsVersion_ = 0;
    Overrides startup_{};
    SessionSetting<Overrides> settings_;
public:
    void refreshDefaults(LockManager& locks);
    void setStartup(TimeoutParameter parameter, int value) {
        startup_[static_cast<size_t>(parameter)] = value;
    }
    int get(TimeoutParameter parameter) const {
        const auto index = static_cast<size_t>(parameter);
        return settings_.get()[index].value_or(startup_[index].value_or(instanceDefaults_[index]));
    }
    void set(TimeoutParameter parameter, std::optional<int> value, bool local) {
        settings_.update([&](Overrides& target) {
            target[static_cast<size_t>(parameter)] = value;
        }, local);
    }
    void resetAll() { settings_.assign({}, false); }
    void begin() { settings_.begin(); }
    void finish(bool commit, LockManager& locks);
    void save(const std::string& name) { settings_.save(name); }
    void rollback(const std::string& name, LockManager& locks);
    void release(const std::string& name) { settings_.release(name); }
    void syncLocks(LockManager& locks) const;
};

} // namespace dbms
