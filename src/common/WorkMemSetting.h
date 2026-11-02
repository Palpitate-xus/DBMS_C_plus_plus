#pragma once

#include <cerrno>
#include <cctype>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace dbms {

// PostgreSQL 18 guc.c parses integer memory settings in this order: base-0
// integer, decimal/exponent fallback, next-smaller-unit rounding, then integer
// rounding. Units are case-sensitive; the work_mem range is 64..INT_MAX kB.
inline std::optional<size_t> parseWorkMemKb(std::string input) {
    if (input.find('\0') != std::string::npos) return std::nullopt;
    const auto* begin = input.c_str();
    char* end = nullptr;
    errno = 0;
    double value = std::strtol(begin, &end, 0);
    if (*end == '.' || *end == 'e' || *end == 'E' || errno == ERANGE) {
        errno = 0;
        value = std::strtod(begin, &end);
    }
    if (end == begin || errno == ERANGE || !std::isfinite(value)) return std::nullopt;
    while (std::isspace(static_cast<unsigned char>(*end))) ++end;
    if (*end) {
        const char* unit = end;
        while (*end && !std::isspace(static_cast<unsigned char>(*end))) ++end;
        if (end - unit > 2) return std::nullopt;
        const std::string name(unit, static_cast<size_t>(end - unit));
        while (std::isspace(static_cast<unsigned char>(*end))) ++end;
        if (*end) return std::nullopt;
        double multiplier = 0, next = 0;
        if (name == "TB") { multiplier = 1073741824.0; next = 1048576.0; }
        else if (name == "GB") { multiplier = 1048576.0; next = 1024.0; }
        else if (name == "MB") { multiplier = 1024.0; next = 1.0; }
        else if (name == "kB") { multiplier = 1.0; next = 1.0 / 1024.0; }
        else if (name == "B") multiplier = 1.0 / 1024.0;
        else return std::nullopt;
        value *= multiplier;
        if (next) value = std::rint(value / next) * next;
    }
    value = std::rint(value);
    if (!std::isfinite(value) || value < 64 || value > INT_MAX) return std::nullopt;
    return static_cast<size_t>(value);
}

inline std::string formatWorkMemKb(size_t value) {
    static constexpr struct { size_t divisor; const char* unit; } units[] = {
        {1073741824, "TB"}, {1048576, "GB"}, {1024, "MB"}, {1, "kB"}
    };
    for (const auto& unit : units)
        if (value % unit.divisor == 0) return std::to_string(value / unit.divisor) + unit.unit;
    return {}; // The final divisor is one.
}

// A missing override inherits the startup option, or the current instance
// default if there was no startup option. Never freeze that default in a SET
// snapshot: RESET and an aborted transaction must still inherit later reloads.
class SessionWorkMem {
    using Setting = std::optional<size_t>;
    struct Savepoint { std::string name; Setting visible, persistent; };
    Setting startup_, visible_, original_, persistent_;
    bool active_ = false;
    std::vector<Savepoint> savepoints_;
public:
    size_t effective(size_t instanceDefault) const {
        return visible_.value_or(startup_.value_or(instanceDefault));
    }
    void assign(Setting value, bool local) {
        if (local && !active_) return;
        visible_ = value;
        if (active_ && !local) persistent_ = value;
    }
    void setStartup(size_t value) {
        startup_ = value;
        visible_.reset(); original_.reset(); persistent_.reset();
        active_ = false; savepoints_.clear();
    }
    void begin() {
        if (active_) return;
        original_ = persistent_ = visible_;
        active_ = true; savepoints_.clear();
    }
    void finish(bool commit) {
        if (!active_) return;
        visible_ = commit ? persistent_ : original_;
        active_ = false;
        original_.reset(); persistent_.reset(); savepoints_.clear();
    }
    void save(std::string name) {
        begin(); savepoints_.push_back({std::move(name), visible_, persistent_});
    }
    void rollback(std::string name) {
        for (size_t index = savepoints_.size(); index; --index)
            if (savepoints_[index - 1].name == name) {
                visible_ = savepoints_[index - 1].visible;
                persistent_ = savepoints_[index - 1].persistent;
                savepoints_.resize(index); return;
            }
    }
    void release(std::string name) {
        for (size_t index = savepoints_.size(); index; --index)
            if (savepoints_[index - 1].name == name) {
                savepoints_.resize(index - 1); return;
            }
    }
};

} // namespace dbms
