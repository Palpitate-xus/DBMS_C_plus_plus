#include "GUC.h"
#include <cctype>
#include <algorithm>
#include <limits>

namespace dbms {

static std::string normalizeName(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return s;
}

GUCRegistry& GUCRegistry::instance() {
    static GUCRegistry reg;
    return reg;
}

void GUCRegistry::initialize() {
    if (!vars_.empty()) return;

    auto add = [this](const GUCVariable& v) { define(v); };

    add({"search_path", "schema search order", "", "public", "public"});
    add({"timezone", "time zone for timestamp display", "", "UTC", "UTC"});
    add({"datestyle", "display format for date/time values", "", "ISO, MDY", "ISO, MDY"});
    add({"default_transaction_isolation", "default isolation level", "",
         "read committed", "read committed", GucType::Enum, GucContext::UserSet, 0, 0,
         {"read uncommitted", "read committed", "repeatable read", "serializable"}});
    add({"application_name", "application name for statistics and logging", "", "", ""});
    add({"client_encoding", "client character set encoding", "", "UTF8", "UTF8"});
    add({"standard_conforming_strings", "treat backslashes literally in strings", "",
         "on", "on", GucType::Bool});
    add({"autocommit", "autocommit mode (non-PG compat)", "", "on", "on", GucType::Bool});
}

bool GUCRegistry::define(const GUCVariable& var) {
    std::string key = normalizeName(var.name);
    if (vars_.count(key)) return false;
    vars_[key] = var;
    vars_[key].name = key;
    return true;
}

const GUCVariable* GUCRegistry::find(const std::string& name) const {
    auto it = vars_.find(normalizeName(name));
    if (it != vars_.end()) return &it->second;
    return nullptr;
}

std::optional<std::string> GUCRegistry::get(const std::string& name) const {
    auto v = find(name);
    if (!v) return std::nullopt;
    return v->value;
}

std::string GUCRegistry::set(const std::string& name, const std::string& value,
                             bool isLocal, bool isSuperuser) {
    (void)isLocal; // session/local distinction can be layered later
    auto v = find(name);
    if (!v) return "unrecognized configuration parameter \"" + name + "\"";
    if (v->context == GucContext::Internal) return "parameter \"" + name + "\" cannot be changed";
    if (v->context == GucContext::SuperuserSet && !isSuperuser) {
        return "permission denied to set parameter \"" + name + "\"";
    }
    if (v->type == GucType::Bool) {
        std::string lv = normalizeName(value);
        if (lv != "on" && lv != "off" && lv != "true" && lv != "false" &&
            lv != "yes" && lv != "no" && lv != "1" && lv != "0") {
            return "parameter \"" + name + "\" requires a Boolean value";
        }
    } else if (v->type == GucType::Enum && !v->allowedValues.empty()) {
        std::string lv = normalizeName(value);
        bool ok = false;
        for (const auto& av : v->allowedValues) {
            if (normalizeName(av) == lv) { ok = true; break; }
        }
        if (!ok) return "invalid value for parameter \"" + name + "\": \"" + value + "\"";
    } else if (v->type == GucType::Int) {
        try {
            int iv = std::stoi(value);
            if (iv < v->minValue || iv > v->maxValue) {
                return "value " + value + " out of range for parameter \"" + name + "\"";
            }
        } catch (...) {
            return "parameter \"" + name + "\" requires an integer value";
        }
    }
    if (v->checkHook && !v->checkHook(value)) {
        return "invalid value for parameter \"" + name + "\": \"" + value + "\"";
    }
    vars_[normalizeName(name)].value = value;
    vars_[normalizeName(name)].assigned = true;
    return {};
}

std::string GUCRegistry::reset(const std::string& name) {
    auto it = vars_.find(normalizeName(name));
    if (it == vars_.end()) return "unrecognized configuration parameter \"" + name + "\"";
    it->second.value = it->second.bootValue;
    it->second.assigned = false;
    return {};
}

void GUCRegistry::resetAll() {
    for (auto& kv : vars_) {
        kv.second.value = kv.second.bootValue;
        kv.second.assigned = false;
    }
}

std::vector<const GUCVariable*> GUCRegistry::all() const {
    std::vector<const GUCVariable*> result;
    for (const auto& kv : vars_) result.push_back(&kv.second);
    return result;
}

} // namespace dbms
