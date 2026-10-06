#pragma once

#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <algorithm>
#include <cctype>
#include <set>
#include <string>
#include <vector>

namespace dbms {
namespace common_type_detail {

inline std::string canonical(std::string name) {
    const auto first = name.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "unknown";
    name = name.substr(first, name.find_last_not_of(" \t\r\n") - first + 1);
    bool array = false;
    while (name.size() >= 2 && name.compare(name.size()-2, 2, "[]") == 0) {
        array = true; name.resize(name.size()-2);
    }
    if (const auto modifier = name.find('('); modifier != std::string::npos)
        name.resize(modifier);
    const auto end = name.find_last_not_of(" \t\r\n");
    if (end != std::string::npos) name.resize(end+1);
    std::transform(name.begin(),name.end(),name.begin(),[](unsigned char c){return std::tolower(c);});
    const auto normalized = TypeRegistry::instance().normalizeTypeName(name);
    if (!normalized.empty()) name = normalized;
    if (name == "character varying") name = "varchar";
    if (name == "character") name = "bpchar";
    if (name == "timestamp without time zone") name = "timestamp";
    if (name == "timestamp with time zone") name = "timestamptz";
    if (name == "time without time zone") name = "time";
    if (name == "time with time zone") name = "timetz";
    return name + (array ? "[]" : "");
}
inline bool array(const std::string& type) {
    return type.size() >= 2 && type.compare(type.size()-2,2,"[]") == 0;
}
inline int numericRank(const std::string& type) {
    const std::vector<std::string> chain = {"smallint","integer","bigint","numeric","real","double precision"};
    const auto position = std::find(chain.begin(),chain.end(),type);
    return position == chain.end() ? 0 : static_cast<int>(position-chain.begin()+1);
}
inline bool oidType(const std::string& type) {
    static const std::set<std::string> names = {"oid","regproc","regprocedure","regoper","regoperator",
        "regclass","regtype","regconfig","regdictionary","regrole","regnamespace","regcollation"};
    return names.count(type);
}
inline char category(const std::string& type) {
    if (array(type)) return 'A';
    if (type == "interval") return 'T'; // not the datetime category
    if (oidType(type)) return 'N';
    if (type == "\"char\"" || type == "bpchar" || type == "name") return 'S';
    const auto* entry = TypeRegistry::instance().findType(type);
    if (!entry) return 'U';
    switch(entry->category) {
    case TypeCategory::Boolean: return 'B';
    case TypeCategory::Numeric: return 'N';
    case TypeCategory::String: return 'S';
    case TypeCategory::DateTime: return 'D';
    case TypeCategory::Geometric: return 'G';
    case TypeCategory::Network: return type == "inet" || type == "cidr" ? 'I' : 'U';
    case TypeCategory::BitString: return 'V';
    case TypeCategory::Array: return 'A';
    case TypeCategory::Composite: return 'C';
    case TypeCategory::Enum: return 'E';
    case TypeCategory::Range: return 'R';
    default: return 'U';
    }
}
inline bool preferred(const std::string& type) {
    static const std::set<std::string> names = {"boolean","text","double precision","timestamptz","interval","inet","bit varying"};
    return names.count(type);
}
// Builtin implicit, not assignment/explicit, cast edges. No expression or
// datum is evaluated here, and implicit paths are not guessed transitively.
inline bool implicit(const std::string& from, const std::string& to) {
    if (from == to || from == "unknown") return true;
    if (array(from) && array(to))
        return implicit(from.substr(0,from.size()-2),to.substr(0,to.size()-2));
    const int left = numericRank(from), right = numericRank(to);
    if (left && right) return left <= right;
    if (oidType(to) && (from == "smallint" || from == "integer" || from == "bigint")) return true;
    if ((from == "oid" && oidType(to)) || (oidType(from) && to == "oid")) return true;
    if ((from == "regproc" && to == "regprocedure") || (from == "regprocedure" && to == "regproc") ||
        (from == "regoper" && to == "regoperator") || (from == "regoperator" && to == "regoper")) return true;
    static const std::set<std::pair<std::string,std::string>> edges = {
        {"text","bpchar"},{"text","varchar"},{"bpchar","text"},{"bpchar","varchar"},
        {"varchar","text"},{"varchar","bpchar"},{"name","text"},{"text","name"},
        {"bpchar","name"},{"varchar","name"},{"\"char\"","text"},
        {"date","timestamp"},{"date","timestamptz"},{"timestamp","timestamptz"},
        {"time","timetz"},{"time","interval"},{"cidr","inet"},
        {"bit","bit varying"},{"bit varying","bit"},
        {"macaddr","macaddr8"},{"macaddr8","macaddr"}
    };
    return edges.count({from,to});
}
} // namespace common_type_detail

// The caller supplies semantic input order: CASE uses ELSE first, THENs in
// source order. Unknown-only inputs become TEXT before any outer context.
// Same named custom types are retained; domain-base/custom-cast catalog
// resolution is a separate metadata capability, not inferred from values.
inline std::string selectCommonType(const std::vector<std::string>& inputs,
                                    const std::string& context) {
    using namespace common_type_detail;
    std::vector<std::string> types;
    for (const auto& input : inputs) types.push_back(canonical(input));
    if (types.empty()) return "text";
    if (types.front() != "unknown" &&
        std::all_of(types.begin(),types.end(),[&](const auto& type){return type == types.front();}))
        return types.front();
    std::string candidate = "unknown";
    for (const auto& type : types) {
        if (type == "unknown") continue;
        if (candidate == "unknown") candidate = type;
        else if (candidate != type) {
            if (category(candidate) != category(type))
                throw DbError("42804",context+" types "+candidate+" and "+type+" cannot be matched");
            if (!preferred(candidate) && implicit(candidate,type) && !implicit(type,candidate))
                candidate = type;
        }
    }
    if (candidate == "unknown") candidate = "text";
    for (const auto& type : types)
        if (!implicit(type,candidate))
            throw DbError("42846",context+" could not convert type "+type+" to "+candidate);
    return candidate;
}
} // namespace dbms
