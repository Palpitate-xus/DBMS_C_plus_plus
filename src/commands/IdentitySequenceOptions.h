#pragma once
#include "common/DbError.h"
#include "interfaces/table_schema.h"
#include "catalog/type_registry.h"
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>
namespace dbms::identity_sequence {
struct Options {
    SequenceInfo sequence;
    std::optional<std::string> name;
    std::optional<std::string> requestedOwner;
    char persistence = 0;
    bool restartSpecified = false;
    std::optional<int64_t> restart;
};
inline int64_t integer(const std::string& raw) {
    size_t at = !raw.empty() && (raw[0] == '+' || raw[0] == '-') ? 1 : 0;
    if (at == raw.size()) throw DbError("22P02", "invalid input syntax for type bigint: \"" + raw + "\"");
    for (size_t i = at; i < raw.size(); ++i)
        if (raw[i] < '0' || raw[i] > '9')
            throw DbError("22P02", "invalid input syntax for type bigint: \"" + raw + "\"");
    errno = 0; char* end = nullptr; const auto value = std::strtoll(raw.c_str(), &end, 10);
    if (errno == ERANGE) throw DbError("22003", "value \"" + raw + "\" is out of range for type bigint");
    if (end != raw.c_str() + raw.size()) throw DbError("22P02", "invalid input syntax for type bigint");
    return static_cast<int64_t>(value);
}
inline std::pair<int64_t, int64_t> bounds(const Column& column) {
    const auto* type = TypeRegistry::instance().findType(column.dataType);
    if (column.isArray || !column.domainName.empty() || !type)
        throw DbError("22023", "identity column type must be smallint, integer, or bigint");
    if (type->canonicalName == "smallint") return {std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max()};
    if (type->canonicalName == "int" || type->canonicalName == "integer") return {std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
    if (type->canonicalName == "bigint") return {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
    throw DbError("22023", "identity column type must be smallint, integer, or bigint");
}
inline Options parameters(const Column& column,
    const std::vector<std::pair<std::string, std::string>>& options,
    const SequenceInfo* copied = nullptr, bool copyForLike = false) {
    Options result; auto& info = result.sequence;
    (void)bounds(column);
    const auto& columnName = TypeRegistry::instance().findType(column.dataType)->canonicalName;
    const uint32_t columnType = columnName == "smallint" ? 21 :
        (columnName == "int" || columnName == "integer") ? 23 : 20;
    // LIKE copies sequence_options(), which excludes AS. PostgreSQL creates
    // an int8 generator with the copied parameters; ALTER uses the actual
    // existing generator type even if it differs from the owner column.
    const uint32_t sequenceType = copied ? (copyForLike ? 20 : copied->dataTypeOid) : columnType;
    if (!SequenceInfo::validDataType(sequenceType)) throw DbError("XX001","invalid stored identity sequence type");
    const auto low = SequenceInfo::typeMinimum(sequenceType), high = SequenceInfo::typeMaximum(sequenceType);
    // Identity has already supplied AS <column type>. Preserve every raw
    // option first; duplicate diagnostics precede conversion of its values.
    std::set<std::string> seen{"as"};
    for (const auto& [key, value] : options) {
        (void)value; std::string canonical = key;
        if (key == "nominvalue") canonical = "minvalue";
        else if (key == "nomaxvalue") canonical = "maxvalue";
        else if (key == "nocycle") canonical = "cycle";
        else if (key == "logged" || key == "unlogged") canonical = "persistence";
        if (!seen.insert(canonical).second) throw DbError("42601", "conflicting or redundant options");
    }
    if (copied) info = *copied;
    info.dataTypeOid = sequenceType;
    info.dataTypeSpecified = !copied || copyForLike;
    std::optional<int64_t> minimum, maximum, start;
    if (copied) { minimum = info.minValue; maximum = info.maxValue; start = info.start; }
    for (const auto& [key, raw] : options) {
        if (key == "increment") info.increment = integer(raw);
        else if (key == "start") start = integer(raw);
        else if (key == "minvalue") minimum = integer(raw);
        else if (key == "maxvalue") maximum = integer(raw);
        else if (key == "nominvalue") minimum.reset();
        else if (key == "nomaxvalue") maximum.reset();
        else if (key == "cache") info.cache = integer(raw);
        else if (key == "cycle") info.cycle = true;
        else if (key == "nocycle") info.cycle = false;
        else if (key == "sequence_name") result.name = raw;
        else if (key == "ownedby") result.requestedOwner = raw;
        else if (key == "logged") result.persistence = 'p';
        else if (key == "unlogged") result.persistence = 'u';
        else if (key == "restart") {
            result.restartSpecified = true;
            if (!raw.empty()) result.restart = integer(raw);
        } else throw DbError("42601", "invalid identity sequence option");
    }
    if (info.increment == 0) throw DbError("22023", "INCREMENT must not be zero");
    if (info.cache <= 0) throw DbError("22023", "CACHE must be greater than zero");
    info.minValue = minimum.value_or(info.increment > 0 ? 1 : low);
    info.maxValue = maximum.value_or(info.increment > 0 ? high : -1);
    if (info.minValue < low || info.minValue > high)
        throw DbError("22023", "MINVALUE is out of range for sequence data type");
    if (info.maxValue < low || info.maxValue > high)
        throw DbError("22023", "MAXVALUE is out of range for sequence data type");
    if (info.minValue >= info.maxValue)
        throw DbError("22023", "MINVALUE must be less than MAXVALUE");
    info.start = start.value_or(info.increment > 0 ? info.minValue : info.maxValue);
    if (info.start < info.minValue || info.start > info.maxValue)
        throw DbError("22023", "START value is outside sequence bounds");
    if (result.restart && (*result.restart < info.minValue || *result.restart > info.maxValue))
        throw DbError("22023", "RESTART value is outside sequence bounds");
    // Native SequenceInfo defaults are int8-wide. Pass the actual resolved
    // column-specific values explicitly; these request flags are not catalog
    // claims about whether the SQL spelled a default.
    info.hasMinValue = info.hasMaxValue = true;
    info.noMinValue = info.noMaxValue = false;
    info.startSpecified = info.incrementSpecified = info.cacheSpecified = info.cycleSpecified = true;
    info.ownedByTable.clear(); info.ownedByColumn.clear(); info.ownedBySpecified = false;
    info.restartSpecified = info.restartValueSpecified = false;
    return result;
}
}
