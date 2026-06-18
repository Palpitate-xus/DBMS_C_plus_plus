#include "type_registry.h"

#include <cctype>
#include <cstdint>
#include <limits>
#include <sstream>

namespace dbms {

namespace {

std::string toLower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

bool parseInt(const std::string& s, int64_t& out) {
    if (s.empty()) return false;
    char* end = nullptr;
    out = std::strtoll(s.c_str(), &end, 10);
    return *end == '\0';
}

} // anonymous namespace

TypeRegistry& TypeRegistry::instance() {
    static TypeRegistry reg;
    static bool bootstrapped = false;
    if (!bootstrapped) {
        reg.bootstrap();
        bootstrapped = true;
    }
    return reg;
}

void TypeRegistry::registerType(const TypeEntry& entry, const std::vector<std::string>& aliases) {
    const std::string canonical = toLower(entry.canonicalName);
    TypeEntry copy = entry;
    copy.canonicalName = canonical;
    canonicalToEntry_[canonical] = copy;
    aliasToCanonical_[canonical] = canonical;
    for (const auto& a : aliases) {
        aliasToCanonical_[toLower(a)] = canonical;
    }
}

std::string TypeRegistry::normalizeTypeName(const std::string& name) const {
    auto it = aliasToCanonical_.find(toLower(name));
    return it == aliasToCanonical_.end() ? std::string() : it->second;
}

const TypeEntry* TypeRegistry::findType(const std::string& canonicalOrAlias) const {
    std::string canonical = normalizeTypeName(canonicalOrAlias);
    if (canonical.empty()) return nullptr;
    auto it = canonicalToEntry_.find(canonical);
    return it == canonicalToEntry_.end() ? nullptr : &it->second;
}

TypeModResult TypeRegistry::applyTypeMods(const std::string& canonicalName,
                                          const std::vector<std::string>& mods) const {
    const TypeEntry* entry = findType(canonicalName);
    TypeModResult res;
    if (!entry) {
        res.error = "unknown type: " + canonicalName;
        return res;
    }

    if (mods.empty()) {
        res.typmod = entry->defaultTypeMod;
        res.dsize = entry->typlen > 0 ? static_cast<size_t>(entry->typlen) : entry->maxLength;
        res.isVariableLength = entry->typlen < 0;
        return res;
    }

    if (!entry->hasTypeMod) {
        res.error = "type " + canonicalName + " does not accept type modifiers";
        return res;
    }

    // 单修饰符：长度 / 精度
    if (mods.size() == 1) {
        int64_t n = 0;
        if (!parseInt(mods[0], n)) {
            res.error = "invalid type modifier for " + canonicalName + ": " + mods[0];
            return res;
        }
        if (n < 0) {
            res.error = "type modifier must be non-negative";
            return res;
        }

        const std::string& c = entry->canonicalName;
        if (c == "character" || c == "character varying" || c == "bit" || c == "bit varying" ||
            c == "varchar" || c == "char" || c == "nchar" || c == "nvarchar" ||
            c == "binary" || c == "varbinary") {
            size_t len = static_cast<size_t>(n);
            if (entry->maxLength > 0 && len > entry->maxLength) {
                res.error = "length " + std::to_string(len) + " exceeds maximum " +
                            std::to_string(entry->maxLength) + " for type " + canonicalName;
                return res;
            }
            res.typmod = static_cast<int32_t>(len);
            res.dsize = len;
            res.isVariableLength = entry->typlen < 0;
        } else if (c == "numeric" || c == "decimal") {
            int64_t precision = n;
            if (precision < 1 || precision > 1000) {
                res.error = "numeric precision must be between 1 and 1000";
                return res;
            }
            res.typmod = static_cast<int32_t>((precision + 4) << 16);
            res.dsize = 8; // 内部仍按 double 存储，后续改为变长 decimal
            res.isVariableLength = false;
        } else if (c == "timestamp" || c == "timestamptz" || c == "time" || c == "timetz") {
            int64_t prec = n;
            if (prec < 0 || prec > 6) {
                res.error = "timestamp precision must be between 0 and 6";
                return res;
            }
            res.typmod = static_cast<int32_t>(prec);
            res.dsize = entry->typlen > 0 ? static_cast<size_t>(entry->typlen) : entry->maxLength;
            res.isVariableLength = entry->typlen < 0;
        } else if (c == "interval") {
            // 简化：interval(p) 保留精度，不改宽度
            int64_t prec = n;
            if (prec < 0 || prec > 6) {
                res.error = "interval precision must be between 0 and 6";
                return res;
            }
            res.typmod = static_cast<int32_t>(prec);
            res.dsize = entry->maxLength;
            res.isVariableLength = true;
        } else if (c == "geometry" || c == "geography") {
            // PostGIS 风格 SRID/类型修饰符，暂时仅记录
            res.typmod = static_cast<int32_t>(n);
            res.dsize = entry->maxLength;
            res.isVariableLength = true;
        } else {
            res.error = "unsupported single type modifier for " + canonicalName;
        }
        return res;
    }

    // 双修饰符：NUMERIC(precision, scale)
    if (mods.size() == 2) {
        if (entry->canonicalName != "numeric" && entry->canonicalName != "decimal") {
            res.error = "type " + canonicalName + " does not accept two modifiers";
            return res;
        }
        int64_t precision = 0, scale = 0;
        if (!parseInt(mods[0], precision) || !parseInt(mods[1], scale)) {
            res.error = "invalid numeric modifiers";
            return res;
        }
        if (precision < 1 || precision > 1000 || scale < 0 || scale > precision) {
            res.error = "numeric precision/scale out of range";
            return res;
        }
        res.typmod = static_cast<int32_t>(((precision + 4) << 16) + (scale & 0xFFFF));
        res.dsize = 8;
        res.isVariableLength = false;
        return res;
    }

    res.error = "too many type modifiers for " + canonicalName;
    return res;
}

std::string TypeRegistry::resolveColumnType(Column& col,
                                            const std::string& typeName,
                                            const std::vector<std::string>& typeMods,
                                            bool isArray) const {
    std::string canonical = normalizeTypeName(typeName);
    if (canonical.empty()) {
        return "unknown type: " + typeName;
    }

    const TypeEntry* entry = findType(canonical);
    if (!entry) {
        return "unknown type: " + typeName;
    }

    // ENUM 特殊处理：保留原值列表
    if (entry->category == TypeCategory::Enum) {
        col.dataType = "enum";
        col.isVariableLength = false;
        col.dsize = 4; // 内部存枚举索引
        col.isArray = isArray;
        return "";
    }

    // 数组：先解析基类型，再标记为数组
    if (isArray) {
        TypeModResult base = applyTypeMods(canonical, typeMods);
        if (!base.ok()) return base.error;
        col.dataType = canonical + "[]";
        col.dsize = base.dsize;
        col.isVariableLength = true;
        col.isArray = true;
        return "";
    }

    TypeModResult r = applyTypeMods(canonical, typeMods);
    if (!r.ok()) return r.error;

    col.dataType = canonical;
    col.dsize = r.dsize;
    col.isVariableLength = r.isVariableLength;
    col.isArray = false;

    // 同步 dataName（列名）不应由类型系统设置，但保留调用者已设置的值
    return "";
}

std::string TypeRegistry::validateColumn(Column& col) const {
    if (col.dataType.empty()) {
        return "column '" + col.dataName + "' has no data type";
    }

    std::string baseType = col.dataType;
    bool isArray = false;
    if (baseType.size() >= 2 && baseType.substr(baseType.size() - 2) == "[]") {
        isArray = true;
        baseType = baseType.substr(0, baseType.size() - 2);
    }

    const TypeEntry* entry = findType(baseType);
    if (!entry) {
        return "column '" + col.dataName + "' has unknown type: " + col.dataType;
    }

    // 补齐 dsize（当调用者未设置或设置为 0 时）
    if (col.dsize == 0) {
        if (isArray) {
            col.dsize = entry->typlen > 0 ? static_cast<size_t>(entry->typlen) : entry->maxLength;
            col.isVariableLength = true;
        } else if (entry->typlen > 0) {
            col.dsize = static_cast<size_t>(entry->typlen);
            col.isVariableLength = false;
        } else {
            col.dsize = entry->maxLength;
            col.isVariableLength = true;
        }
    }

    // 修正 isVariableLength 标记：以注册表为准
    if (!isArray) {
        col.isVariableLength = entry->typlen < 0;
    } else {
        col.isVariableLength = true;
    }

    return "";
}

void TypeRegistry::bootstrap() {
    registerNumericTypes();
    registerStringTypes();
    registerBinaryTypes();
    registerDateTimeTypes();
    registerGeometricTypes();
    registerNetworkTypes();
    registerBitTypes();
    registerTextSearchTypes();
    registerUuidTypes();
    registerXmlJsonTypes();
    registerRangeTypes();
    registerEnumCompositeDomainPseudoTypes();
}

void TypeRegistry::registerNumericTypes() {
    registerType({"boolean", 1, 'c', 'p', TypeCategory::Boolean, false, -1, 0, false},
                 {"bool"});

    registerType({"smallint", 2, 's', 'p', TypeCategory::Numeric, false, -1, 0, true},
                 {"int2"});
    registerType({"integer", 4, 'i', 'p', TypeCategory::Numeric, false, -1, 0, true},
                 {"int", "int4"});
    registerType({"bigint", 8, 'd', 'p', TypeCategory::Numeric, false, -1, 0, true},
                 {"int8"});

    registerType({"real", 4, 'i', 'p', TypeCategory::Numeric, false, -1, 0, true},
                 {"float", "float4"});
    registerType({"double precision", 8, 'd', 'p', TypeCategory::Numeric, false, -1, 0, true},
                 {"double", "float8"});

    registerType({"numeric", 8, 'd', 'p', TypeCategory::Numeric, true, -1, 0, false},
                 {"decimal"}); // 当前内部用 double，后续改为变长 decimal

    // SERIAL 是宏，不是真正类型；注册为别名以便解析时识别
    registerType({"smallserial", 2, 's', 'p', TypeCategory::Numeric, false, -1, 0, false},
                 {"serial2"});
    registerType({"serial", 4, 'i', 'p', TypeCategory::Numeric, false, -1, 0, false});
    registerType({"bigserial", 8, 'd', 'p', TypeCategory::Numeric, false, -1, 0, false},
                 {"serial8"});

    // MySQL 风格无符号别名（保留兼容）
    registerType({"smallint unsigned", 2, 's', 'p', TypeCategory::Numeric, false, -1, 0, false});
    registerType({"integer unsigned", 4, 'i', 'p', TypeCategory::Numeric, false, -1, 0, false},
                 {"int unsigned"});
    registerType({"bigint unsigned", 8, 'd', 'p', TypeCategory::Numeric, false, -1, 0, false});
}

void TypeRegistry::registerStringTypes() {
    registerType({"character", -1, 'c', 'p', TypeCategory::String, true, 1, 1005, false},
                 {"char"});
    registerType({"character varying", -1, 'c', 'p', TypeCategory::String, true, -1, 65535, false},
                 {"varchar"});
    registerType({"text", -1, 'c', 'x', TypeCategory::String, false, -1, 65535, false});

    registerType({"nchar", -1, 'c', 'p', TypeCategory::String, true, 1, 4000, false});
    registerType({"nvarchar", -1, 'c', 'p', TypeCategory::String, true, -1, 4000, false});
}

void TypeRegistry::registerBinaryTypes() {
    registerType({"bytea", -1, 'i', 'x', TypeCategory::Binary, false, -1, 65535, false});
    registerType({"binary", -1, 'i', 'p', TypeCategory::Binary, true, 1, 1005, false});
    registerType({"varbinary", -1, 'i', 'p', TypeCategory::Binary, true, -1, 65535, false});
    registerType({"blob", -1, 'i', 'x', TypeCategory::Binary, false, -1, 65535, false});
}

void TypeRegistry::registerDateTimeTypes() {
    registerType({"date", 12, 'i', 'p', TypeCategory::DateTime, false, -1, 0, false});
    registerType({"time", 4, 'i', 'p', TypeCategory::DateTime, true, -1, 0, false});
    registerType({"timetz", 4, 'i', 'p', TypeCategory::DateTime, true, -1, 0, false});
    registerType({"timestamp", 8, 'd', 'p', TypeCategory::DateTime, true, -1, 0, false});
    registerType({"timestamptz", 8, 'd', 'p', TypeCategory::DateTime, true, -1, 0, false});
    registerType({"datetime", 8, 'd', 'p', TypeCategory::DateTime, false, -1, 0, false});
    registerType({"interval", -1, 'd', 'p', TypeCategory::DateTime, true, -1, 64, false});
}

void TypeRegistry::registerGeometricTypes() {
    registerType({"point", 16, 'd', 'p', TypeCategory::Geometric, false, -1, 0, false});
    registerType({"line", 32, 'd', 'p', TypeCategory::Geometric, false, -1, 0, false});
    registerType({"lseg", 32, 'd', 'p', TypeCategory::Geometric, false, -1, 0, false});
    registerType({"box", 32, 'd', 'p', TypeCategory::Geometric, false, -1, 0, false});
    registerType({"path", -1, 'd', 'p', TypeCategory::Geometric, false, -1, 4096, false});
    registerType({"polygon", -1, 'd', 'p', TypeCategory::Geometric, false, -1, 4096, false});
    registerType({"circle", 24, 'd', 'p', TypeCategory::Geometric, false, -1, 0, false});
}

void TypeRegistry::registerNetworkTypes() {
    registerType({"inet", 20, 'c', 'p', TypeCategory::Network, false, -1, 0, false});
    registerType({"cidr", 20, 'c', 'p', TypeCategory::Network, false, -1, 0, false});
    registerType({"macaddr", 6, 'c', 'p', TypeCategory::Network, false, -1, 0, false});
    registerType({"macaddr8", 8, 'c', 'p', TypeCategory::Network, false, -1, 0, false});
}

void TypeRegistry::registerBitTypes() {
    registerType({"bit", -1, 'c', 'p', TypeCategory::BitString, true, 1, 8388608, false});
    registerType({"bit varying", -1, 'c', 'p', TypeCategory::BitString, true, -1, 8388608, false},
                 {"varbit"});
}

void TypeRegistry::registerTextSearchTypes() {
    registerType({"tsvector", -1, 'i', 'x', TypeCategory::TextSearch, false, -1, 65535, false});
    registerType({"tsquery", -1, 'i', 'x', TypeCategory::TextSearch, false, -1, 65535, false});
}

void TypeRegistry::registerUuidTypes() {
    registerType({"uuid", 36, 'c', 'p', TypeCategory::UUID, false, -1, 0, false});
}

void TypeRegistry::registerXmlJsonTypes() {
    registerType({"xml", -1, 'i', 'x', TypeCategory::XML, false, -1, 65535, false});
    registerType({"json", -1, 'i', 'x', TypeCategory::JSON, false, -1, 65535, false});
    registerType({"jsonb", -1, 'i', 'x', TypeCategory::JSON, false, -1, 65535, false});
    registerType({"jsonpath", -1, 'i', 'x', TypeCategory::JSONPath, false, -1, 65535, false});
}

void TypeRegistry::registerRangeTypes() {
    registerType({"int4range", -1, 'i', 'x', TypeCategory::Range, false, -1, 64, false});
    registerType({"int8range", -1, 'd', 'x', TypeCategory::Range, false, -1, 64, false});
    registerType({"numrange", -1, 'i', 'x', TypeCategory::Range, false, -1, 128, false});
    registerType({"tsrange", -1, 'd', 'x', TypeCategory::Range, false, -1, 64, false});
    registerType({"tstzrange", -1, 'd', 'x', TypeCategory::Range, false, -1, 64, false});
    registerType({"daterange", -1, 'i', 'x', TypeCategory::Range, false, -1, 64, false});
}

void TypeRegistry::registerEnumCompositeDomainPseudoTypes() {
    // ENUM 作为伪基类型，真实 ENUM 值列表由 Column.enumValues 承载
    registerType({"enum", 4, 'i', 'p', TypeCategory::Enum, false, -1, 0, false});

    // 复合 / 域 / 伪类型占位
    registerType({"record", -1, 'd', 'x', TypeCategory::Composite, false, -1, 65535, false});
    registerType({"anyelement", -1, 'i', 'p', TypeCategory::Pseudo, false, -1, 0, false});
    registerType({"anyarray", -1, 'i', 'p', TypeCategory::Pseudo, false, -1, 0, false});
    registerType({"anynonarray", -1, 'i', 'p', TypeCategory::Pseudo, false, -1, 0, false});
    registerType({"anyenum", -1, 'i', 'p', TypeCategory::Pseudo, false, -1, 0, false});
    registerType({"anyrange", -1, 'i', 'p', TypeCategory::Pseudo, false, -1, 0, false});
    registerType({"void", 0, 'i', 'p', TypeCategory::Pseudo, false, -1, 0, false});
}

} // namespace dbms
