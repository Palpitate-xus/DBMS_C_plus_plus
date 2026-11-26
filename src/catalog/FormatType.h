#pragma once

#include "catalog/CatalogService.h"
#include "catalog/RelationSearchPath.h"
#include "commands/TableManage.h"
#include "utils/interval_type.h"

#include <algorithm>
#include <optional>
#include <string>
#include <unordered_map>

namespace dbms::type_display {

inline std::string modifier(int32_t value) {
    return "(" + std::to_string(value) + ")";
}

// These SQL grammar names always select the built-in type. In particular
// "bit" and bpchar need their catalog names when an explicit negative
// modifier must survive parsing the result.
inline std::optional<std::string> sqlBuiltinName(Oid oid, std::optional<int32_t> typmod) {
    const int32_t value = typmod.value_or(-1);
    switch (oid) {
        case 16: return "boolean";
        case 20: return "bigint";
        case 21: return "smallint";
        case 23: return "integer";
        case 700: return "real";
        case 701: return "double precision";
        case 114: return "json";
        case 1042:
            if (value < 0 && typmod) return std::nullopt;
            return "character" + (value > 4 ? modifier(value - 4) : "");
        case 1043: return "character varying" + (value > 4 ? modifier(value - 4) : "");
        case 1560:
            if (value < 0 && typmod) return std::nullopt;
            return "bit" + (value >= 0 ? modifier(value) : "");
        case 1562: return "bit varying" + (value >= 0 ? modifier(value) : "");
        case 1083: case 1266:
            return std::string("time") + (value >= 0 ? modifier(value) : "") +
                (oid == 1266 ? " with time zone" : " without time zone");
        case 1114: case 1184:
            return std::string("timestamp") + (value >= 0 ? modifier(value) : "") +
                (oid == 1184 ? " with time zone" : " without time zone");
        case 1186: {
            std::string result = "interval";
            if (value < 0) return result;
            const auto range = interval_type_detail::fields(static_cast<uint32_t>(value) >> 16);
            if (!range) throw DbError("XX000", "invalid interval type modifier");
            if (*range) result += " " + SQLParser::toLower(range);
            const auto precision = value & 0xffff;
            if (precision != interval_type_detail::fullPrecision) result += modifier(precision);
            return result;
        }
        case 1700: {
            if (value < 4) return "numeric";
            // format_type displays the supplied bits, even for modifiers
            // that numeric's SQL input grammar would reject.
            const auto packed = static_cast<uint32_t>(value - 4);
            const auto scale = static_cast<int>((packed & 0x7ff) ^ 0x400) - 0x400;
            return "numeric(" + std::to_string(packed >> 16) + "," + std::to_string(scale) + ")";
        }
        default: return std::nullopt;
    }
}

inline bool arrayType(const PgTypeRow& row) {
    return row.typcategory == 'A' && row.typelem && row.typstorage != 'p';
}

inline const PgTypeRow* builtinRow(Oid oid) {
    static const auto byOid = [] {
        std::unordered_map<Oid, const PgTypeRow*> rows;
        for (const auto& row : CatalogManager::builtinTypeRows()) rows.emplace(row.oid, &row);
        return rows;
    }();
    const auto found = byOid.find(oid);
    return found == byOid.end() ? nullptr : found->second;
}

inline std::string catalogName(const PgTypeRow& type, std::optional<int32_t> typmod) {
    auto result = SQLParser::quoteIdentifier(type.typname);
    if (typmod && *typmod >= 0) {
        if (type.typmodout) throw DbError("0A000", "type modifier output function is not supported");
        result += modifier(*typmod);
    }
    return result;
}

inline std::string format(StorageEngine& engine, const std::string& database,
                          Oid oid, std::optional<int32_t> typmod, const Session* session) {
    if (!oid) return "-";
    const auto* builtin = builtinRow(oid);
    if (builtin && builtin->typnamespace == 11) {
        if (!arrayType(*builtin)) {
            if (auto name = sqlBuiltinName(oid, typmod)) return *name;
        } else if (const auto* element = builtinRow(builtin->typelem);
                   element && element->typarray == oid) {
            if (auto name = sqlBuiltinName(element->oid, typmod)) return *name + "[]";
            builtin = element;
        }
    }

    const auto path = effectiveRelationSearchPath(session);
    if (builtin && builtin->typnamespace == 11) {
        // With pg_catalog first, an immutable built-in identity cannot be shadowed.
        if (!path.empty() && path.front() == "pg_catalog")
            return catalogName(*builtin, typmod) + (builtin->oid == oid ? "" : "[]");
    }

    const auto catalog = database.empty() ? CatalogManager::MetadataSnapshot{} :
        engine.catalogService().metadataSnapshot(database, false);
    const auto& types = catalog.types.empty() && catalog.namespaces.empty()
        ? CatalogManager::builtinTypeRows() : catalog.types;
    const auto& spaces = catalog.types.empty() && catalog.namespaces.empty()
        ? CatalogManager::builtinNamespaceRows() : catalog.namespaces;
    const auto typeByOid = [&](Oid wanted) -> const PgTypeRow* {
        const auto row = std::find_if(types.begin(), types.end(),
            [wanted](const auto& type) { return type.oid == wanted; });
        return row == types.end() ? nullptr : &*row;
    };
    const auto* type = typeByOid(oid);
    if (!type) return "???";
    bool array = false;
    if (arrayType(*type)) {
        const auto* element = typeByOid(type->typelem);
        if (!element) return std::string("???") + "[]";
        if (element->typarray == oid) {
            type = element;
            array = true;
        }
    }
    if (type->typnamespace == 11) {
        if (const auto builtin = sqlBuiltinName(type->oid, typmod))
            return *builtin + (array ? "[]" : "");
    }

    const auto space = std::find_if(spaces.begin(), spaces.end(),
        [&](const auto& row) { return row.oid == type->typnamespace; });
    if (space == spaces.end()) throw DbError("XX001", "type has no catalog namespace");
    bool visible = false;
    for (const auto& schema : path) {
        const auto entry = std::find_if(spaces.begin(), spaces.end(),
            [&](const auto& row) { return row.nspname == schema; });
        if (entry == spaces.end()) continue;
        if (entry->oid == space->oid) {
            visible = true;
            break;
        }
        if (std::any_of(types.begin(), types.end(), [&](const auto& row) {
                return row.typnamespace == entry->oid && row.typname == type->typname;
            })) break;
    }
    std::string result;
    if (!visible) {
        const auto schema = session && space->nspname == sessionTempSchemaName(*session)
            ? std::string("pg_temp") : space->nspname;
        result = SQLParser::quoteIdentifier(schema) + ".";
    }
    result += catalogName(*type, typmod);
    if (array) result += "[]";
    return result;
}

} // namespace dbms::type_display
