// ============================================================================
// DDL AST Executor — Phase 4 Wave 0.3
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/DdlTransaction.h"
#include "parser/parser.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "catalog/type_registry.h"
#include "access/IndexFileUtil.h"
#include "common/logs.h"
#include "common/FeatureGate.h"
#include "common/GeometryValue.h"
#include "common/scram_sha256.h"
#include "permissions.h"
#include <algorithm>
#include <charconv>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;

// Helpers defined in main.cpp (now non-static)
bool checkAdmin(const Session& s);
bool checkDB(const Session& s);
std::string resolveTableName(Session& s, const std::string& name);

namespace dbms {

namespace {

std::string toLower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}

std::string trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

bool rejectMalformedDdlAst() {
    std::cout << "ERROR: malformed DDL statement (SQLSTATE XX000)"
              << std::endl;
    return true;
}

bool declaredVarcharTypeMod(const ColumnDef& definition, int32_t& modifier) {
    const std::string type = toLower(trim(definition.typeName));
    if (definition.isArray ||
        (type != "varchar" && type != "character varying")) return false;
    if (definition.typeMods.empty()) {
        modifier = -1;
        return true;
    }
    if (definition.typeMods.size() != 1) return false;
    const std::string& length = definition.typeMods.front();
    int32_t parsed = 0;
    const auto result = std::from_chars(
        length.data(), length.data() + length.size(), parsed);
    if (result.ec != std::errc() ||
        result.ptr != length.data() + length.size() ||
        parsed <= 0 || parsed > 65535) return false;
    modifier = parsed + 4;
    return true;
}

bool splitProcedureStatements(const std::string& body,
                              std::vector<std::string>& statements,
                              std::string& error) {
    statements.clear();
    error.clear();
    size_t statementStart = 0;
    bool statementHasContent = false;
    bool singleQuoted = false;
    bool escapeSingleQuoted = false;
    bool doubleQuoted = false;
    bool lineComment = false;
    size_t blockCommentDepth = 0;
    std::string dollarDelimiter;

    const auto appendStatement = [&](size_t end) {
        if (!statementHasContent) return;
        const std::string statement =
            trim(body.substr(statementStart, end - statementStart));
        if (!statement.empty()) statements.push_back(statement);
    };
    const auto markStatementContent = [&](size_t offset) {
        if (!statementHasContent) statementStart = offset;
        statementHasContent = true;
    };

    for (size_t i = 0; i < body.size(); ++i) {
        const char current = body[i];
        if (lineComment) {
            if (current == '\n' || current == '\r') lineComment = false;
            continue;
        }
        if (blockCommentDepth != 0) {
            if (current == '/' && i + 1 < body.size() &&
                body[i + 1] == '*') {
                ++blockCommentDepth;
                ++i;
            } else if (current == '*' && i + 1 < body.size() &&
                       body[i + 1] == '/') {
                --blockCommentDepth;
                ++i;
            }
            continue;
        }
        if (!dollarDelimiter.empty()) {
            if (body.compare(i, dollarDelimiter.size(), dollarDelimiter) == 0) {
                i += dollarDelimiter.size() - 1;
                dollarDelimiter.clear();
            }
            continue;
        }
        if (singleQuoted) {
            if (escapeSingleQuoted && current == '\\' &&
                i + 1 < body.size()) {
                ++i;
            } else if (current == '\'' && i + 1 < body.size() &&
                       body[i + 1] == '\'') {
                ++i;
            } else if (current == '\'') {
                singleQuoted = false;
                escapeSingleQuoted = false;
            }
            continue;
        }
        if (doubleQuoted) {
            if (current == '"' && i + 1 < body.size() &&
                body[i + 1] == '"') {
                ++i;
            } else if (current == '"') {
                doubleQuoted = false;
            }
            continue;
        }
        if (current == '-' && i + 1 < body.size() && body[i + 1] == '-') {
            lineComment = true;
            ++i;
            continue;
        }
        if (current == '/' && i + 1 < body.size() && body[i + 1] == '*') {
            blockCommentDepth = 1;
            ++i;
            continue;
        }
        if (current == '\'') {
            markStatementContent(i);
            singleQuoted = true;
            escapeSingleQuoted = i > 0 &&
                (body[i - 1] == 'e' || body[i - 1] == 'E') &&
                (i < 2 || (!std::isalnum(
                    static_cast<unsigned char>(body[i - 2])) &&
                    body[i - 2] != '_' && body[i - 2] != '$'));
            continue;
        }
        if (current == '"') {
            markStatementContent(i);
            doubleQuoted = true;
            continue;
        }
        if (current == '$') {
            size_t end = i + 1;
            if (end < body.size() &&
                (std::isalpha(static_cast<unsigned char>(body[end])) ||
                 body[end] == '_')) {
                while (end < body.size() &&
                       (std::isalnum(static_cast<unsigned char>(body[end])) ||
                        body[end] == '_')) {
                    ++end;
                }
            }
            if (end < body.size() && body[end] == '$') {
                markStatementContent(i);
                dollarDelimiter = body.substr(i, end - i + 1);
                i = end;
                continue;
            }
        }
        if (current == ';') {
            appendStatement(i);
            statementStart = i + 1;
            statementHasContent = false;
            continue;
        }
        if (!std::isspace(static_cast<unsigned char>(current))) {
            markStatementContent(i);
        }
    }

    if (singleQuoted) error = "unterminated string literal";
    else if (doubleQuoted) error = "unterminated quoted identifier";
    else if (!dollarDelimiter.empty()) error = "unterminated dollar quote";
    else if (blockCommentDepth != 0) error = "unterminated block comment";
    if (!error.empty()) {
        statements.clear();
        return false;
    }
    appendStatement(body.size());
    return true;
}

std::optional<StorageEngine::MaterializedViewResolution>
resolveMaterializedViewForSession(Session& session,
                                  const std::string& requestedName) {
    CatalogManager::QualifiedName qualified;
    if (!CatalogManager::parseQualifiedName(requestedName, qualified) ||
        qualified.name.empty()) {
        return std::nullopt;
    }
    if (!qualified.schema.empty()) {
        return g_engine.resolveMaterializedView(
            session.currentDB, qualified.schema, qualified.name);
    }

    std::vector<std::string> searchPath;
    std::string canonical;
    if (!dbms::parseSessionSearchPath(
            session.searchPath, searchPath, canonical)) {
        searchPath = {"public"};
    }
    for (const auto& rawSchema : searchPath) {
        const std::string schema = dbms::expandSessionSearchPathEntry(
            rawSchema, session.username);
        if (schema == "pg_catalog" || schema == "pg_temp" ||
            schema.rfind("pg_temp_", 0) == 0 ||
            !g_engine.schemaExists(session.currentDB, schema)) {
            continue;
        }
        if (auto materialized = g_engine.resolveMaterializedView(
                session.currentDB, schema, qualified.name)) {
            return materialized;
        }
    }
    return std::nullopt;
}

std::string stripQuotes(const std::string& s) {
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                          (s.front() == '"' && s.back() == '"'))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

bool parseInt64Strict(const std::string& token, int64_t& value) {
    if (token.empty()) return false;
    const char* begin = token.data();
    const char* end = begin + token.size();
    if (*begin == '+') {
        ++begin;
        if (begin == end) return false;
    }
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

bool parseInt64Tokens(const std::vector<std::string>& tokens,
                      size_t position, int64_t& value,
                      size_t& consumed) {
    consumed = 0;
    if (position >= tokens.size()) return false;
    std::string token = tokens[position];
    consumed = 1;
    if (token == "+" || token == "-") {
        if (position + 1 >= tokens.size()) return false;
        token += tokens[position + 1];
        consumed = 2;
    }
    if (!parseInt64Strict(token, value)) {
        consumed = 0;
        return false;
    }
    return true;
}

bool likeSourceHasExtendedStatistics(const std::string& dbname,
                                     const std::string& tableName,
                                     bool& hasStatistics,
                                     std::string& error) {
    hasStatistics = false;
    error.clear();
    const std::filesystem::path path =
        g_engine.dbPath(dbname) / ".extended_stats";
    std::error_code filesystemError;
    if (!std::filesystem::exists(path, filesystemError)) {
        if (filesystemError) error = filesystemError.message();
        return !filesystemError;
    }
    if (!std::filesystem::is_regular_file(path, filesystemError) ||
        filesystemError) {
        error = filesystemError
            ? filesystemError.message()
            : "extended statistics catalog is not a regular file";
        return false;
    }

    std::ifstream input(path);
    if (!input) {
        error = "cannot read extended statistics catalog";
        return false;
    }
    std::string line;
    while (std::getline(input, line)) {
        if (trim(line).empty()) continue;
        const size_t nameEnd = line.find('|');
        const size_t tableEnd = nameEnd == std::string::npos
            ? std::string::npos : line.find('|', nameEnd + 1);
        if (nameEnd == std::string::npos || tableEnd == std::string::npos) {
            error = "malformed extended statistics catalog";
            return false;
        }
        if (line.substr(nameEnd + 1, tableEnd - nameEnd - 1) == tableName) {
            hasStatistics = true;
            return true;
        }
    }
    if (input.bad()) {
        error = "cannot read extended statistics catalog";
        return false;
    }
    return true;
}

bool inspectLikeSourceIndexes(const std::string& dbname,
                              const std::string& tableName,
                              const TableSchema& table,
                              bool& hasUncopyableIndex,
                              std::string& error) {
    hasUncopyableIndex = false;
    error.clear();

    // Every SQL-created standalone index has a durable name mapping. Check
    // that first, including for temporary/legacy relations with no pg_class
    // entry. Constraint-backed indexes created with the table do not use this
    // file and are represented by TableSchema below.
    const std::filesystem::path namesPath =
        g_engine.dbPath(dbname) / (tableName + ".idxnames");
    std::error_code filesystemError;
    if (std::filesystem::exists(namesPath, filesystemError)) {
        if (!std::filesystem::is_regular_file(namesPath, filesystemError) ||
            filesystemError) {
            error = filesystemError
                ? filesystemError.message()
                : "index-name metadata is not a regular file";
            return false;
        }
        std::ifstream names(namesPath, std::ios::binary);
        if (!names) {
            error = "cannot read index-name metadata";
            return false;
        }
        const std::string contents{
            std::istreambuf_iterator<char>(names),
            std::istreambuf_iterator<char>()};
        if (names.bad()) {
            error = "cannot read index-name metadata";
            return false;
        }
        if (!trim(contents).empty()) {
            hasUncopyableIndex = true;
            return true;
        }
    } else if (filesystemError) {
        error = filesystemError.message();
        return false;
    }

    // A plain single-column index is implicit only when it is the default
    // index backing a column-level UNIQUE constraint. Every richer B-tree
    // shape and every specialized access method is a standalone index.
    for (const auto& metadata :
         g_engine.getIndexMetadata(dbname, tableName)) {
        const Column* indexedColumn = nullptr;
        if (!metadata.isExpression) {
            for (size_t column = 0; column < table.len; ++column) {
                if (table.cols[column].dataName == metadata.name) {
                    indexedColumn = &table.cols[column];
                    break;
                }
            }
        }
        if (!indexedColumn || !indexedColumn->isUnique ||
            metadata.descending || !metadata.includeCols.empty() ||
            !metadata.whereCondition.empty()) {
            hasUncopyableIndex = true;
            return true;
        }
    }
    if (!g_engine.getCompositeIndexes(dbname, tableName).empty() ||
        !g_engine.getHashIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getBloomIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getFullTextIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getGinIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getGiSTIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getBrinIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getSPGiSTIndexedColumns(dbname, tableName).empty() ||
        !g_engine.getExclusionConstraints(dbname, tableName).empty()) {
        hasUncopyableIndex = true;
    }
    return true;
}

std::string canonicalRoleName(const std::string& raw) {
    const std::string value = trim(raw);
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return stripQuotes(value);
    }
    return toLower(stripQuotes(value));
}

bool tableConstraintExists(const std::string& dbname,
                           const std::string& tableName,
                           const std::string& constraintName) {
    if (!g_engine.tableExists(dbname, tableName)) return false;
    const auto table = g_engine.getTableSchema(dbname, tableName);
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].checkConstraintName == constraintName) return true;
    }
    for (const auto& check : table.additionalCheckConstraints) {
        if (check.name == constraintName) return true;
    }
    for (const auto& name : table.uniqueConstraintNames) {
        if (name == constraintName) return true;
    }
    for (size_t i = 0; i < table.fkLen; ++i) {
        if (table.fks[i].name == constraintName) return true;
    }
    const auto exclusions = g_engine.getExclusionConstraints(dbname, tableName);
    return std::any_of(exclusions.begin(), exclusions.end(),
                       [&](const auto& constraint) { return constraint.name == constraintName; });
}

bool checkConstraintExists(const std::string& dbname,
                           const std::string& tableName,
                           const std::string& constraintName) {
    if (!g_engine.tableExists(dbname, tableName)) return false;
    const auto table = g_engine.getTableSchema(dbname, tableName);
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].checkConstraintName == constraintName) return true;
    }
    for (const auto& check : table.additionalCheckConstraints) {
        if (check.name == constraintName) return true;
    }
    return false;
}

std::string constraintMetadataKey(const std::string& name, const std::string& option) {
    return "constraint." + name + "." + option;
}

bool metadataBool(const std::map<std::string, std::string>& params,
                  const std::string& key, bool fallback) {
    auto it = params.find(key);
    if (it == params.end()) return fallback;
    return it->second == "1" || it->second == "true";
}

DBStatus persistConstraintMetadata(const std::string& dbname,
                                   const std::string& tableName,
                                   const std::string& constraintName,
                                   bool validated,
                                   bool notValid,
                                   bool deferrable,
                                   bool initiallyDeferred) {
    std::map<std::string, std::string> changes;
    changes[constraintMetadataKey(constraintName, "validated")] = validated ? "1" : "0";
    changes[constraintMetadataKey(constraintName, "not_valid")] = notValid ? "1" : "0";
    changes[constraintMetadataKey(constraintName, "deferrable")] = deferrable ? "1" : "0";
    changes[constraintMetadataKey(constraintName, "initially_deferred")] =
        initiallyDeferred ? "1" : "0";
    return g_engine.updateStorageParams(dbname, tableName, changes);
}

// ---- Shell type sidecar catalog (CREATE TYPE name) ----
// Stored in {db}/.shell_types as one type name per line.
static std::filesystem::path shellTypesPath(const std::string& dbname) {
    return g_engine.dbPath(dbname) / ".shell_types";
}

static bool validAuxiliaryTypeName(const std::string& name) {
    if (name.empty() || name.find('\0') != std::string::npos ||
        name.find('\n') != std::string::npos ||
        name.find('\r') != std::string::npos) {
        return false;
    }
    CatalogManager::QualifiedName qualified;
    return CatalogManager::parseQualifiedName(name, qualified) &&
           !qualified.name.empty() &&
           qualified.schema.find('.') == std::string::npos;
}

static DBStatus loadShellTypes(const std::string& dbname,
                               std::vector<std::string>& names) {
    names.clear();
    const auto path = shellTypesPath(dbname);
    std::error_code error;
    const bool exists = std::filesystem::exists(path, error);
    if (error) return DBStatus::IO_ERROR;
    if (!exists) return DBStatus::OK;

    std::ifstream in(path);
    if (!in) return DBStatus::IO_ERROR;
    std::set<std::string> normalizedNames;
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty()) continue;
        if (!validAuxiliaryTypeName(line) ||
            !normalizedNames.insert(toLower(line)).second) {
            names.clear();
            return DBStatus::CORRUPTED_DATA;
        }
        names.push_back(std::move(line));
    }
    return in.bad() ? DBStatus::IO_ERROR : DBStatus::OK;
}

static DBStatus saveShellTypes(const std::string& dbname,
                               const std::vector<std::string>& names) {
    std::ostringstream serialized;
    for (const auto& name : names) serialized << name << '\n';
    return index_file::writeAtomically(
               shellTypesPath(dbname), serialized.str())
        ? DBStatus::OK : DBStatus::IO_ERROR;
}

static DBStatus shellTypeExists(const std::string& dbname,
                                const std::string& name, bool& exists) {
    exists = false;
    std::vector<std::string> names;
    const DBStatus status = loadShellTypes(dbname, names);
    if (status != DBStatus::OK) return status;
    for (const auto& existing : names) {
        if (toLower(existing) == toLower(name)) {
            exists = true;
            break;
        }
    }
    return DBStatus::OK;
}

static DBStatus recordShellType(const std::string& dbname,
                                const std::string& name) {
    if (!validAuxiliaryTypeName(name)) return DBStatus::INVALID_ARGUMENT;
    std::vector<std::string> names;
    const DBStatus status = loadShellTypes(dbname, names);
    if (status != DBStatus::OK) return status;
    for (const auto& existing : names) {
        if (toLower(existing) == toLower(name)) {
            return DBStatus::TABLE_ALREADY_EXISTS;
        }
    }
    names.push_back(name);
    return saveShellTypes(dbname, names);
}

static DBStatus removeShellType(const std::string& dbname,
                                const std::string& name) {
    if (!validAuxiliaryTypeName(name)) return DBStatus::INVALID_ARGUMENT;
    std::vector<std::string> names;
    const DBStatus status = loadShellTypes(dbname, names);
    if (status != DBStatus::OK) return status;
    bool removed = false;
    std::vector<std::string> retained;
    retained.reserve(names.size());
    for (auto& existing : names) {
        if (toLower(existing) == toLower(name)) {
            removed = true;
        } else {
            retained.push_back(std::move(existing));
        }
    }
    if (!removed) return DBStatus::TABLE_NOT_FOUND;
    return saveShellTypes(dbname, retained);
}

// ---- Range / base type metadata sidecar (CREATE TYPE AS RANGE / CREATE TYPE name (...)) ----
// Legacy format: kind|name|key=value;key=value...
struct UdtMeta {
    std::string kind;  // "range" or "base"
    std::string name;
    std::map<std::string, std::string> attrs;
};

static std::filesystem::path udtMetaPath(const std::string& dbname) {
    return g_engine.dbPath(dbname) / ".udt_meta";
}

static constexpr const char* UDT_METADATA_V2_PREFIX = "DBMS_UDT_V2:";

static std::string encodeUdtField(const std::string& value) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for (unsigned char byte : value) {
        encoded.push_back(hex[byte >> 4]);
        encoded.push_back(hex[byte & 0x0f]);
    }
    return encoded;
}

static bool decodeUdtField(const std::string& encoded, std::string& value) {
    if (encoded.size() % 2 != 0) return false;
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    value.clear();
    value.reserve(encoded.size() / 2);
    for (size_t i = 0; i < encoded.size(); i += 2) {
        const int high = nibble(encoded[i]);
        const int low = nibble(encoded[i + 1]);
        if (high < 0 || low < 0) {
            value.clear();
            return false;
        }
        value.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

static bool validUdtMeta(const UdtMeta& meta) {
    if ((meta.kind != "range" && meta.kind != "base") ||
        !validAuxiliaryTypeName(meta.name)) {
        return false;
    }
    for (const auto& [key, value] : meta.attrs) {
        if (key.empty() || key.find('\0') != std::string::npos ||
            value.find('\0') != std::string::npos) {
            return false;
        }
    }
    if (meta.kind == "range") return meta.attrs.count("subtype") != 0;
    return meta.attrs.count("input") != 0 && meta.attrs.count("output") != 0;
}

static bool parseUdtRecord(const std::string& rawLine, UdtMeta& meta) {
    meta = {};
    if (rawLine.rfind(UDT_METADATA_V2_PREFIX, 0) == 0) {
        const size_t prefixLength =
            std::char_traits<char>::length(UDT_METADATA_V2_PREFIX);
        std::vector<std::string> fields;
        size_t position = prefixLength;
        while (true) {
            const size_t separator = rawLine.find('|', position);
            fields.push_back(separator == std::string::npos
                ? rawLine.substr(position)
                : rawLine.substr(position, separator - position));
            if (separator == std::string::npos) break;
            position = separator + 1;
        }
        if (fields.size() < 2 || (fields.size() - 2) % 2 != 0 ||
            !decodeUdtField(fields[0], meta.kind) ||
            !decodeUdtField(fields[1], meta.name)) {
            return false;
        }
        for (size_t i = 2; i < fields.size(); i += 2) {
            std::string key;
            std::string value;
            if (!decodeUdtField(fields[i], key) ||
                !decodeUdtField(fields[i + 1], value) ||
                !meta.attrs.emplace(std::move(key), std::move(value)).second) {
                return false;
            }
        }
    } else {
        const std::string line = trim(rawLine);
        const size_t kindEnd = line.find('|');
        const size_t nameEnd = kindEnd == std::string::npos
            ? std::string::npos : line.find('|', kindEnd + 1);
        if (kindEnd == std::string::npos || nameEnd == std::string::npos) {
            return false;
        }
        meta.kind = line.substr(0, kindEnd);
        meta.name = line.substr(kindEnd + 1, nameEnd - kindEnd - 1);
        std::stringstream attributes(line.substr(nameEnd + 1));
        std::string attribute;
        while (std::getline(attributes, attribute, ';')) {
            attribute = trim(attribute);
            if (attribute.empty()) continue;
            const size_t equals = attribute.find('=');
            if (equals == std::string::npos) return false;
            const std::string key = trim(attribute.substr(0, equals));
            const std::string value = trim(attribute.substr(equals + 1));
            if (!meta.attrs.emplace(key, value).second) return false;
        }
    }
    return validUdtMeta(meta);
}

static DBStatus loadUdtMeta(const std::string& dbname,
                            std::vector<UdtMeta>& metas) {
    metas.clear();
    const auto path = udtMetaPath(dbname);
    std::error_code error;
    const bool exists = std::filesystem::exists(path, error);
    if (error) return DBStatus::IO_ERROR;
    if (!exists) return DBStatus::OK;

    std::ifstream in(path);
    if (!in) return DBStatus::IO_ERROR;
    std::set<std::string> normalizedNames;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        UdtMeta meta;
        if (!parseUdtRecord(line, meta) ||
            !normalizedNames.insert(toLower(meta.name)).second) {
            metas.clear();
            return DBStatus::CORRUPTED_DATA;
        }
        metas.push_back(std::move(meta));
    }
    return in.bad() ? DBStatus::IO_ERROR : DBStatus::OK;
}

static DBStatus saveUdtMeta(const std::string& dbname,
                            const std::vector<UdtMeta>& metas) {
    std::ostringstream serialized;
    for (const auto& meta : metas) {
        serialized << UDT_METADATA_V2_PREFIX
                   << encodeUdtField(meta.kind) << '|'
                   << encodeUdtField(meta.name);
        for (const auto& [key, value] : meta.attrs) {
            serialized << '|' << encodeUdtField(key)
                       << '|' << encodeUdtField(value);
        }
        serialized << '\n';
    }
    return index_file::writeAtomically(
               udtMetaPath(dbname), serialized.str())
        ? DBStatus::OK : DBStatus::IO_ERROR;
}

static DBStatus udtMetaExists(const std::string& dbname,
                              const std::string& name, bool& exists,
                              const std::string& kind = "") {
    exists = false;
    std::vector<UdtMeta> metas;
    const DBStatus status = loadUdtMeta(dbname, metas);
    if (status != DBStatus::OK) return status;
    for (const auto& meta : metas) {
        if (toLower(meta.name) == toLower(name) &&
            (kind.empty() || toLower(meta.kind) == toLower(kind))) {
            exists = true;
            break;
        }
    }
    return DBStatus::OK;
}

static DBStatus recordUdtMeta(const std::string& dbname,
                              const UdtMeta& meta) {
    if (!validUdtMeta(meta)) return DBStatus::INVALID_ARGUMENT;
    std::vector<UdtMeta> metas;
    const DBStatus status = loadUdtMeta(dbname, metas);
    if (status != DBStatus::OK) return status;
    for (const auto& existing : metas) {
        if (toLower(existing.name) == toLower(meta.name)) {
            return DBStatus::TABLE_ALREADY_EXISTS;
        }
    }
    metas.push_back(meta);
    return saveUdtMeta(dbname, metas);
}

static DBStatus removeUdtMeta(const std::string& dbname,
                              const std::string& name) {
    if (!validAuxiliaryTypeName(name)) return DBStatus::INVALID_ARGUMENT;
    std::vector<UdtMeta> metas;
    const DBStatus status = loadUdtMeta(dbname, metas);
    if (status != DBStatus::OK) return status;
    bool removed = false;
    std::vector<UdtMeta> kept;
    kept.reserve(metas.size());
    for (auto& meta : metas) {
        if (toLower(meta.name) == toLower(name)) {
            removed = true;
        } else {
            kept.push_back(std::move(meta));
        }
    }
    if (!removed) return DBStatus::TABLE_NOT_FOUND;
    return saveUdtMeta(dbname, kept);
}

static DBStatus anyTypeExists(const std::string& dbname,
                              const std::string& name, bool& exists) {
    exists = g_engine.isCompositeType(dbname, name) ||
             !g_engine.getEnumType(dbname, name).name.empty();
    if (exists) return DBStatus::OK;
    DBStatus status = shellTypeExists(dbname, name, exists);
    if (status != DBStatus::OK || exists) return status;
    return udtMetaExists(dbname, name, exists);
}

} // anonymous namespace

// ----------------------------------------------------------------------------
// Catalog registration helpers
// ----------------------------------------------------------------------------

static Oid ensureTypeInCatalog(CatalogManager& cat, Oid nspOid, const Column& col) {
    Oid typid = mapBuiltinTypeNameToOid(col.dataType);
    if (typid != INVALID_OID) return typid;

    CatalogManager::QualifiedName typeName;
    if (!CatalogManager::parseQualifiedName(col.dataType, typeName))
        throw std::runtime_error("invalid column type name");
    Oid typeNamespace = nspOid;
    if (!typeName.schema.empty()) {
        const PgNamespaceRow* namedNamespace =
            cat.findNamespaceByName(typeName.schema);
        if (!namedNamespace)
            throw std::runtime_error("column type schema has no catalog entry");
        typeNamespace = namedNamespace->oid;
    }
    if (const auto* existing =
            cat.findTypeByName(typeName.name, typeNamespace)) {
        const Oid existingOid = existing->oid;
        if (!col.enumValues.empty()) {
            if (existing->typtype != 'e') {
                PgTypeRow corrected = *existing;
                corrected.typlen = 4;
                corrected.typbyval = true;
                corrected.typtype = 'e';
                corrected.typcategory = 'E';
                if (!cat.updateType(existingOid, corrected))
                    throw std::runtime_error("cannot repair enum catalog type");
            }
            if (!cat.replaceEnumLabels(existingOid, col.enumValues))
                throw std::runtime_error("cannot synchronize enum labels");
        }
        return existingOid;
    }

    PgTypeRow typ;
    typ.typname = typeName.name;
    typ.typnamespace = typeNamespace;
    typ.typlen = !col.enumValues.empty()
        ? static_cast<int16_t>(4)
        : (col.isVariableLength ? static_cast<int16_t>(-1)
                                : static_cast<int16_t>(col.dsize));
    typ.typbyval = !col.enumValues.empty();
    typ.typtype = col.enumValues.empty() ? 'b' : 'e';
    typ.typcategory = col.enumValues.empty() ? 'U' : 'E';
    const Oid createdOid = cat.createType(typ);
    if (!col.enumValues.empty() &&
        !cat.replaceEnumLabels(createdOid, col.enumValues)) {
        (void)cat.dropType(createdOid);
        throw std::runtime_error("cannot register enum labels");
    }
    return createdOid;
}

static PgAttributeRow catalogAttributeForColumn(
    CatalogManager& cat, Oid relationOid, Oid namespaceOid,
    const Column& column, size_t columnIndex,
    const PgAttributeRow* previous = nullptr) {
    PgAttributeRow attribute = previous ? *previous : PgAttributeRow{};
    attribute.attrelid = relationOid;
    attribute.attnum = static_cast<int16_t>(columnIndex + 1);
    attribute.attname = column.dataName;
    attribute.atttypid = ensureTypeInCatalog(cat, namespaceOid, column);
    const bool postgresVarlenaNetwork =
        column.dataType == "inet" || column.dataType == "cidr";
    const int16_t postgresGeometryLength =
        geometryTypeLengthForOid(attribute.atttypid);
    if (attribute.atttypid == 1042) {
        // SQL CHAR is bpchar, a varlena protocol type even when our storage
        // column has a fixed declared width.
        attribute.attlen = -1;
    } else {
        attribute.attlen = postgresGeometryLength != 0
            ? postgresGeometryLength
            : (column.isVariableLength || postgresVarlenaNetwork
                   ? static_cast<int16_t>(-1)
                   : static_cast<int16_t>(column.dsize));
    }
    attribute.attndims = column.isArray ? 1 : 0;
    attribute.atttypmod = -1;
    if (!column.isArray && column.dataType == "bit") {
        attribute.atttypmod = static_cast<int32_t>(column.dsize + 4);
    } else if (!column.isArray && column.dataType == "bit varying" &&
               column.dsize != 8388608) {
        attribute.atttypmod = static_cast<int32_t>(column.dsize + 4);
    } else if (!column.isArray &&
               (column.dataType == "character" || column.dataType == "char" ||
                column.dataType == "bpchar") && column.dsize > 0) {
        attribute.atttypmod = static_cast<int32_t>(column.dsize + 4);
    } else if (!column.isArray && column.dataType == "varchar" &&
               column.dsize > 0 && column.dsize != 65535) {
        attribute.atttypmod = static_cast<int32_t>(column.dsize + 4);
    }
    if (previous && attribute.atttypid == previous->atttypid &&
        attribute.atttypid == 1043 && previous->atttypmod >= 4 &&
        static_cast<size_t>(previous->atttypmod - 4) == column.dsize) {
        // A catalog-backed VARCHAR bound can equal the physical capacity.
        // Preserve it across unrelated ALTER TABLE catalog synchronizations.
        attribute.atttypmod = previous->atttypmod;
    }
    attribute.attnotnull = !column.isNull;
    attribute.atthasdef = !column.defaultValue.empty();
    attribute.attstorage = column.isVariableLength ? 'x' : 'p';
    attribute.attidentity = column.identityKind;
    attribute.attgenerated = column.generatedExpr.empty()
        ? '\0' : (column.generatedKind == 'v' ? 'v' : 's');
    // Preserve inheritance provenance while synchronizing storage-backed
    // column metadata after ALTER TABLE.  CREATE TABLE supplies the initial
    // value below; an unrelated ALTER must not turn an inherited column into
    // a local one.
    if (!previous) attribute.attislocal = true;
    attribute.attisdropped = false;
    return attribute;
}

struct CatalogInheritanceColumn {
    bool isLocal = true;
    int32_t directParentCount = 0;
};

using CatalogInheritanceColumns =
    std::map<std::string, CatalogInheritanceColumn>;

static int16_t tableCheckConstraintCount(const TableSchema& table) {
    size_t count = 0;
    for (size_t column = 0; column < table.len; ++column) {
        if (!table.cols[column].checkExpr.empty()) ++count;
    }
    for (const auto& check : table.additionalCheckConstraints) {
        if (!check.expression.empty()) ++count;
    }
    return static_cast<int16_t>(count);
}

static bool normalizeNamedCheckConstraints(TableSchema& table,
                                           std::string& error) {
    struct Definition {
        std::string expression;
        bool deferrable = false;
        bool initiallyDeferred = false;
    };
    std::map<std::string, Definition> named;
    const auto record = [&](const std::string& name,
                            const std::string& expression,
                            bool deferrable,
                            bool initiallyDeferred) {
        if (name.empty()) return 1;
        const auto [position, inserted] = named.emplace(
            name, Definition{expression, deferrable, initiallyDeferred});
        if (inserted) return 1;
        if (position->second.expression != expression ||
            position->second.deferrable != deferrable ||
            position->second.initiallyDeferred != initiallyDeferred) {
            error = "CHECK constraint \"" + name +
                "\" has conflicting definitions";
            return -1;
        }
        return 0;
    };

    for (size_t columnIndex = 0; columnIndex < table.len; ++columnIndex) {
        Column& column = table.cols[columnIndex];
        if (column.checkExpr.empty()) continue;
        const int result = record(
            column.checkConstraintName, column.checkExpr,
            column.deferrable, column.initiallyDeferred);
        if (result < 0) return false;
        if (result == 0) {
            column.checkExpr.clear();
            column.checkConstraintName.clear();
            column.deferrable = false;
            column.initiallyDeferred = false;
        }
    }

    std::vector<CheckConstraint> retained;
    retained.reserve(table.additionalCheckConstraints.size());
    for (const auto& check : table.additionalCheckConstraints) {
        if (check.expression.empty()) continue;
        const int result = record(
            check.name, check.expression,
            check.deferrable, check.initiallyDeferred);
        if (result < 0) return false;
        if (result > 0) retained.push_back(check);
    }
    table.additionalCheckConstraints = std::move(retained);
    return true;
}

static bool tableSchemaHasImplicitIndex(const TableSchema& table) {
    if (table.hasPrimaryKey()) return true;
    for (size_t column = 0; column < table.len; ++column) {
        if (table.cols[column].isUnique) return true;
    }
    return false;
}

static bool tableSchemaHasPartitionChildren(const TableSchema& table) {
    switch (table.partitionType) {
        case TableSchema::PartitionType::Range:
            return !table.rangePartitions.empty() ||
                   !table.defaultPartitionName.empty();
        case TableSchema::PartitionType::List:
            return !table.listPartitions.empty() ||
                   !table.defaultPartitionName.empty();
        case TableSchema::PartitionType::Hash:
            return table.hashPartitions != 0;
        case TableSchema::PartitionType::None:
            return false;
    }
    return false;
}

static bool storageTableHasIndex(const std::string& dbname,
                                 const std::string& physicalTableName) {
    const TableSchema table =
        g_engine.getTableSchema(dbname, physicalTableName);
    return tableSchemaHasImplicitIndex(table) ||
           !g_engine.getIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getCompositeIndexes(dbname, physicalTableName).empty() ||
           !g_engine.getHashIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getBloomIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getFullTextIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getGinIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getGiSTIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getBrinIndexedColumns(dbname, physicalTableName).empty() ||
           !g_engine.getSPGiSTIndexedColumns(dbname, physicalTableName).empty();
}

static void registerTableInCatalog(CatalogManager& cat, const TableSchema& tbl,
                                   const std::string& logicalSchema,
                                   const std::string& logicalName,
                                   const CatalogInheritanceColumns*
                                       inheritedColumns = nullptr,
                                   const std::map<std::string, int32_t>*
                                       declaredVarcharMods = nullptr) {
    const auto* ns = cat.findNamespaceByName(logicalSchema);
    if (!ns) {
        throw std::runtime_error("table schema has no catalog entry");
    }
    const Oid nspOid = ns->oid;
    if (cat.findClassByName(logicalName, nspOid)) {
        throw std::runtime_error("table name already exists in catalog");
    }

    PgClassRow cls;
    cls.relname = logicalName;
    cls.relnamespace = nspOid;
    cls.relkind = 'r';
    cls.relnatts = static_cast<int16_t>(tbl.len);
    cls.relchecks = tableCheckConstraintCount(tbl);
    cls.relhasindex = tableSchemaHasImplicitIndex(tbl);
    cls.relhassubclass = tableSchemaHasPartitionChildren(tbl);
    cls.relpersistence = tbl.isUnlogged ? 'u' : 'p';
    if (!tbl.owner.empty()) {
        const auto owner = authCatalog().getAuthIdByName(tbl.owner);
        if (owner) cls.relowner = owner->oid;
    }
    Oid classOid = cat.createClass(cls);

    for (size_t i = 0; i < tbl.len; ++i) {
        PgAttributeRow attribute = catalogAttributeForColumn(
            cat, classOid, nspOid, tbl.cols[i], i);
        if (declaredVarcharMods) {
            const auto declared = declaredVarcharMods->find(tbl.cols[i].dataName);
            if (declared != declaredVarcharMods->end()) {
                attribute.atttypmod = declared->second;
            }
        }
        if (inheritedColumns) {
            const auto provenance =
                inheritedColumns->find(tbl.cols[i].dataName);
            if (provenance != inheritedColumns->end()) {
                attribute.attislocal = provenance->second.isLocal;
                attribute.attinhcount =
                    provenance->second.directParentCount;
            }
        }
        cat.addAttribute(attribute);
    }

    for (size_t i = 0; i < tbl.fkLen; ++i) {
        PgDependRow dep;
        dep.classid = PgClassOid_Class;
        dep.objid = classOid;
        dep.objsubid = 0;
        dep.refclassid = PgClassOid_Class;
        dep.refobjid = INVALID_OID; // FK target OID resolution deferred
        dep.refobjsubid = 0;
        dep.deptype = 'n';
        cat.addDepend(dep);
    }
}

static Oid registerMaterializedViewInCatalog(
    CatalogManager& catalog, const TableSchema& output,
    const std::string& logicalSchema, const std::string& logicalName,
    bool populated) {
    const auto* viewNamespace =
        catalog.findNamespaceByName(logicalSchema);
    if (!viewNamespace) {
        throw std::runtime_error("materialized-view schema has no catalog entry");
    }
    const Oid namespaceOid = viewNamespace->oid;
    if (catalog.findClassByName(logicalName, namespaceOid)) {
        throw std::runtime_error("materialized-view name already exists");
    }

    PgClassRow relation;
    relation.relname = logicalName;
    relation.relnamespace = namespaceOid;
    relation.relkind = 'm';
    relation.relnatts = static_cast<int16_t>(output.len);
    relation.relispopulated = populated;
    if (!output.owner.empty()) {
        const auto owner = authCatalog().getAuthIdByName(output.owner);
        if (owner) relation.relowner = owner->oid;
    }
    const Oid relationOid = catalog.createClass(relation);
    for (size_t column = 0; column < output.len; ++column) {
        catalog.addAttribute(catalogAttributeForColumn(
            catalog, relationOid, namespaceOid, output.cols[column],
            column));
    }
    return relationOid;
}

static Oid registerViewInCatalog(
    CatalogManager& catalog, const TableSchema* output,
    const std::string& logicalSchema, const std::string& logicalName,
    const std::string& owner, Oid replaceOid = INVALID_OID) {
    const auto* viewNamespace = catalog.findNamespaceByName(logicalSchema);
    if (!viewNamespace) {
        throw std::runtime_error("view schema has no catalog entry");
    }
    const Oid namespaceOid = viewNamespace->oid;

    Oid relationOid = replaceOid;
    if (relationOid == INVALID_OID) {
        if (catalog.findClassByName(logicalName, namespaceOid)) {
            throw std::runtime_error("view name already exists");
        }
        PgClassRow relation;
        relation.relname = logicalName;
        relation.relnamespace = namespaceOid;
        relation.relkind = 'v';
        relation.relnatts = output
            ? static_cast<int16_t>(output->len) : 0;
        const auto ownerRole = authCatalog().getAuthIdByName(owner);
        if (ownerRole) relation.relowner = ownerRole->oid;
        relationOid = catalog.createClass(relation);
    } else {
        const PgClassRow* current = catalog.findClass(relationOid);
        if (!current || current->relkind != 'v' ||
            current->relnamespace != namespaceOid ||
            current->relname != logicalName) {
            throw std::runtime_error("cannot replace non-view catalog object");
        }
        if (output) {
            PgClassRow replacement = *current;
            replacement.relnatts = static_cast<int16_t>(output->len);
            if (!catalog.updateClass(relationOid, replacement)) {
                throw std::runtime_error("cannot update view catalog row");
            }
        }
    }

    if (output) {
        std::vector<PgAttributeRow> attributes;
        attributes.reserve(output->len);
        for (size_t column = 0; column < output->len; ++column) {
            attributes.push_back(catalogAttributeForColumn(
                catalog, relationOid, namespaceOid, output->cols[column],
                column));
        }
        if (!catalog.replaceAttributes(relationOid, attributes)) {
            throw std::runtime_error("cannot replace view attributes");
        }
    }
    return relationOid;
}

static bool synchronizeTableAttributesInCatalog(
    const std::string& dbname, const std::string& physicalTableName,
    const std::map<std::string, int32_t>* declaredVarcharMods = nullptr) {
    try {
        CatalogManager& catalog = g_engine.catalogService().get(dbname);
        const auto qualifiedName =
            CatalogService::logicalName(physicalTableName);
        const std::string schemaName = qualifiedName.schema.empty()
            ? "public" : qualifiedName.schema;
        const auto* relation = catalog.resolveRelation(
            qualifiedName.name, {schemaName});
        // Storage-only relations intentionally remain outside pg_catalog.
        if (!relation) return true;
        const Oid relationOid = relation->oid;
        const Oid namespaceOid = relation->relnamespace;

        const TableSchema table =
            g_engine.getTableSchema(dbname, physicalTableName);
        if (table.len == 0) return false;
        const std::vector<PgAttributeRow> previous =
            catalog.findAttributes(relationOid);
        std::vector<PgAttributeRow> replacement;
        replacement.reserve(table.len);
        for (size_t columnIndex = 0; columnIndex < table.len;
             ++columnIndex) {
            const Column& column = table.cols[columnIndex];
            const PgAttributeRow* retained = nullptr;
            for (const auto& attribute : previous) {
                if (attribute.attname == column.dataName) {
                    retained = &attribute;
                    break;
                }
            }
            if (!retained) {
                for (const auto& attribute : previous) {
                    if (attribute.attnum ==
                        static_cast<int16_t>(columnIndex + 1)) {
                        retained = &attribute;
                        break;
                    }
                }
            }
            PgAttributeRow attribute = catalogAttributeForColumn(
                catalog, relationOid, namespaceOid, column,
                columnIndex, retained);
            if (declaredVarcharMods) {
                const auto declared = declaredVarcharMods->find(column.dataName);
                if (declared != declaredVarcharMods->end()) {
                    attribute.atttypmod = declared->second;
                }
            }
            replacement.push_back(std::move(attribute));
        }
        if (!catalog.replaceAttributes(relationOid, replacement)) {
            return false;
        }
        const auto* updatedRelation = catalog.findClass(relationOid);
        if (!updatedRelation) return false;
        PgClassRow classReplacement = *updatedRelation;
        classReplacement.relchecks = tableCheckConstraintCount(table);
        return catalog.updateClass(relationOid, classReplacement) &&
               catalog.persistAll();
    } catch (const std::exception& error) {
        std::cerr << "ALTER TABLE column catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

template <typename Update>
static bool updateTableClassInCatalog(
    const std::string& dbname, const std::string& physicalTableName,
    Update update) {
    try {
        CatalogManager& catalog = g_engine.catalogService().get(dbname);
        const auto qualifiedName =
            CatalogService::logicalName(physicalTableName);
        const std::string schemaName = qualifiedName.schema.empty()
            ? "public" : qualifiedName.schema;
        const auto* relation = catalog.resolveRelation(
            qualifiedName.name, {schemaName});
        // Storage-only relations intentionally remain outside pg_catalog.
        if (!relation) return true;
        const Oid relationOid = relation->oid;
        PgClassRow replacement = *relation;
        update(replacement);
        return catalog.updateClass(relationOid, replacement) &&
               catalog.persistAll();
    } catch (const std::exception& error) {
        std::cerr << "ALTER TABLE relation catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

static bool synchronizeTableRlsInCatalog(
    const std::string& dbname, const std::string& physicalTableName) {
    try {
        const TableSchema table =
            g_engine.getTableSchema(dbname, physicalTableName);
        return updateTableClassInCatalog(
            dbname, physicalTableName,
            [&](PgClassRow& relation) {
                relation.relrowsecurity = table.rowLevelSecurity;
                relation.relforcerowsecurity = table.forceRowLevelSecurity;
            });
    } catch (const std::exception& error) {
        std::cerr << "ALTER TABLE row-security catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

static bool synchronizeRelationTriggerFlagInCatalog(
    const std::string& dbname, const std::string& physicalTableName) {
    std::vector<StorageEngine::Trigger> triggers;
    if (!g_engine.tryGetAllTriggers(dbname, triggers)) return false;
    const bool hasTriggers = std::any_of(
        triggers.begin(), triggers.end(),
        [&](const StorageEngine::Trigger& trigger) {
            return trigger.tableName == physicalTableName;
        });
    return updateTableClassInCatalog(
        dbname, physicalTableName,
        [&](PgClassRow& relation) {
            relation.relhastriggers = hasTriggers;
        });
}

static bool synchronizeTableCheckCountInCatalog(
    const std::string& dbname, const std::string& physicalTableName) {
    try {
        const TableSchema table =
            g_engine.getTableSchema(dbname, physicalTableName);
        const int16_t count = tableCheckConstraintCount(table);
        return updateTableClassInCatalog(
            dbname, physicalTableName,
            [&](PgClassRow& relation) {
                relation.relchecks = count;
            });
    } catch (const std::exception& error) {
        std::cerr << "ALTER TABLE CHECK catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

static bool synchronizeTableIndexFlagInCatalog(
    const std::string& dbname, const std::string& physicalTableName) {
    try {
        const bool hasIndex =
            storageTableHasIndex(dbname, physicalTableName);
        return updateTableClassInCatalog(
            dbname, physicalTableName,
            [&](PgClassRow& relation) {
                relation.relhasindex = hasIndex;
            });
    } catch (const std::exception& error) {
        std::cerr << "ALTER TABLE index catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

static bool updateTableHierarchyFlagsInCatalog(
    CatalogManager& catalog, const std::string& dbname,
    const std::string& physicalTableName,
    std::optional<bool> isPartition = std::nullopt) {
    const auto qualifiedName =
        CatalogService::logicalName(physicalTableName);
    const std::string schemaName = qualifiedName.schema.empty()
        ? "public" : qualifiedName.schema;
    const auto* relation = catalog.resolveRelation(
        qualifiedName.name, {schemaName});
    // Storage-only relations intentionally remain outside pg_catalog.
    if (!relation) return true;
    if (!g_engine.tableExists(dbname, physicalTableName)) return false;

    PgClassRow replacement = *relation;
    const TableSchema table =
        g_engine.getTableSchema(dbname, physicalTableName);
    replacement.relhassubclass =
        tableSchemaHasPartitionChildren(table) ||
        !g_engine.getInheritedChildren(
            dbname, physicalTableName).empty();
    if (isPartition.has_value()) {
        replacement.relispartition = *isPartition;
    }
    return catalog.updateClass(replacement.oid, replacement);
}

static bool synchronizeTableHierarchyFlagsInCatalog(
    const std::string& dbname, const std::string& physicalTableName,
    std::optional<bool> isPartition = std::nullopt) {
    try {
        CatalogManager& catalog = g_engine.catalogService().get(dbname);
        return updateTableHierarchyFlagsInCatalog(
                   catalog, dbname, physicalTableName, isPartition) &&
               catalog.persistAll();
    } catch (const std::exception& error) {
        std::cerr << "table hierarchy catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

static bool updateInheritanceColumnsInCatalog(
    CatalogManager& catalog, const std::string& dbname,
    const std::string& physicalChildName,
    const std::string& physicalParentName, bool adding) {
    const auto qualifiedChild =
        CatalogService::logicalName(physicalChildName);
    const std::string childSchema = qualifiedChild.schema.empty()
        ? "public" : qualifiedChild.schema;
    const auto* childRelation = catalog.resolveRelation(
        qualifiedChild.name, {childSchema});
    // Temporary and storage-only relations intentionally have no catalog row.
    if (!childRelation) return true;

    auto attributes = catalog.findAttributes(childRelation->oid);
    std::map<std::string, int32_t> directParentCounts;
    for (const auto& candidate : g_engine.getTableNames(dbname)) {
        if (candidate == physicalChildName) continue;
        const auto children =
            g_engine.getInheritedChildren(dbname, candidate);
        if (std::find(children.begin(), children.end(), physicalChildName) ==
            children.end()) {
            continue;
        }
        const TableSchema parent =
            g_engine.getTableSchema(dbname, candidate);
        for (size_t column = 0; column < parent.len; ++column) {
            ++directParentCounts[parent.cols[column].dataName];
        }
    }

    for (auto& attribute : attributes) {
        attribute.attinhcount = directParentCounts[attribute.attname];
    }
    if (!adding) {
        const TableSchema removedParent =
            g_engine.getTableSchema(dbname, physicalParentName);
        for (size_t parentColumn = 0; parentColumn < removedParent.len;
             ++parentColumn) {
            const auto attribute = std::find_if(
                attributes.begin(), attributes.end(),
                [&](const PgAttributeRow& row) {
                    return row.attname ==
                        removedParent.cols[parentColumn].dataName;
                });
            if (attribute == attributes.end()) return false;
            // PostgreSQL deliberately retains columns after NO INHERIT by
            // turning them into local definitions.
            attribute->attislocal = true;
        }
    }
    return catalog.replaceAttributes(childRelation->oid, attributes);
}

static bool columnHasInheritanceParents(
    const std::string& dbname, const std::string& physicalTableName,
    const std::string& columnName, bool& inherited) {
    inherited = false;
    try {
        CatalogManager& catalog = g_engine.catalogService().get(dbname);
        const auto qualifiedName =
            CatalogService::logicalName(physicalTableName);
        const std::string schemaName = qualifiedName.schema.empty()
            ? "public" : qualifiedName.schema;
        const auto* relation = catalog.resolveRelation(
            qualifiedName.name, {schemaName});
        if (!relation) return true;
        const auto* attribute =
            catalog.findAttribute(relation->oid, columnName);
        inherited = attribute && attribute->attinhcount > 0;
        // The graph is authoritative for databases created by older builds,
        // whose pg_attribute rows always contained attinhcount=0.
        if (!inherited) {
            for (const auto& candidate : g_engine.getTableNames(dbname)) {
                if (candidate == physicalTableName) continue;
                const auto children =
                    g_engine.getInheritedChildren(dbname, candidate);
                if (std::find(children.begin(), children.end(),
                              physicalTableName) == children.end()) {
                    continue;
                }
                const TableSchema parent =
                    g_engine.getTableSchema(dbname, candidate);
                for (size_t column = 0; column < parent.len; ++column) {
                    if (parent.cols[column].dataName == columnName) {
                        inherited = true;
                        break;
                    }
                }
                if (inherited) break;
            }
        }
        return true;
    } catch (const std::exception& error) {
        std::cerr << "inheritance catalog lookup failed: "
                  << error.what() << std::endl;
        return false;
    }
}

static bool updateColumnStatisticsInCatalog(
    const std::string& dbname, const std::string& physicalTableName,
    const std::string& columnName, int statisticsTarget) {
    try {
        CatalogManager& catalog = g_engine.catalogService().get(dbname);
        const auto qualifiedName =
            CatalogService::logicalName(physicalTableName);
        const std::string schemaName = qualifiedName.schema.empty()
            ? "public" : qualifiedName.schema;
        const auto* relation = catalog.resolveRelation(
            qualifiedName.name, {schemaName});
        // Storage-only relations intentionally remain outside pg_catalog.
        if (!relation) return true;
        const Oid relationOid = relation->oid;
        auto attributes = catalog.findAttributes(relationOid);
        const auto attribute = std::find_if(
            attributes.begin(), attributes.end(),
            [&](const PgAttributeRow& row) {
                return row.attname == columnName;
            });
        if (attribute == attributes.end()) return false;
        attribute->attstattarget = statisticsTarget;
        return catalog.replaceAttributes(relationOid, attributes) &&
               catalog.persistAll();
    } catch (const std::exception& error) {
        std::cerr << "ALTER COLUMN SET STATISTICS catalog update failed: "
                  << error.what() << std::endl;
        return false;
    }
}

// ----------------------------------------------------------------------------
// Public entry points
// ----------------------------------------------------------------------------

bool DdlExecutor::execute(const StmtPtr& stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    Session* previousSession = currentSession();
    setCurrentSession(&s);
    struct SessionGuard {
        Session* previous;
        ~SessionGuard() { setCurrentSession(previous); }
    } sessionGuard{previousSession};

    // PostgreSQL rejects catalog and relation utility writes in a read-only
    // transaction, including operations on temporary relations.  Enforce the
    // boundary before an executor can implicitly commit, create a file, or
    // register a catalog row.
    if (g_engine.inTransaction() && g_engine.isReadOnly()) {
        std::cout << "ERROR: cannot execute " << stmt->toString()
                  << " in a read-only transaction (SQLSTATE 25006)"
                  << std::endl;
        return true;
    }

    switch (stmt->command) {
        case SqlCommand::CreateTable:
            return executeCreateTable(dynamic_cast<const CreateTableStmt*>(stmt.get()), s);
        case SqlCommand::DropTable:
            return executeDropTable(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::AlterTable:
            return executeAlterTable(dynamic_cast<const AlterTableStmt*>(stmt.get()), s);
        case SqlCommand::CreateIndex:
            return executeCreateIndex(dynamic_cast<const CreateIndexStmt*>(stmt.get()), s);
        case SqlCommand::DropIndex:
            return executeDropIndex(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateSequence:
            return executeCreateSequence(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::AlterSequence:
            return executeAlterSequence(dynamic_cast<const AlterObjectStmt*>(stmt.get()), s);
        case SqlCommand::DropSequence:
            return executeDropSequence(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateDomain:
            return executeCreateDomain(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::DropDomain:
            return executeDropDomain(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateType:
            return executeCreateType(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::DropType:
            return executeDropType(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateView:
            return executeCreateView(dynamic_cast<const CreateViewStmt*>(stmt.get()), s);
        case SqlCommand::DropView:
            return executeDropView(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateTrigger:
            return executeCreateTrigger(dynamic_cast<const CreateTriggerStmt*>(stmt.get()), s);
        case SqlCommand::DropTrigger:
            return executeDropTrigger(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateFunction:
            return executeCreateFunction(dynamic_cast<const CreateFunctionStmt*>(stmt.get()), s);
        case SqlCommand::DropFunction:
            return executeDropFunction(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateProcedure:
            return executeCreateProcedure(dynamic_cast<const CreateFunctionStmt*>(stmt.get()), s);
        case SqlCommand::CreatePolicy:
            return executeCreatePolicy(dynamic_cast<const CreatePolicyStmt*>(stmt.get()), s);
        case SqlCommand::CreateMaterializedView:
            return executeCreateMaterializedView(dynamic_cast<const CreateViewStmt*>(stmt.get()), s);
        case SqlCommand::RefreshMaterializedView:
            return executeRefreshMaterializedView(
                dynamic_cast<const RefreshMaterializedViewStmt*>(stmt.get()), s);
        case SqlCommand::DropMaterializedView:
            return executeDropMaterializedView(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateCollation:
            return executeCreateCollation(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::DropCollation:
            return executeDropCollation(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateDatabase:
            return executeCreateDatabase(dynamic_cast<const CreateDatabaseStmt*>(stmt.get()), s);
        case SqlCommand::DropDatabase:
            return executeDropDatabase(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateSchema:
            return executeCreateSchema(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::CreateRole:
            return executeCreateRole(dynamic_cast<const CreateRoleStmt*>(stmt.get()), s);
        case SqlCommand::AlterRole:
        case SqlCommand::AlterUser:
            return executeAlterRole(dynamic_cast<const AlterObjectStmt*>(stmt.get()), s);
        case SqlCommand::AlterDefaultPrivileges:
            return executeAlterDefaultPrivileges(
                dynamic_cast<const AlterDefaultPrivilegesStmt*>(stmt.get()), s);
        case SqlCommand::Truncate:
            return executeTruncate(dynamic_cast<const TruncateStmt*>(stmt.get()), s);
        case SqlCommand::DropRole:
        case SqlCommand::DropUser:
            return executeDropRole(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::DropSchema:
            return executeDropSchema(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::Comment:
            return executeComment(dynamic_cast<const CommentStmt*>(stmt.get()), s);
        default:
            return false; // not handled by bridge; fall back to legacy dispatch
    }
}

bool DdlExecutor::executeSql(const std::string& sql, Session& s) {
    SQLParser parser;
    ParseResult r = parser.parse(sql);
    if (!r.success || !r.stmt) {
        std::cout << "SQL syntax error";
        if (!r.error.empty()) std::cout << ": " << r.error;
        std::cout << " (SQLSTATE 42601)" << std::endl;
        return true;
    }
    return execute(r.stmt, s);
}

// ----------------------------------------------------------------------------
// DDL AST bridge helper (used by main.cpp::execute)
// ----------------------------------------------------------------------------

bool tryDdlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled, const std::string& rawSql) {
    handled = false;
    switch (parsedCmd) {
        case dbms::SqlCommand::CreateTable:
        case dbms::SqlCommand::DropTable:
        case dbms::SqlCommand::AlterTable:
        case dbms::SqlCommand::CreateIndex:
        case dbms::SqlCommand::CreateFullTextIndex:
        case dbms::SqlCommand::CreateHashIndex:
        case dbms::SqlCommand::DropIndex:
        case dbms::SqlCommand::DropFullTextIndex:
        case dbms::SqlCommand::CreateSequence:
        case dbms::SqlCommand::AlterSequence:
        case dbms::SqlCommand::DropSequence:
        case dbms::SqlCommand::CreateDomain:
        case dbms::SqlCommand::DropDomain:
        case dbms::SqlCommand::CreateType:
        case dbms::SqlCommand::DropType:
        case dbms::SqlCommand::CreateView:
        case dbms::SqlCommand::DropView:
        case dbms::SqlCommand::CreateTrigger:
        case dbms::SqlCommand::DropTrigger:
        case dbms::SqlCommand::CreateFunction:
        case dbms::SqlCommand::DropFunction:
        case dbms::SqlCommand::CreateProcedure:
        case dbms::SqlCommand::CreatePolicy:
        case dbms::SqlCommand::CreateMaterializedView:
        case dbms::SqlCommand::RefreshMaterializedView:
        case dbms::SqlCommand::DropMaterializedView:
        case dbms::SqlCommand::CreateDatabase:
        case dbms::SqlCommand::DropDatabase:
        case dbms::SqlCommand::CreateSchema:
        case dbms::SqlCommand::DropSchema:
        case dbms::SqlCommand::CreateCollation:
        case dbms::SqlCommand::DropCollation:
        case dbms::SqlCommand::CreateRole:
        case dbms::SqlCommand::CreateUser:
        case dbms::SqlCommand::AlterRole:
        case dbms::SqlCommand::AlterUser:
        case dbms::SqlCommand::AlterDefaultPrivileges:
        case dbms::SqlCommand::Truncate:
        case dbms::SqlCommand::DropRole:
        case dbms::SqlCommand::DropUser:
        case dbms::SqlCommand::Comment:
            handled = true;
            break;
        default:
            return false;
    }

    // The main dispatcher normalizes SQL before routing. Authentication DDL
    // must receive the original statement so password literals retain case.
    const bool authDdl = parsedCmd == dbms::SqlCommand::CreateRole ||
                         parsedCmd == dbms::SqlCommand::CreateUser ||
                         parsedCmd == dbms::SqlCommand::AlterRole ||
                         parsedCmd == dbms::SqlCommand::AlterUser ||
                         parsedCmd == dbms::SqlCommand::DropRole ||
                         parsedCmd == dbms::SqlCommand::DropUser;
    const bool preservesLiteralText =
        authDdl || parsedCmd == dbms::SqlCommand::Comment ||
        parsedCmd == dbms::SqlCommand::CreateFunction ||
        parsedCmd == dbms::SqlCommand::DropFunction ||
        parsedCmd == dbms::SqlCommand::CreateProcedure;
    const std::string& parseInput = preservesLiteralText && !rawSql.empty()
        ? rawSql : sql;
    dbms::SQLParser parser;
    dbms::ParseResult r = parser.parse(parseInput);
    if (!r.success || !r.stmt) {
        // A bridge-owned command must fail closed. Falling back after a parse
        // error can execute a different legacy interpretation.
        handled = true;
        std::cout << "SQL syntax error";
        if (!r.error.empty()) std::cout << ": " << r.error;
        std::cout << " (SQLSTATE 42601)" << std::endl;
        return true;
    }
    if (parsedCmd == dbms::SqlCommand::AlterTable) {
        const auto* alter = dynamic_cast<const dbms::AlterTableStmt*>(r.stmt.get());
        bool supported = alter && !alter->subCommands.empty();
        if (supported) {
            for (const auto& sub : alter->subCommands) {
                switch (sub.action) {
                    case dbms::AlterTableStmt::Action::AddColumn:
                    case dbms::AlterTableStmt::Action::DropColumn:
                    case dbms::AlterTableStmt::Action::AlterColumn:
                    case dbms::AlterTableStmt::Action::RenameColumn:
                    case dbms::AlterTableStmt::Action::RenameConstraint:
                    case dbms::AlterTableStmt::Action::RenameTable:
                    case dbms::AlterTableStmt::Action::SetSchema:
                    case dbms::AlterTableStmt::Action::SetTablespace:
                    case dbms::AlterTableStmt::Action::AddConstraint:
                    case dbms::AlterTableStmt::Action::DropConstraint:
                    case dbms::AlterTableStmt::Action::SetOptions:
                    case dbms::AlterTableStmt::Action::ResetOptions:
                    case dbms::AlterTableStmt::Action::SetLogged:
                    case dbms::AlterTableStmt::Action::SetUnlogged:
                    case dbms::AlterTableStmt::Action::ValidateConstraint:
                    case dbms::AlterTableStmt::Action::AlterConstraint:
                    case dbms::AlterTableStmt::Action::ClusterOn:
                    case dbms::AlterTableStmt::Action::SetWithoutCluster:
                    case dbms::AlterTableStmt::Action::SetReplicaIdentity:
                    case dbms::AlterTableStmt::Action::EnableRowLevelSecurity:
                    case dbms::AlterTableStmt::Action::DisableRowLevelSecurity:
                    case dbms::AlterTableStmt::Action::ForceRowLevelSecurity:
                    case dbms::AlterTableStmt::Action::NoForceRowLevelSecurity:
                    case dbms::AlterTableStmt::Action::EnableTrigger:
                    case dbms::AlterTableStmt::Action::DisableTrigger:
                    case dbms::AlterTableStmt::Action::AttachPartition:
                    case dbms::AlterTableStmt::Action::DetachPartition:
                    case dbms::AlterTableStmt::Action::SetStatistics:
                    case dbms::AlterTableStmt::Action::Inherit:
                    case dbms::AlterTableStmt::Action::NoInherit:
                    case dbms::AlterTableStmt::Action::Owner:
                        break;
                    default:
                        supported = false;
                        break;
                }
                if (!supported) break;
            }
        }
        if (!supported) {
            // Leave the command to main.cpp's still-supported legacy handlers.
            handled = false;
            return false;
        }
    }
    dbms::DdlExecutor ddlExec;
    return ddlExec.execute(r.stmt, s); // false=success, true=error
}

// ----------------------------------------------------------------------------
// Transaction helpers
// ----------------------------------------------------------------------------

bool DdlExecutor::checkDatabaseCommandOutsideTransaction(Session& s) {
    (void)s;
    if (!g_engine.inTransaction()) return true;
    std::cout << "ERROR: CREATE/DROP DATABASE cannot run inside a "
                 "transaction block (SQLSTATE 25001)"
              << std::endl;
    return false;
}

// ALTER TABLE is deliberately executed from the typed AST.  Keep the
// conversion here small and deterministic: the storage engine receives the
// same Column representation used by CREATE TABLE, so ALTER COLUMN TYPE does
// not have a second, subtly different type mapping.
static ColumnDef columnDefFromAlterType(const std::string& name,
                                        const std::string& typeSpec) {
    ColumnDef cd;
    cd.name = name;
    cd.isNull = true;
    std::string spec = trim(typeSpec);
    size_t lp = spec.find('(');
    if (lp == std::string::npos) {
        cd.typeName = trim(spec);
        return cd;
    }
    cd.typeName = trim(spec.substr(0, lp));
    size_t rp = spec.rfind(')');
    std::string mods = spec.substr(lp + 1, (rp == std::string::npos ? spec.size() : rp) - lp - 1);
    std::stringstream ss(mods);
    std::string mod;
    while (std::getline(ss, mod, ',')) {
        mod = trim(mod);
        if (!mod.empty()) cd.typeMods.push_back(mod);
    }
    return cd;
}

static bool alterStatusOk(DBStatus status, const std::string& operation) {
    if (status == DBStatus::OK) return true;
    if (status == DBStatus::TABLE_NOT_FOUND) std::cout << "Table not found" << std::endl;
    else if (status == DBStatus::TABLE_ALREADY_EXISTS)
        std::cout << operation << " already exists" << std::endl;
    else if (status == DBStatus::INVALID_VALUE)
        std::cout << operation << " is invalid or does not exist" << std::endl;
    else
        std::cout << operation << " failed" << std::endl;
    return false;
}

static bool updateOwnedSequenceTableNames(
    CatalogManager& catalog, const std::string& dbname, Oid tableOid,
    const std::string& oldTableName, const std::string& newTableName);

static bool updateOwnedSequenceColumnNames(
    CatalogManager& catalog, const std::string& dbname, Oid tableOid,
    int32_t columnNumber, const std::string& tableName,
    const std::string& oldColumnName, const std::string& newColumnName);

static bool dropOwnedSequencesForColumn(
    CatalogManager& catalog, const std::string& dbname,
    const std::string& physicalTableName,
    const std::string& logicalTableName, Oid tableOid,
    int32_t columnNumber, const std::string& columnName, bool cascade,
    std::set<std::string>& droppedSequenceStorageNames);

static bool executeSchemaPhysicalDropPlan(
    CatalogManager& catalog, const std::string& dbname,
    const CatalogManager::DropPlan& catalogPlan, bool cascade,
    std::set<std::string>& droppedSequenceStorageNames,
    std::string& error);

bool DdlExecutor::executeAlterTable(const AlterTableStmt* stmt, Session& s) {
    if (!stmt) return true;
    if (!checkDB(s)) return true;
    if (stmt->tableName.empty() || stmt->subCommands.empty()) {
        std::cout << "SQL syntax error: ALTER TABLE requires a subcommand" << std::endl;
        return true;
    }

    const bool ownerOnly = std::all_of(
        stmt->subCommands.begin(), stmt->subCommands.end(),
        [](const auto& sub) { return sub.action == AlterTableStmt::Action::Owner; });
    if (!ownerOnly && !checkAdmin(s)) return true;

    const bool tableIsTemporary =
        s.tempTables.count(stmt->tableName) != 0;
    const std::string tableName = resolveTableName(s, stmt->tableName);
    std::string pendingTemporaryRename;
    std::set<std::string> droppedOwnedSequenceStorageNames;

    // ALTER TABLE actions can rewrite schemas, indexes, parameters, and
    // relation files directly; the row-level undo log cannot restore those
    // changes.  Run the whole statement inside the DDL transaction so any
    // later action failure restores the pre-statement database snapshot.
    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    // Validate the action set before making any change.  The transaction
    // snapshot below additionally protects failures that occur after a
    // storage primitive has already persisted an earlier subcommand.
    for (const auto& sub : stmt->subCommands) {
        switch (sub.action) {
            case AlterTableStmt::Action::AddColumn:
            case AlterTableStmt::Action::DropColumn:
            case AlterTableStmt::Action::AlterColumn:
            case AlterTableStmt::Action::RenameColumn:
            case AlterTableStmt::Action::RenameConstraint:
            case AlterTableStmt::Action::RenameTable:
            case AlterTableStmt::Action::SetSchema:
            case AlterTableStmt::Action::SetTablespace:
            case AlterTableStmt::Action::AddConstraint:
            case AlterTableStmt::Action::DropConstraint:
            case AlterTableStmt::Action::SetOptions:
            case AlterTableStmt::Action::ResetOptions:
            case AlterTableStmt::Action::SetLogged:
            case AlterTableStmt::Action::SetUnlogged:
            case AlterTableStmt::Action::ValidateConstraint:
            case AlterTableStmt::Action::AlterConstraint:
            case AlterTableStmt::Action::ClusterOn:
            case AlterTableStmt::Action::SetWithoutCluster:
            case AlterTableStmt::Action::SetReplicaIdentity:
            case AlterTableStmt::Action::EnableRowLevelSecurity:
            case AlterTableStmt::Action::DisableRowLevelSecurity:
            case AlterTableStmt::Action::ForceRowLevelSecurity:
            case AlterTableStmt::Action::NoForceRowLevelSecurity:
            case AlterTableStmt::Action::EnableTrigger:
            case AlterTableStmt::Action::DisableTrigger:
            case AlterTableStmt::Action::AttachPartition:
            case AlterTableStmt::Action::DetachPartition:
            case AlterTableStmt::Action::SetStatistics:
            case AlterTableStmt::Action::Inherit:
            case AlterTableStmt::Action::NoInherit:
            case AlterTableStmt::Action::Owner:
                break;
            default:
                std::cout << "ALTER TABLE subcommand is not supported by the AST executor" << std::endl;
                return true;
        }
    }

    for (const auto& sub : stmt->subCommands) {
        txn.markSnapshotDirty();
        DBStatus status = DBStatus::OK;
        switch (sub.action) {
            case AlterTableStmt::Action::AddColumn: {
                if (sub.colDef.name.empty() || sub.colDef.typeName.empty()) {
                    std::cout << "SQL syntax error: ADD COLUMN requires name and type" << std::endl;
                    return true;
                }
                Column column;
                std::string typeError;
                if (!columnDefToColumn(sub.colDef, s.currentDB, column, typeError,
                                        s.compatibilityMode)) {
                    std::cout << "Invalid column type: " << typeError << std::endl;
                    return true;
                }
                status = g_engine.alterTableAddColumn(s.currentDB, tableName, column);
                if (status == DBStatus::TABLE_ALREADY_EXISTS && sub.ifNotExists) {
                    std::cout << "NOTICE: column already exists, skipping" << std::endl;
                    break;
                }
                if (status == DBStatus::TABLE_ALREADY_EXISTS) {
                    std::cout << "ERROR: column \"" << sub.colDef.name
                              << "\" of relation \"" << stmt->tableName
                              << "\" already exists (SQLSTATE 42701)"
                              << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Column")) return true;
                std::map<std::string, int32_t> declaredVarcharMods;
                int32_t varcharModifier = -1;
                if (declaredVarcharTypeMod(sub.colDef, varcharModifier)) {
                    declaredVarcharMods[sub.colDef.name] = varcharModifier;
                }
                if (!tableIsTemporary &&
                    !synchronizeTableAttributesInCatalog(
                        s.currentDB, tableName, &declaredVarcharMods)) {
                    std::cout << "ALTER TABLE ADD COLUMN catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            }
            case AlterTableStmt::Action::DropColumn:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: DROP COLUMN requires a name" << std::endl;
                    return true;
                }
                if (!tableIsTemporary) {
                    bool inherited = false;
                    if (!columnHasInheritanceParents(
                            s.currentDB, tableName, sub.name, inherited)) {
                        return true;
                    }
                    if (inherited) {
                        std::cout << "ERROR: cannot drop inherited column \""
                                  << sub.name << "\"" << std::endl;
                        return true;
                    }
                }
                if (!g_engine.getColumnComment(
                         s.currentDB, tableName, sub.name).empty() &&
                    g_engine.commentOnColumn(
                        s.currentDB, tableName, sub.name, "") !=
                        DBStatus::OK) {
                    std::cout << "ALTER TABLE DROP COLUMN comment cleanup failed"
                              << std::endl;
                    return true;
                }
                status = g_engine.alterTableDropColumn(s.currentDB, tableName, sub.name);
                if (status == DBStatus::INVALID_VALUE) {
                    const TableSchema currentSchema =
                        g_engine.getTableSchema(s.currentDB, tableName);
                    bool columnExists = false;
                    for (size_t i = 0; i < currentSchema.len; ++i) {
                        if (currentSchema.cols[i].dataName == sub.name) {
                            columnExists = true;
                            break;
                        }
                    }
                    if (!columnExists) {
                        if (sub.ifExists) {
                            std::cout << "NOTICE: column does not exist, skipping"
                                      << std::endl;
                            break;
                        }
                        std::cout << "ERROR: column \"" << sub.name
                                  << "\" of relation \"" << stmt->tableName
                                  << "\" does not exist (SQLSTATE 42703)"
                                  << std::endl;
                        return true;
                    }
                }
                if (!alterStatusOk(status, "Column")) return true;
                if (!tableIsTemporary) {
                    try {
                        CatalogManager& catalog =
                            g_engine.catalogService().get(s.currentDB);
                        const auto qualifiedName =
                            CatalogService::logicalName(tableName);
                        const std::string schemaName =
                            qualifiedName.schema.empty()
                                ? "public" : qualifiedName.schema;
                        const auto* relation = catalog.resolveRelation(
                            qualifiedName.name, {schemaName});
                        const auto* droppedAttribute = relation
                            ? catalog.findAttribute(relation->oid, sub.name)
                            : nullptr;
                        const int32_t droppedAttributeNumber =
                            droppedAttribute ? droppedAttribute->attnum : 0;
                        if (relation &&
                            (!droppedAttribute ||
                             !dropOwnedSequencesForColumn(
                                 catalog, s.currentDB, tableName,
                                 schemaName == "public"
                                     ? qualifiedName.name
                                     : schemaName + "." + qualifiedName.name,
                                 relation->oid, droppedAttributeNumber,
                                 sub.name,
                                 sub.options.find("cascade") !=
                                     sub.options.end(),
                                 droppedOwnedSequenceStorageNames) ||
                             !catalog.remapColumnMetadataAfterDrop(
                                 relation->oid,
                                 droppedAttributeNumber))) {
                            std::cout
                                << "ALTER TABLE DROP COLUMN has dependent catalog objects"
                                << std::endl;
                            return true;
                        }
                    } catch (const std::exception& error) {
                        std::cout << "ALTER TABLE DROP COLUMN catalog remap failed: "
                                  << error.what() << std::endl;
                        return true;
                    }
                    if (!synchronizeTableAttributesInCatalog(
                            s.currentDB, tableName)) {
                        std::cout
                            << "ALTER TABLE DROP COLUMN catalog update failed"
                            << std::endl;
                        return true;
                    }
                }
                break;
            case AlterTableStmt::Action::RenameColumn:
                if (sub.name.empty() || sub.newName.empty()) {
                    std::cout << "SQL syntax error: RENAME COLUMN requires two names" << std::endl;
                    return true;
                }
                if (!tableIsTemporary) {
                    bool inherited = false;
                    if (!columnHasInheritanceParents(
                            s.currentDB, tableName, sub.name, inherited)) {
                        return true;
                    }
                    if (inherited) {
                        std::cout << "ERROR: cannot rename inherited column \""
                                  << sub.name << "\"" << std::endl;
                        return true;
                    }
                }
                status = g_engine.alterTableRenameColumn(s.currentDB, tableName,
                                                         sub.name, sub.newName);
                if (status == DBStatus::INVALID_VALUE) {
                    const TableSchema currentSchema =
                        g_engine.getTableSchema(s.currentDB, tableName);
                    bool sourceColumnExists = false;
                    for (size_t i = 0; i < currentSchema.len; ++i) {
                        if (currentSchema.cols[i].dataName == sub.name) {
                            sourceColumnExists = true;
                            break;
                        }
                    }
                    if (!sourceColumnExists) {
                        if (sub.ifExists) {
                            std::cout << "NOTICE: column does not exist, skipping"
                                      << std::endl;
                            break;
                        }
                        std::cout << "ERROR: column \"" << sub.name
                                  << "\" does not exist (SQLSTATE 42703)"
                                  << std::endl;
                        return true;
                    }
                }
                if (status == DBStatus::TABLE_ALREADY_EXISTS) {
                    std::cout << "ERROR: column \"" << sub.newName
                              << "\" of relation \"" << stmt->tableName
                              << "\" already exists (SQLSTATE 42701)"
                              << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Column")) return true;
                if (!tableIsTemporary) {
                    try {
                        dbms::CatalogManager& catalog =
                            g_engine.catalogService().get(s.currentDB);
                        const auto qualifiedName =
                            dbms::CatalogService::logicalName(tableName);
                        const std::string schemaName =
                            qualifiedName.schema.empty()
                                ? "public" : qualifiedName.schema;
                        const auto* relation = catalog.resolveRelation(
                            qualifiedName.name, {schemaName});
                        if (relation) {
                            const dbms::Oid relationOid = relation->oid;
                            if (!catalog.renameAttribute(
                                    relationOid, sub.name, sub.newName)) {
                                std::cout
                                    << "ALTER TABLE RENAME COLUMN catalog update failed"
                                    << std::endl;
                                return true;
                            }
                            const auto* renamedAttribute = catalog.findAttribute(
                                relationOid, sub.newName);
                            const std::string logicalTableName =
                                schemaName == "public"
                                    ? qualifiedName.name
                                    : schemaName + "." + qualifiedName.name;
                            if (!renamedAttribute ||
                                !updateOwnedSequenceColumnNames(
                                    catalog, s.currentDB, relationOid,
                                    renamedAttribute->attnum, logicalTableName,
                                    sub.name, sub.newName) ||
                                !catalog.persistAll()) {
                                std::cout
                                    << "ALTER TABLE RENAME COLUMN owned sequence update failed"
                                    << std::endl;
                                return true;
                            }
                        }
                    } catch (const std::exception& e) {
                        std::cerr
                            << "ALTER TABLE RENAME COLUMN catalog update failed: "
                            << e.what() << std::endl;
                        return true;
                    }
                }
                break;
            case AlterTableStmt::Action::RenameConstraint:
                if (sub.name.empty() || sub.newName.empty()) {
                    std::cout << "SQL syntax error: RENAME CONSTRAINT requires two names" << std::endl;
                    return true;
                }
                status = g_engine.alterTableRenameConstraint(s.currentDB, tableName,
                                                             sub.name, sub.newName);
                if (status == DBStatus::NOT_FOUND && sub.ifExists) {
                    std::cout << "NOTICE: constraint does not exist, skipping" << std::endl;
                    break;
                }
                if (status == DBStatus::NOT_FOUND) {
                    std::cout << "ERROR: constraint \"" << sub.name
                              << "\" for table \"" << stmt->tableName
                              << "\" does not exist (SQLSTATE 42704)"
                              << std::endl;
                    return true;
                }
                if (status == DBStatus::TABLE_ALREADY_EXISTS) {
                    std::cout << "ERROR: constraint \"" << sub.newName
                              << "\" for relation \"" << stmt->tableName
                              << "\" already exists (SQLSTATE 42710)"
                              << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Constraint")) return true;
                break;
            case AlterTableStmt::Action::RenameTable: {
                if (sub.newName.empty()) {
                    std::cout << "SQL syntax error: RENAME TO requires a table name" << std::endl;
                    return true;
                }
                const auto qualifiedName =
                    dbms::CatalogService::logicalName(tableName);
                if (!tableIsTemporary &&
                    sub.newName != qualifiedName.name) {
                    try {
                        dbms::CatalogManager& catalog =
                            g_engine.catalogService().get(s.currentDB);
                        const std::string schemaName =
                            qualifiedName.schema.empty()
                                ? "public" : qualifiedName.schema;
                        if (catalog.resolveRelation(sub.newName,
                                                    {schemaName})) {
                            std::cout << "ERROR: relation \"" << sub.newName
                                      << "\" already exists (SQLSTATE 42P07)"
                                      << std::endl;
                            return true;
                        }
                    } catch (const std::exception& error) {
                        std::cout << "ALTER TABLE RENAME catalog lookup failed: "
                                  << error.what() << std::endl;
                        return true;
                    }
                }
                const std::string physicalNewName = tableIsTemporary
                    ? tempTablePrefix(s, sub.newName)
                    : (qualifiedName.schema.empty()
                           ? sub.newName
                           : qualifiedName.schema + "__" + sub.newName);
                status = g_engine.alterTableRenameTable(
                    s.currentDB, tableName, physicalNewName);
                if (status == DBStatus::TABLE_ALREADY_EXISTS) {
                    std::cout << "ERROR: relation \"" << sub.newName
                              << "\" already exists (SQLSTATE 42P07)"
                              << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Table")) return true;
                if (tableIsTemporary) {
                    pendingTemporaryRename = sub.newName;
                } else {
                    try {
                        dbms::CatalogManager& catalog =
                            g_engine.catalogService().get(s.currentDB);
                        const std::string schemaName = qualifiedName.schema.empty()
                            ? "public"
                            : qualifiedName.schema;
                        const auto* relation = catalog.resolveRelation(
                            qualifiedName.name, {schemaName});
                        if (relation) {
                            const dbms::Oid relationOid = relation->oid;
                            if (!catalog.renameClass(relationOid, sub.newName)) {
                                std::cout << "ALTER TABLE RENAME catalog update failed"
                                          << std::endl;
                                return true;
                            }
                            const std::string oldLogicalName =
                                schemaName == "public"
                                    ? qualifiedName.name
                                    : schemaName + "." + qualifiedName.name;
                            const std::string newLogicalName =
                                schemaName == "public"
                                    ? sub.newName
                                    : schemaName + "." + sub.newName;
                            if (!updateOwnedSequenceTableNames(
                                    catalog, s.currentDB, relationOid,
                                    oldLogicalName, newLogicalName) ||
                                !catalog.persistAll()) {
                                std::cout
                                    << "ALTER TABLE RENAME owned sequence update failed"
                                    << std::endl;
                                return true;
                            }
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "ALTER TABLE RENAME catalog update failed: "
                                  << e.what() << std::endl;
                        return true;
                    }
                }
                break;
            }
            case AlterTableStmt::Action::AlterColumn: {
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: ALTER COLUMN requires a name" << std::endl;
                    return true;
                }
                std::map<std::string, int32_t> declaredVarcharMods;
                if (!sub.identityAction.empty()) {
                    if (sub.colDef.hasIdentityOptions) {
                        std::cout << "ERROR: identity sequence options are not "
                                     "supported (SQLSTATE 0A000)" << std::endl;
                        return true;
                    }
                    status = g_engine.alterTableIdentity(
                        s.currentDB, tableName, sub.name,
                        sub.identityAction, sub.identityKind,
                        sub.identityIfExists);
                } else if (sub.defaultValue) {
                    status = g_engine.alterTableSetDefault(s.currentDB, tableName,
                                                            sub.name, sub.defaultValue->toString());
                } else if (sub.dropDefault) {
                    status = g_engine.alterTableDropDefault(s.currentDB, tableName, sub.name);
                } else if (sub.setNotNull) {
                    status = g_engine.alterTableSetNotNull(s.currentDB, tableName, sub.name);
                } else if (sub.dropNotNull) {
                    status = g_engine.alterTableDropNotNull(s.currentDB, tableName, sub.name);
                } else if (!sub.dataType.empty()) {
                    ColumnDef cd = columnDefFromAlterType(sub.name, sub.dataType);
                    Column column;
                    std::string error;
                    if (!columnDefToColumn(cd, s.currentDB, column, error,
                                            s.compatibilityMode)) {
                        std::cout << "Invalid column type: " << error << std::endl;
                        return true;
                    }
                    int32_t varcharModifier = -1;
                    if (declaredVarcharTypeMod(cd, varcharModifier)) {
                        declaredVarcharMods[sub.name] = varcharModifier;
                    }
                    status = g_engine.alterTableAlterColumnType(
                        s.currentDB, tableName, sub.name, column);
                } else {
                    std::cout << "ALTER COLUMN subcommand is unsupported" << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Column")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableAttributesInCatalog(
                        s.currentDB, tableName, &declaredVarcharMods)) {
                    std::cout << "ALTER COLUMN catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            }
            case AlterTableStmt::Action::SetStatistics: {
                if (sub.name.empty() || sub.statisticsTarget < -1 ||
                    sub.statisticsTarget > 10000) {
                    std::cout << "Invalid statistics target" << std::endl;
                    return true;
                }
                const TableSchema table =
                    g_engine.getTableSchema(s.currentDB, tableName);
                bool columnExists = false;
                for (size_t column = 0; column < table.len; ++column) {
                    if (table.cols[column].dataName == sub.name) {
                        columnExists = true;
                        break;
                    }
                }
                if (!columnExists) {
                    std::cout << "Column " << sub.name << " not found"
                              << std::endl;
                    return true;
                }
                std::map<std::string, std::string> params;
                params["column_statistics:" + sub.name] =
                    sub.statisticsTarget == -1
                        ? std::string{}
                        : std::to_string(sub.statisticsTarget);
                status = g_engine.updateStorageParams(s.currentDB, tableName, params);
                if (!alterStatusOk(status, "Statistics")) return true;
                if (!tableIsTemporary &&
                    !updateColumnStatisticsInCatalog(
                        s.currentDB, tableName, sub.name,
                        sub.statisticsTarget)) {
                    std::cout
                        << "ALTER COLUMN SET STATISTICS catalog update failed"
                        << std::endl;
                    return true;
                }
                break;
            }
            case AlterTableStmt::Action::SetLogged:
                status = g_engine.alterTableSetLogged(s.currentDB, tableName, true);
                if (!alterStatusOk(status, "Table")) return true;
                if (!tableIsTemporary &&
                    !updateTableClassInCatalog(
                        s.currentDB, tableName,
                        [](PgClassRow& relation) {
                            relation.relpersistence = 'p';
                        })) {
                    std::cout << "ALTER TABLE SET LOGGED catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            case AlterTableStmt::Action::SetUnlogged:
                status = g_engine.alterTableSetLogged(s.currentDB, tableName, false);
                if (!alterStatusOk(status, "Table")) return true;
                if (!tableIsTemporary &&
                    !updateTableClassInCatalog(
                        s.currentDB, tableName,
                        [](PgClassRow& relation) {
                            relation.relpersistence = 'u';
                        })) {
                    std::cout
                        << "ALTER TABLE SET UNLOGGED catalog update failed"
                        << std::endl;
                    return true;
                }
                break;
            case AlterTableStmt::Action::ClusterOn:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: CLUSTER ON requires an index name" << std::endl;
                    return true;
                }
                status = g_engine.alterTableSetCluster(s.currentDB, tableName, sub.name);
                if (!alterStatusOk(status, "Cluster")) return true;
                break;
            case AlterTableStmt::Action::SetWithoutCluster:
                status = g_engine.alterTableSetCluster(s.currentDB, tableName, "");
                if (!alterStatusOk(status, "Cluster")) return true;
                break;
            case AlterTableStmt::Action::SetReplicaIdentity:
                if (sub.replicaIdentity.empty()) {
                    std::cout << "SQL syntax error: invalid REPLICA IDENTITY mode" << std::endl;
                    return true;
                }
                status = g_engine.alterTableSetReplicaIdentity(
                    s.currentDB, tableName, sub.replicaIdentity, sub.name);
                if (!alterStatusOk(status, "Replica identity")) return true;
                break;
            case AlterTableStmt::Action::ValidateConstraint: {
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: VALIDATE CONSTRAINT requires a name" << std::endl;
                    return true;
                }
                if (!tableConstraintExists(s.currentDB, tableName, sub.name)) {
                    std::cout << "Constraint not found" << std::endl;
                    return true;
                }
                status = g_engine.updateStorageParams(
                    s.currentDB, tableName,
                    {{constraintMetadataKey(sub.name, "validated"), "1"},
                     {constraintMetadataKey(sub.name, "not_valid"), "0"}});
                if (!alterStatusOk(status, "Constraint")) return true;
                std::cout << "Constraint " << sub.name << " validated" << std::endl;
                break;
            }
            case AlterTableStmt::Action::AlterConstraint: {
                if (sub.name.empty() || (!sub.setDeferrable && !sub.setInitiallyDeferred)) {
                    std::cout << "SQL syntax error: ALTER CONSTRAINT requires a deferrability option"
                              << std::endl;
                    return true;
                }
                if (!tableConstraintExists(s.currentDB, tableName, sub.name)) {
                    std::cout << "Constraint not found" << std::endl;
                    return true;
                }
                const auto params = g_engine.getStorageParams(s.currentDB, tableName);
                const bool validated = metadataBool(
                    params, constraintMetadataKey(sub.name, "validated"), true);
                const bool notValid = metadataBool(
                    params, constraintMetadataKey(sub.name, "not_valid"), !validated);
                bool deferrable = metadataBool(
                    params, constraintMetadataKey(sub.name, "deferrable"), false);
                bool initiallyDeferred = metadataBool(
                    params, constraintMetadataKey(sub.name, "initially_deferred"), false);
                if (sub.setDeferrable) deferrable = sub.deferrable;
                if (sub.setInitiallyDeferred) initiallyDeferred = sub.initiallyDeferred;
                if (initiallyDeferred) deferrable = true;
                status = persistConstraintMetadata(
                    s.currentDB, tableName, sub.name, validated, notValid,
                    deferrable, initiallyDeferred);
                if (!alterStatusOk(status, "Constraint")) return true;
                if (checkConstraintExists(s.currentDB, tableName, sub.name)) {
                    status = g_engine.alterTableSetConstraintDeferrability(
                        s.currentDB, tableName, sub.name, deferrable, initiallyDeferred);
                    if (!alterStatusOk(status, "Constraint")) return true;
                }
                std::cout << "Constraint " << sub.name << " options updated" << std::endl;
                break;
            }
            case AlterTableStmt::Action::EnableRowLevelSecurity:
                status = g_engine.alterTableRowLevelSecurity(
                    s.currentDB, tableName, true, std::nullopt);
                if (!alterStatusOk(status, "Table")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableRlsInCatalog(s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE row-security catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            case AlterTableStmt::Action::DisableRowLevelSecurity:
                status = g_engine.alterTableRowLevelSecurity(
                    s.currentDB, tableName, false, std::nullopt);
                if (!alterStatusOk(status, "Table")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableRlsInCatalog(s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE row-security catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            case AlterTableStmt::Action::ForceRowLevelSecurity:
                status = g_engine.alterTableRowLevelSecurity(
                    s.currentDB, tableName, std::nullopt, true);
                if (!alterStatusOk(status, "Table")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableRlsInCatalog(s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE row-security catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            case AlterTableStmt::Action::NoForceRowLevelSecurity:
                status = g_engine.alterTableRowLevelSecurity(
                    s.currentDB, tableName, std::nullopt, false);
                if (!alterStatusOk(status, "Table")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableRlsInCatalog(s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE row-security catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            case AlterTableStmt::Action::EnableTrigger:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: ENABLE TRIGGER requires a name" << std::endl;
                    return true;
                }
                status = g_engine.enableTrigger(
                    s.currentDB, sub.name, tableName);
                if (!alterStatusOk(status, "Trigger")) return true;
                break;
            case AlterTableStmt::Action::DisableTrigger:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: DISABLE TRIGGER requires a name" << std::endl;
                    return true;
                }
                status = g_engine.disableTrigger(
                    s.currentDB, sub.name, tableName);
                if (!alterStatusOk(status, "Trigger")) return true;
                break;
            case AlterTableStmt::Action::AttachPartition:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: ATTACH PARTITION requires a name" << std::endl;
                    return true;
                }
                status = g_engine.attachPartition(s.currentDB, tableName, sub.name,
                                                  sub.partitionSpec);
                if (!alterStatusOk(status, "Partition")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableHierarchyFlagsInCatalog(
                        s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE partition catalog update failed"
                              << std::endl;
                    return true;
                }
                if (!tableIsTemporary) {
                    const std::string partitionTableName =
                        resolveTableName(s, sub.name);
                    if (g_engine.tableExists(
                            s.currentDB, partitionTableName) &&
                        !synchronizeTableHierarchyFlagsInCatalog(
                            s.currentDB, partitionTableName, true)) {
                        std::cout << "ALTER TABLE partition catalog update failed"
                                  << std::endl;
                        return true;
                    }
                }
                break;
            case AlterTableStmt::Action::DetachPartition:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: DETACH PARTITION requires a name" << std::endl;
                    return true;
                }
                status = g_engine.detachPartition(s.currentDB, tableName, sub.name);
                if (!alterStatusOk(status, "Partition")) return true;
                if (!tableIsTemporary &&
                    !synchronizeTableHierarchyFlagsInCatalog(
                        s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE partition catalog update failed"
                              << std::endl;
                    return true;
                }
                if (!tableIsTemporary) {
                    const std::string partitionTableName =
                        resolveTableName(s, sub.name);
                    if (g_engine.tableExists(
                            s.currentDB, partitionTableName) &&
                        !synchronizeTableHierarchyFlagsInCatalog(
                            s.currentDB, partitionTableName, false)) {
                        std::cout << "ALTER TABLE partition catalog update failed"
                                  << std::endl;
                        return true;
                    }
                }
                break;
            case AlterTableStmt::Action::SetSchema:
                status = g_engine.alterTableSetSchema(s.currentDB, tableName, sub.newName);
                if (!alterStatusOk(status, "Schema")) return true;
                break;
            case AlterTableStmt::Action::SetTablespace:
                status = g_engine.alterTableTablespace(s.currentDB, tableName, sub.newName);
                if (!alterStatusOk(status, "Tablespace")) return true;
                break;
            case AlterTableStmt::Action::AddConstraint: {
                const auto& tc = sub.constraint;
                const std::string type = toLower(tc.type);
                std::string constraintName = tc.name;
                if (constraintName.empty()) {
                    std::string suffix = tc.columns.empty() ? "constraint" : tc.columns.front();
                    if (type == "primary key") constraintName = tableName + "_pkey";
                    else if (type == "unique") constraintName = tableName + "_" + suffix + "_key";
                    else if (type == "foreign key") constraintName = tableName + "_" + suffix + "_fkey";
                    else if (type == "check") constraintName = tableName + "_check";
                    else if (type == "exclude") constraintName = tableName + "_excl";
                    else {
                        std::cout << "ALTER TABLE constraint name is required" << std::endl;
                        return true;
                    }
                }
                if (type == "exclude") {
                    if (tc.excludeElements.empty()) {
                        std::cout << "EXCLUDE constraint requires at least one element" << std::endl;
                        return true;
                    }
                    if (!g_engine.tableExists(s.currentDB, tableName)) {
                        std::cout << "Table not found" << std::endl;
                        return true;
                    }
                    const auto table = g_engine.getTableSchema(s.currentDB, tableName);
                    bool nameExists = false;
                    for (size_t i = 0; i < table.len && !nameExists; ++i) {
                        nameExists = table.cols[i].checkConstraintName == constraintName;
                    }
                    for (const auto& check : table.additionalCheckConstraints) {
                        if (check.name == constraintName) {
                            nameExists = true;
                            break;
                        }
                    }
                    for (const auto& name : table.uniqueConstraintNames) {
                        if (name == constraintName) { nameExists = true; break; }
                    }
                    for (size_t i = 0; i < table.fkLen && !nameExists; ++i) {
                        nameExists = table.fks[i].name == constraintName;
                    }
                    const auto exclusions = g_engine.getExclusionConstraints(s.currentDB, tableName);
                    if (std::any_of(exclusions.begin(), exclusions.end(),
                                    [&](const auto& ec) { return ec.name == constraintName; })) {
                        nameExists = true;
                    }
                    if (nameExists) {
                        std::cout << "Constraint name already exists" << std::endl;
                        return true;
                    }
                    StorageEngine::ExclusionConstraint ec;
                    ec.name = constraintName;
                    ec.tableName = tableName;
                    ec.accessMethod = tc.accessMethod.empty() ? "btree" : toLower(tc.accessMethod);
                    for (const auto& element : tc.excludeElements) {
                        if (element.first.empty() || element.second.empty()) {
                            std::cout << "Invalid EXCLUDE constraint element" << std::endl;
                            return true;
                        }
                        ec.elements.push_back({element.first, toLower(element.second)});
                    }
                    ec.wherePredicate = tc.excludeWhere;
                    status = g_engine.createExclusionConstraint(s.currentDB, ec);
                    if (!alterStatusOk(status, "Constraint")) return true;
                } else if (type == "check") {
                    if (!tc.checkExpr) {
                        std::cout << "CHECK constraint expression is required" << std::endl;
                        return true;
                    }
                    status = g_engine.alterTableAddCheckConstraint(
                        s.currentDB, tableName, constraintName, tc.checkExpr->toString());
                } else if (type == "primary key") {
                    status = g_engine.alterTableAddPrimaryKey(
                        s.currentDB, tableName, constraintName, tc.columns);
                } else if (type == "unique") {
                    status = g_engine.alterTableAddUniqueConstraint(
                        s.currentDB, tableName, constraintName, tc.columns);
                } else if (type == "foreign key") {
                    status = g_engine.alterTableAddFKConstraint(
                        s.currentDB, tableName, constraintName, tc.columns, tc.refTable,
                        tc.refColumns, tc.onDelete, tc.onUpdate);
                } else {
                    std::cout << "ALTER TABLE constraint type is unsupported" << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Constraint")) return true;
                if (type == "check") {
                    status = g_engine.alterTableSetConstraintDeferrability(
                        s.currentDB, tableName, constraintName,
                        tc.deferrable, tc.initiallyDeferred);
                    if (!alterStatusOk(status, "Constraint")) return true;
                }
                TableConstraint metadata;
                metadata.name = constraintName;
                metadata.type = tc.type;
                metadata.columns = tc.columns;
                metadata.refTable = tc.refTable;
                metadata.refColumns = tc.refColumns;
                metadata.accessMethod = tc.accessMethod;
                metadata.excludeElements = tc.excludeElements;
                metadata.excludeWhere = tc.excludeWhere;
                status = persistConstraintMetadata(
                    s.currentDB, tableName, constraintName, !tc.notValid, tc.notValid,
                    tc.deferrable, tc.initiallyDeferred);
                if (!alterStatusOk(status, "Constraint")) return true;
                if (!tableIsTemporary &&
                    (type == "primary key" || type == "unique") &&
                    !synchronizeTableIndexFlagInCatalog(
                        s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE index catalog update failed"
                              << std::endl;
                    return true;
                }
                if (type == "check" && !tableIsTemporary &&
                    !synchronizeTableCheckCountInCatalog(
                        s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE CHECK catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            }
            case AlterTableStmt::Action::DropConstraint: {
                const auto exclusions = g_engine.getExclusionConstraints(s.currentDB, tableName);
                const bool isExclusion = std::any_of(
                    exclusions.begin(), exclusions.end(),
                    [&](const auto& ec) { return ec.name == sub.name; });
                status = isExclusion
                    ? g_engine.dropExclusionConstraint(s.currentDB, sub.name)
                    : g_engine.alterTableDropConstraint(s.currentDB, tableName, sub.name);
                if (status == DBStatus::INVALID_VALUE && sub.ifExists) {
                    std::cout << "NOTICE: constraint does not exist, skipping" << std::endl;
                    break;
                }
                if (!alterStatusOk(status, "Constraint")) return true;
                status = g_engine.updateStorageParams(
                    s.currentDB, tableName,
                    {{constraintMetadataKey(sub.name, "validated"), ""},
                     {constraintMetadataKey(sub.name, "not_valid"), ""},
                     {constraintMetadataKey(sub.name, "deferrable"), ""},
                     {constraintMetadataKey(sub.name, "initially_deferred"), ""}});
                if (!alterStatusOk(status, "Constraint")) return true;
                if (!tableIsTemporary && !isExclusion &&
                    !synchronizeTableCheckCountInCatalog(
                        s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE CHECK catalog update failed"
                              << std::endl;
                    return true;
                }
                if (!tableIsTemporary && !isExclusion &&
                    !synchronizeTableIndexFlagInCatalog(
                        s.currentDB, tableName)) {
                    std::cout << "ALTER TABLE index catalog update failed"
                              << std::endl;
                    return true;
                }
                break;
            }
            case AlterTableStmt::Action::SetOptions:
            case AlterTableStmt::Action::ResetOptions: {
                std::map<std::string, std::string> params;
                for (const auto& option : sub.options) params[option.first] = option.second;
                status = g_engine.updateStorageParams(s.currentDB, tableName, params);
                if (!alterStatusOk(status, "Storage option")) return true;
                break;
            }
            case AlterTableStmt::Action::Inherit:
            case AlterTableStmt::Action::NoInherit: {
                if (sub.parentTable.empty()) {
                    std::cout << "INHERIT requires a parent table" << std::endl;
                    return true;
                }
                if (!g_engine.tableExists(s.currentDB, tableName)) {
                    std::cout << "Table " << stmt->tableName
                              << " not found" << std::endl;
                    return true;
                }
                const std::string parentName =
                    resolveTableName(s, sub.parentTable);
                if (!g_engine.tableExists(s.currentDB, parentName)) {
                    std::cout << "Parent table " << sub.parentTable
                              << " not found" << std::endl;
                    return true;
                }
                if (parentName == tableName) {
                    std::cout << "A table cannot inherit from itself" << std::endl;
                    return true;
                }

                if (sub.action == AlterTableStmt::Action::Inherit) {
                    const TableSchema childSchema =
                        g_engine.getTableSchema(s.currentDB, tableName);
                    const TableSchema parentSchema =
                        g_engine.getTableSchema(s.currentDB, parentName);
                    for (size_t parentColumn = 0;
                         parentColumn < parentSchema.len; ++parentColumn) {
                        const Column& expected = parentSchema.cols[parentColumn];
                        const Column* actual = nullptr;
                        for (size_t childColumn = 0;
                             childColumn < childSchema.len; ++childColumn) {
                            if (childSchema.cols[childColumn].dataName ==
                                expected.dataName) {
                                actual = &childSchema.cols[childColumn];
                                break;
                            }
                        }
                        if (!actual || actual->dataType != expected.dataType ||
                            actual->dsize != expected.dsize ||
                            actual->isVariableLength != expected.isVariableLength ||
                            actual->isUnsigned != expected.isUnsigned ||
                            actual->isArray != expected.isArray ||
                            actual->collation != expected.collation ||
                            actual->enumValues != expected.enumValues ||
                            actual->domainName != expected.domainName) {
                            std::cout << "Child table has no compatible inherited column "
                                      << expected.dataName << std::endl;
                            return true;
                        }
                        if (!expected.isNull && actual->isNull) {
                            std::cout << "Child column " << expected.dataName
                                      << " must be NOT NULL to inherit"
                                      << std::endl;
                            return true;
                        }
                        const bool parentGenerated =
                            !expected.generatedExpr.empty();
                        const bool childGenerated =
                            !actual->generatedExpr.empty();
                        if (parentGenerated != childGenerated ||
                            (parentGenerated &&
                             (expected.generatedKind != actual->generatedKind ||
                              expected.generatedExpr != actual->generatedExpr))) {
                            std::cout << "Child column " << expected.dataName
                                      << " has incompatible generation expression"
                                      << std::endl;
                            return true;
                        }
                    }

                    struct InheritableCheck {
                        std::string name;
                        std::string expression;
                        bool deferrable = false;
                        bool initiallyDeferred = false;
                    };
                    const auto collectChecks = [](const TableSchema& table) {
                        std::vector<InheritableCheck> checks;
                        for (size_t columnIndex = 0;
                             columnIndex < table.len; ++columnIndex) {
                            const Column& column = table.cols[columnIndex];
                            if (column.checkExpr.empty()) continue;
                            checks.push_back({
                                column.checkConstraintName,
                                column.checkExpr,
                                column.deferrable,
                                column.initiallyDeferred});
                        }
                        for (const auto& check :
                             table.additionalCheckConstraints) {
                            if (check.expression.empty()) continue;
                            checks.push_back({check.name, check.expression,
                                              check.deferrable,
                                              check.initiallyDeferred});
                        }
                        return checks;
                    };
                    auto childChecks = collectChecks(childSchema);
                    for (const auto& required :
                         collectChecks(parentSchema)) {
                        const auto matching = std::find_if(
                            childChecks.begin(), childChecks.end(),
                            [&](const InheritableCheck& candidate) {
                                return candidate.name == required.name &&
                                       candidate.expression ==
                                           required.expression &&
                                       candidate.deferrable ==
                                           required.deferrable &&
                                       candidate.initiallyDeferred ==
                                           required.initiallyDeferred;
                            });
                        if (matching == childChecks.end()) {
                            std::cout << "Child table is missing matching CHECK constraint";
                            if (!required.name.empty()) {
                                std::cout << " " << required.name;
                            }
                            std::cout << std::endl;
                            return true;
                        }
                        childChecks.erase(matching);
                    }

                    // Adding parent -> child is cyclic if parent is already
                    // reachable below child in the persisted graph.
                    std::vector<std::string> pending{tableName};
                    std::set<std::string> visited;
                    while (!pending.empty()) {
                        const std::string current = pending.back();
                        pending.pop_back();
                        if (!visited.insert(current).second) continue;
                        for (const auto& descendant :
                             g_engine.getInheritedChildren(
                                 s.currentDB, current)) {
                            if (descendant == parentName) {
                                std::cout << "Inheritance cycle is not allowed"
                                          << std::endl;
                                return true;
                            }
                            pending.push_back(descendant);
                        }
                    }
                }

                const auto path =
                    std::filesystem::path(g_engine.dbPath(s.currentDB)) /
                    ".inherits";
                std::ostringstream rewritten;
                bool edgeFound = false;
                bool graphChanged = false;
                std::error_code inheritanceError;
                if (std::filesystem::exists(path, inheritanceError)) {
                    std::ifstream input(path);
                    if (!input) {
                        std::cout << "Could not read inheritance metadata"
                                  << std::endl;
                        return true;
                    }
                    std::string line;
                    while (std::getline(input, line)) {
                        const size_t separator = line.find('|');
                        if (separator == std::string::npos) {
                            rewritten << line << '\n';
                            continue;
                        }
                        const std::string storedParent =
                            line.substr(0, separator);
                        const std::string storedChild =
                            line.substr(separator + 1);
                        if (storedParent == parentName &&
                            storedChild == tableName) {
                            edgeFound = true;
                            if (sub.action ==
                                AlterTableStmt::Action::NoInherit) {
                                graphChanged = true;
                                continue;
                            }
                        }
                        rewritten << line << '\n';
                    }
                    if (input.bad()) {
                        std::cout << "Could not read inheritance metadata"
                                  << std::endl;
                        return true;
                    }
                } else if (inheritanceError) {
                    std::cout << "Could not inspect inheritance metadata"
                              << std::endl;
                    return true;
                }
                if (sub.action == AlterTableStmt::Action::Inherit &&
                    edgeFound) {
                    std::cout << "Relation \"" << stmt->tableName
                              << "\" would be inherited from \""
                              << sub.parentTable << "\" more than once"
                              << std::endl;
                    return true;
                }
                if (sub.action == AlterTableStmt::Action::NoInherit &&
                    !edgeFound) {
                    std::cout << "Relation \"" << sub.parentTable
                              << "\" is not a parent of relation \""
                              << stmt->tableName << "\"" << std::endl;
                    return true;
                }
                if (sub.action == AlterTableStmt::Action::Inherit) {
                    rewritten << parentName << '|' << tableName << '\n';
                    graphChanged = true;
                }
                if (graphChanged &&
                    !index_file::writeAtomically(path, rewritten.str())) {
                    std::cout << "Could not persist inheritance metadata"
                              << std::endl;
                    return true;
                }
                if (!tableIsTemporary) {
                    try {
                        CatalogManager& catalog =
                            g_engine.catalogService().get(s.currentDB);
                        if (!updateInheritanceColumnsInCatalog(
                                catalog, s.currentDB, tableName, parentName,
                                sub.action ==
                                    AlterTableStmt::Action::Inherit) ||
                            !updateTableHierarchyFlagsInCatalog(
                                catalog, s.currentDB, parentName) ||
                            !catalog.persistAll()) {
                            std::cout
                                << "ALTER TABLE inheritance catalog update failed"
                                << std::endl;
                            return true;
                        }
                    } catch (const std::exception& error) {
                        std::cout
                            << "ALTER TABLE inheritance catalog update failed: "
                            << error.what() << std::endl;
                        return true;
                    }
                }
                break;
            }
            case AlterTableStmt::Action::Owner:
                {
                    const std::string targetOwner = canonicalRoleName(sub.newName);
                    if (targetOwner.empty()) {
                    std::cout << "ALTER TABLE OWNER TO requires a role" << std::endl;
                    return true;
                    }
                    const std::string sessionUser = s.authenticatedUser.empty()
                                                        ? s.username
                                                        : s.authenticatedUser;
                    const std::string effectiveRole = effectiveSessionRole(s);
                    const auto table = g_engine.getTableSchema(s.currentDB, tableName);
                    if (!canAlterTableOwner(sessionUser, effectiveRole, table.owner,
                                             targetOwner)) {
                        std::cout << "permission denied: must own table and be able to SET ROLE to "
                                  << targetOwner << std::endl;
                        return true;
                    }
                    status = g_engine.alterTableOwner(s.currentDB, tableName, targetOwner);
                }
                if (!alterStatusOk(status, "Owner")) return true;
                break;
            default:
                return true;
        }
    }
    txn.recordUpdate(DdlObjectKind::Table, tableName);
    for (const auto& sequenceName : droppedOwnedSequenceStorageNames) {
        txn.recordDrop(DdlObjectKind::Sequence, sequenceName);
    }
    bool temporarySessionRenamed = false;
    bool hadOnCommitAction = false;
    bool wasCreatedInTransaction = false;
    std::string onCommitAction;
    if (tableIsTemporary && !pendingTemporaryRename.empty()) {
        s.tempTables.erase(stmt->tableName);
        s.tempTables.insert(pendingTemporaryRename);
        const auto action = s.tempTableOnCommit.find(stmt->tableName);
        if (action != s.tempTableOnCommit.end()) {
            hadOnCommitAction = true;
            onCommitAction = action->second;
            s.tempTableOnCommit.erase(action);
            s.tempTableOnCommit[pendingTemporaryRename] = onCommitAction;
        }
        wasCreatedInTransaction =
            s.tempTablesCreatedInTransaction.erase(stmt->tableName) != 0;
        if (wasCreatedInTransaction) {
            s.tempTablesCreatedInTransaction.insert(pendingTemporaryRename);
        }
        temporarySessionRenamed = true;
    }
    if (!txn.commit()) {
        if (temporarySessionRenamed) {
            s.tempTables.erase(pendingTemporaryRename);
            s.tempTables.insert(stmt->tableName);
            s.tempTableOnCommit.erase(pendingTemporaryRename);
            if (hadOnCommitAction) {
                s.tempTableOnCommit[stmt->tableName] = onCommitAction;
            }
            s.tempTablesCreatedInTransaction.erase(pendingTemporaryRename);
            if (wasCreatedInTransaction) {
                s.tempTablesCreatedInTransaction.insert(stmt->tableName);
            }
        }
        return true;
    }
    for (const auto& sequenceName : droppedOwnedSequenceStorageNames) {
        s.sequenceLastValues.erase(sequenceName);
        if (sequenceName.find('.') == std::string::npos) {
            s.sequenceLastValues.erase("public." + sequenceName);
        }
    }
    std::cout << "ALTER TABLE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP DATABASE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateDatabase(const CreateDatabaseStmt* stmt, Session& s) {
    if (!stmt) {
        std::cout << "ERROR: malformed CREATE DATABASE statement (SQLSTATE XX000)"
                  << std::endl;
        return true;
    }
    if (!checkAdmin(s)) return true;
    if (!checkDatabaseCommandOutsideTransaction(s)) return true;
    const std::string& dbname = stmt->databaseName;
    if (dbname.empty()) {
        std::cout << "SQL syntax error: CREATE DATABASE name" << std::endl;
        return true;
    }

    // Parsing every PostgreSQL option is intentional: accepting the syntax
    // and then discarding its semantics creates a dangerously different
    // database.  Until the cluster catalog and template/tablespace machinery
    // exist, reject every option other than this server's fixed UTF-8
    // encoding before StorageEngine can create a directory.
    for (const auto& option : stmt->options) {
        if (option.first == "encoding") continue;
        std::cout << "ERROR: CREATE DATABASE option \"" << option.first
                  << "\" is not supported (SQLSTATE 0A000)" << std::endl;
        return true;
    }

    std::string charset = "utf8";
    auto it = stmt->options.find("encoding");
    if (it != stmt->options.end() && it->second.has_value()) {
        const std::string& rawEncoding = *it->second;
        const bool quotedEncoding = rawEncoding.size() >= 2 &&
            ((rawEncoding.front() == '\'' && rawEncoding.back() == '\'') ||
             (rawEncoding.front() == '"' && rawEncoding.back() == '"'));
        std::string encoding = stripQuotes(rawEncoding);
        std::string normalizedEncoding;
        normalizedEncoding.reserve(encoding.size());
        for (unsigned char c : encoding) {
            // PostgreSQL compares encoding names after discarding all
            // non-alphanumeric characters (so UTF-8, UTF_8, and UTF8 are
            // equivalent).
            if (std::isalnum(c)) {
                normalizedEncoding += static_cast<char>(std::tolower(c));
            }
        }
        int numericEncoding = -1;
        bool numericCode = false;
        if (!quotedEncoding) {
            try {
                size_t consumed = 0;
                const int parsed = std::stoi(encoding, &consumed, 10);
                numericCode = consumed == encoding.size();
                if (numericCode) numericEncoding = parsed;
            } catch (...) {
            }
        }
        if (normalizedEncoding == "utf8" ||
            normalizedEncoding == "unicode" ||
            (numericCode && numericEncoding == 6)) {
            charset = "utf8";
        } else {
            // Normalized names and aliases accepted by PostgreSQL 18 for
            // server encodings.  They are valid requests, but this storage
            // engine cannot honor them, so report feature-not-supported.
            static const std::set<std::string> serverEncodings = {
                "abc", "alt", "euccn", "eucjis2004", "eucjp", "euckr",
                "euctw", "iso88591", "iso885910", "iso885913",
                "iso885914", "iso885915", "iso885916", "iso88592",
                "iso88593", "iso88594", "iso88595", "iso88596",
                "iso88597", "iso88598", "iso88599", "koi8", "koi8r",
                "koi8u", "latin1", "latin10", "latin2", "latin3",
                "latin4", "latin5", "latin6", "latin7", "latin8",
                "latin9", "muleinternal", "sqlascii", "tcvn", "tcvn5712",
                "vscii", "win", "win1250", "win1251", "win1252",
                "win1253", "win1254", "win1255", "win1256", "win1257",
                "win1258", "win866", "win874", "windows1250",
                "windows1251", "windows1252", "windows1253", "windows1254",
                "windows1255", "windows1256", "windows1257", "windows1258",
                "windows866", "windows874"
            };
            if (serverEncodings.count(normalizedEncoding) != 0 ||
                (numericCode && numericEncoding >= 0 && numericEncoding <= 34)) {
                std::cout << "ERROR: database encoding \"" << encoding
                          << "\" is not supported; only UTF8 is available "
                             "(SQLSTATE 0A000)" << std::endl;
            } else {
                std::cout << "ERROR: encoding \"" << encoding
                          << "\" does not exist (SQLSTATE 42704)" << std::endl;
            }
            return true;
        }
    }
    DBStatus res = g_engine.createDatabase(dbname, charset);
    if (res == DBStatus::TABLE_ALREADY_EXISTS || res == DBStatus::ALREADY_EXISTS) {
        std::cout << "ERROR: database \"" << dbname
                  << "\" already exists (SQLSTATE 42P04)" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "ERROR: CREATE DATABASE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    std::cout << "CREATE DATABASE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropDatabase(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDatabaseCommandOutsideTransaction(s)) return true;
    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP DATABASE name" << std::endl;
        return true;
    }
    std::string dbname = stmt->objectNames.front();
    if (dbname == s.currentDB) s.currentDB.clear();

    // Persist and drop the in-memory catalog before removing the directory.
    g_engine.catalogService().evict(dbname);

    DBStatus res = g_engine.dropDatabase(dbname);
    if (res == DBStatus::NOT_FOUND || res == DBStatus::DATABASE_NOT_FOUND) {
        std::cout << "Database not found" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "ERROR: DROP DATABASE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    std::cout << "DROP DATABASE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP SCHEMA
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateRole(const CreateRoleStmt* stmt, Session& s) {
    if (!stmt || stmt->roleName.empty()) {
        std::cout << "SQL syntax error: role name is required" << std::endl;
        return true;
    }
    if (!checkAdmin(s)) return true;
    const std::string roleName = canonicalRoleName(stmt->roleName);
    if (roleName.empty()) {
        std::cout << "SQL syntax error: role name is required" << std::endl;
        return true;
    }
    if (roleExists(roleName)) {
        std::cout << "ERROR: role \"" << roleName << "\" already exists" << std::endl;
        return true;
    }

    dbms::PgAuthIdRow row;
    row.rolname = roleName;
    row.rolsuper = stmt->superuser;
    row.rolinherit = stmt->inherit;
    row.rolcreaterole = stmt->createrole;
    row.rolcreatedb = stmt->createdb;
    row.rolcanlogin = stmt->login;
    row.rolreplication = stmt->replication;
    row.rolbypassrls = stmt->bypassrls;
    row.rolconnlimit = stmt->connectionLimit;
    row.rolpassword = stmt->password.empty()
                          ? std::string()
                          : (stmt->password.rfind("SCRAM-SHA-256$", 0) == 0
                                 ? stmt->password
                                 : dbms::scram::makeRandomVerifier(stmt->password));
    row.rolvaliduntil = stmt->validUntil;
    authCatalog().createAuthId(row);

    for (const auto& membership : stmt->inRole) {
        // IN ROLE means the new role is a member of each existing role.
        const std::string parentRole = canonicalRoleName(membership.first);
        if (grantRoleToUser(parentRole, roleName, false, effectiveSessionRole(s)) != 0) {
            dropRole(roleName);
            std::cout << "ERROR: role \"" << parentRole << "\" does not exist"
                      << std::endl;
            return true;
        }
    }
    for (const auto& membership : stmt->roleMembers) {
        const std::string member = canonicalRoleName(membership.first);
        if (grantRoleToUser(roleName, member, false, effectiveSessionRole(s)) != 0) {
            dropRole(roleName);
            std::cout << "ERROR: role member \"" << member << "\" does not exist"
                      << std::endl;
            return true;
        }
    }
    persistAuthCatalog();
    std::cout << (stmt->isUser ? "CREATE USER" : (stmt->isGroup ? "CREATE GROUP" : "CREATE ROLE"))
              << " succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeAlterRole(const AlterObjectStmt* stmt, Session& s) {
    if (!stmt || stmt->objectName.empty()) {
        std::cout << "SQL syntax error: role name is required" << std::endl;
        return true;
    }
    if (!checkAdmin(s)) return true;

    const std::string roleName = canonicalRoleName(stmt->objectName);
    const auto current = authCatalog().getAuthIdByName(roleName);
    if (!current) {
        if (stmt->ifExists) {
            std::cout << "NOTICE: role \"" << roleName << "\" does not exist, skipping"
                      << std::endl;
            return false;
        }
        std::cout << "ERROR: role \"" << roleName << "\" does not exist" << std::endl;
        return true;
    }

    auto updated = *current;
    const auto tokens = SQLParser::tokenize(stmt->subCommand);
    bool changed = false;
    for (size_t i = 0; i < tokens.size(); ++i) {
        const std::string option = toLower(tokens[i]);
        if (option == "with") continue;
        if (option == "rename" && i + 2 < tokens.size() &&
            toLower(tokens[i + 1]) == "to") {
            const std::string newName = canonicalRoleName(tokens[i + 2]);
            if (newName.empty() || authCatalog().getAuthIdByName(newName)) {
                std::cout << "ERROR: role \"" << newName << "\" already exists" << std::endl;
                return true;
            }
            updated.rolname = newName;
            i += 2;
            changed = true;
            continue;
        }
        if (option == "superuser") updated.rolsuper = true;
        else if (option == "nosuperuser") updated.rolsuper = false;
        else if (option == "createdb") updated.rolcreatedb = true;
        else if (option == "nocreatedb") updated.rolcreatedb = false;
        else if (option == "createrole") updated.rolcreaterole = true;
        else if (option == "nocreaterole") updated.rolcreaterole = false;
        else if (option == "inherit") updated.rolinherit = true;
        else if (option == "noinherit") updated.rolinherit = false;
        else if (option == "login") updated.rolcanlogin = true;
        else if (option == "nologin") updated.rolcanlogin = false;
        else if (option == "replication") updated.rolreplication = true;
        else if (option == "noreplication") updated.rolreplication = false;
        else if (option == "bypassrls") updated.rolbypassrls = true;
        else if (option == "nobypassrls") updated.rolbypassrls = false;
        else if (option == "connection" && i + 2 < tokens.size() &&
                 toLower(tokens[i + 1]) == "limit") {
            try {
                updated.rolconnlimit = std::stoi(tokens[i + 2]);
            } catch (...) {
                std::cout << "ERROR: invalid connection limit" << std::endl;
                return true;
            }
            if (updated.rolconnlimit < -1) {
                std::cout << "ERROR: connection limit must be -1 or greater" << std::endl;
                return true;
            }
            i += 2;
        } else if (option == "password" && i + 1 < tokens.size()) {
            const std::string password = stripQuotes(tokens[++i]);
            updated.rolpassword = toLower(password) == "null"
                                      ? std::string()
                                      : (password.rfind("SCRAM-SHA-256$", 0) == 0
                                             ? password
                                             : dbms::scram::makeRandomVerifier(password));
        } else if (option == "valid" && i + 2 < tokens.size() &&
                   toLower(tokens[i + 1]) == "until") {
            updated.rolvaliduntil = stripQuotes(tokens[i + 2]);
            i += 2;
        } else if (option == "encrypted" || option == "unencrypted") {
            // SCRAM is the only stored password format in this release.
        } else {
            std::cout << "ERROR: unsupported ALTER ROLE option: " << tokens[i] << std::endl;
            return true;
        }
        changed = true;
    }
    if (!changed || !authCatalog().updateAuthId(current->oid, updated)) {
        std::cout << "ERROR: ALTER ROLE failed" << std::endl;
        return true;
    }
    persistAuthCatalog();
    std::cout << "ALTER ROLE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeAlterDefaultPrivileges(const AlterDefaultPrivilegesStmt* stmt,
                                                Session& s) {
    if (!stmt) {
        std::cout << "SQL syntax error: invalid ALTER DEFAULT PRIVILEGES statement" << std::endl;
        return true;
    }
    if (!checkDB(s)) return true;
    if (stmt->privileges.empty() || stmt->grantees.empty() || stmt->objectType.empty()) {
        std::cout << "SQL syntax error: ALTER DEFAULT PRIVILEGES requires privileges, object type, and grantee"
                  << std::endl;
        return true;
    }
    if (stmt->withGrantOption || stmt->grantOptionOnly) {
        std::cout << "ERROR: default privilege grant options are not supported yet" << std::endl;
        return true;
    }

    const std::string actor = effectiveSessionRole(s);
    const std::string owner = stmt->owner.empty()
                                  ? actor
                                  : canonicalRoleName(stmt->owner);
    if (owner.empty()) {
        std::cout << "ERROR: default privilege owner is required" << std::endl;
        return true;
    }
    if (owner != actor) {
        const auto actorAccount = authCatalog().getAuthIdByName(actor);
        if (!sessionIsAdmin(s) && (!actorAccount || !actorAccount->rolcreaterole)) {
            std::cout << "ERROR: permission denied to alter default privileges for role \""
                      << owner << "\"" << std::endl;
            return true;
        }
        if (!authCatalog().getAuthIdByName(owner)) {
            std::cout << "ERROR: role \"" << owner << "\" does not exist" << std::endl;
            return true;
        }
    }

    const std::string schema = stmt->schema.empty()
                                   ? "public"
                                   : canonicalRoleName(stmt->schema);
    if (!g_engine.schemaExists(s.currentDB, schema)) {
        std::cout << "ERROR: schema \"" << schema << "\" does not exist" << std::endl;
        return true;
    }

    std::string objectType = toLower(stripQuotes(trim(stmt->objectType)));
    if (objectType == "tables") objectType = "table";
    if (objectType != "table") {
        std::cout << "ERROR: ALTER DEFAULT PRIVILEGES currently supports TABLE/TABLES only"
                  << std::endl;
        return true;
    }

    using TablePrivilege = StorageEngine::TablePrivilege;
    std::vector<TablePrivilege> privileges;
    for (const auto& rawPrivilege : stmt->privileges) {
        const std::string privilege = toLower(stripQuotes(trim(rawPrivilege)));
        if (privilege == "select") privileges.push_back(TablePrivilege::Select);
        else if (privilege == "insert") privileges.push_back(TablePrivilege::Insert);
        else if (privilege == "update") privileges.push_back(TablePrivilege::Update);
        else if (privilege == "delete") privileges.push_back(TablePrivilege::Delete);
        else if (privilege == "all") privileges.push_back(TablePrivilege::All);
        else {
            std::cout << "ERROR: invalid table privilege \"" << rawPrivilege << "\"" << std::endl;
            return true;
        }
    }

    const auto privilegeName = [](TablePrivilege privilege) {
        switch (privilege) {
            case TablePrivilege::Select: return std::string("select");
            case TablePrivilege::Insert: return std::string("insert");
            case TablePrivilege::Update: return std::string("update");
            case TablePrivilege::Delete: return std::string("delete");
            case TablePrivilege::All: return std::string("all");
            default: return std::string();
        }
    };

    for (const auto privilege : privileges) {
        for (const auto& rawGrantee : stmt->grantees) {
            const std::string grantee = canonicalRoleName(rawGrantee);
            if (grantee.empty()) {
                std::cout << "ERROR: grantee name is required" << std::endl;
                return true;
            }
            if (stmt->revoke) {
                const std::vector<std::string> names =
                    privilege == TablePrivilege::All
                        ? std::vector<std::string>{"select", "insert", "update", "delete", "all"}
                        : std::vector<std::string>{privilegeName(privilege)};
                for (const auto& name : names) {
                    g_engine.removeDefaultPrivilege(s.currentDB, owner, schema, objectType,
                                                    name, grantee);
                }
            } else {
                g_engine.addDefaultPrivilege(s.currentDB, owner, schema, objectType,
                                             privilegeName(privilege), grantee);
            }
        }
    }

    std::cout << "ALTER DEFAULT PRIVILEGES " << (stmt->revoke ? "revoked" : "granted")
              << " for role " << owner << " in schema " << schema << std::endl;
    return false;
}

bool DdlExecutor::executeTruncate(const TruncateStmt* stmt, Session& s) {
    if (!stmt || stmt->tableNames.empty()) {
        std::cout << "SQL syntax error: TRUNCATE requires at least one table" << std::endl;
        return true;
    }
    if (!checkDB(s)) return true;

    std::set<std::string> targets;
    const auto addTargetWithChildren = [&](const std::string& rawName) {
        std::vector<std::string> pending{rawName};
        while (!pending.empty()) {
            const std::string name = pending.back();
            pending.pop_back();
            if (!targets.insert(name).second || stmt->only) continue;
            for (const auto& child : g_engine.getInheritedChildren(s.currentDB, name)) {
                pending.push_back(child);
            }
        }
    };

    for (const auto& rawName : stmt->tableNames) {
        const std::string name = resolveTableName(s, rawName);
        if (!g_engine.tableExists(s.currentDB, name)) {
            std::cout << "Table " << name << " does not exist" << std::endl;
            return true;
        }
        addTargetWithChildren(name);
    }

    // Expand FK dependants before mutating any table.  This makes RESTRICT
    // statement-atomic and makes CASCADE recursive instead of only clearing
    // the first level of references.
    bool expanded = true;
    while (expanded) {
        expanded = false;
        for (const auto& candidate : g_engine.getTableNames(s.currentDB)) {
            if (targets.count(candidate)) continue;
            const TableSchema table = g_engine.getTableSchema(s.currentDB, candidate);
            bool dependsOnTarget = false;
            for (size_t i = 0; i < table.fkLen; ++i) {
                if (targets.count(table.fks[i].refTable)) {
                    dependsOnTarget = true;
                    break;
                }
            }
            if (!dependsOnTarget) continue;
            if (!stmt->cascade) {
                std::cout << "cannot truncate table because table \"" << candidate
                          << "\" references it; use CASCADE" << std::endl;
                return true;
            }
            addTargetWithChildren(candidate);
            expanded = true;
        }
    }

    for (const auto& name : targets) {
        if (!g_engine.tableExists(s.currentDB, name)) {
            std::cout << "TRUNCATE found missing inherited table \"" << name << "\""
                      << std::endl;
            return true;
        }
    }

    const std::string actor = effectiveSessionRole(s);
    for (const auto& name : targets) {
        if (sessionIsAdmin(s)) continue;
        const TableSchema table = g_engine.getTableSchema(s.currentDB, name);
        if (table.owner != actor) {
            std::cout << "permission denied to truncate table \"" << name << "\"" << std::endl;
            return true;
        }
    }

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }
    for (const auto& name : targets) {
        // TRUNCATE rewrites the heap and all RID-bearing sidecars. Mark the
        // snapshot before the first physical change so a failure in any
        // target restores every earlier target in this statement.
        txn.markSnapshotDirty();
        const DBStatus result = g_engine.truncateTable(s.currentDB, name);
        if (result != DBStatus::OK) {
            txn.rollback();
            std::cout << "TRUNCATE failed for table " << name << std::endl;
            return true;
        }
        if (stmt->restartIdentity) {
            const TableSchema table = g_engine.getTableSchema(s.currentDB, name);
            for (size_t i = 0; i < table.len; ++i) {
                if (table.cols[i].isAutoIncrement) {
                    g_engine.resetSequence(s.currentDB, name, table.cols[i].dataName, 1);
                }
            }
        }
        g_engine.bufferLogicalTruncate(s.currentDB, name);
    }
    if (!txn.commit()) {
        std::cout << "TRUNCATE commit failed" << std::endl;
        return true;
    }

    std::cout << "TRUNCATE TABLE completed (" << targets.size() << " table(s))" << std::endl;
    log(s.username, "truncate table", getTime());
    return false;
}

bool DdlExecutor::executeDropRole(const DropStmt* stmt, Session& s) {
    if (!stmt || stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: role name is required" << std::endl;
        return true;
    }
    if (!checkAdmin(s)) return true;
    for (const auto& rawName : stmt->objectNames) {
        const std::string roleName = canonicalRoleName(rawName);
        if (!roleExists(roleName)) {
            if (stmt->ifExists) {
                std::cout << "NOTICE: role \"" << roleName << "\" does not exist, skipping"
                          << std::endl;
                continue;
            }
            std::cout << "ERROR: role \"" << roleName << "\" does not exist" << std::endl;
            return true;
        }
        if (!dropRole(roleName)) {
            std::cout << "ERROR: could not drop role \"" << roleName << "\"" << std::endl;
            return true;
        }
    }
    std::cout << "DROP ROLE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeCreateSchema(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string name = stmt->objectName;
    if (name.empty()) {
        std::cout << "SQL syntax error: CREATE SCHEMA name" << std::endl;
        return true;
    }
    DBStatus res = g_engine.createSchema(s.currentDB, name);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        if (stmt->ifNotExists) {
            std::cout << "NOTICE: schema \"" << name << "\" already exists, skipping"
                      << std::endl;
            return false;
        }
        std::cout << "ERROR: schema \"" << name
                  << "\" already exists (SQLSTATE 42P06)" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "CREATE SCHEMA failed" << std::endl;
        return true;
    }

    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        cat.createNamespace(name, INVALID_OID);
        if (!cat.persistAll()) {
            std::cout << "CREATE SCHEMA catalog persistence failed" << std::endl;
            return true;
        }
    } catch (const std::exception& e) {
        std::cerr << "CREATE SCHEMA catalog registration failed: " << e.what() << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Schema, name);
    if (!txn.commit()) return true;
    std::cout << "CREATE SCHEMA succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropSchema(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP SCHEMA name" << std::endl;
        return true;
    }
    if (stmt->objectNames.size() != 1) {
        std::cout << "DROP SCHEMA with multiple targets is not supported"
                  << std::endl;
        return true;
    }
    std::string name = stmt->objectNames.front();
    const bool storageSchemaExists =
        g_engine.schemaExists(s.currentDB, name);

    // Several object families still use storage sidecars instead of pg_type
    // or pg_proc rows. They nevertheless belong to their logical namespace
    // and must participate in the same RESTRICT/CASCADE decision.
    enum class AuxiliarySchemaObjectKind {
        Domain,
        CompositeType,
        EnumType,
        ShellType,
        UdtType,
        Function,
        TableFunction,
        Procedure,
        Collation
    };
    struct AuxiliarySchemaObject {
        AuxiliarySchemaObjectKind kind;
        std::string name;
    };
    std::vector<AuxiliarySchemaObject> auxiliaryObjects;
    const std::string logicalPrefix = name + ".";
    const std::string physicalPrefix = name + "__";
    const auto hasLogicalNamespace = [&](const std::string& objectName) {
        return objectName.size() > logicalPrefix.size() &&
               objectName.rfind(logicalPrefix, 0) == 0;
    };
    const auto addLogicalObjects = [&](const std::vector<std::string>& names,
                                       AuxiliarySchemaObjectKind kind) {
        for (const auto& objectName : names) {
            if (hasLogicalNamespace(objectName)) {
                auxiliaryObjects.push_back({kind, objectName});
            }
        }
    };
    try {
        std::vector<std::string> shellTypes;
        DBStatus auxiliaryStatus = loadShellTypes(s.currentDB, shellTypes);
        if (auxiliaryStatus != DBStatus::OK) {
            std::cout << "DROP SCHEMA shell-type preflight failed (SQLSTATE "
                      << sqlstateForDBStatus(auxiliaryStatus) << ")"
                      << std::endl;
            return true;
        }
        std::vector<UdtMeta> udtTypes;
        auxiliaryStatus = loadUdtMeta(s.currentDB, udtTypes);
        if (auxiliaryStatus != DBStatus::OK) {
            std::cout << "DROP SCHEMA UDT preflight failed (SQLSTATE "
                      << sqlstateForDBStatus(auxiliaryStatus) << ")"
                      << std::endl;
            return true;
        }
        addLogicalObjects(g_engine.getDomainNames(s.currentDB),
                          AuxiliarySchemaObjectKind::Domain);
        addLogicalObjects(g_engine.getCompositeTypeNames(s.currentDB),
                          AuxiliarySchemaObjectKind::CompositeType);
        addLogicalObjects(g_engine.getEnumTypeNames(s.currentDB),
                          AuxiliarySchemaObjectKind::EnumType);
        addLogicalObjects(shellTypes,
                          AuxiliarySchemaObjectKind::ShellType);
        for (const auto& meta : udtTypes) {
            if (hasLogicalNamespace(meta.name)) {
                auxiliaryObjects.push_back(
                    {AuxiliarySchemaObjectKind::UdtType, meta.name});
            }
        }
        addLogicalObjects(g_engine.getUDFNames(s.currentDB),
                          AuxiliarySchemaObjectKind::Function);
        addLogicalObjects(g_engine.getTVFNames(s.currentDB),
                          AuxiliarySchemaObjectKind::TableFunction);
        addLogicalObjects(g_engine.getProcedureNames(s.currentDB),
                          AuxiliarySchemaObjectKind::Procedure);
        for (const auto& collation :
             g_engine.getCollationNames(s.currentDB)) {
            if (hasLogicalNamespace(collation) ||
                (collation.size() > physicalPrefix.size() &&
                 collation.rfind(physicalPrefix, 0) == 0)) {
                auxiliaryObjects.push_back(
                    {AuxiliarySchemaObjectKind::Collation, collation});
            }
        }
    } catch (const std::exception& error) {
        std::cout << "DROP SCHEMA auxiliary-object preflight failed: "
                  << error.what() << std::endl;
        return true;
    }
    if (!stmt->cascade && !auxiliaryObjects.empty()) {
        std::cout << "ERROR: cannot drop schema " << name
                  << " because object " << auxiliaryObjects.front().name
                  << " depends on it" << std::endl;
        return true;
    }

    // Validate dependencies without mutating the catalog.  The physical
    // schema removal must win the race with catalog publication, otherwise a
    // failed filesystem operation can leave a catalog namespace that no
    // longer matches storage (or vice versa).
    CatalogManager* catalogManager = nullptr;
    CatalogManager::DropPlan catalogDropPlan;
    bool hasCatalogDropPlan = false;
    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        catalogManager = &cat;
        const auto* ns = cat.findNamespaceByName(name);
        if (!ns) {
            if (!storageSchemaExists && auxiliaryObjects.empty()) {
                if (stmt->ifExists) {
                    if (!txn.commit()) return true;
                    std::cout << "NOTICE: schema \"" << name
                              << "\" does not exist, skipping" << std::endl;
                    return false;
                }
                std::cout << "ERROR: schema \"" << name
                          << "\" does not exist (SQLSTATE 3F000)"
                          << std::endl;
            } else {
                std::cout << "DROP SCHEMA refused: storage objects for "
                          << name << " have no catalog namespace" << std::endl;
            }
            return true;
        }
        if (!storageSchemaExists) {
            std::cout << "DROP SCHEMA refused: catalog namespace " << name
                      << " has no storage marker" << std::endl;
            return true;
        }
        const auto behavior = stmt->cascade
            ? CatalogManager::DropBehavior::Cascade
            : CatalogManager::DropBehavior::Restrict;
        catalogDropPlan = cat.planDrop(PgClassOid_Namespace, ns->oid, behavior);
        if (!catalogDropPlan.ok()) {
            std::cout << "ERROR: " << catalogDropPlan.error << std::endl;
            return true;
        }
        hasCatalogDropPlan = true;
    } catch (const std::exception& e) {
        std::cerr << "DROP SCHEMA catalog preflight failed: " << e.what()
                  << std::endl;
        return true;
    }

    // From this point on physical deletion may be partial (CASCADE can
    // remove several relations), so every failure must be able to restore
    // the pre-statement snapshot.
    txn.markSnapshotDirty();
    std::set<std::string> droppedSequenceStorageNames;
    if (hasCatalogDropPlan && catalogManager) {
        std::string error;
        if (!executeSchemaPhysicalDropPlan(
                *catalogManager, s.currentDB, catalogDropPlan,
                stmt->cascade, droppedSequenceStorageNames, error)) {
            std::cout << "DROP SCHEMA physical cleanup failed: "
                      << error << std::endl;
            return true;
        }
    }
    for (const auto& object : auxiliaryObjects) {
        DBStatus status = DBStatus::OK;
        DdlObjectKind ddlKind = DdlObjectKind::Type;
        switch (object.kind) {
            case AuxiliarySchemaObjectKind::Domain:
                status = g_engine.dropDomain(s.currentDB, object.name);
                ddlKind = DdlObjectKind::Domain;
                break;
            case AuxiliarySchemaObjectKind::CompositeType:
                status = g_engine.dropCompositeType(
                    s.currentDB, object.name);
                break;
            case AuxiliarySchemaObjectKind::EnumType:
                status = g_engine.dropEnumType(s.currentDB, object.name);
                break;
            case AuxiliarySchemaObjectKind::ShellType:
                status = removeShellType(s.currentDB, object.name);
                break;
            case AuxiliarySchemaObjectKind::UdtType:
                status = removeUdtMeta(s.currentDB, object.name);
                break;
            case AuxiliarySchemaObjectKind::Function:
                status = g_engine.dropUDF(s.currentDB, object.name);
                ddlKind = DdlObjectKind::Function;
                break;
            case AuxiliarySchemaObjectKind::TableFunction:
                status = g_engine.dropTVF(s.currentDB, object.name);
                ddlKind = DdlObjectKind::Function;
                break;
            case AuxiliarySchemaObjectKind::Procedure:
                status = g_engine.dropProcedure(s.currentDB, object.name);
                ddlKind = DdlObjectKind::Procedure;
                break;
            case AuxiliarySchemaObjectKind::Collation:
                status = g_engine.dropCollation(s.currentDB, object.name);
                ddlKind = DdlObjectKind::Collation;
                break;
        }
        if (status != DBStatus::OK) {
            std::cout << "DROP SCHEMA auxiliary cleanup failed for "
                      << object.name << std::endl;
            return true;
        }
        txn.recordDrop(ddlKind, object.name);
    }
    // Catalog-planned relation removal above is dependency ordered and also
    // includes objects outside this namespace reached by CASCADE. The storage
    // primitive now only removes the namespace marker; asking it to cascade
    // again would double-drop tables while still missing other relation kinds.
    DBStatus res = g_engine.dropSchema(s.currentDB, name, false);
    if (res != DBStatus::OK) {
        std::cout << "DROP SCHEMA failed" << std::endl;
        return true;
    }

    // DROP SCHEMA can remove multiple relations and their side files.  If
    // catalog post-processing fails, restore the transaction snapshot rather
    // than exposing a partially dropped namespace.
    if (hasCatalogDropPlan && catalogManager) {
        std::string error;
        if (!catalogManager->applyDropPlan(catalogDropPlan, &error)) {
            std::cout << "DROP SCHEMA catalog cleanup failed: " << error << std::endl;
            return true;
        }
        if (!catalogManager->persistAll()) {
            std::cout << "DROP SCHEMA catalog persistence failed" << std::endl;
            return true;
        }
    }
    txn.recordDrop(DdlObjectKind::Schema, name);
    for (const auto& sequenceName : droppedSequenceStorageNames) {
        txn.recordDrop(DdlObjectKind::Sequence, sequenceName);
    }
    if (!txn.commit()) return true;
    for (const auto& sequenceName : droppedSequenceStorageNames) {
        s.sequenceLastValues.erase(sequenceName);
        if (sequenceName.find('.') == std::string::npos) {
            s.sequenceLastValues.erase("public." + sequenceName);
        }
    }
    std::cout << "DROP SCHEMA succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP TABLE
// ----------------------------------------------------------------------------

bool DdlExecutor::columnDefToColumn(const ColumnDef& cd, const std::string& dbname,
                                    Column& col, std::string& error,
                                    const std::string& compatibilityMode) {
    error.clear();
    if (cd.name.empty() || cd.typeName.empty()) {
        error = "column name and type are required";
        return false;
    }
    col = Column{};
    col.dataName = cd.name;
    col.isNull = cd.isNull;
    col.isPrimaryKey = cd.isPrimaryKey;
    col.isUnique = cd.isUnique;
    col.isArray = cd.isArray;
    col.defaultValue = cd.defaultValue ? cd.defaultValue->toString() : "";

    col.isAutoIncrement = cd.isGeneratedIdentity;
    col.identityKind = cd.identityKind;
    col.generatedExpr = cd.generatedExpr;
    col.generatedKind = cd.generatedKind;

    std::string baseType = toLower(cd.typeName);
    if (cd.hasIdentityOptions) {
        error = "identity sequence options are not supported (SQLSTATE 0A000)";
        return false;
    }
    if (cd.isGeneratedIdentity && cd.defaultValue) {
        error = "identity column cannot also have a DEFAULT (SQLSTATE 42601)";
        return false;
    }
    const bool extended = compatibilityMode == "extended";
    if ((cd.isAutoIncrementExtension || cd.isUnsignedExtension) &&
        !extended) {
        error = cd.isAutoIncrementExtension
            ? "AUTO_INCREMENT is not PostgreSQL syntax; use GENERATED AS "
              "IDENTITY (SQLSTATE 42601)"
            : "UNSIGNED is not PostgreSQL syntax; use a CHECK constraint "
              "(SQLSTATE 42601)";
        return false;
    }
    const bool extensionIntegerType =
        baseType == "tinyint" || baseType == "smallint" ||
        baseType == "int2" || baseType == "int4" ||
        baseType == "int8" ||
        baseType == "int" || baseType == "integer" ||
        baseType == "bigint" || baseType == "long";
    if ((cd.isAutoIncrementExtension || cd.isUnsignedExtension) &&
        !extensionIntegerType) {
        error = "AUTO_INCREMENT and UNSIGNED require an integer column "
                "(SQLSTATE 42804)";
        return false;
    }
    std::string domainName;
    std::string domainCheck;
    std::vector<std::string> domainTypeMods;
    std::string enumTypeName;
    std::vector<std::string> enumValues;
    if (!dbname.empty()) {
        auto dom = g_engine.getDomain(dbname, baseType);
        if (!dom.name.empty()) {
            domainName = baseType;
            domainCheck = dom.checkExpr;
            if (col.defaultValue.empty() && !dom.defaultValue.empty()) {
                col.defaultValue = dom.defaultValue;
            }
            // Domain base types may carry modifiers (for example VARCHAR(50)).
            // Normalize that specification before applying the normal typed
            // column mapping; otherwise a valid domain would be mistaken for
            // an unknown type.
            ColumnDef domainType = columnDefFromAlterType("", dom.baseType);
            baseType = toLower(domainType.typeName);
            domainTypeMods = std::move(domainType.typeMods);
        }
        auto et = g_engine.getEnumType(dbname, baseType);
        if (!et.name.empty()) {
            enumTypeName = baseType;
            enumValues = et.labels;
            baseType = "varchar"; // store enum values as strings
        }
    }
    // Normalize common aliases
    // DIV-06: MySQL/SQL Server aliases.  postgresql18 mode rejects them
    // with the canonical type in the error; extended mode keeps the legacy
    // silent mapping.
    {
        const bool alias = baseType == "tinyint" || baseType == "datetime" ||
                           baseType == "nvarchar" || baseType == "nchar" ||
                           baseType == "blob" || baseType == "binary" ||
                           baseType == "varbinary" || baseType == "double" ||
                           baseType == "long";
        if (alias && compatibilityMode != "extended") {
            std::string canonical;
            if (baseType == "tinyint" || baseType == "long") canonical = (baseType == "long") ? "bigint" : "smallint";
            else if (baseType == "datetime") canonical = "timestamp";
            else if (baseType == "nvarchar") canonical = "varchar";
            else if (baseType == "nchar") canonical = "char";
            else if (baseType == "double") canonical = "double precision";
            else canonical = "bytea";
            error = "type " + baseType + " does not exist; use " + canonical +
                    " (SQLSTATE 42704)";
            return false;
        }
        if (alias) {
            std::cout << "NOTICE: type " << baseType << " mapped to "
                      << (baseType == "tinyint" ? "smallint"
                          : baseType == "datetime" ? "timestamp"
                          : baseType == "nvarchar" ? "varchar"
                          : baseType == "nchar" ? "char"
                          : baseType == "double" ? "double precision"
                          : baseType == "long" ? "bigint" : "bytea")
                      << std::endl;
        }
    }
    if (baseType == "int" || baseType == "integer") baseType = "int4";
    else if (baseType == "bigint") baseType = "int8";
    else if (baseType == "long") baseType = "int8";
    else if (baseType == "smallint") baseType = "int2";
    else if (baseType == "tinyint") baseType = "smallint";
    else if (baseType == "real") baseType = "float4";
    else if (baseType == "double" || baseType == "double precision") baseType = "float8";
    else if (baseType == "varchar" || baseType == "character varying" || baseType == "nvarchar") baseType = "varchar";
    else if (baseType == "char" || baseType == "character" || baseType == "nchar") baseType = "char";
    else if (baseType == "bool") baseType = "boolean";
    else if (baseType == "datetime") baseType = "timestamp";

    const auto& typeMods = cd.typeMods.empty() ? domainTypeMods : cd.typeMods;
    int typeMod1 = 0, typeMod2 = 0;
    if (!typeMods.empty()) {
        try {
            size_t consumed = 0;
            typeMod1 = std::stoi(typeMods[0], &consumed);
            if (consumed != typeMods[0].size()) throw std::invalid_argument("trailing type modifier");
        } catch (...) {
            error = "invalid type modifier '" + typeMods[0] + "'";
            return false;
        }
    }
    if (typeMods.size() > 1) {
        try {
            size_t consumed = 0;
            typeMod2 = std::stoi(typeMods[1], &consumed);
            if (consumed != typeMods[1].size()) throw std::invalid_argument("trailing type modifier");
        } catch (...) {
            error = "invalid type modifier '" + typeMods[1] + "'";
            return false;
        }
    }
    if ((baseType == "char" || baseType == "varchar") &&
        !typeMods.empty()) {
        const TypeModResult checked =
            TypeRegistry::instance().applyTypeMods(baseType, typeMods);
        if (!checked.ok()) {
            error = checked.error + " (SQLSTATE 22023)";
            return false;
        }
    }
    if (baseType == "bit" || baseType == "bit varying" ||
        baseType == "varbit") {
        constexpr int kMaxSupportedBitLength = 8388608;
        if (typeMods.size() > 1) {
            error = "invalid type modifier for " + baseType +
                    " (SQLSTATE 42601)";
            return false;
        }
        if (!typeMods.empty() &&
            (typeMod1 <= 0 || typeMod1 > kMaxSupportedBitLength)) {
            error = "length for type " + baseType +
                    " must be between 1 and " +
                    std::to_string(kMaxSupportedBitLength) +
                    " (SQLSTATE 22023)";
            return false;
        }
    }

    bool knownType = true;
    if (baseType == "smallserial") {
        col = makeIntColumn(cd.name, cd.isNull, 0, cd.isPrimaryKey);
        col.isAutoIncrement = true;
    } else if (baseType == "serial") {
        col = makeIntColumn(cd.name, cd.isNull, 2, cd.isPrimaryKey);
        col.isAutoIncrement = true;
    } else if (baseType == "bigserial") {
        col = makeIntColumn(cd.name, cd.isNull, 3, cd.isPrimaryKey);
        col.isAutoIncrement = true;
    } else if (baseType == "int2" || baseType == "smallint") {
        col = makeIntColumn(cd.name, cd.isNull, 0, cd.isPrimaryKey,
                            cd.isUnsignedExtension);
    } else if (baseType == "int4" || baseType == "integer" || baseType == "int") {
        col = makeIntColumn(cd.name, cd.isNull, 2, cd.isPrimaryKey,
                            cd.isUnsignedExtension);
    } else if (baseType == "int8" || baseType == "bigint") {
        col = makeIntColumn(cd.name, cd.isNull, 3, cd.isPrimaryKey,
                            cd.isUnsignedExtension);
    } else if (baseType == "varchar" || baseType == "character varying") {
        // The SQL type is unbounded when no length is supplied.  This is
        // still capped by our current physical varchar storage capacity.
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 65535;
        col = makeVarCharColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
    } else if (baseType == "char" || baseType == "character") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 1;
        col = makeStringColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
        // SQL bpchar(n) counts characters; a fixed n-byte heap slot cannot
        // hold n multibyte UTF-8 characters.  Keep the declared character
        // width in dsize and use a varlena physical slot for new SQL columns.
        col.isVariableLength = true;
    } else if (baseType == "text") {
        col = makeTextColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "boolean" || baseType == "bool") {
        col = makeBooleanColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "float4" || baseType == "real") {
        col = makeFloatColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "float8" || baseType == "double precision") {
        col = makeDoubleColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "numeric" || baseType == "decimal") {
        col = makeDecimalColumn(cd.name, cd.isNull, typeMod1 > 0 ? typeMod1 : 18,
                                typeMod2 > 0 ? typeMod2 : 2, cd.isPrimaryKey);
    } else if (baseType == "money") {
        col = makeMoneyColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "date") {
        col = makeDateColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "timestamp" || baseType == "datetime") {
        col = makeTimestampColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "timestamptz") {
        col = makeTimestamptzColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "time") {
        col = makeTimeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "timetz") {
        col = makeTimeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
        col.dataType = "timetz";
    } else if (baseType == "interval") {
        col = makeIntervalColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "json") {
        col = makeJsonColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "jsonb") {
        col = makeJsonbColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "xml") {
        col = makeXmlColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "uuid") {
        col = makeUuidColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "bytea" || baseType == "blob") {
        col = makeBlobColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "binary") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 1;
        col = makeBinaryColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
    } else if (baseType == "varbinary") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 255;
        col = makeVarBinaryColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
    } else if (baseType == "pg_lsn") {
        col = makePgLsnColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "point") {
        col = makePointColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "inet") {
        col = makeINetColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "cidr") {
        col = makeCidrColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "int4range") {
        col = makeInt4RangeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "int8range") {
        col = makeInt8RangeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "numrange") {
        col = makeNumRangeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "tsrange") {
        col = makeTsRangeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "tstzrange") {
        col = makeTstzRangeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "daterange") {
        col = makeDateRangeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "tsvector") {
        col = makeTsVectorColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "tsquery") {
        col = makeTsQueryColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "line") {
        col = makeLineColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "lseg") {
        col = makeLsegColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "box") {
        col = makeBoxColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "path") {
        col = makePathColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "polygon") {
        col = makePolygonColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "circle") {
        col = makeCircleColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "macaddr") {
        col = makeMacAddrColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "macaddr8") {
        col = makeMacAddr8Column(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "bit") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 1;
        col = makeBitColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
    } else if (baseType == "bit varying" || baseType == "varbit") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 0;
        col = makeVarBitColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
    } else if (baseType == "jsonpath") {
        col = makeJsonPathColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (!dbname.empty() && g_engine.isCompositeType(dbname, baseType)) {
        // Column of a composite type: store the row literal as text and keep the
        // composite type name as dataType so INSERT/UPDATE can validate fields.
        col = makeVarCharColumn(cd.name, cd.isNull, 1024, cd.isPrimaryKey);
        col.dataType = baseType;
    } else {
        knownType = false;
    }

    if (!knownType) {
        error = "unknown type '" + cd.typeName + "'";
        return false;
    }

    // Factory functions replace the whole Column; restore metadata they don't set.
    col.defaultValue = cd.defaultValue ? cd.defaultValue->toString() : "";
    col.generatedExpr = cd.generatedExpr;
    col.generatedKind = cd.generatedKind;
    col.isAutoIncrement = col.isAutoIncrement || cd.isGeneratedIdentity ||
                          cd.isAutoIncrementExtension;
    col.identityKind = cd.identityKind;
    if (cd.isGeneratedIdentity) {
        const bool integerIdentity = baseType == "int2" ||
                                     baseType == "int4" ||
                                     baseType == "int8";
        if (!integerIdentity || cd.isArray || !domainName.empty()) {
            error = "identity column type must be smallint, integer, or bigint "
                    "(SQLSTATE 22023)";
            return false;
        }
        col.isNull = false;
    }
    col.isUnique = cd.isUnique;
    col.isArray = cd.isArray;
    col.enumValues = enumValues;
    // Keep the declared enum type as the column's persisted type identity.
    // The varchar factory above still supplies the variable-length physical
    // layout; replacing dataType with "varchar" made two independent enum
    // types with the same labels indistinguishable after CREATE TABLE.
    if (!enumTypeName.empty()) col.dataType = enumTypeName;
    if (!domainName.empty()) {
        col.domainName = domainName;
        // Re-apply domain default if column has no explicit default.
        if (col.defaultValue.empty()) {
            auto dom = g_engine.getDomain(dbname, domainName);
            if (!dom.defaultValue.empty()) col.defaultValue = dom.defaultValue;
        }
        // Merge domain check with column check. PG domain checks use VALUE pseudo-variable.
        if (!domainCheck.empty()) {
            std::string rewritten = domainCheck;
            // Replace case-insensitive VALUE with the actual column name.
            for (size_t i = 0; i + 5 <= rewritten.size(); ) {
                bool isValue = true;
                for (int j = 0; j < 5; ++j) {
                    if (std::tolower(static_cast<unsigned char>(rewritten[i + j])) != "value"[j]) {
                        isValue = false; break;
                    }
                }
                if (isValue) {
                    rewritten.replace(i, 5, cd.name);
                    i += cd.name.size();
                } else {
                    ++i;
                }
            }
            if (!col.checkExpr.empty()) col.checkExpr = "(" + col.checkExpr + ") AND (" + rewritten + ")";
            else col.checkExpr = rewritten;
        }
    }

    // Apply check constraints from column definition
    if (!cd.checkExprs.empty()) {
        col.checkExpr = cd.checkExprs.front()->toString();
    }
    if (!cd.checkNames.empty()) {
        col.checkConstraintName = cd.checkNames.front();
    }
    // COLLATE determined after the storage type is assigned above.
    if (!cd.collation.empty()) col.collation = cd.collation;
    return true;
}

ForeignKey DdlExecutor::tableConstraintToForeignKey(const TableConstraint& tc) {
    ForeignKey fk;
    fk.name = tc.name;
    fk.colNames = tc.columns;
    fk.refTable = tc.refTable;
    fk.refCols = tc.refColumns;
    fk.onDelete = tc.onDelete.empty() ? "restrict" : toLower(tc.onDelete);
    fk.onUpdate = tc.onUpdate.empty() ? "restrict" : toLower(tc.onUpdate);
    return fk;
}

// ----------------------------------------------------------------------------
// CREATE TABLE AS SELECT helper
// ----------------------------------------------------------------------------
static std::vector<std::string> splitSelectColumns(const std::string& cols) {
    std::vector<std::string> result;
    std::string item;
    int depth = 0;
    for (char c : cols) {
        if (c == '(') ++depth;
        else if (c == ')') --depth;
        if (c == ',' && depth == 0) {
            result.push_back(toLower(trim(item)));
            item.clear();
        } else {
            item += c;
        }
    }
    if (!trim(item).empty()) result.push_back(toLower(trim(item)));
    return result;
}

static bool parseSimpleSelect(const std::string& selectSql,
                              std::vector<std::string>& colNames,
                              std::string& srcTable,
                              std::vector<std::string>& conditions) {
    std::string sql = toLower(selectSql);
    size_t selectPos = sql.find("select");
    if (selectPos == std::string::npos) return false;
    size_t fromPos = sql.find(" from ");
    if (fromPos == std::string::npos) return false;

    std::string cols = trim(selectSql.substr(selectPos + 6, fromPos - selectPos - 6));
    if (cols == "*") {
        colNames.clear();
        colNames.push_back("*");
    } else {
        colNames = splitSelectColumns(cols);
    }

    std::string rest = trim(selectSql.substr(fromPos + 6));
    size_t wherePos = rest.find(' ');
    if (wherePos == std::string::npos) {
        srcTable = rest;
    } else {
        srcTable = rest.substr(0, wherePos);
        std::string afterTable = trim(rest.substr(wherePos));
        if (afterTable.size() > 6 && toLower(afterTable.substr(0, 6)) == "where ") {
            std::string condStr = trim(afterTable.substr(6));
            // Simple AND-split equality conditions: col = val
            size_t andPos = 0;
            while (andPos < condStr.size()) {
                size_t nextAnd = condStr.find(" AND ", andPos);
                std::string single = (nextAnd == std::string::npos)
                    ? trim(condStr.substr(andPos))
                    : trim(condStr.substr(andPos, nextAnd - andPos));
                if (!single.empty()) {
                    // Simple AND-split conditions: col op val (op = < > <= >= <> =)
                    size_t opPos = std::string::npos;
                    std::string op;
                    for (size_t i = 0; i + 1 < single.size(); ++i) {
                        char c = single[i];
                        if (c == '=' || c == '<' || c == '>' || c == '!') {
                            opPos = i;
                            if (i + 1 < single.size() &&
                                ((c == '<' && single[i + 1] == '>') ||
                                 (c == '<' && single[i + 1] == '=') ||
                                 (c == '>' && single[i + 1] == '='))) {
                                op = single.substr(i, 2);
                            } else {
                                op = std::string(1, c);
                            }
                            break;
                        }
                    }
                    if (!op.empty()) {
                        std::string cname = trim(single.substr(0, opPos));
                        std::string val = trim(single.substr(opPos + op.size()));
                        conditions.push_back(op + cname + " " + val);
                    }
                }
                if (nextAnd == std::string::npos) break;
                andPos = nextAnd + 5;
            }
        }
    }
    return true;
}

static dbms::Column makeColumnFromSource(const dbms::Column& src, const std::string& name) {
    dbms::Column col = src;
    col.dataName = name;
    // CTAS derives names and data types from query output; it does not clone
    // the source relation's column constraints or value-generation behavior.
    col.isNull = true;
    col.isPrimaryKey = false;
    col.isUnique = false;
    col.isAutoIncrement = false;
    col.defaultValue.clear();
    col.checkExpr.clear();
    col.checkConstraintName.clear();
    col.deferrable = false;
    col.initiallyDeferred = false;
    col.generatedExpr.clear();
    col.generatedKind = 0;
    return col;
}

static bool executeCreateTableAs(const CreateTableStmt* stmt, Session& s,
                                 const std::string& tname,
                                 DdlTransaction& transaction) {
    if (stmt->asSelect.empty()) return false; // not CTAS

    std::vector<std::string> selectCols;
    std::string srcTable;
    std::vector<std::string> conditions;
    if (!parseSimpleSelect(stmt->asSelect, selectCols, srcTable, conditions)) {
        std::cout << "CTAS: unable to parse SELECT clause" << std::endl;
        return true;
    }

    srcTable = resolveTableName(s, srcTable);
    if (!g_engine.tableExists(s.currentDB, srcTable)) {
        std::cout << "CTAS: source table not found" << std::endl;
        return true;
    }

    dbms::TableSchema srcTbl = g_engine.getTableSchema(s.currentDB, srcTable);
    dbms::TableSchema newTbl;
    newTbl.tablename = tname;
    newTbl.owner = effectiveSessionRole(s);
    newTbl.isTemporary = stmt->temp || stmt->localTemp;

    std::vector<size_t> selectedSourceColumns;
    if (selectCols.size() == 1 && selectCols[0] == "*") {
        for (size_t i = 0; i < srcTbl.len; ++i) {
            newTbl.append(makeColumnFromSource(srcTbl.cols[i], srcTbl.cols[i].dataName));
            selectedSourceColumns.push_back(i);
        }
    } else {
        for (const auto& cname : selectCols) {
            bool found = false;
            for (size_t i = 0; i < srcTbl.len; ++i) {
                if (toLower(srcTbl.cols[i].dataName) == cname) {
                    newTbl.append(makeColumnFromSource(srcTbl.cols[i], srcTbl.cols[i].dataName));
                    selectedSourceColumns.push_back(i);
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cout << "CTAS: column '" << cname << "' not found in source" << std::endl;
                return true;
            }
        }
    }

    transaction.markSnapshotDirty();
    DBStatus res = g_engine.createTable(s.currentDB, newTbl);
    if (res != DBStatus::OK) {
        std::cout << "CTAS: create table failed" << std::endl;
        return true;
    }
    // Record ownership as soon as the physical relation exists.  A source
    // scan or target INSERT failure below must remove the partially-built
    // table through the enclosing DDL transaction.
    transaction.recordCreate(DdlObjectKind::Table, tname);

    size_t inserted = 0;
    if (stmt->withData) {
        const auto parsedConditions =
            StorageEngine::parseConditions(conditions);
        std::vector<StorageEngine::SqlRow> sourceRows;
        const bool scanOk = g_engine.forEachVisibleRow(
            s.currentDB, srcTable, "SELECT",
            [&](uint32_t pageId, uint16_t slotId,
                const char* data, size_t length) {
                const int64_t rid =
                    StorageEngine::encodeRid(pageId, slotId);
                StorageEngine::bindNullRow(
                    &g_engine, s.currentDB, srcTable, rid, srcTbl.len);
                struct BindingGuard {
                    ~BindingGuard() { StorageEngine::unbindNullRow(); }
                } bindingGuard;

                const std::string row(data, length);
                for (const auto& condition : parsedConditions) {
                    if (!StorageEngine::evalConditionOnRow(
                            condition, row, srcTbl)) {
                        return;
                    }
                }

                StorageEngine::SqlRow values;
                for (const size_t sourceColumn : selectedSourceColumns) {
                    bool isNull = false;
                    std::string value = g_engine.extractColumnValue(
                        row, srcTbl, sourceColumn, s.currentDB, true,
                        &isNull);
                    const std::string& name =
                        srcTbl.cols[sourceColumn].dataName;
                    if (isNull) values[name] = std::nullopt;
                    else values[name] = std::move(value);
                }
                sourceRows.push_back(std::move(values));
            });
        if (!scanOk) {
            std::cout << "CTAS: source scan failed" << std::endl;
            return true;
        }
        for (const auto& row : sourceRows) {
            const DBStatus status =
                g_engine.insertRow(s.currentDB, tname, row);
            if (status != DBStatus::OK) {
                std::cout << "CTAS: row copy failed" << std::endl;
                return true;
            }
            ++inserted;
        }
    }

    std::cout << "CREATE TABLE AS succeeded: " << inserted << " rows" << std::endl;
    return false;
}

// Extract the sequence name from a DEFAULT expression that calls nextval('seqname'),
// allowing optional whitespace and an optional schema qualifier. Returns empty string
// if the expression is not a simple nextval literal call.
static std::string extractNextvalSequence(const std::string& expr) {
    std::string lower;
    lower.reserve(expr.size());
    for (char c : expr) lower += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    size_t pos = lower.find("nextval");
    if (pos == std::string::npos) return "";
    pos += 7;
    while (pos < lower.size() && std::isspace(static_cast<unsigned char>(lower[pos]))) ++pos;
    if (pos >= lower.size() || lower[pos] != '(') return "";
    ++pos;
    while (pos < lower.size() && std::isspace(static_cast<unsigned char>(lower[pos]))) ++pos;
    if (pos >= lower.size() || expr[pos] != '\'') return "";
    ++pos;
    size_t end = expr.find('\'', pos);
    if (end == std::string::npos) return "";
    return expr.substr(pos, end - pos);
}

// Find all (table, column) pairs in the database whose default expression
// references nextval('seqname') (or schema-qualified variant).
static std::vector<std::pair<std::string, std::string>> findDefaultNextvalDeps(
    const std::string& dbname, const std::string& seqname) {
    auto canonicalSequenceName = [](const std::string& name) {
        return name.rfind("public.", 0) == 0 ? name.substr(7) : name;
    };
    const std::string canonicalTarget = canonicalSequenceName(seqname);
    std::vector<std::pair<std::string, std::string>> deps;
    for (const auto& tname : g_engine.getTableNames(dbname)) {
        dbms::TableSchema tbl = g_engine.getTableSchema(dbname, tname);
        for (size_t i = 0; i < tbl.len; ++i) {
            const std::string sequence =
                extractNextvalSequence(tbl.cols[i].defaultValue);
            if (!sequence.empty() &&
                canonicalSequenceName(sequence) == canonicalTarget) {
                deps.emplace_back(tname, tbl.cols[i].dataName);
            }
        }
    }
    return deps;
}

static std::string renameNextvalSequenceReference(const std::string& expression,
                                                  const std::string& oldName,
                                                  const std::string& newName) {
    std::string lowerExpression = expression;
    std::transform(lowerExpression.begin(), lowerExpression.end(), lowerExpression.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    const std::string referenced = extractNextvalSequence(expression);
    if (referenced.empty()) return expression;
    auto canonicalSequenceName = [](const std::string& name) {
        return name.rfind("public.", 0) == 0 ? name.substr(7) : name;
    };
    if (canonicalSequenceName(referenced) !=
        canonicalSequenceName(oldName)) return expression;

    std::string replacement = newName;
    if (referenced.rfind("public.", 0) == 0 &&
        newName.find('.') == std::string::npos) {
        replacement = "public." + newName;
    }
    const size_t nextvalPos = lowerExpression.find("nextval");
    const size_t literalStart = expression.find('\'', nextvalPos);
    if (literalStart == std::string::npos) return expression;
    const size_t literalEnd = expression.find('\'', literalStart + 1);
    if (literalEnd == std::string::npos) return expression;
    return expression.substr(0, literalStart + 1) + replacement +
           expression.substr(literalEnd);
}

bool DdlExecutor::executeCreateTable(const CreateTableStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    const bool outerTransaction = g_engine.inTransaction();
    DdlTransaction txn(s);
    if (!outerTransaction) txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }
    const bool temporary = stmt->temp || stmt->localTemp;
    if (!stmt->onCommitValid || (stmt->onCommitSpecified && !temporary)) {
        std::cout << "ERROR: ON COMMIT is only supported for valid temporary tables" << std::endl;
        return true;
    }
    CatalogManager::QualifiedName targetName;
    if (!CatalogManager::parseQualifiedName(stmt->tableName, targetName)) {
        std::cout << "ERROR: invalid table name \"" << stmt->tableName
                  << "\"" << std::endl;
        return true;
    }
    std::string targetSchema = targetName.schema;
    if (targetSchema.empty()) {
        std::vector<std::string> searchPath;
        std::string canonicalSearchPath;
        if (!dbms::parseSessionSearchPath(
                s.searchPath, searchPath, canonicalSearchPath)) {
            std::cout << "ERROR: invalid search_path" << std::endl;
            return true;
        }
        for (const auto& rawSchema : searchPath) {
            const std::string candidate =
                dbms::expandSessionSearchPathEntry(rawSchema, s.username);
            if (candidate == "pg_catalog" || candidate == "pg_temp" ||
                candidate.rfind("pg_temp_", 0) == 0) {
                continue;
            }
            if (g_engine.schemaExists(s.currentDB, candidate)) {
                targetSchema = candidate;
                break;
            }
        }
        if (targetSchema.empty() && !temporary) {
            std::cout << "ERROR: no schema has been selected to create in"
                      << std::endl;
            return true;
        }
    }
    if (!temporary && !g_engine.schemaExists(s.currentDB, targetSchema)) {
        std::cout << "ERROR: schema \"" << targetSchema
                  << "\" does not exist" << std::endl;
        return true;
    }
    CatalogManager* tableCatalog = nullptr;
    if (!temporary) {
        try {
            tableCatalog = &g_engine.catalogService().get(s.currentDB);
            const PgNamespaceRow* targetNamespace =
                tableCatalog->findNamespaceByName(targetSchema);
            if (!targetNamespace) {
                std::cout << "ERROR: schema \"" << targetSchema
                          << "\" has no catalog entry" << std::endl;
                return true;
            }
            if (tableCatalog->findClassByName(
                    targetName.name, targetNamespace->oid)) {
                if (stmt->ifNotExists) {
                    std::cout << "NOTICE: relation \"" << stmt->tableName
                              << "\" already exists, skipping" << std::endl;
                    return false;
                }
                std::cout << "ERROR: relation \"" << stmt->tableName
                          << "\" already exists (SQLSTATE 42P07)"
                          << std::endl;
                return true;
            }
        } catch (const std::exception& error) {
            std::cout << "CREATE TABLE catalog preflight failed: "
                      << error.what() << std::endl;
            return true;
        }
    }
    const std::string unqualifiedStorageName = targetSchema == "public"
        ? targetName.name : targetSchema + "__" + targetName.name;
    const std::string tname = temporary
        ? tempTablePrefix(s, targetName.name)
        : (targetName.schema.empty()
               ? unqualifiedStorageName
               : resolveTableName(s, stmt->tableName));
    if (g_engine.tableExists(s.currentDB, tname) || g_engine.viewExists(s.currentDB, tname)) {
        if (stmt->ifNotExists) {
            std::cout << "NOTICE: table \"" << tname << "\" already exists, skipping" << std::endl;
            return false;
        }
        std::cout << "ERROR: relation \"" << tname
                  << "\" already exists (SQLSTATE 42P07)" << std::endl;
        return true;
    }

    if (temporary && !stmt->partitionOf.empty()) {
        std::cout << "CREATE TEMP TABLE PARTITION OF is not supported" << std::endl;
        return true;
    }
    const auto registerTemporaryTable = [&]() {
        if (!temporary) return;
        s.tempTables.insert(stmt->tableName);
        s.tempTableOnCommit[stmt->tableName] = stmt->onCommit;
        if (g_engine.inTransaction()) {
            s.tempTablesCreatedInTransaction.insert(stmt->tableName);
        }
    };

    // CREATE TABLE child PARTITION OF parent ...
    if (!stmt->partitionOf.empty()) {
        std::string parent = resolveTableName(s, stmt->partitionOf);
        if (!g_engine.tableExists(s.currentDB, parent)) {
            std::cout << "Parent partitioned table " << stmt->partitionOf
                      << " not found" << std::endl;
            return true;
        }
        TableSchema parentSchema = g_engine.getTableSchema(s.currentDB, parent);
        if (parentSchema.partitionType == TableSchema::PartitionType::None) {
            std::cout << "Parent table " << stmt->partitionOf
                      << " is not partitioned" << std::endl;
            return true;
        }

        // A partition stores the parent's row layout but owns no partition
        // routing metadata of its own. attachPartition records the bound on
        // the parent and creates the parent's partition data fork.
        TableSchema child = parentSchema;
        child.tablename = tname;
        child.owner = effectiveSessionRole(s);
        child.isTemporary = temporary;
        child.partitionType = TableSchema::PartitionType::None;
        child.partitionKey.clear();
        child.rangePartitions.clear();
        child.listPartitions.clear();
        child.hashPartitions = 0;
        child.defaultPartitionName.clear();
        child.subPartitionType = TableSchema::PartitionType::None;
        child.subPartitionKey.clear();
        child.subHashPartitions = 0;

        txn.markSnapshotDirty();
        if (g_engine.createTable(s.currentDB, child) != DBStatus::OK) {
            std::cout << "CREATE TABLE partition failed" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Table, tname);
        DBStatus attach = g_engine.attachPartition(
            s.currentDB, parent, tname, stmt->partitionBoundSpec);
        if (attach != DBStatus::OK) {
            txn.rollback();
            std::cout << "CREATE TABLE partition attach failed" << std::endl;
            return true;
        }

        if (!temporary) {
            try {
                CatalogManager& cat = *tableCatalog;
                registerTableInCatalog(
                    cat, child, targetSchema, targetName.name);
                if (!updateTableHierarchyFlagsInCatalog(
                        cat, s.currentDB, parent) ||
                    !updateTableHierarchyFlagsInCatalog(
                        cat, s.currentDB, tname, true)) {
                    throw std::runtime_error(
                        "cannot update partition catalog flags");
                }
                if (!cat.persistAll()) {
                    throw std::runtime_error(
                        "cannot persist partition catalog");
                }
            } catch (const std::exception& e) {
                std::cout << "CREATE TABLE partition catalog registration failed: "
                          << e.what() << std::endl;
                return true;
            }
        }
        if (!temporary) {
            g_engine.applyDefaultPrivileges(s.currentDB, targetSchema, "table", tname,
                                            effectiveSessionRole(s));
        }
        registerTemporaryTable();
        if (!txn.commit()) return true;
        return false;
    }

    // CREATE TABLE ... AS SELECT ...
    if (!stmt->asSelect.empty()) {
        if (executeCreateTableAs(stmt, s, tname, txn)) return true;
        if (!temporary) {
            try {
                CatalogManager& cat = *tableCatalog;
                registerTableInCatalog(
                    cat, g_engine.getTableSchema(s.currentDB, tname),
                    targetSchema, targetName.name);
                if (!cat.persistAll()) {
                    throw std::runtime_error("cannot persist CTAS catalog");
                }
            } catch (const std::exception& e) {
                std::cout << "CTAS catalog registration failed: "
                          << e.what() << std::endl;
                return true;
            }
        }
        registerTemporaryTable();
        if (!txn.commit()) return true;
        return false;
    }

    TableSchema tbl;
    tbl.tablename = tname;
    tbl.owner = effectiveSessionRole(s);
    tbl.isTemporary = temporary;
    tbl.isUnlogged = stmt->unlogged;
    tbl.tablespace = stmt->tablespace.empty() ? "pg_default" : stmt->tablespace;
    tbl.storageParams = stmt->options;

    // CREATE TABLE ... (LIKE source [INCLUDING ...]) — copy source columns first.
    // Plain LIKE copies column definitions + NOT NULL + collation only. DEFAULTS,
    // CHECK constraints, identity and PK/UNIQUE are copied only with the matching
    // INCLUDING option (or INCLUDING ALL).
    std::vector<std::pair<std::string, std::string>> likeColumnComments;
    size_t copiedPrimaryKeyCount = 0;
    std::set<std::string> locallyDeclaredColumnNames;
    for (const auto& lc : stmt->likeClauses) {
        if (!lc.optionsValid) {
            std::cout << "ERROR: invalid CREATE TABLE LIKE option \""
                      << lc.invalidOption << "\"" << std::endl;
            return true;
        }
        std::string src = resolveTableName(s, lc.tableName);
        if (!g_engine.tableExists(s.currentDB, src)) {
            std::cout << "LIKE source table " << lc.tableName << " not found" << std::endl;
            return true;
        }
        if (lc.includingStatistics) {
            bool hasStatistics = false;
            std::string statisticsError;
            if (!likeSourceHasExtendedStatistics(
                    s.currentDB, src, hasStatistics, statisticsError)) {
                std::cout << "ERROR: cannot inspect LIKE source statistics: "
                          << statisticsError << std::endl;
                return true;
            }
            if (hasStatistics) {
                std::cout << "ERROR: LIKE INCLUDING STATISTICS is not supported "
                             "for a source with extended statistics"
                          << std::endl;
                return true;
            }
        }
        TableSchema srcSchema = g_engine.getTableSchema(s.currentDB, src);
        if (lc.includingIndexes) {
            bool hasUncopyableIndex = false;
            std::string indexError;
            if (!inspectLikeSourceIndexes(
                    s.currentDB, src, srcSchema,
                    hasUncopyableIndex, indexError)) {
                std::cout << "ERROR: cannot inspect LIKE source indexes: "
                          << indexError << std::endl;
                return true;
            }
            if (hasUncopyableIndex) {
                std::cout << "ERROR: LIKE INCLUDING INDEXES cannot clone one "
                             "or more standalone indexes or exclusion constraints"
                          << std::endl;
                return true;
            }
        }
        const size_t destinationOffset = tbl.len;
        for (size_t i = 0; i < srcSchema.len; ++i) {
            if (!locallyDeclaredColumnNames.insert(
                    srcSchema.cols[i].dataName).second) {
                std::cout << "ERROR: column \""
                          << srcSchema.cols[i].dataName
                          << "\" specified more than once" << std::endl;
                return true;
            }
            if (tbl.len >= MAX_COLUMNS) {
                std::cout << "ERROR: table cannot have more than "
                          << MAX_COLUMNS << " columns" << std::endl;
                return true;
            }
            Column c = srcSchema.cols[i];
            if (!lc.includingDefaults) c.defaultValue.clear();
            if (!lc.includingConstraints) {
                c.checkExpr.clear();
                c.checkConstraintName.clear();
            }
            if (!lc.includingGenerated) {
                c.generatedExpr.clear();
                c.generatedKind = 0;
            }
            if (!lc.includingIdentity && c.identityKind != 0) {
                c.isAutoIncrement = false;
                c.identityKind = 0;
            }
            if (!lc.includingIndexes) {
                c.isPrimaryKey = false;
                c.isUnique = false;
            }
            tbl.append(c);
            if (lc.includingComments) {
                const std::string comment = g_engine.getColumnComment(
                    s.currentDB, src, c.dataName);
                if (!comment.empty()) {
                    likeColumnComments.emplace_back(c.dataName, comment);
                }
            }
        }
        if (lc.includingIndexes) {
            std::vector<size_t> sourcePrimaryKey = srcSchema.pkColIndices;
            if (sourcePrimaryKey.empty()) {
                for (size_t i = 0; i < srcSchema.len; ++i) {
                    if (srcSchema.cols[i].isPrimaryKey) {
                        sourcePrimaryKey.push_back(i);
                    }
                }
            }
            if (!sourcePrimaryKey.empty() && ++copiedPrimaryKeyCount > 1) {
                std::cout << "ERROR: multiple primary keys for table are not allowed"
                          << std::endl;
                return true;
            }
            for (const size_t sourceIndex : sourcePrimaryKey) {
                if (sourceIndex >= srcSchema.len) {
                    std::cout << "ERROR: LIKE source has invalid primary key metadata"
                              << std::endl;
                    return true;
                }
                const size_t targetIndex = destinationOffset + sourceIndex;
                tbl.pkColIndices.push_back(targetIndex);
                tbl.cols[targetIndex].isPrimaryKey = true;
                tbl.cols[targetIndex].isNull = false;
            }
            for (const auto& sourceUnique : srcSchema.uniqueConstraints) {
                std::vector<size_t> targetUnique;
                targetUnique.reserve(sourceUnique.size());
                for (const size_t sourceIndex : sourceUnique) {
                    if (sourceIndex >= srcSchema.len) {
                        std::cout << "ERROR: LIKE source has invalid unique "
                                     "constraint metadata"
                                  << std::endl;
                        return true;
                    }
                    targetUnique.push_back(destinationOffset + sourceIndex);
                }
                tbl.uniqueConstraints.push_back(std::move(targetUnique));
                // LIKE chooses a fresh default constraint name for the target.
                tbl.uniqueConstraintNames.emplace_back();
            }
        }
        if (lc.includingConstraints) {
            tbl.additionalCheckConstraints.insert(
                tbl.additionalCheckConstraints.end(),
                srcSchema.additionalCheckConstraints.begin(),
                srcSchema.additionalCheckConstraints.end());
        }
    }

    // Convert locally declared columns before merging inheritance so a local
    // declaration with the same name as a parent column is merged instead of
    // being appended as a duplicate after the inheritance pass.
    for (const auto& cd : stmt->columns) {
        if (!locallyDeclaredColumnNames.insert(cd.name).second) {
            std::cout << "ERROR: column \"" << cd.name
                      << "\" specified more than once" << std::endl;
            return true;
        }
        Column column;
        std::string typeError;
        if (!columnDefToColumn(cd, s.currentDB, column, typeError,
                              s.compatibilityMode)) {
            std::cout << "Invalid column type: " << typeError << std::endl;
            return true;
        }
        if (tbl.len >= MAX_COLUMNS) {
            std::cout << "ERROR: table cannot have more than "
                      << MAX_COLUMNS << " columns" << std::endl;
            return true;
        }
        tbl.append(column);
        for (size_t checkIndex = 1; checkIndex < cd.checkExprs.size();
             ++checkIndex) {
            CheckConstraint check;
            if (checkIndex < cd.checkNames.size()) {
                check.name = cd.checkNames[checkIndex];
            }
            check.expression = cd.checkExprs[checkIndex]->toString();
            tbl.additionalCheckConstraints.push_back(std::move(check));
        }
    }

    // CREATE TABLE ... INHERITS (parent, ...) — prepend inherited columns
    // (parent columns first, in declaration order, then this table's own;
    // same-named columns are not duplicated). The relationship is recorded
    // in <db>/.inherits so SELECT/UPDATE/DELETE can expand children.
    std::vector<std::string> inheritedParents;
    CatalogInheritanceColumns inheritanceCatalogColumns;
    if (!stmt->inherits.empty()) {
        const TableSchema localSchema = tbl;
        TableSchema merged = localSchema;
        merged.len = 0;
        merged.fkLen = 0;
        merged.pkColIndices.clear();
        merged.uniqueConstraints.clear();
        merged.uniqueConstraintNames.clear();
        merged.additionalCheckConstraints.clear();

        std::map<std::string, size_t> mergedColumns;
        std::set<std::string> resolvedParents;
        std::set<std::string> inheritedColumnNames;
        std::set<std::string> localDefaultOverrides;
        for (size_t i = 0; i < localSchema.len; ++i) {
            if (!localSchema.cols[i].defaultValue.empty()) {
                localDefaultOverrides.insert(localSchema.cols[i].dataName);
            }
        }
        std::vector<size_t> localColumnMap(
            localSchema.len, std::numeric_limits<size_t>::max());
        std::string mergeError;
        const auto columnsHaveCompatibleTypes = [](const Column& left,
                                                   const Column& right) {
            return left.dataType == right.dataType &&
                   left.dsize == right.dsize &&
                   left.isVariableLength == right.isVariableLength &&
                   left.isUnsigned == right.isUnsigned &&
                   left.isArray == right.isArray &&
                   left.collation == right.collation &&
                   left.enumValues == right.enumValues &&
                   left.domainName == right.domainName;
        };
        auto mergeCheckConstraint = [&](Column& target,
                                        const Column& incoming) {
            if (incoming.checkExpr.empty()) return true;
            if (target.checkExpr.empty()) {
                target.checkExpr = incoming.checkExpr;
                target.checkConstraintName = incoming.checkConstraintName;
                target.deferrable = incoming.deferrable;
                target.initiallyDeferred = incoming.initiallyDeferred;
                return true;
            }
            if (!target.checkConstraintName.empty() &&
                target.checkConstraintName == incoming.checkConstraintName) {
                if (target.checkExpr != incoming.checkExpr ||
                    target.deferrable != incoming.deferrable ||
                    target.initiallyDeferred !=
                        incoming.initiallyDeferred) {
                    mergeError = "inherited CHECK constraint \"" +
                        target.checkConstraintName +
                        "\" has conflicting expressions";
                    return false;
                }
                return true;
            }
            CheckConstraint additional;
            additional.name = incoming.checkConstraintName;
            additional.expression = incoming.checkExpr;
            additional.deferrable = incoming.deferrable;
            additional.initiallyDeferred = incoming.initiallyDeferred;
            merged.additionalCheckConstraints.push_back(
                std::move(additional));
            return true;
        };
        auto mergeColumn = [&](Column& target, const Column& incoming,
                               bool localDeclaration) {
            if (!columnsHaveCompatibleTypes(target, incoming)) {
                mergeError = "inherited column \"" + incoming.dataName +
                    "\" has an incompatible type";
                return false;
            }
            target.isNull = target.isNull && incoming.isNull;
            if (!incoming.defaultValue.empty()) {
                if (localDeclaration || target.defaultValue.empty()) {
                    target.defaultValue = incoming.defaultValue;
                } else if (target.defaultValue != incoming.defaultValue &&
                           localDefaultOverrides.count(
                               incoming.dataName) == 0) {
                    mergeError = "inherited column \"" + incoming.dataName +
                        "\" has conflicting default values";
                    return false;
                }
            }
            if (!incoming.generatedExpr.empty()) {
                if (!target.generatedExpr.empty() &&
                    (target.generatedExpr != incoming.generatedExpr ||
                     target.generatedKind != incoming.generatedKind)) {
                    mergeError = "inherited generated column \"" +
                        incoming.dataName + "\" has conflicting expressions";
                    return false;
                }
                target.generatedExpr = incoming.generatedExpr;
                target.generatedKind = incoming.generatedKind;
            }
            if (!mergeCheckConstraint(target, incoming)) return false;
            if (localDeclaration) {
                // PRIMARY KEY, UNIQUE and identity are local-only properties.
                target.isPrimaryKey = incoming.isPrimaryKey;
                target.isUnique = incoming.isUnique;
                target.isAutoIncrement = incoming.isAutoIncrement;
                target.identityKind = incoming.identityKind;
            }
            return true;
        };

        for (const auto& parentRaw : stmt->inherits) {
            std::string parent = resolveTableName(s, parentRaw);
            if (!g_engine.tableExists(s.currentDB, parent)) {
                std::cout << "Parent table " << parentRaw << " not found" << std::endl;
                return true;
            }
            if (!resolvedParents.insert(parent).second) {
                std::cout << "ERROR: relation \"" << parentRaw
                          << "\" would be inherited from more than once"
                          << std::endl;
                return true;
            }
            inheritedParents.push_back(parent);
            TableSchema parentSchema = g_engine.getTableSchema(s.currentDB, parent);
            for (size_t i = 0; i < parentSchema.len; ++i) {
                Column inherited = parentSchema.cols[i];
                auto& provenance =
                    inheritanceCatalogColumns[inherited.dataName];
                provenance.isLocal =
                    locallyDeclaredColumnNames.count(inherited.dataName) != 0;
                ++provenance.directParentCount;
                // These constraints are local to the parent. CHECK and
                // NOT NULL remain on the inherited column.
                inherited.isPrimaryKey = false;
                inherited.isUnique = false;
                inherited.isAutoIncrement = false;
                inherited.identityKind = 0;
                const auto existing = mergedColumns.find(inherited.dataName);
                inheritedColumnNames.insert(inherited.dataName);
                if (existing != mergedColumns.end()) {
                    if (!mergeColumn(
                            merged.cols[existing->second], inherited, false)) {
                        std::cout << "ERROR: " << mergeError << std::endl;
                        return true;
                    }
                    continue;
                }
                if (merged.len >= MAX_COLUMNS) {
                    std::cout << "ERROR: inherited table has too many columns"
                              << std::endl;
                    return true;
                }
                mergedColumns[inherited.dataName] = merged.len;
                merged.append(inherited);
            }
            merged.additionalCheckConstraints.insert(
                merged.additionalCheckConstraints.end(),
                parentSchema.additionalCheckConstraints.begin(),
                parentSchema.additionalCheckConstraints.end());
        }

        for (size_t i = 0; i < localSchema.len; ++i) {
            const Column& local = localSchema.cols[i];
            const auto existing = mergedColumns.find(local.dataName);
            if (existing != mergedColumns.end()) {
                if (inheritedColumnNames.count(local.dataName) == 0) {
                    std::cout << "ERROR: column \"" << local.dataName
                              << "\" specified more than once" << std::endl;
                    return true;
                }
                if (!mergeColumn(
                        merged.cols[existing->second], local, true)) {
                    std::cout << "ERROR: " << mergeError << std::endl;
                    return true;
                }
                localColumnMap[i] = existing->second;
                continue;
            }
            if (merged.len >= MAX_COLUMNS) {
                std::cout << "ERROR: inherited table has too many columns"
                          << std::endl;
                return true;
            }
            localColumnMap[i] = merged.len;
            mergedColumns[local.dataName] = merged.len;
            merged.append(local);
        }
        for (const size_t localIndex : localSchema.pkColIndices) {
            if (localIndex >= localColumnMap.size()) {
                std::cout << "ERROR: invalid local primary key metadata"
                          << std::endl;
                return true;
            }
            merged.pkColIndices.push_back(localColumnMap[localIndex]);
        }
        for (const auto& localUnique : localSchema.uniqueConstraints) {
            std::vector<size_t> remapped;
            for (const size_t localIndex : localUnique) {
                if (localIndex >= localColumnMap.size()) {
                    std::cout << "ERROR: invalid local unique constraint metadata"
                              << std::endl;
                    return true;
                }
                remapped.push_back(localColumnMap[localIndex]);
            }
            merged.uniqueConstraints.push_back(std::move(remapped));
        }
        merged.uniqueConstraintNames = localSchema.uniqueConstraintNames;
        for (size_t i = 0; i < localSchema.fkLen; ++i) {
            merged.appendFK(localSchema.fks[i]);
        }
        merged.additionalCheckConstraints.insert(
            merged.additionalCheckConstraints.end(),
            localSchema.additionalCheckConstraints.begin(),
            localSchema.additionalCheckConstraints.end());
        tbl = merged;
    }

    // CREATE TABLE name OF composite_type — derive columns from the type's fields.
    if (!stmt->ofType.empty()) {
        StorageEngine::CompositeType ct = g_engine.getCompositeType(s.currentDB, stmt->ofType);
        if (ct.name.empty()) {
            ct = g_engine.getCompositeType(s.currentDB, toLower(stmt->ofType));
        }
        if (ct.name.empty()) {
            std::cout << "OF type " << stmt->ofType << " not found" << std::endl;
            return true;
        }
        for (const auto& f : ct.fields) {
            if (tbl.len >= MAX_COLUMNS) {
                std::cout << "ERROR: table cannot have more than "
                          << MAX_COLUMNS << " columns" << std::endl;
                return true;
            }
            ColumnDef cd;
            cd.name = f.first;
            cd.isNull = true;
            // Parse a stored field type string like "varchar(50)" / "numeric(10,2)" / "int".
            const std::string& ts = f.second;
            size_t lp = ts.find('(');
            if (lp != std::string::npos) {
                cd.typeName = trim(ts.substr(0, lp));
                size_t rp = ts.find(')', lp);
                std::string mods = ts.substr(lp + 1,
                    (rp == std::string::npos ? ts.size() : rp) - lp - 1);
                std::stringstream ms(mods);
                std::string m;
                while (std::getline(ms, m, ',')) {
                    m = trim(m);
                    if (!m.empty()) cd.typeMods.push_back(m);
                }
            } else {
                cd.typeName = trim(ts);
            }
            Column column;
            std::string typeError;
            if (!columnDefToColumn(cd, s.currentDB, column, typeError,
                            s.compatibilityMode)) {
                std::cout << "Invalid column type: " << typeError << std::endl;
                return true;
            }
            tbl.append(column);
        }
    }

    // PARTITION BY (col) — wire partition metadata from AST to engine.
    if (!stmt->partitionBy.empty()) {
        auto* colRef = dynamic_cast<ColumnRefExpr*>(stmt->partitionBy[0].expr.get());
        if (colRef) tbl.partitionKey = colRef->column;
        std::string pt = toLower(stmt->partitionType);
        if (pt == "range") tbl.partitionType = TableSchema::PartitionType::Range;
        else if (pt == "list") tbl.partitionType = TableSchema::PartitionType::List;
        else if (pt == "hash") tbl.partitionType = TableSchema::PartitionType::Hash;
    }

    // Inline PRIMARY KEY declarations and a copied key each already define
    // the table's one permitted primary key.  Without composite index metadata,
    // multiple inline flags represent multiple declarations rather than one
    // composite key.
    bool hasPrimaryKeyDefinition = !tbl.pkColIndices.empty();
    if (hasPrimaryKeyDefinition) {
        std::set<size_t> representedPrimaryColumns(
            tbl.pkColIndices.begin(), tbl.pkColIndices.end());
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isPrimaryKey &&
                representedPrimaryColumns.count(i) == 0) {
                std::cout << "ERROR: multiple primary keys for table are not allowed"
                          << std::endl;
                return true;
            }
        }
    }
    if (!hasPrimaryKeyDefinition) {
        size_t inlinePrimaryKeys = 0;
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].isPrimaryKey) ++inlinePrimaryKeys;
        }
        if (inlinePrimaryKeys > 1) {
            std::cout << "ERROR: multiple primary keys for table are not allowed"
                      << std::endl;
            return true;
        }
        hasPrimaryKeyDefinition = inlinePrimaryKeys == 1;
    }

    // Table-level constraints
    for (const auto& tc : stmt->constraints) {
        std::string t = toLower(tc.type);
        if (t == "primary key") {
            if (hasPrimaryKeyDefinition) {
                std::cout << "ERROR: multiple primary keys for table are not allowed"
                          << std::endl;
                return true;
            }
            if (tc.columns.empty()) {
                std::cout << "ERROR: PRIMARY KEY requires at least one column"
                          << std::endl;
                return true;
            }
            std::vector<size_t> primaryColumns;
            std::set<size_t> distinctColumns;
            for (const auto& cname : tc.columns) {
                size_t columnIndex = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == cname) {
                        columnIndex = i;
                        break;
                    }
                }
                if (columnIndex >= tbl.len) {
                    std::cout << "ERROR: PRIMARY KEY column \"" << cname
                              << "\" does not exist" << std::endl;
                    return true;
                }
                if (!distinctColumns.insert(columnIndex).second) {
                    std::cout << "ERROR: PRIMARY KEY column \"" << cname
                              << "\" appears more than once" << std::endl;
                    return true;
                }
                primaryColumns.push_back(columnIndex);
            }
            tbl.pkColIndices = std::move(primaryColumns);
            for (const size_t columnIndex : tbl.pkColIndices) {
                tbl.cols[columnIndex].isPrimaryKey = true;
                tbl.cols[columnIndex].isNull = false;
            }
            hasPrimaryKeyDefinition = true;
        } else if (t == "unique") {
            if (tc.columns.empty()) {
                std::cout << "ERROR: UNIQUE requires at least one column"
                          << std::endl;
                return true;
            }
            std::vector<size_t> idxs;
            std::set<size_t> distinctColumns;
            for (const auto& cname : tc.columns) {
                size_t columnIndex = tbl.len;
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == cname) {
                        columnIndex = i;
                        break;
                    }
                }
                if (columnIndex >= tbl.len) {
                    std::cout << "ERROR: UNIQUE column \"" << cname
                              << "\" does not exist" << std::endl;
                    return true;
                }
                if (!distinctColumns.insert(columnIndex).second) {
                    std::cout << "ERROR: UNIQUE column \"" << cname
                              << "\" appears more than once" << std::endl;
                    return true;
                }
                idxs.push_back(columnIndex);
            }
            tbl.uniqueConstraints.push_back(std::move(idxs));
            tbl.uniqueConstraintNames.push_back(tc.name);
        } else if (t == "foreign key") {
            tbl.appendFK(tableConstraintToForeignKey(tc));
        } else if (t == "check") {
            if (tbl.len == 0 || !tc.checkExpr) {
                std::cout << "ERROR: CHECK constraint requires an expression"
                          << std::endl;
                return true;
            }
            const std::string expression = tc.checkExpr->toString();
            if (tbl.cols[0].checkExpr.empty()) {
                tbl.cols[0].checkExpr = expression;
                tbl.cols[0].checkConstraintName = tc.name;
                tbl.cols[0].deferrable = tc.deferrable;
                tbl.cols[0].initiallyDeferred = tc.initiallyDeferred;
            } else {
                CheckConstraint check;
                check.name = tc.name;
                check.expression = expression;
                check.deferrable = tc.deferrable;
                check.initiallyDeferred = tc.initiallyDeferred;
                tbl.additionalCheckConstraints.push_back(std::move(check));
            }
        } else if (t == "exclude") {
            // Defer creation until the table exists; collect for later.
        }
    }

    std::string checkConstraintError;
    if (!normalizeNamedCheckConstraints(tbl, checkConstraintError)) {
        std::cout << "ERROR: " << checkConstraintError << std::endl;
        return true;
    }

    // Prepare the database-wide inheritance graph before creating any physical
    // relation.  Publishing it is deferred until the child exists, but an
    // unreadable/non-regular graph must fail without leaving a table behind.
    std::filesystem::path inheritancePath;
    std::string inheritanceMetadata;
    if (!inheritedParents.empty()) {
        inheritancePath = std::filesystem::path(g_engine.dbPath(s.currentDB)) /
            ".inherits";
        std::error_code inheritanceError;
        if (std::filesystem::exists(inheritancePath, inheritanceError)) {
            if (!std::filesystem::is_regular_file(
                    inheritancePath, inheritanceError) || inheritanceError) {
                std::cout << "Could not inspect inheritance metadata"
                          << std::endl;
                return true;
            }
            std::ifstream input(inheritancePath, std::ios::binary);
            if (!input) {
                std::cout << "Could not read inheritance metadata" << std::endl;
                return true;
            }
            inheritanceMetadata.assign(
                std::istreambuf_iterator<char>(input),
                std::istreambuf_iterator<char>());
            if (input.bad()) {
                std::cout << "Could not read inheritance metadata" << std::endl;
                return true;
            }
        } else if (inheritanceError) {
            std::cout << "Could not inspect inheritance metadata" << std::endl;
            return true;
        }

        std::set<std::pair<std::string, std::string>> existingEdges;
        std::istringstream existingLines(inheritanceMetadata);
        std::string line;
        while (std::getline(existingLines, line)) {
            const size_t separator = line.find('|');
            if (separator == std::string::npos) continue;
            existingEdges.emplace(line.substr(0, separator),
                                  line.substr(separator + 1));
        }
        for (const auto& parent : inheritedParents) {
            if (!existingEdges.emplace(parent, tname).second) continue;
            if (!inheritanceMetadata.empty() &&
                inheritanceMetadata.back() != '\n') {
                inheritanceMetadata.push_back('\n');
            }
            inheritanceMetadata += parent + "|" + tname + "\n";
        }
    }

    txn.markSnapshotDirty();
    DBStatus res = g_engine.createTable(s.currentDB, tbl);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TABLE failed" << std::endl;
        return true;
    }
    // Constraint metadata and catalog registration happen after the physical
    // relation exists.  Keep the relation in the rollback log immediately.
    txn.recordCreate(DdlObjectKind::Table, tname);

    if (!inheritedParents.empty() &&
        !index_file::writeAtomically(
            inheritancePath, inheritanceMetadata)) {
        std::cout << "Could not persist inheritance metadata" << std::endl;
        return true;
    }

    for (const auto& [columnName, comment] : likeColumnComments) {
        const DBStatus commentStatus = g_engine.commentOnColumn(
            s.currentDB, tname, columnName, comment);
        if (commentStatus != DBStatus::OK) {
            std::cout << "CREATE TABLE LIKE comment copy failed" << std::endl;
            return true;
        }
    }

    // The primary-key name is metadata, not an interchangeable label.  Keep
    // it alongside the other constraint metadata so DROP CONSTRAINT can
    // distinguish the real key from an unrelated or misspelled name.
    if (tbl.hasPrimaryKey()) {
        std::string primaryKeyName = tname + "_pkey";
        for (const auto& tc : stmt->constraints) {
            if (toLower(tc.type) == "primary key" && !tc.name.empty()) {
                primaryKeyName = tc.name;
                break;
            }
        }
        DBStatus primaryKeyMetadataStatus = g_engine.updateStorageParams(
            s.currentDB, tname,
            {{PRIMARY_KEY_CONSTRAINT_NAME_PARAM, primaryKeyName}});
        if (!alterStatusOk(primaryKeyMetadataStatus, "Constraint")) return true;
    }

    // Create exclusion constraints now that the table exists.
    for (const auto& tc : stmt->constraints) {
        if (toLower(tc.type) != "exclude") continue;
        StorageEngine::ExclusionConstraint ec;
        ec.name = tc.name;
        ec.tableName = tname;
        ec.accessMethod = tc.accessMethod.empty() ? "btree" : tc.accessMethod;
        for (const auto& e : tc.excludeElements) {
            ec.elements.push_back({e.first, toLower(e.second)});
        }
        ec.wherePredicate = tc.excludeWhere;
        DBStatus exclusionStatus = g_engine.createExclusionConstraint(s.currentDB, ec);
        if (exclusionStatus != DBStatus::OK) {
            std::cout << "CREATE TABLE exclusion constraint failed" << std::endl;
            return true;
        }
    }
    for (const auto& tc : stmt->constraints) {
        if (tc.name.empty()) continue;
        if (!alterStatusOk(persistConstraintMetadata(
                s.currentDB, tname, tc.name, !tc.notValid, tc.notValid,
                tc.deferrable, tc.initiallyDeferred), "Constraint")) return true;
    }
    if (!temporary) {
        g_engine.applyDefaultPrivileges(s.currentDB, targetSchema, "table", tname,
                                        effectiveSessionRole(s));
    }

    if (!temporary) {
        try {
            CatalogManager& cat = *tableCatalog;
            std::map<std::string, int32_t> declaredVarcharMods;
            for (const auto& definition : stmt->columns) {
                int32_t modifier = -1;
                if (declaredVarcharTypeMod(definition, modifier)) {
                    declaredVarcharMods[definition.name] = modifier;
                }
            }
            registerTableInCatalog(
                cat, tbl, targetSchema, targetName.name,
                inheritedParents.empty()
                    ? nullptr : &inheritanceCatalogColumns,
                &declaredVarcharMods);
            const PgClassRow* tableRelation = cat.findClassByName(
                targetName.name,
                cat.findNamespaceByName(targetSchema)->oid);
            if (!tableRelation || tableRelation->relkind != 'r') {
                throw std::runtime_error("created table catalog row is missing");
            }
            for (const auto& parent : inheritedParents) {
                if (!updateTableHierarchyFlagsInCatalog(
                        cat, s.currentDB, parent)) {
                    throw std::runtime_error(
                        "cannot update inheritance catalog flags");
                }
            }
            // DEFAULT nextval() references remain part of the physical table
            // definition and are handled by sequence DROP/RENAME scanning.
            // Modeling them as sequence -> table auto-dependencies would make
            // an ordinary DROP TABLE incorrectly delete an independent
            // sequence; pg_attrdef is not modeled as a separate catalog class.
            if (!cat.persistAll()) {
                throw std::runtime_error("cannot persist table catalog");
            }
        } catch (const std::exception& e) {
            std::cout << "CREATE TABLE catalog registration failed: "
                      << e.what() << std::endl;
            return true;
        }
    }

    registerTemporaryTable();
    if (!txn.commit()) return true;
    std::cout << "CREATE TABLE succeeded" << std::endl;
    return false;
}

struct PhysicalCascadeAction {
    enum class Kind { Table, Index, Sequence, View, MaterializedView };

    Kind kind = Kind::Table;
    std::string name;
    std::string tableName;
    std::string accessMethod;
    std::string key;
};

static bool catalogTableStorageName(const CatalogManager& catalog,
                                    const PgClassRow& relation,
                                    std::string& storageName,
                                    std::string& error) {
    const PgNamespaceRow* relationNamespace =
        catalog.findNamespace(relation.relnamespace);
    if (!relationNamespace) {
        error = "relation namespace is missing";
        return false;
    }
    storageName = relationNamespace->nspname == "public"
        ? relation.relname
        : relationNamespace->nspname + "__" + relation.relname;
    return true;
}

static bool catalogViewStorageName(const CatalogManager& catalog,
                                   const PgClassRow& relation,
                                   std::string& storageName,
                                   std::string& error) {
    const PgNamespaceRow* relationNamespace =
        catalog.findNamespace(relation.relnamespace);
    if (!relationNamespace) {
        error = "relation namespace is missing";
        return false;
    }
    storageName = relationNamespace->nspname == "public"
        ? relation.relname
        : relationNamespace->nspname + "." + relation.relname;
    return true;
}

static bool catalogSequenceStorageName(const CatalogManager& catalog,
                                       const PgClassRow& relation,
                                       std::string& storageName,
                                       std::string& error) {
    const PgNamespaceRow* relationNamespace =
        catalog.findNamespace(relation.relnamespace);
    if (!relationNamespace) {
        error = "sequence namespace is missing";
        return false;
    }
    storageName = relationNamespace->nspname == "public"
        ? relation.relname
        : relationNamespace->nspname + "." + relation.relname;
    return true;
}

static std::string canonicalOwnedTableName(const std::string& name) {
    return name.rfind("public.", 0) == 0 ? name.substr(7) : name;
}

static bool updateOwnedSequenceNames(
    CatalogManager& catalog, const std::string& dbname, Oid tableOid,
    const std::string& oldTableName, const std::string& newTableName,
    int32_t renamedColumnNumber, const std::string& oldColumnName,
    const std::string& newColumnName) {
    const bool renamingColumn = renamedColumnNumber > 0;
    std::set<Oid> updatedSequences;
    for (const auto& dependency :
         catalog.findRefs(PgClassOid_Class, tableOid, -1)) {
        if (dependency.classid != PgClassOid_Class ||
            dependency.objsubid != 0 ||
            dependency.deptype != 'a') {
            continue;
        }
        if (renamingColumn && dependency.refobjsubid > 0 &&
            dependency.refobjsubid != renamedColumnNumber) {
            continue;
        }
        const PgClassRow* dependent = catalog.findClass(dependency.objid);
        if (!dependent || dependent->relkind != 'S' ||
            updatedSequences.count(dependent->oid) != 0) {
            continue;
        }

        std::string sequenceStorageName;
        std::string sequenceNameError;
        if (!catalogSequenceStorageName(
                catalog, *dependent, sequenceStorageName,
                sequenceNameError)) {
            std::cout << "owned sequence metadata update failed: "
                      << sequenceNameError << std::endl;
            return false;
        }
        SequenceInfo current;
        if (g_engine.getSequenceInfo(
                dbname, sequenceStorageName, current) != DBStatus::OK) {
            std::cout << "owned sequence metadata update failed: cannot read "
                      << sequenceStorageName << std::endl;
            return false;
        }

        // Current releases identify the owning column in pg_depend. Legacy
        // releases used zero for both true ownership and ordinary DEFAULT
        // references, so only trust such a row when the sequence file names
        // this table (and, for a column rename, this column).
        const bool explicitOwnership = dependency.refobjsubid > 0;
        const bool legacyOwnership =
            dependency.refobjsubid == 0 &&
            !current.ownedByTable.empty() &&
            canonicalOwnedTableName(current.ownedByTable) ==
                canonicalOwnedTableName(oldTableName);
        if (!explicitOwnership && !legacyOwnership) continue;

        if (renamingColumn) {
            if ((explicitOwnership &&
                 dependency.refobjsubid != renamedColumnNumber) ||
                (legacyOwnership &&
                 current.ownedByColumn != oldColumnName)) {
                continue;
            }
        }

        SequenceInfo update;
        update.ownedBySpecified = true;
        update.ownedByTable = renamingColumn
            ? canonicalOwnedTableName(oldTableName)
            : canonicalOwnedTableName(newTableName);
        update.ownedByColumn = renamingColumn
            ? newColumnName : current.ownedByColumn;
        if (update.ownedByColumn.empty() && explicitOwnership) {
            for (const auto& attribute :
                 catalog.findAttributesByNum(tableOid)) {
                if (attribute.attnum == dependency.refobjsubid) {
                    update.ownedByColumn = attribute.attname;
                    break;
                }
            }
        }
        if (update.ownedByColumn.empty() ||
            g_engine.alterSequence(
                dbname, sequenceStorageName, update) != DBStatus::OK) {
            std::cout << "owned sequence metadata update failed for "
                      << sequenceStorageName << std::endl;
            return false;
        }
        updatedSequences.insert(dependent->oid);
    }
    return true;
}

static bool updateOwnedSequenceTableNames(
    CatalogManager& catalog, const std::string& dbname, Oid tableOid,
    const std::string& oldTableName, const std::string& newTableName) {
    return updateOwnedSequenceNames(
        catalog, dbname, tableOid, oldTableName, newTableName,
        0, "", "");
}

static bool updateOwnedSequenceColumnNames(
    CatalogManager& catalog, const std::string& dbname, Oid tableOid,
    int32_t columnNumber, const std::string& tableName,
    const std::string& oldColumnName, const std::string& newColumnName) {
    return updateOwnedSequenceNames(
        catalog, dbname, tableOid, tableName, tableName,
        columnNumber, oldColumnName, newColumnName);
}

// Catalog CASCADE plans are dependency-complete, but catalog deletion alone
// is not enough: every file-backed dependent relation must be removed before
// the catalog plan is published.  Build the physical worklist first so an
// unresolvable index can fail closed without having changed storage.
static bool buildPhysicalCascadeActions(
    const CatalogManager& catalog,
    StorageEngine& engine,
    const std::string& dbname,
    Oid rootOid,
    const CatalogManager::DropPlan& plan,
    std::vector<PhysicalCascadeAction>& actions,
    std::string& error) {
    for (const auto& object : plan.objectsToDrop) {
        if (object.first != PgClassOid_Class || object.second == rootOid) continue;

        const PgClassRow* relation = catalog.findClass(object.second);
        if (!relation) {
            error = "dependent catalog relation is missing";
            return false;
        }
        const PgClassRow rel = *relation;

        if (rel.relkind == 'r') {
            std::string tableName;
            if (!catalogTableStorageName(
                    catalog, rel, tableName, error)) {
                return false;
            }
            actions.push_back({PhysicalCascadeAction::Kind::Table,
                               tableName, "", "", ""});
            continue;
        }
        if (rel.relkind == 'S') {
            std::string sequenceName;
            if (!catalogSequenceStorageName(
                    catalog, rel, sequenceName, error)) {
                return false;
            }
            actions.push_back({PhysicalCascadeAction::Kind::Sequence,
                               sequenceName, "", "", ""});
            continue;
        }
        if (rel.relkind == 'v' || rel.relkind == 'm') {
            std::string viewName;
            if (!catalogViewStorageName(
                    catalog, rel, viewName, error)) {
                return false;
            }
            const auto kind = rel.relkind == 'v'
                ? PhysicalCascadeAction::Kind::View
                : PhysicalCascadeAction::Kind::MaterializedView;
            actions.push_back({kind, viewName, "", "", ""});
            continue;
        }
        if (rel.relkind != 'i') continue;

        std::string tableName;
        for (const auto& dependency :
             catalog.findDepends(PgClassOid_Class, rel.oid)) {
            if (dependency.refclassid != PgClassOid_Class) continue;
            const PgClassRow* referenced = catalog.findClass(dependency.refobjid);
            if (referenced && referenced->relkind == 'r') {
                if (!catalogTableStorageName(
                        catalog, *referenced, tableName, error)) {
                    return false;
                }
                break;
            }
        }
        if (tableName.empty()) {
            error = "dependent index has no owning table";
            return false;
        }

        bool resolved = false;
        auto named = engine.getNamedIndex(dbname, tableName, rel.relname);
        if (named) {
            actions.push_back({PhysicalCascadeAction::Kind::Index, rel.relname,
                               tableName, named->accessMethod, named->key});
            resolved = true;
        }
        if (!resolved) {
            for (const auto& composite : engine.getCompositeIndexes(dbname, tableName)) {
                if (composite.name == rel.relname) {
                    actions.push_back({PhysicalCascadeAction::Kind::Index, rel.relname,
                                       tableName, "composite", composite.name});
                    resolved = true;
                    break;
                }
            }
        }
        if (!resolved) {
            error = "dependent index has no physical metadata";
            return false;
        }
    }
    return true;
}

static bool dropPhysicalCascadeAction(StorageEngine& engine,
                                      const std::string& dbname,
                                      const PhysicalCascadeAction& action) {
    DBStatus status = DBStatus::INVALID_VALUE;
    if (action.kind == PhysicalCascadeAction::Kind::Table) {
        status = engine.dropTable(dbname, action.name);
    } else if (action.kind == PhysicalCascadeAction::Kind::Sequence) {
        status = engine.dropSequence(dbname, action.name);
    } else if (action.kind == PhysicalCascadeAction::Kind::View) {
        status = engine.dropView(dbname, action.name);
    } else if (action.kind ==
               PhysicalCascadeAction::Kind::MaterializedView) {
        status = engine.dropMaterializedView(dbname, action.name);
    } else {
        status = engine.dropIndexByAccessMethod(dbname, action.tableName,
                                                action.key, action.accessMethod);
    }
    // A missing physical file is already in the desired post-drop state; the
    // catalog plan still needs to remove its stale metadata.  Other failures
    // must abort and let DdlTransaction restore the snapshot.
    return status == DBStatus::OK || status == DBStatus::TABLE_NOT_FOUND;
}

static bool executeSchemaPhysicalDropPlan(
    CatalogManager& catalog, const std::string& dbname,
    const CatalogManager::DropPlan& catalogPlan, bool cascade,
    std::set<std::string>& droppedSequenceStorageNames,
    std::string& error) {
    std::vector<PhysicalCascadeAction> actions;
    if (!buildPhysicalCascadeActions(
            catalog, g_engine, dbname, INVALID_OID,
            catalogPlan, actions, error)) {
        return false;
    }

    std::set<std::string> droppedTableStorageNames;
    std::set<std::string> sequenceStorageNames;
    for (const auto& action : actions) {
        if (action.kind == PhysicalCascadeAction::Kind::Table) {
            droppedTableStorageNames.insert(action.name);
        } else if (action.kind == PhysicalCascadeAction::Kind::Sequence) {
            sequenceStorageNames.insert(action.name);
        }
    }

    // pg_attrdef is not modeled as a catalog class. Preserve its normal
    // dependency behavior explicitly for sequences reached through the
    // namespace plan, including defaults in other schemas.
    std::set<std::pair<std::string, std::string>> defaultsToClear;
    for (const auto& sequenceName : sequenceStorageNames) {
        for (const auto& dependency :
             findDefaultNextvalDeps(dbname, sequenceName)) {
            if (droppedTableStorageNames.count(dependency.first) != 0) {
                continue;
            }
            if (!cascade) {
                error = "default on " + dependency.first + "." +
                        dependency.second + " depends on sequence " +
                        sequenceName;
                return false;
            }
            defaultsToClear.insert(dependency);
        }
    }

    std::set<std::string> changedDefaultTables;
    for (const auto& dependency : defaultsToClear) {
        if (g_engine.alterTableDropDefault(
                dbname, dependency.first,
                dependency.second) != DBStatus::OK) {
            error = "cannot clear default on " + dependency.first + "." +
                    dependency.second;
            return false;
        }
        changedDefaultTables.insert(dependency.first);
    }
    for (const auto& changedTable : changedDefaultTables) {
        if (!synchronizeTableAttributesInCatalog(dbname, changedTable)) {
            error = "cannot persist default removal for " + changedTable;
            return false;
        }
    }

    for (const auto& action : actions) {
        if (!dropPhysicalCascadeAction(g_engine, dbname, action)) {
            error = "cannot remove relation " + action.name;
            return false;
        }
        if (action.kind == PhysicalCascadeAction::Kind::Sequence) {
            droppedSequenceStorageNames.insert(action.name);
        }
    }
    return true;
}

// An OWNED BY dependency is automatic in PostgreSQL: dropping the owning
// column also drops the sequence, even under RESTRICT. A normal dependency
// on that sequence (represented here by a stored DEFAULT expression rather
// than pg_attrdef) still blocks RESTRICT and is removed only for CASCADE.
static bool dropOwnedSequencesForColumn(
    CatalogManager& catalog, const std::string& dbname,
    const std::string& physicalTableName,
    const std::string& logicalTableName, Oid tableOid,
    int32_t columnNumber, const std::string& columnName, bool cascade,
    std::set<std::string>& droppedSequenceStorageNames) {
    struct Target {
        Oid oid = INVALID_OID;
        std::string storageName;
        std::vector<std::pair<std::string, std::string>> defaultDependencies;
        CatalogManager::DropPlan catalogPlan;
    };

    std::vector<Target> targets;
    std::set<Oid> targetOids;
    std::set<std::string> targetStorageNames;
    for (const auto& dependency :
         catalog.findRefs(PgClassOid_Class, tableOid, -1)) {
        if (dependency.classid != PgClassOid_Class ||
            dependency.objsubid != 0 || dependency.deptype != 'a') {
            continue;
        }
        if (dependency.refobjsubid > 0 &&
            dependency.refobjsubid != columnNumber) {
            continue;
        }

        const PgClassRow* dependent = catalog.findClass(dependency.objid);
        if (!dependent || dependent->relkind != 'S' ||
            targetOids.count(dependent->oid) != 0) {
            continue;
        }

        std::string storageName;
        std::string error;
        if (!catalogSequenceStorageName(
                catalog, *dependent, storageName, error)) {
            std::cout << "ALTER TABLE DROP COLUMN owned sequence planning failed: "
                      << error << std::endl;
            return false;
        }
        SequenceInfo sequenceInfo;
        if (g_engine.getSequenceInfo(
                dbname, storageName, sequenceInfo) != DBStatus::OK) {
            std::cout << "ALTER TABLE DROP COLUMN cannot read owned sequence "
                      << storageName << std::endl;
            return false;
        }

        // Current dependencies carry the owning attribute number. A zero
        // sub-id is accepted only for genuine legacy ownership corroborated
        // by the sequence file; old releases also emitted zero rows for
        // unrelated DEFAULT nextval() references.
        if (dependency.refobjsubid == 0 &&
            (sequenceInfo.ownedByTable.empty() ||
             canonicalOwnedTableName(sequenceInfo.ownedByTable) !=
                 canonicalOwnedTableName(logicalTableName) ||
             sequenceInfo.ownedByColumn != columnName)) {
            continue;
        }

        Target target;
        target.oid = dependent->oid;
        target.storageName = storageName;
        for (const auto& defaultDependency :
             findDefaultNextvalDeps(dbname, storageName)) {
            if (defaultDependency.first == physicalTableName &&
                defaultDependency.second == columnName) {
                continue;
            }
            target.defaultDependencies.push_back(defaultDependency);
        }
        if (!cascade && !target.defaultDependencies.empty()) {
            const auto& blocker = target.defaultDependencies.front();
            std::cout << "ERROR: cannot drop column " << columnName
                      << " because default on " << blocker.first << "."
                      << blocker.second << " depends on owned sequence "
                      << storageName << std::endl;
            return false;
        }
        target.catalogPlan = catalog.planDrop(
            PgClassOid_Class, target.oid,
            cascade ? CatalogManager::DropBehavior::Cascade
                    : CatalogManager::DropBehavior::Restrict);
        if (!target.catalogPlan.ok()) {
            std::cout << "ERROR: " << target.catalogPlan.error << std::endl;
            return false;
        }
        targetOids.insert(target.oid);
        targetStorageNames.insert(target.storageName);
        targets.push_back(std::move(target));
    }

    if (targets.empty()) return true;

    CatalogManager::DropPlan combinedCatalogPlan;
    std::set<std::pair<Oid, Oid>> plannedCatalogObjects;
    std::vector<PhysicalCascadeAction> physicalActions;
    for (const auto& target : targets) {
        for (const auto& object : target.catalogPlan.objectsToDrop) {
            if (plannedCatalogObjects.insert(object).second) {
                combinedCatalogPlan.objectsToDrop.push_back(object);
            }
        }

        std::vector<PhysicalCascadeAction> targetActions;
        std::string error;
        if (!buildPhysicalCascadeActions(
                catalog, g_engine, dbname, target.oid,
                target.catalogPlan, targetActions, error)) {
            std::cout << "ALTER TABLE DROP COLUMN dependency planning failed: "
                      << error << std::endl;
            return false;
        }
        for (auto& action : targetActions) {
            if (action.kind == PhysicalCascadeAction::Kind::Sequence &&
                targetStorageNames.count(action.name) != 0) {
                continue;
            }
            const auto duplicate = std::find_if(
                physicalActions.begin(), physicalActions.end(),
                [&](const PhysicalCascadeAction& existing) {
                    return existing.kind == action.kind &&
                           existing.name == action.name &&
                           existing.tableName == action.tableName &&
                           existing.accessMethod == action.accessMethod &&
                           existing.key == action.key;
                });
            if (duplicate == physicalActions.end()) {
                physicalActions.push_back(std::move(action));
            }
        }
    }

    std::set<std::pair<std::string, std::string>> clearedDefaults;
    std::set<std::string> changedDefaultTables;
    for (const auto& target : targets) {
        for (const auto& dependency : target.defaultDependencies) {
            if (!clearedDefaults.insert(dependency).second) continue;
            if (g_engine.alterTableDropDefault(
                    dbname, dependency.first,
                    dependency.second) != DBStatus::OK) {
                std::cout << "ALTER TABLE DROP COLUMN failed to clear default on "
                          << dependency.first << "." << dependency.second
                          << std::endl;
                return false;
            }
            changedDefaultTables.insert(dependency.first);
        }
    }
    for (const auto& changedTable : changedDefaultTables) {
        // The caller still needs the pre-drop pg_attribute rows in order to
        // compact dependency/description sub-ids. Its final synchronization
        // will publish both the compacted schema and this default change.
        if (changedTable == physicalTableName) continue;
        if (!synchronizeTableAttributesInCatalog(dbname, changedTable)) {
            std::cout << "ALTER TABLE DROP COLUMN default catalog update failed for "
                      << changedTable << std::endl;
            return false;
        }
    }

    for (const auto& action : physicalActions) {
        if (!dropPhysicalCascadeAction(g_engine, dbname, action)) {
            std::cout << "ALTER TABLE DROP COLUMN dependency cleanup failed for "
                      << action.name << std::endl;
            return false;
        }
        if (action.kind == PhysicalCascadeAction::Kind::Sequence) {
            droppedSequenceStorageNames.insert(action.name);
        }
    }
    for (const auto& target : targets) {
        if (g_engine.dropSequence(
                dbname, target.storageName) != DBStatus::OK) {
            std::cout << "ALTER TABLE DROP COLUMN failed to drop owned sequence "
                      << target.storageName << std::endl;
            return false;
        }
        droppedSequenceStorageNames.insert(target.storageName);
    }

    std::string catalogError;
    if (!catalog.applyDropPlan(combinedCatalogPlan, &catalogError)) {
        std::cout << "ALTER TABLE DROP COLUMN sequence catalog cleanup failed: "
                  << catalogError << std::endl;
        return false;
    }
    if (!catalog.persistAll()) {
        std::cout << "ALTER TABLE DROP COLUMN sequence catalog persistence failed"
                  << std::endl;
        return false;
    }
    return true;
}

bool DdlExecutor::executeDropTable(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP TABLE name" << std::endl;
        return true;
    }
    if (stmt->objectNames.size() != 1) {
        std::cout << "DROP TABLE with multiple targets is not supported"
                  << std::endl;
        return true;
    }
    const std::string logicalName = stmt->objectNames.front();
    const bool droppingTemp = s.tempTables.count(logicalName) != 0;
    std::string tname = resolveTableName(s, logicalName);
    if (!g_engine.tableExists(s.currentDB, tname)) {
        if (stmt->ifExists) {
            std::cout << "NOTICE: table \"" << tname << "\" does not exist, skipping" << std::endl;
            return false;
        }
        // PG: DROP TABLE on a missing table reports
        // table "x" does not exist (SQLSTATE 42P01 via the wire
        // layer's relation mapping).
        std::cout << "ERROR:  table \"" << tname << "\" does not exist" << std::endl;
        return true;
    }

    const auto directInheritanceChildren =
        g_engine.getInheritedChildren(s.currentDB, tname);
    if (!directInheritanceChildren.empty()) {
        if (stmt->cascade) {
            // The catalog does not yet model inheritance dependencies in a
            // drop plan.  Never detach surviving descendants silently under
            // a command that PostgreSQL defines as recursively destructive.
            std::cout
                << "ERROR: DROP TABLE CASCADE for an inheritance hierarchy "
                   "is not supported (SQLSTATE 0A000)"
                << std::endl;
        } else {
            std::cout << "ERROR: cannot drop table \"" << logicalName
                      << "\" because table \""
                      << directInheritanceChildren.front()
                      << "\" inherits from it" << std::endl;
        }
        return true;
    }

    std::vector<std::string> inheritanceParents;
    for (const auto& candidate :
         g_engine.getTableNames(s.currentDB)) {
        if (candidate == tname) continue;
        const auto children =
            g_engine.getInheritedChildren(s.currentDB, candidate);
        if (std::find(children.begin(), children.end(), tname) !=
            children.end()) {
            inheritanceParents.push_back(candidate);
        }
    }

    // Build the catalog-side CASCADE/RESTRICT plan before removing physical
    // storage. The legacy sequence-dependency migration below is the only
    // pre-plan mutation; it marks the snapshot dirty so a rejected plan
    // restores both disk and the live CatalogManager.
    CatalogManager* catalogManager = nullptr;
    CatalogManager::DropPlan catalogDropPlan;
    bool hasCatalogDropPlan = false;
    Oid catalogRootOid = INVALID_OID;
    const auto catalogQualifiedName = CatalogService::logicalName(tname);
    const std::string catalogLogicalName = catalogQualifiedName.schema.empty()
        ? catalogQualifiedName.name
        : (catalogQualifiedName.schema + "." + catalogQualifiedName.name);
    try {
        CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        catalogManager = &cat;
        const PgClassRow* cls = cat.resolveRelation(catalogLogicalName, {"public"});
        if (cls) {
            catalogRootOid = cls->oid;
            // Releases before this fix recorded an ordinary DEFAULT
            // nextval() as an auto-owned sequence dependency with a zero
            // referenced column. Distinguish those rows from genuine legacy
            // OWNED BY metadata by reading the sequence file before planning
            // the table drop, so an upgrade cannot silently delete an
            // independent sequence.
            const auto incomingDependencies = cat.findRefs(
                PgClassOid_Class, catalogRootOid, -1);
            for (const auto& dependency : incomingDependencies) {
                if (dependency.classid != PgClassOid_Class ||
                    dependency.objsubid != 0 ||
                    dependency.refobjsubid != 0 ||
                    dependency.deptype != 'a') {
                    continue;
                }
                const PgClassRow* dependent =
                    cat.findClass(dependency.objid);
                if (!dependent || dependent->relkind != 'S') continue;

                std::string sequenceStorageName;
                std::string sequenceNameError;
                if (!catalogSequenceStorageName(
                        cat, *dependent, sequenceStorageName,
                        sequenceNameError)) {
                    std::cout << "DROP TABLE dependency migration failed: "
                              << sequenceNameError << std::endl;
                    return true;
                }
                SequenceInfo sequenceInfo;
                if (g_engine.getSequenceInfo(
                        s.currentDB, sequenceStorageName,
                        sequenceInfo) != DBStatus::OK) {
                    std::cout << "DROP TABLE dependency migration failed: cannot read sequence "
                              << sequenceStorageName << std::endl;
                    return true;
                }
                auto canonicalPublicName = [](const std::string& name) {
                    return name.rfind("public.", 0) == 0
                        ? name.substr(7) : name;
                };
                if (!sequenceInfo.ownedByTable.empty() &&
                    canonicalPublicName(sequenceInfo.ownedByTable) ==
                        canonicalPublicName(catalogLogicalName)) {
                    continue;
                }
                txn.markSnapshotDirty();
                if (!cat.removeDepend(
                        dependency.classid, dependency.objid,
                        dependency.objsubid, dependency.refclassid,
                        dependency.refobjid,
                        dependency.refobjsubid)) {
                    std::cout << "DROP TABLE dependency migration failed"
                              << std::endl;
                    return true;
                }
            }
            auto behavior = stmt->cascade
                                ? CatalogManager::DropBehavior::Cascade
                                : CatalogManager::DropBehavior::Restrict;
            catalogDropPlan = cat.planDrop(PgClassOid_Class, cls->oid, behavior);
            if (!catalogDropPlan.ok()) {
                std::cout << "ERROR: " << catalogDropPlan.error << std::endl;
                return true;
            }
            hasCatalogDropPlan = true;
        } else {
            std::cout << "NOTICE: table \"" << tname
                      << "\" has no catalog entry; falling back to storage drop" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog drop check failed: " << e.what() << std::endl;
    }

    std::vector<PhysicalCascadeAction> physicalCascadeActions;
    if (hasCatalogDropPlan && catalogManager) {
        std::string error;
        if (!buildPhysicalCascadeActions(*catalogManager, g_engine, s.currentDB,
                                          catalogRootOid,
                                          catalogDropPlan, physicalCascadeActions, error)) {
            std::cout << "DROP TABLE dependency planning failed: " << error
                      << std::endl;
            return true;
        }
    }
    std::set<std::string> droppedSequenceStorageNames;
    for (const auto& action : physicalCascadeActions) {
        if (action.kind == PhysicalCascadeAction::Kind::Sequence) {
            droppedSequenceStorageNames.insert(action.name);
        }
    }

    // Defaults are stored in table metadata rather than as pg_attrdef catalog
    // objects, so the catalog plan cannot see their normal dependency on an
    // automatically dropped sequence. Defaults in tables that are themselves
    // being dropped need no action; every surviving reference blocks
    // RESTRICT and is cleared by CASCADE.
    std::set<std::string> droppedTableStorageNames{tname};
    for (const auto& action : physicalCascadeActions) {
        if (action.kind == PhysicalCascadeAction::Kind::Table) {
            droppedTableStorageNames.insert(action.name);
        }
    }
    std::set<std::pair<std::string, std::string>> defaultsToClear;
    for (const auto& sequenceName : droppedSequenceStorageNames) {
        for (const auto& dependency :
             findDefaultNextvalDeps(s.currentDB, sequenceName)) {
            if (droppedTableStorageNames.count(dependency.first) != 0) {
                continue;
            }
            if (!stmt->cascade) {
                std::cout << "ERROR: cannot drop table " << logicalName
                          << " because default on " << dependency.first
                          << "." << dependency.second
                          << " depends on owned sequence " << sequenceName
                          << std::endl;
                return true;
            }
            defaultsToClear.insert(dependency);
        }
    }

    txn.markSnapshotDirty();
    std::set<std::string> changedDefaultTables;
    for (const auto& dependency : defaultsToClear) {
        if (g_engine.alterTableDropDefault(
                s.currentDB, dependency.first,
                dependency.second) != DBStatus::OK) {
            std::cout << "DROP TABLE failed to clear default on "
                      << dependency.first << "." << dependency.second
                      << std::endl;
            return true;
        }
        changedDefaultTables.insert(dependency.first);
    }
    for (const auto& changedTable : changedDefaultTables) {
        if (!synchronizeTableAttributesInCatalog(
                s.currentDB, changedTable)) {
            std::cout << "DROP TABLE default catalog update failed for "
                      << changedTable << std::endl;
            return true;
        }
    }
    for (const auto& action : physicalCascadeActions) {
        if (!dropPhysicalCascadeAction(g_engine, s.currentDB, action)) {
            std::cout << "DROP TABLE dependency cleanup failed for "
                      << action.name << std::endl;
            return true;
        }
    }
    DBStatus res = g_engine.dropTable(s.currentDB, tname);
    if (res != DBStatus::OK) {
        std::cout << "DROP TABLE failed" << std::endl;
        return true;
    }
    bool catalogChanged = false;
    if (catalogManager) {
        if (hasCatalogDropPlan) {
            std::string err;
            if (!catalogManager->applyDropPlan(catalogDropPlan, &err)) {
                std::cout << "DROP TABLE catalog cleanup failed: " << err << std::endl;
                return true;
            }
            catalogChanged = true;
        }
        for (const auto& parent : inheritanceParents) {
            if (!updateTableHierarchyFlagsInCatalog(
                    *catalogManager, s.currentDB, parent)) {
                std::cout << "DROP TABLE inheritance catalog update failed"
                          << std::endl;
                return true;
            }
            catalogChanged = true;
        }
        if (catalogChanged && !catalogManager->persistAll()) {
            std::cout << "DROP TABLE catalog persistence failed" << std::endl;
            return true;
        }
    }
    txn.recordDrop(DdlObjectKind::Table, tname);
    for (const auto& sequenceName : droppedSequenceStorageNames) {
        txn.recordDrop(DdlObjectKind::Sequence, sequenceName);
    }
    if (!txn.commit()) return true;
    for (const auto& sequenceName : droppedSequenceStorageNames) {
        s.sequenceLastValues.erase(sequenceName);
        if (sequenceName.find('.') == std::string::npos) {
            s.sequenceLastValues.erase("public." + sequenceName);
        }
    }
    if (droppingTemp) {
        s.tempTables.erase(logicalName);
        s.tempTableOnCommit.erase(logicalName);
        s.tempTablesCreatedInTransaction.erase(logicalName);
    }
    std::cout << "DROP TABLE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE INDEX
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateIndex(const CreateIndexStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!stmt->compatibilityShortcut.empty() &&
        !isExtendedCompatMode(s.compatibilityMode)) {
        const std::string canonical = stmt->compatibilityShortcut == "hash"
            ? "CREATE INDEX ... USING hash"
            : "CREATE INDEX ... USING gin (to_tsvector(col))";
        std::cout << "SQL syntax error: CREATE "
                  << (stmt->compatibilityShortcut == "hash" ? "HASH" : "FULLTEXT")
                  << " INDEX is not PostgreSQL syntax; use " << canonical
                  << " (SQLSTATE 42601)" << std::endl;
        return true;
    }
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    // Index creation publishes physical sidecars and catalog rows. Protect
    // the whole operation with the current-format snapshot boundary so an
    // outer ROLLBACK cannot leave one half of the index behind.
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }
    if (stmt->columns.empty()) {
        std::cout << "SQL syntax error: CREATE INDEX requires an index key" << std::endl;
        return true;
    }

    std::string tname = resolveTableName(s, stmt->tableName);
    if (!g_engine.tableExists(s.currentDB, tname)) {
        std::cout << "Table " << tname << " not found" << std::endl;
        return true;
    }

    txn.markSnapshotDirty();

    const std::vector<std::string> colnames = [&]() {
        std::vector<std::string> names;
        for (const auto& elem : stmt->columns) names.push_back(elem.column);
        return names;
    }();

    // PostgreSQL's unnamed index syntax (CREATE INDEX ON tbl (col)) derives
    // the index name from table and key columns. Generate the same shape and
    // append a numeric suffix when the derived name is already taken.
    auto namedIndexExists = [&](const std::string& candidate) {
        if (g_engine.getNamedIndex(s.currentDB, tname, candidate).has_value()) return true;
        try {
            const auto tableName = CatalogService::logicalName(tname);
            const std::string schema = tableName.schema.empty() ? "public" : tableName.schema;
            auto& catalog = g_engine.catalogService().get(s.currentDB);
            const auto* ns = catalog.findNamespaceByName(schema);
            return ns != nullptr && catalog.findClassByName(candidate, ns->oid) != nullptr;
        } catch (...) {
            std::cout << "CREATE INDEX failed: cannot inspect catalog metadata" << std::endl;
            std::string dummy;
            return true; // fail-closed on catalog errors
        }
    };
    std::string idxName = stmt->indexName;
    if (idxName.empty()) {
        std::string base = tname;
        for (const auto& cname : colnames) {
            if (!cname.empty()) base += "_" + cname;
        }
        base += "_idx";
        idxName = base;
        for (unsigned long suffix = 1; namedIndexExists(idxName); ++suffix) {
            idxName = base + std::to_string(suffix);
        }
    }
    if (namedIndexExists(idxName)) {
        if (stmt->ifNotExists) {
            std::cout << "NOTICE: index \"" << idxName << "\" already exists, skipping" << std::endl;
            return false;
        }
        std::cout << "ERROR: relation \"" << idxName
                  << "\" already exists (SQLSTATE 42P07)" << std::endl;
        return true;
    }

    std::string whereCondition = stmt->whereClause ? stmt->whereClause->toString() : "";
    std::vector<std::string> includeCols = stmt->includeCols;

    DBStatus res;
    std::string am = toLower(stmt->accessMethod);
    if (stmt->unique) {
        const bool unsupportedMethod = !am.empty() && am != "btree";
        const bool unsupportedShape = stmt->concurrently ||
            stmt->nullsNotDistinct ||
            stmt->whereClause != nullptr ||
            std::any_of(
                stmt->columns.begin(), stmt->columns.end(),
                [](const IndexElem& element) {
                    return element.column.empty() || element.expr != nullptr ||
                           !element.collation.empty() ||
                           !element.opclass.empty();
                });
        if (unsupportedMethod || unsupportedShape) {
            std::cout << "CREATE UNIQUE INDEX currently requires plain "
                         "B-tree columns without WHERE, COLLATE, operator "
                         "classes, expressions, NULLS NOT DISTINCT, or "
                         "CONCURRENTLY"
                      << std::endl;
            return true;
        }
    }
    if (am.empty() || am == "btree") {
        if (colnames.size() == 1) {
            res = g_engine.createIndex(s.currentDB, tname, colnames.front(), true,
                                       includeCols, whereCondition, "",
                                       stmt->concurrently, stmt->unique);
        } else {
            res = g_engine.createCompositeIndex(s.currentDB, tname, colnames,
                                                idxName, includeCols,
                                                whereCondition,
                                                stmt->concurrently,
                                                stmt->unique);
        }
    } else if (am == "hash") {
        if (colnames.size() == 1) {
            res = g_engine.createHashIndex(s.currentDB, tname, colnames.front());
        } else {
            std::cout << "HASH index only supports single column" << std::endl;
            return true;
        }
    } else if (am == "fulltext") {
        if (stmt->unique || stmt->concurrently || colnames.size() != 1 ||
            colnames.front().empty() || stmt->columns.front().expr) {
            std::cout << "FULLTEXT index requires one plain non-unique column"
                      << std::endl;
            return true;
        }
        res = g_engine.createFullTextIndex(s.currentDB, tname,
                                           colnames.front());
    } else if (am == "bloom") {
        if (colnames.size() == 1) {
            res = g_engine.createBloomIndex(s.currentDB, tname, colnames.front());
        } else {
            std::cout << "BLOOM index only supports single column" << std::endl;
            return true;
        }
    } else if (am == "gin" || am == "gist" || am == "brin" || am == "spgist") {
        if (stmt->unique || colnames.size() != 1 || colnames.front().empty()) {
            std::cout << "CREATE INDEX access method requires one non-unique column" << std::endl;
            return true;
        }
        const auto& colname = colnames.front();
        if (am == "gin") res = g_engine.createGinIndex(s.currentDB, tname, colname);
        else if (am == "gist") res = g_engine.createGiSTIndex(s.currentDB, tname, colname);
        else if (am == "brin") res = g_engine.createBrinIndex(s.currentDB, tname, colname);
        else res = g_engine.createSPGiSTIndex(s.currentDB, tname, colname);
    } else {
        std::cout << "Unsupported index access method: " << am << std::endl;
        return true;
    }

    if (res != DBStatus::OK) {
        std::cout << "CREATE INDEX failed" << std::endl;
        return true;
    }
    const std::string physicalMethod = (colnames.size() > 1)
        ? "composite" : (am.empty() ? "btree" : am);
    const std::string physicalKey = (colnames.size() > 1)
        ? idxName : (stmt->columns.front().expr ? stmt->columns.front().expr->toString() : colnames.front());
    auto discardCreatedIndex = [&]() {
        if (physicalMethod == "composite") g_engine.dropCompositeIndex(s.currentDB, tname, idxName);
        else if (physicalMethod == "hash") g_engine.dropHashIndex(s.currentDB, tname, physicalKey);
        else if (physicalMethod == "bloom") g_engine.dropBloomIndex(s.currentDB, tname, physicalKey);
        else if (physicalMethod == "gin") g_engine.dropGinIndex(s.currentDB, tname, physicalKey);
        else if (physicalMethod == "gist") g_engine.dropGiSTIndex(s.currentDB, tname, physicalKey);
        else if (physicalMethod == "brin") g_engine.dropBrinIndex(s.currentDB, tname, physicalKey);
        else if (physicalMethod == "spgist") g_engine.dropSPGiSTIndex(s.currentDB, tname, physicalKey);
        else if (physicalMethod == "fulltext") g_engine.dropFullTextIndex(s.currentDB, tname, physicalKey);
        else g_engine.dropIndex(s.currentDB, tname, physicalKey);
    };
    if (!g_engine.registerIndexName(s.currentDB, tname, idxName, physicalMethod, physicalKey)) {
        discardCreatedIndex();
        std::cout << "CREATE INDEX failed: cannot persist index metadata" << std::endl;
        return true;
    }

    bool catalogRegistered = false;
    Oid catalogIndexOid = INVALID_OID;
    CatalogManager* catalogForCleanup = nullptr;
    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        catalogForCleanup = &cat;
        auto qn = CatalogService::logicalName(tname);
        const auto* ns = cat.findNamespaceByName(qn.schema.empty() ? "public" : qn.schema);
        const auto* tbl = (ns ? cat.resolveRelation(qn.name, {qn.schema.empty() ? "public" : qn.schema}) : nullptr);
        if (!ns || !tbl) {
            throw std::runtime_error("table has no catalog entry");
        }
        if (cat.findClassByName(idxName, ns->oid) != nullptr) {
            throw std::runtime_error("index name already exists");
        }
        {
            // createClass() may grow CatalogManager's backing vector and
            // invalidate pointers returned by resolveRelation(). Copy the
            // referenced OID before mutating the catalog.
            const Oid tableOid = tbl->oid;
            PgClassRow idx;
            idx.relname = idxName;
            idx.relnamespace = ns->oid;
            idx.relkind = 'i';
            idx.relnatts = static_cast<int16_t>(colnames.size());
            Oid idxOid = cat.createClass(idx);
            catalogIndexOid = idxOid;

            PgDependRow dep;
            dep.classid = PgClassOid_Class;
            dep.objid = idxOid;
            dep.objsubid = 0;
            dep.refclassid = PgClassOid_Class;
            dep.refobjid = tableOid;
            dep.refobjsubid = 0;
            dep.deptype = 'a';
            cat.addDepend(dep);

            const auto* currentTable = cat.findClass(tableOid);
            if (!currentTable) {
                throw std::runtime_error("indexed table catalog row disappeared");
            }
            PgClassRow updatedTable = *currentTable;
            updatedTable.relhasindex = true;
            if (!cat.updateClass(tableOid, updatedTable)) {
                throw std::runtime_error("cannot update indexed table catalog row");
            }
            if (!cat.persistAll()) {
                throw std::runtime_error("cannot persist index catalog");
            }
            catalogRegistered = true;
        }
    } catch (const std::exception& e) {
        std::cerr << "ERROR: catalog index registration failed: " << e.what() << std::endl;
    }
    if (!catalogRegistered) {
        if (catalogForCleanup && catalogIndexOid != INVALID_OID) {
            std::string error;
            if (!catalogForCleanup->dropObject(PgClassOid_Class, catalogIndexOid,
                                               CatalogManager::DropBehavior::Cascade,
                                               &error)) {
                std::cerr << "ERROR: failed to roll back catalog index: " << error << std::endl;
            }
        }
        discardCreatedIndex();
        if (!g_engine.unregisterIndexName(s.currentDB, tname, idxName)) {
            std::cerr << "ERROR: failed to remove index name mapping after catalog failure" << std::endl;
        }
        std::cout << "CREATE INDEX failed: cannot persist catalog metadata" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Index, idxName, tname);
    if (!txn.commit()) return true;
    std::cout << "CREATE INDEX succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropIndex(const DropStmt* stmt, Session& s) {
    if (!stmt) return true;
    if (!stmt->compatibilityShortcut.empty() &&
        !isExtendedCompatMode(s.compatibilityMode)) {
        std::cout << "SQL syntax error: DROP FULLTEXT INDEX is not PostgreSQL "
                     "syntax; use DROP INDEX (SQLSTATE 42601)" << std::endl;
        return true;
    }
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP INDEX name" << std::endl;
        return true;
    }

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
    bool catalogChanged = false;
    for (const auto& rawName : stmt->objectNames) {
        CatalogManager::QualifiedName indexQn;
        if (!CatalogManager::parseQualifiedName(rawName, indexQn)) {
            std::cout << "SQL syntax error: invalid index name" << std::endl;
            return true;
        }
        const std::string indexName = indexQn.name;
        const std::string schema = indexQn.schema.empty() ? "public" : indexQn.schema;
        const PgClassRow* indexRel = cat.resolveRelation(rawName, {schema});
        std::string tableName;
        std::optional<StorageEngine::NamedIndexInfo> named;
        Oid indexOid = INVALID_OID;
        Oid tableOid = INVALID_OID;

        if (indexRel && indexRel->relkind == 'i') {
            indexOid = indexRel->oid;
            auto deps = cat.findDepends(PgClassOid_Class, indexOid);
            for (const auto& dep : deps) {
                if (dep.refclassid != PgClassOid_Class) continue;
                const PgClassRow* ref = cat.findClass(dep.refobjid);
                if (ref && ref->relkind == 'r') {
                    const auto* tableNamespace =
                        cat.findNamespace(ref->relnamespace);
                    if (!tableNamespace) {
                        std::cout << "DROP INDEX table namespace lookup failed"
                                  << std::endl;
                        return true;
                    }
                    tableName = tableNamespace->nspname == "public"
                        ? ref->relname
                        : tableNamespace->nspname + "__" + ref->relname;
                    tableOid = ref->oid;
                    break;
                }
            }
        }

        if (!stmt->tableName.empty()) {
            const std::string requestedTable = resolveTableName(s, stmt->tableName);
            if (!tableName.empty() && tableName != requestedTable) {
                std::cout << "Index " << indexName << " is not on table " << requestedTable << std::endl;
                return true;
            }
            tableName = requestedTable;
        }

        if (tableName.empty()) {
            // Catalogs created before the name map may still be usable when
            // the caller supplies ON table; standard name-only syntax is
            // deliberately fail-closed if ownership cannot be proven.
            for (const auto& candidate : g_engine.getTableNames(s.currentDB)) {
                if (g_engine.getNamedIndex(s.currentDB, candidate, indexName)) {
                    tableName = candidate;
                    break;
                }
            }
        }

        if (tableName.empty() || !g_engine.tableExists(s.currentDB, tableName)) {
            if (stmt->ifExists) {
                std::cout << "NOTICE: index \"" << indexName << "\" does not exist, skipping" << std::endl;
                continue;
            }
            std::cout << "ERROR: index \"" << indexName
                      << "\" does not exist (SQLSTATE 42704)"
                      << std::endl;
            return true;
        }

        named = g_engine.getNamedIndex(s.currentDB, tableName, indexName);
        std::string method;
        std::string key;
        if (named) {
            method = named->accessMethod;
            key = named->key;
        } else if (!g_engine.getCompositeIndexes(s.currentDB, tableName).empty()) {
            for (const auto& composite : g_engine.getCompositeIndexes(s.currentDB, tableName)) {
                if (composite.name == indexName) { method = "composite"; key = indexName; break; }
            }
        }
        if (method.empty()) {
            // Transitional fallback for an index created by the old runtime:
            // its physical name was the indexed column rather than SQL name.
            for (const auto& col : g_engine.getIndexedColumns(s.currentDB, tableName)) {
                if (col == indexName) { method = "btree"; key = col; break; }
            }
            for (const auto& col : g_engine.getHashIndexedColumns(s.currentDB, tableName)) {
                if (col == indexName) { method = "hash"; key = col; break; }
            }
            for (const auto& col : g_engine.getGinIndexedColumns(s.currentDB, tableName)) {
                if (col == indexName) { method = "gin"; key = col; break; }
            }
            for (const auto& col : g_engine.getGiSTIndexedColumns(s.currentDB, tableName)) {
                if (col == indexName) { method = "gist"; key = col; break; }
            }
            for (const auto& col : g_engine.getBrinIndexedColumns(s.currentDB, tableName)) {
                if (col == indexName) { method = "brin"; key = col; break; }
            }
            for (const auto& col : g_engine.getSPGiSTIndexedColumns(s.currentDB, tableName)) {
                if (col == indexName) { method = "spgist"; key = col; break; }
            }
        }
        if (method.empty()) {
            if (stmt->ifExists) {
                std::cout << "NOTICE: index \"" << indexName << "\" does not exist, skipping" << std::endl;
                continue;
            }
            std::cout << "ERROR: index \"" << indexName
                      << "\" does not exist (SQLSTATE 42704)"
                      << std::endl;
            return true;
        }

        txn.markSnapshotDirty();
        DBStatus status = g_engine.dropIndexByAccessMethod(s.currentDB, tableName,
                                                           key, method);
        if (status != DBStatus::OK) {
            std::cout << "DROP INDEX failed" << std::endl;
            return true;
        }
        if (named && !g_engine.unregisterIndexName(s.currentDB, tableName, indexName)) {
            std::cout << "DROP INDEX metadata cleanup failed" << std::endl;
            return true;
        }

        if (indexOid != INVALID_OID) {
            auto behavior = stmt->cascade ? CatalogManager::DropBehavior::Cascade
                                           : CatalogManager::DropBehavior::Restrict;
            std::string error;
            if (!cat.dropObject(PgClassOid_Class, indexOid, behavior, &error)) {
                std::cout << "ERROR: " << error << std::endl;
                return true;
            }
            catalogChanged = true;
        }
        if (tableOid == INVALID_OID) {
            const auto tableQn = CatalogService::logicalName(tableName);
            const std::string tableSchema = tableQn.schema.empty()
                ? "public" : tableQn.schema;
            const auto* tableRelation = cat.resolveRelation(
                tableQn.name, {tableSchema});
            if (tableRelation && tableRelation->relkind == 'r') {
                tableOid = tableRelation->oid;
            }
        }
        if (tableOid != INVALID_OID) {
            const bool hasRemainingIndex =
                storageTableHasIndex(s.currentDB, tableName);
            const auto* tableRelation = cat.findClass(tableOid);
            if (!tableRelation) {
                std::cout << "DROP INDEX table catalog update failed"
                          << std::endl;
                return true;
            }
            PgClassRow updatedTable = *tableRelation;
            updatedTable.relhasindex = hasRemainingIndex;
            if (!cat.updateClass(tableOid, updatedTable)) {
                std::cout << "DROP INDEX table catalog update failed"
                          << std::endl;
                return true;
            }
            catalogChanged = true;
        }
        txn.recordDrop(DdlObjectKind::Index, indexName, tableName);
    }
    if (catalogChanged && !cat.persistAll()) {
        std::cout << "DROP INDEX catalog persistence failed" << std::endl;
        return true;
    }
    if (!txn.commit()) return true;
    std::cout << "DROP INDEX succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP SEQUENCE
// ----------------------------------------------------------------------------

static std::vector<std::string> effectiveSequenceSearchPath(
    const Session& session) {
    std::vector<std::string> searchPath;
    std::string canonical;
    if (!dbms::parseSessionSearchPath(
            session.searchPath, searchPath, canonical)) {
        return {"public"};
    }
    for (auto& schema : searchPath) {
        schema = dbms::expandSessionSearchPathEntry(
            schema, session.username);
    }
    return searchPath.empty() ? std::vector<std::string>{"public"}
                              : searchPath;
}

static std::string schemaNameForRelation(
    CatalogManager& catalog, const PgClassRow* relation) {
    if (!relation) return {};
    const PgNamespaceRow* relationNamespace =
        catalog.findNamespace(relation->relnamespace);
    return relationNamespace ? relationNamespace->nspname : std::string{};
}

bool DdlExecutor::executeCreateSequence(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    const std::string requestedName = stmt->schema.empty()
        ? stmt->objectName : stmt->schema + "." + stmt->objectName;
    CatalogManager::QualifiedName sequenceName;
    if (!CatalogManager::parseQualifiedName(
            requestedName, sequenceName)) {
        std::cout << "CREATE SEQUENCE has an invalid name" << std::endl;
        return true;
    }
    std::string sequenceSchema = sequenceName.schema;
    if (sequenceSchema.empty()) {
        for (const auto& candidate : effectiveSequenceSearchPath(s)) {
            if (candidate == "pg_catalog" || candidate == "pg_temp" ||
                candidate.rfind("pg_temp_", 0) == 0) {
                continue;
            }
            if (g_engine.schemaExists(s.currentDB, candidate)) {
                sequenceSchema = candidate;
                break;
            }
        }
        if (sequenceSchema.empty()) {
            std::cout << "ERROR: no schema has been selected to create in"
                      << std::endl;
            return true;
        }
    }
    const std::string seqname = sequenceSchema == "public"
        ? sequenceName.name
        : sequenceSchema + "." + sequenceName.name;
    dbms::SequenceInfo info;
    auto opt = stmt->options.find("start");
    if (opt != stmt->options.end()) {
        if (!parseInt64Strict(opt->second, info.start)) return true;
        info.startSpecified = true;
    }
    opt = stmt->options.find("increment");
    if (opt != stmt->options.end()) {
        if (!parseInt64Strict(opt->second, info.increment)) return true;
        info.incrementSpecified = true;
    }
    opt = stmt->options.find("minvalue");
    if (opt != stmt->options.end()) {
        if (!parseInt64Strict(opt->second, info.minValue)) return true;
        info.hasMinValue = true;
    }
    opt = stmt->options.find("maxvalue");
    if (opt != stmt->options.end()) {
        if (!parseInt64Strict(opt->second, info.maxValue)) return true;
        info.hasMaxValue = true;
    }
    opt = stmt->options.find("cache");
    if (opt != stmt->options.end()) {
        if (!parseInt64Strict(opt->second, info.cache)) return true;
        info.cacheSpecified = true;
    }
    opt = stmt->options.find("cycle");
    if (opt != stmt->options.end()) {
        info.cycleSpecified = true;
        info.cycle = (opt->second == "yes");
    }
    opt = stmt->options.find("nominvalue");
    if (opt != stmt->options.end()) {
        info.noMinValue = true;
        info.hasMinValue = false;
    }
    opt = stmt->options.find("nomaxvalue");
    if (opt != stmt->options.end()) {
        info.noMaxValue = true;
        info.hasMaxValue = false;
    }
    opt = stmt->options.find("ownedby");
    if (opt != stmt->options.end()) {
        info.ownedBySpecified = true;
        std::string owner = opt->second;
        if (owner == "none") {
            info.ownedByTable.clear();
            info.ownedByColumn.clear();
        } else {
            size_t first = owner.find('.');
            size_t last = owner.rfind('.');
            if (first != std::string::npos && last != first) {
                // schema.table.column
                info.ownedByTable = owner.substr(0, last);
                info.ownedByColumn = owner.substr(last + 1);
            } else if (first != std::string::npos) {
                info.ownedByTable = owner.substr(0, first);
                info.ownedByColumn = owner.substr(first + 1);
            } else {
                info.ownedByTable = owner;
            }
        }
    }

    CatalogManager* sequenceCatalog = nullptr;
    Oid sequenceNamespaceOid = INVALID_OID;
    Oid ownedTableOid = INVALID_OID;
    int32_t ownedColumnNumber = 0;
    try {
        CatalogManager& catalog =
            g_engine.catalogService().get(s.currentDB);
        sequenceCatalog = &catalog;
        const auto* sequenceNamespace =
            catalog.findNamespaceByName(sequenceSchema);
        if (!sequenceNamespace) {
            std::cout << "ERROR: schema \"" << sequenceSchema
                      << "\" does not exist" << std::endl;
            return true;
        }
        sequenceNamespaceOid = sequenceNamespace->oid;
        const auto* existing = catalog.findClassByName(
            sequenceName.name, sequenceNamespaceOid);
        if (existing) {
            if (stmt->ifNotExists &&
                (existing->relkind != 'S' ||
                 g_engine.sequenceExists(s.currentDB, seqname))) {
                std::cout << "NOTICE: relation \"" << requestedName
                          << "\" already exists, skipping" << std::endl;
                return false;
            }
            std::cout << "ERROR: relation \"" << requestedName
                      << "\" already exists" << std::endl;
            return true;
        }
        if (g_engine.sequenceExists(s.currentDB, seqname)) {
            std::cout << "CREATE SEQUENCE failed: physical sequence already exists"
                      << std::endl;
            return true;
        }
        if (!info.ownedByTable.empty()) {
            CatalogManager::QualifiedName ownerName;
            if (!CatalogManager::parseQualifiedName(
                    info.ownedByTable, ownerName) ||
                ownerName.name.empty() ||
                ownerName.schema.find('.') != std::string::npos) {
                std::cout << "CREATE SEQUENCE OWNED BY target is invalid"
                          << std::endl;
                return true;
            }
            const std::string ownerSchema = ownerName.schema.empty()
                ? sequenceSchema : ownerName.schema;
            if (ownerSchema != sequenceSchema) {
                std::cout << "ERROR: sequence and owned table must be in the same schema"
                          << std::endl;
                return true;
            }
            const auto* table = catalog.findClassByName(
                ownerName.name, sequenceNamespaceOid);
            const auto* column = table && table->relkind == 'r'
                ? catalog.findAttribute(table->oid, info.ownedByColumn)
                : nullptr;
            if (!table || table->relkind != 'r' || !column) {
                std::cout << "CREATE SEQUENCE OWNED BY target does not exist"
                          << std::endl;
                return true;
            }
            ownedTableOid = table->oid;
            ownedColumnNumber = column->attnum;
            info.ownedByTable = sequenceSchema == "public"
                ? ownerName.name : sequenceSchema + "." + ownerName.name;
        }
    } catch (const std::exception& error) {
        std::cout << "CREATE SEQUENCE catalog preflight failed: "
                  << error.what() << std::endl;
        return true;
    }

    txn.markSnapshotDirty();
    DBStatus res = g_engine.createSequence(s.currentDB, seqname, info);
    if (res != DBStatus::OK) {
        std::cout << "CREATE SEQUENCE failed" << std::endl;
        return true;
    }
    txn.recordCreate(DdlObjectKind::Sequence, seqname);

    try {
        PgClassRow sequence;
        sequence.relname = sequenceName.name;
        sequence.relnamespace = sequenceNamespaceOid;
        sequence.relkind = 'S';
        sequence.relnatts = 0;
        const Oid sequenceOid = sequenceCatalog->createClass(sequence);

        if (ownedTableOid != INVALID_OID) {
            PgDependRow dependency;
            dependency.classid = PgClassOid_Class;
            dependency.objid = sequenceOid;
            dependency.objsubid = 0;
            dependency.refclassid = PgClassOid_Class;
            dependency.refobjid = ownedTableOid;
            dependency.refobjsubid = ownedColumnNumber;
            dependency.deptype = 'a';
            sequenceCatalog->addDepend(dependency);
        }
        if (!sequenceCatalog->persistAll()) {
            throw std::runtime_error("cannot persist sequence catalog");
        }
    } catch (const std::exception& error) {
        std::cout << "CREATE SEQUENCE catalog registration failed: "
                  << error.what() << std::endl;
        return true;
    }

    if (!txn.commit()) return true;
    std::cout << "CREATE SEQUENCE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeAlterSequence(const AlterObjectStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->only) {
        std::cout << "SQL syntax error: ONLY is not valid for ALTER SEQUENCE"
                  << std::endl;
        return true;
    }

    const std::string requestedName = stmt->schema.empty()
        ? stmt->objectName : stmt->schema + "." + stmt->objectName;
    CatalogManager::QualifiedName sequenceName;
    if (!CatalogManager::parseQualifiedName(requestedName, sequenceName) ||
        sequenceName.name.empty() ||
        sequenceName.schema.find('.') != std::string::npos) {
        std::cout << "ALTER SEQUENCE has an invalid name" << std::endl;
        return true;
    }
    std::string sequenceSchema = sequenceName.schema;
    if (sequenceSchema.empty()) {
        CatalogManager& catalog =
            g_engine.catalogService().get(s.currentDB);
        sequenceSchema = schemaNameForRelation(
            catalog, catalog.resolveRelation(
                         sequenceName.name,
                         effectiveSequenceSearchPath(s)));
        if (sequenceSchema.empty()) sequenceSchema = "public";
    }
    const std::string seqname = sequenceSchema == "public"
        ? sequenceName.name : sequenceSchema + "." + sequenceName.name;
    dbms::SequenceInfo info;

    std::string rest = stmt->subCommand;
    std::vector<std::string> tokens;
    {
        std::istringstream iss(rest);
        std::string tok;
        while (iss >> tok) tokens.push_back(tok);
    }

    if (tokens.empty()) {
        std::cout << "SQL syntax error: ALTER SEQUENCE requires an action"
                  << std::endl;
        return true;
    }

    const bool renameRequested = toLower(tokens[0]) == "rename";
    std::string newName;
    if (renameRequested) {
        if (tokens.size() != 3 || toLower(tokens[1]) != "to" || tokens[2].empty()) {
            std::cout << "SQL syntax error: ALTER SEQUENCE name RENAME TO new_name" << std::endl;
            return true;
        }
        CatalogManager::QualifiedName newSequenceName;
        if (!CatalogManager::parseQualifiedName(tokens[2], newSequenceName) ||
            newSequenceName.name.empty() || !newSequenceName.schema.empty()) {
            std::cout << "SQL syntax error: ALTER SEQUENCE RENAME target must be unqualified"
                      << std::endl;
            return true;
        }
        newName = newSequenceName.name;
    }

    auto lower = [](const std::string& str) {
        std::string r = str;
        for (char& c : r) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return r;
    };

    if (!renameRequested) {
        for (const auto& token : tokens) {
            if (toLower(token) == "rename" || toLower(token) == "to") {
                std::cout << "SQL syntax error: ALTER SEQUENCE name RENAME TO new_name"
                          << std::endl;
                return true;
            }
        }

        for (size_t i = 0; i < tokens.size(); ++i) {
            std::string tok = lower(tokens[i]);
            auto readValue = [&](int64_t& target, const char* name) {
                size_t consumed = 0;
                if (!parseInt64Tokens(
                        tokens, i + 1, target, consumed)) {
                    std::cout << "SQL syntax error: invalid " << name
                              << " value" << std::endl;
                    return false;
                }
                i += consumed;
                return true;
            };
            if (tok == "start") {
                info.startSpecified = true;
                if (i + 1 < tokens.size() && lower(tokens[i + 1]) == "with") {
                    ++i;
                }
                if (!readValue(info.start, "START")) return true;
            } else if (tok == "restart") {
                info.restartSpecified = true;
                if (i + 1 < tokens.size() && lower(tokens[i + 1]) == "with") {
                    ++i;
                    if (!readValue(info.restart, "RESTART")) return true;
                    info.restartValueSpecified = true;
                } else if (i + 1 < tokens.size()) {
                    int64_t restartValue = 0;
                    size_t consumed = 0;
                    if (parseInt64Tokens(
                            tokens, i + 1, restartValue, consumed)) {
                        info.restart = restartValue;
                        info.restartValueSpecified = true;
                        i += consumed;
                    } else if (tokens[i + 1] == "+" ||
                               tokens[i + 1] == "-") {
                        std::cout << "SQL syntax error: invalid RESTART value"
                                  << std::endl;
                        return true;
                    }
                }
            } else if (tok == "increment") {
                info.incrementSpecified = true;
                if (i + 1 < tokens.size() && lower(tokens[i + 1]) == "by") {
                    ++i;
                    if (!readValue(info.increment, "INCREMENT")) return true;
                } else if (!readValue(info.increment, "INCREMENT")) {
                    return true;
                }
            } else if (tok == "minvalue") {
                info.hasMinValue = true;
                if (!readValue(info.minValue, "MINVALUE")) return true;
            } else if (tok == "maxvalue") {
                info.hasMaxValue = true;
                if (!readValue(info.maxValue, "MAXVALUE")) return true;
            } else if (tok == "cache") {
                info.cacheSpecified = true;
                if (!readValue(info.cache, "CACHE")) return true;
            } else if (tok == "no") {
                if (i + 1 >= tokens.size()) {
                    std::cout << "SQL syntax error: incomplete NO option"
                              << std::endl;
                    return true;
                }
                const std::string next = lower(tokens[i + 1]);
                if (next == "minvalue") {
                    info.noMinValue = true;
                } else if (next == "maxvalue") {
                    info.noMaxValue = true;
                } else if (next == "cycle") {
                    info.cycleSpecified = true;
                    info.cycle = false;
                } else {
                    std::cout << "SQL syntax error: unsupported ALTER SEQUENCE option NO "
                              << tokens[i + 1] << std::endl;
                    return true;
                }
                ++i;
            } else if (tok == "cycle") {
                info.cycleSpecified = true;
                info.cycle = true;
            } else if (tok == "owned") {
                if (info.ownedBySpecified || i + 2 >= tokens.size() ||
                    lower(tokens[i + 1]) != "by") {
                    std::cout << "SQL syntax error: invalid OWNED BY clause"
                              << std::endl;
                    return true;
                }
                info.ownedBySpecified = true;
                if (lower(tokens[i + 2]) == "none") {
                    info.ownedByTable.clear();
                    info.ownedByColumn.clear();
                    i += 2;
                } else if (i + 6 < tokens.size() &&
                           tokens[i + 3] == "." && tokens[i + 5] == ".") {
                    info.ownedByTable =
                        tokens[i + 2] + "." + tokens[i + 4];
                    info.ownedByColumn = tokens[i + 6];
                    i += 6;
                } else if (i + 4 < tokens.size() && tokens[i + 3] == ".") {
                    info.ownedByTable = tokens[i + 2];
                    info.ownedByColumn = tokens[i + 4];
                    i += 4;
                } else {
                    std::cout << "SQL syntax error: OWNED BY requires table.column"
                              << std::endl;
                    return true;
                }
            } else {
                std::cout << "SQL syntax error: unsupported ALTER SEQUENCE option "
                          << tokens[i] << std::endl;
                return true;
            }
        }
    }

    CatalogManager* sequenceCatalog = nullptr;
    Oid sequenceOid = INVALID_OID;
    Oid sequenceNamespaceOid = INVALID_OID;
    Oid ownedTableOid = INVALID_OID;
    int32_t ownedColumnNumber = 0;
    try {
        CatalogManager& catalog =
            g_engine.catalogService().get(s.currentDB);
        sequenceCatalog = &catalog;
        const bool physicalExists =
            g_engine.sequenceExists(s.currentDB, seqname);
        const auto* sequenceNamespace =
            catalog.findNamespaceByName(sequenceSchema);
        const PgClassRow* sequence = sequenceNamespace
            ? catalog.findClassByName(
                  sequenceName.name, sequenceNamespace->oid)
            : nullptr;
        if (!sequence) {
            if (physicalExists) {
                std::cout << "ALTER SEQUENCE failed: sequence catalog entry is missing"
                          << std::endl;
                return true;
            }
            if (stmt->ifExists) {
                std::cout << "NOTICE: sequence \"" << requestedName
                          << "\" does not exist, skipping" << std::endl;
                return !txn.commit();
            }
            std::cout << "ERROR: sequence \"" << requestedName
                      << "\" does not exist" << std::endl;
            return true;
        }
        if (sequence->relkind != 'S') {
            std::cout << "ERROR: relation \"" << requestedName
                      << "\" is not a sequence" << std::endl;
            return true;
        }
        if (!physicalExists) {
            std::cout << "ALTER SEQUENCE failed: physical sequence is missing"
                      << std::endl;
            return true;
        }
        sequenceOid = sequence->oid;
        sequenceNamespaceOid = sequence->relnamespace;

        if (renameRequested) {
            const auto* collision =
                catalog.findClassByName(newName, sequenceNamespaceOid);
            const std::string newStorageName = sequenceSchema == "public"
                ? newName : sequenceSchema + "." + newName;
            if (collision ||
                g_engine.sequenceExists(s.currentDB, newStorageName)) {
                std::cout << "ERROR: relation \"" << newName
                          << "\" already exists" << std::endl;
                return true;
            }
        }

        if (info.ownedBySpecified && !info.ownedByTable.empty()) {
            CatalogManager::QualifiedName ownerName;
            if (!CatalogManager::parseQualifiedName(
                    info.ownedByTable, ownerName) ||
                ownerName.name.empty() ||
                ownerName.schema.find('.') != std::string::npos) {
                std::cout << "ALTER SEQUENCE OWNED BY target is invalid"
                          << std::endl;
                return true;
            }
            const std::string ownerSchema = ownerName.schema.empty()
                ? sequenceSchema : ownerName.schema;
            if (ownerSchema != sequenceSchema) {
                std::cout << "ERROR: sequence and owned table must be in the same schema"
                          << std::endl;
                return true;
            }
            const auto* table = catalog.findClassByName(
                ownerName.name, sequenceNamespaceOid);
            const auto* column = table && table->relkind == 'r'
                ? catalog.findAttribute(table->oid, info.ownedByColumn)
                : nullptr;
            if (!table || table->relkind != 'r' || !column) {
                std::cout << "ALTER SEQUENCE OWNED BY target does not exist"
                          << std::endl;
                return true;
            }
            ownedTableOid = table->oid;
            ownedColumnNumber = column->attnum;
            info.ownedByTable = sequenceSchema == "public"
                ? ownerName.name : sequenceSchema + "." + ownerName.name;
        }
    } catch (const std::exception& error) {
        std::cout << "ALTER SEQUENCE catalog preflight failed: "
                  << error.what() << std::endl;
        return true;
    }

    if (renameRequested) {
        const std::string newStorageName = sequenceSchema == "public"
            ? newName : sequenceSchema + "." + newName;
        const auto dependencies =
            findDefaultNextvalDeps(s.currentDB, seqname);
        txn.markSnapshotDirty();
        if (g_engine.renameSequence(
                s.currentDB, seqname, newStorageName) != DBStatus::OK) {
            std::cout << "ALTER SEQUENCE RENAME failed" << std::endl;
            return true;
        }
        if (!sequenceCatalog->renameClass(sequenceOid, newName)) {
            std::cout << "ALTER SEQUENCE RENAME catalog update failed"
                      << std::endl;
            return true;
        }
        for (const auto& [tableName, columnName] : dependencies) {
            const auto table =
                g_engine.getTableSchema(s.currentDB, tableName);
            size_t columnIndex = table.len;
            for (size_t i = 0; i < table.len; ++i) {
                if (table.cols[i].dataName == columnName) {
                    columnIndex = i;
                    break;
                }
            }
            if (columnIndex >= table.len) {
                std::cout << "ALTER SEQUENCE RENAME dependency update failed"
                          << std::endl;
                return true;
            }
            const std::string updatedDefault =
                renameNextvalSequenceReference(
                    table.cols[columnIndex].defaultValue,
                    seqname, newStorageName);
            if (g_engine.alterTableSetDefault(
                    s.currentDB, tableName, columnName,
                    updatedDefault) != DBStatus::OK) {
                std::cout << "ALTER SEQUENCE RENAME dependency update failed"
                          << std::endl;
                return true;
            }
        }
        if (!sequenceCatalog->persistAll()) {
            std::cout << "ALTER SEQUENCE RENAME catalog persistence failed"
                      << std::endl;
            return true;
        }
        txn.recordUpdate(DdlObjectKind::Sequence, seqname, newStorageName);
        if (!txn.commit()) return true;

        auto moveSessionValue = [&](const std::string& oldKey,
                                    const std::string& newKey) {
            auto it = s.sequenceLastValues.find(oldKey);
            if (it == s.sequenceLastValues.end()) return;
            const int64_t value = it->second;
            s.sequenceLastValues.erase(it);
            s.sequenceLastValues[newKey] = value;
        };
        moveSessionValue(seqname, newStorageName);
        if (sequenceSchema == "public") {
            moveSessionValue("public." + sequenceName.name,
                             "public." + newName);
        }
        std::cout << "ALTER SEQUENCE succeeded" << std::endl;
        return false;
    }

    txn.markSnapshotDirty();
    if (g_engine.alterSequence(s.currentDB, seqname, info) != DBStatus::OK) {
        std::cout << "ALTER SEQUENCE failed" << std::endl;
        return true;
    }

    if (info.ownedBySpecified) {
        try {
            const auto oldDependencies = sequenceCatalog->findDepends(
                PgClassOid_Class, sequenceOid, 0);
            for (const auto& dependency : oldDependencies) {
                if (dependency.deptype == 'a' &&
                    !sequenceCatalog->removeDepend(
                        dependency.classid, dependency.objid,
                        dependency.objsubid, dependency.refclassid,
                        dependency.refobjid, dependency.refobjsubid)) {
                    throw std::runtime_error(
                        "cannot remove previous sequence ownership");
                }
            }
            if (ownedTableOid != INVALID_OID) {
                PgDependRow dependency;
                dependency.classid = PgClassOid_Class;
                dependency.objid = sequenceOid;
                dependency.objsubid = 0;
                dependency.refclassid = PgClassOid_Class;
                dependency.refobjid = ownedTableOid;
                dependency.refobjsubid = ownedColumnNumber;
                dependency.deptype = 'a';
                sequenceCatalog->addDepend(dependency);
            }
            if (!sequenceCatalog->persistAll()) {
                throw std::runtime_error(
                    "cannot persist sequence ownership catalog");
            }
        } catch (const std::exception& error) {
            std::cout << "ALTER SEQUENCE catalog update failed: "
                      << error.what() << std::endl;
            return true;
        }
    }

    txn.recordUpdate(DdlObjectKind::Sequence, seqname);
    if (!txn.commit()) return true;
    std::cout << "ALTER SEQUENCE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropSequence(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP SEQUENCE name" << std::endl;
        return true;
    }

    struct SequenceDropTarget {
        std::string requestedName;
        std::string schema;
        std::string relationName;
        std::string storageName;
        Oid oid = INVALID_OID;
        std::vector<std::pair<std::string, std::string>> defaultDependencies;
        CatalogManager::DropPlan catalogPlan;
    };

    CatalogManager* catalog = nullptr;
    std::vector<SequenceDropTarget> targets;
    std::set<std::string> targetStorageNames;
    try {
        catalog = &g_engine.catalogService().get(s.currentDB);
        for (const auto& requestedName : stmt->objectNames) {
            CatalogManager::QualifiedName qualifiedName;
            if (!CatalogManager::parseQualifiedName(
                    requestedName, qualifiedName) ||
                qualifiedName.name.empty() ||
                qualifiedName.schema.find('.') != std::string::npos) {
                std::cout << "DROP SEQUENCE has an invalid name: "
                          << requestedName << std::endl;
                return true;
            }
            std::string schema = qualifiedName.schema;
            const PgClassRow* resolvedRelation = nullptr;
            if (schema.empty()) {
                resolvedRelation = catalog->resolveRelation(
                    qualifiedName.name, effectiveSequenceSearchPath(s));
                schema = schemaNameForRelation(*catalog, resolvedRelation);
                if (schema.empty()) schema = "public";
            }
            const std::string storageName = schema == "public"
                ? qualifiedName.name : schema + "." + qualifiedName.name;
            if (!targetStorageNames.insert(storageName).second) continue;

            const bool physicalExists =
                g_engine.sequenceExists(s.currentDB, storageName);
            const auto* sequenceNamespace =
                catalog->findNamespaceByName(schema);
            const PgClassRow* sequence = resolvedRelation
                ? resolvedRelation
                : (sequenceNamespace
                       ? catalog->findClassByName(
                             qualifiedName.name, sequenceNamespace->oid)
                       : nullptr);
            if (!sequence) {
                if (physicalExists) {
                    std::cout << "DROP SEQUENCE failed: sequence catalog entry is missing for \""
                              << requestedName << "\"" << std::endl;
                    return true;
                }
                if (stmt->ifExists) {
                    std::cout << "NOTICE: sequence \"" << requestedName
                              << "\" does not exist, skipping" << std::endl;
                    continue;
                }
                std::cout << "ERROR: sequence \"" << requestedName
                          << "\" does not exist (SQLSTATE 42P01)"
                          << std::endl;
                return true;
            }
            if (sequence->relkind != 'S') {
                std::cout << "ERROR: relation \"" << requestedName
                          << "\" is not a sequence" << std::endl;
                return true;
            }
            if (!physicalExists) {
                std::cout << "DROP SEQUENCE failed: physical sequence is missing for \""
                          << requestedName << "\"" << std::endl;
                return true;
            }

            SequenceDropTarget target;
            target.requestedName = requestedName;
            target.schema = schema;
            target.relationName = qualifiedName.name;
            target.storageName = storageName;
            target.oid = sequence->oid;
            target.defaultDependencies =
                findDefaultNextvalDeps(s.currentDB, storageName);
            if (!stmt->cascade && !target.defaultDependencies.empty()) {
                std::cout << "ERROR: cannot drop sequence " << requestedName
                          << " because other objects depend on it"
                          << std::endl;
                return true;
            }
            target.catalogPlan = catalog->planDrop(
                PgClassOid_Class, target.oid,
                stmt->cascade ? CatalogManager::DropBehavior::Cascade
                              : CatalogManager::DropBehavior::Restrict);
            if (!target.catalogPlan.ok()) {
                std::cout << "ERROR: " << target.catalogPlan.error
                          << std::endl;
                return true;
            }
            targets.push_back(std::move(target));
        }
    } catch (const std::exception& error) {
        std::cout << "DROP SEQUENCE catalog preflight failed: "
                  << error.what() << std::endl;
        return true;
    }

    if (targets.empty()) {
        if (!txn.commit()) return true;
        std::cout << "DROP SEQUENCE succeeded" << std::endl;
        return false;
    }

    CatalogManager::DropPlan combinedCatalogPlan;
    std::set<std::pair<Oid, Oid>> plannedCatalogObjects;
    std::vector<PhysicalCascadeAction> physicalCascadeActions;
    for (const auto& target : targets) {
        for (const auto& object : target.catalogPlan.objectsToDrop) {
            if (plannedCatalogObjects.insert(object).second) {
                combinedCatalogPlan.objectsToDrop.push_back(object);
            }
        }
        if (!stmt->cascade) continue;
        std::vector<PhysicalCascadeAction> targetActions;
        std::string error;
        if (!buildPhysicalCascadeActions(
                *catalog, g_engine, s.currentDB, target.oid,
                target.catalogPlan, targetActions, error)) {
            std::cout << "DROP SEQUENCE CASCADE planning failed: "
                      << error << std::endl;
            return true;
        }
        for (auto& action : targetActions) {
            if (action.kind == PhysicalCascadeAction::Kind::Sequence &&
                targetStorageNames.count(action.name) != 0) {
                continue;
            }
            const auto duplicate = std::find_if(
                physicalCascadeActions.begin(),
                physicalCascadeActions.end(),
                [&](const PhysicalCascadeAction& existing) {
                    return existing.kind == action.kind &&
                           existing.name == action.name &&
                           existing.tableName == action.tableName &&
                           existing.accessMethod == action.accessMethod &&
                           existing.key == action.key;
                });
            if (duplicate == physicalCascadeActions.end()) {
                physicalCascadeActions.push_back(std::move(action));
            }
        }
    }

    txn.markSnapshotDirty();
    std::set<std::pair<std::string, std::string>> clearedDefaults;
    for (const auto& target : targets) {
        for (const auto& dependency : target.defaultDependencies) {
            if (!clearedDefaults.insert(dependency).second) continue;
            if (g_engine.alterTableDropDefault(
                    s.currentDB, dependency.first,
                    dependency.second) != DBStatus::OK) {
                std::cout << "ERROR: failed to clear default on "
                          << dependency.first << "." << dependency.second
                          << std::endl;
                return true;
            }
        }
    }
    for (const auto& action : physicalCascadeActions) {
        if (!dropPhysicalCascadeAction(
                g_engine, s.currentDB, action)) {
            std::cout << "DROP SEQUENCE CASCADE physical cleanup failed for "
                      << action.name << std::endl;
            return true;
        }
    }
    for (const auto& target : targets) {
        if (g_engine.dropSequence(
                s.currentDB, target.storageName) != DBStatus::OK) {
            std::cout << "DROP SEQUENCE failed for "
                      << target.requestedName << std::endl;
            return true;
        }
        txn.recordDrop(DdlObjectKind::Sequence, target.storageName);
    }

    std::string catalogError;
    if (!catalog->applyDropPlan(combinedCatalogPlan, &catalogError)) {
        std::cout << "DROP SEQUENCE catalog cleanup failed: "
                  << catalogError << std::endl;
        return true;
    }
    if (!catalog->persistAll()) {
        std::cout << "DROP SEQUENCE catalog persistence failed"
                  << std::endl;
        return true;
    }
    if (!txn.commit()) return true;

    for (const auto& target : targets) {
        s.sequenceLastValues.erase(target.storageName);
        if (target.schema == "public") {
            s.sequenceLastValues.erase("public." + target.relationName);
        }
    }
    std::cout << "DROP SEQUENCE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP DOMAIN
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateDomain(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    StorageEngine::DomainInfo info;
    info.name = stmt->objectName;
    auto it = stmt->options.find("base_type");
    if (it != stmt->options.end()) info.baseType = it->second;
    it = stmt->options.find("default");
    if (it != stmt->options.end()) info.defaultValue = stripQuotes(it->second);
    it = stmt->options.find("check");
    if (it != stmt->options.end()) info.checkExpr = it->second;
    it = stmt->options.find("constraint_name");
    if (it != stmt->options.end()) info.constraintName = it->second;
    DBStatus res = g_engine.createDomain(s.currentDB, info);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        std::cout << "Domain " << info.name << " already exists" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "CREATE DOMAIN failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    txn.recordCreate(DdlObjectKind::Domain, info.name);
    if (!txn.commit()) return true;
    std::cout << "CREATE DOMAIN succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeCreateCollation(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectName.empty()) {
        std::cout << "CREATE COLLATION requires a name" << std::endl;
        return true;
    }
    const std::string schemaName = stmt->schema.empty()
        ? "public" : stmt->schema;
    if (!g_engine.schemaExists(s.currentDB, schemaName)) {
        std::cout << "Schema " << schemaName << " does not exist" << std::endl;
        return true;
    }
    const std::string cname = schemaName == "public"
        ? stmt->objectName : schemaName + "__" + stmt->objectName;

    if (stmt->options.count("source") != 0) {
        std::cout << "CREATE COLLATION FROM is not supported" << std::endl;
        return true;
    }
    if (stmt->options.count("rules") != 0 ||
        stmt->options.count("version") != 0) {
        std::cout << "CREATE COLLATION rules/version options are not supported"
                  << std::endl;
        return true;
    }

    auto itProvider = stmt->options.find("provider");
    auto itLocale = stmt->options.find("locale");
    std::string provider = itProvider == stmt->options.end()
        ? "libc" : toLower(stripQuotes(itProvider->second));
    if (provider != "libc") {
        std::cout << "CREATE COLLATION currently supports only provider libc"
                  << std::endl;
        return true;
    }
    std::string locale = itLocale == stmt->options.end()
        ? "" : stripQuotes(itLocale->second);
    const auto itCollate = stmt->options.find("lc_collate");
    const auto itCtype = stmt->options.find("lc_ctype");
    const std::string lcCollate = itCollate == stmt->options.end()
        ? "" : stripQuotes(itCollate->second);
    const std::string lcCtype = itCtype == stmt->options.end()
        ? "" : stripQuotes(itCtype->second);
    const auto conflictsWithLocale = [&](const std::string& candidate) {
        return !candidate.empty() && !locale.empty() && candidate != locale;
    };
    if (conflictsWithLocale(lcCollate) || conflictsWithLocale(lcCtype) ||
        (!lcCollate.empty() && !lcCtype.empty() && lcCollate != lcCtype)) {
        std::cout << "CREATE COLLATION requires one representable locale"
                  << std::endl;
        return true;
    }
    if (locale.empty()) locale = !lcCollate.empty() ? lcCollate : lcCtype;
    if (locale.empty()) locale = "C";

    const auto deterministic = stmt->options.find("deterministic");
    if (deterministic != stmt->options.end()) {
        const std::string value = toLower(stripQuotes(deterministic->second));
        if (value != "true" && value != "on" && value != "1") {
            std::cout << "Nondeterministic collations are not supported"
                      << std::endl;
            return true;
        }
    }

    txn.markSnapshotDirty();
    const DBStatus res = g_engine.createCollation(s.currentDB, cname, provider, locale);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        if (stmt->ifNotExists) {
            if (!txn.commit()) return true;
            std::cout << "NOTICE: collation " << cname
                      << " already exists, skipping" << std::endl;
            return false;
        }
        std::cout << "Collation " << cname << " already exists" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "CREATE COLLATION failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Collation, cname);
    if (!txn.commit()) return true;
    std::cout << "CREATE COLLATION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropCollation(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP COLLATION name" << std::endl;
        return true;
    }
    if (stmt->objectNames.size() != 1) {
        std::cout << "DROP COLLATION with multiple targets is not supported"
                  << std::endl;
        return true;
    }
    CatalogManager::QualifiedName qualifiedName;
    if (!CatalogManager::parseQualifiedName(
            stmt->objectNames.front(), qualifiedName) ||
        qualifiedName.name.empty()) {
        std::cout << "DROP COLLATION has an invalid name" << std::endl;
        return true;
    }
    const std::string schemaName = qualifiedName.schema.empty()
        ? "public" : qualifiedName.schema;
    const std::string cname = schemaName == "public"
        ? qualifiedName.name : schemaName + "__" + qualifiedName.name;
    txn.markSnapshotDirty();
    const DBStatus status = g_engine.dropCollation(s.currentDB, cname);
    if (status == DBStatus::TABLE_NOT_FOUND && stmt->ifExists) {
        if (!txn.commit()) return true;
        std::cout << "NOTICE: collation " << cname
                  << " does not exist, skipping" << std::endl;
        return false;
    }
    if (status != DBStatus::OK) {
        if (status == DBStatus::INVALID_VALUE) {
            if (stmt->cascade) {
                std::cout
                    << "DROP COLLATION CASCADE for dependent table columns "
                       "is not supported; no objects were dropped"
                    << std::endl;
            } else {
                std::cout << "Cannot drop collation " << cname
                          << " because a table column depends on it"
                          << std::endl;
            }
            return true;
        }
        std::cout << "DROP COLLATION failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    txn.recordDrop(DdlObjectKind::Collation, cname);
    if (!txn.commit()) return true;
    std::cout << "DROP COLLATION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropDomain(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP DOMAIN name" << std::endl;
        return true;
    }
    std::string name = stmt->objectNames.front();
    txn.markSnapshotDirty();
    txn.recordDrop(DdlObjectKind::Domain, name);
    DBStatus res = g_engine.dropDomain(s.currentDB, name);
    if (res != DBStatus::OK) {
        std::cout << "DROP DOMAIN failed" << std::endl;
        return true;
    }
    if (!txn.commit()) return true;
    std::cout << "DROP DOMAIN succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP TYPE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateType(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectName.empty()) {
        std::cout << "CREATE TYPE requires a type name" << std::endl;
        return true;
    }
    const std::string typeSchema = stmt->schema.empty()
        ? "public" : stmt->schema;
    if (!g_engine.schemaExists(s.currentDB, typeSchema)) {
        std::cout << "Schema " << typeSchema << " does not exist" << std::endl;
        return true;
    }
    const std::string typeName = typeSchema == "public"
        ? stmt->objectName : typeSchema + "." + stmt->objectName;
    bool typeExists = false;
    const DBStatus existenceStatus =
        anyTypeExists(s.currentDB, typeName, typeExists);
    if (existenceStatus != DBStatus::OK) {
        std::cout << "CREATE TYPE metadata lookup failed (SQLSTATE "
                  << sqlstateForDBStatus(existenceStatus) << ")"
                  << std::endl;
        return true;
    }
    if (typeExists) {
        if (stmt->ifNotExists) {
            if (!txn.commit()) return true;
            std::cout << "NOTICE: type " << typeName
                      << " already exists, skipping" << std::endl;
            return false;
        }
        std::cout << "Type " << typeName << " already exists" << std::endl;
        return true;
    }

    std::string typeKind = stmt->options.count("type_kind") ? stmt->options.at("type_kind") : "";
    if (typeKind == "enum") {
        StorageEngine::EnumType et;
        et.name = typeName;
        et.labels = stmt->enumLabels;
        if (et.labels.empty()) {
            std::cout << "CREATE TYPE AS ENUM requires at least one label" << std::endl;
            return true;
        }
        txn.markSnapshotDirty();
        DBStatus res = g_engine.createEnumType(s.currentDB, et);
        if (res != DBStatus::OK) {
            std::cout << "CREATE TYPE AS ENUM failed" << std::endl;
            return true;
        }
        try {
            CatalogManager& catalog =
                g_engine.catalogService().get(s.currentDB);
            const PgNamespaceRow* typeNamespace =
                catalog.findNamespaceByName(typeSchema);
            if (!typeNamespace ||
                catalog.findTypeByName(stmt->objectName,
                                       typeNamespace->oid)) {
                throw std::runtime_error(
                    "enum namespace/type catalog conflict");
            }
            PgTypeRow catalogType;
            catalogType.typname = stmt->objectName;
            catalogType.typnamespace = typeNamespace->oid;
            catalogType.typlen = 4;
            catalogType.typbyval = true;
            catalogType.typtype = 'e';
            catalogType.typcategory = 'E';
            if (const auto owner = authCatalog().getAuthIdByName(
                    effectiveSessionRole(s))) {
                catalogType.typowner = owner->oid;
            }
            const Oid typeOid = catalog.createType(catalogType);
            if (!catalog.replaceEnumLabels(typeOid, et.labels) ||
                !catalog.persistAll()) {
                throw std::runtime_error("cannot persist enum catalog rows");
            }
        } catch (const std::exception& error) {
            std::cout << "CREATE TYPE AS ENUM catalog registration failed: "
                      << error.what() << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, et.name);
        if (!txn.commit()) return true;
        std::cout << "CREATE TYPE AS ENUM succeeded" << std::endl;
        return false;
    }

    // Shell type (CREATE TYPE name)
    if (typeKind == "shell") {
        txn.markSnapshotDirty();
        const DBStatus status = recordShellType(s.currentDB, typeName);
        if (status != DBStatus::OK) {
            std::cout << "CREATE TYPE failed (SQLSTATE "
                      << sqlstateForDBStatus(status) << ")" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, typeName);
        if (!txn.commit()) return true;
        std::cout << "CREATE TYPE succeeded" << std::endl;
        return false;
    }

    // Range type (CREATE TYPE name AS RANGE (...))
    if (typeKind == "range") {
        UdtMeta meta;
        meta.kind = "range";
        meta.name = typeName;
        for (const auto& kv : stmt->options) {
            if (kv.first.substr(0, 6) == "range_") {
                meta.attrs[kv.first.substr(6)] = kv.second;
            }
        }
        if (meta.attrs.count("subtype") == 0) {
            std::cout << "CREATE TYPE AS RANGE requires a subtype" << std::endl;
            return true;
        }
        txn.markSnapshotDirty();
        const DBStatus status = recordUdtMeta(s.currentDB, meta);
        if (status != DBStatus::OK) {
            std::cout << "CREATE TYPE failed (SQLSTATE "
                      << sqlstateForDBStatus(status) << ")" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, typeName);
        if (!txn.commit()) return true;
        std::cout << "CREATE TYPE AS RANGE succeeded" << std::endl;
        return false;
    }

    // Base type (CREATE TYPE name (INPUT=..., OUTPUT=..., ...))
    if (typeKind == "base") {
        UdtMeta meta;
        meta.kind = "base";
        meta.name = typeName;
        for (const auto& kv : stmt->options) {
            if (kv.first.substr(0, 5) == "base_") {
                meta.attrs[kv.first.substr(5)] = kv.second;
            }
        }
        if (meta.attrs.count("input") == 0 || meta.attrs.count("output") == 0) {
            std::cout << "CREATE TYPE base requires INPUT and OUTPUT functions" << std::endl;
            return true;
        }
        txn.markSnapshotDirty();
        const DBStatus status = recordUdtMeta(s.currentDB, meta);
        if (status != DBStatus::OK) {
            std::cout << "CREATE TYPE failed (SQLSTATE "
                      << sqlstateForDBStatus(status) << ")" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, typeName);
        if (!txn.commit()) return true;
        std::cout << "CREATE TYPE succeeded" << std::endl;
        return false;
    }

    // Composite type (existing behavior)
    StorageEngine::CompositeType ct;
    ct.name = typeName;
    auto it = stmt->options.find("fields");
    if (it != stmt->options.end()) {
        std::stringstream ss(it->second);
        std::string item;
        // Fields are ';'-separated so that type modifiers like numeric(10,2)
        // (which contain commas) are preserved intact.
        while (std::getline(ss, item, ';')) {
            item = trim(item);
            size_t sp = item.find(' ');
            if (sp != std::string::npos) {
                ct.fields.emplace_back(trim(item.substr(0, sp)), trim(item.substr(sp + 1)));
            }
        }
    }
    if (ct.fields.empty()) {
        std::cout << "CREATE TYPE requires field list" << std::endl;
        return true;
    }
    txn.markSnapshotDirty();
    DBStatus res = g_engine.createCompositeType(s.currentDB, ct);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TYPE failed" << std::endl;
        return true;
    }
    txn.recordCreate(DdlObjectKind::Type, ct.name);
    if (!txn.commit()) return true;
    std::cout << "CREATE TYPE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropType(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP TYPE name" << std::endl;
        return true;
    }
    if (stmt->objectNames.size() != 1) {
        std::cout << "DROP TYPE with multiple targets is not supported"
                  << std::endl;
        return true;
    }
    CatalogManager::QualifiedName qualifiedName;
    if (!CatalogManager::parseQualifiedName(
            stmt->objectNames.front(), qualifiedName) ||
        qualifiedName.name.empty()) {
        std::cout << "DROP TYPE has an invalid name" << std::endl;
        return true;
    }
    const std::string schemaName = qualifiedName.schema.empty()
        ? "public" : qualifiedName.schema;
    const std::string name = schemaName == "public"
        ? qualifiedName.name : schemaName + "." + qualifiedName.name;

    // Enum dependencies are represented in the physical table schemas.  A
    // previous DROP TYPE path removed only the enum sidecar and left those
    // columns behind with an undeclared type.  Preflight them before touching
    // any metadata; CASCADE delegates each column removal to the normal ALTER
    // TABLE path so indexes, owned sequences, catalog attribute numbers, and
    // the row layout use the same guarded implementation.
    struct TypeColumnDependency {
        std::string tableName;
        std::string columnName;
    };
    std::vector<TypeColumnDependency> enumDependencies;
    const StorageEngine::EnumType enumDefinition =
        g_engine.getEnumType(s.currentDB, name);
    if (!enumDefinition.name.empty()) {
        for (const auto& tableName :
             g_engine.getTableNames(s.currentDB)) {
            const TableSchema table =
                g_engine.getTableSchema(s.currentDB, tableName);
            if (table.len == 0) {
                std::cout << "DROP TYPE dependency scan failed" << std::endl;
                return true;
            }
            for (size_t columnIndex = 0; columnIndex < table.len;
                 ++columnIndex) {
                const Column& column = table.cols[columnIndex];
                if (toLower(column.dataType) == toLower(name) &&
                    !column.enumValues.empty()) {
                    enumDependencies.push_back(
                        {tableName, column.dataName});
                }
            }
        }
    }
    if (!enumDependencies.empty() && !stmt->cascade) {
        const auto& blocker = enumDependencies.front();
        std::cout << "DROP TYPE failed: column " << blocker.tableName
                  << "." << blocker.columnName << " depends on type "
                  << name << "; use CASCADE" << std::endl;
        return true;
    }

    txn.markSnapshotDirty();
    for (const auto& dependency : enumDependencies) {
        const auto logicalTable =
            CatalogService::logicalName(dependency.tableName);
        AlterTableStmt alter;
        alter.tableName = logicalTable.schema.empty()
            ? logicalTable.name
            : logicalTable.schema + "." + logicalTable.name;
        AlterTableStmt::SubCmd dropColumn;
        dropColumn.action = AlterTableStmt::Action::DropColumn;
        dropColumn.name = dependency.columnName;
        dropColumn.options["cascade"] = "true";
        alter.subCommands.push_back(std::move(dropColumn));
        if (executeAlterTable(&alter, s)) {
            std::cout << "DROP TYPE CASCADE failed while dropping column "
                      << dependency.tableName << "."
                      << dependency.columnName << std::endl;
            return true;
        }
    }

    DBStatus res = g_engine.dropCompositeType(s.currentDB, name);
    if (res != DBStatus::OK && res != DBStatus::TABLE_NOT_FOUND) {
        std::cout << "DROP TYPE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    if (res == DBStatus::TABLE_NOT_FOUND) {
        res = g_engine.dropEnumType(s.currentDB, name);
    }
    if (res != DBStatus::OK && res != DBStatus::TABLE_NOT_FOUND) {
        std::cout << "DROP TYPE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    if (res == DBStatus::TABLE_NOT_FOUND) {
        res = removeUdtMeta(s.currentDB, name);
    }
    if (res != DBStatus::OK && res != DBStatus::TABLE_NOT_FOUND) {
        std::cout << "DROP TYPE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    if (res == DBStatus::TABLE_NOT_FOUND) {
        res = removeShellType(s.currentDB, name);
    }
    if (res != DBStatus::OK && res != DBStatus::TABLE_NOT_FOUND) {
        std::cout << "DROP TYPE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }
    if (res == DBStatus::TABLE_NOT_FOUND) {
        if (!stmt->ifExists) {
            std::cout << "DROP TYPE failed: type " << name
                      << " does not exist" << std::endl;
            return true;
        }
        if (!txn.commit()) return true;
        std::cout << "NOTICE: type " << name
                  << " does not exist, skipping" << std::endl;
        return false;
    }

    try {
        CatalogManager& catalog =
            g_engine.catalogService().get(s.currentDB);
        const PgNamespaceRow* typeNamespace =
            catalog.findNamespaceByName(schemaName);
        const PgTypeRow* catalogType = typeNamespace
            ? catalog.findTypeByName(qualifiedName.name,
                                     typeNamespace->oid)
            : nullptr;
        if (catalogType) {
            const Oid typeOid = catalogType->oid;
            if (!catalog.dropType(typeOid) || !catalog.persistAll()) {
                std::cout << "DROP TYPE catalog cleanup failed" << std::endl;
                return true;
            }
        }
    } catch (const std::exception& error) {
        std::cout << "DROP TYPE catalog cleanup failed: "
                  << error.what() << std::endl;
        return true;
    }
    txn.recordDrop(DdlObjectKind::Type, name);
    if (!txn.commit()) return true;
    std::cout << "DROP TYPE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateView(const CreateViewStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string viewname = stmt->viewName;
    std::string viewSql = stmt->selectSql;
    CatalogManager::QualifiedName qualifiedName;
    if (!CatalogManager::parseQualifiedName(viewname, qualifiedName) ||
        qualifiedName.name.empty()) {
        std::cout << "ERROR: invalid view name \"" << viewname << "\""
                  << std::endl;
        return true;
    }
    const std::string schemaName = qualifiedName.schema.empty()
        ? "public" : qualifiedName.schema;
    if (!g_engine.schemaExists(s.currentDB, schemaName)) {
        std::cout << "ERROR: schema \"" << schemaName
                  << "\" does not exist" << std::endl;
        return true;
    }
    if (viewSql.empty()) {
        std::cout << "CREATE VIEW requires AS SELECT" << std::endl;
        return true;
    }

    // Detect base table for simple updatable views.
    std::string baseTable;
    std::string lview = toLower(viewSql);
    if (lview.substr(0, 6) == "select") {
        size_t fromPos = lview.find(" from ");
        if (fromPos != std::string::npos) {
            size_t wherePos = lview.find(" where ", fromPos);
            size_t orderPos = lview.find(" order by ", fromPos);
            size_t groupPos = lview.find(" group by ", fromPos);
            size_t endPos = std::min(wherePos != std::string::npos ? wherePos : viewSql.size(),
                                     std::min(orderPos != std::string::npos ? orderPos : viewSql.size(),
                                              groupPos != std::string::npos ? groupPos : viewSql.size()));
            std::string tablePart = trim(viewSql.substr(fromPos + 6, endPos - fromPos - 6));
            if (tablePart.find(' ') == std::string::npos && tablePart.find(',') == std::string::npos) {
                baseTable = tablePart;
            }
        }
    }

    const std::string physicalRelationName = schemaName == "public"
        ? qualifiedName.name
        : schemaName + "__" + qualifiedName.name;
    if (g_engine.tableExists(s.currentDB, physicalRelationName) ||
        g_engine.isMaterializedView(s.currentDB, viewname)) {
        std::cout << "ERROR: relation \"" << viewname
                  << "\" already exists" << std::endl;
        return true;
    }

    std::string resolvedBaseTable;
    if (!baseTable.empty()) {
        resolvedBaseTable = resolveTableName(s, baseTable);
        if (!g_engine.tableExists(s.currentDB, resolvedBaseTable) &&
            !g_engine.viewExists(s.currentDB, baseTable) &&
            !g_engine.isMaterializedView(s.currentDB, baseTable)) {
            std::cout << "ERROR: relation \"" << baseTable
                      << "\" does not exist" << std::endl;
            return true;
        }
    }

    TableSchema output;
    output.owner = effectiveSessionRole(s);
    bool outputKnown = false;
    std::vector<std::string> selectColumns;
    std::string parsedSource;
    std::vector<std::string> ignoredConditions;
    if (parseSimpleSelect(
            viewSql, selectColumns, parsedSource, ignoredConditions)) {
        const std::string sourceStorageName =
            resolveTableName(s, parsedSource);
        if (g_engine.tableExists(s.currentDB, sourceStorageName)) {
            const TableSchema source =
                g_engine.getTableSchema(s.currentDB, sourceStorageName);
            outputKnown = source.len != 0;
            if (selectColumns.size() == 1 && selectColumns.front() == "*") {
                for (size_t column = 0; column < source.len; ++column) {
                    output.append(makeColumnFromSource(
                        source.cols[column], source.cols[column].dataName));
                }
            } else {
                for (const std::string& projection : selectColumns) {
                    std::string sourceName = projection;
                    std::string outputName;
                    const std::string lowerProjection = toLower(projection);
                    const size_t asPosition = lowerProjection.find(" as ");
                    if (asPosition != std::string::npos) {
                        sourceName = trim(projection.substr(0, asPosition));
                        outputName = trim(projection.substr(asPosition + 4));
                    }
                    const size_t qualifier = sourceName.rfind('.');
                    if (qualifier != std::string::npos) {
                        sourceName = sourceName.substr(qualifier + 1);
                    }
                    if (outputName.empty()) outputName = sourceName;

                    const Column* sourceColumn = nullptr;
                    for (size_t column = 0; column < source.len; ++column) {
                        if (toLower(source.cols[column].dataName) ==
                            toLower(sourceName)) {
                            sourceColumn = &source.cols[column];
                            break;
                        }
                    }
                    if (!sourceColumn) {
                        outputKnown = false;
                        output = TableSchema{};
                        break;
                    }
                    output.append(makeColumnFromSource(
                        *sourceColumn, outputName));
                }
            }
            if (outputKnown && !stmt->columnNames.empty()) {
                if (stmt->columnNames.size() != output.len) {
                    std::cout << "ERROR: CREATE VIEW column list has "
                              << stmt->columnNames.size()
                              << " names but query returns " << output.len
                              << " columns" << std::endl;
                    return true;
                }
                for (size_t column = 0; column < output.len; ++column) {
                    output.cols[column].dataName = stmt->columnNames[column];
                }
            }
        }
    }

    std::string checkOption = stmt->checkOption;
    std::string storeSql = viewSql;
    if (!baseTable.empty()) storeSql += "\nBASE_TABLE:" + baseTable + "\n";
    if (!checkOption.empty()) storeSql += "WITH_CHECK_OPTION:" + checkOption + "\n";

    CatalogManager* catalog = nullptr;
    Oid existingViewOid = INVALID_OID;
    try {
        catalog = &g_engine.catalogService().get(s.currentDB);
        const PgNamespaceRow* viewNamespace =
            catalog->findNamespaceByName(schemaName);
        if (!viewNamespace) {
            throw std::runtime_error("view schema has no catalog entry");
        }
        const PgClassRow* existing = catalog->findClassByName(
            qualifiedName.name, viewNamespace->oid);
        if (existing) {
            if (existing->relkind != 'v') {
                std::cout << "ERROR: relation \"" << viewname
                          << "\" already exists" << std::endl;
                return true;
            }
            if (!stmt->replace) {
                std::cout << "View " << viewname << " already exists"
                          << std::endl;
                return true;
            }
            existingViewOid = existing->oid;
        }
    } catch (const std::exception& error) {
        std::cout << "CREATE VIEW: catalog lookup failed: "
                  << error.what() << std::endl;
        return true;
    }

    const bool physicalViewExisted =
        g_engine.viewExists(s.currentDB, viewname);
    txn.markSnapshotDirty();
    if (stmt->replace && physicalViewExisted) {
        const DBStatus dropStatus =
            g_engine.dropView(s.currentDB, viewname);
        if (dropStatus != DBStatus::OK) {
            std::cout << "CREATE OR REPLACE VIEW: old definition removal failed"
                      << std::endl;
            return true;
        }
    }
    DBStatus res = g_engine.createView(s.currentDB, viewname, storeSql);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        std::cout << "View " << viewname << " already exists" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "CREATE VIEW failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }

    try {
        const TableSchema* catalogOutput = outputKnown ? &output : nullptr;
        const Oid viewOid = registerViewInCatalog(
            *catalog, catalogOutput, schemaName, qualifiedName.name,
            effectiveSessionRole(s), existingViewOid);

        // CREATE OR REPLACE changes the referenced relation set. Preserve
        // the namespace dependency installed by createClass(), but remove
        // the old relation dependencies before publishing the new one.
        for (const auto& dependency :
             catalog->findDepends(PgClassOid_Class, viewOid)) {
            if (dependency.refclassid != PgClassOid_Class) continue;
            catalog->removeDepend(
                dependency.classid, dependency.objid, dependency.objsubid,
                dependency.refclassid, dependency.refobjid,
                dependency.refobjsubid);
        }
        if (!baseTable.empty()) {
            const PgClassRow* baseRelation =
                catalog->resolveRelation(baseTable, {"public"});
            if (baseRelation && baseRelation->oid != viewOid) {
                PgDependRow dependency;
                dependency.classid = PgClassOid_Class;
                dependency.objid = viewOid;
                dependency.objsubid = 0;
                dependency.refclassid = PgClassOid_Class;
                dependency.refobjid = baseRelation->oid;
                dependency.refobjsubid = 0;
                dependency.deptype = 'n';
                catalog->addDepend(dependency);
            }
        }
        if (!catalog->persistAll()) {
            throw std::runtime_error("cannot persist view catalog");
        }
    } catch (const std::exception& error) {
        std::cout << "CREATE VIEW: catalog registration failed: "
                  << error.what() << std::endl;
        return true;
    }

    if (stmt->replace &&
        (physicalViewExisted || existingViewOid != INVALID_OID)) {
        txn.recordUpdate(DdlObjectKind::View, viewname);
    } else {
        txn.recordCreate(DdlObjectKind::View, viewname);
    }
    if (!txn.commit()) return true;
    std::cout << "CREATE VIEW succeeded"
              << (baseTable.empty() ? "" : " (updatable)")
              << (checkOption.empty() ? "" : " [with check option " + checkOption + "]")
              << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// DROP VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeDropView(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }
    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP VIEW name" << std::endl;
        return true;
    }
    if (stmt->objectNames.size() != 1) {
        std::cout << "DROP VIEW with multiple targets is not supported"
                  << std::endl;
        return true;
    }

    const std::string& viewName = stmt->objectNames.front();
    CatalogManager* catalog = nullptr;
    const PgClassRow* relation = nullptr;
    try {
        catalog = &g_engine.catalogService().get(s.currentDB);
        relation = catalog->resolveRelation(viewName, {"public"});
    } catch (const std::exception& error) {
        std::cout << "DROP VIEW: catalog lookup failed: "
                  << error.what() << std::endl;
        return true;
    }

    const bool physicalExists =
        g_engine.viewExists(s.currentDB, viewName);
    if (relation && relation->relkind != 'v') {
        std::cout << "ERROR: \"" << viewName << "\" is not a view"
                  << std::endl;
        return true;
    }
    if (!physicalExists && !relation) {
        if (stmt->ifExists) {
            std::cout << "NOTICE: view \"" << viewName
                      << "\" does not exist, skipping" << std::endl;
            return !txn.commit();
        }
        std::cout << "ERROR: view \"" << viewName
                  << "\" does not exist (SQLSTATE 42P01)"
                  << std::endl;
        return true;
    }

    CatalogManager::DropPlan catalogPlan;
    bool hasCatalogPlan = false;
    Oid viewOid = INVALID_OID;
    if (relation) {
        viewOid = relation->oid;
        const auto behavior = stmt->cascade
            ? CatalogManager::DropBehavior::Cascade
            : CatalogManager::DropBehavior::Restrict;
        catalogPlan = catalog->planDrop(
            PgClassOid_Class, viewOid, behavior);
        if (!catalogPlan.ok()) {
            std::cout << "ERROR: " << catalogPlan.error << std::endl;
            return true;
        }
        hasCatalogPlan = true;
    }

    std::vector<PhysicalCascadeAction> cascadeActions;
    if (stmt->cascade && hasCatalogPlan) {
        std::string error;
        if (!buildPhysicalCascadeActions(
                *catalog, g_engine, s.currentDB, viewOid,
                catalogPlan, cascadeActions, error)) {
            std::cout << "DROP VIEW CASCADE planning failed: "
                      << error << std::endl;
            return true;
        }
    }

    txn.markSnapshotDirty();
    for (const auto& action : cascadeActions) {
        if (!dropPhysicalCascadeAction(
                g_engine, s.currentDB, action)) {
            std::cout << "DROP VIEW CASCADE physical cleanup failed for "
                      << action.name << std::endl;
            return true;
        }
    }
    const DBStatus dropStatus =
        g_engine.dropView(s.currentDB, viewName);
    if (dropStatus != DBStatus::OK &&
        !(dropStatus == DBStatus::TABLE_NOT_FOUND && hasCatalogPlan)) {
        std::cout << "DROP VIEW physical cleanup failed" << std::endl;
        return true;
    }
    if (hasCatalogPlan) {
        std::string error;
        if (!catalog->applyDropPlan(catalogPlan, &error)) {
            std::cout << "DROP VIEW catalog cleanup failed: "
                      << error << std::endl;
            return true;
        }
        if (!catalog->persistAll()) {
            std::cout << "DROP VIEW catalog persistence failed" << std::endl;
            return true;
        }
    }

    txn.recordDrop(DdlObjectKind::View, viewName);
    if (!txn.commit()) return true;
    std::cout << "DROP VIEW succeeded" << std::endl;
    return false;
}

struct MaterializedRefreshRows {
    std::vector<StorageEngine::SqlRow> rows;
};

static bool buildMaterializedRefreshRows(
    const std::string& selectSql, const TableSchema& backingSchema,
    Session& session, MaterializedRefreshRows& output,
    std::string& error, std::string& sqlstate) {
    std::vector<std::string> selectColumns;
    std::string sourceTable;
    std::vector<std::string> conditions;
    if (!parseSimpleSelect(
            selectSql, selectColumns, sourceTable, conditions)) {
        error = "stored materialized-view query is not a supported SELECT";
        sqlstate = "0A000";
        return false;
    }

    sourceTable = resolveTableName(session, sourceTable);
    if (!g_engine.tableExists(session.currentDB, sourceTable)) {
        error = "materialized-view source relation does not exist";
        sqlstate = "42P01";
        return false;
    }
    const TableSchema sourceSchema =
        g_engine.getTableSchema(session.currentDB, sourceTable);

    std::vector<size_t> sourceColumnIndexes;
    std::vector<std::string> outputColumnNames;
    if (selectColumns.size() == 1 && selectColumns.front() == "*") {
        for (size_t index = 0; index < sourceSchema.len; ++index) {
            sourceColumnIndexes.push_back(index);
            outputColumnNames.push_back(sourceSchema.cols[index].dataName);
        }
    } else {
        for (const auto& requestedColumn : selectColumns) {
            size_t sourceIndex = sourceSchema.len;
            for (size_t index = 0; index < sourceSchema.len; ++index) {
                if (toLower(sourceSchema.cols[index].dataName) ==
                    requestedColumn) {
                    sourceIndex = index;
                    break;
                }
            }
            if (sourceIndex == sourceSchema.len) {
                error = "column \"" + requestedColumn +
                    "\" does not exist in materialized-view source";
                sqlstate = "42703";
                return false;
            }
            sourceColumnIndexes.push_back(sourceIndex);
            outputColumnNames.push_back(
                sourceSchema.cols[sourceIndex].dataName);
        }
    }

    if (sourceColumnIndexes.size() != backingSchema.len) {
        error = "stored materialized-view query no longer matches its row type";
        sqlstate = "42804";
        return false;
    }
    for (size_t index = 0; index < backingSchema.len; ++index) {
        if (toLower(backingSchema.cols[index].dataName) !=
            toLower(outputColumnNames[index])) {
            error = "stored materialized-view query no longer matches its row type";
            sqlstate = "42804";
            return false;
        }
    }

    const auto parsedConditions = StorageEngine::parseConditions(conditions);
    output.rows.clear();
    const bool scanOk = g_engine.forEachVisibleRow(
        session.currentDB, sourceTable, "SELECT",
        [&](uint32_t pageId, uint16_t slotId,
            const char* data, size_t length) {
            const int64_t rid = StorageEngine::encodeRid(pageId, slotId);
            StorageEngine::bindNullRow(
                &g_engine, session.currentDB, sourceTable, rid,
                sourceSchema.len);
            struct BindingGuard {
                ~BindingGuard() { StorageEngine::unbindNullRow(); }
            } bindingGuard;

            const std::string row(data, length);
            for (const auto& condition : parsedConditions) {
                if (!StorageEngine::evalConditionOnRow(
                        condition, row, sourceSchema)) {
                    return;
                }
            }

            StorageEngine::SqlRow values;
            for (size_t outputIndex = 0;
                 outputIndex < sourceColumnIndexes.size(); ++outputIndex) {
                const size_t sourceIndex =
                    sourceColumnIndexes[outputIndex];
                bool isNull = false;
                std::string value = g_engine.extractColumnValue(
                    row, sourceSchema, sourceIndex, session.currentDB, true,
                    &isNull);
                const std::string& targetName =
                    backingSchema.cols[outputIndex].dataName;
                if (isNull) values[targetName] = std::nullopt;
                else values[targetName] = std::move(value);
            }
            output.rows.push_back(std::move(values));
        });
    if (!scanOk) {
        error = "failed to scan materialized-view source relation";
        sqlstate = "58030";
        return false;
    }
    return true;
}

// ----------------------------------------------------------------------------
// CREATE MATERIALIZED VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateMaterializedView(const CreateViewStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string viewname = stmt->viewName;
    std::string selectSql = stmt->selectSql;
    CatalogManager::QualifiedName qualifiedName;
    if (!CatalogManager::parseQualifiedName(viewname, qualifiedName) ||
        qualifiedName.name.empty()) {
        std::cout << "ERROR: invalid materialized-view name \""
                  << viewname << "\"" << std::endl;
        return true;
    }
    const std::string schemaName = qualifiedName.schema.empty()
        ? "public" : qualifiedName.schema;
    if (!g_engine.schemaExists(s.currentDB, schemaName)) {
        std::cout << "ERROR: schema \"" << schemaName
                  << "\" does not exist" << std::endl;
        return true;
    }
    if (selectSql.empty()) {
        std::cout << "CREATE MATERIALIZED VIEW requires AS SELECT" << std::endl;
        return true;
    }

    std::vector<std::string> selectCols;
    std::string srcTable;
    std::vector<std::string> conditions;
    if (!parseSimpleSelect(selectSql, selectCols, srcTable, conditions)) {
        std::cout << "CREATE MATERIALIZED VIEW: unable to parse SELECT clause" << std::endl;
        return true;
    }

    const std::string sourceRelationName = srcTable;
    srcTable = resolveTableName(s, srcTable);
    if (!g_engine.tableExists(s.currentDB, srcTable)) {
        std::cout << "CREATE MATERIALIZED VIEW: source table not found" << std::endl;
        return true;
    }

    dbms::TableSchema srcTbl = g_engine.getTableSchema(s.currentDB, srcTable);
    std::vector<std::string> colNames;
    std::vector<size_t> selectedSourceColumns;

    if (selectCols.size() == 1 && selectCols[0] == "*") {
        for (size_t i = 0; i < srcTbl.len; ++i) {
            colNames.push_back(srcTbl.cols[i].dataName);
            selectedSourceColumns.push_back(i);
        }
    } else {
        for (const auto& cname : selectCols) {
            bool found = false;
            for (size_t i = 0; i < srcTbl.len; ++i) {
                if (toLower(srcTbl.cols[i].dataName) == cname) {
                    colNames.push_back(srcTbl.cols[i].dataName);
                    selectedSourceColumns.push_back(i);
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cout << "CREATE MATERIALIZED VIEW: column '" << cname << "' not found" << std::endl;
                return true;
            }
        }
    }

    if (colNames.empty()) {
        std::cout << "CREATE MATERIALIZED VIEW: no columns in SELECT" << std::endl;
        return true;
    }

    std::string backingTable = dbms::StorageEngine::materializedViewPrefix(viewname);
    dbms::TableSchema tbl;
    tbl.tablename = backingTable;
    tbl.owner = effectiveSessionRole(s);
    for (size_t outputColumn = 0;
         outputColumn < selectedSourceColumns.size(); ++outputColumn) {
        tbl.append(makeColumnFromSource(
            srcTbl.cols[selectedSourceColumns[outputColumn]],
            colNames[outputColumn]));
    }

    if (g_engine.tableExists(s.currentDB, backingTable)) {
        txn.markSnapshotDirty();
        g_engine.dropTable(s.currentDB, backingTable);
    }
    txn.markSnapshotDirty();
    DBStatus res = g_engine.createTable(s.currentDB, tbl);
    if (res != DBStatus::OK) {
        std::cout << "CREATE MATERIALIZED VIEW: failed to create backing table" << std::endl;
        return true;
    }

    size_t inserted = 0;
    if (stmt->withData) {
        const auto parsedConditions =
            StorageEngine::parseConditions(conditions);
        std::vector<StorageEngine::SqlRow> sourceRows;
        const bool scanOk = g_engine.forEachVisibleRow(
            s.currentDB, srcTable, "SELECT",
            [&](uint32_t pageId, uint16_t slotId,
                const char* data, size_t length) {
                const int64_t rid = StorageEngine::encodeRid(pageId, slotId);
                StorageEngine::bindNullRow(
                    &g_engine, s.currentDB, srcTable, rid, srcTbl.len);
                struct BindingGuard {
                    ~BindingGuard() { StorageEngine::unbindNullRow(); }
                } bindingGuard;

                const std::string row(data, length);
                for (const auto& condition : parsedConditions) {
                    if (!StorageEngine::evalConditionOnRow(
                            condition, row, srcTbl)) {
                        return;
                    }
                }

                StorageEngine::SqlRow values;
                for (size_t outputColumn = 0;
                     outputColumn < selectedSourceColumns.size();
                     ++outputColumn) {
                    const size_t sourceColumn =
                        selectedSourceColumns[outputColumn];
                    bool isNull = false;
                    std::string value = g_engine.extractColumnValue(
                        row, srcTbl, sourceColumn, s.currentDB, true,
                        &isNull);
                    if (isNull) {
                        values[colNames[outputColumn]] = std::nullopt;
                    } else {
                        values[colNames[outputColumn]] = std::move(value);
                    }
                }
                sourceRows.push_back(std::move(values));
            });
        if (!scanOk) {
            std::cout << "CREATE MATERIALIZED VIEW: source scan failed"
                      << std::endl;
            return true;
        }
        for (const auto& row : sourceRows) {
            if (g_engine.insertRow(s.currentDB, backingTable, row) !=
                DBStatus::OK) {
                std::cout << "CREATE MATERIALIZED VIEW: row copy failed"
                          << std::endl;
                return true;
            }
            ++inserted;
        }
    }

    // Save SQL to .mview file
    auto mviewDir = g_engine.viewsDir(s.currentDB);
    std::error_code metadataError;
    std::filesystem::create_directories(mviewDir, metadataError);
    if (metadataError) {
        std::cout << "CREATE MATERIALIZED VIEW: failed to create metadata directory"
                  << std::endl;
        return true;
    }
    auto mviewPath = mviewDir / (viewname + ".mview");
    if (!index_file::writeAtomically(mviewPath, selectSql)) {
        std::cout << "CREATE MATERIALIZED VIEW: failed to save metadata"
                  << std::endl;
        return true;
    }

    try {
        CatalogManager& catalog =
            g_engine.catalogService().get(s.currentDB);
        const Oid materializedViewOid = registerMaterializedViewInCatalog(
            catalog, tbl, schemaName, qualifiedName.name, stmt->withData);
        const PgClassRow* sourceRelation =
            catalog.resolveRelation(sourceRelationName, {"public"});
        if (sourceRelation && sourceRelation->oid != materializedViewOid) {
            PgDependRow dependency;
            dependency.classid = PgClassOid_Class;
            dependency.objid = materializedViewOid;
            dependency.objsubid = 0;
            dependency.refclassid = PgClassOid_Class;
            dependency.refobjid = sourceRelation->oid;
            dependency.refobjsubid = 0;
            dependency.deptype = 'n';
            catalog.addDepend(dependency);
        }
        if (!catalog.persistAll()) {
            throw std::runtime_error("cannot persist materialized-view catalog");
        }
    } catch (const std::exception& error) {
        std::cout << "CREATE MATERIALIZED VIEW: catalog registration failed: "
                  << error.what() << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::MaterializedView, viewname);
    if (!txn.commit()) return true;
    DmlResult result;
    result.available = true;
    result.metadataOnly = true;
    result.commandTag = stmt->withData
        ? "SELECT " + std::to_string(inserted)
        : "CREATE MATERIALIZED VIEW";
    publishLastDmlResult(std::move(result));
    std::cout << "CREATE MATERIALIZED VIEW succeeded: " << inserted << " rows" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// REFRESH MATERIALIZED VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeRefreshMaterializedView(
    const RefreshMaterializedViewStmt* stmt, Session& s) {
    if (!stmt) return true;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    const std::string requestedViewName = stmt->viewName;
    const auto materialized =
        resolveMaterializedViewForSession(s, requestedViewName);
    if (!materialized) {
        std::cout << "ERROR: relation \"" << requestedViewName
                  << "\" does not exist (SQLSTATE 42P01)" << std::endl;
        return true;
    }
    const std::string& viewName = materialized->storageName;
    // The old handler silently ran an ordinary destructive refresh after
    // stripping CONCURRENTLY. Until a snapshot-preserving two-version swap is
    // available, fail before opening a transaction or touching the backing
    // relation.
    if (stmt->concurrently) {
        std::cout << "ERROR: REFRESH MATERIALIZED VIEW CONCURRENTLY is not "
                     "supported (SQLSTATE 0A000)"
                  << std::endl;
        return true;
    }

    const std::string& backingTable = materialized->backingTable;
    const std::string selectSql =
        g_engine.getMaterializedViewSQL(s.currentDB, viewName);
    if (selectSql.empty()) {
        std::cout << "ERROR: materialized view \"" << viewName
                  << "\" has no stored query (SQLSTATE XX001)"
                  << std::endl;
        return true;
    }

    DdlTransaction transaction(s);
    transaction.enableSnapshotRollback();
    if (!transaction.begin()) {
        std::cout << "ERROR: REFRESH MATERIALIZED VIEW could not start its "
                     "atomic replacement (SQLSTATE 58030)"
                  << std::endl;
        return true;
    }

    const TableSchema backingSchema =
        g_engine.getTableSchema(s.currentDB, backingTable);
    MaterializedRefreshRows replacement;
    if (stmt->withData) {
        std::string error;
        std::string sqlstate;
        if (!buildMaterializedRefreshRows(
                selectSql, backingSchema, s, replacement,
                error, sqlstate)) {
            std::cout << "ERROR: " << error << " (SQLSTATE "
                      << sqlstate << ")" << std::endl;
            return true;
        }
    }

    transaction.markSnapshotDirty();
    DBStatus status =
        g_engine.truncateTable(s.currentDB, backingTable);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: REFRESH MATERIALIZED VIEW could not replace its "
                     "backing relation (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }

    size_t inserted = 0;
    for (const auto& row : replacement.rows) {
        status = g_engine.insertRow(s.currentDB, backingTable, row);
        if (status != DBStatus::OK) {
            std::cout << "ERROR: REFRESH MATERIALIZED VIEW row copy failed "
                         "(SQLSTATE "
                      << sqlstateForDBStatus(status) << ")" << std::endl;
            return true;
        }
        ++inserted;
    }

    try {
        CatalogManager& catalog =
            g_engine.catalogService().get(s.currentDB);
        const PgClassRow* relation = catalog.resolveRelation(
            materialized->relationName, {materialized->schemaName});
        if (!relation || relation->relkind != 'm') {
            std::cout << "ERROR: materialized-view catalog entry is missing "
                         "(SQLSTATE XX001)"
                      << std::endl;
            return true;
        }
        PgClassRow updated = *relation;
        updated.relispopulated = stmt->withData;
        if (!catalog.updateClass(updated.oid, updated) ||
            !catalog.persistAll()) {
            std::cout << "ERROR: materialized-view catalog update failed "
                         "(SQLSTATE 58030)"
                      << std::endl;
            return true;
        }
    } catch (const std::exception& error) {
        std::cout << "ERROR: materialized-view catalog update failed: "
                  << error.what() << " (SQLSTATE 58030)" << std::endl;
        return true;
    }

    transaction.recordUpdate(
        DdlObjectKind::MaterializedView, materialized->storageName);
    if (!transaction.commit()) return true;

    DmlResult result;
    result.available = true;
    result.metadataOnly = true;
    result.commandTag = "REFRESH MATERIALIZED VIEW";
    publishLastDmlResult(std::move(result));
    std::cout << "REFRESH MATERIALIZED VIEW succeeded: " << inserted
              << " rows" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// DROP MATERIALIZED VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeDropMaterializedView(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }
    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP MATERIALIZED VIEW name" << std::endl;
        return true;
    }

    struct DropTarget {
        std::string name;
        bool hasCatalogPlan = false;
        CatalogManager::DropPlan catalogPlan;
    };

    CatalogManager* catalog = nullptr;
    try {
        catalog = &g_engine.catalogService().get(s.currentDB);
    } catch (const std::exception& error) {
        std::cout << "DROP MATERIALIZED VIEW: catalog lookup failed: "
                  << error.what() << std::endl;
        return true;
    }

    std::set<std::string> requestedNames;
    std::vector<DropTarget> targets;
    for (const std::string& name : stmt->objectNames) {
        if (name.empty() || !requestedNames.insert(name).second) {
            std::cout << "DROP MATERIALIZED VIEW: duplicate or empty name"
                      << std::endl;
            return true;
        }

        const PgClassRow* relation =
            catalog->resolveRelation(name, {"public"});
        const bool physicalExists =
            g_engine.isMaterializedView(s.currentDB, name);
        const bool catalogExists = relation && relation->relkind == 'm';
        if (relation && relation->relkind != 'm') {
            std::cout << "ERROR: \"" << name
                      << "\" is not a materialized view" << std::endl;
            return true;
        }
        if (!physicalExists && !catalogExists) {
            if (stmt->ifExists) {
                std::cout << "NOTICE: materialized view \"" << name
                          << "\" does not exist, skipping" << std::endl;
                continue;
            }
            std::cout << "ERROR: materialized view \"" << name
                      << "\" does not exist (SQLSTATE 42P01)"
                      << std::endl;
            return true;
        }

        DropTarget target;
        target.name = name;
        if (catalogExists) {
            const auto behavior = stmt->cascade
                ? CatalogManager::DropBehavior::Cascade
                : CatalogManager::DropBehavior::Restrict;
            target.catalogPlan = catalog->planDrop(
                PgClassOid_Class, relation->oid, behavior);
            if (!target.catalogPlan.ok()) {
                std::cout << "ERROR: " << target.catalogPlan.error
                          << std::endl;
                return true;
            }
            // Catalog dependencies are not enough to remove file-backed
            // dependents. Fail closed until such a plan has a physical
            // worklist instead of leaving orphaned relation files behind.
            if (target.catalogPlan.objectsToDrop.size() != 1) {
                std::cout
                    << "DROP MATERIALIZED VIEW CASCADE physical cleanup is not supported"
                    << std::endl;
                return true;
            }
            target.hasCatalogPlan = true;
        }
        targets.push_back(std::move(target));
    }

    if (targets.empty()) {
        return !txn.commit();
    }

    txn.markSnapshotDirty();
    for (const auto& target : targets) {
        const DBStatus status =
            g_engine.dropMaterializedView(s.currentDB, target.name);
        if (status != DBStatus::OK) {
            std::cout << "DROP MATERIALIZED VIEW physical cleanup failed"
                      << std::endl;
            return true;
        }
        if (target.hasCatalogPlan) {
            std::string error;
            if (!catalog->applyDropPlan(target.catalogPlan, &error)) {
                std::cout << "DROP MATERIALIZED VIEW catalog cleanup failed: "
                          << error << std::endl;
                return true;
            }
        }
        txn.recordDrop(DdlObjectKind::MaterializedView, target.name);
    }

    if (!catalog->persistAll()) {
        std::cout << "DROP MATERIALIZED VIEW catalog persistence failed"
                  << std::endl;
        return true;
    }
    if (!txn.commit()) return true;
    std::cout << "DROP MATERIALIZED VIEW succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE TRIGGER
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateTrigger(const CreateTriggerStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->triggerName.empty()) {
        std::cout << "SQL syntax error: CREATE TRIGGER name" << std::endl;
        return true;
    }
    if (stmt->events.empty()) {
        std::cout << "SQL syntax error: CREATE TRIGGER event missing" << std::endl;
        return true;
    }
    if (stmt->tableName.empty()) {
        std::cout << "SQL syntax error: CREATE TRIGGER requires ON table" << std::endl;
        return true;
    }

    std::string tname = resolveTableName(s, stmt->tableName);
    const bool isTable = g_engine.tableExists(s.currentDB, tname);
    const bool isView = g_engine.viewExists(s.currentDB, tname);
    if (!isTable && !isView) {
        std::cout << "Relation " << tname << " not found" << std::endl;
        return true;
    }

    dbms::StorageEngine::Trigger trg;
    trg.name = stmt->triggerName;
    trg.timing = toLower(stmt->timing);
    trg.event = toLower(stmt->events.front());
    trg.tableName = tname;
    trg.action = stmt->action;
    if (stmt->whenCondition) trg.whenCondition = stmt->whenCondition->toString();
    trg.forEachRow = stmt->forEachRow;
    trg.transitions = stmt->transitionTableNames;

    // PostgreSQL only permits INSTEAD OF triggers on views, and they are
    // row-level triggers.  Enforce this at DDL time so the DML executor never
    // has to guess whether a stored trigger definition is valid.
    if (trg.timing == "instead of") {
        if (!isView) {
            std::cout << "INSTEAD OF triggers require a view" << std::endl;
            return true;
        }
        if (!trg.forEachRow) {
            std::cout << "INSTEAD OF triggers must be FOR EACH ROW" << std::endl;
            return true;
        }
        if (trg.event != "insert" && trg.event != "update" && trg.event != "delete") {
            std::cout << "INSTEAD OF triggers support INSERT, UPDATE, or DELETE" << std::endl;
            return true;
        }
    } else if (isView) {
        std::cout << "Only INSTEAD OF triggers are supported on views" << std::endl;
        return true;
    }

    txn.markSnapshotDirty();
    DBStatus res = g_engine.createTrigger(s.currentDB, trg);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TRIGGER failed" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Trigger, stmt->triggerName, tname);
    if (!synchronizeRelationTriggerFlagInCatalog(s.currentDB, tname)) {
        std::cout << "CREATE TRIGGER catalog update failed" << std::endl;
        return true;
    }
    if (!txn.commit()) return true;
    std::cout << "CREATE TRIGGER succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropTrigger(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;
    if (stmt->objectNames.size() != 1 || stmt->objectNames.front().empty() ||
        stmt->tableName.empty()) {
        std::cout << "SQL syntax error: DROP TRIGGER name ON table"
                  << std::endl;
        return true;
    }

    const std::string& triggerName = stmt->objectNames.front();
    const std::string tableName = resolveTableName(s, stmt->tableName);
    if (!g_engine.tableExists(s.currentDB, tableName) &&
        !g_engine.viewExists(s.currentDB, tableName)) {
        std::cout << "Relation " << tableName << " not found" << std::endl;
        return true;
    }

    std::vector<StorageEngine::Trigger> triggers;
    if (!g_engine.tryGetAllTriggers(s.currentDB, triggers)) {
        std::cout << "DROP TRIGGER metadata read failed" << std::endl;
        return true;
    }
    const bool existsOnRelation = std::any_of(
        triggers.begin(), triggers.end(),
        [&](const StorageEngine::Trigger& trigger) {
            return trigger.name == triggerName &&
                   trigger.tableName == tableName;
        });
    if (!existsOnRelation) {
        if (stmt->ifExists) {
            std::cout << "NOTICE: trigger \"" << triggerName
                      << "\" for relation \"" << stmt->tableName
                      << "\" does not exist, skipping" << std::endl;
            return false;
        }
        std::cout << "Trigger " << triggerName << " does not exist on relation "
                  << stmt->tableName << std::endl;
        return true;
    }

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }
    txn.markSnapshotDirty();
    const DBStatus status =
        g_engine.dropTrigger(s.currentDB, triggerName, tableName);
    if (status != DBStatus::OK) {
        std::cout << "DROP TRIGGER failed" << std::endl;
        return true;
    }
    txn.recordDrop(DdlObjectKind::Trigger, triggerName, tableName);
    if (!synchronizeRelationTriggerFlagInCatalog(s.currentDB, tableName)) {
        std::cout << "DROP TRIGGER catalog update failed" << std::endl;
        return true;
    }
    if (!txn.commit()) return true;
    std::cout << "DROP TRIGGER succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE FUNCTION / PROCEDURE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateFunction(const CreateFunctionStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (stmt->replace && !txn.enableSnapshotRollback()) {
        std::cout << "CREATE OR REPLACE FUNCTION could not create rollback "
                     "snapshot" << std::endl;
        return true;
    }
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->funcName.empty()) {
        std::cout << "SQL syntax error: CREATE FUNCTION name" << std::endl;
        return true;
    }
    if (stmt->body.empty()) {
        std::cout << "SQL syntax error: FUNCTION requires AS body" << std::endl;
        return true;
    }

    DBStatus res;
    char provolatile = 'v';
    if (stmt->immutable) provolatile = 'i';
    else if (stmt->stable) provolatile = 's';
    else if (stmt->volatile_) provolatile = 'v';

    const std::string lang = stmt->language.empty()
        ? "sql" : toLower(stmt->language);
    if (lang != "sql" && lang != "plpgsql") {
        std::cout << "ERROR: function language " << lang
                  << " is not supported (SQLSTATE 0A000)" << std::endl;
        return true;
    }

    const auto validateFunctionType = [&](const std::string& typeSpec,
                                          bool returnType) {
        if (trim(typeSpec).empty()) return false;
        ColumnDef definition = columnDefFromAlterType("value", typeSpec);
        const std::string base = toLower(trim(definition.typeName));
        const std::string canonical =
            TypeRegistry::instance().normalizeTypeName(base);
        if (!canonical.empty()) {
            const TypeEntry* entry =
                TypeRegistry::instance().findType(canonical);
            if (!entry) return false;
            if (entry->category == TypeCategory::Pseudo) {
                return returnType && canonical == "void";
            }
            Column column;
            return TypeRegistry::instance().resolveColumnType(
                       column, canonical, definition.typeMods, false).empty();
        }
        if (!g_engine.getDomain(s.currentDB, base).name.empty()) return true;
        if (!g_engine.getEnumType(s.currentDB, base).name.empty()) return true;
        return g_engine.isCompositeType(s.currentDB, base);
    };
    const auto canonicalFunctionType = [&](const std::string& typeSpec) {
        ColumnDef definition = columnDefFromAlterType("value", typeSpec);
        std::string base = toLower(trim(definition.typeName));
        const std::string canonical =
            TypeRegistry::instance().normalizeTypeName(base);
        if (!canonical.empty()) base = canonical;
        if (!definition.typeMods.empty()) {
            base += '(';
            for (size_t i = 0; i < definition.typeMods.size(); ++i) {
                if (i) base += ',';
                base += trim(definition.typeMods[i]);
            }
            base += ')';
        }
        return base;
    };

    for (const auto& parameter : stmt->params) {
        if (!validateFunctionType(parameter.second, false)) {
            std::cout << "ERROR: function parameter type "
                      << parameter.second
                      << " is not supported (SQLSTATE 42704)" << std::endl;
            return true;
        }
    }

    bool replacedExisting = false;
    if (toLower(stmt->returnType) == "table") {
        if (lang != "sql") {
            std::cout << "ERROR: table-valued PL/pgSQL functions are not "
                         "supported (SQLSTATE 0A000)" << std::endl;
            return true;
        }
        std::string singleParam = stmt->params.empty() ? "" : stmt->params.front().first;
        res = g_engine.createTVF(s.currentDB, stmt->funcName, singleParam, stmt->body);
    } else {
        if (!validateFunctionType(stmt->returnType, true)) {
            std::cout << "ERROR: function return type " << stmt->returnType
                      << " is not supported (SQLSTATE 42704)" << std::endl;
            return true;
        }
        const auto existing = g_engine.getUDF(s.currentDB, stmt->funcName);
        replacedExisting = stmt->replace && !existing.expression.empty();
        if (replacedExisting) {
            std::vector<std::string> requestedTypes;
            requestedTypes.reserve(stmt->params.size());
            for (const auto& parameter : stmt->params) {
                requestedTypes.push_back(
                    canonicalFunctionType(parameter.second));
            }
            std::vector<std::string> storedTypes;
            storedTypes.reserve(existing.paramTypes.size());
            for (const auto& parameterType : existing.paramTypes) {
                storedTypes.push_back(canonicalFunctionType(parameterType));
            }
            if (requestedTypes != storedTypes ||
                canonicalFunctionType(stmt->returnType) !=
                    canonicalFunctionType(existing.returnType)) {
                std::cout << "ERROR: CREATE OR REPLACE FUNCTION cannot change "
                             "argument or return types (SQLSTATE 42P13)"
                          << std::endl;
                return true;
            }
        }
        if (stmt->params.size() <= 1) {
            std::string singleParam = stmt->params.empty() ? "" : stmt->params.front().first;
            std::string singleType = stmt->params.empty() ? "" : stmt->params.front().second;
            res = g_engine.createUDF(s.currentDB, stmt->funcName, singleParam,
                                     stmt->body, provolatile, lang,
                                     stmt->returnType, singleType,
                                     stmt->strict, stmt->replace);
        } else {
            std::vector<std::string> params;
            std::vector<std::string> types;
            for (const auto& p : stmt->params) {
                params.push_back(p.first);
                types.push_back(p.second);
            }
            res = g_engine.createUDF(s.currentDB, stmt->funcName, params, types,
                                     stmt->body, provolatile, lang,
                                     stmt->returnType, stmt->strict,
                                     stmt->replace);
        }
    }

    if (res != DBStatus::OK) {
        std::cout << "CREATE FUNCTION failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }

    if (stmt->replace) txn.markSnapshotDirty();
    if (replacedExisting) {
        txn.recordUpdate(DdlObjectKind::Function, stmt->funcName);
    } else {
        txn.recordCreate(DdlObjectKind::Function, stmt->funcName);
    }
    if (!txn.commit()) return true;
    std::cout << "CREATE FUNCTION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropFunction(const DropStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    // The current function store is keyed by name, not by PostgreSQL's
    // (name, argument types) identity. Never drop a different overload just
    // because a signature was present in the statement.
    const auto& parts = stmt->objectNames;
    const bool bareName = parts.size() == 1;
    const bool zeroArgumentSignature =
        parts.size() == 3 && parts[1] == "(" && parts[2] == ")";
    if ((!bareName && !zeroArgumentSignature) || parts[0].empty()) {
        std::cout << "ERROR: DROP FUNCTION signatures or multiple targets "
                     "are not supported (SQLSTATE 0A000)" << std::endl;
        return true;
    }
    const std::string& name = parts[0];
    const bool scalarExists = g_engine.udfExists(s.currentDB, name);
    const bool tableFunctionExists = g_engine.tvfExists(s.currentDB, name);
    if (scalarExists && tableFunctionExists) {
        std::cout << "ERROR: function \"" << name
                  << "\" is ambiguous (SQLSTATE 42725)" << std::endl;
        return true;
    }
    bool signatureMatches = scalarExists || tableFunctionExists;
    if (zeroArgumentSignature && scalarExists) {
        const auto function = g_engine.getUDF(s.currentDB, name);
        if (function.expression.empty()) {
            std::cout << "ERROR: function metadata is invalid (SQLSTATE 58030)"
                      << std::endl;
            return true;
        }
        signatureMatches = function.paramTypes.empty();
    } else if (zeroArgumentSignature && tableFunctionExists) {
        signatureMatches = g_engine.getTVFParam(s.currentDB, name).empty();
    }
    if (!signatureMatches) {
        if (stmt->ifExists) {
            std::cout << "NOTICE: function \"" << name
                      << "\" does not exist, skipping" << std::endl;
            return false;
        }
        std::cout << "ERROR: function \"" << name
                  << "\" does not exist (SQLSTATE 42883)" << std::endl;
        return true;
    }
    if (stmt->cascade) {
        std::cout << "ERROR: DROP FUNCTION CASCADE is not supported "
                     "(SQLSTATE 0A000)" << std::endl;
        return true;
    }

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DROP FUNCTION transaction begin failed" << std::endl;
        return true;
    }
    txn.markSnapshotDirty();
    const DBStatus status = scalarExists
        ? g_engine.dropUDF(s.currentDB, name)
        : g_engine.dropTVF(s.currentDB, name);
    if (status != DBStatus::OK) {
        std::cout << "ERROR: DROP FUNCTION failed (SQLSTATE "
                  << sqlstateForDBStatus(status) << ")" << std::endl;
        return true;
    }
    txn.recordDrop(DdlObjectKind::Function, name);
    if (!txn.commit()) return true;
    std::cout << "DROP FUNCTION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeCreateProcedure(const CreateFunctionStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (stmt->replace && !txn.enableSnapshotRollback()) {
        std::cout << "CREATE OR REPLACE PROCEDURE could not create rollback "
                     "snapshot" << std::endl;
        return true;
    }
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->funcName.empty()) {
        std::cout << "SQL syntax error: CREATE PROCEDURE name" << std::endl;
        return true;
    }

    const std::string language = stmt->language.empty()
        ? "sql" : toLower(stmt->language);
    if (language != "sql") {
        std::cout << "ERROR: procedure language " << language
                  << " is not supported (SQLSTATE 0A000)" << std::endl;
        return true;
    }

    const auto canonicalProcedureType = [&](const std::string& typeSpec,
                                             bool* valid = nullptr) {
        ColumnDef definition = columnDefFromAlterType("value", typeSpec);
        std::string base = toLower(trim(definition.typeName));
        const std::string canonical =
            TypeRegistry::instance().normalizeTypeName(base);
        bool known = false;
        if (!canonical.empty()) {
            base = canonical;
            const TypeEntry* entry = TypeRegistry::instance().findType(base);
            Column column;
            known = entry && entry->category != TypeCategory::Pseudo &&
                TypeRegistry::instance().resolveColumnType(
                    column, base, definition.typeMods, false).empty();
        } else {
            known = !g_engine.getDomain(s.currentDB, base).name.empty() ||
                !g_engine.getEnumType(s.currentDB, base).name.empty() ||
                g_engine.isCompositeType(s.currentDB, base);
        }
        if (valid) *valid = known;
        if (!definition.typeMods.empty()) {
            base += '(';
            for (size_t i = 0; i < definition.typeMods.size(); ++i) {
                if (i) base += ',';
                base += trim(definition.typeMods[i]);
            }
            base += ')';
        }
        return base;
    };

    std::vector<dbms::StorageEngine::ProcParam> params;
    for (const auto& p : stmt->params) {
        bool valid = false;
        canonicalProcedureType(p.second, &valid);
        if (!valid) {
            std::cout << "ERROR: procedure parameter type " << p.second
                      << " is not supported (SQLSTATE 42704)" << std::endl;
            return true;
        }
        dbms::StorageEngine::ProcParam pp;
        pp.mode = "IN";
        pp.name = p.first;
        pp.type = p.second;
        params.push_back(pp);
    }

    const bool replacedExisting =
        stmt->replace && g_engine.procedureExists(s.currentDB, stmt->funcName);
    if (replacedExisting) {
        const auto existingParams =
            g_engine.getProcedureParams(s.currentDB, stmt->funcName);
        if (existingParams.size() != params.size()) {
            std::cout << "ERROR: CREATE OR REPLACE PROCEDURE cannot change "
                         "argument types (SQLSTATE 42P13)" << std::endl;
            return true;
        }
        for (size_t i = 0; i < params.size(); ++i) {
            if (canonicalProcedureType(existingParams[i].type) !=
                canonicalProcedureType(params[i].type)) {
                std::cout << "ERROR: CREATE OR REPLACE PROCEDURE cannot change "
                             "argument types (SQLSTATE 42P13)" << std::endl;
                return true;
            }
        }
    }

    std::vector<std::string> stmts;
    std::string splitError;
    if (!splitProcedureStatements(stmt->body, stmts, splitError)) {
        std::cout << "SQL syntax error: invalid PROCEDURE body: "
                  << splitError << " (SQLSTATE 42601)" << std::endl;
        return true;
    }
    if (stmts.empty()) {
        std::cout << "SQL syntax error: PROCEDURE body is empty" << std::endl;
        return true;
    }

    DBStatus res = g_engine.createProcedure(
        s.currentDB, stmt->funcName, params, stmts, stmt->replace);
    if (res != DBStatus::OK) {
        std::cout << "CREATE PROCEDURE failed (SQLSTATE "
                  << sqlstateForDBStatus(res) << ")" << std::endl;
        return true;
    }

    if (stmt->replace) txn.markSnapshotDirty();
    if (replacedExisting) {
        txn.recordUpdate(DdlObjectKind::Procedure, stmt->funcName);
    } else {
        txn.recordCreate(DdlObjectKind::Procedure, stmt->funcName);
    }
    if (!txn.commit()) return true;
    std::cout << "CREATE PROCEDURE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE POLICY
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreatePolicy(const CreatePolicyStmt* stmt, Session& s) {
    if (!stmt) return rejectMalformedDdlAst();
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->policyName.empty()) {
        std::cout << "SQL syntax error: CREATE POLICY name" << std::endl;
        return true;
    }
    if (stmt->tableName.empty()) {
        std::cout << "SQL syntax error: CREATE POLICY requires ON table" << std::endl;
        return true;
    }

    std::string tname = resolveTableName(s, stmt->tableName);
    if (!g_engine.tableExists(s.currentDB, tname)) {
        std::cout << "Table " << tname << " not found" << std::endl;
        return true;
    }

    dbms::StorageEngine::RowPolicy policy;
    policy.name = stmt->policyName;
    policy.permissive = stmt->permissive;
    policy.cmd = stmt->command.empty() ? "ALL" : stmt->command;
    policy.usingExpr = stmt->usingExpr;
    policy.withCheckExpr = stmt->withCheckExpr;
    policy.roles = stmt->roles;

    DBStatus res = g_engine.createPolicy(s.currentDB, tname, policy);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        std::cout << "Policy " << stmt->policyName << " already exists" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "CREATE POLICY failed" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Policy, stmt->policyName, tname);
    if (!txn.commit()) return true;
    std::cout << "CREATE POLICY succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// COMMENT ON
// ----------------------------------------------------------------------------

bool DdlExecutor::executeComment(const CommentStmt* stmt, Session& s) {
    if (!stmt) return true;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    const std::string objType = toLower(stmt->objectType);
    if (objType == "unknown" || objType == "function" ||
        objType == "procedure") {
        std::cout << "ERROR: COMMENT ON " << stmt->objectType
                  << " is not supported by the catalog object resolver "
                     "(SQLSTATE 0A000)" << std::endl;
        return true;
    }

    std::vector<std::string> searchPath;
    std::string canonicalSearchPath;
    if (!parseSessionSearchPath(
            s.searchPath, searchPath, canonicalSearchPath)) {
        std::cout << "ERROR: invalid search_path (SQLSTATE 22023)"
                  << std::endl;
        return true;
    }
    for (std::string& entry : searchPath) {
        entry = expandSessionSearchPathEntry(entry, s.username);
    }
    if (std::find(searchPath.begin(), searchPath.end(), "pg_catalog") ==
        searchPath.end()) {
        searchPath.insert(searchPath.begin(), "pg_catalog");
    }
    if (searchPath.empty()) searchPath.push_back("public");

    // Temporary relations are currently session-owned storage objects and do
    // not yet have pg_class rows.  Retain their existing lifecycle-aware
    // sidecar path, but never use it as a fallback for a permanent relation
    // missing catalog identity.
    if (objType == "table" || objType == "column") {
        CatalogManager::QualifiedName temporaryName;
        if (CatalogManager::parseQualifiedName(
                stmt->objectName, temporaryName) &&
            !temporaryName.name.empty()) {
            const bool temporarySchema = temporaryName.schema.empty() ||
                temporaryName.schema == "pg_temp" ||
                temporaryName.schema.rfind("pg_temp_", 0) == 0;
            const bool sessionTemporary = temporarySchema &&
                (s.tempTables.count(temporaryName.name) != 0 ||
                 s.transientTempTables.count(temporaryName.name) != 0);
            if (sessionTemporary) {
                const std::string physicalName =
                    tempTablePrefix(s, temporaryName.name);
                txn.markSnapshotDirty();
                const DBStatus status = objType == "table"
                    ? g_engine.commentOnTable(
                          s.currentDB, physicalName,
                          stmt->isNull ? std::string() : stmt->comment)
                    : g_engine.commentOnColumn(
                          s.currentDB, physicalName, stmt->columnName,
                          stmt->isNull ? std::string() : stmt->comment);
                if (status != DBStatus::OK) {
                    std::cout << "ERROR: COMMENT ON " << stmt->objectType
                              << " failed (SQLSTATE "
                              << sqlstateForDBStatus(status) << ')'
                              << std::endl;
                    return true;
                }
                txn.recordUpdate(
                    DdlObjectKind::Table, physicalName,
                    objType == "column" ? stmt->columnName : std::string());
                if (!txn.commit()) return true;
                std::cout << "COMMENT" << std::endl;
                return false;
            }
        }
    }

    Oid classOid = INVALID_OID;
    Oid objectOid = INVALID_OID;
    int32_t objectSubId = 0;
    DdlObjectKind ddlKind = DdlObjectKind::Table;
    std::string ddlName = stmt->objectName;
    std::string ddlExtra;
    std::string compatibilityStorageName;
    bool writeTableCompatibilityComment = false;
    bool writeColumnCompatibilityComment = false;

    CatalogManager* catalog = nullptr;
    try {
        catalog = &g_engine.catalogService().get(s.currentDB);
        if (objType == "table" || objType == "column" ||
            objType == "index" || objType == "view" ||
            objType == "materialized view" || objType == "sequence") {
            const PgClassRow* relation = catalog->resolveRelation(
                stmt->objectName, searchPath);
            // Embedded callers and clusters created before catalog wiring can
            // legitimately have a durable heap without pg_class identity.
            // Promote only a proven heap target before accepting COMMENT;
            // never fall back to a name-only comment row.  The surrounding
            // database snapshot makes this migration atomic with the comment.
            if (!relation && (objType == "table" || objType == "column")) {
                const std::string physicalName =
                    resolveTableName(s, stmt->objectName);
                if (g_engine.tableExists(s.currentDB, physicalName)) {
                    const CatalogManager::QualifiedName logicalName =
                        CatalogService::logicalName(physicalName);
                    const std::string logicalSchema =
                        logicalName.schema.empty()
                            ? "public" : logicalName.schema;
                    const PgNamespaceRow* nameSpace =
                        catalog->findNamespaceByName(logicalSchema);
                    const TableSchema table = g_engine.getTableSchema(
                        s.currentDB, physicalName);
                    if (!nameSpace || table.len == 0) {
                        std::cout << "ERROR: relation \""
                                  << stmt->objectName
                                  << "\" has incomplete catalog migration "
                                     "metadata (SQLSTATE XX001)"
                                  << std::endl;
                        return true;
                    }
                    txn.markSnapshotDirty();
                    registerTableInCatalog(
                        *catalog, table, logicalSchema, logicalName.name);
                    relation = catalog->findClassByName(
                        logicalName.name, nameSpace->oid);
                }
            }
            if (!relation) {
                std::cout << "ERROR: relation \"" << stmt->objectName
                          << "\" does not exist (SQLSTATE 42P01)"
                          << std::endl;
                return true;
            }
            const char expectedKind = objType == "index" ? 'i'
                : objType == "view" ? 'v'
                : objType == "materialized view" ? 'm'
                : objType == "sequence" ? 'S' : 'r';
            if (relation->relkind != expectedKind && objType != "column") {
                std::cout << "ERROR: \"" << stmt->objectName
                          << "\" is not a " << objType
                          << " (SQLSTATE 42809)" << std::endl;
                return true;
            }
            if (objType == "column" &&
                relation->relkind != 'r' && relation->relkind != 'v' &&
                relation->relkind != 'm') {
                std::cout << "ERROR: \"" << stmt->objectName
                          << "\" has no columns (SQLSTATE 42809)"
                          << std::endl;
                return true;
            }

            classOid = PgClassOid_Class;
            objectOid = relation->oid;
            if (objType == "column") {
                const PgAttributeRow* attribute = catalog->findAttribute(
                    relation->oid, stmt->columnName);
                if (!attribute) {
                    std::cout << "ERROR: column \"" << stmt->columnName
                              << "\" does not exist (SQLSTATE 42703)"
                              << std::endl;
                    return true;
                }
                objectSubId = attribute->attnum;
                ddlExtra = stmt->columnName;
            }

            const PgNamespaceRow* relationNamespace =
                catalog->findNamespace(relation->relnamespace);
            if (!relationNamespace) {
                std::cout << "ERROR: relation namespace metadata is corrupt "
                             "(SQLSTATE XX001)" << std::endl;
                return true;
            }
            compatibilityStorageName =
                relationNamespace->nspname == "public"
                    ? relation->relname
                    : relationNamespace->nspname + "__" + relation->relname;
            if (objType == "table") {
                ddlKind = DdlObjectKind::Table;
                writeTableCompatibilityComment = true;
            } else if (objType == "column") {
                ddlKind = relation->relkind == 'v'
                    ? DdlObjectKind::View
                    : relation->relkind == 'm'
                        ? DdlObjectKind::MaterializedView
                        : DdlObjectKind::Table;
                writeColumnCompatibilityComment = relation->relkind == 'r';
            } else if (objType == "index") {
                ddlKind = DdlObjectKind::Index;
            } else if (objType == "view") {
                ddlKind = DdlObjectKind::View;
            } else if (objType == "materialized view") {
                ddlKind = DdlObjectKind::MaterializedView;
            } else {
                ddlKind = DdlObjectKind::Sequence;
            }
        } else if (objType == "schema") {
            CatalogManager::QualifiedName schemaName;
            if (!CatalogManager::parseQualifiedName(
                    stmt->objectName, schemaName) ||
                !schemaName.schema.empty() || schemaName.name.empty()) {
                std::cout << "ERROR: invalid schema name (SQLSTATE 42601)"
                          << std::endl;
                return true;
            }
            const PgNamespaceRow* nameSpace =
                catalog->findNamespaceByName(schemaName.name);
            if (!nameSpace) {
                std::cout << "ERROR: schema \"" << schemaName.name
                          << "\" does not exist (SQLSTATE 3F000)"
                          << std::endl;
                return true;
            }
            classOid = PgClassOid_Namespace;
            objectOid = nameSpace->oid;
            ddlKind = DdlObjectKind::Schema;
            ddlName = schemaName.name;
        } else if (objType == "type") {
            CatalogManager::QualifiedName typeName;
            if (!CatalogManager::parseQualifiedName(
                    stmt->objectName, typeName) || typeName.name.empty()) {
                std::cout << "ERROR: invalid type name (SQLSTATE 42601)"
                          << std::endl;
                return true;
            }
            const PgTypeRow* type = nullptr;
            if (!typeName.schema.empty()) {
                const PgNamespaceRow* nameSpace =
                    catalog->findNamespaceByName(typeName.schema);
                if (nameSpace) {
                    type = catalog->findTypeByName(
                        typeName.name, nameSpace->oid);
                }
            } else {
                for (const std::string& schema : searchPath) {
                    const PgNamespaceRow* nameSpace =
                        catalog->findNamespaceByName(schema);
                    if (!nameSpace) continue;
                    type = catalog->findTypeByName(
                        typeName.name, nameSpace->oid);
                    if (type) break;
                }
            }
            if (!type) {
                std::cout << "ERROR: type \"" << stmt->objectName
                          << "\" does not exist (SQLSTATE 42704)"
                          << std::endl;
                return true;
            }
            classOid = PgClassOid_Type;
            objectOid = type->oid;
            ddlKind = DdlObjectKind::Type;
        } else {
            std::cout << "ERROR: COMMENT ON " << stmt->objectType
                      << " is not supported (SQLSTATE 0A000)"
                      << std::endl;
            return true;
        }
    } catch (const std::exception& error) {
        std::cout << "ERROR: COMMENT catalog lookup failed: "
                  << error.what() << " (SQLSTATE XX000)" << std::endl;
        return true;
    }

    if (!catalog || classOid == INVALID_OID || objectOid == INVALID_OID) {
        std::cout << "ERROR: COMMENT target has no catalog identity "
                     "(SQLSTATE XX001)" << std::endl;
        return true;
    }

    txn.markSnapshotDirty();
    if (writeTableCompatibilityComment || writeColumnCompatibilityComment) {
        const std::string compatibilityComment =
            stmt->isNull ? std::string() : stmt->comment;
        const DBStatus status = writeTableCompatibilityComment
            ? g_engine.commentOnTable(
                  s.currentDB, compatibilityStorageName,
                  compatibilityComment)
            : g_engine.commentOnColumn(
                  s.currentDB, compatibilityStorageName,
                  stmt->columnName, compatibilityComment);
        if (status != DBStatus::OK) {
            std::cout << "ERROR: COMMENT compatibility metadata update failed "
                         "(SQLSTATE " << sqlstateForDBStatus(status) << ')'
                      << std::endl;
            return true;
        }
    }

    if (stmt->isNull) {
        (void)catalog->removeDescription(
            objectOid, classOid, objectSubId);
    } else {
        catalog->setDescription(
            objectOid, classOid, objectSubId, stmt->comment);
    }
    if (!catalog->persistAll()) {
        std::cout << "ERROR: COMMENT catalog persistence failed "
                     "(SQLSTATE 58030)" << std::endl;
        return true;
    }
    txn.recordUpdate(ddlKind, ddlName, ddlExtra);
    if (!txn.commit()) return true;
    std::cout << "COMMENT" << std::endl;
    return false;
}

} // namespace dbms
