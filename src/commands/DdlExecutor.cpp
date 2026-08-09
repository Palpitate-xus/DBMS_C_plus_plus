// ============================================================================
// DDL AST Executor — Phase 4 Wave 0.3
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "parser/parser.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "common/logs.h"
#include "common/scram_sha256.h"
#include "permissions.h"
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>

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

std::string stripQuotes(const std::string& s) {
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                          (s.front() == '"' && s.back() == '"'))) {
        return s.substr(1, s.size() - 2);
    }
    return s;
}

std::string canonicalRoleName(const std::string& raw) {
    const std::string value = trim(raw);
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return stripQuotes(value);
    }
    return toLower(stripQuotes(value));
}

// ---- Shell type sidecar catalog (CREATE TYPE name) ----
// Stored in {db}/.shell_types as one type name per line.
static std::filesystem::path shellTypesPath(const std::string& dbname) {
    return g_engine.dbPath(dbname) / ".shell_types";
}

static std::vector<std::string> loadShellTypes(const std::string& dbname) {
    std::vector<std::string> names;
    std::ifstream in(shellTypesPath(dbname));
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (!line.empty()) names.push_back(line);
    }
    return names;
}

static bool shellTypeExists(const std::string& dbname, const std::string& name) {
    for (const auto& n : loadShellTypes(dbname))
        if (toLower(n) == toLower(name)) return true;
    return false;
}

static bool recordShellType(const std::string& dbname, const std::string& name) {
    if (shellTypeExists(dbname, name)) return true;
    std::ofstream out(shellTypesPath(dbname), std::ios::app);
    if (!out) return false;
    out << name << "\n";
    return true;
}

static bool removeShellType(const std::string& dbname, const std::string& name) {
    auto names = loadShellTypes(dbname);
    bool removed = false;
    {
        std::ofstream out(shellTypesPath(dbname), std::ios::trunc);
        if (!out) return false;
        for (const auto& n : names) {
            if (toLower(n) == toLower(name)) {
                removed = true;
                continue;
            }
            out << n << "\n";
        }
    }
    return removed;
}

// ---- Range / base type metadata sidecar (CREATE TYPE AS RANGE / CREATE TYPE name (...)) ----
// Format: kind|name|key=value;key=value...
struct UdtMeta {
    std::string kind;  // "range" or "base"
    std::string name;
    std::map<std::string, std::string> attrs;
};

static std::filesystem::path udtMetaPath(const std::string& dbname) {
    return g_engine.dbPath(dbname) / ".udt_meta";
}

static std::vector<UdtMeta> loadUdtMeta(const std::string& dbname) {
    std::vector<UdtMeta> out;
    std::ifstream in(udtMetaPath(dbname));
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty()) continue;
        size_t k1 = line.find('|');
        size_t k2 = line.find('|', k1 + 1);
        if (k1 == std::string::npos || k2 == std::string::npos) continue;
        UdtMeta m;
        m.kind = line.substr(0, k1);
        m.name = line.substr(k1 + 1, k2 - k1 - 1);
        std::string rest = line.substr(k2 + 1);
        std::stringstream ss(rest);
        std::string kv;
        while (std::getline(ss, kv, ';')) {
            kv = trim(kv);
            if (kv.empty()) continue;
            size_t eq = kv.find('=');
            if (eq == std::string::npos) continue;
            m.attrs[trim(kv.substr(0, eq))] = trim(kv.substr(eq + 1));
        }
        out.push_back(std::move(m));
    }
    return out;
}

static bool saveUdtMeta(const std::string& dbname, const std::vector<UdtMeta>& metas) {
    std::ofstream out(udtMetaPath(dbname), std::ios::trunc);
    if (!out) return false;
    for (const auto& m : metas) {
        out << m.kind << "|" << m.name << "|";
        bool first = true;
        for (const auto& kv : m.attrs) {
            if (!first) out << ";";
            first = false;
            out << kv.first << "=" << kv.second;
        }
        out << "\n";
    }
    return true;
}

static bool udtMetaExists(const std::string& dbname, const std::string& name,
                          const std::string& kind = "") {
    for (const auto& m : loadUdtMeta(dbname)) {
        if (toLower(m.name) == toLower(name) && (kind.empty() || toLower(m.kind) == toLower(kind)))
            return true;
    }
    return false;
}

static bool recordUdtMeta(const std::string& dbname, const UdtMeta& meta) {
    auto metas = loadUdtMeta(dbname);
    for (auto& m : metas) {
        if (toLower(m.name) == toLower(meta.name)) { m = meta; return saveUdtMeta(dbname, metas); }
    }
    metas.push_back(meta);
    return saveUdtMeta(dbname, metas);
}

static bool removeUdtMeta(const std::string& dbname, const std::string& name) {
    auto metas = loadUdtMeta(dbname);
    bool removed = false;
    std::vector<UdtMeta> kept;
    for (const auto& m : metas) {
        if (toLower(m.name) == toLower(name)) { removed = true; continue; }
        kept.push_back(m);
    }
    if (!removed) return false;
    return saveUdtMeta(dbname, kept);
}

static bool anyTypeExists(const std::string& dbname, const std::string& name) {
    return g_engine.isCompositeType(dbname, name) ||
           !g_engine.getEnumType(dbname, name).name.empty() ||
           shellTypeExists(dbname, name) ||
           udtMetaExists(dbname, name);
}

} // anonymous namespace

// ----------------------------------------------------------------------------
// Catalog registration helpers
// ----------------------------------------------------------------------------

static Oid ensureTypeInCatalog(CatalogManager& cat, Oid nspOid, const Column& col) {
    Oid typid = mapBuiltinTypeNameToOid(col.dataType);
    if (typid != INVALID_OID) return typid;

    PgTypeRow typ;
    typ.typname = col.dataType;
    typ.typnamespace = nspOid;
    typ.typlen = col.isVariableLength ? static_cast<int16_t>(-1) : static_cast<int16_t>(col.dsize);
    typ.typtype = 'b';
    typ.typcategory = 'U';
    return cat.createType(typ);
}

static void registerTableInCatalog(CatalogManager& cat, const TableSchema& tbl,
                                   const std::string& logicalSchema,
                                   const std::string& logicalName) {
    Oid nspOid = INVALID_OID;
    const auto* ns = cat.findNamespaceByName(logicalSchema);
    if (!ns) {
        // Defensive: CREATE TABLE should only reference an existing schema,
        // but create it if missing to keep catalog consistent.
        nspOid = cat.createNamespace(logicalSchema, 10); // owner=10 (bootstrap)
    } else {
        nspOid = ns->oid;
    }

    PgClassRow cls;
    cls.relname = logicalName;
    cls.relnamespace = nspOid;
    cls.relkind = 'r';
    cls.relnatts = static_cast<int16_t>(tbl.len);
    cls.relpersistence = tbl.isUnlogged ? 'u' : 'p';
    if (!tbl.owner.empty()) {
        const auto owner = authCatalog().getAuthIdByName(tbl.owner);
        if (owner) cls.relowner = owner->oid;
    }
    Oid classOid = cat.createClass(cls);

    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        PgAttributeRow attr;
        attr.attrelid = classOid;
        attr.attnum = static_cast<int16_t>(i + 1);
        attr.attname = col.dataName;
        attr.atttypid = ensureTypeInCatalog(cat, nspOid, col);
        attr.attlen = col.isVariableLength ? -1 : static_cast<int16_t>(col.dsize);
        attr.atttypmod = -1;
        attr.attnotnull = !col.isNull;
        attr.atthasdef = !col.defaultValue.empty();
        attr.attstorage = col.isVariableLength ? 'x' : 'p';
        attr.attislocal = true;
        attr.attisdropped = false;
        if (col.isAutoIncrement) attr.attidentity = 'd';
        if (!col.generatedExpr.empty()) attr.attgenerated = col.generatedKind == 'v' ? 'v' : 's';
        cat.addAttribute(attr);
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

// ----------------------------------------------------------------------------
// Public entry points
// ----------------------------------------------------------------------------

bool DdlExecutor::execute(const StmtPtr& stmt, Session& s) {
    if (!stmt) return false;
    switch (stmt->command) {
        case SqlCommand::CreateTable:
            return executeCreateTable(dynamic_cast<const CreateTableStmt*>(stmt.get()), s);
        case SqlCommand::DropTable:
            return executeDropTable(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::AlterTable:
            return executeAlterTable(dynamic_cast<const AlterTableStmt*>(stmt.get()), s);
        case SqlCommand::CreateIndex:
            return executeCreateIndex(dynamic_cast<const CreateIndexStmt*>(stmt.get()), s);
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
        case SqlCommand::CreateTrigger:
            return executeCreateTrigger(dynamic_cast<const CreateTriggerStmt*>(stmt.get()), s);
        case SqlCommand::CreateFunction:
            return executeCreateFunction(dynamic_cast<const CreateFunctionStmt*>(stmt.get()), s);
        case SqlCommand::CreateProcedure:
            return executeCreateProcedure(dynamic_cast<const CreateFunctionStmt*>(stmt.get()), s);
        case SqlCommand::CreatePolicy:
            return executeCreatePolicy(dynamic_cast<const CreatePolicyStmt*>(stmt.get()), s);
        case SqlCommand::CreateMaterializedView:
            return executeCreateMaterializedView(dynamic_cast<const CreateViewStmt*>(stmt.get()), s);
        case SqlCommand::CreateCollation:
            return executeCreateCollation(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::DropCollation:
            return executeDropCollation(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateDatabase:
            return executeCreateDatabase(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
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
        std::cout << std::endl;
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
        case dbms::SqlCommand::CreateSequence:
        case dbms::SqlCommand::DropSequence:
        case dbms::SqlCommand::CreateDomain:
        case dbms::SqlCommand::DropDomain:
        case dbms::SqlCommand::CreateType:
        case dbms::SqlCommand::DropType:
        case dbms::SqlCommand::CreateView:
        case dbms::SqlCommand::CreateTrigger:
        case dbms::SqlCommand::CreateFunction:
        case dbms::SqlCommand::CreateProcedure:
        case dbms::SqlCommand::CreatePolicy:
        case dbms::SqlCommand::CreateMaterializedView:
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
    const std::string& parseInput = authDdl && !rawSql.empty() ? rawSql : sql;
    dbms::SQLParser parser;
    dbms::ParseResult r = parser.parse(parseInput);
    if (!r.success || !r.stmt) {
        // A bridge-owned command must fail closed. Falling back after a parse
        // error can execute a different legacy interpretation.
        handled = true;
        std::cout << "SQL syntax error";
        if (!r.error.empty()) std::cout << ": " << r.error;
        std::cout << std::endl;
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

void DdlExecutor::checkAndImplicitCommit(Session& s) {
    (void)s;
    if (g_engine.inTransaction()) {
        g_engine.commitTransaction();
        std::cout << "Note: DDL caused implicit commit of open transaction" << std::endl;
    }
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

    checkAndImplicitCommit(s);
    const std::string tableName = resolveTableName(s, stmt->tableName);

    // Validate the action set before making any change.  The storage
    // primitives currently persist each subcommand independently, so a later
    // semantic failure can still leave an earlier supported subcommand
    // applied; full statement-atomic DDL remains a PostgreSQL-alignment gap.
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
        DBStatus status = DBStatus::OK;
        switch (sub.action) {
            case AlterTableStmt::Action::AddColumn: {
                if (sub.colDef.name.empty() || sub.colDef.typeName.empty()) {
                    std::cout << "SQL syntax error: ADD COLUMN requires name and type" << std::endl;
                    return true;
                }
                status = g_engine.alterTableAddColumn(
                    s.currentDB, tableName, columnDefToColumn(sub.colDef, s.currentDB));
                if (status == DBStatus::TABLE_ALREADY_EXISTS && sub.ifNotExists) {
                    std::cout << "NOTICE: column already exists, skipping" << std::endl;
                    break;
                }
                if (!alterStatusOk(status, "Column")) return true;
                break;
            }
            case AlterTableStmt::Action::DropColumn:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: DROP COLUMN requires a name" << std::endl;
                    return true;
                }
                status = g_engine.alterTableDropColumn(s.currentDB, tableName, sub.name);
                if (status == DBStatus::INVALID_VALUE && sub.ifExists) {
                    std::cout << "NOTICE: column does not exist, skipping" << std::endl;
                    break;
                }
                if (!alterStatusOk(status, "Column")) return true;
                break;
            case AlterTableStmt::Action::RenameColumn:
                if (sub.name.empty() || sub.newName.empty()) {
                    std::cout << "SQL syntax error: RENAME COLUMN requires two names" << std::endl;
                    return true;
                }
                status = g_engine.alterTableRenameColumn(s.currentDB, tableName,
                                                         sub.name, sub.newName);
                if (status == DBStatus::INVALID_VALUE && sub.ifExists) {
                    std::cout << "NOTICE: column does not exist, skipping" << std::endl;
                    break;
                }
                if (!alterStatusOk(status, "Column")) return true;
                break;
            case AlterTableStmt::Action::RenameConstraint:
                if (sub.name.empty() || sub.newName.empty()) {
                    std::cout << "SQL syntax error: RENAME CONSTRAINT requires two names" << std::endl;
                    return true;
                }
                status = g_engine.alterTableRenameConstraint(s.currentDB, tableName,
                                                             sub.name, sub.newName);
                if (status == DBStatus::INVALID_VALUE && sub.ifExists) {
                    std::cout << "NOTICE: constraint does not exist, skipping" << std::endl;
                    break;
                }
                if (!alterStatusOk(status, "Constraint")) return true;
                break;
            case AlterTableStmt::Action::RenameTable:
                if (sub.newName.empty()) {
                    std::cout << "SQL syntax error: RENAME TO requires a table name" << std::endl;
                    return true;
                }
                status = g_engine.alterTableRenameTable(s.currentDB, tableName, sub.newName);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::AlterColumn: {
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: ALTER COLUMN requires a name" << std::endl;
                    return true;
                }
                if (sub.defaultValue) {
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
                    status = g_engine.alterTableAlterColumnType(
                        s.currentDB, tableName, sub.name, columnDefToColumn(cd, s.currentDB));
                } else {
                    std::cout << "ALTER COLUMN subcommand is unsupported" << std::endl;
                    return true;
                }
                if (!alterStatusOk(status, "Column")) return true;
                break;
            }
            case AlterTableStmt::Action::SetStatistics: {
                if (sub.name.empty() || sub.statisticsTarget < 0 || sub.statisticsTarget > 10000) {
                    std::cout << "Invalid statistics target" << std::endl;
                    return true;
                }
                std::map<std::string, std::string> params;
                params["column_statistics:" + sub.name] = std::to_string(sub.statisticsTarget);
                g_engine.setStorageParams(s.currentDB, tableName, params);
                break;
            }
            case AlterTableStmt::Action::SetLogged:
                status = g_engine.alterTableSetLogged(s.currentDB, tableName, true);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::SetUnlogged:
                status = g_engine.alterTableSetLogged(s.currentDB, tableName, false);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::EnableRowLevelSecurity:
                status = g_engine.enableRowLevelSecurity(s.currentDB, tableName, false);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::DisableRowLevelSecurity:
                status = g_engine.disableRowLevelSecurity(s.currentDB, tableName);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::ForceRowLevelSecurity:
                status = g_engine.enableRowLevelSecurity(s.currentDB, tableName, true);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::NoForceRowLevelSecurity:
                status = g_engine.enableRowLevelSecurity(s.currentDB, tableName, false);
                if (!alterStatusOk(status, "Table")) return true;
                break;
            case AlterTableStmt::Action::EnableTrigger:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: ENABLE TRIGGER requires a name" << std::endl;
                    return true;
                }
                status = g_engine.enableTrigger(s.currentDB, sub.name);
                if (!alterStatusOk(status, "Trigger")) return true;
                break;
            case AlterTableStmt::Action::DisableTrigger:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: DISABLE TRIGGER requires a name" << std::endl;
                    return true;
                }
                status = g_engine.disableTrigger(s.currentDB, sub.name);
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
                break;
            case AlterTableStmt::Action::DetachPartition:
                if (sub.name.empty()) {
                    std::cout << "SQL syntax error: DETACH PARTITION requires a name" << std::endl;
                    return true;
                }
                status = g_engine.detachPartition(s.currentDB, tableName, sub.name);
                if (!alterStatusOk(status, "Partition")) return true;
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
                TableConstraint metadata;
                metadata.name = constraintName;
                metadata.type = tc.type;
                metadata.columns = tc.columns;
                metadata.refTable = tc.refTable;
                metadata.refColumns = tc.refColumns;
                metadata.accessMethod = tc.accessMethod;
                metadata.excludeElements = tc.excludeElements;
                metadata.excludeWhere = tc.excludeWhere;
                recordConstraintCompat(s.currentDB, tableName, metadata);
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
                removeConstraintCompat(s.currentDB, tableName, sub.name);
                break;
            }
            case AlterTableStmt::Action::SetOptions:
            case AlterTableStmt::Action::ResetOptions: {
                std::map<std::string, std::string> params;
                for (const auto& option : sub.options) params[option.first] = option.second;
                g_engine.setStorageParams(s.currentDB, tableName, params);
                break;
            }
            case AlterTableStmt::Action::Inherit:
            case AlterTableStmt::Action::NoInherit: {
                if (sub.parentTable.empty()) {
                    std::cout << "INHERIT requires a parent table" << std::endl;
                    return true;
                }
                auto path = std::filesystem::path(g_engine.dbPath(s.currentDB)) /
                            ("." + tableName + ".inherits");
                std::vector<std::string> parents;
                if (std::filesystem::exists(path)) {
                    std::ifstream in(path);
                    std::string parent;
                    while (std::getline(in, parent)) if (!parent.empty()) parents.push_back(parent);
                }
                if (sub.action == AlterTableStmt::Action::Inherit) {
                    if (std::find(parents.begin(), parents.end(), sub.parentTable) == parents.end())
                        parents.push_back(sub.parentTable);
                } else {
                    parents.erase(std::remove(parents.begin(), parents.end(), sub.parentTable), parents.end());
                }
                std::ofstream out(path, std::ios::trunc);
                for (const auto& parent : parents) out << parent << '\n';
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
    std::cout << "ALTER TABLE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP DATABASE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateDatabase(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    checkAndImplicitCommit(s);
    std::string dbname = stmt->objectName;
    if (dbname.empty()) {
        std::cout << "SQL syntax error: CREATE DATABASE name" << std::endl;
        return true;
    }
    std::string charset = "utf8";
    auto it = stmt->options.find("encoding");
    if (it != stmt->options.end()) charset = it->second;
    DBStatus res = g_engine.createDatabase(dbname, charset);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        std::cout << "Database already exists" << std::endl;
        return true;
    }
    std::cout << "CREATE DATABASE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropDatabase(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    checkAndImplicitCommit(s);
    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP DATABASE name" << std::endl;
        return true;
    }
    std::string dbname = stmt->objectNames.front();
    if (dbname == s.currentDB) s.currentDB.clear();

    // Persist and drop the in-memory catalog before removing the directory.
    g_engine.catalogService().evict(dbname);

    DBStatus res = g_engine.dropDatabase(dbname);
    if (res == DBStatus::NOT_FOUND) {
        std::cout << "Database not found" << std::endl;
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

    checkAndImplicitCommit(s);
    for (const auto& name : targets) {
        const DBStatus result = g_engine.truncateTable(s.currentDB, name);
        if (result != DBStatus::OK) {
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
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
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
    if (res != DBStatus::OK) {
        std::cout << "CREATE SCHEMA failed" << std::endl;
        return true;
    }

    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        cat.createNamespace(name, INVALID_OID);
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog schema registration failed: " << e.what() << std::endl;
    }

    txn.recordCreate(DdlObjectKind::Schema, name);
    txn.commit();
    std::cout << "CREATE SCHEMA succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropSchema(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP SCHEMA name" << std::endl;
        return true;
    }
    std::string name = stmt->objectNames.front();
    txn.recordDrop(DdlObjectKind::Schema, name);

    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        const auto* ns = cat.findNamespaceByName(name);
        if (ns) {
            auto behavior = stmt->cascade ? CatalogManager::DropBehavior::Cascade
                                          : CatalogManager::DropBehavior::Restrict;
            std::string err;
            bool ok = cat.dropObject(PgClassOid_Namespace, ns->oid, behavior, &err);
            if (!ok) {
                std::cout << "ERROR: " << err << std::endl;
                return true;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog schema drop failed: " << e.what() << std::endl;
    }

    DBStatus res = g_engine.dropSchema(s.currentDB, name, stmt->cascade);
    if (res != DBStatus::OK) {
        std::cout << "DROP SCHEMA failed" << std::endl;
        return true;
    }
    txn.commit();
    std::cout << "DROP SCHEMA succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP TABLE
// ----------------------------------------------------------------------------

Column DdlExecutor::columnDefToColumn(const ColumnDef& cd, const std::string& dbname) {
    Column col;
    col.dataName = cd.name;
    col.isNull = cd.isNull;
    col.isPrimaryKey = cd.isPrimaryKey;
    col.isUnique = cd.isUnique;
    col.isArray = cd.isArray;
    col.defaultValue = cd.defaultValue ? cd.defaultValue->toString() : "";

    col.isAutoIncrement = cd.isGeneratedIdentity;
    col.generatedExpr = cd.generatedExpr;
    col.generatedKind = cd.generatedKind;

    std::string baseType = toLower(cd.typeName);
    std::string domainName;
    std::string domainCheck;
    std::vector<std::string> enumValues;
    if (!dbname.empty()) {
        auto dom = g_engine.getDomain(dbname, baseType);
        if (!dom.name.empty()) {
            domainName = baseType;
            domainCheck = dom.checkExpr;
            if (col.defaultValue.empty() && !dom.defaultValue.empty()) {
                col.defaultValue = dom.defaultValue;
            }
            baseType = toLower(dom.baseType);
        }
        auto et = g_engine.getEnumType(dbname, baseType);
        if (!et.name.empty()) {
            enumValues = et.labels;
            baseType = "varchar"; // store enum values as strings
        }
    }
    // Normalize common aliases
    if (baseType == "int" || baseType == "integer") baseType = "int4";
    else if (baseType == "bigint") baseType = "int8";
    else if (baseType == "smallint") baseType = "int2";
    else if (baseType == "tinyint") baseType = "smallint";
    else if (baseType == "real") baseType = "float4";
    else if (baseType == "double" || baseType == "double precision") baseType = "float8";
    else if (baseType == "varchar" || baseType == "character varying" || baseType == "nvarchar") baseType = "varchar";
    else if (baseType == "char" || baseType == "character" || baseType == "nchar") baseType = "char";
    else if (baseType == "bool") baseType = "boolean";
    else if (baseType == "datetime") baseType = "timestamp";

    int typeMod1 = 0, typeMod2 = 0;
    if (!cd.typeMods.empty()) {
        try { typeMod1 = std::stoi(cd.typeMods[0]); } catch (...) {}
    }
    if (cd.typeMods.size() > 1) {
        try { typeMod2 = std::stoi(cd.typeMods[1]); } catch (...) {}
    }

    if (baseType == "int2" || baseType == "smallint") {
        col = makeIntColumn(cd.name, cd.isNull, 0, cd.isPrimaryKey);
    } else if (baseType == "int4" || baseType == "integer" || baseType == "int") {
        col = makeIntColumn(cd.name, cd.isNull, 2, cd.isPrimaryKey);
    } else if (baseType == "int8" || baseType == "bigint") {
        col = makeIntColumn(cd.name, cd.isNull, 3, cd.isPrimaryKey);
    } else if (baseType == "varchar" || baseType == "character varying") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 255;
        col = makeVarCharColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
    } else if (baseType == "char" || baseType == "character") {
        size_t len = typeMod1 > 0 ? static_cast<size_t>(typeMod1) : 1;
        col = makeStringColumn(cd.name, cd.isNull, len, cd.isPrimaryKey);
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
    } else if (baseType == "date") {
        col = makeDateColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "timestamp" || baseType == "datetime") {
        col = makeTimestampColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "timestamptz") {
        col = makeTimestamptzColumn(cd.name, cd.isNull, cd.isPrimaryKey);
    } else if (baseType == "time") {
        col = makeTimeColumn(cd.name, cd.isNull, cd.isPrimaryKey);
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
        // Unknown type: fall back to varchar so the table can still be created
        col = makeVarCharColumn(cd.name, cd.isNull, 255, cd.isPrimaryKey);
    }

    // Factory functions replace the whole Column; restore metadata they don't set.
    col.defaultValue = cd.defaultValue ? cd.defaultValue->toString() : "";
    col.generatedExpr = cd.generatedExpr;
    col.generatedKind = cd.generatedKind;
    col.isAutoIncrement = cd.isGeneratedIdentity;
    col.isUnique = cd.isUnique;
    col.isArray = cd.isArray;
    col.enumValues = enumValues;
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
    return col;
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

void DdlExecutor::recordConstraintCompat(const std::string& dbname,
                                         const std::string& tablename,
                                         const TableConstraint& tc) {
    // Persist named constraints for compatibility with legacy constraint files.
    if (tc.name.empty()) return;
    auto path = std::filesystem::path(g_engine.dbPath(dbname)) /
                (tablename + ".constraints");
    std::ofstream ofs(path, std::ios::app);
    if (!ofs) return;
    ofs << tc.name << "|" << toLower(tc.type);
    for (const auto& c : tc.columns) ofs << "|" << c;
    if (!tc.refTable.empty()) {
        ofs << "|" << tc.refTable;
        for (const auto& c : tc.refColumns) ofs << "|" << c;
    }
    ofs << "\n";
}

bool DdlExecutor::removeConstraintCompat(const std::string& dbname,
                                         const std::string& tablename,
                                         const std::string& constraintName) {
    const auto path = std::filesystem::path(g_engine.dbPath(dbname)) /
                      (tablename + ".constraints");
    if (!std::filesystem::exists(path)) return false;

    std::ifstream in(path);
    if (!in) return false;
    std::vector<std::string> kept;
    std::string line;
    bool removed = false;
    while (std::getline(in, line)) {
        const auto separator = line.find('|');
        if (separator != std::string::npos && line.substr(0, separator) == constraintName) {
            removed = true;
            continue;
        }
        kept.push_back(std::move(line));
    }
    if (!removed) return false;

    std::ofstream out(path, std::ios::trunc);
    if (!out) return false;
    for (const auto& keptLine : kept) out << keptLine << '\n';
    return static_cast<bool>(out);
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
    return col;
}

static bool executeCreateTableAs(const CreateTableStmt* stmt, Session& s,
                                 const std::string& tname) {
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

    std::set<std::string> queryCols;
    if (selectCols.size() == 1 && selectCols[0] == "*") {
        for (size_t i = 0; i < srcTbl.len; ++i) {
            newTbl.append(makeColumnFromSource(srcTbl.cols[i], srcTbl.cols[i].dataName));
            queryCols.insert(srcTbl.cols[i].dataName);
        }
    } else {
        for (const auto& cname : selectCols) {
            bool found = false;
            for (size_t i = 0; i < srcTbl.len; ++i) {
                if (toLower(srcTbl.cols[i].dataName) == cname) {
                    newTbl.append(makeColumnFromSource(srcTbl.cols[i], srcTbl.cols[i].dataName));
                    queryCols.insert(srcTbl.cols[i].dataName);
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

    DBStatus res = g_engine.createTable(s.currentDB, newTbl);
    if (res != DBStatus::OK) {
        std::cout << "CTAS: create table failed" << std::endl;
        return true;
    }

    auto rows = g_engine.query(s.currentDB, srcTable, conditions, queryCols, {});
    // StorageEngine::query emits values in SOURCE SCHEMA order (filtered to
    // queryCols), NOT in the alphabetical order of the queryCols set. Build the
    // value->column mapping in that same schema order so columns line up.
    std::vector<std::string> orderedCols;
    for (size_t i = 0; i < srcTbl.len; ++i) {
        if (queryCols.count(srcTbl.cols[i].dataName))
            orderedCols.push_back(srcTbl.cols[i].dataName);
    }
    size_t inserted = 0;
    if (stmt->withData) {
        for (const auto& row : rows) {
            std::map<std::string, std::string> values;
            std::istringstream iss(row);
            std::string val;
            size_t idx = 0;
            while (iss >> val && idx < orderedCols.size()) {
                if (val == "NULL") val = "";
                values[orderedCols[idx]] = val;
                ++idx;
            }
            if (idx != orderedCols.size()) continue;
            if (g_engine.insert(s.currentDB, tname, values) == DBStatus::OK) ++inserted;
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
    std::vector<std::pair<std::string, std::string>> deps;
    for (const auto& tname : g_engine.getTableNames(dbname)) {
        dbms::TableSchema tbl = g_engine.getTableSchema(dbname, tname);
        for (size_t i = 0; i < tbl.len; ++i) {
            std::string seq = extractNextvalSequence(tbl.cols[i].defaultValue);
            // Support both bare sequence name and schema-qualified name.
            std::string bareSeq = seq;
            size_t dot = seq.find('.');
            if (dot != std::string::npos) bareSeq = seq.substr(dot + 1);
            if (bareSeq == seqname || seq == seqname) {
                deps.emplace_back(tname, tbl.cols[i].dataName);
            }
        }
    }
    return deps;
}

bool DdlExecutor::executeCreateTable(const CreateTableStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string tname = resolveTableName(s, stmt->tableName);
    if (g_engine.tableExists(s.currentDB, tname) || g_engine.viewExists(s.currentDB, tname)) {
        if (stmt->ifNotExists) {
            std::cout << "NOTICE: table \"" << tname << "\" already exists, skipping" << std::endl;
            return false;
        }
        std::cout << "Table " << tname << " already exists" << std::endl;
        return true;
    }

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
        child.partitionType = TableSchema::PartitionType::None;
        child.partitionKey.clear();
        child.rangePartitions.clear();
        child.listPartitions.clear();
        child.hashPartitions = 0;
        child.defaultPartitionName.clear();
        child.subPartitionType = TableSchema::PartitionType::None;
        child.subPartitionKey.clear();
        child.subHashPartitions = 0;

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

        try {
            CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
            CatalogManager::QualifiedName qn;
            if (!CatalogManager::parseQualifiedName(stmt->tableName, qn)) {
                qn.schema = "";
                qn.name = stmt->tableName;
            }
            if (qn.schema.empty()) qn.schema = "public";
            registerTableInCatalog(cat, child, qn.schema, qn.name);
        } catch (const std::exception& e) {
            std::cerr << "WARNING: partition catalog registration failed: "
                      << e.what() << std::endl;
        }
        g_engine.applyDefaultPrivileges(s.currentDB, "public", "table", tname,
                                        effectiveSessionRole(s));
        txn.commit();
        return false;
    }

    // CREATE TABLE ... AS SELECT ...
    if (!stmt->asSelect.empty()) {
        bool err = executeCreateTableAs(stmt, s, tname);
        if (!err) {
            try {
                dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
                registerTableInCatalog(cat, g_engine.getTableSchema(s.currentDB, tname), tname, s.currentDB);
            } catch (const std::exception& e) {
                std::cerr << "WARNING: CTAS catalog registration failed: " << e.what() << std::endl;
            }
            txn.recordCreate(DdlObjectKind::Table, tname);
            txn.commit();
        }
        return err;
    }

    TableSchema tbl;
    tbl.tablename = tname;
    tbl.owner = effectiveSessionRole(s);
    tbl.isUnlogged = stmt->unlogged;
    tbl.tablespace = stmt->tablespace.empty() ? "pg_default" : stmt->tablespace;
    tbl.storageParams = stmt->options;

    // CREATE TABLE ... (LIKE source [INCLUDING ...]) — copy source columns first.
    // Plain LIKE copies column definitions + NOT NULL + collation only. DEFAULTS,
    // CHECK constraints, identity and PK/UNIQUE are copied only with the matching
    // INCLUDING option (or INCLUDING ALL).
    for (const auto& lc : stmt->likeClauses) {
        std::string src = resolveTableName(s, lc.tableName);
        if (!g_engine.tableExists(s.currentDB, src)) {
            std::cout << "LIKE source table " << lc.tableName << " not found" << std::endl;
            return true;
        }
        bool inclDefaults = lc.includingAll || lc.includingDefaults;
        bool inclConstraints = lc.includingAll || lc.includingConstraints;
        bool inclIndexes = lc.includingAll || lc.includingIndexes;
        bool inclIdentity = lc.includingAll || lc.includingIdentity;
        TableSchema srcSchema = g_engine.getTableSchema(s.currentDB, src);
        for (size_t i = 0; i < srcSchema.len && tbl.len < MAX_COLUMNS; ++i) {
            Column c = srcSchema.cols[i];
            if (!inclDefaults) c.defaultValue.clear();
            if (!inclConstraints) { c.checkExpr.clear(); c.checkConstraintName.clear(); }
            if (!inclIdentity) c.isAutoIncrement = false;
            if (!inclIndexes) { c.isPrimaryKey = false; c.isUnique = false; }
            tbl.append(c);
            if (inclIndexes && c.isPrimaryKey) tbl.pkColIndices.push_back(tbl.len - 1);
        }
    }

    for (const auto& cd : stmt->columns) {
        tbl.append(columnDefToColumn(cd, s.currentDB));
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
            if (tbl.len >= MAX_COLUMNS) break;
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
            tbl.append(columnDefToColumn(cd, s.currentDB));
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

    // Table-level constraints
    for (const auto& tc : stmt->constraints) {
        std::string t = toLower(tc.type);
        if (t == "primary key") {
            for (const auto& cname : tc.columns) {
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == cname) {
                        tbl.cols[i].isPrimaryKey = true;
                        tbl.pkColIndices.push_back(i);
                    }
                }
            }
        } else if (t == "unique") {
            std::vector<size_t> idxs;
            for (const auto& cname : tc.columns) {
                for (size_t i = 0; i < tbl.len; ++i) {
                    if (tbl.cols[i].dataName == cname) idxs.push_back(i);
                }
            }
            if (!idxs.empty()) {
                tbl.uniqueConstraints.push_back(idxs);
                tbl.uniqueConstraintNames.push_back(tc.name);
            }
        } else if (t == "foreign key") {
            tbl.appendFK(tableConstraintToForeignKey(tc));
            recordConstraintCompat(s.currentDB, tname, tc);
        } else if (t == "check") {
            if (tbl.len > 0) {
                if (tbl.cols[0].checkExpr.empty()) {
                    tbl.cols[0].checkExpr = tc.checkExpr ? tc.checkExpr->toString() : "";
                    tbl.cols[0].checkConstraintName = tc.name;
                    tbl.cols[0].deferrable = tc.deferrable;
                    tbl.cols[0].initiallyDeferred = tc.initiallyDeferred;
                }
            }
            recordConstraintCompat(s.currentDB, tname, tc);
        } else if (t == "exclude") {
            // Defer creation until the table exists; collect for later.
        }
    }

    DBStatus res = g_engine.createTable(s.currentDB, tbl);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TABLE failed" << std::endl;
        return true;
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
        g_engine.createExclusionConstraint(s.currentDB, ec);
        recordConstraintCompat(s.currentDB, tname, tc);
    }
    g_engine.applyDefaultPrivileges(s.currentDB, "public", "table", tname,
                                    effectiveSessionRole(s));

    // Register the table in the catalog (best-effort; storage is the authority).
    try {
        CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        CatalogManager::QualifiedName qn;
        if (!CatalogManager::parseQualifiedName(stmt->tableName, qn)) {
            qn.schema = "";
            qn.name = stmt->tableName;
        }
        if (qn.schema.empty()) qn.schema = "public";
        registerTableInCatalog(cat, tbl, qn.schema, qn.name);
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog registration failed: " << e.what() << std::endl;
    }

    // Register sequence ownership for columns with DEFAULT nextval('seqname').
    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        const auto* nsPublic = cat.findNamespaceByName("public");
        auto tableRel = cat.resolveRelation(tname, {"public"});
        if (nsPublic && tableRel) {
            for (size_t i = 0; i < tbl.len; ++i) {
                std::string seq = extractNextvalSequence(tbl.cols[i].defaultValue);
                if (seq.empty()) continue;
                // Use bare sequence name for catalog lookup.
                std::string bareSeq = seq;
                size_t dot = bareSeq.rfind('.');
                if (dot != std::string::npos) bareSeq = bareSeq.substr(dot + 1);
                auto seqRel = cat.resolveRelation(bareSeq, {"public"});
                if (!seqRel) continue;
                PgDependRow dep;
                dep.classid = dbms::PgClassOid_Class;
                dep.objid = seqRel->oid;
                dep.objsubid = 0;
                dep.refclassid = dbms::PgClassOid_Class;
                dep.refobjid = tableRel->oid;
                dep.refobjsubid = 0;
                dep.deptype = 'a';
                cat.addDepend(dep);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: sequence ownership registration failed: " << e.what() << std::endl;
    }

    txn.recordCreate(DdlObjectKind::Table, tname);
    txn.commit();
    std::cout << "CREATE TABLE succeeded" << std::endl;
    return false;
}

// Drop any sequence files owned by the named table.
static void dropOwnedSequences(const std::string& dbname,
                               const std::string& logicalTableName) {
    auto dir = std::filesystem::path(dbname);
    if (!std::filesystem::exists(dir)) return;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (!entry.is_regular_file()) continue;
        std::string fname = entry.path().filename().string();
        if (fname.size() <= 4 || fname.substr(fname.size() - 4) != ".seq") continue;
        std::string seqname = fname.substr(0, fname.size() - 4);
        std::ifstream ifs(entry.path());
        if (!ifs) continue;
        std::vector<std::string> tokens;
        std::string tok;
        while (ifs >> tok) tokens.push_back(tok);
        if (tokens.size() >= 10) {
            if (tokens[8] == logicalTableName) {
                std::filesystem::remove(entry.path());
            }
        }
    }
}

bool DdlExecutor::executeDropTable(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP TABLE name" << std::endl;
        return true;
    }
    std::string tname = resolveTableName(s, stmt->objectNames.front());
    if (!g_engine.tableExists(s.currentDB, tname)) {
        if (stmt->ifExists) {
            std::cout << "NOTICE: table \"" << tname << "\" does not exist, skipping" << std::endl;
            return false;
        }
        std::cout << "Table " << tname << " not found" << std::endl;
        return true;
    }

    // Catalog-side CASCADE/RESTRICT check.
    try {
        CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        auto qn = CatalogService::logicalName(tname);
        std::string logicalName = qn.schema.empty() ? qn.name : (qn.schema + "." + qn.name);
        const PgClassRow* cls = cat.resolveRelation(logicalName, {"public"});
        if (cls) {
            auto behavior = stmt->cascade
                                ? CatalogManager::DropBehavior::Cascade
                                : CatalogManager::DropBehavior::Restrict;
            std::string err;
            bool ok = cat.dropObject(PgClassOid_Class, cls->oid, behavior, &err);
            if (!ok) {
                std::cout << "ERROR: " << err << std::endl;
                return true;
            }
            if (stmt->cascade) {
                dropOwnedSequences(s.currentDB, logicalName);
            }
        } else {
            std::cout << "NOTICE: table \"" << tname
                      << "\" has no catalog entry; falling back to storage drop" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog drop check failed: " << e.what() << std::endl;
    }

    txn.recordDrop(DdlObjectKind::Table, tname);
    DBStatus res = g_engine.dropTable(s.currentDB, tname);
    if (res != DBStatus::OK) {
        std::cout << "DROP TABLE failed" << std::endl;
        return true;
    }
    txn.commit();
    std::cout << "DROP TABLE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE INDEX
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateIndex(const CreateIndexStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string tname = resolveTableName(s, stmt->tableName);
    if (!g_engine.tableExists(s.currentDB, tname)) {
        std::cout << "Table " << tname << " not found" << std::endl;
        return true;
    }

    std::vector<std::string> colnames;
    for (const auto& elem : stmt->columns) {
        colnames.push_back(elem.column);
    }

    std::string whereCondition = stmt->whereClause ? stmt->whereClause->toString() : "";
    std::vector<std::string> includeCols = stmt->includeCols;

    DBStatus res;
    std::string am = toLower(stmt->accessMethod);
    if (am.empty() || am == "btree") {
        if (colnames.size() == 1) {
            res = g_engine.createIndex(s.currentDB, tname, colnames.front(), true,
                                       includeCols, whereCondition, "", stmt->concurrently);
        } else {
            res = g_engine.createCompositeIndex(s.currentDB, tname, colnames,
                                                stmt->indexName, includeCols,
                                                whereCondition, stmt->concurrently);
        }
    } else if (am == "hash") {
        if (colnames.size() == 1) {
            res = g_engine.createHashIndex(s.currentDB, tname, colnames.front());
        } else {
            std::cout << "HASH index only supports single column" << std::endl;
            return true;
        }
    } else {
        if (colnames.size() == 1) {
            res = g_engine.createIndex(s.currentDB, tname, colnames.front(), true,
                                       includeCols, whereCondition, "", stmt->concurrently);
        } else {
            res = g_engine.createCompositeIndex(s.currentDB, tname, colnames,
                                                stmt->indexName, includeCols,
                                                whereCondition, stmt->concurrently);
        }
    }

    if (res != DBStatus::OK) {
        std::cout << "CREATE INDEX failed" << std::endl;
        return true;
    }
    std::string idxName = stmt->indexName.empty() ? (tname + "_idx") : stmt->indexName;

    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        auto qn = CatalogService::logicalName(tname);
        const auto* ns = cat.findNamespaceByName(qn.schema.empty() ? "public" : qn.schema);
        const auto* tbl = (ns ? cat.resolveRelation(qn.name, {qn.schema.empty() ? "public" : qn.schema}) : nullptr);
        if (ns && tbl) {
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

            PgDependRow dep;
            dep.classid = PgClassOid_Class;
            dep.objid = idxOid;
            dep.objsubid = 0;
            dep.refclassid = PgClassOid_Class;
            dep.refobjid = tableOid;
            dep.refobjsubid = 0;
            dep.deptype = 'a';
            cat.addDepend(dep);
        } else {
            std::cerr << "WARNING: table " << tname
                      << " has no catalog entry; index not registered" << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog index registration failed: " << e.what() << std::endl;
    }

    txn.recordCreate(DdlObjectKind::Index, idxName, tname);
    txn.commit();
    std::cout << "CREATE INDEX succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP SEQUENCE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateSequence(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string seqname = stmt->objectName;
    dbms::SequenceInfo info;
    auto opt = stmt->options.find("start");
    if (opt != stmt->options.end()) {
        try { info.start = std::stoll(opt->second); info.startSpecified = true; } catch (...) {}
    }
    opt = stmt->options.find("increment");
    if (opt != stmt->options.end()) {
        try { info.increment = std::stoll(opt->second); info.incrementSpecified = true; } catch (...) {}
    }
    opt = stmt->options.find("minvalue");
    if (opt != stmt->options.end()) {
        try { info.minValue = std::stoll(opt->second); info.hasMinValue = true; } catch (...) {}
    }
    opt = stmt->options.find("maxvalue");
    if (opt != stmt->options.end()) {
        try { info.maxValue = std::stoll(opt->second); info.hasMaxValue = true; } catch (...) {}
    }
    opt = stmt->options.find("cache");
    if (opt != stmt->options.end()) {
        try { info.cache = std::stoll(opt->second); info.cacheSpecified = true; } catch (...) {}
    }
    opt = stmt->options.find("cycle");
    if (opt != stmt->options.end()) {
        info.cycleSpecified = true;
        info.cycle = (opt->second == "yes");
    }
    opt = stmt->options.find("nominvalue");
    if (opt != stmt->options.end()) info.noMinValue = true;
    opt = stmt->options.find("nomaxvalue");
    if (opt != stmt->options.end()) info.noMaxValue = true;
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
                // schema.table.column or table.column with schema
                std::string schemaPart = owner.substr(0, first);
                std::string tablePart = owner.substr(first + 1, last - first - 1);
                info.ownedByTable = (schemaPart == "public") ? tablePart : owner.substr(0, last);
                info.ownedByColumn = owner.substr(last + 1);
            } else if (first != std::string::npos) {
                info.ownedByTable = owner.substr(0, first);
                info.ownedByColumn = owner.substr(first + 1);
            } else {
                info.ownedByTable = owner;
            }
        }
    }

    DBStatus res = g_engine.createSequence(s.currentDB, seqname, info);
    if (res != DBStatus::OK) {
        std::cout << "CREATE SEQUENCE failed" << std::endl;
        return true;
    }

    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        const auto* nsPublic = cat.findNamespaceByName("public");
        if (nsPublic) {
            PgClassRow seq;
            seq.relname = seqname;
            seq.relnamespace = nsPublic->oid;
            seq.relkind = 'S';
            seq.relnatts = 0;
            dbms::Oid seqOid = cat.createClass(seq);

            if (!info.ownedByTable.empty()) {
                // Register dependency: sequence -> owning table (so DROP TABLE CASCADE drops seq).
                auto tableRel = cat.resolveRelation(info.ownedByTable, {"public"});
                if (tableRel) {
                    PgDependRow dep;
                    dep.classid = dbms::PgClassOid_Class;
                    dep.objid = seqOid;
                    dep.objsubid = 0;
                    dep.refclassid = dbms::PgClassOid_Class;
                    dep.refobjid = tableRel->oid;
                    dep.refobjsubid = 0;
                    dep.deptype = 'a';
                    cat.addDepend(dep);
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog sequence registration failed: " << e.what() << std::endl;
    }

    txn.recordCreate(DdlObjectKind::Sequence, seqname);
    txn.commit();
    std::cout << "CREATE SEQUENCE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeAlterSequence(const AlterObjectStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string seqname = stmt->objectName;
    dbms::SequenceInfo info;

    // Parse subCommand (lowercase space-separated tokens saved by parser).
    std::string rest = stmt->subCommand;
    std::vector<std::string> tokens;
    {
        std::istringstream iss(rest);
        std::string tok;
        while (iss >> tok) tokens.push_back(tok);
    }

    auto lower = [](const std::string& str) {
        std::string r = str;
        for (char& c : r) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return r;
    };

    for (size_t i = 0; i < tokens.size(); ++i) {
        std::string tok = lower(tokens[i]);
        if (tok == "restart") {
            info.startSpecified = true;
            if (i + 1 < tokens.size() && lower(tokens[i + 1]) == "with") {
                if (i + 2 < tokens.size()) {
                    try { info.start = std::stoll(tokens[i + 2]); } catch (...) {}
                    i += 2;
                } else ++i;
            } else if (i + 1 < tokens.size()) {
                try { info.start = std::stoll(tokens[i + 1]); } catch (...) {}
                ++i;
            }
        } else if (tok == "increment") {
            info.incrementSpecified = true;
            if (i + 1 < tokens.size() && lower(tokens[i + 1]) == "by") {
                if (i + 2 < tokens.size()) {
                    try { info.increment = std::stoll(tokens[i + 2]); } catch (...) {}
                    i += 2;
                } else ++i;
            } else if (i + 1 < tokens.size()) {
                try { info.increment = std::stoll(tokens[i + 1]); } catch (...) {}
                ++i;
            }
        } else if (tok == "minvalue" && i + 1 < tokens.size()) {
            info.hasMinValue = true;
            try { info.minValue = std::stoll(tokens[i + 1]); } catch (...) {}
            ++i;
        } else if (tok == "maxvalue" && i + 1 < tokens.size()) {
            info.hasMaxValue = true;
            try { info.maxValue = std::stoll(tokens[i + 1]); } catch (...) {}
            ++i;
        } else if (tok == "cache" && i + 1 < tokens.size()) {
            info.cacheSpecified = true;
            try { info.cache = std::stoll(tokens[i + 1]); } catch (...) {}
            ++i;
        } else if (tok == "no" && i + 1 < tokens.size()) {
            std::string next = lower(tokens[i + 1]);
            if (next == "minvalue") { info.noMinValue = true; ++i; }
            else if (next == "maxvalue") { info.noMaxValue = true; ++i; }
            else if (next == "cycle") { info.cycleSpecified = true; info.cycle = false; ++i; }
        } else if (tok == "cycle") {
            info.cycleSpecified = true;
            info.cycle = true;
        } else if (tok == "owned" && i + 1 < tokens.size() && lower(tokens[i + 1]) == "by") {
            info.ownedBySpecified = true;
            if (i + 2 < tokens.size()) {
                std::string owner = tokens[i + 2];
                if (lower(owner) == "none") {
                    info.ownedByTable.clear();
                    info.ownedByColumn.clear();
                } else if (i + 4 < tokens.size() && tokens[i + 3] == "." && i + 6 < tokens.size() && tokens[i + 5] == ".") {
                    // schema.table.column
                    std::string schemaPart = owner;
                    std::string tablePart = tokens[i + 4];
                    info.ownedByTable = (schemaPart == "public") ? tablePart : owner + "." + tablePart;
                    info.ownedByColumn = tokens[i + 6];
                    i += 4;
                } else if (i + 4 < tokens.size() && tokens[i + 3] == ".") {
                    // table.column
                    info.ownedByTable = owner;
                    info.ownedByColumn = tokens[i + 4];
                    i += 2;
                } else {
                    info.ownedByTable = owner;
                }
                i += 2;
            } else {
                i += 1;
            }
        }
    }

    DBStatus res = g_engine.alterSequence(s.currentDB, seqname, info);
    if (res != DBStatus::OK) {
        std::cout << "ALTER SEQUENCE failed" << std::endl;
        return true;
    }

    // Update catalog dependency for OWNED BY changes.
    if (info.ownedBySpecified) {
        try {
            dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
            auto seqRel = cat.resolveRelation(seqname, {"public"});
            if (seqRel) {
                // Remove old auto-dependencies for this sequence.
                auto oldDeps = cat.findDepends(dbms::PgClassOid_Class, seqRel->oid, 0);
                for (const auto& d : oldDeps) {
                    if (d.deptype == 'a') {
                        cat.removeDepend(d.classid, d.objid, d.objsubid,
                                         d.refclassid, d.refobjid, d.refobjsubid);
                    }
                }
                if (!info.ownedByTable.empty()) {
                    auto tableRel = cat.resolveRelation(info.ownedByTable, {"public"});
                    if (tableRel) {
                        PgDependRow dep;
                        dep.classid = dbms::PgClassOid_Class;
                        dep.objid = seqRel->oid;
                        dep.objsubid = 0;
                        dep.refclassid = dbms::PgClassOid_Class;
                        dep.refobjid = tableRel->oid;
                        dep.refobjsubid = 0;
                        dep.deptype = 'a';
                        cat.addDepend(dep);
                    }
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "WARNING: catalog alter sequence failed: " << e.what() << std::endl;
        }
    }

    txn.commit();
    std::cout << "ALTER SEQUENCE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropSequence(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP SEQUENCE name" << std::endl;
        return true;
    }
    std::string seqname = stmt->objectNames.front();

    // Check for columns depending on this sequence via DEFAULT nextval.
    auto defaultDeps = findDefaultNextvalDeps(s.currentDB, seqname);
    if (!defaultDeps.empty()) {
        if (!stmt->cascade) {
            std::cout << "ERROR: cannot drop sequence " << seqname
                     << " because other objects depend on it" << std::endl;
            return true;
        }
        for (const auto& dep : defaultDeps) {
            DBStatus clearRes = g_engine.alterTableDropDefault(s.currentDB, dep.first, dep.second);
            if (clearRes != DBStatus::OK) {
                std::cout << "ERROR: failed to clear default on " << dep.first << "." << dep.second << std::endl;
                return true;
            }
        }
    }

    txn.recordDrop(DdlObjectKind::Sequence, seqname);

    try {
        dbms::CatalogManager& cat = g_engine.catalogService().get(s.currentDB);
        const auto* seq = cat.resolveRelation(seqname, {"public"});
        if (seq) {
            auto behavior = stmt->cascade ? CatalogManager::DropBehavior::Cascade
                                          : CatalogManager::DropBehavior::Restrict;
            std::string err;
            bool ok = cat.dropObject(PgClassOid_Class, seq->oid, behavior, &err);
            if (!ok) {
                std::cout << "ERROR: " << err << std::endl;
                return true;
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "WARNING: catalog sequence drop failed: " << e.what() << std::endl;
    }

    DBStatus res = g_engine.dropSequence(s.currentDB, seqname);
    if (res != DBStatus::OK) {
        std::cout << "DROP SEQUENCE failed" << std::endl;
        return true;
    }
    txn.commit();
    std::cout << "DROP SEQUENCE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP DOMAIN
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateDomain(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return false;
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
    txn.recordCreate(DdlObjectKind::Domain, info.name);
    txn.commit();
    std::cout << "CREATE DOMAIN succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeCreateCollation(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    // Store collation metadata: name|provider|locale
    std::string cname = resolveTableName(s, stmt->objectName);
    auto itProvider = stmt->options.find("provider");
    auto itLocale = stmt->options.find("locale");
    std::string provider = (itProvider != stmt->options.end()) ? itProvider->second : "libc";
    std::string locale = (itLocale != stmt->options.end()) ? itLocale->second : "C";

    std::filesystem::path colPath = std::filesystem::path(s.currentDB) / ".collations";
    // Check for duplicates
    if (std::filesystem::exists(colPath)) {
        std::ifstream ifs(colPath);
        std::string line;
        while (std::getline(ifs, line)) {
            if (!line.empty() && line.substr(0, line.find('|')) == cname) {
                std::cout << "Collation " << cname << " already exists" << std::endl;
                return true;
            }
        }
    }

    std::ofstream ofs(colPath, std::ios::app);
    if (!ofs) return true;
    ofs << cname << "|" << provider << "|" << locale << "\n";

    txn.recordCreate(DdlObjectKind::Collation, cname);
    txn.commit();
    std::cout << "CREATE COLLATION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropCollation(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP COLLATION name" << std::endl;
        return true;
    }
    std::string cname = resolveTableName(s, stmt->objectNames[0]);
    std::filesystem::path colPath = std::filesystem::path(s.currentDB) / ".collations";
    if (!std::filesystem::exists(colPath)) {
        std::cout << "Collation " << cname << " not found" << std::endl;
        return true;
    }
    std::ifstream ifs(colPath);
    std::vector<std::string> lines;
    std::string line;
    bool found = false;
    while (std::getline(ifs, line)) {
        if (!line.empty() && line.substr(0, line.find('|')) == cname) { found = true; continue; }
        lines.push_back(line);
    }
    if (!found) {
        std::cout << "Collation " << cname << " not found" << std::endl;
        return true;
    }
    std::ofstream ofs(colPath, std::ios::trunc);
    for (auto& l : lines) ofs << l << "\n";
    std::cout << "DROP COLLATION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropDomain(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP DOMAIN name" << std::endl;
        return true;
    }
    std::string name = stmt->objectNames.front();
    txn.recordDrop(DdlObjectKind::Domain, name);
    DBStatus res = g_engine.dropDomain(s.currentDB, name);
    if (res != DBStatus::OK) {
        std::cout << "DROP DOMAIN failed" << std::endl;
        return true;
    }
    txn.commit();
    std::cout << "DROP DOMAIN succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE / DROP TYPE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateType(const CreateObjectStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string typeKind = stmt->options.count("type_kind") ? stmt->options.at("type_kind") : "";
    if (typeKind == "enum") {
        StorageEngine::EnumType et;
        et.name = stmt->objectName;
        std::string labels = stmt->options.count("enum_labels") ? stmt->options.at("enum_labels") : "";
        std::stringstream ss(labels);
        std::string item;
        while (std::getline(ss, item, ',')) {
            if (!item.empty()) et.labels.push_back(item);
        }
        if (et.labels.empty()) {
            std::cout << "CREATE TYPE AS ENUM requires at least one label" << std::endl;
            return true;
        }
        DBStatus res = g_engine.createEnumType(s.currentDB, et);
        if (res != DBStatus::OK) {
            std::cout << "CREATE TYPE AS ENUM failed" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, et.name);
        txn.commit();
        std::cout << "CREATE TYPE AS ENUM succeeded" << std::endl;
        return false;
    }

    // Shell type (CREATE TYPE name)
    if (typeKind == "shell") {
        if (anyTypeExists(s.currentDB, stmt->objectName)) {
            std::cout << "Type " << stmt->objectName << " already exists" << std::endl;
            return true;
        }
        if (!recordShellType(s.currentDB, stmt->objectName)) {
            std::cout << "CREATE TYPE failed" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, stmt->objectName);
        txn.commit();
        std::cout << "CREATE TYPE succeeded" << std::endl;
        return false;
    }

    // Range type (CREATE TYPE name AS RANGE (...))
    if (typeKind == "range") {
        if (anyTypeExists(s.currentDB, stmt->objectName)) {
            std::cout << "Type " << stmt->objectName << " already exists" << std::endl;
            return true;
        }
        UdtMeta meta;
        meta.kind = "range";
        meta.name = stmt->objectName;
        for (const auto& kv : stmt->options) {
            if (kv.first.substr(0, 6) == "range_") {
                meta.attrs[kv.first.substr(6)] = kv.second;
            }
        }
        if (meta.attrs.count("subtype") == 0) {
            std::cout << "CREATE TYPE AS RANGE requires a subtype" << std::endl;
            return true;
        }
        if (!recordUdtMeta(s.currentDB, meta)) {
            std::cout << "CREATE TYPE failed" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, stmt->objectName);
        txn.commit();
        std::cout << "CREATE TYPE AS RANGE succeeded" << std::endl;
        return false;
    }

    // Base type (CREATE TYPE name (INPUT=..., OUTPUT=..., ...))
    if (typeKind == "base") {
        if (anyTypeExists(s.currentDB, stmt->objectName)) {
            std::cout << "Type " << stmt->objectName << " already exists" << std::endl;
            return true;
        }
        UdtMeta meta;
        meta.kind = "base";
        meta.name = stmt->objectName;
        for (const auto& kv : stmt->options) {
            if (kv.first.substr(0, 5) == "base_") {
                meta.attrs[kv.first.substr(5)] = kv.second;
            }
        }
        if (meta.attrs.count("input") == 0 || meta.attrs.count("output") == 0) {
            std::cout << "CREATE TYPE base requires INPUT and OUTPUT functions" << std::endl;
            return true;
        }
        if (!recordUdtMeta(s.currentDB, meta)) {
            std::cout << "CREATE TYPE failed" << std::endl;
            return true;
        }
        txn.recordCreate(DdlObjectKind::Type, stmt->objectName);
        txn.commit();
        std::cout << "CREATE TYPE succeeded" << std::endl;
        return false;
    }

    // Composite type (existing behavior)
    StorageEngine::CompositeType ct;
    ct.name = stmt->objectName;
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
    DBStatus res = g_engine.createCompositeType(s.currentDB, ct);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TYPE failed" << std::endl;
        return true;
    }
    txn.recordCreate(DdlObjectKind::Type, ct.name);
    txn.commit();
    std::cout << "CREATE TYPE succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeDropType(const DropStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->objectNames.empty()) {
        std::cout << "SQL syntax error: DROP TYPE name" << std::endl;
        return true;
    }
    std::string name = stmt->objectNames.front();
    txn.recordDrop(DdlObjectKind::Type, name);
    DBStatus res = g_engine.dropCompositeType(s.currentDB, name);
    if (res != DBStatus::OK) {
        res = g_engine.dropEnumType(s.currentDB, name);
    }
    bool droppedMeta = false;
    if (res != DBStatus::OK) {
        droppedMeta = removeUdtMeta(s.currentDB, name);
    }
    bool droppedShell = false;
    if (res != DBStatus::OK && !droppedMeta) {
        droppedShell = removeShellType(s.currentDB, name);
    }
    if (res != DBStatus::OK && !droppedMeta && !droppedShell) {
        std::cout << "DROP TYPE failed" << std::endl;
        return true;
    }
    txn.commit();
    std::cout << "DROP TYPE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateView(const CreateViewStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string viewname = stmt->viewName;
    std::string viewSql = stmt->selectSql;
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

    std::string checkOption = stmt->checkOption;
    std::string storeSql = viewSql;
    if (!baseTable.empty()) storeSql += "\nBASE_TABLE:" + baseTable + "\n";
    if (!checkOption.empty()) storeSql += "WITH_CHECK_OPTION:" + checkOption + "\n";

    if (stmt->replace && g_engine.viewExists(s.currentDB, viewname)) {
        g_engine.dropView(s.currentDB, viewname);
    }

    DBStatus res = g_engine.createView(s.currentDB, viewname, storeSql);
    if (res == DBStatus::TABLE_ALREADY_EXISTS) {
        std::cout << "View " << viewname << " already exists" << std::endl;
        return true;
    }
    if (res != DBStatus::OK) {
        std::cout << "CREATE VIEW failed" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::View, viewname);
    txn.commit();
    std::cout << "CREATE VIEW succeeded"
              << (baseTable.empty() ? "" : " (updatable)")
              << (checkOption.empty() ? "" : " [with check option " + checkOption + "]")
              << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE MATERIALIZED VIEW
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateMaterializedView(const CreateViewStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string viewname = stmt->viewName;
    std::string selectSql = stmt->selectSql;
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

    srcTable = resolveTableName(s, srcTable);
    if (!g_engine.tableExists(s.currentDB, srcTable)) {
        std::cout << "CREATE MATERIALIZED VIEW: source table not found" << std::endl;
        return true;
    }

    dbms::TableSchema srcTbl = g_engine.getTableSchema(s.currentDB, srcTable);
    std::vector<std::string> colNames;
    std::set<std::string> queryCols;

    if (selectCols.size() == 1 && selectCols[0] == "*") {
        for (size_t i = 0; i < srcTbl.len; ++i) {
            colNames.push_back(srcTbl.cols[i].dataName);
            queryCols.insert(srcTbl.cols[i].dataName);
        }
    } else {
        for (const auto& cname : selectCols) {
            bool found = false;
            for (size_t i = 0; i < srcTbl.len; ++i) {
                if (toLower(srcTbl.cols[i].dataName) == cname) {
                    colNames.push_back(srcTbl.cols[i].dataName);
                    queryCols.insert(srcTbl.cols[i].dataName);
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
    for (const auto& cname : colNames) {
        dbms::Column col;
        col.dataName = cname;
        col.dataType = "varchar";
        col.isVariableLength = true;
        col.dsize = 255;
        col.isNull = true;
        tbl.append(col);
    }

    if (g_engine.tableExists(s.currentDB, backingTable)) {
        g_engine.dropTable(s.currentDB, backingTable);
    }
    DBStatus res = g_engine.createTable(s.currentDB, tbl);
    if (res != DBStatus::OK) {
        std::cout << "CREATE MATERIALIZED VIEW: failed to create backing table" << std::endl;
        return true;
    }

    auto rows = g_engine.query(s.currentDB, srcTable, conditions, queryCols, {});
    // query() emits values in SOURCE schema order (filtered to queryCols); map
    // them in that same order so values line up with the right columns.
    std::vector<std::string> orderedCols;
    for (size_t i = 0; i < srcTbl.len; ++i) {
        if (queryCols.count(srcTbl.cols[i].dataName))
            orderedCols.push_back(srcTbl.cols[i].dataName);
    }
    size_t inserted = 0;
    if (stmt->withData) {
        for (const auto& row : rows) {
            std::map<std::string, std::string> values;
            std::istringstream iss(row);
            std::string val;
            size_t idx = 0;
            while (iss >> val && idx < orderedCols.size()) {
                values[orderedCols[idx]] = val;
                ++idx;
            }
            if (idx != orderedCols.size()) continue;
            if (g_engine.insert(s.currentDB, backingTable, values) == DBStatus::OK) ++inserted;
        }
    }

    // Save SQL to .mview file
    auto mviewDir = g_engine.viewsDir(s.currentDB);
    if (!std::filesystem::exists(mviewDir)) {
        std::filesystem::create_directories(mviewDir);
    }
    auto mviewPath = mviewDir / (viewname + ".mview");
    {
        std::ofstream ofs(mviewPath);
        if (!ofs) {
            std::cout << "CREATE MATERIALIZED VIEW: failed to save metadata" << std::endl;
            return true;
        }
        ofs << selectSql;
    }

    txn.recordCreate(DdlObjectKind::MaterializedView, viewname);
    txn.commit();
    std::cout << "CREATE MATERIALIZED VIEW succeeded: " << inserted << " rows" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE TRIGGER
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateTrigger(const CreateTriggerStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
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

    DBStatus res = g_engine.createTrigger(s.currentDB, trg);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TRIGGER failed" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Trigger, stmt->triggerName, tname);
    txn.commit();
    std::cout << "CREATE TRIGGER succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE FUNCTION / PROCEDURE
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreateFunction(const CreateFunctionStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
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

    if (toLower(stmt->returnType) == "table") {
        std::string singleParam = stmt->params.empty() ? "" : stmt->params.front().first;
        res = g_engine.createTVF(s.currentDB, stmt->funcName, singleParam, stmt->body);
    } else {
        if (stmt->params.size() <= 1) {
            std::string singleParam = stmt->params.empty() ? "" : stmt->params.front().first;
            res = g_engine.createUDF(s.currentDB, stmt->funcName, singleParam, stmt->body, provolatile);
        } else {
            std::vector<std::string> params;
            std::vector<std::string> types;
            for (const auto& p : stmt->params) {
                params.push_back(p.first);
                types.push_back(p.second);
            }
            res = g_engine.createUDF(s.currentDB, stmt->funcName, params, types, stmt->body, provolatile);
        }
    }

    if (res != DBStatus::OK) {
        std::cout << "CREATE FUNCTION failed" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Function, stmt->funcName);
    txn.commit();
    std::cout << "CREATE FUNCTION succeeded" << std::endl;
    return false;
}

bool DdlExecutor::executeCreateProcedure(const CreateFunctionStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    if (stmt->funcName.empty()) {
        std::cout << "SQL syntax error: CREATE PROCEDURE name" << std::endl;
        return true;
    }

    std::vector<dbms::StorageEngine::ProcParam> params;
    for (const auto& p : stmt->params) {
        dbms::StorageEngine::ProcParam pp;
        pp.mode = "IN";
        pp.name = p.first;
        pp.type = p.second;
        params.push_back(pp);
    }

    std::vector<std::string> stmts;
    size_t start = 0;
    while (start < stmt->body.size()) {
        size_t sc = stmt->body.find(';', start);
        std::string part = trim(stmt->body.substr(start, sc == std::string::npos ? std::string::npos : sc - start));
        if (!part.empty()) stmts.push_back(part);
        if (sc == std::string::npos) break;
        start = sc + 1;
    }
    if (stmts.empty()) {
        std::cout << "SQL syntax error: PROCEDURE body is empty" << std::endl;
        return true;
    }

    DBStatus res = g_engine.createProcedure(s.currentDB, stmt->funcName, params, stmts);
    if (res != DBStatus::OK) {
        std::cout << "CREATE PROCEDURE failed" << std::endl;
        return true;
    }

    txn.recordCreate(DdlObjectKind::Procedure, stmt->funcName);
    txn.commit();
    std::cout << "CREATE PROCEDURE succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// CREATE POLICY
// ----------------------------------------------------------------------------

bool DdlExecutor::executeCreatePolicy(const CreatePolicyStmt* stmt, Session& s) {
    if (!stmt) return false;
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
    txn.commit();
    std::cout << "CREATE POLICY succeeded" << std::endl;
    return false;
}

// ----------------------------------------------------------------------------
// COMMENT ON
// ----------------------------------------------------------------------------

bool DdlExecutor::executeComment(const CommentStmt* stmt, Session& s) {
    if (!stmt) return false;
    if (!checkAdmin(s)) return true;
    if (!checkDB(s)) return true;

    DdlTransaction txn(s);
    if (!txn.begin()) {
        std::cout << "DDL transaction begin failed" << std::endl;
        return true;
    }

    std::string objType = toLower(stmt->objectType);
    if (objType == "table") {
        DBStatus res = g_engine.commentOnTable(s.currentDB, stmt->objectName, stmt->comment);
        if (res != DBStatus::OK) {
            std::cout << "COMMENT ON TABLE failed" << std::endl;
            return true;
        }
        txn.recordUpdate(DdlObjectKind::Table, stmt->objectName);
    } else if (objType == "column") {
        std::string tname = stmt->objectName;
        std::string cname = stmt->columnName;
        if (tname.empty() || cname.empty()) {
            std::cout << "COMMENT ON COLUMN requires table.column" << std::endl;
            return true;
        }
        DBStatus res = g_engine.commentOnColumn(s.currentDB, tname, cname, stmt->comment);
        if (res != DBStatus::OK) {
            std::cout << "COMMENT ON COLUMN failed" << std::endl;
            return true;
        }
        txn.recordUpdate(DdlObjectKind::Table, tname, cname);
    } else {
        std::cout << "COMMENT ON " << objType << " not yet supported via AST bridge" << std::endl;
        return true;
    }
    txn.commit();
    std::cout << "COMMENT succeeded" << std::endl;
    return false;
}

} // namespace dbms
