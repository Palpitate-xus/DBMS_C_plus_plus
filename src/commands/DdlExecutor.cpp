// ============================================================================
// DDL AST Executor — Phase 4 Wave 0.3
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "parser/parser.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "common/logs.h"
#include "permissions.h"
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
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
    Oid classOid = cat.createClass(cls);

    for (size_t i = 0; i < tbl.len; ++i) {
        const Column& col = tbl.cols[i];
        PgAttributeRow attr;
        attr.attrelid = classOid;
        attr.attnum = static_cast<int16_t>(i + 1);
        attr.attname = col.dataName;
        attr.atttypid = ensureTypeInCatalog(cat, nspOid, col);
        attr.attlen = static_cast<int16_t>(col.dsize);
        attr.atttypmod = -1;
        attr.attnotnull = !col.isNull;
        attr.atthasdef = !col.defaultValue.empty();
        attr.attstorage = col.isVariableLength ? 'x' : 'p';
        attr.attislocal = true;
        attr.attisdropped = false;
        if (col.isAutoIncrement) attr.attidentity = 'd';
        if (!col.generatedExpr.empty()) attr.attgenerated = 's';
        cat.addAttribute(attr);
    }

    for (size_t i = 0; i < tbl.fkLen; ++i) {
        const ForeignKey& fk = tbl.fks[i];
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
        case SqlCommand::CreateMaterializedView:
            return executeCreateMaterializedView(dynamic_cast<const CreateViewStmt*>(stmt.get()), s);
        case SqlCommand::CreateDatabase:
            return executeCreateDatabase(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
        case SqlCommand::DropDatabase:
            return executeDropDatabase(dynamic_cast<const DropStmt*>(stmt.get()), s);
        case SqlCommand::CreateSchema:
            return executeCreateSchema(dynamic_cast<const CreateObjectStmt*>(stmt.get()), s);
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
    if (!r.success || !r.stmt) return false;
    return execute(r.stmt, s);
}

// ----------------------------------------------------------------------------
// DDL AST bridge helper (used by main.cpp::execute)
// ----------------------------------------------------------------------------

bool tryDdlBridge(const std::string& sql, dbms::SqlCommand parsedCmd,
                  Session& s, bool& handled) {
    handled = false;
    switch (parsedCmd) {
        case dbms::SqlCommand::CreateTable:
        case dbms::SqlCommand::DropTable:
        case dbms::SqlCommand::CreateIndex:
        case dbms::SqlCommand::CreateSequence:
        case dbms::SqlCommand::DropSequence:
        case dbms::SqlCommand::CreateDomain:
        case dbms::SqlCommand::DropDomain:
        case dbms::SqlCommand::CreateType:
        case dbms::SqlCommand::DropType:
        case dbms::SqlCommand::CreateView:
        case dbms::SqlCommand::CreateTrigger:
        case dbms::SqlCommand::CreateMaterializedView:
        case dbms::SqlCommand::CreateDatabase:
        case dbms::SqlCommand::DropDatabase:
        case dbms::SqlCommand::CreateSchema:
        case dbms::SqlCommand::DropSchema:
        case dbms::SqlCommand::Comment:
            handled = true;
            break;
        default:
            return false;
    }

    dbms::SQLParser parser;
    dbms::ParseResult r = parser.parse(sql);
    if (!r.success || !r.stmt) {
        // Parse failed: treat as not handled so legacy string dispatch can try.
        handled = false;
        return false;
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
    } else {
        // Unknown type: fall back to varchar so the table can still be created
        col = makeVarCharColumn(cd.name, cd.isNull, 255, cd.isPrimaryKey);
    }

    // Factory functions replace the whole Column; restore metadata they don't set.
    col.defaultValue = cd.defaultValue ? cd.defaultValue->toString() : "";
    col.generatedExpr = cd.generatedExpr;
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
    size_t inserted = 0;
    for (const auto& row : rows) {
        std::map<std::string, std::string> values;
        std::istringstream iss(row);
        std::string val;
        size_t idx = 0;
        std::vector<std::string> orderedCols(queryCols.begin(), queryCols.end());
        while (iss >> val && idx < orderedCols.size()) {
            if (val == "NULL") val = "";
            values[orderedCols[idx]] = val;
            ++idx;
        }
        if (idx != orderedCols.size()) continue;
        if (g_engine.insert(s.currentDB, tname, values) == DBStatus::OK) ++inserted;
    }

    std::cout << "CREATE TABLE AS succeeded: " << inserted << " rows" << std::endl;
    return false;
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
    tbl.isUnlogged = stmt->unlogged;
    tbl.tablespace = stmt->tablespace.empty() ? "pg_default" : stmt->tablespace;
    tbl.storageParams = stmt->options;

    for (const auto& cd : stmt->columns) {
        tbl.append(columnDefToColumn(cd, s.currentDB));
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
                }
            }
            recordConstraintCompat(s.currentDB, tname, tc);
        }
    }

    DBStatus res = g_engine.createTable(s.currentDB, tbl);
    if (res != DBStatus::OK) {
        std::cout << "CREATE TABLE failed" << std::endl;
        return true;
    }
    g_engine.applyDefaultPrivileges(s.currentDB, "public", "table", tname, s.username);

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
            dep.refobjid = tbl->oid;
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

    // Composite type (existing behavior)
    StorageEngine::CompositeType ct;
    ct.name = stmt->objectName;
    auto it = stmt->options.find("fields");
    if (it != stmt->options.end()) {
        std::stringstream ss(it->second);
        std::string item;
        while (std::getline(ss, item, ',')) {
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
    size_t inserted = 0;
    for (const auto& row : rows) {
        std::map<std::string, std::string> values;
        std::istringstream iss(row);
        std::string val;
        size_t idx = 0;
        while (iss >> val && idx < colNames.size()) {
            values[colNames[idx]] = val;
            ++idx;
        }
        if (idx != colNames.size()) continue;
        if (g_engine.insert(s.currentDB, backingTable, values) == DBStatus::OK) ++inserted;
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
    if (!g_engine.tableExists(s.currentDB, tname)) {
        std::cout << "Table " << tname << " not found" << std::endl;
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
