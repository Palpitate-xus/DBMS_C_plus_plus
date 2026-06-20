// ============================================================================
// DDL AST Executor — Phase 4 Wave 0.3
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/DdlTransaction.h"
#include "parser/parser.h"
#include "common/logs.h"
#include "permissions.h"
#include <algorithm>
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

Column DdlExecutor::columnDefToColumn(const ColumnDef& cd) {
    Column col;
    col.dataName = cd.name;
    col.isNull = cd.isNull;
    col.isPrimaryKey = cd.isPrimaryKey;
    col.isUnique = cd.isUnique;
    col.isArray = cd.isArray;
    col.defaultValue = cd.defaultValue ? cd.defaultValue->toString() : "";

    std::string baseType = toLower(cd.typeName);
    // Normalize common aliases
    if (baseType == "int" || baseType == "integer") baseType = "int4";
    else if (baseType == "bigint") baseType = "int8";
    else if (baseType == "smallint") baseType = "int2";
    else if (baseType == "real") baseType = "float4";
    else if (baseType == "double" || baseType == "double precision") baseType = "float8";
    else if (baseType == "varchar" || baseType == "character varying") baseType = "varchar";
    else if (baseType == "char" || baseType == "character") baseType = "char";
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
        col = makeIntColumn(cd.name, cd.isNull, 1, cd.isPrimaryKey);
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
    } else {
        // Unknown type: fall back to varchar so the table can still be created
        col = makeVarCharColumn(cd.name, cd.isNull, 255, cd.isPrimaryKey);
    }

    // Apply check constraints from column definition
    if (!cd.checkExprs.empty()) {
        col.checkExpr = cd.checkExprs.front()->toString();
    }
    if (!cd.checkNames.empty()) {
        col.checkConstraintName = cd.checkNames.front();
    }
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

    TableSchema tbl;
    tbl.tablename = tname;
    tbl.isUnlogged = stmt->unlogged;
    tbl.tablespace = stmt->tablespace.empty() ? "pg_default" : stmt->tablespace;
    tbl.storageParams = stmt->options;

    for (const auto& cd : stmt->columns) {
        tbl.append(columnDefToColumn(cd));
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
    txn.recordCreate(DdlObjectKind::Table, tname);
    txn.commit();
    std::cout << "CREATE TABLE succeeded" << std::endl;
    return false;
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
    int64_t start = 1, increment = 1;
    auto it = stmt->options.find("start");
    if (it != stmt->options.end()) try { start = std::stoll(it->second); } catch (...) {}
    it = stmt->options.find("increment");
    if (it != stmt->options.end()) try { increment = std::stoll(it->second); } catch (...) {}
    DBStatus res = g_engine.createSequence(s.currentDB, seqname, start, increment);
    if (res != DBStatus::OK) {
        std::cout << "CREATE SEQUENCE failed" << std::endl;
        return true;
    }
    txn.recordCreate(DdlObjectKind::Sequence, seqname);
    txn.commit();
    std::cout << "CREATE SEQUENCE succeeded" << std::endl;
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
