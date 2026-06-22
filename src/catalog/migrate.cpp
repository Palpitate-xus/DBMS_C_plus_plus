#include "migrate.h"
#include <filesystem>
#include <fstream>

namespace dbms {

// ============================================================================
// 表名解析：支持 schema__table 和 plain_table 两种格式
// ============================================================================

static std::pair<std::string, std::string> parseTableName(const std::string& fullName) {
    size_t pos = fullName.find("__");
    if (pos != std::string::npos) {
        return {fullName.substr(0, pos), fullName.substr(pos + 2)};
    }
    return {"public", fullName};
}

// ============================================================================
// 主迁移函数
// ============================================================================

MigrateResult migrateDatabaseToCatalog(
    CatalogManager& cat,
    const StorageEngine& engine,
    const std::string& dbname) {

    MigrateResult result;
    std::filesystem::path dbPath = engine.dbPath(dbname);

    if (!std::filesystem::exists(dbPath)) {
        result.errors.push_back("Database directory does not exist: " + dbname);
        return result;
    }

    // Ensure bootstrap namespaces/types exist
    cat.bootstrapSystemNamespaces();
    cat.bootstrapSystemTypes();

    // Scan for .stc (schema) files
    for (const auto& entry : std::filesystem::directory_iterator(dbPath)) {
        if (!entry.is_regular_file()) continue;
        std::string filename = entry.path().filename().string();
        if (filename.size() < 5 || filename.substr(filename.size() - 4) != ".stc") continue;

        std::string fullTableName = filename.substr(0, filename.size() - 4);
        auto [schemaName, tableName] = parseTableName(fullTableName);

        // Load existing schema from StorageEngine
        TableSchema tbl;
        try {
            tbl = engine.getTableSchema(dbname, fullTableName);
        } catch (...) {
            result.errors.push_back("Failed to read schema for: " + fullTableName);
            continue;
        }

        // 1. Ensure namespace exists
        const PgNamespaceRow* ns = cat.findNamespaceByName(schemaName);
        Oid nspOid;
        if (!ns) {
            nspOid = cat.createNamespace(schemaName, 10); // owner = bootstrap superuser
            result.namespacesCreated++;
        } else {
            nspOid = ns->oid;
        }

        // 2. Create pg_class entry
        PgClassRow cls;
        cls.relname = tableName;
        cls.relnamespace = nspOid;
        cls.relkind = 'r'; // ordinary table
        cls.relnatts = static_cast<int16_t>(tbl.len);
        cls.relpersistence = tbl.isUnlogged ? 'u' : 'p';
        cls.relpages = 0;
        cls.reltuples = 0;
        cls.reltablespace = INVALID_OID; // TODO: resolve tablespace name to OID
        cls.relhasindex = false; // TODO: detect from .idx files
        cls.relrowsecurity = tbl.rowLevelSecurity;
        cls.relforcerowsecurity = tbl.forceRowLevelSecurity;

        Oid classOid = cat.createClass(cls);
        result.tablesMigrated++;

        // 3. Create pg_attribute entries
        for (size_t i = 0; i < tbl.len; ++i) {
            const Column& col = tbl.cols[i];
            PgAttributeRow attr;
            attr.attrelid = classOid;
            attr.attnum = static_cast<int16_t>(i + 1); // 1-based
            attr.attname = col.dataName;
            attr.atttypid = mapBuiltinTypeNameToOid(col.dataType);
            attr.attlen = static_cast<int16_t>(col.dsize);
            attr.atttypmod = -1;
            attr.attnotnull = !col.isNull;
            attr.atthasdef = !col.defaultValue.empty();
            attr.attstorage = col.isVariableLength ? 'x' : 'p';
            attr.attislocal = true;
            attr.attisdropped = false;

            if (col.isAutoIncrement) {
                attr.attidentity = 'd'; // DEFAULT identity
            }
            if (!col.generatedExpr.empty()) {
                attr.attgenerated = 's'; // STORED
            }

            // Create type if unknown before adding the attribute
            if (attr.atttypid == INVALID_OID) {
                PgTypeRow typ;
                typ.typname = col.dataType;
                typ.typnamespace = nspOid;
                typ.typlen = col.isVariableLength ? -1 : static_cast<int16_t>(col.dsize);
                typ.typtype = 'b';
                typ.typcategory = 'U';
                Oid typeOid = cat.createType(typ);
                attr.atttypid = typeOid;
                result.typesCreated++;
            }

            cat.addAttribute(attr);
            result.attributesCreated++;
        }

        // 4. Create pg_depend for foreign keys
        for (size_t i = 0; i < tbl.fkLen; ++i) {
            const ForeignKey& fk = tbl.fks[i];
            PgDependRow dep;
            dep.classid = PgClassOid_Class;
            dep.objid = classOid;
            dep.objsubid = 0;
            dep.refclassid = PgClassOid_Class;
            dep.refobjid = INVALID_OID; // Would need to resolve refTable OID
            dep.refobjsubid = 0;
            dep.deptype = 'n'; // normal dependency
            cat.addDepend(dep);
            result.dependsCreated++;
        }
    }

    cat.persistAll();
    return result;
}

} // namespace dbms
