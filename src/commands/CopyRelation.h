#pragma once

#include "catalog/RelationSearchPath.h"
#include "catalog/catalog.h"
#include "commands/TableManage.h"
#include "parser/parser.h"

namespace dbms {

struct CopyRelation {
    std::string physicalName;
    bool temporary = false;
    bool systemPgType = false;
};

inline CopyRelation resolveCopyRelation(
    StorageEngine& engine, Session& session, const std::string& spelling,
    std::string (*resolvePhysical)(Session&, const std::string&)) {
    CatalogManager::QualifiedName name;
    if (!CatalogManager::parseQualifiedName(spelling, name, true))
        throw DbError("42601", "invalid relation name in COPY");

    const bool hasTemporary = session.tempTables.count(name.name) ||
                              session.transientTempTables.count(name.name);
    const auto physicalRelation = [&](const std::string& reference) {
        auto physical = resolvePhysical(session, reference);
        const bool temporary = hasTemporary && physical == tempTablePrefix(session, name.name);
        return CopyRelation{std::move(physical), temporary, false};
    };

    if (name.name != "pg_type") return physicalRelation(spelling);
    if (name.schema == "pg_catalog") return {{}, false, true};
    if (!name.schema.empty()) {
        const bool temporarySchema = name.schema == "pg_temp" ||
                                     name.schema == sessionTempSchemaName(session);
        const bool exists = temporarySchema ? session.tempNamespaceCreated || hasTemporary
                                           : engine.schemaExists(session.currentDB, name.schema);
        if (!exists) throw DbError("3F000", "schema \"" + name.schema + "\" does not exist");
        return physicalRelation(spelling);
    }

    const std::string temporarySchema = sessionTempSchemaName(session);
    const auto quotedName = SQLParser::quoteIdentifier(name.name);
    for (const auto& schema : effectiveRelationSearchPath(&session)) {
        if (schema == "pg_catalog") return {{}, false, true};
        if (schema == temporarySchema) {
            if (hasTemporary) return physicalRelation("pg_temp." + quotedName);
            continue;
        }
        auto relation = physicalRelation(SQLParser::quoteIdentifier(schema) + "." + quotedName);
        if (engine.tableExists(session.currentDB, relation.physicalName) ||
            engine.viewExists(session.currentDB, relation.physicalName))
            return relation;
    }
    throw DbError("42P01", "relation \"" + spelling + "\" does not exist");
}

}  // namespace dbms
