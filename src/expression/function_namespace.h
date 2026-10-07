#pragma once

#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "utils/Session.h"
#include <algorithm>

namespace dbms {
// Metadata-only function lookup path. Temporary namespaces are never implicit
// routine namespaces. pg_catalog is implicit only if not explicitly placed.
inline std::vector<std::string> functionNamespaceSearchPath(
    StorageEngine& engine,const std::string& database,const std::string& explicitSchema={}) {
    if(!explicitSchema.empty())return {explicitSchema};
    std::vector<std::string> path;std::string canonical;
    const auto* session=currentSession();
    if(!session || !parseSessionSearchPath(session->searchPath,path,canonical))path={"public"};
    std::vector<std::string> actual;
    for(const auto& entry:path) {
        const auto schema=expandSessionSearchPathEntry(entry,session?session->username:std::string{});
        if(schema.empty() || schema=="pg_temp" || schema.rfind("pg_temp_",0)==0)continue;
        if(std::find(actual.begin(),actual.end(),schema)==actual.end())actual.push_back(schema);
    }
    if(std::find(actual.begin(),actual.end(),"pg_catalog")==actual.end())actual.insert(actual.begin(),"pg_catalog");
    if(!database.empty() && engine.databaseExists(database)) {
        const auto snapshot=engine.catalogService().metadataSnapshot(database);
        actual.erase(std::remove_if(actual.begin(),actual.end(),[&](const auto& schema) {
            // Native StorageEngine creation persists real schema markers
            // before any catalog is bootstrapped. A pure lookup may read that
            // namespace fact, but must not create/bootstrap a catalog to find
            // it, nor assume public exists after an actual DROP.
            const bool storedName=!schema.empty() && schema.size()<MAX_TABLE_NAME_LEN &&
                schema.find('\0')==std::string::npos && schema.find('/')==std::string::npos &&
                schema.find('\\')==std::string::npos && schema!="." && schema!="..";
            if(storedName && std::filesystem::exists(engine.dbPath(database)/(".schema_"+schema)))return false;
            return schema!="pg_catalog" && std::none_of(snapshot.namespaces.begin(),snapshot.namespaces.end(),
                [&](const auto& entry){return entry.nspname==schema;});
        }),actual.end());
    }
    return actual;
}
} // namespace dbms
