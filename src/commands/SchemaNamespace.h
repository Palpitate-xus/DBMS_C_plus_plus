#pragma once

#include "commands/TableManage.h"
#include <set>
#include <string>

namespace dbms {

// Shared namespace owner. SQL callers retain their real privilege check and
// session; native callers supply their actual engine/database. Neither path
// changes currentSession(), redirects engines or fabricates an administrator.
struct SchemaNamespaceResult {
    DBStatus status = DBStatus::OK;
    std::string error;
    std::set<std::string> droppedSequences;
};

SchemaNamespaceResult createSchemaNamespace(StorageEngine& engine,
    const std::string& database, const std::string& schema,
    Session* sqlSession = nullptr);
SchemaNamespaceResult dropSchemaNamespace(StorageEngine& engine,
    const std::string& database, const std::string& schema, bool cascade,
    Session* sqlSession = nullptr);

} // namespace dbms
