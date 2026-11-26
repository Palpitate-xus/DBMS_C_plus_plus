#pragma once

#include "common/DbError.h"
#include "utils/Session.h"

#include <algorithm>
#include <string_view>

namespace dbms {

inline std::string activeTemporarySchema(const Session* session) {
    return session && session->tempNamespaceCreated ? sessionTempSchemaName(*session) : std::string{};
}

inline std::vector<std::string> effectiveRelationSearchPath(const Session* session) {
    std::vector<std::string> path;
    std::string canonical;
    if (!session || !parseSessionSearchPath(session->searchPath, path, canonical))
        path = {"public"};

    const auto temporary = activeTemporarySchema(session);
    if (session) {
        for (auto& schema : path) {
            schema = expandSessionSearchPathEntry(schema, session->username);
            if (schema == "pg_temp") schema = temporary;
        }
        path.erase(std::remove(path.begin(), path.end(), std::string{}), path.end());
    }
    if (std::find(path.begin(), path.end(), "pg_catalog") == path.end())
        path.insert(path.begin(), "pg_catalog");
    if (!temporary.empty() && std::find(path.begin(), path.end(), temporary) == path.end())
        path.insert(path.begin(), temporary);
    return path;
}

inline void requireWritableBuiltinRelation(std::string_view schema, std::string_view name) {
    if (schema == "pg_catalog" &&
        (name == "pg_type" || name == "pg_am" || name == "pg_collation" || name == "pg_constraint"))
        throw DbError("0A000", "writing pg_catalog." + std::string(name) + " is not supported");
}

}  // namespace dbms
