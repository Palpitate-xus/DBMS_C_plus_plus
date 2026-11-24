#pragma once

#include "common/DbError.h"
#include "utils/Session.h"

#include <algorithm>
#include <string_view>

namespace dbms {

inline std::vector<std::string> effectiveRelationSearchPath(const Session* session) {
    std::vector<std::string> path;
    std::string canonical;
    if (!session || !parseSessionSearchPath(session->searchPath, path, canonical))
        path = {"public"};

    const std::string temporary = session ? sessionTempSchemaName(*session) : "";
    if (session) {
        for (auto& schema : path) {
            schema = expandSessionSearchPathEntry(schema, session->username);
            if (schema == "pg_temp") schema = temporary;
        }
    }
    if (std::find(path.begin(), path.end(), "pg_catalog") == path.end())
        path.insert(path.begin(), "pg_catalog");
    if (session && std::find(path.begin(), path.end(), temporary) == path.end())
        path.insert(path.begin(), temporary);
    return path;
}

inline void requireWritableBuiltinRelation(std::string_view schema, std::string_view name) {
    if (schema == "pg_catalog" && (name == "pg_type" || name == "pg_am" || name == "pg_collation"))
        throw DbError("0A000", "writing pg_catalog." + std::string(name) + " is not supported");
}

}  // namespace dbms
