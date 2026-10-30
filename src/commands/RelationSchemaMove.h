#pragma once

#include "interfaces/dbms_defs.h"
#include <functional>
#include <string>

namespace dbms {
class StorageEngine;
struct TableSchema;

// One explicit engine/database owner for SQL and native namespace moves.
// The engine supplies its private schema writer, preserving declaration
// provenance rather than turning a metadata rewrite into ALTER DEFAULT.
DBStatus moveTableSchema(StorageEngine& engine, const std::string& database,
    const std::string& physicalName, const std::string& targetSchema,
    const std::function<bool(const TableSchema&)>& persistSchema);
}
