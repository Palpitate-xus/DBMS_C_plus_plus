#pragma once

#include "executor/ExecutionPlan.h"

namespace dbms {

// Shared physical source construction for native and frontend prepared
// graphs. This never opens a source or evaluates a query expression.
OpPtr makePreparedTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, SelectStmt* select,
    const TableSchema& schema, OpPtr source);

} // namespace dbms
