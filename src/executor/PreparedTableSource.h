#pragma once

#include "executor/ExecutionPlan.h"

#include <optional>

namespace dbms {

struct PreparedIntegerKeyOrder {
    size_t columnOrdinal;
    bool ascending;
};

// Available only while the physical page owns rows in checked key order.
std::optional<PreparedIntegerKeyOrder> openedIntegerPrimaryKeyOrder(const Operator& source);

// A transparent logical source may carry the physical proof for the exact
// bound occurrence it reads. The proof belongs to its current execution.
class PreparedOrderedTableInput {
public:
    virtual ~PreparedOrderedTableInput() = default;
    virtual std::optional<PreparedIntegerKeyOrder> integerKeyOrder(size_t sourceOrdinal) const = 0;
};

// Shared physical source construction for native and frontend prepared
// graphs. This never opens a source or evaluates a query expression.
OpPtr makePreparedTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, SelectStmt* select,
    const TableSchema& schema, OpPtr source);

} // namespace dbms
