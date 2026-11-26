#pragma once

#include "executor/ExecutionPlan.h"

#include <optional>

namespace dbms {

struct PreparedIntegerKeyOrder {
    size_t columnOrdinal;
    bool ascending;
    bool canStopAtLimit = false;
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

struct PreparedViewIntegerLookup {
    QueryColumnBinding key;
    int64_t value;
};

// Translate necessary equalities on a view's output into its bound input
// occurrences. Every query keeps its original predicates.
std::vector<PreparedViewIntegerLookup> preparedViewIntegerLookups(const PreparedQuery& parent,
    const PreparedQuery::SourceRange& range, const std::vector<PreparedViewIntegerLookup>& lookups);

// Shared physical source construction for native and frontend prepared
// graphs. This never opens a source or evaluates a query expression.
OpPtr makePreparedTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, SelectStmt* select,
    const TableSchema& schema, OpPtr source,
    const std::vector<PreparedViewIntegerLookup>& viewLookups = {});

// Traditional inheritance is one SQL range. Descendants are returned once
// even when more than one path reaches a relation.
std::vector<std::string> inheritedTableDescendants(StorageEngine& engine,
    const std::string& database, const std::string& parent);
bool supportsInheritedTableSource(StorageEngine& engine, const std::string& database,
    const TableSchema& parent, const std::vector<std::string>& descendants);
OpPtr makeInheritedTableSource(StorageEngine& engine, const std::string& database,
    const TableSchema& parent, const PreparedQuery::SourceRange& range,
    const std::vector<std::string>& descendants, bool allColumns = false);

} // namespace dbms
