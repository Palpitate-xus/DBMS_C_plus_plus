#pragma once

#include <optional>
#include <string>

#include "commands/TableManage.h"

namespace dbms {

class Expr;

// The complete scalar key is available only from the physical B-tree owner.
bool completeStoredZonedClockIndex(StorageEngine& engine, const std::string& database,
                                   const TableSchema& schema, const Column& key);

// Decode a literal or a literal cast to the builtin TIMETZ. Everything else
// stays with its original expression evaluator.
std::optional<std::string> constantStoredZonedClockInput(StorageEngine& engine,
    const std::string& database, const Expr* input);

void validateStoredZonedClockComparisonInput(const Column& column, const std::string& value);

struct StoredZonedClockRange {
    std::string column;
    int64_t lower = INT64_MIN;
    int64_t upper = INT64_MAX;

    bool matches(const std::string& value) const;
};

std::optional<StoredZonedClockRange> storedZonedClockRange(StorageEngine& engine,
    const std::string& database, const TableSchema& schema, const Expr* predicate,
    size_t sourceOrdinal);

std::optional<StoredZonedClockRange> storedZonedClockRange(StorageEngine& engine,
    const std::string& database, const TableSchema& schema,
    const std::vector<StorageEngine::Condition>& conditions);

// An engaged result covers the whole range for the current snapshot, even
// when it has no rows. An unavailable or expensive read uses the heap.
std::optional<std::vector<int64_t>> readStoredZonedClockRange(StorageEngine& engine,
    const std::string& database, const TableSchema& schema, const StoredZonedClockRange& range);

} // namespace dbms
