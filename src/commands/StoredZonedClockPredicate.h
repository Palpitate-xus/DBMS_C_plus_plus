#pragma once

#include <optional>
#include <string>

#include "commands/TableManage.h"

namespace dbms {

class Expr;
struct PreparedQuery;
struct SelectStmt;

// The complete scalar key is available only from the physical B-tree owner.
bool completeStoredZonedClockIndex(StorageEngine& engine, const std::string& database,
                                   const TableSchema& schema, const Column& key);

// Decode a literal or a literal cast to the builtin TIMETZ. Everything else
// stays with its original expression evaluator.
std::optional<std::string> constantStoredZonedClockInput(StorageEngine& engine,
    const std::string& database, const Expr* input);

void validateStoredZonedClockComparisonInput(const Column& column, const std::string& value);

struct StoredBtreeRange {
    enum class Key { ZonedClock, ObjectNumber };
    std::string column;
    int64_t lower = INT64_MIN;
    int64_t upper = INT64_MAX;
    Key key = Key::ZonedClock;

    bool matches(const std::string& value) const;
};

std::optional<StoredBtreeRange> storedBtreeRange(StorageEngine& engine,
    const std::string& database, const TableSchema& schema, const Expr* predicate,
    size_t sourceOrdinal);

std::optional<StoredBtreeRange> storedBtreeRange(StorageEngine& engine,
    const std::string& database, const TableSchema& schema,
    const std::vector<StorageEngine::Condition>& conditions);

// An engaged result covers the whole range for the current snapshot, even
// when it has no rows. An unavailable or expensive read uses the heap.
std::optional<std::vector<int64_t>> readStoredBtreeRange(StorageEngine& engine,
    const std::string& database, const TableSchema& schema, const StoredBtreeRange& range);

struct StoredZonedClockOrder {
    static constexpr size_t maxRows = 512;
    size_t count;
    bool descending;
    bool nullsFirst;
    bool excludesNulls;
    bool withTies;
};

struct StoredZonedClockPage {
    struct Row {
        int64_t rid;
        std::string data;
        std::vector<bool> nulls;
    };
    std::vector<Row> rows;
    size_t indexNodes = 0;
};

std::optional<StoredZonedClockPage> readStoredZonedClockPage(StorageEngine& engine,
    const std::string& database, const TableSchema& schema, const StoredBtreeRange& range,
    const StoredZonedClockOrder& order, size_t byteBudget);

// Query admission and source construction use the same bound order and range.
bool mayUseStoredZonedClockPage(StorageEngine& engine, const std::string& database,
                               const PreparedQuery& query, const SelectStmt& select);

} // namespace dbms
