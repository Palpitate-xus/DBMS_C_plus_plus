#pragma once

#include <optional>
#include <string>

namespace dbms {

class StorageEngine;
struct Column;
class Expr;
struct TableSchema;

// The complete scalar key is available only from the physical B-tree owner.
bool completeStoredZonedClockIndex(StorageEngine& engine, const std::string& database,
                                   const TableSchema& schema, const Column& key);

// Decode a literal or a literal cast to the builtin TIMETZ. Everything else
// stays with its original expression evaluator.
std::optional<std::string> constantStoredZonedClockInput(StorageEngine& engine,
    const std::string& database, const Expr* input);

void validateStoredZonedClockEqualityInput(const Column& column, const std::string& value);

} // namespace dbms
