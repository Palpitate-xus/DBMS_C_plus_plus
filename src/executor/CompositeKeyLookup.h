#pragma once

#include "commands/TableManage.h"

namespace dbms::composite_key_lookup {

// Empty means the predicates do not prove equality on the complete key.
std::map<std::string, std::string> compactValues(
    const TableSchema& table, const std::vector<StorageEngine::Condition>& conditions);

// Storage predicates for a target-only, fully bound equality conjunction.
// The original bound WHERE still owns qualification and locked-row rechecks.
std::map<std::string, std::string> boundMutationValues(
    const TableSchema& table, const Expr* predicate, size_t target);

} // namespace dbms::composite_key_lookup
