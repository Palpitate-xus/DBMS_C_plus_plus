#pragma once

#include "parser/query_binding.h"

namespace dbms {

class Operator;
class StorageEngine;

namespace description_catalog {

// Returns an aggregate source only for counts over the owned catalog with
// row-local predicates whose evaluation cannot fail or have side effects.
std::unique_ptr<Operator> makeCountSource(StorageEngine& engine, const std::string& database,
                                         const PreparedQuery::SourceRange& range);

} // namespace description_catalog
} // namespace dbms
