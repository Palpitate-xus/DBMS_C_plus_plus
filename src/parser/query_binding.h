#pragma once

#include "parser/ast.h"
#include "expression/ExprEvaluator.h"
#include <functional>
#include <optional>

namespace dbms {

struct QueryOutputColumn { std::string name, type; };
using QueryRowDescriptor = std::vector<QueryOutputColumn>;
struct QueryRelationMetadata {
    std::string schema, name;
    QueryRowDescriptor columns;
    std::string viewSql;
};
struct QueryBindingDatum {
    std::string identity, name, type;
    std::vector<std::string> qualifiers; // separately canonicalized name parts
    bool visible = true;                // false for a shadowed outer datum
    std::optional<std::string> value;
    size_t position = 0;                // $n alias of a function parameter
};
struct QueryBindingMetadata {
    // Must return a copied, metadata-only descriptor, never execute a query.
    std::function<QueryRelationMetadata(const std::string&)> relation;
    std::function<std::string(const FunctionCallExpr*)> functionType;
};
struct PreparedQuery {
    struct Use { size_t begin, end, slot; };
    std::string source;
    StmtPtr ast;
    std::vector<ExprValue> parameters;
    std::vector<Use> uses;
    std::vector<std::pair<size_t, std::string>> projectionAliases;
    QueryRowDescriptor output;
    // Transitional adapter for dispatchers that still parse SQL strings.
    // Encoding is permitted only after whole-tree preparation has succeeded.
    std::string legacySql() const;
};

PreparedQuery prepareQuery(const std::string& sql,
    const std::vector<QueryBindingDatum>& datums, const QueryBindingMetadata& metadata);
} // namespace dbms
