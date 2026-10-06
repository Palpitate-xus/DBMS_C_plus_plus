#pragma once

#include "parser/ast.h"
#include "expression/ExprEvaluator.h"
#include <functional>
#include <optional>
#include <set>

namespace dbms {

struct QueryOutputColumn {
    std::string name, type;
    bool generated = false;
    char identity = 0;
};
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
    // Optional pure assignment-context validation. Never execute a routine,
    // query or source row to infer its type/value. source is null when the
    // descriptor has no direct value-expression leaf (star/set/derived).
    std::function<void(const QueryOutputColumn&, const Expr*, const std::string&)> assignmentInput;
};
struct PreparedQuery {
    struct Use { size_t begin, end, slot; };
    struct SourceRange {
        size_t ordinal;
        const Stmt* owner;
        const FromItem* source;
        std::string schema, name;
        QueryRowDescriptor columns;
        bool mergedUsing;
        // The physical identity is independent of the visible SQL alias.
        std::string relationSchema, relationName;
        // A logical CTE is a prepared statement, never a physical table
        // found by reinterpreting its spelling at execution time.
        const Stmt* cteStatement = nullptr;
        // Actual namespace visibility after JOIN USING/NATURAL merging or
        // RETURNING transition registration. Qualified references still
        // retain every original column; unqualified stars must skip these.
        std::set<std::string> hiddenUnqualified;
        // A view keeps its independently prepared query and namespace. Its
        // output cells are rebound only to this occurrence, never flattened
        // into the caller's textual SQL or procedural parameter namespace.
        std::shared_ptr<PreparedQuery> viewQuery;
    };
    std::string source;
    StmtPtr ast;
    std::vector<ExprValue> parameters;
    std::vector<Use> uses;
    std::vector<std::pair<size_t, std::string>> projectionAliases;
    QueryRowDescriptor output;
    // Source pointers belong to ast; their identities survive AST ownership
    // moves. Ordinals are unique in this prepared query, not SQL text keys.
    std::vector<SourceRange> sourceRanges;
    std::map<const Stmt*, QueryRowDescriptor> statementOutputs;
    // Transitional adapter for dispatchers that still parse SQL strings.
    // Encoding is permitted only after whole-tree preparation has succeeded.
    std::string legacySql() const;
};

PreparedQuery prepareQuery(const std::string& sql,
    const std::vector<QueryBindingDatum>& datums, const QueryBindingMetadata& metadata);
} // namespace dbms
