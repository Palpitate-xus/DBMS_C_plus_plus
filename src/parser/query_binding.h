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
    uint32_t typeOid = 0;
    int32_t typeMod = -1; // same-generation physical assignment metadata
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
    // Existing public datum initializers describe statement-owned frozen
    // inputs. Synthetic runtime/metadata producers override this explicitly.
    ParameterOrigin origin = ParameterOrigin::StatementInput;
    // SQL-function argument names yield to a matching SQL range column;
    // PL/pgSQL locals retain the existing ambiguity check by default.
    bool columnPrecedence = false;
};
struct QueryEnumType {
    std::string identity, typeName;
    uint32_t typeOid = 0;
    std::vector<std::string> labels;
};
struct QueryBindingMetadata {
    // Must return a copied, metadata-only descriptor, never execute a query.
    std::function<QueryRelationMetadata(const std::string&)> relation;
    std::function<std::string(const FunctionCallExpr*)> functionType;
    // Optional pure assignment-context validation. Never execute a routine,
    // query or source row to infer its type/value. source is null when the
    // descriptor has no direct value-expression leaf (star/set/derived).
    std::function<void(const QueryOutputColumn&, const Expr*, const std::string&)> assignmentInput;
    // Canonical query-host role/signature metadata, not a scalar callback.
    // A stored routine with the same spelling must never inherit this role.
    // Appended so existing aggregate initialization keeps its callback roles.
    std::function<std::optional<QuerySetReturningBinding>(const FunctionCallExpr*)> setReturning;
    // Pure operator type lookup. A physical catalog OID wins over a rendered
    // alias/name; domains resolve to their actual base without reading rows.
    std::function<std::string(const std::string&, uint32_t)> baseType;
    // INSERT/UPDATE DEFAULT is a stored value expression of the resolved physical
    // target, not a source/PL variable. nullopt means no default (typed NULL
    // at assignment); lookup never evaluates the expression or scans rows.
    std::function<std::optional<std::string>(const std::string&,
        const std::string&, const std::string&)> updateDefault;
    // Same-generation catalog type + ordered enum labels, copied once.
    // An actual source OID wins over the current search_path spelling.
    std::function<std::optional<QueryEnumType>(const std::string&, uint32_t)> enumType;
    // Resolve a grammar type declaration against the same copied catalog as
    // relation/operator metadata. This must not evaluate input or a routine.
    std::function<QueryOutputColumn(const std::string&)> declaredType;
    // An explicit query-output receiver (e.g. an SQL function body) resolves
    // remaining UNKNOWN outputs to real TEXT declarations after binding.
    // Appended to preserve positional aggregate initialization of callbacks.
    // INSERT SELECT assignment contexts do not opt into this boundary.
    bool finalizeUnknownOutput = false;
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
    struct ProjectionBinding {
        // Original, live value-expression site, including a star that expands
        // to several output ordinals. Never reconstructed from column names.
        const Expr* expression = nullptr;
        std::optional<QueryColumnBinding> column;
    };
    // SELECT expanded output ordinal -> genuine expression/source binding.
    // Compound/parameter outputs retain their expression with no source cell.
    std::map<const SelectStmt*, std::vector<ProjectionBinding>> projectionBindings;
    struct SetOperationInputs {
        // Static output types of the real left SELECT body (or composed
        // lhs) and right child, before execution's common-type conversion.
        QueryRowDescriptor left, right;
    };
    // Inline-left and wrapped set queries retain their original AST shape.
    // An executor must not invent a replacement SELECT by rendering SQL.
    std::map<const SelectStmt*, SetOperationInputs> setOperationInputs;
    // Global set ORDER keys refer to genuine expanded output ordinals, not
    // branch source columns or an invented SQL range. The vector follows the
    // actual root's retained orderBy entries and is bound before execution.
    std::map<const SelectStmt*, std::vector<size_t>> setOrderColumns;
    // Transitional adapter for dispatchers that still parse SQL strings.
    // Encoding is permitted only after whole-tree preparation has succeeded.
    std::string legacySql() const;
};

PreparedQuery prepareQuery(const std::string& sql,
    const std::vector<QueryBindingDatum>& datums, const QueryBindingMetadata& metadata);
} // namespace dbms
