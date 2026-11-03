#pragma once

#include "catalog/catalog.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "expression/aggregate_type.h"
#include "parser/query_binding.h"

#include <algorithm>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace dbms::prepared_window_detail {

// Private retained-AST descriptions. These own no parameter, source,
// execution provider or query-child cursor; every pointer names its actual
// wholly bound query node.
struct Producer {
    FunctionCallExpr* call = nullptr;
    std::string name, inputType, resultType;
    QueryColumnBinding column;
    std::optional<QueryComparisonBinding> valueComparison;
};
struct OrderKey {
    Expr* expression = nullptr;
    bool asc = true, nullsFirst = false;
    std::optional<QueryComparisonBinding> comparison;
};
struct ResolvedSpec {
    std::vector<Expr*> partition;
    std::vector<std::optional<QueryComparisonBinding>> partitionComparisons;
    std::vector<OrderKey> order;
    std::string frameMode, exclusion;
    WindowDef::BoundKind start = WindowDef::BoundKind::Unspecified;
    WindowDef::BoundKind end = WindowDef::BoundKind::Unspecified;
    Expr* startOffset = nullptr;
    Expr* endOffset = nullptr;
    // A PARTITION key also present in ORDER BY inherits that key's sort
    // direction/NULL order, as transformGroupClause does in PostgreSQL.
    std::vector<bool> partitionAsc, partitionNullsFirst;
};
struct Stage {
    ResolvedSpec spec;
    std::vector<size_t> producers;
    // Full specification identity groups window stages. keyIdentity omits
    // framing, allowing a later stage to reuse the actual typed key stream.
    std::string identity, keyIdentity;
};

inline ResolvedSpec resolve(SelectStmt&, const WindowDef&);
inline std::vector<Producer> collect(SelectStmt&, const PreparedQuery&,
    StorageEngine*, const std::string& database);
inline std::vector<Stage> stages(SelectStmt&, const PreparedQuery&,
    const std::vector<Producer>&, StorageEngine*, const std::string& database);

namespace spec_detail {
inline std::string identifier(const std::string& spelling) {
    CatalogManager::QualifiedName name;
    if (!CatalogManager::parseQualifiedName(spelling, name, true) || !name.schema.empty())
        throw DbError("42601", "invalid window name: " + spelling);
    return name.name;
}

inline ResolvedSpec resolve(SelectStmt& select, const WindowDef& definition,
                            std::set<const WindowDef*>& visiting) {
    if (!visiting.insert(&definition).second)
        throw DbError("42P20", "circular window inheritance");
    const auto declaration = std::find_if(select.windowDefs.begin(), select.windowDefs.end(),
        [&](const auto& candidate) { return &candidate == &definition; });
    const bool declared = declaration != select.windowDefs.end();
    const auto reference = !definition.referenceName.empty() ? definition.referenceName
        : declared ? std::string() : definition.name;
    ResolvedSpec result;
    if (!reference.empty()) {
        const auto name = identifier(reference);
        const auto parent = std::find_if(select.windowDefs.begin(), select.windowDefs.end(),
            [&](const auto& candidate) { return identifier(candidate.name) == name; });
        if (parent == select.windowDefs.end() || (declared && parent >= declaration))
            throw DbError("42704", "window \"" + name + "\" does not exist");
        result = resolve(select, *parent, visiting);
        if (definition.referenceName.empty()) {
            visiting.erase(&definition);
            return result; // OVER name keeps its actual parent's frame.
        }
        if (!definition.partitionBy.empty())
            throw DbError("42P20", "cannot override PARTITION BY clause of window \"" + name + "\"");
        if (!definition.orderBy.empty() && !result.order.empty())
            throw DbError("42P20", "cannot override ORDER BY clause of window \"" + name + "\"");
        if (!parent->frameMode.empty())
            throw DbError("42P20", "cannot copy window \"" + name + "\" because it has a frame clause");
    }
    if (reference.empty()) {
        for (size_t i = 0; i < definition.partitionBy.size(); ++i) {
            result.partition.push_back(definition.partitionBy[i].get());
            result.partitionComparisons.push_back(i < definition.partitionComparisons.size()
                ? definition.partitionComparisons[i] : std::nullopt);
        }
    }
    if (!definition.orderBy.empty()) {
        result.order.clear();
        for (size_t i = 0; i < definition.orderBy.size(); ++i) {
            const auto& key = definition.orderBy[i];
            const bool nullsFirst = i < definition.orderNullsFirst.size() &&
                definition.orderNullsFirst[i].has_value()
                ? *definition.orderNullsFirst[i] : !key.second;
            result.order.push_back({key.first.get(), key.second, nullsFirst,
                i < definition.orderComparisons.size() ? definition.orderComparisons[i] : std::nullopt});
        }
    }
    // A copied window inherits only partition/order. Its frame is its own
    // explicit clause or the real default RANGE UNBOUNDED PRECEDING/CURRENT.
    result.frameMode = definition.frameMode.empty() ? "range" : definition.frameMode;
    result.exclusion = definition.frameExclusion == "no others" ? "" : definition.frameExclusion;
    result.start = definition.frameStartKind == WindowDef::BoundKind::Unspecified
        ? WindowDef::BoundKind::UnboundedPreceding : definition.frameStartKind;
    result.end = definition.frameEndKind == WindowDef::BoundKind::Unspecified
        ? WindowDef::BoundKind::CurrentRow : definition.frameEndKind;
    result.startOffset = definition.frameStart.get();
    result.endOffset = definition.frameEnd.get();
    result.partitionAsc.assign(result.partition.size(), true);
    result.partitionNullsFirst.assign(result.partition.size(), false);
    visiting.erase(&definition);
    return result;
}

template <typename Visit>
inline void valueChildren(Expr* expression, const Visit& visit) {
    if (auto* unary = dynamic_cast<UnaryOpExpr*>(expression)) visit(unary->operand.get());
    else if (auto* binary = dynamic_cast<BinaryOpExpr*>(expression)) {
        visit(binary->left.get());
        if (binary->op != "::") visit(binary->right.get());
    } else if (auto* quantified = dynamic_cast<QuantifiedComparisonExpr*>(expression)) {
        visit(quantified->left.get()); visit(quantified->right.get());
    } else if (auto* cast = dynamic_cast<CastExpr*>(expression)) visit(cast->operand.get());
    else if (auto* conditional = dynamic_cast<CaseExpr*>(expression)) {
        visit(conditional->switchExpr.get());
        for (auto& arm : conditional->whenClauses) {
            visit(arm.first.get()); visit(arm.second.get());
        }
        visit(conditional->elseExpr.get());
    } else if (auto* call = dynamic_cast<FunctionCallExpr*>(expression)) {
        for (auto& argument : call->args) visit(argument.get());
        for (auto& argument : call->namedArgs) visit(argument.value.get());
        visit(call->filter.get());
        for (auto& key : call->aggregateOrderKeys) visit(key.expr.get());
    } else if (auto* array = dynamic_cast<ArrayExpr*>(expression))
        for (auto& field : array->elements) visit(field.get());
    else if (auto* record = dynamic_cast<RowExpr*>(expression))
        for (auto& field : record->elements) visit(field.get());
}

inline std::string inputType(const Expr* value, StorageEngine* engine,
                              const std::string& database) {
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(value);
        literal && !literal->preparedSubquery && literal->value == "*") return "*";
    return ExprHelper::canonicalResultTypeName(
        ExprHelper::inferParsedResultType(value, {}, database, engine));
}

inline std::optional<QueryComparisonBinding> enumValueComparison(const Producer& producer,
    const PreparedQuery& query, StorageEngine* engine, const std::string& database) {
    if (!engine || (producer.name != "min" && producer.name != "max") ||
        producer.call->args.size() != 1 ||
        (producer.inputType.size() >= 2 &&
         producer.inputType.compare(producer.inputType.size() - 2, 2, "[]") == 0))
        return std::nullopt;
    const auto* argument = producer.call->args.front().get();
    uint32_t oid = 0;
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(argument); column && column->binding) {
        const auto& binding = *column->binding;
        oid = binding.typeOid;
        // The original source occurrence's descriptor is authoritative,
        // including a domain OID and a user enum named like a builtin.
        for (const auto& source : query.sourceRanges)
            if (source.ordinal == binding.sourceOrdinal && binding.columnOrdinal < source.columns.size() &&
                source.columns[binding.columnOrdinal].typeOid)
                oid = source.columns[binding.columnOrdinal].typeOid;
    } else if (argument->preparedSubquery) {
        const auto output = query.statementOutputs.find(argument->preparedSubquery.get());
        if (output != query.statementOutputs.end() && output->second.size() == 1)
            oid = output->second.front().typeOid;
    }
    CatalogManager::QualifiedName declaration;
    if (!oid) {
        // A bound MIN/MAX result has the same enum identity as its input.
        // Only an already qualified declaration may stand in for a missing
        // expression OID; never repeat search_path lookup for a bare name.
        if (!CatalogManager::parseQualifiedName(producer.call->resolvedResultType, declaration, true) ||
            declaration.schema.empty()) return std::nullopt;
    } else {
        for (const auto& builtin : CatalogManager::builtinTypeRows())
            if (builtin.oid == oid && builtin.typtype != 'd' && builtin.typtype != 'e')
                return std::nullopt;
    }
    const auto catalog = engine->catalogService().metadataSnapshot(database);
    const PgTypeRow* type = nullptr;
    if (oid) {
        for (const auto& candidate : catalog.types)
            if (candidate.oid == oid) { type = &candidate; break; }
    } else {
        uint32_t namespaceOid = 0;
        for (const auto& space : catalog.namespaces)
            if (space.nspname == declaration.schema) { namespaceOid = space.oid; break; }
        for (const auto& candidate : catalog.types)
            if (candidate.typnamespace == namespaceOid && candidate.typname == declaration.name) {
                type = &candidate; break;
            }
    }
    std::set<uint32_t> visited;
    while (type && type->typtype == 'd') {
        if (!visited.insert(type->oid).second || visited.size() > 64)
            throw DbError("XX001", "circular window input domain metadata");
        const uint32_t base = type->typbasetype;
        type = nullptr;
        for (const auto& candidate : catalog.types)
            if (candidate.oid == base) { type = &candidate; break; }
    }
    if (!type) {
        if (oid) throw DbError("0A000", "window input type has no available comparison metadata");
        return std::nullopt;
    }
    if (type->typtype != 'e') return std::nullopt;
    const auto quoted = [](const std::string& name) {
        std::string result = "\"";
        for (char ch : name) { result += ch; if (ch == '"') result += ch; }
        return result + '"';
    };
    std::string schema;
    for (const auto& space : catalog.namespaces)
        if (space.oid == type->typnamespace) { schema = space.nspname; break; }
    if (schema.empty()) throw DbError("XX001", "window enum namespace metadata is missing");
    QueryComparisonBinding result;
    result.op = "<"; result.strict = true; result.enumTypeOid = type->oid;
    result.leftType = result.rightType = quoted(schema) + "." + quoted(type->typname);
    result.identity = "enum:" + database + ":" + std::to_string(type->oid);
    std::vector<const PgEnumRow*> labels;
    for (const auto& label : catalog.enumLabels)
        if (label.enumtypid == type->oid) labels.push_back(&label);
    std::sort(labels.begin(), labels.end(),
        [](const auto* left, const auto* right) { return left->enumsortorder < right->enumsortorder; });
    for (const auto* label : labels) {
        result.enumLabels.push_back(label->enumlabel);
        result.identity += ":" + std::to_string(label->enumlabel.size()) + ":" + label->enumlabel;
    }
    result.identity += ":<";
    return result;
}

inline Producer producer(FunctionCallExpr& call, const PreparedQuery& query, StorageEngine* engine,
                          const std::string& database, ExprEvaluator& evaluator) {
    CatalogManager::QualifiedName routine;
    const auto spelling = call.schema.empty() ? call.funcName : call.schema + "." + call.funcName;
    if (evaluator.hasScalarFunction(&call, engine))
        throw DbError("42809", "OVER specified, but " + spelling +
            " is not a window function nor an aggregate function");
    if (!CatalogManager::parseQualifiedName(spelling, routine, true) ||
        (!routine.schema.empty() && routine.schema != "pg_catalog"))
        throw DbError("42883", "function does not exist: " + spelling);
    if (call.setReturning)
        throw DbError("42809", "OVER specified for a set-returning function");
    if (call.distinct)
        throw DbError("0A000", "DISTINCT is not implemented for window functions");
    if (!call.orderBy.empty() || !call.aggregateOrderKeys.empty())
        throw DbError("0A000", "aggregate ORDER BY is not implemented for window functions");
    if (!call.namedArgs.empty())
        throw DbError("42883", "function does not exist: " + spelling);
    const auto& name = routine.name;
    const size_t count = call.args.size();
    const bool ranking = name == "row_number" || name == "rank" || name == "dense_rank" ||
        name == "percent_rank" || name == "cume_dist";
    const bool shifted = name == "lag" || name == "lead";
    const bool selected = name == "first_value" || name == "last_value" || name == "nth_value";
    const bool aggregate = name == "count" || name == "sum" || name == "avg" ||
        name == "min" || name == "max" || name == "bool_and" || name == "bool_or" ||
        name == "every" || name == "array_agg" || name == "string_agg" ||
        name == "bit_and" || name == "bit_or" || name == "bit_xor";
    const auto integer = [&](size_t index) {
        const auto type = inputType(call.args[index].get(), engine, database);
        if (type == "smallint" || type == "integer") return true;
        const auto* literal = dynamic_cast<const LiteralExpr*>(call.args[index].get());
        return type == "unknown" && literal && !literal->preparedSubquery;
    };
    bool signature = (ranking && count == 0) ||
        (shifted && count >= 1 && count <= 3 && (count < 2 || integer(1))) ||
        ((name == "first_value" || name == "last_value") && count == 1) ||
        (name == "nth_value" && count == 2 && integer(1)) ||
        (name == "ntile" && count == 1 && integer(0)) ||
        (aggregate && count == (name == "string_agg" ? 2u : 1u));
    if (!signature)
        throw DbError("42883", "function does not exist: " + spelling);
    if (!aggregate && call.filter)
        throw DbError("0A000", "FILTER is not implemented for non-aggregate window functions");
    Producer result;
    result.call = &call;
    result.name = name;
    result.inputType = count ? inputType(call.args.front().get(), engine, database) : "";
    result.resultType = ExprHelper::canonicalResultTypeName(call.resolvedResultType);
    result.valueComparison = enumValueComparison(result, query, engine, database);
    if (result.valueComparison) result.inputType = result.valueComparison->leftType;
    if (result.resultType.empty() || result.resultType == "unknown")
        throw DbError("XX000", "window producer lost its bound result declaration");
    if (aggregate && name != "array_agg" && name != "string_agg" &&
        name != "bit_and" && name != "bit_or" && name != "bit_xor") {
        const auto expected = aggregate_type_detail::resultType(&call, name, result.inputType);
        if (ExprHelper::canonicalResultTypeName(expected) != result.resultType)
            throw DbError("XX000", "window aggregate changed its bound result declaration");
    }
    if ((shifted || selected || name == "array_agg") && result.inputType == "*")
        throw DbError("42883", "function does not exist: " + spelling);
    if (name == "string_agg") {
        const auto delimiter = inputType(call.args[1].get(), engine, database);
        if ((result.resultType != "text" && result.resultType != "bytea") ||
            result.inputType != result.resultType || delimiter != result.resultType)
            throw DbError("42883", "function does not exist: " + spelling);
    }
    if (name == "bit_and" || name == "bit_or" || name == "bit_xor") {
        const bool integral = result.inputType == "smallint" || result.inputType == "integer" ||
            result.inputType == "bigint";
        if (!integral && result.inputType != "bit")
            throw DbError("42883", "function does not exist: " + spelling);
        if (result.resultType != result.inputType)
            throw DbError("XX000", "window bit aggregate changed its bound result declaration");
    }
    return result;
}

inline std::string field(const std::string& value) {
    return std::to_string(value.size()) + ":" + value;
}
inline std::string expressionIdentity(const Expr* expression, const PreparedQuery& query,
    StorageEngine* engine, const std::string& database, const SelectStmt& select) {
    return ExprHelper::preparedSortExpressionIdentity(expression, query,
        [](const ColumnRefExpr& column) {
            if (!column.binding) throw DbError("XX000", "window key lost its actual source binding");
            const auto& binding = *column.binding;
            return std::to_string(binding.scopeDepth) + ":" + std::to_string(binding.sourceOrdinal) +
                ":" + std::to_string(binding.columnOrdinal) + ":" + binding.declaredType +
                ":" + std::to_string(binding.typeOid);
        }, database, engine, false, &select);
}
inline std::string comparisonIdentity(const std::optional<QueryComparisonBinding>& comparison) {
    if (!comparison) return "builtin";
    return field(comparison->identity) + field(comparison->collation) +
        field(std::to_string(comparison->enumTypeOid));
}
inline void normalize(ResolvedSpec& spec, const PreparedQuery& query,
    StorageEngine* engine, const std::string& database, const SelectStmt& select) {
    std::set<std::string> partitions, orders;
    std::vector<Expr*> partition;
    std::vector<std::optional<QueryComparisonBinding>> comparisons;
    for (size_t i = 0; i < spec.partition.size(); ++i) {
        const auto identity = expressionIdentity(spec.partition[i], query, engine, database, select) +
            field(comparisonIdentity(spec.partitionComparisons.at(i)));
        if (!partitions.insert(identity).second) continue;
        partition.push_back(spec.partition[i]);
        comparisons.push_back(spec.partitionComparisons[i]);
    }
    spec.partition = std::move(partition);
    spec.partitionComparisons = std::move(comparisons);
    std::vector<OrderKey> order;
    for (const auto& key : spec.order) {
        const auto identity = expressionIdentity(key.expression, query, engine, database, select) +
            field(comparisonIdentity(key.comparison));
        // The supported canonical < and > are commutators. A later use of
        // that datum cannot break any further tie, regardless of NULL order.
        if (orders.insert(identity).second) order.push_back(key);
    }
    spec.order = std::move(order);
    spec.partitionAsc.assign(spec.partition.size(), true);
    spec.partitionNullsFirst.assign(spec.partition.size(), false);
    for (size_t i = 0; i < spec.partition.size(); ++i) {
        const auto identity = expressionIdentity(spec.partition[i], query, engine, database, select);
        for (const auto& key : spec.order)
            if (identity == expressionIdentity(key.expression, query, engine, database, select)) {
                spec.partitionAsc[i] = key.asc;
                spec.partitionNullsFirst[i] = key.nullsFirst;
                break;
            }
    }
}
inline std::string keyIdentity(const ResolvedSpec& spec, const PreparedQuery& query,
    StorageEngine* engine, const std::string& database, const SelectStmt& select) {
    std::string result = "partition";
    for (size_t i = 0; i < spec.partition.size(); ++i)
        result += field(expressionIdentity(spec.partition[i], query, engine, database, select)) +
            field(comparisonIdentity(spec.partitionComparisons.at(i))) +
            field(spec.partitionAsc.at(i) ? "asc" : "desc") +
            field(spec.partitionNullsFirst.at(i) ? "first" : "last");
    result += field("order");
    for (const auto& key : spec.order)
        result += field(expressionIdentity(key.expression, query, engine, database, select)) +
            field(key.asc ? "asc" : "desc") + field(key.nullsFirst ? "first" : "last") +
            field(comparisonIdentity(key.comparison));
    return result;
}

// WindowClause identity is established from the original WindowDef before
// its expressions acquire implicit input conversions and resolved key slots.
// Semantically equal key streams can share a sort while retaining separate
// frame-offset initialization, as for explicit ASC/NULLS, BETWEEN, qualifiers,
// and explicit no-op casts. This is not the key comparison/cache identity.
inline std::string clauseSyntaxIdentity(const WindowDef& window, const PreparedQuery& query,
    StorageEngine* engine, const std::string& database) {
    const auto expression = [&](const Expr* value) {
        return ExprHelper::preparedSortExpressionIdentity(value, query,
            [](const ColumnRefExpr& column) {
                return field(column.schema) + field(column.table) + field(column.column);
            }, database, engine, true);
    };
    std::string result = field(window.referenceName.empty() ? "" : identifier(window.referenceName)) +
        field(window.frameMode) + field(window.frameBetween ? "between" : "single") +
        field(std::to_string(static_cast<unsigned>(window.frameStartKind))) +
        field(std::to_string(static_cast<unsigned>(window.frameEndKind))) +
        field(window.frameExclusion == "no others" ? "" : window.frameExclusion);
    for (const auto& key : window.partitionBy) result += field(expression(key.get()));
    result += field("order");
    for (size_t i = 0; i < window.orderBy.size(); ++i) {
        const auto& key = window.orderBy[i];
        const bool direction = i < window.orderDirectionSpecified.size() && window.orderDirectionSpecified[i];
        const auto placement = i < window.orderNullsFirst.size() ? window.orderNullsFirst[i] : std::optional<bool>{};
        result += field(expression(key.first.get())) + field(direction ? (key.second ? "asc" : "desc") : "default") +
            field(placement ? (*placement ? "first" : "last") : "default");
    }
    return result + field(expression(window.frameStart.get())) + field(expression(window.frameEnd.get()));
}
} // namespace spec_detail

inline ResolvedSpec resolve(SelectStmt& select, const WindowDef& definition) {
    std::set<const WindowDef*> visiting;
    return spec_detail::resolve(select, definition, visiting);
}

inline std::vector<Producer> collect(SelectStmt& select, const PreparedQuery& query,
    StorageEngine* engine, const std::string& database) {
    std::vector<Producer> result;
    std::set<const FunctionCallExpr*> seen;
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(database);
    std::function<void(Expr*)> visit = [&](Expr* expression) {
        if (!expression || expression->preparedSubquery) return;
        if (auto* call = dynamic_cast<FunctionCallExpr*>(expression); call && call->hasOver) {
            if (seen.insert(call).second)
                result.push_back(spec_detail::producer(*call, query, engine, database, evaluator));
            // This query owns the producer site. Its SQL children and other
            // windows have their own owners; never collect them as peers.
            return;
        }
        spec_detail::valueChildren(expression, visit);
    };
    for (auto& target : select.selectList) visit(target.expr.get());
    for (auto& key : select.orderBy) visit(key.expr.get());
    return result;
}

inline std::vector<Stage> stages(SelectStmt& select, const PreparedQuery& query,
    const std::vector<Producer>& producers, StorageEngine* engine, const std::string& database) {
    using spec_detail::field;
    std::vector<Stage> result;
    // Match PostgreSQL's original winref assignment: declared WINDOW entries
    // exist first. OVER name selects that exact declaration, even if another
    // name has an equal body; anonymous/copy syntax matches the first equal
    // raw WindowDef or appends a new one. Declaration names themselves are
    // not part of the anonymous body's equality comparison.
    std::vector<std::string> clauses;
    for (const auto& definition : select.windowDefs)
        clauses.push_back(spec_detail::clauseSyntaxIdentity(definition, query, engine, database));
    for (size_t i = 0; i < producers.size(); ++i) {
        if (!producers[i].call || !producers[i].call->hasOver)
            throw DbError("XX000", "window stage lost its original producer");
        auto spec = resolve(select, producers[i].call->over);
        spec_detail::normalize(spec, query, engine, database, select);
        const auto keys = spec_detail::keyIdentity(spec, query, engine, database, select);
        const auto& syntax = producers[i].call->over;
        size_t clause;
        if (!syntax.name.empty() && syntax.referenceName.empty()) {
            const auto name = spec_detail::identifier(syntax.name);
            const auto declared = std::find_if(select.windowDefs.begin(), select.windowDefs.end(),
                [&](const auto& definition) { return spec_detail::identifier(definition.name) == name; });
            if (declared == select.windowDefs.end()) throw DbError("XX000", "window stage lost its named declaration");
            clause = static_cast<size_t>(declared - select.windowDefs.begin());
        } else {
            const auto raw = spec_detail::clauseSyntaxIdentity(syntax, query, engine, database);
            const auto existing = std::find(clauses.begin(), clauses.end(), raw);
            clause = static_cast<size_t>(existing - clauses.begin());
            if (existing == clauses.end()) clauses.push_back(raw);
        }
        const auto identity = "window-clause" + field(std::to_string(clause));
        const auto existing = std::find_if(result.begin(), result.end(),
            [&](const auto& stage) { return stage.identity == identity; });
        if (existing != result.end()) existing->producers.push_back(i);
        else result.push_back({std::move(spec), {i}, identity, keys});
    }

    // Mirror select_active_windows' ordering: equivalent key lists stay
    // adjacent, stronger prefix lists run first, and later sort references
    // precede the final ORDER/DISTINCT references. Each producer remains an
    // original site even when its specification shares a stage.
    struct SortReference {
        size_t ordinal;
        bool asc, nullsFirst;
        std::string comparison;
        bool operator==(const SortReference& other) const {
            return ordinal == other.ordinal && asc == other.asc &&
                nullsFirst == other.nullsFirst && comparison == other.comparison;
        }
    };
    std::map<std::string, size_t> references;
    const auto reference = [&](const Expr* expression) {
        const auto identity = spec_detail::expressionIdentity(expression, query, engine, database, select);
        auto [found, inserted] = references.try_emplace(identity, references.size() + 1);
        (void)inserted;
        return found->second;
    };
    const auto outputReference = [&](size_t ordinal) {
        const auto projected = query.projectionBindings.find(&select);
        if (projected == query.projectionBindings.end() || ordinal >= projected->second.size())
            throw DbError("XX000", "window final sort lost its output producer");
        const auto& producer = projected->second[ordinal];
        if (producer.column) {
            ColumnRefExpr column;
            column.binding = producer.column;
            return reference(&column);
        }
        return reference(producer.expression);
    };
    for (const auto& key : select.orderBy) {
        const Expr* expression = key.expr.get();
        bool output = false;
        if (const auto* name = dynamic_cast<const ColumnRefExpr*>(expression);
            name && name->schema.empty() && name->table.empty() && !name->binding) {
            const auto descriptor = query.statementOutputs.find(&select);
            if (descriptor != query.statementOutputs.end())
                for (size_t i = 0; i < descriptor->second.size(); ++i)
                    if (descriptor->second[i].name == name->column) {
                        outputReference(i); output = true; break;
                    }
        } else if (const auto* ordinal = dynamic_cast<const LiteralExpr*>(expression);
            ordinal && !ordinal->preparedSubquery && ordinal->typeName.empty() &&
            !ordinal->value.empty() && std::all_of(ordinal->value.begin(), ordinal->value.end(),
                [](unsigned char ch) { return ch >= '0' && ch <= '9'; })) {
            size_t index = 0;
            const auto descriptor = query.statementOutputs.find(&select);
            for (char ch : ordinal->value) {
                if (descriptor == query.statementOutputs.end() || index > descriptor->second.size()) break;
                index = index * 10 + static_cast<size_t>(ch - '0');
            }
            if (descriptor != query.statementOutputs.end() && index && index <= descriptor->second.size()) {
                outputReference(index - 1); output = true;
            }
        }
        if (!output) reference(expression);
    }
    const auto assign = [&](const ResolvedSpec& spec) {
        // PostgreSQL transforms a window's ORDER BY before PARTITION BY.
        for (const auto& key : spec.order) reference(key.expression);
        for (const auto* key : spec.partition) reference(key);
    };
    for (const auto& definition : select.windowDefs) assign(resolve(select, definition));
    for (const auto& stage : result) assign(stage.spec);
    const auto ordering = [&](const Stage& stage) {
        std::vector<SortReference> keys;
        const auto add = [&](SortReference key) {
            if (std::find(keys.begin(), keys.end(), key) == keys.end()) keys.push_back(std::move(key));
        };
        for (size_t i = 0; i < stage.spec.partition.size(); ++i)
            add({reference(stage.spec.partition[i]), stage.spec.partitionAsc.at(i),
                stage.spec.partitionNullsFirst.at(i),
                spec_detail::comparisonIdentity(stage.spec.partitionComparisons.at(i))});
        for (const auto& key : stage.spec.order)
            add({reference(key.expression), key.asc, key.nullsFirst,
                spec_detail::comparisonIdentity(key.comparison)});
        return keys;
    };
    std::map<std::string, std::vector<SortReference>> orders;
    for (const auto& stage : result) orders.emplace(stage.identity, ordering(stage));
    std::stable_sort(result.begin(), result.end(), [&](const Stage& left, const Stage& right) {
        const auto& a = orders.at(left.identity);
        const auto& b = orders.at(right.identity);
        for (size_t i = 0; i < std::min(a.size(), b.size()); ++i) {
            if (a[i].ordinal != b[i].ordinal) return a[i].ordinal > b[i].ordinal;
            if (a[i].comparison != b[i].comparison) return a[i].comparison > b[i].comparison;
            if (a[i].asc != b[i].asc) return !a[i].asc;
            if (a[i].nullsFirst != b[i].nullsFirst) return a[i].nullsFirst;
        }
        return a.size() > b.size();
    });
    return result;
}

} // namespace dbms::prepared_window_detail
