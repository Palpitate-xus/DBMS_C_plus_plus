#include "catalog/SystemCatalogRows.h"
#include "catalog/PgDefaultCatalog.h"
#include "ExecutionPlan.h"
#include "parser/BetweenSyntax.h"
#include "expression/StoredRegtypeValue.h"
#include "expression/ArrayDatumInput.h"
#include "executor/PreparedTableSource.h"
#include "executor/CompositeKeyLookup.h"
#include "executor/EqualityStatistics.h"
#include "executor/AnalyzedSelectivity.h"
#include "executor/LikePrefix.h"
#include "executor/SortSpill.h"
#include "executor/BoundedSortRuns.h"
#include "executor/TypedSortSpill.h"
#include "executor/PreparedWindowSpec.h"
#include "executor/WindowKernel.h"
#include "executor/WindowAggregateState.h"
#include "commands/StoredRowNull.h"
#include "commands/HeapScanCursor.h"
#include "network/SimpleQueryResultScope.h"
#include "common/DbError.h"
#include "common/TemporalStorageCodec.h"
#include "commands/StoredZonedClockPredicate.h"
#include "access/BPTree.h"
#include "access/PrimaryKeyIndex.h"
#include "access/HashIndex.h"
#include "access/BloomIndex.h"
#include "catalog/type_registry.h"
#include "catalog/declared_type.h"
#include "catalog/catalog.h"
#include "catalog/collation.h"
#include "catalog/CatalogService.h"
#include "catalog/type_catalog.h"
#include "catalog/TypeCatalogScan.h"
#include "catalog/namespace_catalog.h"
#include "catalog/systables.h"
#include "Config.h"
#include "process/RuntimeStats.h"
#include "utils/permissions.h"
#include "types/numeric.h"
#include "types/encoding_conversion.h"
#include "expression/expr_helper.h"
#include "expression/ExprEvaluator.h"
#include "expression/prepared_query_execution.h"
#include "expression/aggregate_type.h"
#include "parser/parser.h"
#include "Session.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <iterator>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <thread>
#include <typeinfo>
#include <unordered_set>

extern dbms::Config g_config;

namespace dbms {

// Parallel query support
int QueryPlanner::parallelWorkers_ = 0;
QueryPlanner::CostModel QueryPlanner::costModel_{};

static size_t sortWorkMemBytes();

static std::string trimExec(const std::string& value) {
    const size_t first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    const size_t last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

// Sorting may spend substantial time after its child has been drained.
// Poll at both algorithm boundaries and every 128 comparisons, including
// worker-local sorts. Copies of the comparator share this invocation's
// counter; SQL comparison, NULL/tie semantics and key evaluation stay intact.
template <typename Iterator, typename Compare>
static void interruptibleSort(Iterator first, Iterator last, Compare compare) {
    checkForQueryInterrupt();
    size_t comparisons = 0;
    std::sort(first, last, [&](const auto& left, const auto& right) {
        if ((comparisons++ & 127U) == 0) checkForQueryInterrupt();
        return compare(left, right);
    });
    checkForQueryInterrupt();
}

template <typename Iterator, typename Compare>
static void interruptibleStableSort(Iterator first, Iterator last, Compare compare) {
    checkForQueryInterrupt();
    size_t comparisons = 0;
    std::stable_sort(first, last, [&](const auto& left, const auto& right) {
        if ((comparisons++ & 127U) == 0) checkForQueryInterrupt();
        return compare(left, right);
    });
    checkForQueryInterrupt();
}

// ========================================================================
// Helper: format a raw row buffer into display string
// ========================================================================
static bool rawColumnIsNull(const std::string& row, const TableSchema& tbl,
                            size_t colIdx) {
    // TableScanOp deliberately strips the MVCC/null bitmap header before
    // exposing rows.  The payload format uses the same empty-value sentinel
    // as extractColumnValueStatic for nullable fixed-width values.
    return colIdx < tbl.len() && tbl.cols[colIdx].isNull &&
           StorageEngine::extractColumnValueStatic(row, tbl, colIdx).empty();
}

// ========================================================================
// TableScanOp
// ========================================================================

bool MaterializedRowsOp::open() {
    pos_ = 0;
    return true;
}

bool MaterializedRowsOp::next(std::string& outRow) {
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_++];
    return true;
}

void MaterializedRowsOp::close() {
    pos_ = 0;
}

bool PreparedSourceRowsOp::open() { clearError(); position_ = 0; row_.clear(); return true; }
bool PreparedSourceRowsOp::next(std::string& text) {
    NextInstrument instrumentation(this);
    if (!reader_(position_, row_)) { row_.clear(); return false; }
    if (row_.size() != descriptor_.size()) throw DbError("XX000", "logical source row width differs from descriptor");
    text.clear();
    for (size_t i = 0; i < row_.size(); ++i) {
        if (ExprHelper::canonicalResultTypeName(row_[i].typeName) != ExprHelper::canonicalResultTypeName(descriptor_[i].type))
            throw DbError("XX000", "logical source cell type differs from descriptor");
        text += row_[i].isNull ? "NULL " : row_[i].value + " ";
    }
    ++position_; instrumentation.emitted = true; return true;
}
void PreparedSourceRowsOp::close() { row_.clear(); position_ = 0; }
bool PreparedSourceRowsOp::lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const {
    if (!position_ || row_.size() != descriptor_.size()) return false;
    cells.clear(); nulls.clear();
    for (const auto& value : row_) { cells.push_back(value.value); nulls.push_back(value.isNull); }
    return true;
}
bool PreparedSourceRowsOp::lastColumnIsNull(size_t ordinal) const { return ordinal < row_.size() && row_[ordinal].isNull; }

namespace {
bool hasColumnCollation(const Column& column) {
    return !column.isArray && !column.collation.empty();
}

ExpressionCollation declaredColumnCollation(const Column& column) {
    if (!hasColumnCollation(column)) return {};
    std::string identity = column.collation;
    const auto normalized = collation::normalizeName(identity);
    if (normalized == "default" || normalized == "pg_catalog.default") return {};
    if (collation::isValid(identity)) {
        const auto builtins = collation::listBuiltins();
        if (identity.size() > 1 && identity.front() == '"' && identity.back() == '"')
            identity = identity.substr(1, identity.size() - 2);
        const auto lower = SQLParser::toLower(identity);
        const auto builtin = std::find_if(builtins.begin(), builtins.end(), [&](const auto& name) {
            return SQLParser::toLower(name) == lower;
        });
        identity = "pg_catalog." + (builtin == builtins.end()
            ? normalized : *builtin);
    } else if (identity.find('.') == std::string::npos) {
        identity = "public." + identity;
    }
    return {std::move(identity), column.resolvedCollation.empty() ? column.collation
        : column.resolvedCollation, column.resolvedCollationUsesLocale};
}

// VALUES owns the same per-invocation carrier lifetime as SELECT/Append.
// Keep its actual typed source graph, but never retain an initplan or open
// quantified child across a root cursor close/restart.
class PreparedValuesExecutionOp final : public Operator {
    OpPtr child_;
    std::function<void()> begin_, finish_;
    bool opened_ = false;
public:
    PreparedValuesExecutionOp(OpPtr child, std::function<void()> begin, std::function<void()> finish)
        : child_(std::move(child)), begin_(std::move(begin)), finish_(std::move(finish)) {}
    bool open() override {
        OpenInstrument instrument(this); clearError();
        begin_(); opened_ = true;
        return child_->open();
    }
    bool next(std::string& row) override {
        NextInstrument instrument(this); const bool present = child_->next(row);
        instrument.emitted = present; return present;
    }
    void close() override {
        if (!opened_) return;
        opened_ = false; std::exception_ptr failure;
        try { finish_(); } catch (...) { failure = std::current_exception(); }
        try { child_->close(); } catch (...) { if (!failure) failure = std::current_exception(); }
        if (failure) std::rethrow_exception(failure);
    }
    bool supportsStructuredRows() const override { return child_->supportsStructuredRows(); }
    bool lastStructuredValues(std::vector<ExprValue>& row) const override { return child_->lastStructuredValues(row); }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override { return child_->lastStructuredRow(cells,nulls); }
    bool lastColumnIsNull(size_t ordinal) const override { return child_->lastColumnIsNull(ordinal); }
    bool supportsPreparedOuterRow() const override { return child_->supportsPreparedOuterRow(); }
    bool bindPreparedOuterRow(const RowContext& row) override { close(); return child_->bindPreparedOuterRow(row); }
    std::string preparedPlanNodeName() const override { return "ValuesScan"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};


static void prepareNullWhereBranches(PreparedQueryExecution& execution,Expr* expression);

// A prepared plan owns its AST and the evaluator's metadata-only routine
// bindings. Columns consume the binder's true source occurrence/ordinal,
// and the shared execution carrier owns parameter and lazy-child evaluation.
struct PreparedSelectState {
    struct Aggregate {
        FunctionCallExpr* call;
        std::string name, argumentType, resultType;
        QueryColumnBinding column;
        // noTarget means a new producer; noTarget-1 reuses the argument;
        // another index reuses an earlier analyzed equivalent ORDER key.
        std::vector<size_t> keyOwners;
    };
    std::vector<Aggregate> aggregates;
    std::vector<prepared_window_detail::Producer> windows;
    std::vector<prepared_window_detail::Stage> windowStages;
    std::vector<size_t> windowBaseOwners,windowSortedOwners,windowBaseOrder,windowSortedOrder;
    bool windowResultSortBarrier=false;
    struct WindowInput {
        Expr* expression=nullptr;
        std::string identity;
        std::vector<Expr*> targets;
        std::vector<QueryColumnBinding> columns;
    };
    std::vector<WindowInput> windowInputs;
    std::vector<size_t> windowOrderIndexes;
    std::function<std::vector<ExprValue>()> windowOrderKeys;
    std::string windowInputIdentity(const Expr* expression) const {
        return prepared_window_detail::spec_detail::expressionIdentity(expression,*query,engine,dbname,*statement);
    }
    bool windowInputDemanded(const WindowInput& input) const {
        for(const auto* target:input.targets)if(execution->windowInputColumnDemanded(target))return true;
        for(const auto& stage:windowStages) {
            if(!std::any_of(stage.producers.begin(),stage.producers.end(),[&](size_t index) {
                return execution->aggregateColumnDemanded(windows.at(index).call);
            }))continue;
            for(const auto* key:stage.spec.partition)if(windowInputIdentity(key)==input.identity)return true;
            for(const auto& key:stage.spec.order)if(windowInputIdentity(key.expression)==input.identity)return true;
        }
        return false;
    }
    size_t windowInputIndex(const Expr* expression) const {
        const auto value=windowInputIdentity(expression);
        for(size_t i=0;i<windowInputs.size();++i)if(windowInputs[i].identity==value)return i;
        throw DbError("XX000","window key lost its actual input projection");
    }
    void prepareWindowInputs() {
        if(windowStages.empty())return;
        std::map<std::string,Expr*> expressions;
        for(const auto& stage:windowStages) {
            for(const auto& key:stage.spec.order)expressions.try_emplace(windowInputIdentity(key.expression),key.expression);
            for(auto* key:stage.spec.partition)expressions.try_emplace(windowInputIdentity(key),key);
        }
        const auto add=[&](Expr* expression) {
            const auto id=windowInputIdentity(expression);
            const auto found=expressions.find(id);
            if(found==expressions.end())return;
            if(std::none_of(windowInputs.begin(),windowInputs.end(),[&](const auto& item){return item.identity==id;}))
                windowInputs.push_back({found->second,id,{},{}});
        };
        // Existing SELECT target entries precede the resjunk keys added by
        // window transformation. Only actual key expressions are projected.
        for(auto& target:select().selectList) {
            const auto* column=dynamic_cast<const ColumnRefExpr*>(target.expr.get());
            const auto* literal=dynamic_cast<const LiteralExpr*>(target.expr.get());
            if(!target.expr || target.expr->type==ExprType::A_Star ||
                (column && column->column=="*") || (literal && literal->value=="*"))continue;
            add(target.expr.get());
        }
        for(const auto& key:keys)add(key.target==noTarget?key.expression:targets.at(key.target));
        const auto addSpec=[&](const prepared_window_detail::ResolvedSpec& spec) {
            for(const auto& key:spec.order)add(key.expression);
            for(auto* key:spec.partition)add(key);
        };
        for(const auto& definition:select().windowDefs)addSpec(prepared_window_detail::resolve(select(),definition));
        for(const auto& producer:windows)addSpec(prepared_window_detail::resolve(select(),producer.call->over));
        std::function<void(Expr*)> reuse=[&](Expr* expression) {
            if(!expression || expression->preparedSubquery)return;
            // Aggregate inputs run below WindowInput and keep their existing
            // producer. Upper scalar subexpressions may reference real keys.
            if(auto* call=dynamic_cast<FunctionCallExpr*>(expression);
                call && !call->hasOver && !aggregate_type_detail::builtinName(call).empty())return;
            if(!dynamic_cast<ColumnRefExpr*>(expression) && !dynamic_cast<LiteralExpr*>(expression) &&
                !dynamic_cast<ParameterExpr*>(expression)) {
                const auto id=windowInputIdentity(expression);
                for(auto& input:windowInputs)if(input.identity==id) {
                    if(std::find(input.targets.begin(),input.targets.end(),expression)==input.targets.end())
                        input.targets.push_back(expression);
                    return;
                }
            }
            prepared_window_detail::spec_detail::valueChildren(expression,reuse);
        };
        for(auto& target:select().selectList)reuse(target.expr.get());
        for(auto& key:select().orderBy)reuse(key.expr.get());
    }
    StorageEngine* engine;
    std::string dbname;
    TableSchema schema;
    std::shared_ptr<PreparedQuery> query;
    SelectStmt* statement = nullptr;
    QueryRowDescriptor output;
    RowContext outerRow;
    PreparedChildExecutor childExecutor;
    PreparedChildCursorFactory childCursorFactory;
    bool cursorOwnsScalarChildren = true;
    bool planRootConstants = false;
    std::shared_ptr<PreparedQueryExecution> execution;
    QueryPlanner::PreparedExecutionProvider executionProvider;
    bool executionStarted = false;
    ExprEvaluator evaluator;
    std::vector<Expr*> targets;
    std::vector<ExprPtr> starTargets;
    std::vector<bool> targetsBeforeSort;
    std::vector<size_t> targetOwners;
    struct SortKey { Expr* expression; size_t target, priorKey; bool asc, nullsFirst; std::optional<QueryComparisonBinding> enumComparison; };
    std::vector<SortKey> keys;
    static constexpr size_t noTarget = std::numeric_limits<size_t>::max();
    size_t sourceOrdinal = noTarget;
    bool sourceContexts = false;
    std::vector<bool> projectedPhysicalColumns;
    std::vector<size_t> functionProjectionColumns;
    Operator* logicalSource = nullptr; // Owned by the actual operator graph.
    std::vector<const PreparedQuery::SourceRange*> visibleRanges;
    SelectStmt& select() const { return *statement; }
    size_t columnOrdinal(const ColumnRefExpr& column) const {
        if (!column.binding) throw DbError("XX000", "prepared source reference has no binding");
        const auto& binding = *column.binding;
        if (binding.scopeDepth || binding.sourceOrdinal != sourceOrdinal || binding.mergedUsing)
            throw DbError("0A000", "prepared source reference requires additional range lowering");
        if (binding.columnOrdinal >= schema.len())
            throw DbError("XX000", "prepared source ordinal is outside its descriptor");
        return binding.columnOrdinal;
    }
    std::string identity(const Expr* expression) const {
        return ExprHelper::preparedSortExpressionIdentity(expression, *query, [&](const ColumnRefExpr& column) {
            if (column.binding && column.binding->scopeDepth) {
                const auto& binding = *column.binding;
                return std::to_string(binding.sourceOrdinal) + ":" + std::to_string(binding.columnOrdinal) + ":" + binding.declaredType;
            }
            if (sourceContexts) {
                if (!column.binding) throw DbError("XX000", "prepared expression has no source identity");
                const auto& binding = *column.binding;
                for (const auto& range : query->sourceRanges)
                    if (range.owner == statement && range.ordinal == binding.sourceOrdinal &&
                        binding.columnOrdinal < range.columns.size())
                        return std::to_string(binding.sourceOrdinal) + ":" +
                            std::to_string(binding.columnOrdinal) + ":" + binding.declaredType;
                throw DbError("XX000", "prepared expression references an absent source occurrence");
            }
            const auto ordinal = columnOrdinal(column);
            const auto& physical = schema.cols[ordinal];
            return std::to_string(sourceOrdinal) + ":" + std::to_string(ordinal) + ":" +
                column.binding->declaredType + ":" + std::to_string(physical.dsize) + ":" + physical.collation;
        }, dbname, engine, false, windows.empty()?nullptr:statement);
    }
    std::vector<ExprValue> cells(Operator& source, const std::string& raw,
                                 const std::vector<bool>* storedNulls = nullptr) const {
        std::vector<ExprValue> values;
        if (!storedNulls && source.lastStructuredValues(values)) {
            if (values.size() != schema.len())
                throw DbError("XX000","prepared source lost its typed descriptor width");
            for (size_t i = 0; i < values.size(); ++i) {
                const auto declared = ExprHelper::canonicalResultTypeName(schema.cols[i].dataType +
                    (schema.cols[i].isArray ? "[]" : ""));
                if (ExprHelper::canonicalResultTypeName(values[i].typeName) != declared)
                    throw DbError("XX000","prepared source changed its declared cell type");
            }
            // Record fields/OID identities are part of the actual datum,
            // never reconstructed from its formatted text representation.
            return values;
        }
        std::vector<std::string> structured;
        std::vector<bool> nulls;
        bool available = storedNulls != nullptr;
        if (storedNulls) {
            detail::decodeStoredRowInputs(*engine, dbname, raw, schema,
                                         *storedNulls, structured, nulls, projectedPhysicalColumns);
        } else {
            available = source.supportsStructuredRows() && source.lastStructuredRow(structured, nulls);
        }
        if (!available) {
            const auto origin = source.scanOrigin();
            if (origin.engine == engine && origin.dbname == dbname &&
                origin.tablename == schema.tablename) {
                std::vector<bool> captured;
                if (source.lastStoredNullBitmap(captured)) {
                    detail::decodeStoredRowInputs(*engine, dbname, raw, schema,
                                                 captured, structured, nulls, projectedPhysicalColumns);
                    available = true;
                } else if (const auto bitmap = detail::storedRowNullBitmap(
                               *engine, dbname, origin.tablename, schema, origin.rid)) {
                    detail::decodeStoredRowInputs(*engine, dbname, raw, schema,
                                                 *bitmap, structured, nulls, projectedPhysicalColumns);
                    available = true;
                }
            }
        }
        const QueryRowDescriptor* physicalDeclaration=nullptr;
        bool needsPhysicalDatum=false;
        for(size_t i=0;i<schema.len();++i)
            if((projectedPhysicalColumns.empty() || projectedPhysicalColumns[i]) &&
               schema.cols[i].generatedKind!='v' &&
               (schema.cols[i].isArray || stored_regtype_detail::columnType(schema.cols[i].dataType))) {
                needsPhysicalDatum=true;break;
            }
        // Only the actual array/reference loaders below consume provenance.
        // Primitive rows retain the original NULL/decode source path above.
        if(needsPhysicalDatum) {
            const auto physicalOrigin=source.scanOrigin();
            if(physicalOrigin.engine==engine && physicalOrigin.dbname==dbname &&
               physicalOrigin.tablename==schema.tablename && sourceOrdinal!=noTarget) {
                for(const auto& range:query->sourceRanges)
                    if(range.owner==statement && range.ordinal==sourceOrdinal) {
                        physicalDeclaration=&range.columns;break;
                    }
            }
        }
        // Reserve only after the original source supplied a complete row.
        // Unavailable/malformed fallback keeps its original allocation/errors.
        if(!needsPhysicalDatum && schema.len()<=MAX_COLUMNS && values.empty() &&
           available && structured.size()==schema.len() && nulls.size()==schema.len())
            values.reserve(schema.len());
        for (size_t i = 0; i < schema.len(); ++i) {
            if(schema.cols[i].generatedKind=='v'){values.emplace_back();continue;}
            const auto& column = schema.cols[i];
            if (!projectedPhysicalColumns.empty() && !projectedPhysicalColumns[i]) {
                values.emplace_back(ExprHelper::canonicalResultTypeName(column.dataType +
                    (column.isArray ? "[]" : "")), "", true);
                continue;
            }
            bool computedNull = false;
            std::string value = available ? std::move(structured.at(i)) :
                engine->extractColumnValue(raw, schema, i, dbname, true, &computedNull);
            const bool isNull = available ? nulls.at(i) : computedNull ||
                (schema.cols[i].generatedKind != 'v' && source.lastColumnIsNull(i));
            values.emplace_back(ExprHelper::canonicalResultTypeName(schema.cols[i].dataType +
                (schema.cols[i].isArray ? "[]" : "")), std::move(value), isNull);
            if (physicalDeclaration && i < physicalDeclaration->size()) {
                const auto& declaredField = physicalDeclaration->at(i).typeMetadata;
                // The physical scan, persisted domain column and actual
                // attribute jointly certify the already typed stored datum.
                uint32_t validatedArrayDomainOid = 0;
                if (column.isArray && !column.domainName.empty() && declaredField &&
                    declaredField->kind == DatumKind::Array &&
                    declaredField->actualOid != declaredField->codecOid)
                    validatedArrayDomainOid = declaredField->actualOid;
                if (stored_regtype_detail::columnType(column.dataType)) {
                    const auto catalog = regtype_detail::metadata(*engine, dbname);
                    values.back() = stored_regtype_detail::load(values.back().value, column.isArray, isNull,
                        catalog, currentSession(), declaredField, column.dataType, validatedArrayDomainOid);
                } else if (column.isArray &&
                    !compound_value_detail::decodeArrayStorageInput(values.back(), declaredField,
                        {}, validatedArrayDomainOid) && validatedArrayDomainOid) {
                    throw DbError("XX001", "could not decode stored array domain value");
                }
            }
            values.back().collation = column.collation;
        }
        return values;
    }
    RowContext context(std::vector<ExprValue> values) const {
        auto row = execution->context(outerRow);
        if (sourceOrdinal != noTarget) execution->setSourceInputs(row, sourceOrdinal, std::move(values), true);
        return row;
    }
    RowContext context(Operator& source, const std::string& raw) const {
        RowContext row;
        if (source.supportsPreparedContexts() && source.lastPreparedContext(row)) {
            return execution->context(row);
        }
        if (sourceContexts) throw DbError("XX000", "logical source lost its bound row context");
        return context(cells(source, raw));
    }
    ExprValue evaluate(const Expr* expression, const RowContext& row) const {
        ExprValue value = execution->evaluate(expression, row);
        if (value.isUnknown()) throw DbError("0A000", "unsupported prepared expression value");
        return value;
    }
    void prepareExecution() {
        auto provided = executionProvider ? executionProvider() : nullptr;
        if (execution && (!executionProvider || execution == provided)) return;
        if (executionProvider && !provided) throw DbError("XX000","prepared branch lost its execution owner");
        execution = provided ? std::move(provided) : std::make_shared<PreparedQueryExecution>(query, engine, dbname);
        if (!executionProvider) {
            execution->setQueryExecutor(childExecutor);
            execution->setChildCursorFactory(childCursorFactory, cursorOwnsScalarChildren);
        }
        for (auto& aggregate : aggregates) {
            aggregate.column = execution->registerAggregateColumn(aggregate.call, aggregate.resultType);
            const auto* literal = dynamic_cast<const LiteralExpr*>(aggregate.call->args.front().get());
            if (!literal || literal->value != "*")
                execution->prepareExpression(aggregate.call->args.front().get());
            execution->prepareExpression(aggregate.call->filter.get());
            for (const auto& key : aggregate.call->aggregateOrderKeys)
                execution->prepareExpression(key.expr.get());
        }
        // Window values use the existing execution-owned computed-column
        // registry. Their original AST sites and statement inputs stay intact.
        for (auto& window : windows)
            window.column=execution->registerAggregateColumn(window.call,window.resultType);
        for(auto& input:windowInputs) {
            input.columns.clear();
            const auto type=ExprHelper::inferParsedResultType(input.expression,{},dbname,engine);
            for(auto* target:input.targets)
                input.columns.push_back(execution->registerWindowInputColumn(target,type,true));
        }
        if (!executionProvider && !sourceContexts && sourceOrdinal != noTarget &&
            QueryPlanner::usesDeclaredFilterCollations(select(), schema)) {
            std::vector<ExpressionCollation> declarations;
            declarations.reserve(schema.len());
            for (const auto& column : schema.cols)
                declarations.push_back(declaredColumnCollation(column));
            execution->setPredicateCollations(select().whereClause.get(),
                [ordinal = sourceOrdinal, declarations = std::move(declarations)](const ColumnRefExpr& column) {
                    if (!column.binding || column.binding->scopeDepth ||
                        column.binding->sourceOrdinal != ordinal) return ExpressionCollation{};
                    return declarations.at(column.binding->columnOrdinal);
                });
        }
        // Only the execution root opts into whole-query planning. Child
        // graph construction must not reacquire outputs that root demand
        // already pruned, or plan a child in a discarded CASE arm.
        if (planRootConstants && !executionProvider) execution->planStatementConstants(statement);
        if (!select().existsTargetPrunable) for (auto& target : starTargets)
            execution->prepareProjectionColumn(static_cast<ColumnRefExpr*>(target.get()), statement);
        if (!select().existsTargetPrunable)
            for (size_t i=0;i<targets.size();++i)
                if(windows.empty() || targetOwners.at(i)==i)execution->prepareExpression(targets[i]);
        execution->prepareExpression(select().whereClause.get());
        prepareNullWhereBranches(*execution,select().whereClause.get());
        execution->prepareExpression(select().having.get());
        for (const auto& key : keys)
            if (key.target == noTarget) execution->prepareExpression(key.expression);
        if (planRootConstants && !executionProvider && statement == query->ast.get() &&
            dbms::SimpleQueryResultScope::active()) {
            for (const auto& aggregate : aggregates) {
                const auto* argument = aggregate.call->args.front().get();
                const auto* star = dynamic_cast<const LiteralExpr*>(argument);
                const bool planArgument = (!star || star->value != "*") &&
                    !dynamic_cast<const ColumnRefExpr*>(argument);
                if (!aggregate.call->aggregateOrderKeys.empty() ||
                    (!planArgument && !aggregate.call->filter)) continue;
                if (!execution->aggregateColumnDemanded(aggregate.call)) continue;
                // The root expression now reads a result slot. Its separate
                // aggregate inputs must finish planning before delivery starts.
                if (planArgument) execution->planExpressionConstants(aggregate.call->args.front().get());
                execution->planExpressionConstants(aggregate.call->filter.get());
            }
        }
        for(const auto& stage:windowStages) {
            const bool demanded=std::any_of(stage.producers.begin(),stage.producers.end(),[&](size_t index) {
                return execution->aggregateColumnDemanded(windows.at(index).call);
            });
            if(!demanded)continue;
            for(const auto* key:stage.spec.partition)execution->prepareExpression(const_cast<Expr*>(key));
            for(const auto& key:stage.spec.order)execution->prepareExpression(key.expression);
            execution->prepareExpression(stage.spec.startOffset);
            execution->prepareExpression(stage.spec.endOffset);
            for(size_t index:stage.producers) {
                const auto& window=windows.at(index);
                if(!execution->aggregateColumnDemanded(window.call))continue;
                for(const auto& argument:window.call->args) {
                    const auto* star=dynamic_cast<const LiteralExpr*>(argument.get());
                    if(!star || star->value!="*")execution->prepareExpression(argument.get());
                }
                execution->prepareExpression(window.call->filter.get());
            }
        }
        for(const auto& input:windowInputs)if(windowInputDemanded(input))
            execution->prepareExpression(input.expression);
        execution->prepareChildCursors();
    }
    void beginExecution() {
        if (executionStarted && !executionProvider) execution.reset();
        prepareExecution(); executionStarted = true;
    }
    bool parameterized() const {
        // Updating this state alone cannot rebind captured provider cells.
        // A logical graph advertises restart only through its real factory.
        if (sourceContexts) return logicalSource && logicalSource->supportsPreparedOuterRow();
        if (!select().ctes.empty()) return false;
        if (!select().fromClause) return true;
        if (select().fromClause->type != FromItem::Type::Table) return false;
        for (const auto& range:query->sourceRanges)
            if (range.owner==statement && range.source==select().fromClause.get())
                return !range.cteStatement;
        return false;
    }
    bool containsVolatile(const Expr* expression) const {
        if (!expression) return false;
        if (expression->preparedSubquery) return true;
        if (const auto* quantified=dynamic_cast<const QuantifiedComparisonExpr*>(expression))
            return containsVolatile(quantified->left.get()) || containsVolatile(quantified->right.get());
        if (auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
            if(std::any_of(windows.begin(),windows.end(),[&](const auto& window){return window.call==call;}))return false;
            if (std::any_of(aggregates.begin(), aggregates.end(),
                    [&](const auto& aggregate) { return aggregate.call == call; })) return false;
            if (evaluator.scalarFunctionVolatility(call, engine) == 'v') return true;
            for (const auto& argument : call->args) if (containsVolatile(argument.get())) return true;
            for (const auto& argument : call->namedArgs) if (containsVolatile(argument.value.get())) return true;
        } else if (auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) return containsVolatile(unary->operand.get());
        else if (auto* binary = dynamic_cast<const BinaryOpExpr*>(expression))
            return containsVolatile(binary->left.get()) ||
                ((binary->op != "::" && binary->op != "COLLATE") && containsVolatile(binary->right.get()));
        else if (auto* cast = dynamic_cast<const CastExpr*>(expression)) return containsVolatile(cast->operand.get());
        else if (auto* conditional = dynamic_cast<const CaseExpr*>(expression)) {
            if (containsVolatile(conditional->switchExpr.get()) || containsVolatile(conditional->elseExpr.get())) return true;
            for (const auto& arm : conditional->whenClauses)
                if (containsVolatile(arm.first.get()) || containsVolatile(arm.second.get())) return true;
        } else if (auto* array = dynamic_cast<const ArrayExpr*>(expression)) {
            for (const auto& value : array->elements) if (containsVolatile(value.get())) return true;
        } else if (auto* row = dynamic_cast<const RowExpr*>(expression)) {
            for (const auto& value : row->elements) if (containsVolatile(value.get())) return true;
        }
        return false;
    }
    static bool rowIndependent(const Expr* expression) {
        if (!expression || expression->preparedSubquery) return !expression;
        if (dynamic_cast<const LiteralExpr*>(expression)) return true;
        if (const auto* parameter = dynamic_cast<const ParameterExpr*>(expression))
            return parameter->origin == ParameterOrigin::StatementInput;
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) return rowIndependent(unary->operand.get());
        if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) return rowIndependent(cast->operand.get());
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression))
            return rowIndependent(binary->left.get()) &&
                ((binary->op == "::" || binary->op == "COLLATE") || rowIndependent(binary->right.get()));
        if (const auto* conditional = dynamic_cast<const CaseExpr*>(expression)) {
            if (!rowIndependent(conditional->switchExpr.get()) || !rowIndependent(conditional->elseExpr.get())) return false;
            for (const auto& arm : conditional->whenClauses)
                if (!rowIndependent(arm.first.get()) || !rowIndependent(arm.second.get())) return false;
            return true;
        }
        // A routine, source column or lazy query child is not a planning
        // constant, even when its text or current datum happens to be NULL.
        return false;
    }
};

// A builtin COALESCE always evaluates its first argument. Do not infer
// unconditional demand from later arguments, arbitrary routines or operators.
// Resolve its actual scalar identity just as the existing constant planner
// does, without evaluating or rewriting any expression.
static bool projectionAlwaysDemandsWindow(const Expr* expression,
    const FunctionCallExpr* window, PreparedSelectState& state) {
    if(expression==window)return true;
    if(!expression || expression->preparedSubquery)return false;
    const auto* call=dynamic_cast<const FunctionCallExpr*>(expression);
    if(!call || !call->schema.empty() || SQLParser::toLower(call->funcName)!="coalesce" ||
        call->args.empty() || !call->namedArgs.empty() || call->hasOver || call->filter ||
        call->distinct || !call->orderBy.empty() || !call->aggregateOrderKeys.empty() ||
        call->setReturning || !state.evaluator.hasScalarFunction(call,state.engine))return false;
    FunctionCallExpr builtin;builtin.funcName="coalesce";
    for(size_t i=0;i<call->args.size();++i) {
        auto null=std::make_unique<LiteralExpr>();null->value="NULL";
        builtin.args.push_back(std::move(null));
    }
    if(state.evaluator.scalarFunctionIdentity(call,state.engine)!=
       state.evaluator.scalarFunctionIdentity(&builtin,state.engine))return false;
    return projectionAlwaysDemandsWindow(call->args.front().get(),window,state);
}

// Prove the final ORDER from actual window pathkeys. An unconditionally
// projected producer guarantees sorting for every invocation of this graph;
// every later potential sorting stage must retain the same ORDER prefix.
static bool prepareWindowOutputOrder(PreparedSelectState& state) {
    if(state.windowStages.empty() || state.keys.empty() || state.select().existsTargetPrunable)return false;
    struct Key {std::string expression,comparison;bool asc,nullsFirst;};
    const auto append=[](std::vector<Key>& keys,Key key) {
        if(std::none_of(keys.begin(),keys.end(),[&](const auto& old) {
            return old.expression==key.expression && old.comparison==key.comparison;
        }))keys.push_back(std::move(key));
    };
    const auto input=[&](const PreparedSelectState::SortKey& key) {
        return key.target==PreparedSelectState::noTarget?key.expression:
            state.targets.at(state.targetOwners.at(key.target));
    };
    std::vector<Key> wanted;
    for(const auto& key:state.keys)append(wanted,{state.windowInputIdentity(input(key)),
        prepared_window_detail::spec_detail::comparisonIdentity(key.enumComparison),key.asc,key.nullsFirst});
    size_t guaranteed=PreparedSelectState::noTarget;
    for(size_t i=state.windowStages.size();i--;) {
        const auto& stage=state.windowStages[i];
        if(stage.spec.partition.empty() && stage.spec.order.empty())continue;
        for(size_t producer:stage.producers)for(size_t target=0;target<state.targets.size();++target)
            if(state.targetOwners.at(target)==target &&
                projectionAlwaysDemandsWindow(state.targets[target],state.windows.at(producer).call,state))
                guaranteed=i;
        if(guaranteed!=PreparedSelectState::noTarget)break;
    }
    if(guaranteed==PreparedSelectState::noTarget)return false;
    for(size_t i=guaranteed;i<state.windowStages.size();++i) {
        const auto& spec=state.windowStages[i].spec;
        if(spec.partition.empty() && spec.order.empty())continue; // preserves its input order
        std::vector<Key> supplied;
        for(size_t j=0;j<spec.partition.size();++j)append(supplied,{state.windowInputIdentity(spec.partition[j]),
            prepared_window_detail::spec_detail::comparisonIdentity(spec.partitionComparisons.at(j)),
            spec.partitionAsc.at(j),spec.partitionNullsFirst.at(j)});
        for(const auto& key:spec.order)append(supplied,{state.windowInputIdentity(key.expression),
            prepared_window_detail::spec_detail::comparisonIdentity(key.comparison),key.asc,key.nullsFirst});
        if(wanted.size()>supplied.size())return false;
        for(size_t j=0;j<wanted.size();++j) {
            const auto& a=wanted[j];const auto& b=supplied[j];
            if(a.expression!=b.expression || a.comparison!=b.comparison || a.asc!=b.asc || a.nullsFirst!=b.nullsFirst)return false;
        }
    }
    for(const auto& key:state.keys)state.windowOrderIndexes.push_back(state.windowInputIndex(input(key)));
    return true;
}

static void prepareWindowResultSharing(PreparedSelectState&,bool actualFinalSort);
static OpPtr makePreparedWindowStages(OpPtr source,const std::shared_ptr<PreparedSelectState>& state);
static OpPtr makePreparedWindowTiesLimit(OpPtr source,const std::shared_ptr<PreparedSelectState>& state,size_t limit);

class PreparedResultOp final : public Operator {
    bool emitted_ = false;
public:
    bool open() override { clearError(); emitted_ = false; return true; }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        if (emitted_) return false;
        emitted_ = true; row.clear(); instrumentation.emitted = true; return true;
    }
    void close() override { emitted_ = false; }
    std::string preparedPlanNodeName() const override { return "Result"; }
};

// A prepared operator supplies strictness and retained SQL datum identity.
// Do not infer constants from row cells, parameter values or routine output.
static bool preparedLiteralNull(const Expr* expression) {
    if(!expression || expression->preparedSubquery) return false;
    if(const auto* literal=dynamic_cast<const LiteralExpr*>(expression))
        return SQLParser::toLower(literal->value)=="null";
    const auto* cast=dynamic_cast<const CastExpr*>(expression);
    return cast && cast->implicit && cast->typeMods.empty() && preparedLiteralNull(cast->operand.get());
}

static bool preparedStrictNullComparison(const BinaryOpExpr& binary) {
    const bool leftNull=preparedLiteralNull(binary.left.get()),rightNull=preparedLiteralNull(binary.right.get());
    if(!leftNull && !rightNull) return false;
    if(binary.comparison) return binary.comparison->strict;
    // The existing binder retains enum implementations, but not all builtin
    // comparison objects. Resolve that existing pure implementation only from
    // a real bound source descriptor, never a guessed name or evaluated cell.
    const Expr* source=leftNull?binary.right.get():binary.left.get();
    while(const auto* cast=dynamic_cast<const CastExpr*>(source)) {
        if(!cast->implicit || !cast->typeMods.empty()) return false;
        source=cast->operand.get();
    }
    const auto* column=dynamic_cast<const ColumnRefExpr*>(source);
    if(!column || !column->binding) return false;
    try {
        const auto& type=column->binding->declaredType;
        return ExprEvaluator::resolveComparison(binary.op,leftNull?"unknown":type,leftNull?type:"unknown").strict;
    } catch(const DbError&) {
        // A non-builtin/domain implementation without retained strict metadata
        // stays on its original prepared evaluator and admission path.
        return false;
    }
}

static unsigned preparedWhereTruths(const Expr* expression) {
    // Bit 0 TRUE, bit 1 FALSE, bit 2 UNKNOWN: all runtime possibilities.
    if (preparedLiteralNull(expression)) return 4;
    if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(expression);unary && unary->op=="NOT") {
        const auto input=preparedWhereTruths(unary->operand.get());
        return ((input&1)<<1) | ((input&2)>>1) | (input&4);
    }
    const auto* binary=dynamic_cast<const BinaryOpExpr*>(expression);
    if(!binary) return 7;
    if((binary->op=="=" || binary->op=="<>" || binary->op=="!=" || binary->op=="<" ||
        binary->op==">" || binary->op=="<=" || binary->op==">=") &&
       preparedStrictNullComparison(*binary)) return 4;
    if(binary->op!="AND" && binary->op!="OR") return 7;
    const auto left=preparedWhereTruths(binary->left.get()),right=preparedWhereTruths(binary->right.get());
    unsigned output=0;
    for(unsigned a=1;a<=4;a<<=1) if(left&a)
        for(unsigned b=1;b<=4;b<<=1) if(right&b) {
            if(binary->op=="AND") output|=(a==2 || b==2)?2:(a==4 || b==4)?4:1;
            else output|=(a==1 || b==1)?1:(a==4 || b==4)?4:2;
        }
    return output;
}

static bool preparedNullWhereBranch(const Expr* expression) {
    if(!(preparedWhereTruths(expression)&1)) return true;
    const auto* binary=dynamic_cast<const BinaryOpExpr*>(expression);
    return binary && (binary->op=="AND" || binary->op=="OR") &&
        (preparedNullWhereBranch(binary->left.get()) || preparedNullWhereBranch(binary->right.get()));
}

static void prepareNullWhereBranches(PreparedQueryExecution& execution,Expr* expression) {
    if(!expression || !(preparedWhereTruths(expression)&1)) return;
    auto* binary=dynamic_cast<BinaryOpExpr*>(expression);
    if(!binary || (binary->op!="AND" && binary->op!="OR") || !preparedNullWhereBranch(expression)) return;
    const auto child=[&](Expr* node) {
        if(!(preparedWhereTruths(node)&1)) return;
        const auto* connective=dynamic_cast<const BinaryOpExpr*>(node);
        if(connective && (connective->op=="AND" || connective->op=="OR") && preparedNullWhereBranch(node))
            prepareNullWhereBranches(execution,node);
        else execution.prepareExpression(node);
    };
    child(binary->left.get());child(binary->right.get());
}

static ExprValue evaluatePreparedWhere(const PreparedSelectState& state,const Expr* expression,const RowContext& row) {
    if(!(preparedWhereTruths(expression)&1)) return ExprValue("boolean","false");
    const auto* binary=dynamic_cast<const BinaryOpExpr*>(expression);
    if(binary && (binary->op=="AND" || binary->op=="OR") && preparedNullWhereBranch(expression)) {
        const auto left=evaluatePreparedWhere(state,binary->left.get(),row);
        const bool truth=!left.isNull && left.asBool();
        if((binary->op=="AND" && !truth) || (binary->op=="OR" && truth)) return left;
        return evaluatePreparedWhere(state,binary->right.get(),row);
    }
    return state.evaluate(expression,row);
}

class PreparedFilterOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    std::optional<StorageEngine::Condition> storedTextPredicate_;
    bool childOpened_ = false, empty_ = false;
    size_t storedWhereRows_ = 0;

    std::optional<StorageEngine::Condition> bindStoredTextPredicate() const {
        const auto* comparison = dynamic_cast<const BinaryOpExpr*>(state_->select().whereClause.get());
        if (!comparison || comparison->rowComparison || comparison->preparedSubquery ||
            state_->sourceContexts || state_->executionProvider || typeid(*child_) != typeid(TableScanOp))
            return {};
        if (comparison->comparison &&
            (!comparison->comparison->strict || !comparison->comparison->collation.empty() ||
             comparison->comparison->identity.rfind("builtin-comparison:", 0) != 0))
            return {};
        static const std::set<std::string> operations = {"=", "<>", "!=", "<", ">", "<=", ">="};
        if (!operations.count(comparison->op)) return {};

        const auto& planned = state_->schema;
        const auto ordinal = [&](const Expr* expression) -> std::optional<size_t> {
            const auto* column = dynamic_cast<const ColumnRefExpr*>(expression);
            if (!column || column->preparedSubquery || !column->binding || column->generatedRead)
                return {};
            const auto& binding = *column->binding;
            if (binding.scopeDepth || binding.mergedUsing ||
                binding.sourceOrdinal != state_->sourceOrdinal || binding.columnOrdinal >= planned.len())
                return {};
            return binding.columnOrdinal;
        };
        const auto left = ordinal(comparison->left.get());
        const auto right = ordinal(comparison->right.get());
        if (!left || !right || (planned.cols[*left].collation.empty() && planned.cols[*right].collation.empty()))
            return {};

        const auto origin = child_->scanOrigin();
        if (origin.engine != state_->engine || origin.dbname != state_->dbname ||
            origin.tablename != planned.tablename) return {};
        // The scan now holds its relation lock. Verify its current declaration
        // before using the prepared query's physical field positions.
        const auto current = origin.engine->getTableSchema(origin.dbname, origin.tablename);
        if (!planned.physicalRelationId || current.physicalRelationId != planned.physicalRelationId ||
            current.formatVersion != planned.formatVersion || current.len() != planned.len() ||
            !std::equal(planned.cols.begin(), planned.cols.end(), current.cols.begin(),
                [](const Column& a, const Column& b) {
                    return !a.isArray && !b.isArray && a.generatedKind != 'v' && b.generatedKind != 'v' &&
                        a.domainName.empty() && b.domainName.empty() &&
                        !stored_regtype_detail::columnType(a.dataType) &&
                        a.dataName == b.dataName && a.dataType == b.dataType && a.dsize == b.dsize &&
                        a.isVariableLength == b.isVariableLength && a.isNull == b.isNull &&
                        a.storageCodec == b.storageCodec && a.generatedKind == b.generatedKind &&
                        a.collation == b.collation && a.resolvedCollation == b.resolvedCollation &&
                        a.resolvedCollationUsesLocale == b.resolvedCollationUsesLocale &&
                        a.resolvedCollationIsBinary == b.resolvedCollationIsBinary;
                }))
            return {};

        StorageEngine::Condition condition;
        condition.op = "typedexpr";
        condition.colName = planned.cols[*left].dataName;
        condition.columnComparison = StorageEngine::Condition::ColumnComparison{
            planned.cols[*right].dataName, comparison->op};
        StorageEngine::bindStoredComparison(condition, planned);
        using Kind = StorageEngine::Condition::ColumnComparison::Kind;
        if (condition.columnComparison->kind != Kind::Text) return {};
        return condition;
    }
public:
    PreparedFilterOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {}
    bool open() override {
        OpenInstrument startup(this); clearError(); childOpened_ = false; empty_ = false;
        storedWhereRows_ = 0;
        storedTextPredicate_.reset();
        if(!(preparedWhereTruths(state_->select().whereClause.get())&1)) { empty_=true;return true; }
        if (PreparedSelectState::rowIndependent(state_->select().whereClause.get())) {
            auto row = state_->execution->context(state_->outerRow);
            const auto value = state_->evaluate(state_->select().whereClause.get(), row);
            if (value.isNull || !value.asBool()) { empty_ = true; return true; }
        }
        childOpened_ = true;
        if (!child_->open()) return false;
        storedTextPredicate_ = bindStoredTextPredicate();
        return true;
    }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        if (empty_) return false;
        std::vector<bool> nulls;
        while (child_->next(row)) {
            if (child_->lastSatisfiedWhere() == state_->select().whereClause.get()) {
                ++storedWhereRows_;
                instrumentation.emitted = true;
                return true;
            }
            if (storedTextPredicate_ && child_->lastStoredNullBitmap(nulls) &&
                nulls.size() == state_->schema.len()) {
                if (!StorageEngine::evalConditionOnRow(*storedTextPredicate_, row, state_->schema, nulls))
                    continue;
                instrumentation.emitted = true;
                return true;
            }
            const auto value = evaluatePreparedWhere(*state_,state_->select().whereClause.get(),
                state_->context(*child_, row));
            if (!value.isNull && value.asBool()) { instrumentation.emitted = true; return true; }
        }
        if (child_->hasError()) return propagateChildError(child_.get(), "prepared filter child failed");
        return false;
    }
    void close() override {
        if (childOpened_) { childOpened_ = false; child_->close(); }
        empty_ = false;
    }
    bool lastColumnIsNull(size_t ordinal) const override { return child_->lastColumnIsNull(ordinal); }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override {
        return child_->lastStoredNullBitmap(nulls);
    }
    bool supportsStructuredRows() const override { return child_->supportsStructuredRows(); }
    bool supportsPreparedContexts() const override { return child_->supportsPreparedContexts(); }
    bool lastPreparedContext(RowContext& row) const override { return child_->lastPreparedContext(row); }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override { return child_->lastStructuredValues(values); }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        return child_->lastStructuredRow(cells, nulls);
    }
    ScanOrigin scanOrigin() const override { return child_->scanOrigin(); }
    const Operator* openedSource() const { return childOpened_ ? child_.get() : nullptr; }
    std::string preparedPlanNodeName() const override {
        const auto children=state_->execution?state_->execution->childPlans(state_->select().whereClause.get()):std::vector<Operator*>{};
        return children.empty()?"TypedFilter":"QuantifiedSubqueryFilter";
    }
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        if (storedWhereRows_ == 0) return {};
        return {{"storedWhereRows", std::to_string(storedWhereRows_)}};
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        auto plans=state_->execution?state_->execution->childPlans(state_->select().whereClause.get()):std::vector<Operator*>{};
        plans.insert(plans.begin(),child_.get());return plans;
    }
};

// Aggregates consume the original typed source graph. Each original call has
// an execution-owned result column; shared SQL ASTs and parameter frames are
// never rewritten to a rendered SQL query or to per-row text placeholders.
static std::optional<int> comparePreparedIdentityKeys(ExprEvaluator& evaluator,
    const ExprValue& left, const ExprValue& right);

static bool supportsPreparedArrayOrder(const std::string& type) {
    if (!common_type_detail::array(type)) return false;
    // These element types have both a typed order and the corresponding
    // equality. In particular, equality-only types cannot use this for DISTINCT.
    static const std::set<std::string> orderedTypes{
        "smallint[]", "integer[]", "bigint[]", "numeric[]", "real[]", "double precision[]",
        "boolean[]", "oid[]", "name[]", "\"char\"[]", "text[]", "varchar[]", "character varying[]",
        "bpchar[]", "character[]", "bytea[]", "uuid[]", "money[]", "interval[]",
        "bit[]", "bit varying[]", "inet[]", "cidr[]", "macaddr[]", "macaddr8[]",
        "date[]", "time[]", "time without time zone[]", "timetz[]", "time with time zone[]", "jsonb[]",
        "timestamp[]", "timestamp without time zone[]", "timestamptz[]", "timestamp with time zone[]"
    };
    return orderedTypes.count(type) != 0;
}

// A single typed comparison owner for top-level and inside-aggregate sorting.
// Callers handle NULL placement/direction; this function never evaluates keys.
static int comparePreparedOrderValues(ExprEvaluator& evaluator,
    const ExprValue& left, const ExprValue& right,
    const std::optional<QueryComparisonBinding>& enumComparison) {
    if (enumComparison) {
        if (evaluator.comparePrepared(*enumComparison, left, right).asBool()) return -1;
        if (evaluator.comparePrepared(*enumComparison, right, left).asBool()) return 1;
        return 0;
    }
    if (const auto comparison = comparePreparedIdentityKeys(evaluator, left, right))
        return *comparison;
    const auto type = ExprHelper::canonicalResultTypeName(left.typeName);
    const auto rightType = ExprHelper::canonicalResultTypeName(right.typeName);
    if (type == "timetz" && rightType == "timetz") {
        const auto order = temporal_storage_detail::compareZonedClocks(left.value, right.value);
        if (!order) throw DbError("22007", "invalid time with time zone sort key");
        return *order;
    }
    if (type == rightType && supportsPreparedArrayOrder(type))
        return ExprEvaluator::compareArrayValues(left, right);
    if ((type == "real" || type == "double precision") &&
        type == rightType) {
        // Use the expression comparison owner for IEEE special values and
        // float4 width. The generic storage predicate can reject infinities;
        // SQL sorting instead orders NaN above every non-NaN value.
        const auto binding = ExprEvaluator::resolveComparison("<", type, type);
        if (evaluator.comparePrepared(binding, left, right).asBool()) return -1;
        if (evaluator.comparePrepared(binding, right, left).asBool()) return 1;
        return 0;
    }
    Column column;
    const auto error = TypeRegistry::instance().resolveColumnType(column, left.typeName, {}, false);
    if (!error.empty()) throw DbError("0A000", "unsupported prepared sort type: " + left.typeName);
    column.collation = left.collation;
    const auto less = StorageEngine::compareValues(column, left.value, false, right.value, false, "<");
    const auto greater = StorageEngine::compareValues(column, left.value, false, right.value, false, ">");
    if (less == StorageEngine::PredicateTruth::Unknown || greater == StorageEngine::PredicateTruth::Unknown)
        throw DbError("0A000", "unsupported prepared sort comparison");
    return less == StorageEngine::PredicateTruth::True ? -1 :
           greater == StorageEngine::PredicateTruth::True ? 1 : 0;
}

static void incrementAggregateCount(int64_t& count) {
    if (count == std::numeric_limits<int64_t>::max())
        throw DbError("22003", "aggregate count out of range");
    ++count;
}

class PreparedAggregateOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    struct Entry { ExprValue value; std::vector<ExprValue> keys; };
    std::vector<std::vector<Entry>> inputs_;
    std::vector<std::optional<int64_t>> counts_;
    RowContext result_;
    bool emitted_ = false, opened_ = false;

    ExprValue binary(const std::string& operation, const ExprValue& left,
                     const ExprValue& right) const {
        BinaryOpExpr expression; expression.op = operation;
        auto first = std::make_unique<ParameterExpr>();
        first->slot = 0; first->origin = ParameterOrigin::RuntimeCell; first->declaredType = left.typeName;
        auto second = std::make_unique<ParameterExpr>();
        second->slot = 1; second->origin = ParameterOrigin::RuntimeCell; second->declaredType = right.typeName;
        expression.left = std::move(first); expression.right = std::move(second);
        RowContext row; row.setParameters({left, right});
        return state_->evaluator.eval(&expression, row);
    }
    ExprValue cast(const ExprValue& value, const std::string& type) const {
        if (ExprHelper::canonicalResultTypeName(value.typeName) == type) return value;
        CastExpr expression; expression.typeName = type;
        auto input = std::make_unique<ParameterExpr>();
        input->origin = ParameterOrigin::RuntimeCell; input->declaredType = value.typeName;
        expression.operand = std::move(input);
        RowContext row; row.setParameters({value});
        return state_->evaluator.eval(&expression, row);
    }
    ExprValue finish(size_t index) {
        if (counts_[index]) return ExprValue("bigint", std::to_string(*counts_[index]));
        const auto& aggregate = state_->aggregates[index];
        auto& inputs = inputs_[index];
        if (!aggregate.call->aggregateOrderKeys.empty()) {
            interruptibleStableSort(inputs.begin(), inputs.end(), [&](const Entry& left, const Entry& right) {
                for (size_t i = 0; i < aggregate.call->aggregateOrderKeys.size(); ++i) {
                    const auto& key = aggregate.call->aggregateOrderKeys[i];
                    const auto& a = left.keys.at(i);
                    const auto& b = right.keys.at(i);
                    if (a.isNull || b.isNull) {
                        if (a.isNull == b.isNull) continue;
                        return a.isNull == key.nullsFirst;
                    }
                    const int order = comparePreparedOrderValues(state_->evaluator, a, b, key.enumComparison);
                    if (order) return key.ascending ? order < 0 : order > 0;
                }
                return false;
            });
        }
        int64_t count = 0;
        ExprValue value(aggregate.resultType, "", true);
        std::vector<ExprValue> distinct;
        for (const auto& input : inputs) {
            if (input.value.isNull) continue;
            if (aggregate.call->distinct) {
                if (std::any_of(distinct.begin(), distinct.end(), [&](const auto& previous) {
                        return binary("=", previous, input.value).asBool(); })) continue;
                distinct.push_back(input.value);
            }
            incrementAggregateCount(count);
            if (aggregate.name == "count") continue;
            if (aggregate.name == "sum" || aggregate.name == "avg") {
                const auto converted = cast(input.value, aggregate.resultType);
                value = value.isNull ? converted : binary("+", value, converted);
            } else if (aggregate.name == "min" || aggregate.name == "max") {
                const auto converted = cast(input.value, aggregate.resultType);
                if (value.isNull || binary(aggregate.name == "min" ? "<" : ">", converted, value).asBool())
                    value = converted;
            } else value = value.isNull ? input.value :
                binary(aggregate.name == "bool_or" ? "OR" : "AND", value, input.value);
        }
        if (aggregate.name == "count") return ExprValue("bigint", std::to_string(count));
        if (aggregate.name == "avg" && count)
            value = binary("/", value, cast(ExprValue("bigint", std::to_string(count)),
                aggregate.resultType == "interval" ? "double precision" : aggregate.resultType));
        return value;
    }
public:
    PreparedAggregateOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {}
    bool open() override {
        OpenInstrument instrument(this); clearError(); emitted_ = false;
        inputs_.assign(state_->aggregates.size(), {});
        counts_.clear();
        for (const auto& aggregate : state_->aggregates) {
            if (aggregate.name == "count" && !aggregate.call->distinct && aggregate.call->aggregateOrderKeys.empty())
                counts_.emplace_back(0);
            else counts_.emplace_back(std::nullopt);
        }
        opened_ = true; return child_->open();
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if (emitted_) return false;
        emitted_ = true;
        std::string raw;
        while (child_->next(raw)) {
            checkForQueryInterrupt();
            const auto row = state_->context(*child_, raw);
            for (size_t i = 0; i < state_->aggregates.size(); ++i) {
                const auto& aggregate = state_->aggregates[i];
                if (!state_->select().existsTargetPrunable &&
                    !state_->execution->aggregateColumnDemanded(aggregate.call)) continue;
                if (aggregate.call->filter) {
                    const auto filter = state_->evaluate(aggregate.call->filter.get(), row);
                    if (filter.isNull || !filter.asBool()) continue;
                }
                const auto* literal = dynamic_cast<const LiteralExpr*>(aggregate.call->args.front().get());
                Entry input;
                input.value = literal && literal->value == "*" ? ExprValue("boolean", "t") :
                    state_->evaluate(aggregate.call->args.front().get(), row);
                if (auto& count = counts_[i]; count) {
                    if (!input.value.isNull) incrementAggregateCount(*count);
                    continue;
                }
                for (size_t key = 0; key < aggregate.call->aggregateOrderKeys.size(); ++key) {
                    const auto owner = aggregate.keyOwners.at(key);
                    if (owner == PreparedSelectState::noTarget - 1)
                        input.keys.push_back(input.value);
                    else if (owner != PreparedSelectState::noTarget)
                        input.keys.push_back(input.keys.at(owner));
                    else input.keys.push_back(state_->evaluate(aggregate.call->aggregateOrderKeys[key].expr.get(), row));
                }
                inputs_[i].push_back(std::move(input));
            }
        }
        if (child_->hasError()) return propagateChildError(child_.get(), "prepared aggregate input failed");
        result_ = state_->execution->context(state_->outerRow);
        for (size_t i = 0; i < state_->aggregates.size(); ++i) {
            const auto& column = state_->aggregates[i].column;
            result_.setBoundColumn(column.sourceOrdinal, column.columnOrdinal, finish(i));
        }
        if (state_->select().having) {
            const auto having = state_->evaluate(state_->select().having.get(), result_);
            if (having.isNull || !having.asBool()) return false;
        }
        display.clear(); instrument.emitted = true; return true;
    }
    void close() override {
        inputs_.clear(); counts_.clear(); emitted_ = false;
        if (opened_) { opened_ = false; child_->close(); }
    }
    bool supportsPreparedContexts() const override { return true; }
    bool lastPreparedContext(RowContext& row) const override {
        if (!emitted_) return false; row = result_; return true;
    }
    std::string preparedPlanNodeName() const override { return "Aggregate"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};

// Catalog identities are genuine scalar datums, not physical Column codecs.
// NAME/internal-char use their existing typed implementations; OIDs order as
// unsigned32 identities. The same comparison defines sorting and WITH TIES.
static uint32_t preparedOidValue(const std::string& value) {
    uint32_t result=0;
    const auto parsed=std::from_chars(value.data(),value.data()+value.size(),result);
    if(parsed.ec==std::errc::result_out_of_range)throw DbError("22003","oid datum out of range");
    if(parsed.ec!=std::errc{} || parsed.ptr!=value.data()+value.size())
        throw DbError("22P02","invalid oid datum");
    return result;
}

static std::optional<int> comparePreparedIdentityKeys(ExprEvaluator& evaluator,
    const ExprValue& left,const ExprValue& right) {
    const auto type=ExprHelper::canonicalResultTypeName(left.typeName);
    if(type!=ExprHelper::canonicalResultTypeName(right.typeName))return std::nullopt;
    if(type=="oid") {
        const auto a=preparedOidValue(left.value),b=preparedOidValue(right.value);
        return (a>b)-(a<b);
    }
    if(stored_regtype_detail::scalarType(type) || type=="name" || type=="\"char\"") {
        const auto binding=ExprEvaluator::resolveComparison("<",type,type);
        if(evaluator.comparePrepared(binding,left,right).asBool())return -1;
        if(evaluator.comparePrepared(binding,right,left).asBool())return 1;
        return 0;
    }
    return std::nullopt;
}

// An owning input row. Physical source cells are already owned by the bound
// context, so ordinary Sort need not retain another copy of those cells.
struct TypedSortRow {
    std::string raw;
    std::vector<bool> deferredNulls;
    std::vector<ExprValue> cells, keys;
    std::vector<std::optional<int64_t>> integerKeys;
    RowContext context;
    std::vector<std::optional<ExprValue>> targets;
    uint64_t ordinal = 0;
};
struct TypedSortRowCodec {
    size_t cells, keys, targets;
    // Ordinary Sort caches one integer per owning key. Set output keys can
    // borrow cells instead, so validate their independent cache descriptor.
    std::optional<size_t> integers = std::nullopt;
    size_t deferredColumns = 0;
    mutable std::shared_ptr<typed_sort_detail::GeneratedRegistry> generatedRegistry; // lazy actual spill metadata only
    struct Header { uint64_t raw=0,cells=0,keys=0,integers=0,targets=0,ordinal=0,deferredNulls=0; };
    size_t bytes(const TypedSortRow& row) const {
        using namespace typed_sort_detail;
        size_t result=addBytes(sizeof(row),row.raw.capacity());
        result=addBytes(result,row.deferredNulls.capacity() / 8);
        const auto values=[&](const auto& list) {
            size_t total=multiplyBytes(list.capacity(),sizeof(ExprValue));
            for(const auto& value:list)total=addBytes(total,dynamicValueBytes(value));
            return total;
        };
        result=addBytes(result,addBytes(values(row.cells),values(row.keys)));
        result=addBytes(result,multiplyBytes(row.integerKeys.capacity(),sizeof(std::optional<int64_t>)));
        result=addBytes(result,contextBytes(row.context)-sizeof(RowContext));
        result=addBytes(result,multiplyBytes(row.targets.capacity(),sizeof(std::optional<ExprValue>)));
        for(const auto& value:row.targets)if(value)result=addBytes(result,dynamicValueBytes(*value));
        return result;
    }
    void write(sort_detail::Writer& writer,const TypedSortRow& row) const {
        writer.scalar(Header{row.raw.size(),row.cells.size(),row.keys.size(),row.integerKeys.size(),
                             row.targets.size(),row.ordinal,row.deferredNulls.size()});
        writer.append(row.raw.data(),row.raw.size());
        for (bool isNull : row.deferredNulls) writer.scalar(uint8_t(isNull));
        for(const auto& value:row.cells)typed_sort_detail::writeValue(writer,value);
        for(const auto& value:row.keys)typed_sort_detail::writeValue(writer,value);
        for(const auto& value:row.integerKeys) {
            writer.scalar(uint8_t(value.has_value()));if(value)writer.scalar(*value);
        }
        // BoundedRuns uses this same codec for all its writes/reads/merges.
        // Allocate only when a real generated context is actually spilled;
        // ordinary/SetSort owner algorithms and zero-demand paths are intact.
        if(row.context.hasGeneratedInputs() && !generatedRegistry)
            generatedRegistry=std::make_shared<typed_sort_detail::GeneratedRegistry>();
        typed_sort_detail::writeContext(writer,row.context,generatedRegistry.get());
        for(const auto& value:row.targets) {
            writer.scalar(uint8_t(value.has_value()));if(value)typed_sort_detail::writeValue(writer,*value);
        }
    }
    TypedSortRow read(sort_detail::Reader& reader) const {
        const auto header=reader.scalar<Header>();
        if(header.cells!=cells || header.keys!=keys || header.integers!=integers.value_or(keys) || header.targets!=targets ||
           (header.deferredNulls && header.deferredNulls!=deferredColumns) ||
           header.raw>reader.remaining() || header.raw>std::numeric_limits<size_t>::max())
            typed_sort_detail::invalid("row descriptor");
        TypedSortRow row;row.ordinal=header.ordinal;row.raw=reader.string(static_cast<size_t>(header.raw));
        if (header.deferredNulls > reader.remaining()) typed_sort_detail::invalid("stored NULL count");
        row.deferredNulls.reserve(static_cast<size_t>(header.deferredNulls));
        for (uint64_t i = 0; i < header.deferredNulls; ++i) {
            const auto isNull = reader.scalar<uint8_t>();
            if (isNull > 1) typed_sort_detail::invalid("stored NULL flag");
            row.deferredNulls.push_back(isNull != 0);
        }
        const auto values=[&](uint64_t count,std::vector<ExprValue>& list) {
            if(count>reader.remaining()/sizeof(typed_sort_detail::ValueHeader))typed_sort_detail::invalid("row value count");
            // The extent and plan descriptor have been validated. Avoid
            // geometric growth enlarging each decoded merge-head snapshot.
            list.reserve(static_cast<size_t>(count));
            for(uint64_t i=0;i<count;++i)list.push_back(typed_sort_detail::readValue(reader));
        };
        values(header.cells,row.cells);values(header.keys,row.keys);
        if(header.integers>reader.remaining())typed_sort_detail::invalid("integer cache count");
        row.integerKeys.reserve(static_cast<size_t>(header.integers));
        for(uint64_t i=0;i<header.integers;++i) {
            const auto present=reader.scalar<uint8_t>();if(present>1)typed_sort_detail::invalid("integer cache flag");
            row.integerKeys.push_back(present?std::optional<int64_t>(reader.scalar<int64_t>()):std::nullopt);
        }
        row.context=typed_sort_detail::readContext(reader,generatedRegistry.get());
        if(header.targets>reader.remaining())typed_sort_detail::invalid("target cache count");
        row.targets.reserve(static_cast<size_t>(header.targets));
        for(uint64_t i=0;i<header.targets;++i) {
            const auto present=reader.scalar<uint8_t>();if(present>1)typed_sort_detail::invalid("target cache flag");
            row.targets.push_back(present?std::optional<ExprValue>(typed_sort_detail::readValue(reader)):std::nullopt);
        }
        return row;
    }
};

// Ordinary reference-array consumers share the actual owning declaration and
// identity checks. Display text cannot provide either piece of provenance.
static const CompoundDatum& owningReferenceArray(const ExprValue& value,
                                                uint32_t elementCodec) {
    const auto& data = value.compound;
    if (value.isNull || !data || !data->type ||
        data->type->kind != DatumKind::Array || !data->type->actualOid ||
        !data->type->codecOid || !data->type->arrayElement ||
        data->type->arrayElement->kind != DatumKind::Scalar ||
        !data->type->arrayElement->actualOid ||
        data->type->arrayElement->codecOid != elementCodec ||
        !data->arrayElements)
        throw DbError("0A000", "reference array requires its actual owning declaration");
    if (compound_value_detail::elementCount(data->dimensions) !=
            data->arrayElements->size() ||
        (!value.elementOids.empty() &&
         value.elementOids.size() != data->arrayElements->size()))
        throw DbError("XX000", "reference array lost its owning shape or identities");
    return *data;
}

static uint32_t owningReferenceElementOid(const ExprValue& parent,
    const CompoundDatum& data, const ExprValue& element, size_t index) {
    if (element.isNull) {
        if ((data.arrayNullBitmap && !*data.arrayNullBitmap) ||
            (!parent.elementOids.empty() && parent.elementOids[index]))
            throw DbError("XX000", "reference array lost its element NULL metadata");
        return 0;
    }
    if (!element.objectOid || (!parent.elementOids.empty() &&
            parent.elementOids[index] != element.objectOid))
        throw DbError("XX000", "reference array element lost its actual object identity");
    return *element.objectOid;
}

// Compare unsigned identities and the owned row-major shape directly
// (PostgreSQL REL_18_6 array_cmp ordering), without interpreting output names.
static int compareOwningReferenceArrays(const ExprValue& left,
                                        const ExprValue& right) {
    const auto type = ExprHelper::canonicalResultTypeName(left.typeName);
    const uint32_t elementCodec = type == "regtype[]" ? 2206 : 2205;
    const auto& a = owningReferenceArray(left, elementCodec);
    const auto& b = owningReferenceArray(right, elementCodec);
    if (a.type->actualOid != b.type->actualOid ||
        a.type->codecOid != b.type->codecOid ||
        a.type->arrayElement->actualOid != b.type->arrayElement->actualOid)
        throw DbError("42804", "cannot compare arrays of different actual element types");
    const auto& x = *a.arrayElements;
    const auto& y = *b.arrayElements;
    for (size_t i = 0; i < std::min(x.size(), y.size()); ++i) {
        if ((i & 127U) == 0) checkForQueryInterrupt();
        const uint32_t aOid = owningReferenceElementOid(left, a, x[i], i);
        const uint32_t bOid = owningReferenceElementOid(right, b, y[i], i);
        if (x[i].isNull != y[i].isNull) return x[i].isNull ? 1 : -1;
        if (!x[i].isNull && aOid != bOid) return aOid < bOid ? -1 : 1;
    }
    if (x.size() != y.size()) return x.size() < y.size() ? -1 : 1;
    if (a.dimensions.size() != b.dimensions.size())
        return a.dimensions.size() < b.dimensions.size() ? -1 : 1;
    for (size_t i = 0; i < a.dimensions.size(); ++i)
        if (a.dimensions[i].length != b.dimensions[i].length)
            return a.dimensions[i].length < b.dimensions[i].length ? -1 : 1;
    for (size_t i = 0; i < a.dimensions.size(); ++i)
        if (a.dimensions[i].lower != b.dimensions[i].lower)
            return a.dimensions[i].lower < b.dimensions[i].lower ? -1 : 1;
    return 0;
}

// A complete, framed logical identity for one non-NULL ordinary DISTINCT
// target. Bitmap retention is physical provenance, not SQL array equality.
static std::string owningReferenceArrayDistinctKey(const ExprValue& value,
                                                  uint32_t elementCodec) {
    const auto& data = owningReferenceArray(value, elementCodec);
    std::string key = "reference-array:" + std::to_string(data.type->actualOid) + ":" +
        std::to_string(data.type->codecOid) + ":" +
        std::to_string(data.type->arrayElement->actualOid) + ":" +
        std::to_string(elementCodec) + ";" + std::to_string(data.dimensions.size()) + ";";
    for (const auto& dimension : data.dimensions)
        key += std::to_string(dimension.length) + ":" + std::to_string(dimension.lower) + ";";
    const auto& elements = *data.arrayElements;
    for (size_t i = 0; i < elements.size(); ++i) {
        if ((i & 127U) == 0) checkForQueryInterrupt();
        const uint32_t oid = owningReferenceElementOid(value, data, elements[i], i);
        key += elements[i].isNull ? "N;" : "V" + std::to_string(oid) + ";";
    }
    return key;
}

// Ordinary Sort owns one recent comparison signature per key. Descriptor
// setup is lazy at the original comparison demand, never at key evaluation.
// Aggregate/grouped sorting keeps its existing comparison owner above.
class OrdinarySortComparisons {
    struct Entry {
        bool ready=false;
        std::string leftType,rightType,leftCollation,rightCollation;
        enum class Kind { Storage,Expression,Oid,ReferenceArray,Array,ZonedClock } kind=Kind::Storage;
        std::optional<Column> column;
        std::optional<QueryComparisonBinding> binding;
    };
    std::vector<Entry> entries_;
public:
    void reset(size_t size=0) {std::vector<Entry> fresh(size);entries_.swap(fresh);}
    int compare(size_t index,ExprEvaluator& evaluator,const ExprValue& a,const ExprValue& b,
                const std::optional<QueryComparisonBinding>& enumeration) {
        if(enumeration) {
            if(evaluator.comparePrepared(*enumeration,a,b).asBool())return -1;
            if(evaluator.comparePrepared(*enumeration,b,a).asBool())return 1;
            return 0;
        }
        auto& entry=entries_.at(index);
        if(!entry.ready || entry.leftType!=a.typeName || entry.rightType!=b.typeName ||
           entry.leftCollation!=a.collation || entry.rightCollation!=b.collation) {
            Entry next;next.leftType=a.typeName;next.rightType=b.typeName;
            next.leftCollation=a.collation;next.rightCollation=b.collation;
            const auto type=ExprHelper::canonicalResultTypeName(a.typeName);
            const bool same=type==ExprHelper::canonicalResultTypeName(b.typeName);
            if(same && type=="oid")next.kind=Entry::Kind::Oid;
            else if(same && type=="timetz")next.kind=Entry::Kind::ZonedClock;
            else if(same && (type=="regtype[]" || type=="regclass[]"))
                next.kind=Entry::Kind::ReferenceArray;
            else if (same && supportsPreparedArrayOrder(type))
                next.kind = Entry::Kind::Array;
            else if(same && (stored_regtype_detail::scalarType(type) || type=="name" || type=="\"char\"" ||
                             type=="real" || type=="double precision")) {
                next.kind=Entry::Kind::Expression;
                next.binding=ExprEvaluator::resolveComparison("<",type,type);
            } else {
                next.column.emplace();
                const auto error=TypeRegistry::instance().resolveColumnType(*next.column,a.typeName,{},false);
                if(!error.empty())throw DbError("0A000","unsupported prepared sort type: "+a.typeName);
                next.column->collation=a.collation;
            }
            next.ready=true;entry=std::move(next);
        }
        if(entry.kind==Entry::Kind::Oid) {
            const auto x=preparedOidValue(a.value),y=preparedOidValue(b.value);
            return (x>y)-(x<y);
        }
        if(entry.kind==Entry::Kind::ReferenceArray)
            return compareOwningReferenceArrays(a,b);
        if (entry.kind == Entry::Kind::Array)
            return ExprEvaluator::compareArrayValues(a, b);
        if (entry.kind == Entry::Kind::ZonedClock) {
            const auto order = temporal_storage_detail::compareZonedClocks(a.value, b.value);
            if (!order) throw DbError("22007", "invalid time with time zone sort key");
            return *order;
        }
        if(entry.kind==Entry::Kind::Expression) {
            if(evaluator.comparePrepared(*entry.binding,a,b).asBool())return -1;
            if(evaluator.comparePrepared(*entry.binding,b,a).asBool())return 1;
            return 0;
        }
        const auto lower=StorageEngine::compareValues(*entry.column,a.value,false,b.value,false,"<");
        const auto greater=StorageEngine::compareValues(*entry.column,a.value,false,b.value,false,">");
        if(lower==StorageEngine::PredicateTruth::Unknown || greater==StorageEngine::PredicateTruth::Unknown)
            throw DbError("0A000","unsupported prepared sort comparison");
        return lower==StorageEngine::PredicateTruth::True?-1:greater==StorageEngine::PredicateTruth::True?1:0;
    }
};

std::optional<PreparedIntegerKeyOrder> matchingPrimaryKeyInputOrder(
    const Operator* source, const PreparedSelectState& state);
} // namespace

class PreparedSortOp final : public Operator {
    using Row=TypedSortRow;
    struct Less {PreparedSortOp* owner;bool operator()(const Row& a,const Row& b) const {return owner->less(a,b);}};
    using Runs=sort_detail::BoundedRuns<Row,TypedSortRowCodec,Less>;
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    std::vector<Row> rows_; // Existing aggregate/HAVING path only.
    std::unique_ptr<Runs> runs_;
    std::optional<size_t> bound_;
    std::set<std::string> physicalKeyColumns_;
    std::vector<size_t> physicalKeyOrdinals_;
    size_t position_=0;
    bool budgeted_=false,opened_=false,available_=false;
    bool presortedInput_ = false, spilled_ = false;
    bool deferredPhysicalRows_ = false;
    Row current_;
    mutable OrdinarySortComparisons comparisons_;
    const Row& current() const {return budgeted_?current_:rows_.at(position_-1);}
    bool less(const Row& left,const Row& right) {
        for(size_t i=0;i<state_->keys.size();++i) {
            const auto& a=left.keys[i];const auto& b=right.keys[i];const auto& key=state_->keys[i];
            if(a.isNull || b.isNull) {if(a.isNull==b.isNull)continue;return a.isNull==key.nullsFirst;}
            int order;
            if(!key.enumComparison && a.typeName==b.typeName && left.integerKeys[i] && right.integerKeys[i]) {
                const auto x=*left.integerKeys[i],y=*right.integerKeys[i];order=(x>y)-(x<y);
            } else order=budgeted_?comparisons_.compare(i,state_->evaluator,a,b,key.enumComparison)
                :comparePreparedOrderValues(state_->evaluator,a,b,key.enumComparison);
            if(order)return key.asc?order<0:order>0;
        }
        return (budgeted_ || bound_) && left.ordinal<right.ordinal;
    }
    bool fail(const char* message) {
        propagateChildError(child_.get(),message);try{close();}catch(...){}return false;
    }
    void selectPhysicalKeyColumns() {
        const auto& schema = state_->schema;
        if (!bound_ || typeid(*child_) != typeid(TableScanOp) || state_->sourceContexts ||
            !state_->windows.empty() || !state_->select().locking.empty() ||
            schema.partitionType == TableSchema::PartitionType::None ||
            schema.formatVersion != DATA_FILE_FORMAT_VERSION || !schema.hasVariableLength()) return;
        for (size_t i = 0; i < schema.len(); ++i) {
            const auto& column = schema.cols[i];
            const auto type = ExprHelper::canonicalResultTypeName(column.dataType);
            if (column.isArray || column.isUnsigned || column.generatedKind ||
                !column.domainName.empty() || !column.enumValues.empty() ||
                (type != "smallint" && type != "integer" && type != "bigint" &&
                 type != "text" && type != "character varying" && type != "boolean")) return;
        }
        const auto columnOrdinal = [&](const Expr* expression) -> std::optional<size_t> {
            const auto* column = dynamic_cast<const ColumnRefExpr*>(expression);
            if (!column || !column->binding || column->preparedSubquery || column->generatedRead)
                return {};
            const auto& field = *column->binding;
            if (field.mergedUsing || field.scopeDepth || field.sourceOrdinal != state_->sourceOrdinal ||
                field.columnOrdinal >= schema.len()) return {};
            return field.columnOrdinal;
        };
        for (const auto* target : state_->targets) if (!columnOrdinal(target)) return;
        std::set<size_t> ordinals;
        for (const auto& key : state_->keys) {
            const auto* expression = key.target == PreparedSelectState::noTarget
                ? key.expression : state_->targets.at(key.target);
            const auto ordinal = columnOrdinal(expression);
            // Variable sort keys require their long values for every row.
            if (!ordinal || key.enumComparison || schema.cols[*ordinal].isVariableLength) return;
            ordinals.insert(*ordinal);
        }
        if (ordinals.empty() || ordinals.size() == schema.len()) return;
        for (const auto ordinal : ordinals)
            physicalKeyColumns_.insert(schema.cols[ordinal].dataName);
        physicalKeyOrdinals_.assign(ordinals.begin(), ordinals.end());
        static_cast<TableScanOp&>(*child_).scanPartitionsOnDemand_ = true;
    }
    void materializeCurrent() {
        if (current_.deferredNulls.empty()) return;
        const auto& schema = state_->schema;
        detail::ScopedStoredRowNullBitmap nullOwner(schema, current_.deferredNulls);
        bool ok = false;
        current_.raw = state_->engine->resolveToastValues(state_->dbname,
            schema.tablename, std::move(current_.raw), schema, &ok);
        if (!ok) throw DbError("58030", "could not read partition sort row values");
        // The scan has already advanced past this row. Use the bitmap saved
        // with the original tuple, including when the row comes from a run.
        current_.context = state_->context(state_->cells(*child_, current_.raw, &current_.deferredNulls));
    }
    bool nextRow(std::string* row) {
        NextInstrument instrument(this);
        available_ = false;
        try {
            if (budgeted_) {
                if (!runs_ || !runs_->next(current_)) return false;
            } else if (position_ == rows_.size()) return false;
            ++position_;
            if (row) {
                if (budgeted_) materializeCurrent();
                *row = current().raw;
                available_ = true;
            }
            instrument.emitted = true;
            return true;
        } catch (...) {
            const auto primary = std::current_exception();
            try { close(); } catch (...) {}
            std::rethrow_exception(primary);
        }
    }
public:
    PreparedSortOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {
        const auto& select = state_->select();
        // DISTINCT and WITH TIES can need rows beyond the ordinary LIMIT.
        // Keep aggregate/grouped paths unchanged; this bound belongs only to
        // the ordinary sorted input immediately below projection/OFFSET/LIMIT.
        if (select.limit && *select.limit && !select.distinct &&
            select.distinctOn.empty() && !select.withTies &&
            state_->aggregates.empty() && !select.having &&
            select.groupBy.empty() && select.groupByElems.empty() &&
            (!select.signedFetchCount || *select.signedFetchCount >= 0)) {
            const size_t offset = select.offset.value_or(0);
            if (*select.limit <= std::numeric_limits<size_t>::max() - offset)
                bound_ = offset + *select.limit;
        }
        selectPhysicalKeyColumns();
    }
    ~PreparedSortOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument startup(this);clearError();rows_.clear();runs_.reset();position_=0;available_=false;current_=Row{};
        presortedInput_ = spilled_ = deferredPhysicalRows_ = false;
        const auto& select=state_->select();
        budgeted_=state_->aggregates.empty() && !select.having && select.groupBy.empty() && select.groupByElems.empty();
        comparisons_.reset(budgeted_?state_->keys.size():0);
        if(budgeted_)runs_=std::make_unique<Runs>(sortWorkMemBytes(),TypedSortRowCodec{0,state_->keys.size(),state_->targets.size(),
            std::nullopt,physicalKeyColumns_.empty()?0:state_->schema.len()},Less{this},bound_,
            [](std::vector<Row>& rows,const Less& less){interruptibleStableSort(rows.begin(),rows.end(),less);});
        try {
            opened_=true;
            if(!child_->open())return fail("prepared sort child could not open");
            uint64_t ordinal=0;
            std::string raw;
            auto* scan = physicalKeyColumns_.empty() ? nullptr : static_cast<TableScanOp*>(child_.get());
            std::vector<std::string> keyCells;
            std::vector<bool> keyNulls;
            bool decodedKeys = false;
            const auto pull = [&] {
                return scan ? scan->nextForProjection(raw, state_->schema, physicalKeyColumns_,
                    keyCells, keyNulls, decodedKeys) : child_->next(raw);
            };
            // A logical table source may open its physical scan lazily at
            // the first pull. Inspect the chosen path before buffering rows.
            bool hasRow = pull();
            const auto order = runs_ ? matchingPrimaryKeyInputOrder(child_.get(), *state_) : std::nullopt;
            if (order) {
                runs_->usePresortedInput();
                presortedInput_ = true;
            }
            for (; hasRow; hasRow = pull()) {
                if((ordinal&127U)==0)checkForQueryInterrupt();
                Row row;
                if(ordinal==std::numeric_limits<uint64_t>::max())throw DbError("54000","too many sort input rows");
                row.ordinal=ordinal++;
                if (decodedKeys) {
                    if (!scan->lastStoredNullBitmap(row.deferredNulls))
                        throw DbError("XX001", "partition sort lost the input NULL bitmap");
                    deferredPhysicalRows_ = true;
                    row.context = state_->execution->context(state_->outerRow);
                    for (size_t i = 0; i < physicalKeyOrdinals_.size(); ++i) {
                        const auto column = physicalKeyOrdinals_[i];
                        const auto& field = state_->schema.cols[column];
                        ExprValue value(ExprHelper::canonicalResultTypeName(field.dataType),
                            std::move(keyCells.at(i)), keyNulls.at(i));
                        value.collation = field.collation;
                        row.context.setBoundColumn(state_->sourceOrdinal, column, std::move(value));
                    }
                } else if (!budgeted_ && !state_->sourceContexts && !child_->supportsPreparedContexts()) {
                    row.cells=state_->cells(*child_,raw);row.context=state_->context(row.cells);
                } else {
                    row.context=state_->context(*child_,raw);
                    // WindowAgg emits the genuine owning context. Its
                    // display row is empty and cannot be decoded as storage.
                    if (!budgeted_ && !state_->sourceContexts && state_->windows.empty())
                        row.cells=state_->cells(*child_,raw);
                }
                row.raw = std::move(raw);
            row.targets.resize(state_->targets.size());
            const auto& context = row.context;
            // Keep required input values here. Delayed targets use the
            // owning context when Project requests the sorted row. Sort keys,
            // aliases and structurally identical targets still share slots.
            for (size_t i = 0; i < state_->targets.size(); ++i)
                if (!decodedKeys && state_->targetOwners[i]==i && state_->targetsBeforeSort[i])
                    row.targets[i] = state_->evaluate(state_->targets[i], context);
            for (const auto& key : state_->keys) {
                if (key.priorKey != PreparedSelectState::noTarget) {
                    row.keys.push_back(row.keys.at(key.priorKey));
                    continue;
                }
                if (key.target != PreparedSelectState::noTarget) {
                    if (!row.targets[key.target])
                        row.targets[key.target] = state_->evaluate(state_->targets[key.target], context);
                    row.keys.push_back(*row.targets[key.target]);
                } else row.keys.push_back(state_->evaluate(key.expression, context));
            }
            // Cache only exact builtin signed integers after the original
            // expression has run. Domains, OIDs, numeric, mixed/invalid values
            // and all other types retain their existing comparison owner.
            row.integerKeys.reserve(row.keys.size());
            for (const auto& value : row.keys) {
                std::optional<int64_t> integer;
                if (!value.isNull && (value.typeName == "smallint" ||
                    value.typeName == "integer" || value.typeName == "bigint")) {
                    int64_t parsed = 0;
                    const auto end = value.value.data() + value.value.size();
                    const auto converted = std::from_chars(value.value.data(), end, parsed);
                    if (converted.ec == std::errc{} && converted.ptr == end &&
                        (value.typeName != "smallint" || (parsed >= INT16_MIN && parsed <= INT16_MAX)) &&
                        (value.typeName != "integer" || (parsed >= INT32_MIN && parsed <= INT32_MAX)))
                        integer = parsed;
                }
                row.integerKeys.push_back(integer);
            }
                if (decodedKeys) row.context = {};
                if (budgeted_) runs_->add(std::move(row));
                else rows_.push_back(std::move(row));
                // The matching physical scan also proved that the WHERE
                // terms are covered and the row window is fixed.
                if (bound_ && order && order->canStopAtLimit && ordinal >= *bound_) break;
            }
            if(child_->hasError())return fail("prepared sort child failed");
            if (budgeted_) {
                runs_->finish();
                spilled_ = runs_->spilled();
            }
            else interruptibleStableSort(rows_.begin(),rows_.end(),[&](const Row& a,const Row& b){return less(a,b);});
            return true;
        } catch(...) {
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    bool next(std::string& row) override { return nextRow(&row); }
    std::optional<size_t> skipUnprojectedRows(size_t count) {
        if (!deferredPhysicalRows_) return std::nullopt;
        size_t skipped = 0;
        while (skipped < count && nextRow(nullptr)) ++skipped;
        return skipped;
    }
    const std::vector<std::optional<ExprValue>>& targets() const {return current().targets;}
    const std::vector<ExprValue>& keys() const {return current().keys;}
    bool peers(const std::vector<ExprValue>& left,const std::vector<ExprValue>& right) const {
        for(size_t i=0;i<state_->keys.size();++i) {
            const auto& a=left.at(i);const auto& b=right.at(i);
            if(a.isNull || b.isNull){if(a.isNull!=b.isNull)return false;continue;}
            if(budgeted_) {if(comparisons_.compare(i,state_->evaluator,a,b,state_->keys[i].enumComparison))return false;continue;}
            if(const auto& comparison=state_->keys[i].enumComparison) {
                if(state_->evaluator.comparePrepared(*comparison,a,b).asBool() || state_->evaluator.comparePrepared(*comparison,b,a).asBool())return false;
                continue;
            }
            if(const auto comparison=comparePreparedIdentityKeys(state_->evaluator,a,b)) {if(*comparison)return false;continue;}
            const auto type = ExprHelper::canonicalResultTypeName(a.typeName);
            if (supportsPreparedArrayOrder(type) && type == ExprHelper::canonicalResultTypeName(b.typeName)) {
                if (ExprEvaluator::compareArrayValues(a, b)) return false;
                continue;
            }
            Column column;
            const auto error=TypeRegistry::instance().resolveColumnType(column,a.typeName,{},false);
            if(!error.empty())throw DbError("0A000","unsupported prepared peer type: "+a.typeName);
            column.collation=a.collation;
            const auto equality=StorageEngine::compareValues(column,a.value,false,b.value,false,"=");
            if(equality==StorageEngine::PredicateTruth::Unknown)throw DbError("0A000","unsupported prepared peer comparison");
            if(equality!=StorageEngine::PredicateTruth::True)return false;
        }
        return true;
    }
    bool supportsPreparedContexts() const override {return true;}
    bool lastPreparedContext(RowContext& row) const override {if(!available_)return false;row=current().context;return true;}
    bool supportsStructuredRows() const override {return true;}
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        if(!available_ || state_->sourceContexts)return false;
        values.clear();
        if(!budgeted_)values=current().cells;
        else if(state_->sourceOrdinal!=PreparedSelectState::noTarget)
            for(size_t i=0;i<state_->schema.len();++i)values.push_back(state_->execution->readSourceColumn(current().context,state_->sourceOrdinal,i));
        return true;
    }
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        if(!available_)return false;
        cells.clear();nulls.clear();
        if(budgeted_ && !state_->sourceContexts && state_->sourceOrdinal!=PreparedSelectState::noTarget) {
            for(size_t i=0;i<state_->schema.len();++i) {
                const auto value=state_->execution->readSourceColumn(current().context,state_->sourceOrdinal,i);
                cells.push_back(value.value);nulls.push_back(value.isNull);
            }
        } else for(const auto& value:current().cells){cells.push_back(value.value);nulls.push_back(value.isNull);}
        return true;
    }
    void close() override {
        runs_.reset();std::vector<Row>().swap(rows_);current_=Row{};position_=0;available_=false;comparisons_.reset();
        deferredPhysicalRows_ = false;
        if(opened_){opened_=false;child_->close();}
    }
    std::string preparedPlanNodeName() const override {return "TypedSort";}
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        if (!presortedInput_) return {};
        return {{"presortedInput", "true"}, {"spilled", spilled_ ? "true" : "false"}};
    }
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
};

namespace {
class PreparedProjectOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    std::vector<ExprValue> values_;
    std::optional<StorageEngine::NullRowBindingState> callerNullBinding_;
    bool childOpened_ = false;
public:
    PreparedProjectOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {}
    std::optional<size_t> skipUnprojectedRows(size_t count) {
        if (auto* sorted = dynamic_cast<PreparedSortOp*>(child_.get()))
            return sorted->skipUnprojectedRows(count);
        return std::nullopt;
    }
    bool open() override {
        OpenInstrument startup(this); clearError(); values_.clear(); childOpened_ = false;
        callerNullBinding_ = StorageEngine::captureNullRowBinding();
        state_->beginExecution();
        childOpened_ = true;
        return child_->open();
    }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        std::string raw;
        if (!child_->next(raw)) {
            if (child_->hasError()) return propagateChildError(child_.get(), "prepared projection child failed");
            return false;
        }
        values_.clear(); row.clear();
        if (state_->targets.empty()) {
            instrumentation.emitted = true;
            return true;
        }
        if (!state_->functionProjectionColumns.empty()) {
            RowContext source;
            if (!child_->lastPreparedContext(source))
                throw DbError("XX000", "function projection lost its source row");
            const auto occurrence = state_->visibleRanges.front()->ordinal;
            for (const auto column : state_->functionProjectionColumns) {
                const auto& value = source.boundColumn(occurrence, column);
                values_.push_back(value);
                row += value.isNull ? "NULL " : value.value + " ";
            }
            instrumentation.emitted = true;
            return true;
        }
        const auto context = state_->context(*child_, raw);
        const auto* sorted = dynamic_cast<const PreparedSortOp*>(child_.get());
        for (size_t i = 0; i < state_->targets.size(); ++i) {
            const auto owner=state_->targetOwners[i];
            values_.push_back(state_->select().existsTargetPrunable
                ? ExprValue(state_->output.at(i).type, "", true)
                : owner!=i ? values_.at(owner)
                : sorted && sorted->targets()[i] ? *sorted->targets()[i]
                : state_->evaluate(state_->targets[i], context));
            // A bare NULL's unknown type resolves to text at a SELECT output
            // boundary. Keep declared typed NULLs (parameters/casts) intact.
            if (values_.back().typeName == "unknown") values_.back().typeName = "text";
            row += values_.back().isNull ? "NULL " : values_.back().value + " ";
        }
        instrumentation.emitted = true; return true;
    }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        values = values_;
        return true;
    }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        cells.clear(); nulls.clear();
        for (const auto& value : values_) { cells.push_back(value.value); nulls.push_back(value.isNull); }
        return true;
    }
    std::string typedKey(std::vector<ExprValue>& arrays) const {
        std::string key;
        const auto& select = state_->select();
        const bool ordinary = state_->aggregates.empty() && !select.having &&
            select.groupBy.empty() && select.groupByElems.empty();
        for (const auto& value : values_) {
            const auto type=ExprHelper::canonicalResultTypeName(value.typeName);
            if (ordinary && (type == "regtype[]" || type == "regclass[]")) {
                if (value.isNull) { key += "N;"; continue; }
                const auto material = owningReferenceArrayDistinctKey(
                    value, type == "regtype[]" ? 2206U : 2205U);
                key += "V" + std::to_string(material.size()) + ":" + material;
                continue;
            }
            if (ordinary && supportsPreparedArrayOrder(type)) {
                if (value.isNull) {
                    key += "N;";
                } else {
                    // Keep the position of the array in the framed row key.
                    // The typed array is compared separately, including its bounds.
                    key += "A;";
                    arrays.push_back(value);
                }
                continue;
            }
            if(type=="oid" || stored_regtype_detail::scalarType(type) || type=="name" || type=="\"char\"") {
                if(value.isNull){key+="N;";continue;}
                std::string material;
                if(type=="oid" || stored_regtype_detail::scalarType(type)) {
                    ExprEvaluator evaluator;
                    const auto oid=stored_regtype_detail::scalarType(type)?evaluator.coerceComparison(ExprEvaluator::resolveComparison("=",type,type),value,true):value;
                    material="oid:"+std::to_string(oid.objectOid?*oid.objectOid:preparedOidValue(oid.value));
                }
                else {
                    auto binding=ExprEvaluator::resolveComparison("=",type,type);
                    if(type=="name")binding.collation=value.collation.empty()?"C":value.collation;
                    material=ExprEvaluator::comparisonHashKey(binding,value,true);
                }
                key+="V"+std::to_string(material.size())+":"+material;
                continue;
            }
            Column column;
            const auto error = TypeRegistry::instance().resolveColumnType(column, value.typeName, {}, false);
            if (!error.empty()) throw DbError("0A000", "unsupported prepared DISTINCT type: " + value.typeName);
            column.collation = value.collation;
            key += StorageEngine::groupingValueKey(column, value.value, value.isNull);
        }
        return key;
    }
    void close() override {
        const auto restore = [&]() noexcept {
            if (callerNullBinding_) {
                StorageEngine::restoreNullRowBinding(std::move(*callerNullBinding_));
                callerNullBinding_.reset();
            }
        };
        values_.clear();std::exception_ptr failure;
        // A never-opened metadata graph owns no live child execution. Do
        // not consume its freshly prepared cursor before the first restart.
        if(childOpened_ && state_->execution && !state_->executionProvider)
            try {state_->execution->closeChildCursors();}catch(...){failure=std::current_exception();}
        try { if (childOpened_) child_->close(); }
        catch (...) {if(!failure)failure=std::current_exception();}
        childOpened_ = false;
        restore();
        if(failure)std::rethrow_exception(failure);
    }
    std::string preparedPlanNodeName() const override { return "TypedProject"; }
    std::vector<Operator*> preparedPlanChildren() const override {
        // WHERE already exposes these actual child graphs at its filter.
        std::vector<Operator*> plans;
        if(state_->execution)for(const auto* target:state_->targets) {
            auto children=state_->execution->childPlans(target);
            plans.insert(plans.end(),children.begin(),children.end());
        }
        plans.insert(plans.begin(),child_.get());return plans;
    }
    bool supportsPreparedOuterRow() const override { return state_->parameterized(); }
    bool bindPreparedOuterRow(const RowContext& row) override {
        if (!supportsPreparedOuterRow()) return false;
        if (state_->sourceContexts && (!state_->logicalSource ||
            !state_->logicalSource->bindPreparedOuterRow(row))) return false;
        state_->outerRow=row;return true;
    }
};

// Limit consumes the real projection stream, including its boundary probe
// and OFFSET sites. Its peer test reads the sort's already evaluated typed
// key slots; it never reexecutes a target, child query or volatile sort key.
class PreparedTiesLimitOp final : public Operator {
    OpPtr child_;
    PreparedSortOp* sort_;
    size_t limit_,emitted_=0;
    std::vector<ExprValue> boundary_;
    bool opened_=false,done_=false;
public:
    PreparedTiesLimitOp(OpPtr child,PreparedSortOp* sort,size_t limit)
        :child_(std::move(child)),sort_(sort),limit_(limit) {
        if(!sort_)throw DbError("XX000","prepared WITH TIES lost its actual sort owner");
    }
    ~PreparedTiesLimitOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();emitted_=0;boundary_.clear();done_=!limit_;
        if(done_)return true;
        opened_=true;return child_->open() || propagateChildError(child_.get(),"prepared ties child open failed");
    }
    bool next(std::string& row) override {
        NextInstrument instrument(this);checkForQueryInterrupt();
        if(done_)return false;
        if(!child_->next(row)) {
            done_=true;return child_->hasError()?propagateChildError(child_.get(),"prepared ties child failed"):false;
        }
        const auto& keys=sort_->keys();
        if(emitted_>=limit_ && !sort_->peers(keys,boundary_)){done_=true;return false;}
        if(++emitted_==limit_)boundary_=keys;
        instrument.emitted=true;return true;
    }
    void close() override {if(opened_){opened_=false;child_->close();}boundary_.clear();emitted_=0;done_=false;}
    bool supportsStructuredRows() const override {return child_->supportsStructuredRows();}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {return child_->lastStructuredValues(row);}
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {return child_->lastStructuredRow(cells,nulls);}
    bool lastColumnIsNull(size_t ordinal) const override {return child_->lastColumnIsNull(ordinal);}
    bool supportsPreparedContexts() const override {return child_->supportsPreparedContexts();}
    bool lastPreparedContext(RowContext& row) const override {return child_->lastPreparedContext(row);}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {return child_->bindPreparedOuterRow(row);}
    std::string preparedPlanNodeName() const override {return "LimitWithTies";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
};

// A negative FETCH input is syntactically and statically typed valid. Keep
// its actual Limit demand site: constructing/describing a graph is pure,
// and an outer zero/false/dead branch never opens this child.
class PreparedNegativeFetchOp final : public Operator {
    OpPtr child_;
public:
    explicit PreparedNegativeFetchOp(OpPtr child):child_(std::move(child)) {}
    bool open() override {throw DbError("2201W","FETCH count must not be negative");}
    bool next(std::string&) override {throw DbError("2201W","FETCH count must not be negative");}
    void close() override {}
    bool supportsStructuredRows() const override {return child_->supportsStructuredRows();}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {return child_->bindPreparedOuterRow(row);}
    std::string preparedPlanNodeName() const override {return "Limit";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
};

// Invalid literal row counts fail when their real Limit is opened. The
// unopened producer keeps the existing query/parameter/cursor ownership.
class PreparedRowCountErrorOp final : public Operator {
    OpPtr child_;
    const char* state_;
    const char* message_;
public:
    PreparedRowCountErrorOp(OpPtr child,const char* state,const char* message)
        :child_(std::move(child)),state_(state),message_(message) {}
    bool open() override {throw DbError(state_,message_);}
    bool next(std::string&) override {throw DbError(state_,message_);}
    void close() override {}
    bool supportsStructuredRows() const override {return child_->supportsStructuredRows();}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {return child_->bindPreparedOuterRow(row);}
    std::string preparedPlanNodeName() const override {return "Limit";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
};

class PreparedDistinctOp final : public Operator {
    OpPtr child_;
    struct Row {
        std::string text;
        std::vector<std::string> cells;
        std::vector<bool> nulls;
        std::vector<ExprValue> values;
    };
    using ArrayKey = std::pair<std::string, std::vector<ExprValue>>;
    struct ArrayKeyLess {
        bool operator()(const ArrayKey& a, const ArrayKey& b) const {
            if (a.first != b.first) return a.first < b.first;
            for (size_t i = 0; i < std::min(a.second.size(), b.second.size()); ++i) {
                const int comparison = ExprEvaluator::compareArrayValues(a.second[i], b.second[i]);
                if (comparison) return comparison < 0;
            }
            return a.second.size() < b.second.size();
        }
    };
    std::vector<Row> rows_;
    size_t position_ = 0;
public:
    explicit PreparedDistinctOp(OpPtr child) : child_(std::move(child)) {}
    bool open() override {
        OpenInstrument startup(this);
        clearError(); rows_.clear(); position_ = 0;
        if (!child_->open()) return false;
        std::set<std::string> seen;
        std::set<ArrayKey, ArrayKeyLess> seenArrays;
        auto* project = dynamic_cast<PreparedProjectOp*>(child_.get());
        Row row;
        while (child_->next(row.text)) {
            if (!project || !child_->lastStructuredRow(row.cells, row.nulls) ||
                !child_->lastStructuredValues(row.values))
                throw DbError("XX000", "prepared DISTINCT lost typed projection");
            std::vector<ExprValue> arrays;
            auto key = project->typedKey(arrays);
            const bool first = arrays.empty() ? seen.insert(std::move(key)).second
                : seenArrays.emplace(std::move(key), std::move(arrays)).second;
            if (first) rows_.push_back(row);
        }
        if (child_->hasError()) return propagateChildError(child_.get(), "prepared DISTINCT child failed");
        return true;
    }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        if (position_ == rows_.size()) return false;
        row = rows_[position_++].text; instrumentation.emitted = true; return true;
    }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        if (!position_ || position_ > rows_.size()) return false;
        values = rows_[position_ - 1].values;
        return true;
    }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        if (!position_ || position_ > rows_.size()) return false;
        cells = rows_[position_ - 1].cells; nulls = rows_[position_ - 1].nulls; return true;
    }
    void close() override { rows_.clear(); position_ = 0; child_->close(); }
    std::string preparedPlanNodeName() const override { return "TypedDistinct"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
    bool supportsPreparedOuterRow() const override { return child_->supportsPreparedOuterRow(); }
    bool bindPreparedOuterRow(const RowContext& row) override { return child_->bindPreparedOuterRow(row); }
};
} // namespace

namespace {
class MaterializedProviderCursor final : public QueryHostValueCursor {
    std::vector<ExprValue> rows_;
    size_t position_=0;
public:
    explicit MaterializedProviderCursor(std::vector<ExprValue> rows):rows_(std::move(rows)){}
    bool next(ExprValue& value) override {
        if(position_==rows_.size())return false;
        value=rows_[position_++];return true;
    }
};
// This is an actual ProjectSet execution node. Array expressions are evaluated
// once on the qualifying input, then their declared cells are emitted lazily.
// No SQL is rendered/reexecuted to obtain a row or an output descriptor.
class PreparedSetReturningOp final : public Operator {
    std::shared_ptr<PreparedQuery> query_;
    SelectStmt* select_;
    PreparedQueryExecution execution_;
    RowContext outer_;
    std::string database_;
    PreparedSetReturningReader providerReader_;
    std::vector<const FunctionCallExpr*> calls_;
    std::vector<std::unique_ptr<QueryHostValueCursor>> providerCursors_;
    std::vector<ExprValue> values_;
    size_t position_=0;
    bool initialized_=false,qualified_=false,ended_=false,opened_=false;
    std::optional<StorageEngine::NullRowBindingState> callerNullBinding_;
public:
    PreparedSetReturningOp(StorageEngine* engine,const std::string& database,
        std::shared_ptr<PreparedQuery> query,SelectStmt* select,const RowContext& outer,
        PreparedChildExecutor reader,PreparedChildCursorFactory cursor,bool planRootConstants,
        PreparedSetReturningReader providerReader)
        :query_(std::move(query)),select_(select),
         execution_(query_,engine,database),outer_(outer),database_(database),providerReader_(std::move(providerReader)) {
        execution_.setQueryExecutor(std::move(reader));
        execution_.setChildCursorFactory(std::move(cursor));
        if(planRootConstants)execution_.planStatementConstants(select_);
        for(auto& target:select_->selectList) {
            const auto* call=dynamic_cast<const FunctionCallExpr*>(target.expr.get());
            calls_.push_back(call && call->setReturning?call:nullptr);
            if(calls_.back()) {
                (void)queryHostSetReturningProvider(*call->setReturning);
                for(const auto& argument:call->args) {
                    if(planRootConstants)execution_.planExpressionConstants(argument.get());
                    execution_.prepareExpression(argument.get());
                }
            } else execution_.prepareExpression(target.expr.get());
        }
        providerCursors_.resize(calls_.size());
        execution_.prepareExpression(select_->whereClause.get());
        execution_.prepareChildCursors();
    }
    bool open() override {
        OpenInstrument instrument(this);values_.clear();position_=0;
        for(auto& cursor:providerCursors_)cursor.reset();
        callerNullBinding_=StorageEngine::captureNullRowBinding();
        initialized_=false;qualified_=false;ended_=false;opened_=true;return true;
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        checkForQueryInterrupt();
        if(ended_)return false;
        const bool tableSource = select_->functionSourceProducer;
        const auto context = tableSource && initialized_ ? RowContext{} : execution_.context(outer_);
        if(!qualified_) {
            qualified_=true;
            if(select_->whereClause) {
                const auto filter=execution_.evaluate(select_->whereClause.get(),context);
                if(filter.isNull || !filter.asBool()){ended_=true;return false;}
            }
        }
        bool hasSetRow=false;values_.clear();display.clear();
        for(size_t i=0;i<calls_.size();++i) {
            if(const auto* call=calls_[i]) {
                if(!initialized_) {
                    std::vector<ExprValue> arguments;
                    for(const auto& argument:call->args)arguments.push_back(execution_.evaluate(argument.get(),context));
                    const auto& binding=*call->setReturning;
                    const auto& provider=queryHostSetReturningProvider(binding);
                    if(provider.open && !providerReader_)
                        providerCursors_[i]=provider.open(arguments,binding,currentSession(),database_);
                    else {
                        if(!providerReader_ && !provider.read)
                            throw DbError("XX000","set-returning receiver has no cursor factory");
                        auto rows=providerReader_?providerReader_(binding,arguments):provider.read(arguments,currentSession(),database_);
                        for(const auto& cell:rows)
                            if(cell.typeName != binding.elementType && ExprHelper::canonicalResultTypeName(cell.typeName)!=
                               ExprHelper::canonicalResultTypeName(binding.elementType))
                                throw DbError("XX000","set-returning receiver lost its declared element type");
                        providerCursors_[i]=std::make_unique<MaterializedProviderCursor>(std::move(rows));
                    }
                    if(!providerCursors_[i])throw DbError("XX000","set-returning receiver lost its cursor");
                }
                ExprValue cell;
                if(providerCursors_[i] && providerCursors_[i]->next(cell)) {
                    if(cell.typeName != call->setReturning->elementType && ExprHelper::canonicalResultTypeName(cell.typeName)!=
                       ExprHelper::canonicalResultTypeName(call->setReturning->elementType))
                        throw DbError("XX000","set-returning cursor changed its declared element type");
                    hasSetRow=true;values_.push_back(std::move(cell));
                } else {
                    providerCursors_[i].reset();
                    values_.emplace_back(call->setReturning->elementType,"",true);
                }
            } else if (tableSource && initialized_) {
                // A scalar FROM function contributes exactly one row, even
                // when its result is NULL, then pads a longer member's rows.
                const auto& output = query_->statementOutputs.at(select_);
                values_.emplace_back(output[i].type, "", true);
            } else {
                // Ordinary targets are evaluated on each ProjectSet attempt,
                // including its terminal attempt. WHERE false/LIMIT 0 never
                // reaches this phase. Genuine PG18 effect controls preserve
                // that distinction even when the SRF emits no row.
                auto value=execution_.evaluate(select_->selectList[i].expr.get(),context);
                if(value.typeName=="unknown")value.typeName="text";
                values_.push_back(std::move(value));
                hasSetRow = hasSetRow || tableSource;
            }
        }
        initialized_=true;
        if(!hasSetRow){ended_=true;values_.clear();return false;}
        ++position_;
        // The FROM receiver reads owning typed cells through its cursor.
        if (!tableSource)
            for (const auto& value : values_) display += value.isNull ? "NULL " : value.value + " ";
        instrument.emitted=true;return true;
    }
    bool supportsStructuredRows() const override {return true;}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {
        if(values_.empty())return false;
        row=values_;return true;
    }
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        if(values_.empty())return false;
        cells.clear();nulls.clear();
        for(const auto& value:values_){cells.push_back(value.value);nulls.push_back(value.isNull);}return true;
    }
    void close() override {
        values_.clear();position_=0;initialized_=false;qualified_=false;ended_=false;
        for(auto& cursor:providerCursors_)cursor.reset();
        std::exception_ptr failure;
        if(opened_){opened_=false;try{execution_.closeChildCursors();}catch(...){failure=std::current_exception();}}
        if(callerNullBinding_) {
            StorageEngine::restoreNullRowBinding(std::move(*callerNullBinding_));
            callerNullBinding_.reset();
        }
        if(failure)std::rethrow_exception(failure);
    }
    std::string preparedPlanNodeName() const override {
        return select_->functionSourceProducer ? "Rows From" : "ProjectSet";
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        std::vector<Operator*> plans;
        for(size_t i=0;i<calls_.size();++i) {
            const auto append=[&](const Expr* expression){auto children=execution_.childPlans(expression);plans.insert(plans.end(),children.begin(),children.end());};
            if(calls_[i])for(const auto& argument:calls_[i]->args)append(argument.get());
            else append(select_->selectList[i].expr.get());
        }
        auto filters=execution_.childPlans(select_->whereClause.get());
        plans.insert(plans.end(),filters.begin(),filters.end());return plans;
    }
    bool supportsPreparedOuterRow() const override {return select_->ctes.empty();}
    bool bindPreparedOuterRow(const RowContext& row) override {
        if(!supportsPreparedOuterRow())return false;outer_=row;return true;
    }
};
}

static bool supportsPreparedSelectShape(const SelectStmt& select, bool allowCtes, bool sourceContexts = false,
                                       bool selectBodyOnly = false) {
    if (select.command != SqlCommand::Select || (!allowCtes && !select.ctes.empty()) ||
        (!selectBodyOnly && (select.setOp != SetOp::None || select.setOpLhs || select.setOpRhs)) ||
        (selectBodyOnly && (select.setOp == SetOp::None || select.setOpLhs || !select.setOpRhs)) ||
        !select.groupBy.empty() || !select.groupByElems.empty() ||
        !select.distinctOn.empty() ||
        !select.locking.empty() || (select.withTies && ((select.distinct && !select.existsTargetPrunable) || selectBodyOnly))) return false;
    if (!sourceContexts && select.fromClause && select.fromClause->type != FromItem::Type::Table &&
        select.fromClause->type != FromItem::Type::Subquery) return false;
    std::function<bool(const Expr*)> scalar = [&](const Expr* expr) {
        if (!expr) return true;
        if (auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
            if(call->setReturning)return false;
            // A reached window call is lowered to an actual computed result
            // by the shared window pipeline, never evaluated as a scalar.
            if(call->hasOver)return true;
            static const std::set<std::string> aggregates = {"count", "sum", "avg", "min", "max", "string_agg", "array_agg", "bool_and", "bool_or", "every", "bit_and", "bit_or", "bit_xor", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg", "xmlagg", "mode", "percentile_cont", "percentile_disc", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp", "corr", "covar_pop", "covar_samp"};
            CatalogManager::QualifiedName routine;
            const auto spelling = call->schema.empty() ? call->funcName : call->schema + "." + call->funcName;
            const bool builtinRole = CatalogManager::parseQualifiedName(spelling, routine, true) &&
                (routine.schema.empty() || routine.schema == "pg_catalog");
            if (builtinRole && routine.name == "exists")
                return call->schema.empty() && call->args.size() == 1 && call->args.front()->preparedSubquery;
            if (!aggregate_type_detail::builtinName(call).empty()) {
                // The AST still stores inside-call ORDER BY as opaque text.
                // Do not accept that clause and silently discard its role.
                if (!call->orderBy.empty()) return false;
                for (const auto& argument : call->args) if (!scalar(argument.get())) return false;
                for (const auto& key : call->aggregateOrderKeys) if (!scalar(key.expr.get())) return false;
                if (!scalar(call->filter.get())) return false;
                return true;
            }
            if (call->hasOver || call->distinct || call->filter || !call->orderBy.empty() ||
                (builtinRole && aggregates.count(routine.name))) return false;
            for (const auto& argument : call->args) if (!scalar(argument.get())) return false;
            for (const auto& argument : call->namedArgs) if (!scalar(argument.value.get())) return false;
        } else if (auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) return scalar(unary->operand.get());
        else if (auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) return scalar(binary->left.get()) && scalar(binary->right.get());
        else if (auto* cast = dynamic_cast<const CastExpr*>(expr)) return scalar(cast->operand.get());
        else if (auto* conditional = dynamic_cast<const CaseExpr*>(expr)) {
            if (!scalar(conditional->switchExpr.get()) || !scalar(conditional->elseExpr.get())) return false;
            for (const auto& arm : conditional->whenClauses) if (!scalar(arm.first.get()) || !scalar(arm.second.get())) return false;
        }
        return true;
    };
    for (const auto& target : select.selectList) if (!scalar(target.expr.get())) return false;
    for (const auto& key : select.orderBy) if (!scalar(key.expr.get()) || !key.usingOp.empty()) return false;
    return scalar(select.whereClause.get()) && scalar(select.having.get());
}

bool QueryPlanner::usesDeclaredFilterCollations(const SelectStmt& select, const TableSchema& schema) {
    if (!select.whereClause || !select.fromClause || select.fromClause->type != FromItem::Type::Table)
        return false;
    const Column* first = nullptr;
    bool mixedSchema = false;
    for (const auto& column : schema.cols) {
        if (!hasColumnCollation(column)) continue;
        if (!first) first = &column;
        else if (first->collation != column.collation) { mixedSchema = true; break; }
    }
    if (!mixedSchema) return false;

    std::string firstIdentity;
    const auto& aliases = select.fromClause->columnAliases;
    const auto inspect = [&](const auto& walk, const Expr* expression) -> bool {
        if (!expression || expression->preparedSubquery) return false;
        if (const auto* reference = dynamic_cast<const ColumnRefExpr*>(expression)) {
            for (size_t i = 0; i < schema.len(); ++i) {
                if ((i < aliases.size() ? aliases[i] : schema.cols[i].dataName) != reference->column) continue;
                if (!hasColumnCollation(schema.cols[i])) return false;
                auto identity = declaredColumnCollation(schema.cols[i]).identity;
                if (identity.empty()) identity = "pg_catalog.default";
                if (!firstIdentity.empty()) return firstIdentity != identity;
                firstIdentity = std::move(identity);
                break;
            }
        } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
            return walk(walk, binary->left.get()) ||
                (binary->op != "::" && walk(walk, binary->right.get()));
        } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) {
            return walk(walk, unary->operand.get());
        } else if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
            return walk(walk, cast->operand.get());
        } else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
            for (const auto& argument : call->args)
                if (walk(walk, argument.get())) return true;
        } else if (const auto* row = dynamic_cast<const RowExpr*>(expression)) {
            for (const auto& element : row->elements)
                if (walk(walk, element.get())) return true;
        } else if (const auto* conditional = dynamic_cast<const CaseExpr*>(expression)) {
            if (walk(walk, conditional->switchExpr.get()) || walk(walk, conditional->elseExpr.get())) return true;
            for (const auto& arm : conditional->whenClauses)
                if (walk(walk, arm.first.get()) || walk(walk, arm.second.get())) return true;
        }
        return false;
    };
    const auto checkConjuncts = [&](const auto& walk, const Expr* expression) -> bool {
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
            binary && (binary->op == "AND" || binary->op == "OR"))
            return walk(walk, binary->left.get()) || walk(walk, binary->right.get());
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression);
            unary && unary->op == "NOT")
            return walk(walk, unary->operand.get());
        firstIdentity.clear();
        return inspect(inspect, expression);
    };
    return checkConjuncts(checkConjuncts, select.whereClause.get());
}

bool QueryPlanner::supportsPreparedSelectPlan(const SelectStmt& select) {
    return supportsPreparedSelectShape(select, false);
}
bool QueryPlanner::supportsPreparedSourceSelectPlan(const SelectStmt& select, bool selectBodyOnly) {
    return supportsPreparedSelectShape(select, true, true, selectBodyOnly);
}

static bool preparedAggregateRole(const FunctionCallExpr* call, const PreparedSelectState& state) {
    return !aggregate_type_detail::builtinName(call).empty() &&
        !state.evaluator.hasScalarFunction(call, state.engine);
}

static void visitPreparedAggregateExpression(Expr* expression,
    const std::function<bool(Expr*)>& visit) {
    if (!expression || expression->preparedSubquery || !visit(expression)) return;
    const auto child = [&](Expr* value) { visitPreparedAggregateExpression(value, visit); };
    if (auto* call = dynamic_cast<FunctionCallExpr*>(expression)) {
        for (auto& argument : call->args) child(argument.get());
        for (auto& argument : call->namedArgs) child(argument.value.get());
        for (auto& key : call->aggregateOrderKeys) child(key.expr.get());
        child(call->filter.get());
    } else if (auto* unary = dynamic_cast<UnaryOpExpr*>(expression)) child(unary->operand.get());
    else if (auto* binary = dynamic_cast<BinaryOpExpr*>(expression)) {
        child(binary->left.get()); if (binary->op != "::") child(binary->right.get());
    } else if (auto* cast = dynamic_cast<CastExpr*>(expression)) child(cast->operand.get());
    else if (auto* conditional = dynamic_cast<CaseExpr*>(expression)) {
        child(conditional->switchExpr.get()); child(conditional->elseExpr.get());
        for (auto& arm : conditional->whenClauses) { child(arm.first.get()); child(arm.second.get()); }
    } else if (auto* array = dynamic_cast<ArrayExpr*>(expression))
        for (auto& value : array->elements) child(value.get());
    else if (auto* row = dynamic_cast<RowExpr*>(expression))
        for (auto& value : row->elements) child(value.get());
}

static void prepareAggregateRoles(PreparedSelectState& state) {
    // Role preparation precedes physical-source lowering. Producer equality
    // uses the already analyzed range/ordinal identities, never the not-yet
    // populated physical schema/sourceOrdinal used by the later sort operator.
    const auto inputIdentity = [&](const Expr* value) {
        return ExprHelper::preparedSortExpressionIdentity(value, *state.query,
            [](const ColumnRefExpr& column) {
                if (!column.binding) throw DbError("XX000", "aggregate input has no analyzed source identity");
                const auto& binding = *column.binding;
                return std::to_string(binding.scopeDepth) + ":" +
                    std::to_string(binding.sourceOrdinal) + ":" +
                    std::to_string(binding.columnOrdinal) + ":" + binding.declaredType + ":" +
                    (binding.mergedUsing ? "merged" : "ordinary");
            }, state.dbname, state.engine);
    };
    const auto forbidAggregate = [&](Expr* root) {
        visitPreparedAggregateExpression(root, [&](Expr* expression) {
            if (const auto* call = dynamic_cast<FunctionCallExpr*>(expression);
                call && preparedAggregateRole(call, state))
                throw DbError("42803", "aggregate functions are not allowed in this input context");
            return true;
        });
    };
    forbidAggregate(state.select().whereClause.get());
    const auto gather = [&](Expr* root) {
        visitPreparedAggregateExpression(root, [&](Expr* expression) {
            auto* call = dynamic_cast<FunctionCallExpr*>(expression);
            if (!call || !preparedAggregateRole(call, state)) return true;
            if (std::any_of(state.aggregates.begin(), state.aggregates.end(),
                    [&](const auto& aggregate) { return aggregate.call == call; })) return false;
            const auto name = aggregate_type_detail::builtinName(call);
            const auto type = call->args.empty() ? std::string("unknown") :
                ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedInputType(
                    call->args.front().get(), {}, state.dbname, state.engine));
            const auto resultType = aggregate_type_detail::resultType(call, name, type);
            for (const auto& argument : call->args) forbidAggregate(argument.get());
            for (const auto& key : call->aggregateOrderKeys) forbidAggregate(key.expr.get());
            forbidAggregate(call->filter.get());
            std::vector<size_t> keyOwners;
            std::vector<std::string> keyIdentities;
            const auto argumentIdentity = call->aggregateOrderKeys.empty() || call->args.empty()
                ? std::string{} : inputIdentity(call->args.front().get());
            for (const auto& key : call->aggregateOrderKeys) {
                const auto identity = inputIdentity(key.expr.get());
                if (call->distinct && identity != argumentIdentity)
                    throw DbError("42P10", "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list");
                size_t owner = PreparedSelectState::noTarget;
                if (identity == argumentIdentity) owner = PreparedSelectState::noTarget - 1;
                else for (size_t i = 0; i < keyIdentities.size(); ++i)
                    if (identity == keyIdentities[i]) { owner = i; break; }
                keyOwners.push_back(owner);
                keyIdentities.push_back(identity);
            }
            state.aggregates.push_back({call, name, type, resultType, {}, std::move(keyOwners)});
            return false;
        });
    };
    for (auto& target : state.select().selectList) gather(target.expr.get());
    gather(state.select().having.get());
    for (auto& key : state.select().orderBy) gather(key.expr.get());
    if (state.aggregates.empty() && !state.select().having) return;
    const auto grouped = [&](Expr* root) {
        visitPreparedAggregateExpression(root, [&](Expr* expression) {
            if (auto* call = dynamic_cast<FunctionCallExpr*>(expression);
                call && preparedAggregateRole(call, state)) return false;
            if (const auto* column = dynamic_cast<ColumnRefExpr*>(expression);
                column && column->binding && !column->binding->scopeDepth)
                throw DbError("42803", "column must appear in GROUP BY or be used in an aggregate function");
            return true;
        });
    };
    for (auto& target : state.select().selectList) grouped(target.expr.get());
    grouped(state.select().having.get());
    for (auto& key : state.select().orderBy) grouped(key.expr.get());
}

static bool isSingleColumnPrimaryKey(const TableSchema&, const std::string&);
static std::vector<int64_t> lookupBtreeMultiChecked(BPTree*, const std::string&);

static std::string integerIndexCastType(const std::string& spelling) {
    CatalogManager::QualifiedName name;
    if (CatalogManager::parseQualifiedName(spelling, name) && name.schema == "pg_catalog") {
        if (name.name == "int2") return "smallint";
        if (name.name == "int4") return "integer";
        if (name.name == "int8") return "bigint";
    }
    const auto keyword = SQLParser::toLower(spelling);
    if (keyword == "smallint" || keyword == "integer" || keyword == "bigint") return keyword;
    if (keyword == "int") return "integer";
    return {};
}

static bool integerIndexLiteralTree(const Expr* expression) {
    if (!expression || expression->preparedSubquery) return false;
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expression)) {
        const auto type = SQLParser::toLower(literal->typeName);
        return type.empty() || type == "unknown" || type == "text" ||
            type == "smallint" || type == "integer" || type == "bigint";
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        return cast->typeMods.empty() && !integerIndexCastType(cast->typeName).empty() &&
            integerIndexLiteralTree(cast->operand.get());
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression))
        return (unary->op == "+" || unary->op == "-") && integerIndexLiteralTree(unary->operand.get());
    return false;
}

// Wire Bind inputs can reach the legacy planner as CAST(literal AS
// "pg_catalog"."int4"). Decode only a structural builtin constant, never
// a parameter, routine or subquery. Errors retain their original predicate
// demand: an unusable input simply keeps the existing heap/filter path.
static std::optional<ExprValue> constantIntegerIndexInput(const Expr* expression,
                                                         const std::string& type) {
    const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
    if ((!dynamic_cast<const CastExpr*>(expression) &&
         !(literal && literal->analyzedPrimitiveConstant)) || !integerIndexLiteralTree(expression))
        return std::nullopt;
    try {
        ExprEvaluator evaluator;
        auto value = evaluator.eval(expression, RowContext{});
        if (value.isNull || ExprHelper::canonicalResultTypeName(value.typeName) != type)
            return std::nullopt;
        return value;
    } catch (const DbError&) {
        return std::nullopt;
    }
}

static std::vector<size_t> compositePrimaryColumns(const TableSchema& schema) {
    if (!primary_key_index::isCompoundKey(schema)) return {};
    if (!schema.pkColIndices.empty()) return schema.pkColIndices;
    std::vector<size_t> result;
    for (size_t i = 0; i < schema.len(); ++i)
        if (schema.cols[i].isPrimaryKey) result.push_back(i);
    return result;
}

static std::string compositeEqualityType(const Column& column) {
    if (column.isArray || column.isUnsigned || column.generatedKind ||
        !column.domainName.empty() || !column.enumValues.empty()) return {};
    if (!column.collation.empty()) {
        const auto& rule = column.resolvedCollation.empty() ? column.collation : column.resolvedCollation;
        if (!column.resolvedCollationIsBinary || column.resolvedCollationUsesLocale ||
            !collation::isBinary(rule)) return {};
    }
    const auto type = ExprHelper::canonicalResultTypeName(column.dataType);
    if (type == "text" || type == "character varying") return "text";
    if (type == "smallint" || type == "integer" || type == "bigint") return type;
    return {};
}

static std::optional<std::string> compositeEqualityValue(
        const std::string& type, const std::string& input) {
    if (type == "text") return input;
    const auto* begin = input.data();
    const auto* end = begin + input.size();
    if (begin != end && *begin == '+') {
        if (++begin == end || *begin < '0' || *begin > '9') return {};
    }
    int64_t number = 0;
    const auto parsed = std::from_chars(begin, end, number);
    if (parsed.ec != std::errc{} || parsed.ptr != end ||
        (type == "smallint" && (number < INT16_MIN || number > INT16_MAX)) ||
        (type == "integer" && (number < INT32_MIN || number > INT32_MAX))) return {};
    return std::to_string(number);
}

struct CompositeEqualityInput {
    size_t column;
    std::string name;
    std::string type;
    std::string value;
    std::optional<size_t> slot;
};

struct CompositeSuffixBound {
    CompositeEqualityInput input;
    bool lower;
    bool inclusive;
};

enum class StoredComparison { Equal, NotEqual, Less, LessEqual, Greater, GreaterEqual };

struct StoredIntegerField {
    size_t offset, width;

    bool operator==(const StoredIntegerField& other) const {
        return offset == other.offset && width == other.width;
    }

    std::optional<int64_t> read(std::string_view row) const {
        if (offset > row.size() || width > row.size() - offset) return {};
        int64_t value = 0;
        std::memcpy(&value, row.data() + offset, width);
        // Use the same physical encoding as extractColumnValueStatic.
        if (width == 4 && (value & 0x80000000LL)) value |= 0xFFFFFFFF00000000LL;
        if (width == 2 && (value & 0x8000LL)) value |= 0xFFFFFFFFFFFF0000LL;
        return value;
    }
};

static std::optional<StoredIntegerField> storedIntegerField(const TableSchema& schema, size_t ordinal) {
    const auto& column = schema.cols[ordinal];
    const auto type = compositeEqualityType(column);
    const size_t width = type == "smallint" ? sizeof(int16_t) : type == "integer" ? sizeof(int32_t) :
        type == "bigint" ? sizeof(int64_t) : 0;
    if (!width || column.isVariableLength || column.dsize != width ||
        column.storageCodec != Column::StorageCodec::Legacy) return {};
    size_t offset = 0;
    for (size_t i = 0; i < ordinal; ++i)
        if (!schema.cols[i].isVariableLength) offset += schema.cols[i].dsize;
    return StoredIntegerField{offset, width};
}

using IntegerInterval = std::pair<int64_t, int64_t>;

static IntegerInterval storedIntegerLimits(size_t width) {
    if (width == sizeof(int16_t)) return {INT16_MIN, INT16_MAX};
    if (width == sizeof(int32_t)) return {INT32_MIN, INT32_MAX};
    return {INT64_MIN, INT64_MAX};
}

static StorageEngine::IntegerPartitionFilter outsideIntegerInterval(int64_t lower, int64_t upper) {
    if (lower > upper) return {{{INT64_MIN, INT64_MAX}}, false};
    StorageEngine::IntegerPartitionFilter result;
    if (lower != INT64_MIN) result.intervals.emplace_back(INT64_MIN, lower - 1);
    if (upper != INT64_MAX) result.intervals.emplace_back(upper + 1, INT64_MAX);
    return result;
}

struct StoredPredicate {
    enum class Input { IsNull, IsNotNull, Text, Integer, TextLength, IntegerOutsideRange, IntegerRanges };
    size_t column;
    std::string name, type;
    Input input;
    StoredComparison comparison = StoredComparison::Equal;
    std::variant<std::string, int64_t, IntegerInterval, StorageEngine::IntegerPartitionFilter> constant;
    std::optional<StoredIntegerField> integer = std::nullopt;
};

using StoredPredicates = std::vector<StoredPredicate>;

static bool supportsStoredHeapPredicate(const TableSchema& schema) {
    return schema.formatVersion == DATA_FILE_FORMAT_VERSION &&
        schema.partitionType == TableSchema::PartitionType::None &&
        std::all_of(schema.cols.begin(), schema.cols.end(), [](const Column& column) {
            return !compositeEqualityType(column).empty();
        });
}

static bool storedPredicateSchemaMatches(const StoredPredicates& predicates, const TableSchema& schema) {
    return std::all_of(predicates.begin(), predicates.end(), [&](const auto& predicate) {
        if (predicate.column >= schema.len()) return false;
        const auto& column = schema.cols[predicate.column];
        return column.dataName == predicate.name && column.dataType == predicate.type &&
            !compositeEqualityType(column).empty() &&
            (!predicate.integer || storedIntegerField(schema, predicate.column) == predicate.integer);
    });
}

static bool matchesStoredComparison(StoredComparison comparison, int order) {
    switch (comparison) {
        case StoredComparison::Equal: return order == 0;
        case StoredComparison::NotEqual: return order != 0;
        case StoredComparison::Less: return order < 0;
        case StoredComparison::LessEqual: return order <= 0;
        case StoredComparison::Greater: return order > 0;
        case StoredComparison::GreaterEqual: return order >= 0;
    }
    return false;
}

static std::optional<bool> storedPredicatesMatch(const StoredPredicates& predicates,
        StorageEngine& engine, const std::string& database, const TableSchema& schema,
        const std::string& row, const std::vector<bool>& nulls) {
    if (predicates.empty()) return true;
    using Input = StoredPredicate::Input;
    std::optional<detail::ScopedStoredRowNullBitmap> bitmap;
    for (const auto& predicate : predicates) {
        const bool isNull = nulls[predicate.column];
        if (predicate.input == Input::IsNull || predicate.input == Input::IsNotNull) {
            if (isNull != (predicate.input == Input::IsNull)) return false;
            continue;
        }
        if (predicate.input == Input::IntegerRanges) {
            const auto& filter = std::get<StorageEngine::IntegerPartitionFilter>(predicate.constant);
            if (isNull) {
                if (!filter.matchesNull) return false;
                continue;
            }
            const auto actual = predicate.integer->read(row);
            if (!actual) return {};
            const auto found = std::lower_bound(filter.intervals.begin(), filter.intervals.end(), *actual,
                [](const IntegerInterval& interval, int64_t value) { return interval.second < value; });
            if (found == filter.intervals.end() || *actual < found->first) return false;
            continue;
        }
        if (isNull) return false;
        if (predicate.integer) {
            const auto actual = predicate.integer->read(row);
            if (!actual) return {};
            if (predicate.input == Input::IntegerOutsideRange) {
                const auto [lower, upper] = std::get<IntegerInterval>(predicate.constant);
                if (*actual >= lower && *actual <= upper) return false;
                continue;
            }
            const auto expected = std::get<int64_t>(predicate.constant);
            if (!matchesStoredComparison(predicate.comparison, (*actual > expected) - (*actual < expected)))
                return false;
            continue;
        }
        if (!bitmap) bitmap.emplace(schema, nulls);
        const auto value = engine.extractColumnValue(row, schema, predicate.column, database, false);
        if (predicate.input == Input::Text) {
            if (!matchesStoredComparison(predicate.comparison, value.compare(std::get<std::string>(predicate.constant))))
                return false;
            continue;
        }
        int64_t actual = 0;
        if (predicate.input == Input::TextLength) {
            actual = static_cast<int64_t>(utf8CharCount(value));
        } else {
            const auto* end = value.data() + value.size();
            const auto parsed = std::from_chars(value.data(), end, actual);
            if (parsed.ec != std::errc{} || parsed.ptr != end) return {};
        }
        const auto expected = std::get<int64_t>(predicate.constant);
        if (!matchesStoredComparison(predicate.comparison, (actual > expected) - (actual < expected))) return false;
    }
    return true;
}

// These predicates have already been bound as total comparisons on one
// stored source. Track values that can make every conjunct true, with NULL
// as a separate possibility rather than an endpoint of the value range.
static bool storedPredicatesContradict(const TableSchema& schema, const StoredPredicates& where,
                                      const StoredPredicates& filter) {
    using Input = StoredPredicate::Input;
    using Range = StorageEngine::IntegerPartitionFilter;
    struct Requirements {
        bool mustBeNull = false, mustHaveValue = false;
        const std::string* text = nullptr;
        std::optional<Range> numbers;
    };
    std::map<size_t, Requirements> columns;
    auto apply = [&](const StoredPredicates& predicates) {
        for (const auto& predicate : predicates) {
            auto& rules = columns[predicate.column];
            if (predicate.input == Input::IsNull) {
                if (rules.mustHaveValue || !schema.cols[predicate.column].isNull) return false;
                rules.mustBeNull = true;
                continue;
            }
            if (predicate.input == Input::IntegerRanges) {
                const auto& range = std::get<Range>(predicate.constant);
                if (!rules.numbers) rules.numbers = Range{{storedIntegerLimits(predicate.integer->width)}, false};
                rules.numbers->intersect(range);
                if (!range.matchesNull || !schema.cols[predicate.column].isNull) {
                    if (rules.mustBeNull) return false;
                    rules.mustHaveValue = true;
                }
                if (rules.mustHaveValue && rules.numbers->intervals.empty()) return false;
                continue;
            }
            if (rules.mustBeNull) return false;
            rules.mustHaveValue = true;
            if (rules.numbers && rules.numbers->intervals.empty()) return false;
            if (predicate.input == Input::Text && predicate.comparison == StoredComparison::Equal) {
                const auto& text = std::get<std::string>(predicate.constant);
                if (rules.text && *rules.text != text) return false;
                rules.text = &text;
            } else if (predicate.integer || predicate.input == Input::TextLength) {
                if (!rules.numbers) {
                    const auto limits = predicate.integer
                        ? storedIntegerLimits(predicate.integer->width) : IntegerInterval{0, INT64_MAX};
                    rules.numbers = Range{{limits}, false};
                }
                static constexpr std::string_view operators[] = {"=", "<>", "<", "<=", ">", ">="};
                if (predicate.input == Input::IntegerOutsideRange) {
                    const auto [lower, upper] = std::get<IntegerInterval>(predicate.constant);
                    rules.numbers->intersect(outsideIntegerInterval(lower, upper));
                } else {
                    rules.numbers->intersect(*Range::comparison(operators[static_cast<size_t>(predicate.comparison)],
                        std::get<int64_t>(predicate.constant)));
                }
                if (rules.numbers->intervals.empty()) return false;
            }
        }
        return true;
    };
    if (!apply(where) || !apply(filter)) return true;
    const auto excludesText = [&](const StoredPredicates& predicates) {
        return std::any_of(predicates.begin(), predicates.end(), [&](const auto& predicate) {
            if (predicate.input != Input::Text || predicate.comparison != StoredComparison::NotEqual) return false;
            const auto* text = columns.at(predicate.column).text;
            return text && *text == std::get<std::string>(predicate.constant);
        });
    };
    return excludesText(where) || excludesText(filter);
}

struct CompositeSuffixOrder {
    bool ascending;
    std::optional<size_t> fetchCount;
    StoredPredicates filter;
};

static bool compositeOperandType(const std::string&, const std::string&);
static OpPtr prefilterPreparedHeap(StorageEngine&, const std::string&, const PreparedQuery&,
                                   const SelectStmt&, const TableSchema&, OpPtr&);

// Own one heap page while qualifying an ordered batch. The page lock and
// buffer pin are released before any SQL expression or TOAST lookup runs.
class CompositeHeapPage {
    LockManager& locks_;
    PageAllocator& allocator_;
    const std::string& database_;
    const TableSchema& schema_;
    StorageEngine::ReadView view_;
    alignas(PgPage::MAXALIGN) std::array<char, PgPage::PAGE_SIZE> bytes_;
    uint32_t pageId_ = 0;

    void load(uint32_t page) {
        if (!locks_.pageLockShared(database_, schema_.tablename, page))
            throw DbError("55P03", "could not obtain heap page lock");
        struct ReleasePage {
            LockManager& locks;
            PageAllocator& allocator;
            const std::string& database;
            const std::string& table;
            uint32_t page;
            bool pinned = false;
            ~ReleasePage() {
                if (pinned) allocator.unpinPage(page);
                locks.pageUnlock(database, table, page);
            }
        } release{locks_, allocator_, database_, schema_.tablename, page};
        const char* buffer = allocator_.fetchPage(page);
        if (!buffer) throw DbError("58030", "could not read indexed heap page");
        release.pinned = true;
        std::copy_n(buffer, bytes_.size(), bytes_.data());
        pageId_ = page;
    }

public:
    CompositeHeapPage(LockManager& locks, PageAllocator& allocator, const std::string& database,
                      const TableSchema& schema, const StorageEngine::ReadView& view)
        : locks_(locks), allocator_(allocator), database_(database), schema_(schema), view_(view) {}

    bool read(uint32_t page, uint16_t slot, std::string& row, std::vector<bool>& nulls) {
        if (page != pageId_) load(page);
        PageWrapper heap(bytes_.data(), bytes_.size(), schema_.formatVersion);
        const char* tuple = nullptr;
        size_t length = 0;
        if (!heap.read(slot, tuple, length)) return false;
        if (length < sizeof(HeapTupleHeaderData))
            throw DbError("XX001", "invalid indexed heap tuple header");
        if (!view_.isVisible(tuple, length, schema_.formatVersion)) return false;
        const auto* header = castHeapHeader(tuple);
        const size_t minimum = sizeof(HeapTupleHeaderData) +
            ((header->t_infomask & HEAP_HASNULL) ? (schema_.len() + 7) / 8 : 0);
        if (header->t_hoff < minimum || header->t_hoff >= length)
            throw DbError("XX001", "invalid indexed heap tuple header");
        row.assign(tuple + header->t_hoff, length - header->t_hoff);
        nulls.assign(schema_.len(), false);
        for (size_t column = 0; column < schema_.len(); ++column)
            if (schema_.cols[column].isNull) nulls[column] = dbms::isNull(header, static_cast<int>(column));
        return true;
    }
};

// The tree can only narrow a current snapshot, and several distinct full
// keys can share the same stored bytes. Keep the original SQL predicate
// above this source and verify the exact key before resolving the payload.
class CompositePrimaryKeyScanOp final : public Operator {
    StorageEngine* engine_;
    std::string database_, table_;
    uint64_t relationId_;
    std::vector<CompositeEqualityInput> inputs_;
    std::optional<size_t> suffix_;
    std::vector<CompositeSuffixBound> bounds_;
    std::optional<CompositeSuffixOrder> order_;
    std::shared_ptr<PreparedQuery> query_;
    const SelectStmt* fallbackSelect_;
    OpPtr fallback_;
    TableSchema schema_;
    std::map<std::string, std::string> values_;
    std::vector<int64_t> rids_;
    std::vector<int64_t> suffixValues_;
    std::vector<std::pair<int64_t, std::string>> prefixRows_;
    size_t position_ = 0, fetched_ = 0;
    int64_t lastRid_ = 0;
    std::optional<int64_t> lastOrderedValue_;
    bool locked_ = false, fallbackActive_ = false, usedIndex_ = false;
    bool opened_ = false;

    std::optional<std::string> inputValue(const CompositeEqualityInput& input) const {
        if (!input.slot) return input.value;
        if (!query_ || *input.slot >= query_->parameters.size()) return {};
        const auto& parameter = query_->parameters[*input.slot];
        if (parameter.isNull || !compositeOperandType(input.type, parameter.typeName)) return {};
        return compositeEqualityValue(input.type, parameter.value);
    }

    bool buildValues() {
        const auto columns = compositePrimaryColumns(schema_);
        if (schema_.physicalRelationId != relationId_ ||
            columns.size() != inputs_.size() + (suffix_ ? 1 : 0) ||
            (suffix_ && (columns.back() != *suffix_ ||
                         primary_key_index::orderedSuffixColumn(schema_) != suffix_)))
            return false;
        for (size_t i = 0; i < inputs_.size(); ++i) {
            const auto& input = inputs_[i];
            if (columns[i] != input.column || input.column >= schema_.len()) return false;
            const auto& column = schema_.cols[input.column];
            if (column.dataName != input.name || compositeEqualityType(column) != input.type)
                return false;
            const auto value = inputValue(input);
            if (!value) return false;
            values_.emplace(input.name, *value);
        }
        return true;
    }

    bool matchesCandidate(size_t candidate, const std::string& row,
                          std::optional<uint64_t> fence = std::nullopt) {
        const bool matches = std::all_of(inputs_.begin(), inputs_.end(), [&](const auto& input) {
            bool isNull = false;
            const auto actual = engine_->extractColumnValue(row, schema_, input.column,
                                                            database_, false, &isNull);
            return !isNull && actual == values_.at(input.name);
        });
        if (!matches) return false;
        if (order_) {
            bool isNull = false;
            const auto value = engine_->extractColumnValue(
                row, schema_, *suffix_, database_, false, &isNull);
            int64_t number = 0;
            const auto end = value.data() + value.size();
            const auto parsed = std::from_chars(value.data(), end, number);
            if (isNull || parsed.ec != std::errc{} || parsed.ptr != end ||
                number != suffixValues_[candidate] ||
                (lastOrderedValue_ && (order_->ascending
                    ? number <= *lastOrderedValue_ : number >= *lastOrderedValue_))) {
                if (fence && engine_->indexReadSnapshotFence(database_) != fence) return false;
                throw DbError("XX001", "composite primary key index order does not match the table");
            }
            lastOrderedValue_ = number;
        }
        return true;
    }

    bool fetchCandidate(size_t candidate, std::string& row,
                        std::optional<uint64_t> fence = std::nullopt) {
        checkForQueryInterrupt();
        const auto rid = rids_[candidate];
        bool failed = false;
        ++fetched_;
        if (!engine_->readIndexedRowByRid(database_, table_, rid, row, schema_, nullptr, &failed)) {
            if (failed) setError("indexed heap fetch failed");
            return false;
        }
        StorageEngine::bindNullRow(engine_, database_, table_, rid, schema_.len());
        return matchesCandidate(candidate, row, fence);
    }

    bool readIndexRange(BPTree& index, const std::string& first, const std::string& last,
                        size_t candidates, size_t maxNodes, BPTree::CompositeRangeRead mode,
                        size_t* visitedNodes = nullptr) {
        const auto status = index.readCompositePrimaryRange(first, last, candidates, maxNodes,
            rids_, order_ ? &suffixValues_ : nullptr, mode, visitedNodes);
        if (status == BPTree::LeafEntriesResult::Error)
            throw DbError("XX001", "could not read B-tree index for relation \"" + table_ + "\"");
        return status == BPTree::LeafEntriesResult::Complete;
    }

    bool bufferPrefix(BPTree& index, std::string first, std::string last, size_t count,
                      size_t batch, size_t maxNodes, double heapCost, uint64_t fence) {
        struct RestoreNullBinding {
            StorageEngine::NullRowBindingState saved = StorageEngine::captureNullRowBinding();
            ~RestoreNullBinding() { StorageEngine::restoreNullRowBinding(std::move(saved)); }
        } restore;
        // Certify the first page before the sort is allowed to consume it.
        // A digest collision or invisible tuple can leave the copied prefix
        // short of the required number of real matches.
        size_t budget = sortWorkMemBytes();
        const size_t rowSize = sizeof(decltype(prefixRows_)::value_type);
        std::optional<CompositeHeapPage> heap;
        if (!order_->filter.empty()) {
            auto* allocator = engine_->getPageAllocator(database_, table_);
            const auto* view = engine_->getCurrentReadView();
            if (!allocator || !view || schema_.formatVersion != DATA_FILE_FORMAT_VERSION ||
                allocator->pageSize() != PgPage::PAGE_SIZE || budget < PgPage::PAGE_SIZE) return false;
            heap.emplace(engine_->getLockManager(), *allocator, database_, schema_, *view);
            budget -= PgPage::PAGE_SIZE;
        }
        using Mode = BPTree::CompositeRangeRead;
        const auto mode = order_->ascending ? Mode::FirstAscending : Mode::FirstDescending;
        const auto& costs = QueryPlanner::costModel();
        const double tupleCost = costs.cpuIndexTupleCost + costs.cpuOperatorCost * order_->filter.size();
        constexpr size_t maxProbes = 8192, maxBatch = 512;
        std::vector<int64_t> overlap;
        int64_t lastSuffix = 0;
        uint32_t lastHeapPage = 0;
        double heapPageCost = 0;
        std::vector<bool> nulls;
        for (;;) {
            if (!maxNodes || engine_->indexReadSnapshotFence(database_) != fence) return false;
            const size_t requested = batch + overlap.size();
            size_t visited = 0;
            if (!readIndexRange(index, first, last, requested, maxNodes, mode, &visited) ||
                engine_->indexReadSnapshotFence(database_) != fence) return false;
            maxNodes -= visited;
            if (!overlap.empty() && (rids_.size() < overlap.size() ||
                suffixValues_[overlap.size() - 1] != lastSuffix ||
                !std::equal(overlap.begin(), overlap.end(), rids_.begin()))) return false;

            if (!fetched_) {
                const size_t capacity = std::min(count, rids_.size());
                if (capacity > budget / rowSize) return false;
                prefixRows_.reserve(capacity);
                if (prefixRows_.capacity() > budget / rowSize) return false;
                budget -= prefixRows_.capacity() * rowSize;
            }
            for (size_t candidate = overlap.size(); candidate < rids_.size() && prefixRows_.size() < count;
                 ++candidate) {
                std::string row;
                if (heap) {
                    checkForQueryInterrupt();
                    uint32_t page;
                    uint16_t slot;
                    StorageEngine::decodeRid(rids_[candidate], page, slot);
                    if (!page || StorageEngine::encodeRid(page, slot) != rids_[candidate])
                        throw DbError("XX001", "invalid indexed heap row position");
                    if (page != lastHeapPage) {
                        const auto distance = std::max(page, lastHeapPage) - std::min(page, lastHeapPage);
                        heapPageCost += lastHeapPage && distance == 1
                            ? std::min(costs.seqPageCost, costs.randomPageCost) : costs.randomPageCost;
                        lastHeapPage = page;
                    }
                    const auto candidateCost = costs.customCost
                        ? QueryPlanner::costScan("index_scan", fetched_ + 1, 0)
                        : heapPageCost + (fetched_ + 1) * tupleCost;
                    if (candidateCost >= heapCost) return false;
                    ++fetched_;
                    if (!heap->read(page, slot, row, nulls)) continue;
                    detail::ScopedStoredRowNullBitmap bitmap(schema_, nulls);
                    if (!matchesCandidate(candidate, row, fence)) continue;
                    const auto qualifies = storedPredicatesMatch(order_->filter, *engine_, database_, schema_, row, nulls);
                    if (!qualifies) return false;
                    if (!*qualifies) continue;
                } else if (!fetchCandidate(candidate, row, fence)) {
                    if (hasError()) return false;
                    continue;
                }
                if (row.capacity() > budget) return false;
                budget -= row.capacity();
                prefixRows_.emplace_back(rids_[candidate], std::move(row));
            }
            if (prefixRows_.size() == count || rids_.size() < requested) break;
            if (!heap || fetched_ >= maxProbes) return false;

            // Retain the consumed entries at the inclusive boundary. Equal
            // stored keys can have different row positions across leaves.
            // A checked overlap both validates the leaf chain at the join and
            // lets the following batch fetch only previously unread tuples.
            lastSuffix = suffixValues_.back();
            const auto beforeTail = std::find_if(suffixValues_.rbegin(), suffixValues_.rend(),
                [&](int64_t key) { return key != lastSuffix; });
            const size_t offset = beforeTail.base() - suffixValues_.begin();
            overlap.assign(rids_.begin() + offset, rids_.end());
            const auto boundary = primary_key_index::suffixRangeKeys(schema_, *suffix_, values_,
                lastSuffix, lastSuffix).first;
            if (order_->ascending) first = boundary;
            else last = boundary;
            batch = std::min({batch * 2, maxBatch, maxProbes - fetched_});
        }
        rids_.clear();
        suffixValues_.clear();
        return true;
    }

    bool readSuffixRange(BPTree& index, uint64_t fence) {
        if (!index.usesCompositePrimaryRanges()) return false;
        int64_t lower = INT64_MIN;
        int64_t upper = INT64_MAX;
        for (const auto& bound : bounds_) {
            const auto value = inputValue(bound.input);
            if (!value) return false;
            int64_t endpoint = 0;
            const auto parsed = std::from_chars(value->data(), value->data() + value->size(), endpoint);
            if (parsed.ec != std::errc{} || parsed.ptr != value->data() + value->size()) return false;
            if (!bound.inclusive) {
                if (endpoint == (bound.lower ? INT64_MAX : INT64_MIN)) return true;
                endpoint += bound.lower ? 1 : -1;
            }
            if (bound.lower) lower = std::max(lower, endpoint);
            else upper = std::min(upper, endpoint);
        }
        if (lower > upper) return true;
        const auto [first, last] = primary_key_index::suffixRangeKeys(schema_, *suffix_, values_, lower, upper);
        uint64_t liveRows = 0;
        const size_t pages = engine_->tableNumPages(database_, table_);
        const double rows = getRuntimeLiveRowEstimate(database_, table_, liveRows)
            ? static_cast<double>(liveRows) : 32.0 * pages;
        const double heapCost = QueryPlanner::costScan("seq_scan", rows, pages);
        // The samples only decide whether to fetch heap rows via the index.
        // A wide range can always use the ordinary scan and its SQL filter.
        constexpr size_t maxRows = 4096;
        constexpr size_t maxNodes = 128;
        const auto indexCost = [](size_t fetches) {
            return QueryPlanner::costScan("index_scan", static_cast<double>(fetches), 0);
        };
        if (order_ && !storedPredicateSchemaMatches(order_->filter, schema_)) return false;
        const auto count = order_ ? order_->fetchCount : std::nullopt;
        const bool prefix = count && *count <= maxRows && indexCost(*count) < heapCost;
        size_t candidates = prefix ? *count : maxRows;
        if (prefix && !order_->filter.empty()) {
            // Estimate only the maximum speculative I/O. Stop fetching as
            // soon as enough rows pass both the key and the SQL filter.
            candidates = std::min(maxRows, std::max<size_t>(32, *count * 4));
            while (candidates > *count && indexCost(candidates) >= heapCost)
                candidates = *count + (candidates - *count) / 2;
        }
        if (prefix) return bufferPrefix(index, first, last, *count, candidates, maxNodes, heapCost, fence);
        if (!readIndexRange(index, first, last, maxRows, maxNodes, BPTree::CompositeRangeRead::Entire))
            return false;
        size_t fetches = rids_.size();
        if (count) fetches = std::min(fetches, *count);
        if (fetches && indexCost(fetches) >= heapCost) return false;
        if (order_ && !order_->ascending) {
            std::reverse(rids_.begin(), rids_.end());
            std::reverse(suffixValues_.begin(), suffixValues_.end());
        }
        return true;
    }

    bool finishRow(int64_t rid, std::string& row) {
        bool toastOk = false;
        row = engine_->resolveToastValues(database_, table_, row, schema_, &toastOk);
        if (!toastOk) {
            setError("TOAST value read failed");
            return false;
        }
        lastRid_ = rid;
        return true;
    }
public:
    CompositePrimaryKeyScanOp(StorageEngine* engine, std::string database,
            const TableSchema& schema, std::vector<CompositeEqualityInput> inputs,
            OpPtr fallback, std::shared_ptr<PreparedQuery> query = {},
            std::optional<size_t> suffix = {}, std::vector<CompositeSuffixBound> bounds = {},
            std::optional<CompositeSuffixOrder> order = {}, const SelectStmt* fallbackSelect = nullptr)
        : engine_(engine), database_(std::move(database)), table_(schema.tablename),
          relationId_(schema.physicalRelationId), inputs_(std::move(inputs)),
          suffix_(suffix), bounds_(std::move(bounds)),
          order_(std::move(order)), query_(std::move(query)), fallbackSelect_(fallbackSelect),
          fallback_(std::move(fallback)) {}

    ~CompositePrimaryKeyScanOp() override {
        if (locked_) try { close(); } catch (...) {}
    }

    bool open() override {
        OpenInstrument instrumentation(this);
        clearError();
        position_ = fetched_ = 0;
        lastRid_ = 0;
        lastOrderedValue_.reset();
        rids_.clear();
        suffixValues_.clear();
        prefixRows_.clear();
        values_.clear();
        usedIndex_ = fallbackActive_ = false;
        auto& locks = engine_->getLockManager();
        locks.setResourceNamespace(database_);
        if (!locks.lockShared(table_))
            throw DbError("55P03", "could not obtain lock on relation \"" + table_ + "\"");
        locked_ = opened_ = true;
        schema_ = engine_->getTableSchema(database_, table_);
        engine_->noteQuerySnapshot();
        const auto before = engine_->indexReadSnapshotFence(database_);
        if (before && engine_->currentTxnId() != 0 &&
            engine_->getIsolationLevel() != IsolationLevel::SERIALIZABLE &&
            !engine_->rlsAppliesTo(database_, table_) &&
            engine_->getInheritedChildren(database_, table_).empty() && buildValues()) {
            auto* index = engine_->getPKIndex(database_, table_);
            if (!index || !index->isOpen())
                throw DbError("XX001", "could not open B-tree index for relation \"" + table_ + "\"");
            bool read = false;
            if (suffix_) {
                read = readSuffixRange(*index, *before);
            } else if (index->hasCompletePrimaryKeys()) {
                const auto key = primary_key_index::keyForValues(schema_, *index, values_);
                rids_ = lookupBtreeMultiChecked(index, key);
                read = true;
            }
            if (hasError()) return false;
            if (read && engine_->indexReadSnapshotFence(database_) == before) {
                usedIndex_ = true;
                return true;
            }
            rids_.clear();
            suffixValues_.clear();
            prefixRows_.clear();
        }
        if (fallbackSelect_) {
            auto filtered = prefilterPreparedHeap(*engine_, database_, *query_, *fallbackSelect_, schema_, fallback_);
            if (filtered) fallback_ = std::move(filtered);
        }
        fallbackActive_ = true;
        return fallback_->open() || propagateChildError(fallback_.get(), "primary key heap scan failed");
    }

    bool next(std::string& row) override {
        NextInstrument instrument(this);
        if (fallbackActive_) {
            if (!fallback_->next(row)) {
                if (fallback_->hasError())
                    return propagateChildError(fallback_.get(), "primary key heap scan failed");
                return false;
            }
            instrument.emitted = true;
            return true;
        }
        if (!prefixRows_.empty()) {
            if (position_ == prefixRows_.size()) return false;
            auto& [rid, raw] = prefixRows_[position_++];
            checkForQueryInterrupt();
            row = std::move(raw);
            StorageEngine::bindNullRow(engine_, database_, table_, rid, schema_.len());
            instrument.emitted = finishRow(rid, row);
            return instrument.emitted;
        }
        while (position_ < rids_.size()) {
            const auto candidate = position_++;
            if (!fetchCandidate(candidate, row)) {
                if (hasError()) return false;
                continue;
            }
            instrument.emitted = finishRow(rids_[candidate], row);
            return instrument.emitted;
        }
        return false;
    }

    ScanOrigin scanOrigin() const override {
        if (fallbackActive_) return fallback_->scanOrigin();
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = database_;
        origin.tablename = table_;
        origin.rid = lastRid_;
        return origin;
    }
    bool lastColumnIsNull(size_t column) const override {
        if (fallbackActive_) return fallback_->lastColumnIsNull(column);
        return lastRid_ > 0 &&
            detail::storedRowColumnIsNull(*engine_, database_, table_, schema_, lastRid_, column);
    }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override {
        if (fallbackActive_) return fallback_->lastStoredNullBitmap(nulls);
        if (lastRid_ <= 0) return false;
        auto bitmap = detail::storedRowNullBitmap(*engine_, database_, table_, schema_, lastRid_);
        if (!bitmap) return false;
        nulls = std::move(*bitmap);
        return true;
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        return fallbackActive_ ? std::vector<Operator*>{fallback_.get()} : std::vector<Operator*>{};
    }
    std::string preparedPlanNodeName() const override {
        return suffix_ ? "CompositePrimaryKeyRangeScan" : "CompositePrimaryKeyScan";
    }
    bool readsSuffixRange() const { return suffix_.has_value(); }
    std::optional<PreparedIntegerKeyOrder> openedKeyOrder() const {
        if (usedIndex_ && order_)
            return PreparedIntegerKeyOrder{*suffix_, order_->ascending, order_->fetchCount.has_value()};
        return {};
    }
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        std::map<std::string, std::string> attributes{
            {"table", table_}, {"access", !opened_ ? "planned" : usedIndex_ ? "index" : "heap"}};
        if (const auto order = openedKeyOrder())
            attributes.emplace("keyOrder", order->ascending ? "asc" : "desc");
        return attributes;
    }
    double estimatedRows() const override { return suffix_ ? fallback_->estimatedRows() : 1; }
    void close() override {
        if (!locked_) return;
        std::exception_ptr error;
        try {
            if (fallbackActive_) fallback_->close();
            if (usedIndex_ || fetched_) recordTableScan(database_, table_, fetched_, true, false);
        } catch (...) { error = std::current_exception(); }
        auto& locks = engine_->getLockManager();
        locks.setResourceNamespace(database_);
        locks.unlock(table_);
        locked_ = false;
        lastRid_ = 0;
        rids_.clear();
        suffixValues_.clear();
        prefixRows_.clear();
        if (error) std::rethrow_exception(error);
    }
};

// A V3 summary supplies lossy pages; the normal heap cursor captures one
// read view and owns each row/NULL bitmap. Full predicates stay above it.
class BrinScanOp final : public Operator {
    StorageEngine* engine_;
    std::string database_, table_, column_, op_, value_, plannedShape_;
    OpPtr fallback_;
    std::unique_ptr<HeapScanCursor> cursor_;
    std::vector<bool> lastNulls_;
    TableSchema schema_;
    size_t position_ = 0, plannedPages_ = 0;
    int64_t rid_ = 0;
    bool opened_ = false, locked_ = false, fallbackOpen_ = false;
    bool usedSummary_ = false, statsRecorded_ = false;
    double rowsEstimate_ = 1, costEstimate_ = 0;
    static std::string shape(const TableSchema& schema) {
        std::string out;
        const auto field = [&](const std::string& text) { out += std::to_string(text.size())+":"+text; };
        field(std::to_string(schema.physicalRelationId)); field(std::to_string(schema.formatVersion));
        field(std::to_string(schema.len()));
        for (size_t i = 0; i < schema.len(); ++i) {
            const auto& c = schema.cols[i];
            for (const auto& text : {c.dataName,c.dataType,c.domainName,c.collation,c.resolvedCollation,c.generatedExpr}) field(text);
            for (const auto n : {int64_t(c.dsize),int64_t(c.typeMod),int64_t(c.storageCodec),int64_t(c.isArray),int64_t(c.isNull),int64_t(c.generatedKind),int64_t(c.isUnsigned)}) field(std::to_string(n));
            field(std::to_string(c.enumValues.size()));
            field(std::to_string(c.resolvedCollationIsBinary)); field(std::to_string(c.resolvedCollationUsesLocale));
            for (const auto& value : c.enumValues) field(value);
        }
        return out;
    }
    void record() {
        if (!statsRecorded_) {
            recordTableScan(database_, table_, position_, true, false);
            statsRecorded_ = true;
        }
    }
public:
    BrinScanOp(StorageEngine* engine, std::string database, std::string table,
        std::string column, std::string op, std::string value, OpPtr fallback,
        const TableSchema& schema, size_t pages, double estimatedRows, double cost)
        : engine_(engine), database_(std::move(database)), table_(std::move(table)),
          column_(std::move(column)), op_(std::move(op)), value_(std::move(value)),
          plannedShape_(shape(schema)), fallback_(std::move(fallback)), plannedPages_(pages),
          rowsEstimate_(estimatedRows), costEstimate_(cost) {}
    ~BrinScanOp() override { if (opened_) try { close(); } catch (...) {} }
    bool open() override {
        OpenInstrument instrumentation(this);
        clearError(); cursor_.reset(); lastNulls_.clear(); position_ = 0; rid_ = 0;
        usedSummary_ = false; statsRecorded_ = false; opened_ = true;
        auto& locks = engine_->getLockManager(); locks.setResourceNamespace(database_);
        if (!locks.lockShared(table_))
            throw DbError("55P03", "could not obtain lock on relation \"" + table_ + "\"");
        locked_ = true;
        schema_ = engine_->getTableSchema(database_, table_);
        engine_->noteQuerySnapshot();
        const auto fence = engine_->indexReadSnapshotFence(database_);
        if (fence && shape(schema_) == plannedShape_ &&
            (!engine_->inTransaction() || engine_->getIsolationLevel() != IsolationLevel::SERIALIZABLE) &&
            !engine_->rlsAppliesTo(database_, table_) &&
            engine_->getInheritedChildren(database_, table_).empty()) {
            if (const auto pages = engine_->tryBrinPageRanges(database_, table_, column_, op_, value_)) {
                cursor_ = engine_->beginHeapScan(database_, table_, schema_, &*pages);
                if (engine_->indexReadSnapshotFence(database_) == fence &&
                    engine_->tryBrinPageRanges(database_, table_, column_, op_, value_) == pages) {
                    usedSummary_ = true; return true;
                }
                // No heap row or demanded datum has escaped. Later commits
                // are covered by the already captured immutable read view.
                cursor_.reset();
            }
        }
        fallbackOpen_ = true;
        return fallback_->open() || propagateChildError(fallback_.get(), "BRIN heap fallback failed");
    }
    bool next(std::string& out) override {
        NextInstrument instrumentation(this);
        if (fallbackOpen_) {
            if (!fallback_->next(out)) {
                if (fallback_->hasError()) return propagateChildError(fallback_.get(), "BRIN fallback failed");
                return false;
            }
        } else {
            HeapScanRow row;
            if (!cursor_ || !cursor_->next(row)) { record(); return false; }
            rid_ = row.rid; lastNulls_ = std::move(row.nulls);
            StorageEngine::bindNullRow(engine_, database_, table_, rid_, schema_.len());
            bool ok = false;
            out = engine_->resolveToastValues(database_, table_, row.data, schema_, &ok);
            if (!ok) { setError("BRIN heap TOAST read failed"); return false; }
            ++position_;
        }
        instrumentation.emitted = true; return true;
    }
    bool lastColumnIsNull(size_t column) const override {
        if (fallbackOpen_) return fallback_->lastColumnIsNull(column);
        return rid_ && column < lastNulls_.size() && lastNulls_[column];
    }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override {
        if (fallbackOpen_) return fallback_->lastStoredNullBitmap(nulls);
        if (!rid_) return false;
        nulls = lastNulls_; return true;
    }
    ScanOrigin scanOrigin() const override {
        if (fallbackOpen_) return fallback_->scanOrigin();
        ScanOrigin origin; origin.engine = engine_; origin.dbname = database_;
        origin.tablename = table_; origin.rid = rid_; return origin;
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        return fallbackOpen_ ? std::vector<Operator*>{fallback_.get()} : std::vector<Operator*>{};
    }
    std::vector<IOperator*> children() override {
        return fallbackOpen_ ? std::vector<IOperator*>{fallback_.get()} : std::vector<IOperator*>{};
    }
    double estimatedRows() const override { return rowsEstimate_; }
    double estimatedCost() const override { return costEstimate_; }
    const std::string& table() const { return table_; }
    std::string condition() const { return column_ + " " + op_ + " " + value_; }
    size_t plannedPages() const { return plannedPages_; }
    bool usedSummary() const { return usedSummary_; }
    void close() override {
        if (!opened_) return;
        opened_ = false;
        std::exception_ptr error;
        try { if (fallbackOpen_) fallback_->close(); else if (usedSummary_) record(); }
        catch (...) { error = std::current_exception(); }
        fallbackOpen_ = false; cursor_.reset(); lastNulls_.clear(); position_ = 0; rid_ = 0;
        StorageEngine::unbindNullRow();
        if (locked_) {
            locked_ = false; engine_->getLockManager().setResourceNamespace(database_);
            engine_->getLockManager().unlock(table_);
        }
        if (error) std::rethrow_exception(error);
    }
};

// Only immutable literal cells and the binder's implicit primitive coercion
// envelope are inspected. Explicit CAST, functions and retained parameters
// keep their existing demand/owner and are never evaluated by this planner.
static std::optional<std::string> brinLiteralInput(const Expr* expression,
    std::string* literalType = nullptr) {
    while (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
        if (!cast->implicit || !cast->typeMods.empty() || cast->preparedSubquery) return {};
        expression = cast->operand.get();
    }
    const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression);
    bool negate = false;
    if (unary && (unary->op == "-" || unary->op == "+")) {
        negate = unary->op == "-"; expression = unary->operand.get();
    }
    const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
    if (!literal || literal->preparedSubquery ||
        (!literal->analyzedPrimitiveConstant && !literal->typeName.empty() &&
         literal->typeName != "unknown")) return {};
    try {
        ExprEvaluator evaluator;
        const auto value = evaluator.eval(literal, RowContext{});
        if (value.isNull) return {};
        if (literalType) *literalType = value.typeName;
        if (!negate) return value.value;
        const auto type = ExprHelper::canonicalResultTypeName(value.typeName);
        if (type != "smallint" && type != "integer" && type != "bigint" &&
            type != "numeric" && type != "real" && type != "double precision") return {};
        return "-" + value.value;
    } catch (const DbError& error) {
        if (error.sqlState().rfind("22", 0) == 0) return {};
        throw;
    }
}

static bool compositeOperandType(const std::string& keyType, const std::string& operandType) {
    const auto type = ExprHelper::canonicalResultTypeName(operandType);
    return keyType == "text"
        ? type == "text" || type == "character varying"
        : type == "smallint" || type == "integer" || type == "bigint";
}

static const Expr* compositeTextEnvelope(const Expr* node) {
    while (const auto* cast = dynamic_cast<const CastExpr*>(node)) {
        if (!cast->implicit || !cast->typeMods.empty() || cast->preparedSubquery ||
            !compositeOperandType("text", cast->typeName)) break;
        node = cast->operand.get();
    }
    return node;
}

// The protocol can lower a Bind datum to a qualified builtin CAST. Decode
// only these text casts, integer constants and ordinary literal leaves.
static std::optional<std::string> compositeLiteralValue(const Expr* input,
                                                        const std::string& type) {
    if (const auto* cast = dynamic_cast<const CastExpr*>(input)) {
        if (type == "text") {
            CatalogManager::QualifiedName target;
            if (cast->implicit || !cast->typeMods.empty() || cast->preparedSubquery ||
                !CatalogManager::parseQualifiedName(cast->typeName, target) ||
                target.schema != "pg_catalog" ||
                (target.name != "text" && target.name != "varchar")) return {};
            // Cast a decoded string only. Numeric/boolean-to-text casts use
            // their SQL output spelling, which may differ from the input.
            if (!dynamic_cast<const LiteralExpr*>(cast->operand.get())) return {};
            input = cast->operand.get();
        } else {
            const auto target = integerIndexCastType(cast->typeName);
            if (target.empty()) return {};
            const auto value = constantIntegerIndexInput(input, target);
            return value ? compositeEqualityValue(type, value->value) : std::nullopt;
        }
    }
    std::string inputType;
    const auto literal = brinLiteralInput(input, &inputType);
    if (!literal || (!compositeOperandType(type, inputType) &&
        !(type == "text" && inputType == "unknown"))) return {};
    return compositeEqualityValue(type, *literal);
}

static bool resolveCompositeInput(const Expr* expr, bool bound,
        CompositeEqualityInput& input) {
    if (const auto* parameter = dynamic_cast<const ParameterExpr*>(expr)) {
        if (!bound || parameter->origin != ParameterOrigin::StatementInput ||
            !compositeOperandType(input.type, parameter->declaredType)) return false;
        input.slot = parameter->slot;
        return true;
    }
    const auto value = compositeLiteralValue(expr, input.type);
    if (!value) return false;
    input.value = *value;
    return true;
}

static const FunctionCallExpr* rangePredicateCall(const Expr* expression) {
    const auto* call = dynamic_cast<const FunctionCallExpr*>(expression);
    if (!call || call->preparedSubquery || !betweenSyntax(*call) ||
        (call->args.size() != 3 && call->args.size() != 4) ||
        call->filter || call->hasOver || call->distinct || !call->namedArgs.empty() ||
        !call->orderBy.empty() || call->identitySequenceOid) return nullptr;
    return call;
}

static bool collectCompositeBetween(const TableSchema& schema, size_t suffix,
        const Expr* expression, std::optional<size_t> range,
        std::vector<CompositeSuffixBound>& bounds) {
    const auto* call = rangePredicateCall(expression);
    if (!call) return false;
    const auto syntax = betweenSyntax(*call);
    if (syntax->negated || syntax->symmetric) return false;
    const auto& column = schema.cols[suffix];
    const auto type = compositeEqualityType(column);
    if (type.empty() || type == "text") return false;
    const auto isSuffix = [&](const Expr* expr) {
        const auto* reference = dynamic_cast<const ColumnRefExpr*>(expr);
        if (!reference || reference->preparedSubquery) return false;
        if (!range) return reference->schema.empty() && reference->table.empty() &&
                           reference->column == column.dataName;
        if (!reference->binding) return false;
        const auto& binding = *reference->binding;
        return !binding.scopeDepth && !binding.mergedUsing &&
            binding.sourceOrdinal == *range && binding.columnOrdinal == suffix &&
            binding.typeMetadata && binding.typeMetadata->kind == DatumKind::Scalar &&
            !binding.typeMetadata->enumType &&
            binding.typeMetadata->actualOid == binding.typeMetadata->codecOid &&
            compositeOperandType(type, binding.declaredType);
    };
    if (!isSuffix(call->args.front().get()) ||
        (call->args.size() == 4 && !isSuffix(call->args.back().get()))) return false;
    CompositeEqualityInput low{suffix, column.dataName, "bigint", {}, {}};
    CompositeEqualityInput high = low;
    if (!resolveCompositeInput(call->args[1].get(), range.has_value(), low) ||
        !resolveCompositeInput(call->args[2].get(), range.has_value(), high)) return false;
    bounds.push_back({std::move(low), true, true});
    bounds.push_back({std::move(high), false, true});
    return true;
}

static size_t collectCompositePredicates(const TableSchema& schema, const std::vector<size_t>& columns,
        const Expr* predicate, std::optional<size_t> range,
        std::map<size_t, CompositeEqualityInput>& matches,
        std::vector<CompositeSuffixBound>* bounds = nullptr,
        std::optional<size_t> suffix = {}, std::vector<const Expr*>* residual = nullptr) {
    std::vector<const Expr*> terms{predicate};
    size_t conjuncts = 0;
    while (!terms.empty()) {
        const auto* term = terms.back();
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(term);
        terms.pop_back();
        if (binary && !binary->preparedSubquery &&
            (binary->op == "AND" || binary->op == "and")) {
            terms.push_back(binary->right.get());
            terms.push_back(binary->left.get());
            continue;
        }
        ++conjuncts;
        if (residual) residual->push_back(term);
        if (!binary && bounds && suffix &&
            collectCompositeBetween(schema, *suffix, term, range, *bounds)) {
            if (residual) residual->pop_back();
            continue;
        }
        if (!binary || binary->preparedSubquery || binary->rowComparison) continue;
        const bool equality = binary->op == "=";
        if (!equality && (!bounds || !suffix ||
            (binary->op != "<" && binary->op != "<=" &&
             binary->op != ">" && binary->op != ">="))) continue;
        if (binary->comparison && (!binary->comparison->strict ||
            binary->comparison->identity.rfind("builtin-comparison:" + binary->op + "(", 0) != 0 ||
            binary->comparison->enumTypeOid || binary->comparison->collationUsesSystemLocale ||
            (!binary->comparison->collation.empty() &&
                !collation::isBinary(binary->comparison->collation)))) continue;
        if (binary->predicateCollation) {
            const auto& rule = *binary->predicateCollation;
            if (rule.indeterminate || rule.selected.usesSystemLocale ||
                (!rule.selected.comparisonName.empty() &&
                    !collation::isBinary(rule.selected.comparisonName))) continue;
        }
        const Expr* columnExpr = binary->left.get();
        const auto* column = dynamic_cast<const ColumnRefExpr*>(compositeTextEnvelope(columnExpr));
        const Expr* input = binary->right.get();
        if (!column) {
            columnExpr = input;
            column = dynamic_cast<const ColumnRefExpr*>(compositeTextEnvelope(columnExpr));
            input = binary->left.get();
        }
        if (!column) continue;
        size_t ordinal = schema.len();
        if (range) {
            if (!column->binding || column->binding->scopeDepth || column->binding->mergedUsing ||
                column->binding->sourceOrdinal != *range) continue;
            ordinal = column->binding->columnOrdinal;
        } else {
            // The compact planner has only this physical range. Do not
            // reinterpret a qualified or outer reference as a local key.
            if (!column->schema.empty() || !column->table.empty()) continue;
            for (const auto index : columns)
                if (index < schema.len() && schema.cols[index].dataName == column->column) {
                    ordinal = index;
                    break;
                }
        }
        if (ordinal >= schema.len()) continue;
        if (equality) {
            if (std::find(columns.begin(), columns.end(), ordinal) == columns.end() ||
                matches.count(ordinal)) continue;
        } else if (ordinal != suffix) continue;
        const auto& key = schema.cols[ordinal];
        const auto type = compositeEqualityType(key);
        if (type.empty() || (type != "text" && columnExpr != column)) continue;
        if (range) {
            const auto& declaration = column->binding->typeMetadata;
            if (!declaration || declaration->kind != DatumKind::Scalar || declaration->enumType ||
                declaration->actualOid != declaration->codecOid ||
                !compositeOperandType(type, column->binding->declaredType)) continue;
        }
        if (binary->comparison &&
            (!compositeOperandType(type, binary->comparison->leftType) ||
             !compositeOperandType(type, binary->comparison->rightType))) continue;
        if (type == "text") input = compositeTextEnvelope(input);
        const std::string inputType = equality ? type : "bigint";
        CompositeEqualityInput termInput{ordinal, key.dataName, inputType, {}, {}};
        if (!resolveCompositeInput(input, range.has_value(), termInput)) continue;
        if (equality) matches.emplace(ordinal, std::move(termInput));
        else bounds->push_back({std::move(termInput),
            (binary->op.front() == '>') == (columnExpr == binary->left.get()),
            binary->op.size() == 2});
        if (residual) residual->pop_back();
    }
    return conjuncts;
}

static std::vector<CompositeEqualityInput> orderedCompositeInputs(const std::vector<size_t>& columns,
        std::map<size_t, CompositeEqualityInput>& matches, bool allowPrefix = false) {
    if (matches.size() != columns.size() &&
        (!allowPrefix || matches.size() + 1 != columns.size())) return {};
    std::vector<CompositeEqualityInput> ordered;
    ordered.reserve(matches.size());
    for (size_t i = 0; i < matches.size(); ++i) {
        const auto found = matches.find(columns[i]);
        if (found == matches.end()) return {};
        ordered.push_back(std::move(found->second));
    }
    return ordered;
}

static std::optional<QueryColumnBinding> preparedDirectOrderColumn(
    const PreparedQuery& query, const SelectStmt& select);
static std::optional<QueryColumnBinding> integerPageOrder(
    const PreparedQuery& query, const SelectStmt& select);

static const ColumnRefExpr* boundStoredPredicateColumn(const Expr* expression,
        const TableSchema& schema, size_t source) {
    const auto* column = dynamic_cast<const ColumnRefExpr*>(expression);
    if (!column || column->preparedSubquery || column->generatedRead || !column->binding) return nullptr;
    const auto& binding = *column->binding;
    const auto& declaration = binding.typeMetadata;
    if (binding.scopeDepth || binding.mergedUsing || binding.sourceOrdinal != source ||
        binding.columnOrdinal >= schema.len() || !declaration || declaration->kind != DatumKind::Scalar ||
        declaration->actualOid != declaration->codecOid || declaration->enumType) return nullptr;
    const auto& field = schema.cols[binding.columnOrdinal];
    if (field.dataName != column->column || compositeEqualityType(field).empty() ||
        ExprHelper::canonicalResultTypeName(field.dataType) !=
            ExprHelper::canonicalResultTypeName(binding.declaredType)) return nullptr;
    return column;
}

static const ColumnRefExpr* boundStoredIntegerIdentity(const Expr* expression,
        const TableSchema& schema, size_t source) {
    // These operations preserve every signed integer and SQL NULL. Strip
    // them only for comparisons on a real fixed-width field, leaving the
    // bound expression and its declared result type intact.
    const auto isIdentity = [](const Expr* input, std::string_view expected) {
        if (const auto* cast = dynamic_cast<const BinaryOpExpr*>(input); cast && cast->op == "::") {
            const auto* target = dynamic_cast<const LiteralExpr*>(cast->right.get());
            if (cast->preparedSubquery || !target || target->preparedSubquery ||
                integerIndexCastType(target->value).empty()) return false;
            // Both identity values fit any of the integer cast targets.
            input = cast->left.get();
        }
        if (!dynamic_cast<const LiteralExpr*>(input) && !dynamic_cast<const CastExpr*>(input)) return false;
        const auto value = compositeLiteralValue(input, "bigint");
        return value && *value == expected;
    };
    if (!dynamic_cast<const BinaryOpExpr*>(expression)) return nullptr;
    while (const auto* arithmetic = dynamic_cast<const BinaryOpExpr*>(expression)) {
        if (arithmetic->preparedSubquery || arithmetic->comparison || arithmetic->rowComparison ||
            arithmetic->arrayConcat) return nullptr;
        const auto& op = arithmetic->op;
        if (op != "+" && op != "-" && op != "*" && op != "/") return nullptr;
        const std::string_view identity = (op == "+" || op == "-") ? "0" : "1";
        if (isIdentity(arithmetic->right.get(), identity)) {
            expression = arithmetic->left.get();
        } else if ((op == "+" || op == "*") && isIdentity(arithmetic->left.get(), identity)) {
            expression = arithmetic->right.get();
        } else {
            return nullptr;
        }
    }
    const auto* column = boundStoredPredicateColumn(expression, schema, source);
    return column && storedIntegerField(schema, column->binding->columnOrdinal) ? column : nullptr;
}

static const ColumnRefExpr* boundIntegerBetweenColumn(const FunctionCallExpr& between,
        const TableSchema& schema, size_t source) {
    const auto column = [&](const Expr* expr) {
        const auto* ref = boundStoredPredicateColumn(expr, schema, source);
        return ref ? ref : boundStoredIntegerIdentity(expr, schema, source);
    };
    const auto* first = column(between.args[0].get());
    const auto* second = between.args.size() == 4 ? column(between.args[3].get()) : first;
    if (!first || !second || first->binding->columnOrdinal != second->binding->columnOrdinal ||
        !storedIntegerField(schema, first->binding->columnOrdinal)) return nullptr;
    return first;
}

static std::optional<IntegerInterval> integerBetweenEndpoints(
    const FunctionCallExpr& between, const PreparedQuery* query);

static std::optional<StorageEngine::IntegerPartitionFilter> integerKeyPredicate(
    const Expr* expression, size_t source, size_t key, const std::string& keyType,
    const PreparedQuery* inputs, size_t* remaining = nullptr);

// Bound repeated range unions for long, left-deep OR expressions.
static constexpr size_t maxRangePredicateNodes = 256;

static std::optional<StoredPredicate> bindIntegerDisjunction(const BinaryOpExpr& expression,
        const TableSchema& schema, size_t source, const PreparedQuery* query) {
    const Expr* first = &expression;
    const ColumnRefExpr* column = nullptr;
    for (size_t depth = 0; first && depth < maxRangePredicateNodes; ++depth) {
        if (first->preparedSubquery) return {};
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(first)) {
            if (binary->op == "AND" || binary->op == "OR") {
                first = binary->left.get();
                continue;
            }
            column = boundStoredPredicateColumn(binary->left.get(), schema, source);
            if (!column) column = boundStoredPredicateColumn(binary->right.get(), schema, source);
        } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(first)) {
            column = boundStoredPredicateColumn(unary->operand.get(), schema, source);
        } else if (const auto* between = rangePredicateCall(first)) {
            column = boundStoredPredicateColumn(between->args.front().get(), schema, source);
        }
        break;
    }
    if (!column) return {};
    const auto ordinal = column->binding->columnOrdinal;
    const auto field = storedIntegerField(schema, ordinal);
    if (!field) return {};
    size_t remaining = maxRangePredicateNodes;
    const auto& declaration = schema.cols[ordinal];
    auto filter = integerKeyPredicate(&expression, source, ordinal,
        ExprHelper::canonicalResultTypeName(declaration.dataType), query, &remaining);
    if (!filter) return {};
    return StoredPredicate{ordinal, declaration.dataName, declaration.dataType,
        StoredPredicate::Input::IntegerRanges, StoredComparison::Equal, std::move(*filter), field};
}

// Only total, row-local predicates may reject a row before materialization.
// Keep the ordinary WHERE above the source for expressions and fallback.
static std::optional<StoredPredicates> bindStoredPredicates(StorageEngine& engine,
        const std::string& database, const TableSchema& schema, size_t source,
        const std::vector<const Expr*>& residual, const PreparedQuery* query = nullptr) {
    using Input = StoredPredicate::Input;
    StoredPredicates filter;
    if (residual.empty()) return filter;
    std::optional<ExprEvaluator> evaluator;
    const auto localColumn = [&](const Expr* expression) {
        return boundStoredPredicateColumn(expression, schema, source);
    };
    const auto predicateFor = [&](const ColumnRefExpr& column, Input input) {
        const auto ordinal = column.binding->columnOrdinal;
        return StoredPredicate{ordinal, column.column, schema.cols[ordinal].dataType, input,
            StoredComparison::Equal, {}};
    };
    const auto readOperand = [&](const Expr* operand) -> std::pair<const ColumnRefExpr*, Input> {
        if (!operand || operand->preparedSubquery) return {};
        if (const auto* column = localColumn(compositeTextEnvelope(operand))) {
            const auto type = compositeEqualityType(schema.cols[column->binding->columnOrdinal]);
            if (type == "text") return {column, Input::Text};
            if (operand == column) return {column, Input::Integer};
        }
        if (const auto* column = boundStoredIntegerIdentity(operand, schema, source))
            return {column, Input::Integer};
        const auto* call = dynamic_cast<const FunctionCallExpr*>(operand);
        if (!call || call->args.size() != 1 || call->identitySequenceOid ||
            call->sqlValue != FunctionCallExpr::SqlValue::None) return {};
        CatalogManager::QualifiedName name;
        if (!CatalogManager::parseQualifiedName(call->funcName, name, true) || name.name != "length") return {};
        const auto* column = localColumn(compositeTextEnvelope(call->args.front().get()));
        if (!column || compositeEqualityType(schema.cols[column->binding->columnOrdinal]) != "text") return {};
        if (!evaluator) {
            evaluator.emplace();
            evaluator->setCurrentDB(database);
        }
        if (evaluator->strictStringResultType(call, &engine) != "integer") return {};
        return {column, Input::TextLength};
    };
    const auto integerWidth = [](const std::string& declaredType) {
        const auto type = ExprHelper::canonicalResultTypeName(declaredType);
        if (type == "smallint") return 16;
        if (type == "integer") return 32;
        return 64;
    };
    static const std::map<std::string, StoredComparison> comparisons{
        {"=", StoredComparison::Equal}, {"<>", StoredComparison::NotEqual}, {"!=", StoredComparison::NotEqual},
        {"<", StoredComparison::Less}, {"<=", StoredComparison::LessEqual},
        {">", StoredComparison::Greater}, {">=", StoredComparison::GreaterEqual},
    };
    for (const auto* clause : residual) {
        if (!clause || clause->preparedSubquery) return {};
        if (const auto* between = rangePredicateCall(clause)) {
            const auto* column = boundIntegerBetweenColumn(*between, schema, source);
            if (!column) return {};
            const auto endpoints = integerBetweenEndpoints(*between, query);
            if (!endpoints) return {};
            auto predicate = predicateFor(*column, Input::Integer);
            predicate.integer = storedIntegerField(schema, predicate.column);
            if (betweenSyntax(*between)->negated) {
                predicate.input = Input::IntegerOutsideRange;
                predicate.constant = *endpoints;
                filter.push_back(std::move(predicate));
                continue;
            }
            predicate.comparison = StoredComparison::GreaterEqual;
            predicate.constant = endpoints->first;
            filter.push_back(predicate);
            predicate.comparison = StoredComparison::LessEqual;
            predicate.constant = endpoints->second;
            filter.push_back(std::move(predicate));
            continue;
        }
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(clause)) {
            if (unary->op != "IS NULL" && unary->op != "IS NOT NULL") return {};
            const auto* column = localColumn(unary->operand.get());
            if (!column) column = boundStoredIntegerIdentity(unary->operand.get(), schema, source);
            if (!column) return {};
            filter.push_back(predicateFor(*column, unary->op == "IS NULL" ? Input::IsNull : Input::IsNotNull));
            continue;
        }
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(clause);
        if (!binary || binary->rowComparison) return {};
        const auto& op = binary->op;
        if (op == "OR") {
            auto ranges = bindIntegerDisjunction(*binary, schema, source, query);
            if (!ranges) return {};
            filter.push_back(std::move(*ranges));
            continue;
        }
        const bool equality = op == "=" || op == "<>" || op == "!=";
        auto comparator = comparisons.find(op);
        if (comparator == comparisons.end()) return {};
        if (binary->predicateCollation) {
            const auto& rule = *binary->predicateCollation;
            if (rule.indeterminate || rule.selected.usesSystemLocale ||
                (!rule.selected.comparisonName.empty() && !collation::isBinary(rule.selected.comparisonName)))
                return {};
        }
        auto [column, kind] = readOperand(binary->left.get());
        const bool reversed = !column;
        const auto* literal = binary->right.get();
        if (reversed) {
            std::tie(column, kind) = readOperand(binary->right.get());
            literal = binary->left.get();
            if (!equality) {
                const std::string reverseOp = std::string(1, op[0] == '<' ? '>' : '<') + op.substr(1);
                comparator = comparisons.find(reverseOp);
            }
        }
        if (!column || (kind == Input::Text && !equality)) return {};
        const std::string type = kind == Input::Text ? "text" : "bigint";
        if (kind == Input::Text) literal = compositeTextEnvelope(literal);
        const auto constant = compositeLiteralValue(literal, type);
        if (!constant) return {};
        if (binary->comparison) {
            const auto& binding = *binary->comparison;
            if (!binding.strict || binding.enumTypeOid || binding.collationUsesSystemLocale ||
                (!binding.collation.empty() && !collation::isBinary(binding.collation)) ||
                !compositeOperandType(type, binding.leftType) || !compositeOperandType(type, binding.rightType) ||
                binding.identity != "builtin-comparison:" + op + "(" + binding.leftType + "," + binding.rightType + ")")
                return {};
            if (kind != Input::Text) {
                const auto& fieldType = reversed ? binding.rightType : binding.leftType;
                const auto& constantType = reversed ? binding.leftType : binding.rightType;
                const int width = kind == Input::TextLength ? 32 : integerWidth(column->binding->declaredType);
                if (integerWidth(fieldType) < width ||
                    !compositeEqualityValue(ExprHelper::canonicalResultTypeName(constantType), *constant))
                    return {};
            }
        }
        auto predicate = predicateFor(*column, kind);
        predicate.comparison = comparator->second;
        if (kind == Input::Integer) predicate.integer = storedIntegerField(schema, predicate.column);
        if (kind == Input::Text) {
            predicate.constant = *constant;
        } else {
            int64_t number = 0;
            const auto* end = constant->data() + constant->size();
            const auto parsed = std::from_chars(constant->data(), end, number);
            if (parsed.ec != std::errc{} || parsed.ptr != end) return {};
            predicate.constant = number;
        }
        filter.push_back(std::move(predicate));
    }
    return filter;
}

static std::optional<StoredPredicates> bindStoredWhere(StorageEngine& engine, const std::string& database,
        const TableSchema& schema, size_t source, const Expr* expression,
        const PreparedQuery* query = nullptr) {
    if (!expression) return StoredPredicates{};
    std::vector<const Expr*> terms{expression}, clauses;
    while (!terms.empty()) {
        const auto* term = terms.back();
        terms.pop_back();
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(term);
        if (!binary || binary->preparedSubquery || binary->op != "AND") {
            clauses.push_back(term);
            continue;
        }
        terms.push_back(binary->right.get());
        terms.push_back(binary->left.get());
    }
    return bindStoredPredicates(engine, database, schema, source, clauses, query);
}

class StoredFilterScanOp final : public Operator {
    StorageEngine& engine_;
    std::string database_, table_;
    uint64_t relationId_;
    const Expr* where_;
    StoredPredicates predicates_;
    bool sampleSelectivity_;
    OpPtr scan_;
    TableSchema schema_;
    std::vector<bool> nulls_;
    bool opened_ = false, active_ = false, rejected_ = false;
    bool matchedWhere_ = false;
    size_t initialMatches_ = 0;

public:
    StoredFilterScanOp(StorageEngine& engine, std::string database, const TableSchema& schema,
                       const Expr* where, StoredPredicates predicates, OpPtr scan)
        : engine_(engine), database_(std::move(database)), table_(schema.tablename),
          relationId_(schema.physicalRelationId), where_(where), predicates_(std::move(predicates)),
          sampleSelectivity_(std::any_of(predicates_.begin(), predicates_.end(), [](const auto& predicate) {
              using Input = StoredPredicate::Input;
              return !predicate.integer && predicate.input != Input::IsNull && predicate.input != Input::IsNotNull;
          })), scan_(std::move(scan)) {}

    ~StoredFilterScanOp() override { try { close(); } catch (...) {} }

    bool open() override {
        OpenInstrument instrument(this);
        clearError();
        opened_ = true;
        active_ = rejected_ = matchedWhere_ = false;
        initialMatches_ = 0;
        if (!scan_->open()) return propagateChildError(scan_.get(), "stored filter scan failed");
        schema_ = engine_.getTableSchema(database_, table_);
        active_ = schema_.physicalRelationId == relationId_ && supportsStoredHeapPredicate(schema_) &&
            !engine_.rlsAppliesTo(database_, table_) && storedPredicateSchemaMatches(predicates_, schema_);
        return true;
    }

    bool next(std::string& row) override {
        NextInstrument instrument(this);
        matchedWhere_ = false;
        while (scan_->next(row)) {
            if (active_) {
                const auto matches = scan_->lastStoredNullBitmap(nulls_) && nulls_.size() == schema_.len()
                    ? storedPredicatesMatch(predicates_, engine_, database_, schema_, row, nulls_)
                    : std::nullopt;
                if (matches && !*matches) {
                    rejected_ = true;
                    continue;
                }
                // A prefix of matching rows does not predict the rest of a
                // clustered range. Keep cheap integer and NULL checks active.
                // Sample only predicates that extract or process values.
                if (!matches || (sampleSelectivity_ && !rejected_ && ++initialMatches_ == 64)) active_ = false;
                matchedWhere_ = matches.value_or(false) && !sampleSelectivity_;
            }
            instrument.emitted = true;
            return true;
        }
        if (scan_->hasError()) return propagateChildError(scan_.get(), "stored filter scan failed");
        return false;
    }

    ScanOrigin scanOrigin() const override { return scan_->scanOrigin(); }
    bool lastColumnIsNull(size_t ordinal) const override { return scan_->lastColumnIsNull(ordinal); }
    bool lastStoredNullBitmap(std::vector<bool>& bitmap) const override { return scan_->lastStoredNullBitmap(bitmap); }
    const Expr* lastSatisfiedWhere() const override { return matchedWhere_ ? where_ : nullptr; }
    TableScanOp* tableScan() const { return static_cast<TableScanOp*>(scan_.get()); }
    const StoredPredicates& predicates() const { return predicates_; }
    std::string preparedPlanNodeName() const override { return "StoredFilter"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {scan_.get()}; }
    double estimatedRows() const override { return scan_->estimatedRows(); }
    void close() override {
        matchedWhere_ = false;
        if (!opened_) return;
        opened_ = false;
        scan_->close();
    }
};

static OpPtr prefilterPreparedHeap(StorageEngine& engine, const std::string& database,
        const PreparedQuery& query, const SelectStmt& select, const TableSchema& schema, OpPtr& scan) {
    if (!select.whereClause || !select.locking.empty() || !select.fromClause ||
        select.fromClause->type != FromItem::Type::Table || !scan || typeid(*scan) != typeid(TableScanOp) ||
        static_cast<const TableScanOp&>(*scan).tableName() != schema.tablename || !schema.physicalRelationId ||
        !supportsStoredHeapPredicate(schema)) return {};
    const auto range = std::find_if(query.sourceRanges.begin(), query.sourceRanges.end(), [&](const auto& candidate) {
        return candidate.owner == &select && candidate.source == select.fromClause.get() && !candidate.mergedUsing;
    });
    if (range == query.sourceRanges.end()) return {};
    auto predicates = bindStoredWhere(engine, database, schema, range->ordinal, select.whereClause.get());
    if (!predicates || predicates->empty()) return {};
    return std::make_unique<StoredFilterScanOp>(engine, database, schema, select.whereClause.get(),
                                                std::move(*predicates), std::move(scan));
}

static OpPtr preparedCompositePrimarySource(StorageEngine* engine, const std::string& database,
        const std::shared_ptr<PreparedQuery>& query, const SelectStmt& select,
        const TableSchema& schema, OpPtr& source, bool allowRange) {
    const auto columns = compositePrimaryColumns(schema);
    const auto* scan = dynamic_cast<const TableScanOp*>(source.get());
    if (columns.empty() || !scan || typeid(*scan) != typeid(TableScanOp) ||
        scan->tableName() != schema.tablename || !select.whereClause ||
        !select.locking.empty() || !select.fromClause ||
        select.fromClause->type != FromItem::Type::Table) return {};
    const auto range = std::find_if(query->sourceRanges.begin(), query->sourceRanges.end(),
        [&](const auto& candidate) {
            return candidate.owner == &select && candidate.source == select.fromClause.get() &&
                !candidate.mergedUsing;
        });
    if (range == query->sourceRanges.end()) return {};
    std::map<size_t, CompositeEqualityInput> matches;
    std::vector<CompositeSuffixBound> bounds;
    auto suffix = allowRange ? primary_key_index::orderedSuffixColumn(schema) : std::nullopt;
    if (allowRange && !suffix) return {};
    std::vector<const Expr*> residual;
    collectCompositePredicates(schema, columns, select.whereClause.get(),
                               range->ordinal, matches, &bounds, suffix, &residual);
    auto ordered = orderedCompositeInputs(columns, matches, suffix.has_value());
    if (ordered.empty()) return {};
    if (ordered.size() == columns.size()) suffix.reset();
    std::optional<CompositeSuffixOrder> order;
    if (suffix) {
        const auto key = preparedDirectOrderColumn(*query, select);
        if (key && !key->scopeDepth && !key->mergedUsing &&
            key->sourceOrdinal == range->ordinal && key->columnOrdinal == *suffix) {
            order = CompositeSuffixOrder{select.orderBy.front().asc, std::nullopt, {}};
            const auto plainOutput = [&](const SelectItem& item) {
                const auto* column = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
                return column && !column->preparedSubquery && !column->generatedRead && column->binding &&
                    !column->binding->scopeDepth && !column->binding->mergedUsing &&
                    column->binding->sourceOrdinal == range->ordinal &&
                    column->binding->columnOrdinal < schema.len() &&
                    !compositeEqualityType(schema.cols[column->binding->columnOrdinal]).empty();
            };
            if (integerPageOrder(*query, select) &&
                std::all_of(select.selectList.begin(), select.selectList.end(), plainOutput)) {
                if (auto filter = bindStoredPredicates(*engine, database, schema, range->ordinal, residual)) {
                    order->fetchCount = *select.limit + select.offset.value_or(0);
                    order->filter = std::move(*filter);
                }
            }
        }
    }
    return std::make_unique<CompositePrimaryKeyScanOp>(engine, database, schema,
        std::move(ordered), std::move(source), query, suffix, std::move(bounds), std::move(order), &select);
}

static OpPtr literalBrinTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, const SelectStmt* select,
    const TableSchema& schema, OpPtr& source) {
    const auto* physical = dynamic_cast<const TableScanOp*>(source.get());
    if (!physical || !query->parameters.empty() || !select->whereClause ||
        !select->fromClause || select->fromClause->type != FromItem::Type::Table ||
        schema.partitionType != TableSchema::PartitionType::None) return {};
    const auto table = physical->tableName();
    std::vector<const Expr*> conjuncts;
    const auto gather = [&](const auto& self, const Expr* node) -> void {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(node);
        if (binary && binary->op == "AND") { self(self, binary->left.get()); self(self, binary->right.get()); }
        else conjuncts.push_back(node);
    };
    gather(gather, select->whereClause.get());
    std::string bestColumn, bestOp, bestValue;
    size_t bestPages = 0; std::optional<double> bestCost;
    std::optional<uint32_t> extent;
    double sequential = 0;
    for (const auto* expression : conjuncts) {
        const ColumnRefExpr* column = nullptr; const Expr* input = nullptr;
        std::string op;
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression);
            unary && (unary->op == "IS NULL" || unary->op == "IS NOT NULL")) {
            column = dynamic_cast<const ColumnRefExpr*>(unary->operand.get()); op = unary->op;
        } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
                   binary && (binary->op == "=" || binary->op == "<" || binary->op == "<=" || binary->op == ">" || binary->op == ">=")) {
            if (binary->comparison && (!binary->comparison->strict ||
                binary->comparison->identity.rfind("builtin-comparison:", 0) != 0)) continue;
            column = dynamic_cast<const ColumnRefExpr*>(binary->left.get());
            input = binary->right.get(); op = binary->op;
            if (!column) {
                column = dynamic_cast<const ColumnRefExpr*>(binary->right.get()); input = binary->left.get();
                if (op == "<") op = ">"; else if (op == "<=") op = ">=";
                else if (op == ">") op = "<"; else if (op == ">=") op = "<=";
            }
        }
        if (!column || !column->binding || column->binding->scopeDepth ||
            column->binding->mergedUsing || column->binding->columnOrdinal >= schema.len()) continue;
        bool sameRange = false;
        for (const auto& range : query->sourceRanges)
            if (range.owner == select && range.source == select->fromClause.get() &&
                range.ordinal == column->binding->sourceOrdinal) sameRange = true;
        const auto& key = schema.cols[column->binding->columnOrdinal];
        const auto& declaration = column->binding->typeMetadata;
        const bool nullTest = op == "IS NULL" || op == "IS NOT NULL";
        if (!sameRange || key.generatedKind || !declaration ||
            declaration->kind == DatumKind::Record ||
            (!nullTest && (key.isArray || key.isUnsigned || !key.domainName.empty() ||
                declaration->kind != DatumKind::Scalar || declaration->enumType ||
                declaration->actualOid != declaration->codecOid ||
                (!key.collation.empty() && !key.resolvedCollationIsBinary))) ||
            !engine->hasBrinIndex(database, table, key.dataName)) continue;
        if (!nullTest) {
            // Ordinary scalar builtins use the evaluator's implicit dispatch;
            // the optional AST cell is retained only for enum/reference roles.
            // Validate the same pure builtin signature without mutating that
            // AST or evaluating a provider, parameter or CAST expression.
            const auto type = ExprHelper::canonicalResultTypeName(key.dataType);
            if (ExprHelper::canonicalResultTypeName(column->binding->declaredType) != type) continue;
            try {
                const auto role = ExprEvaluator::resolveComparison(op, type, type);
                if (!role.strict || role.identity.rfind("builtin-comparison:", 0) != 0) continue;
            } catch (const DbError& error) {
                if (error.sqlState() == "42883") continue;
                throw;
            }
        }
        if (op == "=" && isSingleColumnPrimaryKey(schema, key.dataName)) continue;
        if (!extent) {
            if (engine->rlsAppliesTo(database, table) || !engine->getInheritedChildren(database, table).empty()) return {};
            extent = engine->tableNumPages(database, table);
            if (*extent <= 1) return {};
            sequential = QueryPlanner::costScan("seq_scan", 0, *extent-1);
        }
        const auto value = input ? brinLiteralInput(input) : std::optional<std::string>("");
        if (!value) continue;
        const auto ranges = engine->tryBrinPageRanges(database, table, key.dataName, op, *value, false);
        if (!ranges) continue;
        uint64_t pages = 0;
        for (const auto& range : *ranges) pages += uint64_t(range.second) - range.first + 1;
        const double cost = QueryPlanner::costScan("seq_scan", 0, pages) +
                            QueryPlanner::costScan("index_scan", 1, 0);
        if (!std::isfinite(cost) || !std::isfinite(sequential) || cost >= sequential ||
            (bestCost && cost >= *bestCost)) continue;
        bestCost = cost; bestColumn = key.dataName; bestOp = op; bestValue = *value; bestPages = pages;
    }
    if (!bestCost) return {};
    return std::make_unique<BrinScanOp>(engine, database, table, bestColumn, bestOp, bestValue,
                                      std::move(source), schema, bestPages, bestPages*32.0, *bestCost);
}

// Bind input is read when this physical source opens, including cursor
// restarts. The typed Filter above it remains the final predicate owner.
// Reuse IndexScan's checked heap/MVCC/NULL access and keep the conservative
// heap path whenever current-version indexes cannot serve the snapshot.
class PreparedParameterIndexScanOp final : public Operator {
    StorageEngine* engine_;
    std::string database_, table_, column_, type_;
    std::shared_ptr<PreparedQuery> query_;
    size_t slot_;
    uint64_t relationId_;
    OpPtr fallback_, heapFilter_;
    std::unique_ptr<IndexScanOp> index_;
    Operator* active_ = nullptr;
    bool opened_ = false, locked_ = false, usedFallback_ = false;

    bool sameKey(const TableSchema& schema) const {
        if (schema.partitionType != TableSchema::PartitionType::None ||
            (type_ != "timetz" && !isSingleColumnPrimaryKey(schema, column_))) return false;
        const Column* key = nullptr;
        for (size_t i = 0; i < schema.len(); ++i) {
            if (schema.cols[i].generatedKind == 'v') return false;
            if (schema.cols[i].dataName == column_) key = &schema.cols[i];
        }
        if (!key || key->isArray || key->isUnsigned || !key->domainName.empty() ||
            key->generatedKind || ExprHelper::canonicalResultTypeName(key->dataType) != type_)
            return false;
        return type_ != "timetz" || (schema.physicalRelationId == relationId_ &&
            completeStoredZonedClockIndex(*engine_, database_, schema, *key));
    }
    bool key(std::string& value) const {
        if (slot_ >= query_->parameters.size()) return false;
        const auto& input = query_->parameters[slot_];
        if (input.isNull || ExprHelper::canonicalResultTypeName(input.typeName) != type_)
            return false;
        if (type_ == "timetz") {
            if (!temporal_storage_detail::parseZonedClock(input.value)) return false;
            value = input.value;
            return true;
        }
        int64_t integer = 0;
        const auto parsed = std::from_chars(input.value.data(),
            input.value.data() + input.value.size(), integer);
        if (parsed.ec != std::errc{} || parsed.ptr != input.value.data() + input.value.size())
            return false;
        if ((type_ == "smallint" && (integer < -32768 || integer > 32767)) ||
            (type_ == "integer" && (integer < INT32_MIN || integer > INT32_MAX))) return false;
        value = std::to_string(integer);
        return true;
    }
public:
    PreparedParameterIndexScanOp(StorageEngine* engine, std::string database,
        std::string table, std::string column, std::string type,
        std::shared_ptr<PreparedQuery> query, size_t slot, OpPtr fallback,
        uint64_t relationId = 0)
        : engine_(engine), database_(std::move(database)), table_(std::move(table)),
          column_(std::move(column)), type_(std::move(type)), query_(std::move(query)),
          slot_(slot), relationId_(relationId), fallback_(std::move(fallback)) {}
    ~PreparedParameterIndexScanOp() override {
        if (opened_) try { close(); } catch (...) {}
    }
    bool open() override {
        OpenInstrument instrumentation(this);
        clearError(); active_ = nullptr; index_.reset(); heapFilter_.reset(); usedFallback_ = false;
        auto& locks = engine_->getLockManager();
        locks.setResourceNamespace(database_);
        if (!locks.lockShared(table_)) throw DbError("55P03", "could not obtain lock on relation \"" + table_ + "\"");
        locked_ = true; opened_ = true;
        const auto schema = engine_->getTableSchema(database_, table_);
        engine_->noteQuerySnapshot();
        std::string value;
        const auto fence = engine_->indexReadSnapshotFence(database_);
        const bool usableKey = sameKey(schema) && key(value) &&
            engine_->getInheritedChildren(database_, table_).empty();
        if (fence && usableKey &&
            engine_->getIsolationLevel() != IsolationLevel::SERIALIZABLE &&
            !engine_->rlsAppliesTo(database_, table_)) {
            index_ = std::make_unique<IndexScanOp>(engine_, database_, table_, column_, value);
            active_ = index_.get();
            if (!active_->open()) return propagateChildError(active_, "parameter index source failed");
            if (engine_->indexReadSnapshotFence(database_) == fence &&
                (type_ != "timetz" || index_->usedIndex())) return true;
            // Allocation/ownership changed during the candidate probe.
            active_->close(); active_ = nullptr;
            if (type_ == "timetz") index_.reset();
        }
        usedFallback_ = true;
        if (usableKey) {
            // Old/overlapping snapshots still scan the visible heap. Apply
            // the same proven equality before the legacy typed
            // predicate rebuilds a full expression context for each row.
            // TableScan retains RLS and serializable dependency ownership.
            StorageEngine::Condition condition;
            condition.op = "=";
            condition.colName = column_;
            condition.value = value;
            condition.decodedLiteralRhs = true;
            heapFilter_ = std::make_unique<FilterOp>(
                std::make_unique<TableScanOp>(engine_, database_, table_), schema,
                std::vector<StorageEngine::Condition>{std::move(condition)});
        }
        active_ = heapFilter_ ? heapFilter_.get() : fallback_.get();
        return active_->open() || propagateChildError(active_, "parameter heap source failed");
    }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        if (!active_ || !active_->next(row)) {
            if (active_ && active_->hasError())
                return propagateChildError(active_, "parameter source failed");
            return false;
        }
        instrumentation.emitted = true;
        return true;
    }
    bool lastColumnIsNull(size_t column) const override {
        return active_ && active_->lastColumnIsNull(column);
    }
    ScanOrigin scanOrigin() const override {
        return active_ ? active_->scanOrigin() : ScanOrigin{};
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        std::vector<Operator*> result;
        if (index_) result.push_back(index_.get());
        if (usedFallback_) result.push_back(heapFilter_ ? heapFilter_.get() : fallback_.get());
        return result;
    }
    std::vector<IOperator*> children() override {
        const auto result = preparedPlanChildren();
        return {result.begin(), result.end()};
    }
    std::string preparedPlanNodeName() const override {
        return "TypedEqualityScan(table=" + table_ + ", col=" + column_ + ")";
    }
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        if (type_ != "timetz") return {};
        return {{"usedIndex", index_ && index_->usedIndex() ? "true" : "false"}};
    }
    double estimatedRows() const override { return 1; }
    void close() override {
        if (!opened_) return;
        opened_ = false;
        std::exception_ptr error;
        try { if (active_) active_->close(); } catch (...) { error = std::current_exception(); }
        if (locked_) {
            locked_ = false;
            engine_->getLockManager().setResourceNamespace(database_);
            engine_->getLockManager().unlock(table_);
        }
        if (error) std::rethrow_exception(error);
    }
};

// Only an actual string literal behind the binder's builtin text coercion is
// inspected. No CAST tree, parameter, routine or subquery is evaluated here.
static std::optional<std::string> literalLikeInput(const Expr* input) {
    while (const auto* cast = dynamic_cast<const CastExpr*>(input)) {
        if (!cast->implicit || cast->typeName != "text" || !cast->typeMods.empty() ||
            cast->preparedSubquery) return std::nullopt;
        input = cast->operand.get();
    }
    const auto* literal = dynamic_cast<const LiteralExpr*>(input);
    if (!literal || literal->preparedSubquery || !literal->typeName.empty() ||
        literal->value.size() < 2 || literal->value.front() != '\'' ||
        literal->value.back() != '\'') return std::nullopt;
    ExprEvaluator evaluator;
    const auto value = evaluator.eval(literal, RowContext{});
    return value.isNull ? std::nullopt : std::optional<std::string>(value.value);
}

static std::optional<std::string> nativeLikePrefix(
    const StorageEngine::Condition& condition, const TableSchema& schema) {
    if (condition.op != "like" || condition.patternIsNull ||
        condition.patternEscapeIsNull || !condition.patternEscapeColumn.empty())
        return std::nullopt;
    const Column* key = nullptr;
    for (size_t i = 0; i < schema.len(); ++i) {
        if (schema.cols[i].dataName == condition.colName) key = &schema.cols[i];
        if (!condition.decodedLiteralRhs && schema.cols[i].dataName == condition.value)
            return std::nullopt;
    }
    if (!key || key->isArray || !key->domainName.empty() || key->generatedKind ||
        (!key->collation.empty() && !key->resolvedCollationIsBinary))
        return std::nullopt;
    const auto type = ExprHelper::canonicalResultTypeName(key->dataType);
    const auto patternType = ExprHelper::canonicalResultTypeName(condition.patternType);
    if ((type != "text" && type != "character varying") ||
        (!patternType.empty() && patternType != "unknown" && patternType != "text" &&
         patternType != "character varying"))
        return std::nullopt;
    // The old normalizer's 0x01 escape envelope belongs to its matcher.
    if (!condition.decodedLiteralRhs && condition.patternType.empty() &&
        condition.value.find(char(1)) != std::string::npos)
        return std::nullopt;
    return planner_pattern::likePrefix(condition.value,
        condition.patternEscape.value_or("\\"));
}

// Do not turn a visibly dense prefix into random heap fetches. Existing
// typed ANALYZE MCVs and fully covered equidepth buckets provide evidence of
// density; straddling/unknown buckets contribute nothing to this guard, so a
// rare prefix is not rejected just because the histogram is coarse. This is
// a scan-cost guard, not PostgreSQL's full patternsel/correlation model.
static bool densePrefixPrefersHeap(StorageEngine* engine, const std::string& database,
    const std::string& table, const std::string& column, const std::string& prefix) {
    size_t analyzedRows = 0;
    double analyzedPopulation = 0;
    std::optional<double> fraction;
    if (const auto typed = engine->getAnalyzedColumnStatistics(database, table, column)) {
        fraction = planner_statistics::analyzedPrefixDensity(*typed, prefix);
        if (fraction) analyzedPopulation = typed->totalRows;
    }
    if (!fraction) {
        const auto statistics = engine->getColumnStats(database, table, column);
        if (statistics.analyzedRows) analyzedRows = *statistics.analyzedRows;
        else if (!engine->tryGetTableRowCount(database, table, analyzedRows)) return false;
        if (!analyzedRows) return false;
        analyzedPopulation = analyzedRows;
        double common = 0;
        for (const auto& value : statistics.mcv)
            if (value.first.compare(0,prefix.size(),prefix)==0) common += value.second;
        fraction = common / analyzedRows;
        if (!statistics.histogram.empty()) {
            size_t covered = 0;
            for (const auto& bounds : statistics.histogram)
                covered += bounds.first.compare(0,prefix.size(),prefix)==0 && bounds.second.compare(0,prefix.size(),prefix)==0;
            const double nonnull = 1.0 - std::min(1.0,
                static_cast<double>(statistics.nullCount) / analyzedRows);
            fraction = std::max(*fraction, nonnull * covered / statistics.histogram.size());
        }
    }
    uint64_t runtimeRows = 0;
    const double rows = getRuntimeLiveRowEstimate(database, table, runtimeRows)
        ? static_cast<double>(runtimeRows) : analyzedPopulation;
    const auto pages = engine->tableNumPages(database, table);
    if (!pages || !rows) return false;
    // Absent/truncated legacy slots are not evidence of a zero-cost probe.
    // Use the ordinary planner's one-row costing floor for unknown/empty
    // candidate estimates, while keeping the executor's true empty result.
    const double index = QueryPlanner::costScan("index_scan",
        std::max(1.0, rows * std::min(1.0, *fraction)), 0);
    const double heap = QueryPlanner::costScan("seq_scan", rows, pages);
    return std::isfinite(index) && std::isfinite(heap) && index >= heap;
}

static OpPtr literalLikeTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, const SelectStmt* select,
    const TableSchema& schema, OpPtr& source) {
    const auto* physical = dynamic_cast<const TableScanOp*>(source.get());
    if (!physical || !query->parameters.empty() || !select->whereClause ||
        !select->fromClause || select->fromClause->type != FromItem::Type::Table ||
        schema.partitionType != TableSchema::PartitionType::None)
        return {};
    std::vector<const Expr*> conjuncts;
    const auto gather = [&](const auto& self, const Expr* node) -> void {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(node);
        if (binary && binary->op == "AND") {
            self(self, binary->left.get());
            self(self, binary->right.get());
        } else conjuncts.push_back(node);
    };
    gather(gather, select->whereClause.get());
    for (const auto* node : conjuncts) {
        const Expr* left = nullptr;
        const Expr* right = nullptr;
        const Expr* escape = nullptr;
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node);
            binary && binary->op == "LIKE") {
            left = binary->left.get();
            right = binary->right.get();
        } else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(node);
                   call && call->schema.empty() && call->funcName == "LIKE ESCAPE" &&
                   call->args.size() == 3 && call->namedArgs.empty() && !call->hasOver &&
                   !call->filter && !call->distinct) {
            left = call->args[0].get();
            right = call->args[1].get();
            escape = call->args[2].get();
        } else continue;
        const auto* column = dynamic_cast<const ColumnRefExpr*>(left);
        if (!column || !column->binding || column->binding->scopeDepth ||
            column->binding->mergedUsing || column->binding->columnOrdinal >= schema.len())
            continue;
        bool sameRange = false;
        for (const auto& range : query->sourceRanges)
            if (range.owner == select && range.source == select->fromClause.get() &&
                range.ordinal == column->binding->sourceOrdinal) sameRange = true;
        const auto& key = schema.cols[column->binding->columnOrdinal];
        const auto type = ExprHelper::canonicalResultTypeName(key.dataType);
        const auto& declaration = column->binding->typeMetadata;
        if (!sameRange || key.isArray || !key.domainName.empty() || key.generatedKind ||
            (type != "text" && type != "character varying") ||
            ExprHelper::canonicalResultTypeName(column->binding->declaredType) != type ||
            !declaration || declaration->kind != DatumKind::Scalar ||
            declaration->actualOid != declaration->codecOid ||
            (declaration->codecOid != 25 && declaration->codecOid != 1043) ||
            (!key.collation.empty() && !key.resolvedCollationIsBinary))
            continue;
        const auto pattern = literalLikeInput(right);
        const auto escapeValue = escape ? literalLikeInput(escape)
            : std::optional<std::string>("\\");
        if (!pattern || !escapeValue) continue;
        const auto prefix = planner_pattern::likePrefix(*pattern, *escapeValue);
        if (!prefix) continue;
        const auto table = physical->tableName();
        if (engine->rlsAppliesTo(database, table) ||
            !engine->hasGiSTIndex(database, table, key.dataName)) continue;
        if (densePrefixPrefersHeap(engine, database, table, key.dataName, *prefix))
            continue;
        // Preserve the LIKE role as well as its decoded immutable values.
        // Default-locale pattern prefixes use bytes; ordinary comparison
        // bounds must retain their independent SQL collation admission.
        StorageEngine::Condition patternCondition;
        patternCondition.op = "like";
        patternCondition.colName = key.dataName;
        patternCondition.value = *pattern;
        patternCondition.patternType = "text";
        patternCondition.patternEscape = *escapeValue;
        patternCondition.patternEscapeType = "text";
        patternCondition.decodedLiteralRhs = true;
        std::vector<StorageEngine::Condition> bounds{std::move(patternCondition)};
        // The existing PreparedFilter remains above this candidate source;
        // it owns the original LIKE, residual expressions and NULL semantics.
        return std::make_unique<GiSTScanOp>(engine, database, table, bounds,
            std::optional<TableSchema>(schema), std::move(source));
    }
    return {};
}

// Candidate narrowing only: the original PreparedFilter still owns every
// predicate and its demand. No index contents are read while choosing this
// source, and the real physical source moves only after all admission checks.
static OpPtr literalGiSTTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, const SelectStmt* select,
    const TableSchema& schema, OpPtr& source) {
    const auto* physical = dynamic_cast<const TableScanOp*>(source.get());
    if (!physical || !query->parameters.empty() || !select->whereClause ||
        !select->fromClause || select->fromClause->type != FromItem::Type::Table ||
        schema.partitionType != TableSchema::PartitionType::None)
        return {};
    const auto table = physical->tableName();
    if (engine->rlsAppliesTo(database, table) ||
        !engine->getInheritedChildren(database, table).empty()) return {};

    std::vector<const Expr*> conjuncts;
    const auto gather = [&](const auto& self, const Expr* expression) -> void {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
        if (binary && binary->op == "AND") {
            self(self, binary->left.get());
            self(self, binary->right.get());
        } else conjuncts.push_back(expression);
    };
    gather(gather, select->whereClause.get());
    struct ColumnBounds {
        size_t ordinal;
        std::vector<StorageEngine::Condition> conditions;
        bool useful = false;
    };
    std::vector<ColumnBounds> columns;
    for (const auto* expression : conjuncts) {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
        if (!binary || (binary->op != "=" && binary->op != "<" &&
            binary->op != "<=" && binary->op != ">" && binary->op != ">="))
            continue;
        const auto* column = dynamic_cast<const ColumnRefExpr*>(binary->left.get());
        const Expr* input = binary->right.get();
        std::string op = binary->op;
        if (!column) {
            column = dynamic_cast<const ColumnRefExpr*>(binary->right.get());
            input = binary->left.get();
            if (op == "<") op = ">";
            else if (op == "<=") op = ">=";
            else if (op == ">") op = "<";
            else if (op == ">=") op = "<=";
        }
        if (!column || !column->binding || column->binding->scopeDepth ||
            column->binding->mergedUsing ||
            column->binding->columnOrdinal >= schema.len()) continue;
        bool sameRange = false;
        for (const auto& range : query->sourceRanges)
            if (range.owner == select && range.source == select->fromClause.get() &&
                range.ordinal == column->binding->sourceOrdinal) sameRange = true;
        const auto ordinal = column->binding->columnOrdinal;
        const auto& key = schema.cols[ordinal];
        const auto& declaration = column->binding->typeMetadata;
        const auto type = ExprHelper::canonicalResultTypeName(key.dataType);
        // The default text locale is not byte ordered even though an empty
        // Column::collation leaves resolvedCollationIsBinary at its default.
        if (!sameRange || key.isArray || key.isUnsigned || key.generatedKind ||
            !key.domainName.empty() || !key.enumValues.empty() ||
            (type != "text" && type != "character varying") ||
            ExprHelper::canonicalResultTypeName(column->binding->declaredType) != type ||
            !declaration || declaration->kind != DatumKind::Scalar || declaration->enumType ||
            declaration->actualOid != declaration->codecOid ||
            declaration->actualOid != (type == "text" ? 25u : 1043u) ||
            key.collation.empty() || !key.resolvedCollationIsBinary ||
            key.resolvedCollationUsesLocale ||
            !engine->hasGiSTIndex(database, table, key.dataName)) continue;
        try {
            const auto role = ExprEvaluator::resolveComparison(binary->op, type, type);
            if (!role.strict || role.identity.rfind("builtin-comparison:", 0) != 0)
                continue;
            if (binary->comparison) {
                const auto& bound = *binary->comparison;
                if (!bound.strict || bound.identity != role.identity ||
                    bound.op != role.op || bound.leftType != role.leftType ||
                    bound.rightType != role.rightType || bound.enumTypeOid ||
                    !bound.enumLabels.empty() || !bound.collation.empty()) continue;
            }
        } catch (const DbError& error) {
            if (error.sqlState() == "42883") continue;
            throw;
        }
        // Only binder-owned primitive text coercion envelopes are safe to
        // inspect. A domain/qualified/other target is opaque even if the
        // original leaf happens to be a string literal.
        bool primitiveEnvelope = true;
        for (const Expr* envelope = input; envelope;) {
            const auto* cast = dynamic_cast<const CastExpr*>(envelope);
            if (!cast) break;
            if (!cast->implicit || !cast->typeMods.empty() || cast->preparedSubquery ||
                (cast->typeName != "text" && cast->typeName != "varchar" &&
                 cast->typeName != "character varying")) {
                primitiveEnvelope = false;
                break;
            }
            envelope = cast->operand.get();
        }
        if (!primitiveEnvelope) continue;
        // Reuse the existing immutable-literal decoder once. Its actual
        // output type prevents numeric/boolean bytes from becoming text
        // bounds; explicit CAST/functions/parameters/children remain opaque.
        std::string literalType;
        const auto value = brinLiteralInput(input, &literalType);
        const auto canonicalLiteral = ExprHelper::canonicalResultTypeName(literalType);
        if (!value || (canonicalLiteral != "text" &&
                       canonicalLiteral != "character varying")) continue;
        auto found = std::find_if(columns.begin(), columns.end(),
            [&](const ColumnBounds& candidate) { return candidate.ordinal == ordinal; });
        if (found == columns.end()) {
            columns.push_back({ordinal, {}, false});
            found = std::prev(columns.end());
        }
        StorageEngine::Condition condition;
        condition.op = op;
        condition.colName = key.dataName;
        condition.value = *value;
        condition.decodedLiteralRhs = true;
        found->conditions.push_back(std::move(condition));
        // Do not select a stand-alone >= empty bound that retains every
        // non-NULL byte string. A later upper/equality/strict bound on this
        // same column still makes the combined candidate worth considering.
        found->useful = found->useful || op != ">=" || !value->empty();
    }
    for (auto& column : columns) {
        if (!column.useful) continue;
        return std::make_unique<GiSTScanOp>(engine, database, table,
            column.conditions, std::optional<TableSchema>(schema), std::move(source));
    }
    return {};
}

static std::optional<QueryColumnBinding> preparedDirectOrderColumn(
        const PreparedQuery& query, const SelectStmt& select) {
    if (select.orderBy.size() != 1 || select.orderBy.front().enumComparison ||
        !select.orderBy.front().usingOp.empty()) return {};
    const auto& order = select.orderBy.front();
    const auto output = query.statementOutputs.find(&select);
    const auto projections = query.projectionBindings.find(&select);
    if (output == query.statementOutputs.end() || projections == query.projectionBindings.end()) return {};
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(order.expr.get())) {
        if (column->table.empty() && column->schema.empty()) {
            for (size_t i = 0; i < output->second.size(); ++i)
                if (output->second[i].name == column->column) return projections->second.at(i).column;
        }
        return column->binding;
    }
    const auto* literal = dynamic_cast<const LiteralExpr*>(order.expr.get());
    if (!literal || literal->preparedSubquery) return {};
    size_t ordinal = 0;
    const auto end = literal->value.data() + literal->value.size();
    const auto parsed = std::from_chars(literal->value.data(), end, ordinal);
    if (parsed.ec != std::errc{} || parsed.ptr != end || !ordinal || ordinal > output->second.size()) return {};
    return projections->second.at(ordinal - 1).column;
}

static bool hasCardinalityChangingTargets(const SelectStmt& select) {
    const auto cardinalityChanges = [&](const auto& self, Expr* expression) -> bool {
        if (!expression || expression->preparedSubquery) return false;
        if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression);
            call && (call->hasOver || call->setReturning || !aggregate_type_detail::builtinName(call).empty()))
            return true;
        bool changes = false;
        prepared_window_detail::spec_detail::valueChildren(expression,
            [&](Expr* child) { if (!changes) changes = self(self, child); });
        return changes;
    };
    return std::any_of(select.selectList.begin(), select.selectList.end(), [&](const auto& target) {
        return cardinalityChanges(cardinalityChanges, target.expr.get());
    });
}

static std::optional<size_t> simpleIntegerScanDemand(const SelectStmt& select) {
    if (!select.limit || !*select.limit || select.limitExpr || select.offsetExpr ||
        select.distinct || !select.distinctOn.empty() || select.having || !select.groupBy.empty() ||
        !select.groupByElems.empty() || !select.windowDefs.empty() || !select.ctes.empty() ||
        select.setOp != SetOp::None || select.setOpLhs || select.setOpRhs || select.withTies ||
        !select.locking.empty() || select.existsTargetPrunable ||
        (select.signedFetchCount && *select.signedFetchCount < 0) ||
        *select.limit > std::numeric_limits<size_t>::max() - select.offset.value_or(0) ||
        hasCardinalityChangingTargets(select)) return {};
    return select.offset.value_or(0) + *select.limit;
}

static std::optional<QueryColumnBinding> integerPageOrder(const PreparedQuery& query,
                                                        const SelectStmt& select) {
    if (!select.fromClause || select.fromClause->type != FromItem::Type::Table ||
        select.orderBy.size() != 1 || !simpleIntegerScanDemand(select)) return {};
    return preparedDirectOrderColumn(query, select);
}

struct IntegerPageBounds {
    std::optional<int64_t> lower;
    std::optional<int64_t> upper;
    std::vector<OrderedPrimaryKeyNullTest> nullTests;
    std::optional<StorageEngine::IntegerPartitionFilter> ranges;
    bool hasDisjunction = false;

    void constrain(bool isLower, bool inclusive, int64_t value) {
        if (!inclusive) {
            if ((isLower && value == INT64_MAX) || (!isLower && value == INT64_MIN)) {
                lower = INT64_MAX;
                upper = INT64_MIN;
                return;
            }
            value += isLower ? 1 : -1;
        }
        if (isLower) lower = std::max(lower.value_or(INT64_MIN), value);
        else upper = std::min(upper.value_or(INT64_MAX), value);
    }

    void intersect(StorageEngine::IntegerPartitionFilter filter) {
        if (ranges) ranges->intersect(filter);
        else ranges = std::move(filter);
    }

    void exclude(const IntegerInterval& interval) {
        if (interval.first <= interval.second)
            intersect(outsideIntegerInterval(interval.first, interval.second));
    }

    bool applyRanges(const TableSchema& schema, size_t key) {
        if (!ranges) return true;
        const auto field = storedIntegerField(schema, key);
        if (!field) return false;
        const auto [minKey, maxKey] = storedIntegerLimits(field->width);
        ranges->intersect({{{std::max(minKey, lower.value_or(minKey)),
                              std::min(maxKey, upper.value_or(maxKey))}}, false});
        if (ranges->intervals.size() > 1) return true;
        if (ranges->intervals.empty()) {
            lower = INT64_MAX;
            upper = INT64_MIN;
        } else {
            const auto [start, end] = ranges->intervals.front();
            if (lower || start != minKey) lower = start;
            if (upper || end != maxKey) upper = end;
        }
        return true;
    }

    const std::vector<IntegerInterval>& disjointRanges() const {
        static const std::vector<IntegerInterval> none;
        return ranges && ranges->intervals.size() > 1 ? ranges->intervals : none;
    }

    bool bounded() const { return lower || upper || !disjointRanges().empty(); }
};

struct IntegerPageEndpoint {
    int64_t value;
    std::string type;
};

static std::optional<IntegerPageEndpoint> parseIntegerPageEndpoint(const std::string& text,
                                                                   std::string type) {
    if (text.empty() || (type != "smallint" && type != "integer" && type != "bigint")) return {};
    const char* begin = text.data();
    const char* end = begin + text.size();
    if (*begin == '+' && (++begin == end || *begin < '0' || *begin > '9')) return {};
    int64_t value = 0;
    const auto parsed = std::from_chars(begin, end, value);
    if (parsed.ec != std::errc{} || parsed.ptr != end ||
        (type == "smallint" && (value < INT16_MIN || value > INT16_MAX)) ||
        (type == "integer" && (value < INT32_MIN || value > INT32_MAX))) return {};
    return IntegerPageEndpoint{value, std::move(type)};
}

static std::optional<IntegerPageEndpoint> integerPageEndpoint(const Expr* expression,
                                                            const PreparedQuery* query) {
    if (!expression || expression->preparedSubquery) return {};
    if (const auto* parameter = dynamic_cast<const ParameterExpr*>(expression)) {
        if (!query || parameter->origin != ParameterOrigin::StatementInput ||
            parameter->slot >= query->parameters.size()) return {};
        const auto type = ExprHelper::canonicalResultTypeName(parameter->declaredType);
        const auto& input = query->parameters[parameter->slot];
        if (input.isNull || ExprHelper::canonicalResultTypeName(input.typeName) != type) return {};
        return parseIntegerPageEndpoint(input.value, type);
    }

    const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
    // Binding absorbs unary signs on bare decimal tokens, including INT64_MIN.
    if (literal && !literal->analyzedPrimitiveConstant && literal->typeName.empty())
        return parseIntegerPageEndpoint(literal->value, ExprHelper::inferValuesResultType(literal->value));

    // The wire frontend expands each Bind into a constant with its declared
    // cast. The equality lookup already proves and decodes these pure inputs.
    std::string type;
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        type = integerIndexCastType(cast->typeName);
    else if (literal && literal->analyzedPrimitiveConstant)
        type = integerIndexCastType(literal->typeName);
    if (type.empty()) return {};
    const auto input = constantIntegerIndexInput(expression, type);
    return input ? parseIntegerPageEndpoint(input->value, std::move(type)) : std::nullopt;
}

static std::optional<IntegerInterval> integerBetweenEndpoints(
        const FunctionCallExpr& between, const PreparedQuery* query) {
    const auto lower = integerPageEndpoint(between.args[1].get(), query);
    const auto upper = integerPageEndpoint(between.args[2].get(), query);
    if (!lower || !upper) return {};
    if (betweenSyntax(between)->symmetric && lower->value > upper->value)
        return std::pair{upper->value, lower->value};
    return std::pair{lower->value, upper->value};
}

static bool isOrdinaryNullTestColumn(const Column& column) {
    if (column.generatedKind || column.isArray || column.isUnsigned || !column.domainName.empty() ||
        !column.enumValues.empty()) return false;
    const auto type = ExprHelper::canonicalResultTypeName(column.dataType);
    return type == "smallint" || type == "integer" || type == "bigint" || type == "boolean" ||
        type == "text" || type == "character varying";
}

static std::optional<StorageEngine::IntegerPartitionFilter> integerKeyPredicate(
    const Expr* expression, size_t source, size_t key, const std::string& keyType,
    const PreparedQuery* inputs, size_t* remaining) {
    using Filter = StorageEngine::IntegerPartitionFilter;
    if (!expression || expression->preparedSubquery || (remaining && !*remaining)) return std::nullopt;
    if (remaining) --*remaining;
    const auto oid = mapBuiltinTypeNameToOid(keyType);
    const auto isKey = [&](const Expr* operand) {
        const auto* column = dynamic_cast<const ColumnRefExpr*>(operand);
        if (!column || column->preparedSubquery || column->generatedRead || !column->binding) return false;
        const auto& binding = *column->binding;
        // Physical scans can lack a catalog OID in the binder. Here the caller
        // has already proved the declared type and width of this exact slot.
        const auto& declaration = binding.typeMetadata;
        return !binding.scopeDepth && !binding.mergedUsing && binding.sourceOrdinal == source &&
            binding.columnOrdinal == key &&
            ExprHelper::canonicalResultTypeName(binding.declaredType) == keyType &&
            (!binding.typeOid || binding.typeOid == oid) &&
            (!declaration || (declaration->actualOid == oid && declaration->codecOid == oid &&
                declaration->kind == DatumKind::Scalar && !declaration->enumType));
    };
    if (const auto* between = rangePredicateCall(expression)) {
        if (!isKey(between->args.front().get()) ||
            (between->args.size() == 4 && !isKey(between->args.back().get()))) return {};
        const auto interval = integerBetweenEndpoints(*between, inputs);
        if (!interval) return {};
        if (betweenSyntax(*between)->negated) return outsideIntegerInterval(interval->first, interval->second);
        if (interval->first > interval->second) return Filter{};
        return Filter{{*interval}, false};
    }
    if (const auto* test = dynamic_cast<const UnaryOpExpr*>(expression)) {
        if (!isKey(test->operand.get())) return std::nullopt;
        if (test->op == "IS NULL") return Filter{{}, true};
        if (test->op == "IS NOT NULL") return Filter{{{INT64_MIN, INT64_MAX}}, false};
        return std::nullopt;
    }
    const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
    if (!binary || binary->rowComparison || binary->predicateCollation) return std::nullopt;
    if (binary->op == "AND" || binary->op == "OR") {
        auto left = integerKeyPredicate(binary->left.get(), source, key, keyType, inputs, remaining);
        if (!left) return std::nullopt;
        const auto right = integerKeyPredicate(binary->right.get(), source, key, keyType, inputs, remaining);
        if (!right) return std::nullopt;
        if (binary->op == "AND") left->intersect(*right);
        else left->unite(*right);
        return left;
    }
    std::string_view op = binary->op;
    if (op != "=" && op != "<>" && op != "!=" && op != "<" && op != "<=" && op != ">" && op != ">=")
        return std::nullopt;
    const bool keyOnLeft = isKey(binary->left.get());
    if (!keyOnLeft && !isKey(binary->right.get())) return std::nullopt;
    const auto* input = keyOnLeft ? binary->right.get() : binary->left.get();
    // An external execution provider may supply a different parameter owner.
    const auto endpoint = integerPageEndpoint(input, inputs);
    if (!endpoint) return std::nullopt;
    if (binary->comparison) {
        const auto& binding = *binary->comparison;
        const auto& leftType = keyOnLeft ? keyType : endpoint->type;
        const auto& rightType = keyOnLeft ? endpoint->type : keyType;
        const auto boundOp = op == "!=" ? std::string("<>") : binary->op;
        if (!binding.strict || binding.enumTypeOid || !binding.collation.empty() ||
            binding.op != boundOp || binding.leftType != leftType || binding.rightType != rightType ||
            binding.identity != "builtin-comparison:" + boundOp + "(" + leftType + "," + rightType + ")")
            return std::nullopt;
    }
    if (!keyOnLeft) {
        if (op == "<") op = ">";
        else if (op == "<=") op = ">=";
        else if (op == ">") op = "<";
        else if (op == ">=") op = "<=";
    }
    return Filter::comparison(op, endpoint->value);
}

static bool intersectIntegerPageBounds(const Expr* expression, const QueryColumnBinding& key,
                                       const TableSchema& schema, const PreparedQuery& query,
                                       IntegerPageBounds& bounds) {
    if (!expression || expression->preparedSubquery) return false;
    if (const auto* between = rangePredicateCall(expression)) {
        const auto* column = boundIntegerBetweenColumn(*between, schema, key.sourceOrdinal);
        if (!column || column->binding->columnOrdinal != key.columnOrdinal) return false;
        const auto endpoints = integerBetweenEndpoints(*between, &query);
        if (!endpoints) return false;
        if (betweenSyntax(*between)->negated) bounds.exclude(*endpoints);
        else {
            bounds.constrain(true, true, endpoints->first);
            bounds.constrain(false, true, endpoints->second);
        }
        return true;
    }
    if (const auto* test = dynamic_cast<const UnaryOpExpr*>(expression)) {
        if (test->op != "IS NULL" && test->op != "IS NOT NULL") return false;
        const auto* column = dynamic_cast<const ColumnRefExpr*>(test->operand.get());
        if (!column || column->preparedSubquery || !column->binding) return false;
        const auto& input = *column->binding;
        if (input.scopeDepth || input.mergedUsing || input.sourceOrdinal != key.sourceOrdinal ||
            input.columnOrdinal >= schema.len() || !input.typeMetadata ||
            input.typeMetadata->kind != DatumKind::Scalar || input.typeMetadata->enumType ||
            input.typeMetadata->actualOid != input.typeMetadata->codecOid ||
            !isOrdinaryNullTestColumn(schema.cols[input.columnOrdinal])) return false;
        bounds.nullTests.push_back({input.columnOrdinal, test->op == "IS NULL"});
        return true;
    }
    const auto* comparison = dynamic_cast<const BinaryOpExpr*>(expression);
    if (!comparison || comparison->rowComparison) return false;
    const auto& op = comparison->op;
    if (op == "AND")
        return intersectIntegerPageBounds(comparison->left.get(), key, schema, query, bounds) &&
            intersectIntegerPageBounds(comparison->right.get(), key, schema, query, bounds);
    if (op == "OR") {
        size_t remaining = maxRangePredicateNodes;
        auto filter = integerKeyPredicate(expression, key.sourceOrdinal, key.columnOrdinal,
            ExprHelper::canonicalResultTypeName(key.declaredType), &query, &remaining);
        if (!filter || (filter->matchesNull && schema.cols[key.columnOrdinal].isNull)) return false;
        bounds.intersect(std::move(*filter));
        bounds.hasDisjunction = true;
        return true;
    }
    if (op != "<" && op != "<=" && op != ">" && op != ">=") return false;

    const auto* column = dynamic_cast<const ColumnRefExpr*>(comparison->left.get());
    const bool endpointFirst = !column;
    if (endpointFirst) column = dynamic_cast<const ColumnRefExpr*>(comparison->right.get());
    if (!column || column->preparedSubquery || !column->binding) return false;
    const auto& input = *column->binding;
    if (input.scopeDepth != key.scopeDepth || input.mergedUsing || input.sourceOrdinal != key.sourceOrdinal ||
        input.columnOrdinal != key.columnOrdinal || input.typeOid != key.typeOid ||
        input.declaredType != key.declaredType) return false;

    const auto endpoint = integerPageEndpoint(
        endpointFirst ? comparison->left.get() : comparison->right.get(), &query);
    if (!endpoint) return false;
    if (comparison->comparison) {
        const auto keyType = ExprHelper::canonicalResultTypeName(key.declaredType);
        const auto& leftType = endpointFirst ? endpoint->type : keyType;
        const auto& rightType = endpointFirst ? keyType : endpoint->type;
        const auto& binding = *comparison->comparison;
        if (!binding.strict || binding.enumTypeOid || binding.op != op ||
            binding.leftType != leftType || binding.rightType != rightType ||
            binding.identity != "builtin-comparison:" + op + "(" + leftType + "," + rightType + ")") return false;
    }

    const bool isLower = (op.front() == '>') != endpointFirst;
    bounds.constrain(isLower, op.size() == 2, endpoint->value);
    return true;
}

static std::optional<IntegerPageBounds> integerPageBounds(const PreparedQuery& query, const SelectStmt& select,
                                                          const QueryColumnBinding& key, const TableSchema& schema) {
    IntegerPageBounds bounds;
    if (select.whereClause &&
        (!intersectIntegerPageBounds(select.whereClause.get(), key, schema, query, bounds) ||
         !bounds.applyRanges(schema, key.columnOrdinal))) return {};
    // The existing filter eagerly decodes stored inputs. Only use bitmap
    // rejection if that decoding requires no array/domain/reference handling.
    if (!bounds.nullTests.empty())
        for (size_t i = 0; i < schema.len(); ++i)
            if (!isOrdinaryNullTestColumn(schema.cols[i])) return {};
    return bounds;
}

static const PreparedQuery::SourceRange* integerPageRelation(const PreparedQuery& query,
        const SelectStmt& select, const QueryColumnBinding& key) {
    if (key.scopeDepth || key.mergedUsing) return nullptr;
    const auto range = std::find_if(query.sourceRanges.begin(), query.sourceRanges.end(),
        [&](const auto& item) {
            return item.owner == &select && item.source == select.fromClause.get() && item.ordinal == key.sourceOrdinal;
        });
    if (range == query.sourceRanges.end() || range->relationName.empty() || range->cteStatement || range->viewQuery ||
        range->relationSchema == "pg_catalog" || range->relationSchema == "pg_temp" ||
        (currentSession() && range->relationSchema == sessionTempSchemaName(*currentSession()))) return nullptr;
    return &*range;
}

static bool eligibleIntegerPageKey(const TableSchema& schema, const QueryColumnBinding& key) {
    if (schema.partitionType != TableSchema::PartitionType::None || !schema.physicalRelationId ||
        key.columnOrdinal >= schema.len()) return false;
    const auto& column = schema.cols[key.columnOrdinal];
    const auto type = ExprHelper::canonicalResultTypeName(column.dataType);
    return (type == "smallint" || type == "integer" || type == "bigint") &&
        ExprHelper::canonicalResultTypeName(key.declaredType) == type && !column.isArray && !column.isUnsigned &&
        !column.generatedKind && column.domainName.empty() && isSingleColumnPrimaryKey(schema, column.dataName);
}

bool QueryPlanner::mayUseIntegerPrimaryKeyPage(StorageEngine* engine, const std::string& database,
        const PreparedQuery& query, const SelectStmt& select) {
    const auto key = integerPageOrder(query, select);
    const auto* range = key ? integerPageRelation(query, select, *key) : nullptr;
    if (!range) return false;
    const auto table = range->relationSchema.empty() || range->relationSchema == "public" ? range->relationName
        : range->relationSchema + "__" + range->relationName;
    const auto schema = engine->getTableSchema(database, table);
    return eligibleIntegerPageKey(schema, *key) && integerPageBounds(query, select, *key, schema).has_value();
}

static std::optional<std::pair<size_t, IntegerPageBounds>> completeIntegerPrimaryRange(
        StorageEngine& engine, const std::string& database, const PreparedQuery& query,
        const SelectStmt& select, const TableSchema& schema) {
    if (!select.whereClause || !select.locking.empty() || !select.fromClause ||
        select.fromClause->type != FromItem::Type::Table || !schema.physicalRelationId ||
        !supportsStoredHeapPredicate(schema) || QueryPlanner::supportsStoredCountSelect(select)) return {};
    const auto range = std::find_if(query.sourceRanges.begin(), query.sourceRanges.end(), [&](const auto& source) {
        return source.owner == &select && source.source == select.fromClause.get() && !source.mergedUsing;
    });
    if (range == query.sourceRanges.end() || range->relationName.empty() || range->cteStatement || range->viewQuery)
        return {};
    const auto predicates = bindStoredWhere(engine, database, schema, range->ordinal, select.whereClause.get(), &query);
    if (!predicates) return {};
    IntegerPageBounds bounds;
    std::optional<size_t> key;
    bool inequality = false;
    using Input = StoredPredicate::Input;
    for (const auto& predicate : *predicates) {
        if (predicate.input == Input::IsNull || predicate.input == Input::IsNotNull) {
            bounds.nullTests.push_back({predicate.column, predicate.input == Input::IsNull});
            continue;
        }
        if (!predicate.integer || (key && predicate.column != *key) ||
            !isSingleColumnPrimaryKey(schema, predicate.name)) return {};
        key = predicate.column;
        if (predicate.input == Input::IntegerRanges) {
            const auto& filter = std::get<StorageEngine::IntegerPartitionFilter>(predicate.constant);
            if (filter.matchesNull && schema.cols[*key].isNull) return {};
            bounds.intersect(filter);
            bounds.hasDisjunction = true;
            inequality = true;
            continue;
        }
        if (predicate.input == Input::IntegerOutsideRange) {
            bounds.exclude(std::get<IntegerInterval>(predicate.constant));
            inequality = true;
            continue;
        }
        const auto endpoint = std::get<int64_t>(predicate.constant);
        switch (predicate.comparison) {
            case StoredComparison::Equal:
                bounds.constrain(true, true, endpoint);
                bounds.constrain(false, true, endpoint);
                continue;
            case StoredComparison::NotEqual: return {};
            case StoredComparison::Less: bounds.constrain(false, false, endpoint); break;
            case StoredComparison::LessEqual: bounds.constrain(false, true, endpoint); break;
            case StoredComparison::Greater: bounds.constrain(true, false, endpoint); break;
            case StoredComparison::GreaterEqual: bounds.constrain(true, true, endpoint); break;
        }
        inequality = true;
    }
    if (!key || !inequality || !bounds.applyRanges(schema, *key) ||
        (!bounds.hasDisjunction && !bounds.bounded()))
        return {};
    return std::pair<size_t, IntegerPageBounds>{*key, std::move(bounds)};
}

bool QueryPlanner::mayUseIntegerPrimaryRangeAlternatives(StorageEngine& engine, const std::string& database,
        const PreparedQuery& query, const SelectStmt& select, const TableSchema& schema) {
    const auto range = completeIntegerPrimaryRange(engine, database, query, select, schema);
    if (range && (range->second.hasDisjunction || !range->second.disjointRanges().empty())) return true;

    // The compact OR executor can combine alternative scans and sort an
    // integer key as display text. Keep the bound sort even when an OR arm
    // uses a different column or a non-integer endpoint and needs a scan.
    if (!select.locking.empty() || !supportsStoredHeapPredicate(schema) || select.orderBy.size() != 1)
        return false;
    const auto key = preparedDirectOrderColumn(query, select);
    if (!key || !integerPageRelation(query, select, *key) || !eligibleIntegerPageKey(schema, *key))
        return false;
    const auto hasDisjunction = [](const auto& self, const Expr* expression) -> bool {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
        if (!binary || binary->preparedSubquery) return false;
        return binary->op == "OR" || (binary->op == "AND" &&
            (self(self, binary->left.get()) || self(self, binary->right.get())));
    };
    return hasDisjunction(hasDisjunction, select.whereClause.get());
}

class IntegerPrimaryKeyPageOp final : public Operator {
    StorageEngine* engine_;
    std::string database_;
    TableSchema schema_;
    size_t keyColumn_, count_;
    bool descending_;
    IntegerPageBounds bounds_;
    OpPtr fallback_;
    const Expr* rangePredicate_;
    std::vector<bool> projectedColumns_;
    std::optional<OrderedPrimaryKeyReadPage> page_;
    size_t position_ = 0, indexEntries_ = 0, indexNodesVisited_ = 0, heapRows_ = 0;
    bool fallbackOpen_ = false, usedPage_ = false, orderedIndex_ = false;
    int64_t lastRid_ = 0;
    std::vector<bool> lastNulls_;
public:
    IntegerPrimaryKeyPageOp(StorageEngine* engine, std::string database, const TableSchema& schema,
            size_t keyColumn, size_t count, bool descending, IntegerPageBounds bounds, OpPtr fallback,
            const std::vector<bool>& projectedColumns, const Expr* rangePredicate = nullptr)
        : engine_(engine), database_(std::move(database)), schema_(schema), keyColumn_(keyColumn),
          count_(count), descending_(descending), bounds_(std::move(bounds)), fallback_(std::move(fallback)),
          rangePredicate_(rangePredicate), projectedColumns_(projectedColumns) {}
    std::optional<PreparedIntegerKeyOrder> openedKeyOrder() const {
        // Both signed-key scans and successful legacy numeric prefixes
        // return checked heap rows in key order. A heap fallback does not.
        if (!page_ || rangePredicate_) return {};
        return PreparedIntegerKeyOrder{keyColumn_, !descending_};
    }
    bool open() override {
        OpenInstrument instrument(this);
        clearError();
        close();
        usedPage_ = orderedIndex_ = false;
        indexEntries_ = indexNodesVisited_ = heapRows_ = 0;
        page_ = engine_->tryReadOrderedIntegerPrimaryKey(database_, schema_.tablename, schema_,
            keyColumn_, descending_, count_, sortWorkMemBytes(), bounds_.lower, bounds_.upper, bounds_.nullTests,
            rangePredicate_ != nullptr, bounds_.disjointRanges(), projectedColumns_);
        if (page_) {
            usedPage_ = true;
            orderedIndex_ = page_->orderedIndex;
            indexEntries_ = page_->indexEntries;
            indexNodesVisited_ = page_->indexNodesVisited;
            heapRows_ = page_->heapRowsFetched;
            return true;
        }
        fallbackOpen_ = true;
        return fallback_->open() || propagateChildError(fallback_.get(), "ordered primary key heap scan failed");
    }
    bool next(std::string& raw) override {
        NextInstrument instrument(this);
        if (!page_) {
            if (!fallback_->next(raw)) {
                if (fallback_->hasError()) return propagateChildError(fallback_.get(), "ordered primary key heap scan failed");
                return false;
            }
        } else {
            if (position_ == page_->rows.size()) { lastRid_ = 0; return false; }
            auto& row = page_->rows[position_++];
            lastRid_ = row.rid;
            lastNulls_ = std::move(row.nulls);
            raw = std::move(row.data);
        }
        instrument.emitted = true;
        return true;
    }
    void close() override {
        page_.reset();
        position_ = 0;
        lastRid_ = 0;
        lastNulls_.clear();
        if (fallbackOpen_) {
            fallbackOpen_ = false;
            fallback_->close();
        }
    }
    bool lastColumnIsNull(size_t column) const override {
        if (!page_) return fallback_->lastColumnIsNull(column);
        return column < lastNulls_.size() && lastNulls_[column];
    }
    bool lastStoredNullBitmap(std::vector<bool>& bitmap) const override {
        if (!page_) return fallback_->lastStoredNullBitmap(bitmap);
        if (!lastRid_) return false;
        bitmap = lastNulls_;
        return true;
    }
    const Expr* lastSatisfiedWhere() const override {
        return page_ ? (lastRid_ ? rangePredicate_ : nullptr) : fallback_->lastSatisfiedWhere();
    }
    ScanOrigin scanOrigin() const override {
        if (!page_) return fallback_->scanOrigin();
        ScanOrigin origin;
        origin.engine = engine_;
        origin.dbname = database_;
        origin.tablename = schema_.tablename;
        origin.rid = lastRid_;
        return origin;
    }
    std::string preparedPlanNodeName() const override {
        return rangePredicate_ ? "IntegerPrimaryKeyRange" : "IntegerPrimaryKeyPage";
    }
    std::vector<Operator*> preparedPlanChildren() const override { return {fallback_.get()}; }
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        const char* used = rangePredicate_ ? "usedPrimaryKeyRange" : "usedPrimaryKeyPage";
        if (!usedPage_) return {{used, "false"}};
        std::map<std::string, std::string> attributes{{used, "true"},
            {"orderedIndex", orderedIndex_ ? "true" : "false"}, {"indexEntries", std::to_string(indexEntries_)},
            {"heapRowsFetched", std::to_string(heapRows_)}};
        if (orderedIndex_) attributes.emplace("indexNodesVisited", std::to_string(indexNodesVisited_));
        if (bounds_.bounded()) attributes.emplace("indexKeyRange", "true");
        if (!projectedColumns_.empty())
            attributes.emplace("projectedColumns", std::to_string(std::count(projectedColumns_.begin(),
                projectedColumns_.end(), true)));
        if (!bounds_.disjointRanges().empty())
            attributes.emplace("indexRanges", std::to_string(bounds_.disjointRanges().size()));
        if (!bounds_.nullTests.empty()) attributes.emplace("indexNullFilter", "true");
        return attributes;
    }
};

std::optional<PreparedIntegerKeyOrder> openedIntegerPrimaryKeyOrder(const Operator& source) {
    if (const auto* page = dynamic_cast<const IntegerPrimaryKeyPageOp*>(&source))
        return page->openedKeyOrder();
    if (const auto* composite = dynamic_cast<const CompositePrimaryKeyScanOp*>(&source))
        return composite->openedKeyOrder();
    return {};
}

namespace {
std::optional<PreparedIntegerKeyOrder> matchingPrimaryKeyInputOrder(
        const Operator* source, const PreparedSelectState& state) {
    if (const auto* filter = dynamic_cast<const PreparedFilterOp*>(source))
        source = filter->openedSource();
    if (!source || state.keys.size() != 1) return {};

    const auto& key = state.keys.front();
    const auto* column = dynamic_cast<const ColumnRefExpr*>(key.target == PreparedSelectState::noTarget
        ? key.expression : state.targets.at(key.target));
    if (key.enumComparison || !column || column->preparedSubquery || !column->binding) return {};
    const auto& binding = *column->binding;
    if (binding.scopeDepth || binding.mergedUsing) return {};

    std::optional<PreparedIntegerKeyOrder> order;
    if (const auto* logical = dynamic_cast<const PreparedOrderedTableInput*>(source))
        order = logical->integerKeyOrder(binding.sourceOrdinal);
    else if (!state.sourceContexts && binding.sourceOrdinal == state.sourceOrdinal)
        order = openedIntegerPrimaryKeyOrder(*source);
    if (!order || order->columnOrdinal != binding.columnOrdinal || order->ascending != key.asc)
        return {};
    return order;
}
} // namespace

bool QueryPlanner::supportsStoredCountSelect(const SelectStmt& select) {
    if (!select.fromClause || select.fromClause->type != FromItem::Type::Table ||
        select.setOp != SetOp::None || !select.ctes.empty() || !select.groupBy.empty() ||
        !select.groupByElems.empty() || select.having || !select.orderBy.empty() ||
        select.distinct || !select.distinctOn.empty() || !select.locking.empty() ||
        !select.windowDefs.empty() || select.withTies || select.existsTargetPrunable ||
        select.selectList.empty()) return false;
    return std::all_of(select.selectList.begin(), select.selectList.end(), [](const auto& item) {
        const auto* call = dynamic_cast<const FunctionCallExpr*>(item.expr.get());
        if (!call || call->preparedSubquery || aggregate_type_detail::builtinName(call) != "count" ||
            call->args.size() != 1 || !call->namedArgs.empty() || call->hasOver || call->setReturning ||
            call->distinct || !call->orderBy.empty() || !call->aggregateOrderKeys.empty()) return false;
        const auto* argument = call->args.front().get();
        const auto* literal = dynamic_cast<const LiteralExpr*>(argument);
        return (literal && literal->value == "*") || dynamic_cast<const ColumnRefExpr*>(argument);
    });
}

static std::optional<StoredPredicates> bindStoredCountFilter(StorageEngine& engine,
        const std::string& database, const TableSchema& schema, size_t source, const FunctionCallExpr& call) {
    auto filter = bindStoredWhere(engine, database, schema, source, call.filter.get());
    if (!filter) return {};
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(call.args.front().get())) {
        if (!boundStoredPredicateColumn(column, schema, source)) return {};
        const auto ordinal = column->binding->columnOrdinal;
        filter->push_back({ordinal, column->column, schema.cols[ordinal].dataType,
            StoredPredicate::Input::IsNotNull, StoredComparison::Equal, {}});
    }
    return filter;
}

bool QueryPlanner::usesStoredCountSource(StorageEngine& engine, const std::string& database,
        const SelectStmt& select, const TableSchema& schema, size_t sourceOrdinal) {
    if (!supportsStoredCountSelect(select) || !schema.physicalRelationId ||
        !supportsStoredHeapPredicate(schema) || schema.rowLevelSecurity || schema.forceRowLevelSecurity ||
        !bindStoredWhere(engine, database, schema, sourceOrdinal, select.whereClause.get())) return false;
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(database);
    return std::all_of(select.selectList.begin(), select.selectList.end(), [&](const auto& item) {
        const auto& call = static_cast<const FunctionCallExpr&>(*item.expr);
        return !evaluator.hasScalarFunction(&call, &engine) &&
            bindStoredCountFilter(engine, database, schema, sourceOrdinal, call).has_value();
    });
}

class PreparedStoredCountOp final : public Operator {
    OpPtr fallback_;
    TableScanOp* scan_;
    std::shared_ptr<PreparedSelectState> state_;
    StoredPredicates where_;
    std::vector<StoredPredicates> filters_;
    std::vector<bool> impossibleFilters_;
    RowContext result_;
    bool opened_ = false, stored_ = false, emitted_ = false, emptyInput_ = false;

public:
    PreparedStoredCountOp(OpPtr fallback, TableScanOp* scan,
                          std::shared_ptr<PreparedSelectState> state, StoredPredicates where,
                          std::vector<StoredPredicates> filters)
        : fallback_(std::move(fallback)), scan_(scan), state_(std::move(state)),
          where_(std::move(where)), filters_(std::move(filters)) {}

    ~PreparedStoredCountOp() override { try { close(); } catch (...) {} }

    bool open() override {
        OpenInstrument instrument(this);
        if (opened_) close();
        clearError();
        emitted_ = stored_ = emptyInput_ = false;
        opened_ = true;
        if (!fallback_->open()) return propagateChildError(fallback_.get(), "count input failed");
        const auto& schema = scan_->tbl_;
        stored_ = scan_->cursor_ && schema.physicalRelationId == state_->schema.physicalRelationId &&
            supportsStoredHeapPredicate(schema) && storedPredicateSchemaMatches(where_, schema) &&
            std::all_of(filters_.begin(), filters_.end(), [&](const auto& filter) {
                return storedPredicateSchemaMatches(filter, schema);
            });
        if (stored_) {
            emptyInput_ = storedPredicatesContradict(schema, where_, {});
            if (!emptyInput_) {
                impossibleFilters_.assign(filters_.size(), false);
                for (size_t i = 0; i < filters_.size(); ++i)
                    if (!filters_[i].empty())
                        impossibleFilters_[i] = storedPredicatesContradict(schema, where_, filters_[i]);
                emptyInput_ = std::all_of(impossibleFilters_.begin(), impossibleFilters_.end(),
                    [](bool impossible) { return impossible; });
            }
            // The scan's open step has acquired the relation lock and read
            // view. Release the unused cursor, retaining the lock until close.
            if (emptyInput_) scan_->cursor_.reset();
        }
        return true;
    }

    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if (emitted_) return false;
        if (stored_) {
            std::vector<int64_t> counts(filters_.size(), 0);
            std::string row;
            const auto& schema = scan_->tbl_;
            while (!emptyInput_ && scan_->next(row)) {
                checkForQueryInterrupt();
                const auto& bitmap = scan_->lastNulls_;
                const auto match = [&](const StoredPredicates& predicates) -> std::optional<bool> {
                    if (predicates.empty()) return true;
                    if (bitmap.size() != schema.len()) return {};
                    return storedPredicatesMatch(predicates, *state_->engine, state_->dbname,
                        schema, row, bitmap);
                };
                // An unavailable comparison can use the ordinary evaluator
                // for this row under the same relation lock and read view.
                std::optional<RowContext> ordinary;
                const auto context = [&]() -> const RowContext& {
                    if (!ordinary) ordinary.emplace(state_->context(*scan_, row));
                    return *ordinary;
                };
                auto where = match(where_);
                if (!where) {
                    const auto value = evaluatePreparedWhere(*state_, state_->select().whereClause.get(), context());
                    where = !value.isNull && value.asBool();
                }
                if (!*where) continue;
                for (size_t i = 0; i < counts.size(); ++i) {
                    if (impossibleFilters_[i]) continue;
                    auto selected = match(filters_[i]);
                    if (!selected) {
                        const auto& call = *state_->aggregates[i].call;
                        if (call.filter) {
                            const auto filter = state_->evaluate(call.filter.get(), context());
                            if (filter.isNull || !filter.asBool()) continue;
                        }
                        const auto* column = dynamic_cast<const ColumnRefExpr*>(call.args.front().get());
                        selected = !column || !state_->evaluate(column, context()).isNull;
                    }
                    if (*selected) incrementAggregateCount(counts[i]);
                }
            }
            if (scan_->hasError()) return propagateChildError(scan_, "count scan failed");
            result_ = state_->execution->context(state_->outerRow);
            for (size_t i = 0; i < counts.size(); ++i) {
                const auto& column = state_->aggregates[i].column;
                result_.setBoundColumn(column.sourceOrdinal, column.columnOrdinal,
                    ExprValue("bigint", std::to_string(counts[i])));
            }
            display.clear();
            emitted_ = instrument.emitted = true;
            return true;
        }
        if (!fallback_->next(display)) {
            if (fallback_->hasError()) return propagateChildError(fallback_.get(), "count input failed");
            return false;
        }
        emitted_ = instrument.emitted = true;
        return true;
    }

    void close() override {
        if (!opened_) return;
        opened_ = false;
        fallback_->close();
    }
    bool supportsPreparedContexts() const override { return true; }
    bool lastPreparedContext(RowContext& row) const override {
        if (!emitted_) return false;
        if (!stored_) return fallback_->lastPreparedContext(row);
        row = result_;
        return true;
    }
    std::string preparedPlanNodeName() const override { return "StoredCount"; }
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        if (emptyInput_) return {{"emptyInput", "true"}};
        return {};
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        return {stored_ ? static_cast<Operator*>(scan_) : fallback_.get()};
    }
};

// The outer filter keeps responsibility for the SQL predicate. This source
// also checks each heap value against the physical interval before emitting it.
class ZonedClockRangeScanOp final : public Operator {
public:
    ZonedClockRangeScanOp(StorageEngine& engine, std::string database,
                          const TableSchema& schema, StoredZonedClockRange range, OpPtr fallback)
        : engine_(engine), database_(std::move(database)), table_(schema.tablename),
          relationId_(schema.physicalRelationId), range_(std::move(range)), fallback_(std::move(fallback)) {}

    ~ZonedClockRangeScanOp() override { try { close(); } catch (...) {} }

    bool open() override {
        OpenInstrument instrument(this);
        close();
        clearError();
        usedIndex_ = false;
        auto& locks = engine_.getLockManager();
        locks.setResourceNamespace(database_);
        if (!locks.lockShared(table_)) throw DbError("55P03", "could not obtain lock on relation \"" + table_ + "\"");
        locked_ = true;
        schema_ = engine_.getTableSchema(database_, table_);
        column_ = 0;
        while (column_ < schema_.len() && schema_.cols[column_].dataName != range_.column) ++column_;
        if (column_ < schema_.len() && schema_.physicalRelationId == relationId_ &&
            engine_.getInheritedChildren(database_, table_).empty()) {
            engine_.noteQuerySnapshot();
            if (auto positions = readStoredZonedClockRange(engine_, database_, schema_, range_)) {
                rids_ = std::move(*positions);
                // Read heap pages in physical order. A requested SQL order
                // still belongs to the ordinary sort above this source.
                std::sort(rids_.begin(), rids_.end());
                rids_.erase(std::unique(rids_.begin(), rids_.end()), rids_.end());
                usedIndex_ = true;
                recordTableScan(database_, table_, rids_.size(), true, false);
                return true;
            }
        }
        fallbackOpened_ = true;
        return fallback_->open() || propagateChildError(fallback_.get(), "range heap source failed");
    }

    bool next(std::string& row) override {
        NextInstrument instrument(this);
        if (fallbackOpened_) {
            instrument.emitted = fallback_->next(row);
            if (!instrument.emitted && fallback_->hasError()) return propagateChildError(fallback_.get(), "range heap source failed");
            return instrument.emitted;
        }
        while (position_ < rids_.size()) {
            checkForQueryInterrupt();
            const auto rid = rids_[position_++];
            bool failed = false;
            if (!engine_.readIndexedRowByRid(database_, table_, rid, row, schema_, nullptr, &failed)) {
                if (failed) {
                    setError("indexed heap fetch failed");
                    return false;
                }
                continue;
            }
            lastRid_ = rid;
            StorageEngine::bindNullRow(&engine_, database_, table_, rid, schema_.len());
            if (lastColumnIsNull(column_) || !range_.matches(engine_.extractColumnValue(
                    row, schema_, column_, database_, true))) continue;
            bool toastOk = false;
            row = engine_.resolveToastValues(database_, table_, row, schema_, &toastOk);
            if (!toastOk) {
                setError("TOAST value read failed");
                return false;
            }
            instrument.emitted = true;
            return true;
        }
        return false;
    }

    ScanOrigin scanOrigin() const override {
        if (fallbackOpened_) return fallback_->scanOrigin();
        return {&engine_, database_, table_, lastRid_};
    }
    bool lastColumnIsNull(size_t ordinal) const override {
        if (fallbackOpened_) return fallback_->lastColumnIsNull(ordinal);
        return lastRid_ > 0 && ordinal < schema_.len() &&
            detail::storedRowColumnIsNull(engine_, database_, table_, schema_, lastRid_, ordinal);
    }
    void close() override {
        std::exception_ptr error;
        try { if (std::exchange(fallbackOpened_, false)) fallback_->close(); }
        catch (...) { error = std::current_exception(); }
        if (lastRid_) StorageEngine::unbindNullRow();
        rids_.clear(); position_ = 0; lastRid_ = 0;
        if (std::exchange(locked_, false)) {
            auto& locks = engine_.getLockManager();
            locks.setResourceNamespace(database_);
            locks.unlock(table_);
        }
        if (error) std::rethrow_exception(error);
    }
    std::string preparedPlanNodeName() const override { return "ZonedClockRangeScan"; }
    std::map<std::string, std::string> preparedPlanAttributes() const override {
        return {{"usedIndex", usedIndex_ ? "true" : "false"}, {"column", range_.column}};
    }
    std::vector<Operator*> preparedPlanChildren() const override {
        return usedIndex_ ? std::vector<Operator*>{} : std::vector<Operator*>{fallback_.get()};
    }
    std::vector<IOperator*> children() override {
        return usedIndex_ ? std::vector<IOperator*>{} : std::vector<IOperator*>{fallback_.get()};
    }

private:
    StorageEngine& engine_;
    std::string database_, table_;
    uint64_t relationId_;
    StoredZonedClockRange range_;
    OpPtr fallback_;
    TableSchema schema_;
    std::vector<int64_t> rids_;
    size_t position_ = 0, column_ = 0;
    int64_t lastRid_ = 0;
    bool locked_ = false, fallbackOpened_ = false, usedIndex_ = false;
};

static OpPtr makePreparedTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, SelectStmt* select,
    const TableSchema& schema, OpPtr source, const std::vector<bool>& projectedColumns) {
    if (auto narrowed = preparedCompositePrimarySource(engine, database, query, *select, schema, source, false))
        return narrowed;
    if (auto narrowed = literalBrinTableSource(engine, database, query, select, schema, source))
        return narrowed;
    if (auto narrowed = literalGiSTTableSource(engine, database, query, select, schema, source))
        return narrowed;
    if (auto narrowed = literalLikeTableSource(engine, database, query, select,
                                               schema, source))
        return narrowed;
    if (auto narrowed = preparedCompositePrimarySource(engine, database, query, *select, schema, source, true))
        return narrowed;
    const auto heap = [&] {
        auto filtered = prefilterPreparedHeap(*engine, database, *query, *select, schema, source);
        return filtered ? std::move(filtered) : std::move(source);
    };
    const auto* physical = dynamic_cast<const TableScanOp*>(source.get());
    if (physical && typeid(*physical) == typeid(TableScanOp) && physical->tableName() == schema.tablename) {
        if (select->locking.empty() && select->fromClause && select->fromClause->type == FromItem::Type::Table) {
            for (const auto& sourceRange : query->sourceRanges) {
                if (sourceRange.owner != select || sourceRange.source != select->fromClause.get() ||
                    sourceRange.cteStatement || sourceRange.viewQuery) continue;
                if (auto range = storedZonedClockRange(*engine, database, schema,
                        select->whereClause.get(), sourceRange.ordinal)) {
                    return std::make_unique<ZonedClockRangeScanOp>(*engine, database, schema, std::move(*range), heap());
                }
            }
        }
        const auto key = integerPageOrder(*query, *select);
        if (key && integerPageRelation(*query, *select, *key) && eligibleIntegerPageKey(schema, *key)) {
            if (auto bounds = integerPageBounds(*query, *select, *key, schema)) {
                auto fallback = bounds->hasDisjunction ? heap() : std::move(source);
                return std::make_unique<IntegerPrimaryKeyPageOp>(engine, database, schema, key->columnOrdinal,
                    select->offset.value_or(0) + *select->limit, !select->orderBy.front().asc, std::move(*bounds),
                    std::move(fallback), projectedColumns);
            }
        }
        if (auto range = completeIntegerPrimaryRange(*engine, database, *query, *select, schema);
            range && range->second.bounded()) {
            const auto demand = select->orderBy.empty() ? simpleIntegerScanDemand(*select) : std::nullopt;
            return std::make_unique<IntegerPrimaryKeyPageOp>(engine, database, schema, range->first,
                demand.value_or(std::numeric_limits<size_t>::max()), false, std::move(range->second),
                heap(), projectedColumns, select->whereClause.get());
        }
    }
    const auto* equality = dynamic_cast<const BinaryOpExpr*>(select->whereClause.get());
    if (!physical || !select->fromClause || select->fromClause->type != FromItem::Type::Table ||
        !equality || equality->op != "=" || schema.partitionType != TableSchema::PartitionType::None)
        return heap();
    const auto* column = dynamic_cast<const ColumnRefExpr*>(equality->left.get());
    const Expr* input = equality->right.get();
    if (!column) {
        column = dynamic_cast<const ColumnRefExpr*>(equality->right.get());
        input = equality->left.get();
    }
    const auto* parameter = dynamic_cast<const ParameterExpr*>(input);
    if (!column || !column->binding || column->binding->scopeDepth ||
        column->binding->mergedUsing || column->binding->columnOrdinal >= schema.len())
        return heap();
    bool sameRange = false;
    for (const auto& range : query->sourceRanges)
        if (range.owner == select && range.source == select->fromClause.get() &&
            range.ordinal == column->binding->sourceOrdinal) sameRange = true;
    const auto& key = schema.cols[column->binding->columnOrdinal];
    const auto type = ExprHelper::canonicalResultTypeName(key.dataType);
    if (!sameRange || key.isArray || key.isUnsigned || !key.domainName.empty() || key.generatedKind ||
        ExprHelper::canonicalResultTypeName(column->binding->declaredType) != type) return heap();
    if (equality->comparison && (equality->comparison->identity !=
        "builtin-comparison:=(" + type + "," + type + ")" || !equality->comparison->strict))
        return heap();
    const auto table = physical->tableName();
    const bool zonedClock = type == "timetz";
    if (zonedClock) {
        const auto& declaration = column->binding->typeMetadata;
        if (typeid(*physical) != typeid(TableScanOp) || table != schema.tablename ||
            !equality->comparison || equality->comparison->enumTypeOid || equality->rowComparison ||
            equality->preparedSubquery || column->generatedRead || column->generatedBaseDeclaration ||
            !declaration || declaration->actualOid != 1266 || declaration->codecOid != 1266 ||
            !select->locking.empty() || !completeStoredZonedClockIndex(*engine, database, schema, key)) return heap();
    } else if ((type != "smallint" && type != "integer" && type != "bigint") ||
               !isSingleColumnPrimaryKey(schema, key.dataName)) return heap();
    if (!parameter) {
        std::optional<ExprValue> value;
        if (zonedClock) {
            if (auto clock = constantStoredZonedClockInput(*engine, database, input))
                value.emplace("timetz", std::move(*clock));
        } else {
            value = constantIntegerIndexInput(input, type);
        }
        if (!value) return heap();
        auto frozen = std::make_shared<PreparedQuery>();
        frozen->parameters.push_back(*value);
        return std::make_unique<PreparedParameterIndexScanOp>(engine, database, table,
            key.dataName, type, std::move(frozen), 0, std::move(source), schema.physicalRelationId);
    }
    if (parameter->origin != ParameterOrigin::StatementInput ||
        parameter->slot >= query->parameters.size() ||
        ExprHelper::canonicalResultTypeName(parameter->declaredType) != type) return heap();
    return std::make_unique<PreparedParameterIndexScanOp>(engine, database, table,
        key.dataName, type, query, parameter->slot, std::move(source), schema.physicalRelationId);
}

OpPtr makePreparedTableSource(StorageEngine* engine, const std::string& database,
    const std::shared_ptr<PreparedQuery>& query, SelectStmt* select,
    const TableSchema& schema, OpPtr source) {
    // The raw-row consumers need the complete physical value envelope.
    return makePreparedTableSource(engine, database, query, select, schema, std::move(source), {});
}

static std::vector<bool> projectedPrimaryKeyReadColumns(const PreparedSelectState& state) {
    const auto& select = *state.statement;
    const auto& schema = state.schema;
    if (state.sourceContexts || state.sourceOrdinal == PreparedSelectState::noTarget ||
        !schema.hasPrimaryKey() || !schema.hasVariableLength() || schema.formatVersion != DATA_FILE_FORMAT_VERSION ||
        !state.aggregates.empty() || !state.windows.empty() || !select.locking.empty() || select.having ||
        !select.groupBy.empty() || !select.groupByElems.empty() || select.limitExpr || select.offsetExpr ||
        (!select.whereClause && state.keys.empty())) return {};

    std::vector<bool> columns(schema.len(), false);
    bool complete = true;
    const auto visit = [&](const auto& self, Expr* value) -> void {
        if (!value || !complete) return;
        if (value->preparedSubquery) { complete = false; return; }
        switch (value->type) {
            case ExprType::ColumnRef: {
                const auto& column = static_cast<const ColumnRefExpr&>(*value);
                const auto& input = column.binding;
                if (!input || input->scopeDepth || input->mergedUsing || input->sourceOrdinal != state.sourceOrdinal ||
                    input->columnOrdinal >= schema.len() || column.generatedRead || column.generatedBaseDeclaration ||
                    schema.cols[input->columnOrdinal].generatedKind == 'v' ||
                    schema.cols[input->columnOrdinal].dataName != column.column) {
                    complete = false;
                    return;
                }
                columns[input->columnOrdinal] = true;
                return;
            }
            case ExprType::Literal:
                complete = static_cast<const LiteralExpr&>(*value).value != "*";
                return;
            case ExprType::Parameter:
                return;
            case ExprType::FunctionCall: {
                const auto& call = static_cast<const FunctionCallExpr&>(*value);
                if (call.hasOver || call.setReturning || call.filter || !call.orderBy.empty() ||
                    !call.aggregateOrderKeys.empty()) { complete = false; return; }
                break;
            }
            case ExprType::UnaryOp: case ExprType::BinaryOp: case ExprType::CaseExpr:
            case ExprType::CastExpr: case ExprType::ArrayExpr: case ExprType::RowExpr:
            case ExprType::QuantifiedComparison:
                break;
            default:
                complete = false;
                return;
        }
        prepared_window_detail::spec_detail::valueChildren(value, [&](Expr* child) { self(self, child); });
    };
    for (auto* target : state.targets) visit(visit, target);
    visit(visit, select.whereClause.get());
    for (const auto& key : state.keys)
        if (key.target == PreparedSelectState::noTarget) visit(visit, key.expression);
    for (const auto& key : select.distinctOn) visit(visit, key.get());
    if (!complete) return {};
    for (size_t i = 0; i < schema.len(); ++i)
        if (schema.cols[i].isVariableLength && !columns[i]) return columns;
    return {};
}

std::vector<std::string> inheritedTableDescendants(StorageEngine& engine,
    const std::string& database, const std::string& parent) {
    const auto path = engine.dbPath(database) / ".inherits";
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory) return {};
    if (error || !std::filesystem::is_regular_file(status))
        throw DbError("58030", "could not inspect inheritance links");
    std::ifstream input(path);
    if (!input) throw DbError("58030", "could not read inheritance links");
    std::map<std::string, std::vector<std::string>> edges;
    std::string line;
    while (std::getline(input, line)) {
        const auto separator = line.find('|');
        if (separator != std::string::npos)
            edges[line.substr(0, separator)].push_back(line.substr(separator + 1));
    }
    if (input.bad()) throw DbError("58030", "could not finish reading inheritance links");
    std::set<std::string> seen{parent};
    std::vector<std::string> relations{parent};
    for (size_t i = 0; i < relations.size(); ++i) {
        const auto children = edges.find(relations[i]);
        if (children == edges.end()) continue;
        for (const auto& name : children->second)
            if (seen.insert(name).second) relations.push_back(name);
    }
    relations.erase(relations.begin());
    return relations;
}

namespace {
bool supportsInheritedHeap(const TableSchema& schema) {
    return schema.formatVersion == DATA_FILE_FORMAT_VERSION &&
        schema.partitionType == TableSchema::PartitionType::None &&
        !schema.rowLevelSecurity && !schema.forceRowLevelSecurity &&
        schema.tablename.rfind("__tmp_", 0) != 0;
}

bool compatibleInheritedField(const Column& parent, const Column& child) {
    return parent.dataType == child.dataType && parent.isArray == child.isArray &&
        parent.isUnsigned == child.isUnsigned && parent.dsize == child.dsize &&
        parent.isVariableLength == child.isVariableLength && parent.storageCodec == child.storageCodec &&
        parent.typeMod == child.typeMod && parent.domainName == child.domainName &&
        parent.enumValues == child.enumValues && parent.collation == child.collation &&
        parent.generatedKind == child.generatedKind && parent.generatedKind != 'v';
}

class InheritedTableScanOp final : public Operator {
public:
    InheritedTableScanOp(StorageEngine& engine, std::string database, TableSchema parent,
                         QueryRowDescriptor declaration, std::vector<std::string> names,
                         std::vector<uint64_t> generations, size_t sourceOrdinal, bool allColumns)
        : engine_(engine), database_(std::move(database)), parent_(std::move(parent)),
          declaration_(std::move(declaration)), names_(std::move(names)),
          generations_(std::move(generations)), sourceOrdinal_(sourceOrdinal) {
        if (allColumns)
            for (size_t i = 0; i < parent_.len(); ++i) selected_.insert(parent_.cols[i].dataName);
    }

    ~InheritedTableScanOp() override { try { close(); } catch (...) {} }

    void prepareDemand(const PreparedSelectState& state) {
        outer_ = state.outerRow;
        std::vector<bool> demand(parent_.len());
        bool allColumns = !state.windowStages.empty();
        const auto visit = [&](const auto& self, Expr* expression) -> void {
            if (!expression || allColumns) return;
            // A subquery can obtain this row's values through a correlated
            // reference not present among the local expression's children.
            if (expression->preparedSubquery) {
                allColumns = true;
                return;
            }
            if (const auto* column = dynamic_cast<const ColumnRefExpr*>(expression)) {
                if (column->binding && column->binding->sourceOrdinal == sourceOrdinal_) {
                    const auto ordinal = column->binding->columnOrdinal;
                    if (ordinal >= parent_.len())
                        throw DbError("XX000", "inherited column is outside the parent descriptor");
                    demand[ordinal] = true;
                }
                return;
            }
            prepared_window_detail::spec_detail::valueChildren(expression,
                [&](Expr* child) { self(self, child); });
        };
        for (auto* target : state.targets) visit(visit, target);
        for (const auto& key : state.keys)
            if (key.target == PreparedSelectState::noTarget) visit(visit, key.expression);
        visit(visit, state.select().whereClause.get());
        visit(visit, state.select().having.get());
        for (size_t i = 0; i < parent_.len(); ++i)
            if (allColumns || demand[i]) selected_.insert(parent_.cols[i].dataName);
    }

    bool open() override {
        OpenInstrument instrument(this);
        close();
        clearError();
        if (!engine_.inTransaction() || engine_.transactionDatabase() != database_)
            throw DbError("XX000", "inheritance scan requires its statement snapshot");
        auto lockOrder = names_;
        std::sort(lockOrder.begin(), lockOrder.end());
        locked_.reserve(lockOrder.size());
        auto& manager = engine_.getLockManager();
        manager.setResourceNamespace(database_);
        for (auto& name : lockOrder) {
            if (!manager.lockShared(name))
                throw DbError("55P03", "could not lock inherited relation " + name);
            locked_.push_back(std::move(name));
        }
        const auto descendants = inheritedTableDescendants(engine_, database_, parent_.tablename);
        if (!std::equal(names_.begin() + 1, names_.end(), descendants.begin(), descendants.end()))
            throw DbError("40001", "inheritance links changed before scan");
        sources_.reserve(names_.size());
        for (size_t relation = 0; relation < names_.size(); ++relation) {
            const auto& name = names_[relation];
            auto schema = engine_.getTableSchema(database_, name);
            if (schema.tablename != name || !supportsInheritedHeap(schema) ||
                schema.physicalRelationId != generations_[relation])
                throw DbError("40001", "inheritance source changed before scan");
            std::map<std::string, size_t> fields;
            for (size_t i = 0; i < schema.len(); ++i) fields.emplace(schema.cols[i].dataName, i);
            std::vector<std::pair<size_t, size_t>> projection;
            projection.reserve(selected_.size());
            for (size_t i = 0; i < parent_.len(); ++i) {
                const auto& field = parent_.cols[i];
                const auto found = fields.find(field.dataName);
                if (found == fields.end() ||
                    !compatibleInheritedField(field, schema.cols[found->second]))
                    throw DbError("42804", "incompatible inherited column " + field.dataName);
                if (selected_.count(field.dataName)) projection.emplace_back(found->second, i);
            }
            // Storage decodes in physical column order; expressions still
            // address the parent relation's bound ordinals.
            std::sort(projection.begin(), projection.end());
            std::vector<size_t> parentOrdinals;
            parentOrdinals.reserve(projection.size());
            for (const auto& field : projection) parentOrdinals.push_back(field.second);
            sources_.push_back({std::move(schema), std::move(parentOrdinals)});
        }
        return true;
    }

    bool next(std::string& raw) override {
        NextInstrument instrument(this);
        hasRow_ = false;
        checkForQueryInterrupt();
        while (source_ < sources_.size()) {
            const auto& part = sources_[source_];
            // The outer SQL command owns the same ReadView for every heap.
            if (!cursor_) cursor_ = engine_.beginHeapScan(database_, part.schema.tablename, part.schema);
            HeapScanRow row;
            if (!cursor_->next(row)) {
                cursor_.reset();
                ++source_;
                recordTableScan(database_, part.schema.tablename, std::exchange(rowsRead_, 0), false, true);
                continue;
            }
            std::vector<std::string> values;
            std::vector<bool> nulls;
            if (!selected_.empty() && !detail::decodeStoredRowProjection(engine_, database_, row.data, part.schema,
                    row.nulls, selected_, values, nulls))
                throw DbError("58030", "could not read inherited row values");
            RowContext context = outer_;
            for (size_t position = 0; position < part.parentOrdinals.size(); ++position) {
                const auto i = part.parentOrdinals[position];
                const auto& field = parent_.cols[i];
                const auto& metadata = declaration_[i].typeMetadata;
                const auto type = ExprHelper::canonicalResultTypeName(field.dataType +
                    (field.isArray ? "[]" : ""));
                uint32_t domainOid = 0;
                if (field.isArray && !field.domainName.empty() && metadata &&
                    metadata->kind == DatumKind::Array && metadata->actualOid != metadata->codecOid)
                    domainOid = metadata->actualOid;
                ExprValue value(type, std::move(values[position]), nulls[position]);
                if (stored_regtype_detail::columnType(field.dataType)) {
                    const auto catalog = regtype_detail::metadata(engine_, database_);
                    value = stored_regtype_detail::load(value.value, field.isArray, value.isNull,
                        catalog, currentSession(), metadata, field.dataType, domainOid);
                } else if (field.isArray &&
                           !compound_value_detail::decodeArrayStorageInput(value, metadata, {}, domainOid) && domainOid) {
                    throw DbError("XX001", "could not decode inherited array domain value");
                }
                value.collation = field.collation;
                context.setBoundColumn(sourceOrdinal_, i, std::move(value));
            }
            row_ = std::move(context);
            hasRow_ = true;
            ++rowsRead_;
            raw.clear();
            instrument.emitted = true;
            return true;
        }
        return false;
    }

    bool supportsPreparedContexts() const override { return true; }

    bool supportsPreparedOuterRow() const override { return true; }

    bool bindPreparedOuterRow(const RowContext& outer) override {
        close();
        outer_ = outer;
        return true;
    }

    bool lastPreparedContext(RowContext& row) const override {
        if (!hasRow_) return false;
        row = row_;
        return true;
    }

    void close() override {
        auto& manager = engine_.getLockManager();
        manager.setResourceNamespace(database_);
        cursor_.reset();
        std::exception_ptr statsFailure;
        if (rowsRead_ != 0) {
            try {
                recordTableScan(database_, sources_[source_].schema.tablename,
                               std::exchange(rowsRead_, 0), false, false);
            } catch (...) { statsFailure = std::current_exception(); }
        }
        for (auto it = locked_.rbegin(); it != locked_.rend(); ++it) manager.unlock(*it);
        locked_.clear();
        sources_.clear();
        source_ = 0;
        row_ = {};
        hasRow_ = false;
        if (statsFailure) std::rethrow_exception(statsFailure);
    }

    std::string preparedPlanNodeName() const override { return "Append"; }

private:
    struct Source { TableSchema schema; std::vector<size_t> parentOrdinals; };
    StorageEngine& engine_;
    std::string database_;
    TableSchema parent_;
    QueryRowDescriptor declaration_;
    std::vector<std::string> names_;
    std::vector<uint64_t> generations_;
    std::set<std::string> selected_;
    std::vector<std::string> locked_;
    std::vector<Source> sources_;
    std::unique_ptr<HeapScanCursor> cursor_;
    RowContext outer_;
    RowContext row_;
    bool hasRow_ = false;
    size_t sourceOrdinal_;
    size_t source_ = 0;
    size_t rowsRead_ = 0;
};
}  // namespace

bool supportsInheritedTableSource(StorageEngine& engine, const std::string& database,
    const TableSchema& parent,
    const std::vector<std::string>& descendants) {
    if (descendants.empty() || parent.len() == 0 || !supportsInheritedHeap(parent) ||
        std::any_of(parent.cols.begin(), parent.cols.end(),
                    [](const Column& column) { return column.generatedKind == 'v'; })) return false;
    for (const auto& descendant : descendants) {
        const auto schema = engine.getTableSchema(database, descendant);
        if (schema.tablename != descendant || !supportsInheritedHeap(schema)) return false;
    }
    return true;
}

OpPtr makeInheritedTableSource(StorageEngine& engine, const std::string& database,
    const TableSchema& parent, const PreparedQuery::SourceRange& range,
    const std::vector<std::string>& descendants, bool allColumns) {
    if (range.mergedUsing || range.columns.size() != parent.len())
        throw DbError("XX000", "inheritance scan lost its parent column declaration");
    std::vector<std::string> relations{parent.tablename};
    relations.insert(relations.end(), descendants.begin(), descendants.end());
    std::vector<uint64_t> generations{parent.physicalRelationId};
    generations.reserve(relations.size());
    for (const auto& name : descendants)
        generations.push_back(engine.getTableSchema(database, name).physicalRelationId);
    return std::make_unique<InheritedTableScanOp>(engine, database, parent,
        range.columns, std::move(relations), std::move(generations), range.ordinal, allColumns);
}

OpPtr QueryPlanner::buildPreparedSelectPlan(StorageEngine* engine,
    const std::string& dbname, const std::string& tablename, PreparedQuery prepared) {
    auto* select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    if (!select || !supportsPreparedSelectPlan(*select))
        throw DbError("0A000", "query requires an additional prepared plan lowering");
    if (select->fromClause && select->fromClause->type!=FromItem::Type::Table)
        return buildPreparedQueryPlan(engine,dbname,std::make_shared<PreparedQuery>(std::move(prepared)),select);
    const auto schema = select->fromClause ? engine->getTableSchema(dbname, tablename) : TableSchema{};
    OpPtr source = select->fromClause ? OpPtr(std::make_unique<TableScanOp>(engine, dbname, tablename))
        : OpPtr(std::make_unique<PreparedResultOp>());
    return buildPreparedSelectPlan(engine, dbname, std::make_shared<PreparedQuery>(std::move(prepared)),
        select, schema, std::move(source));
}

static bool ordersByIntegerPrimaryKey(const PreparedSelectState& state) {
    const auto* source = state.select().fromClause.get();
    if (!source || source->type != FromItem::Type::Table || state.keys.size() != 1) return false;
    const auto& key = state.keys.front();
    const auto* column = dynamic_cast<const ColumnRefExpr*>(key.target == PreparedSelectState::noTarget
        ? key.expression : state.targets.at(key.target));
    if (!column || !column->binding || column->binding->scopeDepth || column->binding->mergedUsing)
        return false;
    const auto& binding = *column->binding;
    const auto type = ExprHelper::canonicalResultTypeName(binding.declaredType);
    if (type != "smallint" && type != "integer" && type != "bigint") return false;
    const auto range = std::find_if(state.query->sourceRanges.begin(), state.query->sourceRanges.end(),
        [&](const auto& item) {
            return item.owner == state.statement && item.source == source && item.ordinal == binding.sourceOrdinal;
        });
    if (range == state.query->sourceRanges.end() || range->relationName.empty() || range->cteStatement ||
        range->viewQuery || range->relationSchema == "pg_catalog") return false;
    const auto* session = currentSession();
    const bool temporary = session &&
        (range->relationSchema == sessionTempSchemaName(*session) || range->relationSchema == "pg_temp");
    const auto table = temporary ? tempTablePrefix(*session, range->relationName)
        : range->relationSchema.empty() || range->relationSchema == "public" ? range->relationName
        : range->relationSchema + "__" + range->relationName;
    const auto matches = [&](const TableSchema& schema) {
        if (schema.partitionType != TableSchema::PartitionType::None || binding.columnOrdinal >= schema.len())
            return false;
        const auto& physical = schema.cols[binding.columnOrdinal];
        return !physical.isArray && !physical.isUnsigned && !physical.generatedKind && physical.domainName.empty() &&
            ExprHelper::canonicalResultTypeName(physical.dataType) == type &&
            isSingleColumnPrimaryKey(schema, physical.dataName);
    };
    if (!(state.sourceContexts ? matches(state.engine->getTableSchema(state.dbname, table)) : matches(state.schema)))
        return false;
    return state.engine->getInheritedChildren(state.dbname, table).empty();
}

static bool costlyArrayTextProjection(Expr* expression, const PreparedSelectState& state) {
    // Coercion through text performs an output and an input conversion. Use
    // this cost only for recognized integer array I/O, leaving other casts
    // and unanalyzed costs to their existing placement.
    constexpr size_t expensiveCost = 10;
    size_t cost = 0;
    const auto integerArray = [](const std::string& type) {
        return type == "smallint[]" || type == "integer[]" || type == "bigint[]";
    };
    const auto visit = [&](const auto& self, Expr* node) -> void {
        if (!node || node->preparedSubquery || cost > expensiveCost) return;
        const Expr* operand = nullptr;
        const std::string* declared = nullptr;
        if (const auto* cast = dynamic_cast<const CastExpr*>(node)) {
            operand = cast->operand.get();
            declared = &cast->typeName;
        } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node); binary && binary->op == "::") {
            if (const auto* type = dynamic_cast<const LiteralExpr*>(binary->right.get())) {
                operand = binary->left.get();
                declared = &type->value;
            }
        }
        if (operand) {
            const auto result = ExprHelper::canonicalResultTypeName(*declared);
            if (result == "text" || integerArray(result)) {
                const auto input = ExprHelper::canonicalResultTypeName(ExprHelper::inferParsedResultType(
                    operand, {}, state.dbname, state.engine));
                if ((result == "text" && integerArray(input)) || (integerArray(result) && input == "text"))
                    cost += 2;
            }
        }
        prepared_window_detail::spec_detail::valueChildren(node, [&](Expr* child) { self(self, child); });
    };
    visit(visit, expression);
    return cost > expensiveCost;
}

static bool constantFullJoinCondition(Expr* condition) {
    const auto constant = [&](const auto& self, Expr* expression) -> bool {
        if (!expression || expression->preparedSubquery) return false;
        if (dynamic_cast<const LiteralExpr*>(expression)) return true;
        if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
            return self(self, cast->operand.get());
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression))
            return self(self, unary->operand.get());
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression))
            return self(self, binary->left.get()) && self(self, binary->right.get());
        return false;
    };
    if (constant(constant, condition)) return true;
    // These boolean arms alone determine whether the condition can match.
    // Inspect the bound literals without running any operand or subquery.
    const auto* binary = dynamic_cast<const BinaryOpExpr*>(condition);
    if (!binary) return false;
    const auto op = SQLParser::toLower(binary->op);
    if (op != "and" && op != "or") return false;
    const auto decisive = [&](const Expr* expression) {
        const auto* literal = dynamic_cast<const LiteralExpr*>(expression);
        if (!literal || literal->preparedSubquery) return false;
        const auto value = SQLParser::toLower(literal->value);
        return op == "and" ? (value == "false" || value == "null") : value == "true";
    };
    return decisive(binary->left.get()) || decisive(binary->right.get());
}

static void validatePreparedFullJoin(const PreparedQuery& query, const SelectStmt& select) {
    const auto check = [&](const auto& visit, const FromItem* join) -> void {
        if (!join || join->type != FromItem::Type::Join) return;
        auto* condition = join->joinCondition.get();
        if (SQLParser::toLower(join->joinType).find("full") != std::string::npos && condition &&
            !constantFullJoinCondition(condition)) {
            std::map<size_t, unsigned> inputs;
            const auto collect = [&](const auto& self, const FromItem* item, unsigned side) -> void {
                if (!item) return;
                for (const auto& range : query.sourceRanges)
                    if (range.owner == &select && range.source == item) inputs.emplace(range.ordinal, side);
                self(self, item->left.get(), side);
                self(self, item->right.get(), side);
            };
            collect(collect, join->left.get(), 1);
            collect(collect, join->right.get(), 2);
            const auto operandInputs = [&](const auto& self, Expr* expression) -> unsigned {
                if (!expression) return 0;
                if (expression->preparedSubquery) return 4;
                if (const auto* column = dynamic_cast<const ColumnRefExpr*>(expression)) {
                    if (!column->binding) return 4;
                    if (column->binding->scopeDepth) return 0;
                    const auto identity = inputs.find(column->binding->sourceOrdinal);
                    return identity != inputs.end() ? identity->second : 4;
                }
                unsigned sides = 0;
                prepared_window_detail::spec_detail::valueChildren(expression,
                    [&](Expr* child) { sides |= self(self, child); });
                return sides;
            };
            const auto hasEquality = [&](const auto& self, Expr* expression) -> bool {
                const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression);
                if (!binary) return false;
                if (SQLParser::toLower(binary->op) == "and")
                    return self(self, binary->left.get()) || self(self, binary->right.get());
                if (binary->op != "=") return false;
                const auto left = operandInputs(operandInputs, binary->left.get());
                const auto right = operandInputs(operandInputs, binary->right.get());
                return (left == 1 && right == 2) || (left == 2 && right == 1);
            };
            if (!hasEquality(hasEquality, condition))
                throw DbError("0A000", "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions");
        }
        visit(visit, join->left.get());
        visit(visit, join->right.get());
    };
    check(check, select.fromClause.get());
}

OpPtr QueryPlanner::buildPreparedSelectPlan(StorageEngine* engine,
    const std::string& dbname, std::shared_ptr<PreparedQuery> prepared,
    SelectStmt* select, const TableSchema& sourceSchema, OpPtr source,
    const RowContext& outerRow, PreparedChildExecutor childExecutor,
    PreparedChildCursorFactory childCursorFactory, bool cursorOwnsScalarChildren,
    bool planRootConstants, PreparedExecutionProvider executionProvider, bool selectBodyOnly) {
    if (!prepared || !select || !source || !supportsPreparedSelectShape(*select, true, source->supportsPreparedContexts(),selectBodyOnly))
        throw DbError("0A000", "query requires an additional prepared plan lowering");
    validatePreparedFullJoin(*prepared, *select);
    const auto output = prepared->statementOutputs.find(select);
    if (output == prepared->statementOutputs.end())
        throw DbError("XX000", "borrowed SELECT has no prepared output descriptor");
    auto state = std::make_shared<PreparedSelectState>();
    state->engine = engine; state->dbname = dbname;
    state->query = std::move(prepared); state->statement = select;
    state->output = output->second; state->outerRow = outerRow;
    if (selectBodyOnly) {
        const auto inputs=state->query->setOperationInputs.find(select);
        if(inputs==state->query->setOperationInputs.end())throw DbError("XX000","set body has no bound input descriptor");
        state->output=inputs->second.left;
    }
    state->executionProvider=std::move(executionProvider);
    state->childExecutor = std::move(childExecutor);
    state->planRootConstants = planRootConstants;
    // Keep explicit scalar readers/engine hosts, while quantified children
    // still require a real typed stream. Only a cursor deliberately supplied
    // by the same execution owner may supersede those scalar contracts.
    state->cursorOwnsScalarChildren = childCursorFactory && cursorOwnsScalarChildren;
    state->childCursorFactory = childCursorFactory ? std::move(childCursorFactory) :
        PreparedChildCursorFactory([engine,dbname,query=state->query](const Stmt* child,const RowContext& row) {
            auto plan=QueryPlanner::buildPreparedQueryPlan(engine,dbname,query,child,row);
            const auto descriptor=query->statementOutputs.find(child);
            if(descriptor==query->statementOutputs.end())throw DbError("XX000","prepared child has no output descriptor");
            return QueryPlanner::makePreparedCursor(std::move(plan),descriptor->second);
        });
    state->evaluator.setCurrentDB(dbname);
    prepareAggregateRoles(*state);
    state->windows=prepared_window_detail::collect(*select,*state->query,engine,dbname);
    state->windowStages=prepared_window_detail::stages(*select,*state->query,state->windows,engine,dbname);
    if (sourceSchema.partitionType != TableSchema::PartitionType::None &&
        !selectBodyOnly && select->limit && !select->withTies && select->orderBy.empty() &&
        !select->distinct && select->distinctOn.empty() && select->locking.empty() &&
        select->groupBy.empty() && select->groupByElems.empty() && !select->having &&
        state->aggregates.empty() && state->windows.empty() &&
        typeid(*source) == typeid(TableScanOp)) {
        // LIMIT and OFFSET pull filtered rows. Open each leaf on demand.
        static_cast<TableScanOp&>(*source).scanPartitionsOnDemand_ = true;
    }
    state->sourceContexts = source->supportsPreparedContexts();
    if (state->sourceContexts) state->logicalSource = source.get();
    if (state->sourceContexts && select->fromClause) {
        std::function<void(const FromItem*)> ranges = [&](const FromItem* item) {
            for (const auto& range : state->query->sourceRanges)
                if (range.owner == select && range.source == item) state->visibleRanges.push_back(&range);
            if (item->type == FromItem::Type::Join) { ranges(item->left.get()); ranges(item->right.get()); }
        };
        ranges(select->fromClause.get());
    } else if (select->fromClause) {
        state->schema = sourceSchema;
        for (const auto& range : state->query->sourceRanges) {
            if (range.owner != select || range.source != select->fromClause.get() || range.mergedUsing) continue;
            state->sourceOrdinal = range.ordinal;
            if (range.columns.size() != state->schema.len())
                throw DbError("0A000", "prepared plan requires a virtual or view source lowering");
            for (size_t i = 0; i < range.columns.size(); ++i)
                if (range.columns[i].name != state->schema.cols[i].dataName &&
                    i>=select->fromClause->columnAliases.size())
                    throw DbError("XX000", "prepared and physical source descriptors differ");
            break;
        }
        if (state->sourceOrdinal == PreparedSelectState::noTarget)
            throw DbError("XX000", "prepared physical source has no range identity");
    }
    for (auto& target : select->selectList) {
        const auto* reference = dynamic_cast<const ColumnRefExpr*>(target.expr.get());
        const auto* literal = dynamic_cast<const LiteralExpr*>(target.expr.get());
        if (target.expr && (target.expr->type == ExprType::A_Star ||
            (reference && reference->column == "*") || (literal && literal->value == "*"))) {
            if (!select->fromClause) throw DbError("42601", "SELECT * requires a FROM source");
            const auto projections = state->query->projectionBindings.find(select);
            if (projections == state->query->projectionBindings.end())
                throw DbError("XX000", "prepared star has no projection bindings");
            for (const auto& projection : projections->second) {
                if (projection.expression != target.expr.get()) continue;
                if (!projection.column)
                    throw DbError("XX000", "prepared star has no positional source identity");
                const auto& binding = *projection.column;
                const auto range = std::find_if(state->query->sourceRanges.begin(), state->query->sourceRanges.end(),
                    [&](const auto& sourceRange) { return sourceRange.ordinal == binding.sourceOrdinal; });
                if (range == state->query->sourceRanges.end() || range->owner != select ||
                    binding.scopeDepth != 0 || binding.columnOrdinal >= range->columns.size())
                    throw DbError("XX000", "prepared star source identity is invalid");
                auto expression = std::make_unique<ColumnRefExpr>();
                expression->column = range->columns[binding.columnOrdinal].name;
                expression->binding = binding;
                state->targets.push_back(expression.get());
                state->starTargets.push_back(std::move(expression));
            }
        } else state->targets.push_back(target.expr.get());
    }
    if (state->visibleRanges.size() == 1 && select->fromClause &&
        select->fromClause->type == FromItem::Type::Function &&
        state->aggregates.empty() && state->windowStages.empty() && !select->having &&
        select->orderBy.empty() && !select->existsTargetPrunable) {
        const auto& range = *state->visibleRanges.front();
        for (const auto* target : state->targets) {
            const auto* column = dynamic_cast<const ColumnRefExpr*>(target);
            if (!column || !column->binding || column->generatedRead || column->generatedBaseDeclaration ||
                column->binding->scopeDepth || column->binding->mergedUsing ||
                column->binding->sourceOrdinal != range.ordinal ||
                column->binding->columnOrdinal >= range.columns.size()) {
                state->functionProjectionColumns.clear();
                break;
            }
            state->functionProjectionColumns.push_back(column->binding->columnOrdinal);
        }
    }
    const auto aliases=state->query->sortOutputOwners.find(select);
    for(size_t i=0;i<state->targets.size();++i)state->targetOwners.push_back(
        aliases==state->query->sortOutputOwners.end()?i:aliases->second.at(i));
    std::vector<std::string> identities;
    for (const auto* target : state->targets) identities.push_back(state->identity(target));
    std::vector<std::string> keyIdentities;
    for (auto& order : select->orderBy) {
        if (select->existsTargetPrunable) break;
        size_t target = PreparedSelectState::noTarget;
        bool windowOrderExpression=false;
        if(!state->windows.empty()) {
            const auto* ordinal=dynamic_cast<const LiteralExpr*>(order.expr.get());
            bool negative=false;
            if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(order.expr.get());unary && unary->op=="-") {
                ordinal=dynamic_cast<const LiteralExpr*>(unary->operand.get());negative=true;
            }
            if(ordinal && !ordinal->preparedSubquery && ordinal->typeName.empty()) {
                // Binding absorbs lexical signs when choosing a literal's
                // numeric type. Its original '+' still denotes an expression
                // in ORDER BY, rather than an output ordinal.
                windowOrderExpression=ordinal->sourceBegin<state->query->source.size() &&
                    state->query->source[ordinal->sourceBegin]=='+';
                if(!windowOrderExpression) {
                    int32_t position=0;
                    const auto& text=ordinal->value;
                    const bool signedNegative=!text.empty() && text.front()=='-';
                    const size_t begin=signedNegative?1:0;
                    const auto parsed=std::from_chars(text.data()+begin,text.data()+text.size(),position);
                    if(begin==text.size() || !std::all_of(text.begin()+begin,text.end(),[](unsigned char c){return c>='0' && c<='9';}) ||
                        parsed.ec!=std::errc{} || parsed.ptr!=text.data()+text.size())
                        throw DbError("42601","non-integer constant in ORDER BY");
                    if(negative || signedNegative || !position || static_cast<size_t>(position)>state->targets.size())
                        throw DbError("42P10","ORDER BY position is not in select list");
                }
            }
        }
        if (auto* ref = dynamic_cast<ColumnRefExpr*>(order.expr.get())) {
            if (ref->schema.empty() && ref->table.empty()) {
                for (size_t i = 0; i < state->output.size(); ++i) {
                    if (state->output[i].name != ref->column) continue;
                    const size_t owner=state->targetOwners[i];
                    if (target != PreparedSelectState::noTarget && target!=owner && identities[target] != identities[i])
                        throw DbError("42702", "ORDER BY name is ambiguous");
                    if(target==PreparedSelectState::noTarget)target=owner;
                }
            }
        } else if (auto* literal = dynamic_cast<LiteralExpr*>(order.expr.get())) {
            if (!windowOrderExpression && !literal->value.empty() && std::all_of(literal->value.begin(), literal->value.end(), [](unsigned char c) { return std::isdigit(c); })) {
                const auto ordinal = std::stoull(literal->value);
                if (!ordinal || ordinal > state->targets.size()) throw DbError("42P10", "ORDER BY position is not in select list");
                target = static_cast<size_t>(ordinal - 1);
            }
        }
        std::string identity;
        if (target == PreparedSelectState::noTarget) {
            identity = state->identity(order.expr.get());
            for (size_t i = 0; i < identities.size(); ++i) if (identities[i] == identity) { target = i; break; }
        }
        if (select->distinct && target == PreparedSelectState::noTarget)
            throw DbError("42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        // Reuse an actual sort-input value, not only a fingerprint. Explicit
        // references to two genuine SELECT sites remain different targets.
        size_t priorKey = PreparedSelectState::noTarget;
        for (size_t i = 0; i < state->keys.size(); ++i) {
            if (target != PreparedSelectState::noTarget) {
                if (state->keys[i].target == target) { priorKey = i; break; }
            } else if (state->keys[i].target == PreparedSelectState::noTarget && keyIdentities[i] == identity) {
                priorKey = i; break;
            }
        }
        keyIdentities.push_back(std::move(identity));
        state->keys.push_back({order.expr.get(), target, priorKey, order.asc, order.nullsFirst,order.enumComparison});
    }
    if (auto* inherited = dynamic_cast<InheritedTableScanOp*>(source.get()))
        inherited->prepareDemand(*state);
    // All bindings and collation checks precede opening any source or
    // evaluating a volatile expression. The retained parameter cells remain
    // typed, including NULL, and are supplied directly to the evaluator.
    const bool ordinarySort = !state->keys.empty() && !select->distinct && state->windows.empty() &&
        state->aggregates.empty() && !select->having;
    const bool primaryKeyOrder = ordinarySort && ordersByIntegerPrimaryKey(*state);
    for (size_t i = 0; i < state->targets.size(); ++i) {
        auto* target = state->targets[i];
        bool beforeSort = !state->containsVolatile(target);
        if (beforeSort && ordinarySort && state->targetOwners[i] == i &&
            std::none_of(state->keys.begin(), state->keys.end(), [&](const auto& key) { return key.target == i; }))
            beforeSort = !primaryKeyOrder && !(select->limit && costlyArrayTextProjection(target, *state));
        state->targetsBeforeSort.push_back(beforeSort);
    }
    state->prepareWindowInputs();
    state->prepareExecution();
    const bool orderedWindow=prepareWindowOutputOrder(*state);
    prepareWindowResultSharing(*state,!state->keys.empty() && !orderedWindow);
    if (typeid(*source) == typeid(TableScanOp))
        state->projectedPhysicalColumns = projectedPrimaryKeyReadColumns(*state);
    source = makePreparedTableSource(engine, dbname, state->query, select,
                                    sourceSchema, std::move(source), state->projectedPhysicalColumns);
    if (!dynamic_cast<const IntegerPrimaryKeyPageOp*>(source.get())) state->projectedPhysicalColumns.clear();
    if (select->selectList.empty() && !select->whereClause && !select->having &&
        state->keys.empty() && state->aggregates.empty() && state->windowStages.empty() &&
        typeid(*source) == typeid(TableScanOp)) {
        static_cast<TableScanOp&>(*source).readVisibleRowsOnly_ = true;
    }
    if (select->whereClause && select->locking.empty() && sourceSchema.physicalRelationId &&
        state->sourceOrdinal != PreparedSelectState::noTarget && typeid(*source) == typeid(TableScanOp)) {
        if (const auto domain = StorageEngine::integerPartitionDomain(sourceSchema)) {
            for (size_t i = 0; i < sourceSchema.len(); ++i) {
                const auto& column = sourceSchema.cols[i];
                if (column.dataName != sourceSchema.partitionKey) continue;
                const auto keyType = ExprHelper::canonicalResultTypeName(column.dataType);
                auto filter = integerKeyPredicate(select->whereClause.get(), state->sourceOrdinal, i, keyType,
                    state->executionProvider ? nullptr : state->query.get());
                if (filter) static_cast<TableScanOp&>(*source).rangePruning_ = TableScanOp::RangePruning{
                    sourceSchema.physicalRelationId, column.dataName, i, *domain, std::move(*filter)};
                break;
            }
        }
    }
    TableScanOp* countScan = nullptr;
    StoredPredicates countWhere;
    std::vector<StoredPredicates> countFilters;
    if (sourceSchema.physicalRelationId && state->aggregates.size() == select->selectList.size() &&
        supportsStoredCountSelect(*select) && supportsStoredHeapPredicate(sourceSchema)) {
        if (auto* filtered = dynamic_cast<StoredFilterScanOp*>(source.get())) {
            countScan = filtered->tableScan();
            countWhere = filtered->predicates();
        } else if (!select->whereClause && typeid(*source) == typeid(TableScanOp)) {
            countScan = static_cast<TableScanOp*>(source.get());
        }
        if (countScan) {
            const auto origin = countScan->scanOrigin();
            if (origin.engine != engine || origin.dbname != dbname || origin.tablename != sourceSchema.tablename)
                countScan = nullptr;
        }
        if (countScan) {
            for (const auto& aggregate : state->aggregates) {
                auto filter = bindStoredCountFilter(*engine, dbname, sourceSchema, state->sourceOrdinal,
                    *aggregate.call);
                if (!filter) break;
                countFilters.push_back(std::move(*filter));
            }
            if (countFilters.size() != state->aggregates.size()) countScan = nullptr;
        }
    }
    OpPtr root = std::move(source);
    if (select->whereClause) root = std::make_unique<PreparedFilterOp>(std::move(root), state);
    if (!state->aggregates.empty() || select->having)
        root = std::make_unique<PreparedAggregateOp>(std::move(root), state);
    if (countScan)
        root = std::make_unique<PreparedStoredCountOp>(std::move(root), countScan, state,
            std::move(countWhere), std::move(countFilters));
    if(!state->windowStages.empty())root=makePreparedWindowStages(std::move(root),state);
    if (!state->keys.empty() && !orderedWindow) root = std::make_unique<PreparedSortOp>(std::move(root), state);
    auto* tiesSort=select->withTies?dynamic_cast<PreparedSortOp*>(root.get()):nullptr;
    root = std::make_unique<PreparedProjectOp>(std::move(root), state);
    if (select->distinct && !select->existsTargetPrunable) root = std::make_unique<PreparedDistinctOp>(std::move(root));
    if (select->offset && *select->offset) root = std::make_unique<OffsetOp>(std::move(root), *select->offset);
    if(select->signedFetchCount && *select->signedFetchCount<0)
        root=std::make_unique<PreparedNegativeFetchOp>(std::move(root));
    else if(select->fetchFirst && select->withTies && !select->limit && !select->signedFetchCount)
        root=std::make_unique<PreparedRowCountErrorOp>(std::move(root),"2201W","row count cannot be null in FETCH FIRST ... WITH TIES clause");
    else if(select->limit) {
        if(select->withTies && !select->existsTargetPrunable) {
            if(orderedWindow)root=makePreparedWindowTiesLimit(std::move(root),state,*select->limit);
            else root=std::make_unique<PreparedTiesLimitOp>(std::move(root),tiesSort,*select->limit);
        }
        else root=std::make_unique<LimitOp>(std::move(root),*select->limit);
    }
    return root;
}

namespace {
class PreparedAppendOp final : public Operator {
    OpPtr branches_[2];
    std::vector<std::string> inputTypes_[2], outputTypes_;
    std::vector<uint32_t> outputOids_;
    bool renderRows_;
    std::function<void()> begin_, finish_;
    std::vector<ExprValue> values_;
    size_t branch_=0;
    bool opened_[2]={false,false}, ownerActive_=false, rowAvailable_=false;
    ExprEvaluator evaluator_;
    void closeBranch(size_t index) {
        if(!opened_[index])return;
        opened_[index]=false;branches_[index]->close();
    }
public:
    PreparedAppendOp(OpPtr left,OpPtr right,const QueryRowDescriptor& leftInput,
        const QueryRowDescriptor& rightInput,const QueryRowDescriptor& output,bool renderRows,
        std::function<void()> begin,std::function<void()> finish)
        :renderRows_(renderRows),begin_(std::move(begin)),finish_(std::move(finish)) {
        branches_[0]=std::move(left);branches_[1]=std::move(right);
        for (const auto& column : leftInput) inputTypes_[0].push_back(ExprHelper::canonicalResultTypeName(column.type));
        for (const auto& column : rightInput) inputTypes_[1].push_back(ExprHelper::canonicalResultTypeName(column.type));
        for (const auto& column : output) {
            outputTypes_.push_back(ExprHelper::canonicalResultTypeName(column.type));
            outputOids_.push_back(column.typeOid);
        }
    }
    ~PreparedAppendOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();values_.clear();branch_=0;rowAvailable_=false;
        ownerActive_=true;if(begin_)begin_();
        opened_[0]=true;
        if(!branches_[0]->open())return propagateChildError(branches_[0].get(),"typed append left open failed");
        return true;
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);rowAvailable_=false;checkForQueryInterrupt();
        while(branch_<2) {
            std::string ignored;
            if(branches_[branch_]->next(ignored)) {
                if(!branches_[branch_]->lastStructuredValues(values_) || values_.size()!=outputTypes_.size())
                    throw DbError("XX000","typed append child lost its declared row");
                display.clear();
                for(size_t i=0;i<values_.size();++i) {
                    const auto& source=inputTypes_[branch_][i];
                    const auto& target=outputTypes_[i];
                    if(values_[i].typeName!=source && ExprHelper::canonicalResultTypeName(values_[i].typeName)!=source)
                        throw DbError("XX000","typed append child cell differs from its bound input type");
                    if(source!=target) {
                        // Convert the genuine typed datum, never its rendered
                        // row/SQL spelling. NULL and text 'NULL' stay distinct.
                        CastExpr cast;cast.typeName=target;cast.implicit=true;
                        auto parameter=std::make_unique<ParameterExpr>();
                        parameter->origin=ParameterOrigin::RuntimeCell;
                        parameter->slot=0;parameter->declaredType=source;cast.operand=std::move(parameter);
                        RowContext row;row.setParameters({values_[i]});
                        values_[i]=evaluator_.eval(&cast,row);
                    } else {
                        const auto& data = values_[i].compound;
                        if (data && data->type && data->type->kind == DatumKind::Array &&
                            data->type->actualOid != data->type->codecOid &&
                            outputOids_[i] == data->type->codecOid)
                            values_[i] = compound_value_detail::asBaseArray(std::move(values_[i]));
                    }
                    if(values_[i].typeName!=target && ExprHelper::canonicalResultTypeName(values_[i].typeName)!=target)
                        throw DbError("XX000","typed append coercion lost its common output type");
                    values_[i].typeName=target;
                    if (renderRows_) display+=values_[i].isNull?"NULL ":values_[i].value+" ";
                }
                rowAvailable_=instrument.emitted=true;return true;
            }
            if(branches_[branch_]->hasError())return propagateChildError(branches_[branch_].get(),"typed append child failed");
            closeBranch(branch_++);
            if(branch_<2) {
                opened_[branch_]=true;
                if(!branches_[branch_]->open())return propagateChildError(branches_[branch_].get(),"typed append right open failed");
            }
        }
        values_.clear();return false;
    }
    void close() override {
        std::exception_ptr primary;
        for(size_t i=0;i<2;++i)try{closeBranch(i);}catch(...){if(!primary)primary=std::current_exception();}
        if(ownerActive_) {
            ownerActive_=false;
            try{if(finish_)finish_();}catch(...){if(!primary)primary=std::current_exception();}
        }
        values_.clear();branch_=0;rowAvailable_=false;
        if(primary)std::rethrow_exception(primary);
    }
    bool supportsStructuredRows() const override {return true;}
    size_t currentBranch() const {return branch_;}
    const std::vector<ExprValue>& values() const {return values_;}
    std::vector<ExprValue> takeValues() {return std::move(values_);}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {row=values_;return rowAvailable_;}
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        cells.clear();nulls.clear();for(const auto& value:values_){cells.push_back(value.value);nulls.push_back(value.isNull);}return rowAvailable_;
    }
    std::string preparedPlanNodeName() const override {return "Append";}
    std::vector<Operator*> preparedPlanChildren() const override {return {branches_[0].get(),branches_[1].get()};}
    bool supportsPreparedOuterRow() const override {
        return branches_[0]->supportsPreparedOuterRow() && branches_[1]->supportsPreparedOuterRow();
    }
    bool bindPreparedOuterRow(const RowContext& row) override {
        return supportsPreparedOuterRow() && branches_[0]->bindPreparedOuterRow(row) && branches_[1]->bindPreparedOuterRow(row);
    }
};

class PreparedSetMatchOp final : public Operator {
    struct Row {
        std::vector<ExprValue> values;
        size_t group;
    };
    struct KeyColumn {
        Column storage;
        std::optional<QueryComparisonBinding> floatingEquality;
    };
    std::unique_ptr<PreparedAppendOp> child_;
    SetOp operation_;
    bool all_;
    std::vector<KeyColumn> keyColumns_;
    std::vector<Row> rows_;
    size_t position_ = 0;
    bool loaded_ = false;
    bool available_ = false;

    bool load() {
        std::unordered_map<std::string, size_t> groups;
        std::vector<size_t> rightCounts;
        std::string display;
        while (child_->next(display)) {
            const auto& values = child_->values();
            std::string key;
            for (size_t i = 0; i < values.size(); ++i) {
                const auto& column = keyColumns_[i];
                const auto& value = values[i];
                if (column.floatingEquality && !value.isNull) {
                    const auto material = ExprEvaluator::comparisonHashKey(*column.floatingEquality, value, true);
                    key += "V" + std::to_string(material.size()) + ":" + material;
                } else {
                    key += StorageEngine::groupingValueKey(column.storage, value.value, value.isNull);
                }
            }

            if (operation_ != SetOp::Union && child_->currentBranch() == 1) {
                const auto match = groups.find(key);
                if (match != groups.end()) ++rightCounts[match->second];
                continue;
            }
            const auto [entry, inserted] = groups.try_emplace(std::move(key), groups.size());
            if (inserted && operation_ != SetOp::Union) rightCounts.push_back(0);
            if (inserted || all_) rows_.push_back({child_->takeValues(), entry->second});
        }
        if (child_->hasError()) return propagateChildError(child_.get(), "typed set input failed");
        if (operation_ != SetOp::Union) {
            const auto keep = [&](const Row& row) {
                checkForQueryInterrupt();
                size_t& right = rightCounts[row.group];
                const bool matched = right != 0;
                if (matched && all_) --right;
                return operation_ == SetOp::Intersect ? matched : !matched;
            };
            rows_.erase(std::remove_if(rows_.begin(), rows_.end(),
                [&](const Row& row) { return !keep(row); }), rows_.end());
        }
        return true;
    }

public:
    PreparedSetMatchOp(std::unique_ptr<PreparedAppendOp> child, SetOp operation, bool all,
                       const QueryRowDescriptor& output)
        : child_(std::move(child)), operation_(operation), all_(all) {
        keyColumns_.reserve(output.size());
        for (const auto& field : output) {
            KeyColumn column;
            const auto error = TypeRegistry::instance().resolveColumnType(column.storage, field.type, {}, false);
            if (!error.empty()) throw DbError("0A000", "unsupported set comparison type: " + field.type);
            if (field.typeOid == 700 || field.typeOid == 701) {
                const auto type = ExprHelper::canonicalResultTypeName(field.type);
                column.floatingEquality = ExprEvaluator::resolveComparison("=", type, type);
            }
            keyColumns_.push_back(std::move(column));
        }
    }
    bool open() override {
        OpenInstrument instrument(this);
        clearError(); rows_.clear(); position_ = 0; loaded_ = available_ = false;
        return child_->open() || propagateChildError(child_.get(), "typed set input open failed");
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        available_ = false;
        if (!loaded_) {
            loaded_ = true;
            if (!load()) return false;
        }
        if (position_ == rows_.size()) return false;
        display.clear();
        for (const auto& value : rows_[position_++].values)
            display += value.isNull ? "NULL " : value.value + " ";
        available_ = instrument.emitted = true;
        return true;
    }
    void close() override {
        rows_.clear(); position_ = 0; loaded_ = available_ = false;
        child_->close();
    }
    bool supportsStructuredRows() const override {return true;}
    bool lastStructuredValues(std::vector<ExprValue>& values) const override {
        if (!available_) return false;
        values = rows_[position_ - 1].values;
        return true;
    }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        if (!available_) return false;
        cells.clear(); nulls.clear();
        for (const auto& value : rows_[position_ - 1].values) {
            cells.push_back(value.value); nulls.push_back(value.isNull);
        }
        return true;
    }
    std::string preparedPlanNodeName() const override {return "TypedSetMatch";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {return child_->bindPreparedOuterRow(row);}
};

// Keep the existing RuntimeCell expression comparison owner. Reuse only
// its immutable node shape, never a datum/context, branch or parameter frame.
class SetOutputComparisons {
    struct Entry {
        bool ready = false;
        std::string left, right;
        std::unique_ptr<BinaryOpExpr> expressions[2];
    };
    std::vector<Entry> entries_;
    Entry& signature(size_t key, const ExprValue& a, const ExprValue& b) {
        auto& entry = entries_.at(key);
        if (!entry.ready || entry.left != a.typeName || entry.right != b.typeName) {
            Entry next;
            next.left = a.typeName; next.right = b.typeName; next.ready = true;
            entry = std::move(next);
        }
        return entry;
    }
    static const BinaryOpExpr* expression(Entry& entry, size_t slot, const char* op,
        const std::optional<QueryComparisonBinding>& enumeration) {
        auto& cached = entry.expressions[slot];
        if (!cached || cached->op != op) {
            auto next = std::make_unique<BinaryOpExpr>(); next->op = op;
            auto left = std::make_unique<ParameterExpr>();
            auto right = std::make_unique<ParameterExpr>();
            left->slot = 0; right->slot = 1;
            left->origin = right->origin = ParameterOrigin::RuntimeCell;
            left->declaredType = entry.left; right->declaredType = entry.right;
            next->left = std::move(left); next->right = std::move(right);
            if (enumeration) {
                next->comparison = *enumeration;
                next->comparison->op = op;
                next->comparison->identity += std::string(":set-output:") + op;
            } else {
                const auto a = ExprHelper::canonicalResultTypeName(entry.left);
                const auto b = ExprHelper::canonicalResultTypeName(entry.right);
                const auto floating = [](const std::string& type) {
                    return type == "real" || type == "double precision";
                };
                // The existing prepared scalar implementation owns float4
                // rounding and PostgreSQL NaN/infinity order. Other datums,
                // including records/arrays, keep the original recursive eval.
                if (floating(a) && floating(b))
                    next->comparison = ExprEvaluator::resolveComparison(op, a, b);
            }
            cached = std::move(next);
        }
        return cached.get();
    }
    static bool truth(const ExprValue& value) {
        return !value.isNull && (value.value == "t" || value.value == "true");
    }
    static RowContext context(const ExprValue& a,const ExprValue& b) {
        // initializer_list elements are const; constructing a vector from
        // {a,b} copies the owning nested values twice. Copy each datum into
        // this invocation's vector once, then transfer its ownership.
        std::vector<ExprValue> cells;cells.reserve(2);
        cells.emplace_back(a);cells.emplace_back(b);
        RowContext row;row.setParameters(std::move(cells));return row;
    }
public:
    void reset(size_t keys = 0) { std::vector<Entry> fresh(keys); entries_.swap(fresh); }
    int compare(size_t key, ExprEvaluator& evaluator, const ExprValue& a, const ExprValue& b,
        bool ascending, const std::optional<QueryComparisonBinding>& enumeration) {
        auto& entry = signature(key, a, b);
        auto row=context(a,b);
        if (truth(evaluator.eval(expression(entry,0,ascending?"<":">",enumeration),row))) return -1;
        if (truth(evaluator.eval(expression(entry,1,ascending?">":"<",enumeration),row))) return 1;
        return 0;
    }
    bool equal(size_t key, ExprEvaluator& evaluator, const ExprValue& a, const ExprValue& b,
        const std::optional<QueryComparisonBinding>& enumeration) {
        auto& entry = signature(key, a, b);
        auto row=context(a,b);
        return truth(evaluator.eval(expression(entry,0,"=",enumeration),row));
    }
};

#include "executor/PreparedWindowOp.inc"

// Sort the genuine common-typed Append output. Global set keys are bound
// output ordinals, not re-evaluated branch expressions or display strings.
class PreparedSetSortOp final : public Operator {
    using Row = TypedSortRow;
    using KeyProjector = std::function<std::vector<ExprValue>(const std::vector<ExprValue>&)>;
    struct Less {
        PreparedSetSortOp* owner;
        bool operator()(const Row& a, const Row& b) const { return owner->less(a,b); }
    };
    using Runs = sort_detail::BoundedRuns<Row,TypedSortRowCodec,Less>;
    OpPtr child_;
    std::vector<size_t> columns_;
    std::vector<std::pair<bool,bool>> directions_;
    std::vector<std::optional<QueryComparisonBinding>> enumComparisons_;
    size_t arity_;
    std::optional<size_t> bound_;
    KeyProjector projectKeys_;
    bool integerCache_=false;
    std::unique_ptr<Runs> runs_;
    Row current_;
    ExprEvaluator evaluator_;
    SetOutputComparisons comparisons_;
    bool opened_ = false, loaded_ = false, available_ = false;
    bool less(const Row& left, const Row& right) {
        for (size_t key = 0; key < columns_.size(); ++key) {
            const auto& a = projectKeys_ ? left.keys.at(columns_[key]) : left.cells.at(columns_[key]);
            const auto& b = projectKeys_ ? right.keys.at(columns_[key]) : right.cells.at(columns_[key]);
            if (a.isNull || b.isNull) {
                if (a.isNull != b.isNull) return a.isNull == directions_[key].second;
                continue;
            }
            int order;
            if(integerCache_ && !enumComparisons_[key] && a.typeName==b.typeName &&
               left.integerKeys.at(key) && right.integerKeys.at(key)) {
                const auto x=*left.integerKeys[key],y=*right.integerKeys[key];
                order=(x>y)-(x<y);
                if(!directions_[key].first)order=-order;
            } else order=comparisons_.compare(key,evaluator_,a,b,
                directions_[key].first,enumComparisons_[key]);
            if (order) return order < 0;
        }
        return left.ordinal < right.ordinal;
    }
    bool fail(const char* message) {
        propagateChildError(child_.get(),message);
        try { close(); } catch (...) {}
        return false;
    }
public:
    PreparedSetSortOp(OpPtr child, std::vector<size_t> columns,
        const std::vector<SelectStmt::OrderByElem>& order, const QueryRowDescriptor& output,
        std::optional<size_t> bound, const std::string& database, KeyProjector projectKeys = {})
        : child_(std::move(child)), columns_(std::move(columns)), arity_(output.size()), bound_(bound),
          projectKeys_(std::move(projectKeys)) {
        if (columns_.size() != order.size() ||
            std::any_of(columns_.begin(),columns_.end(),[&](size_t key){return key>=(projectKeys_?order.size():arity_);}))
            throw DbError("XX000","set sort has no complete bound output keys");
        for (const auto& key : order) {
            directions_.push_back({key.asc,key.nullsFirst});
            enumComparisons_.push_back(key.enumComparison);
        }
        // Hidden VALUES keys already have a parallel cache vector. Ordinary
        // set outputs need one only when their real descriptor admits an
        // exact built-in signed integer key; pure text/record sorts keep none.
        integerCache_=bool(projectKeys_);
        if(!integerCache_)for(size_t key=0;key<columns_.size();++key) {
            const auto& type=output.at(columns_[key]).type;
            if(!enumComparisons_[key] && (type=="smallint" || type=="integer" || type=="bigint"))
                integerCache_=true;
        }
        evaluator_.setCurrentDB(database);
    }
    ~PreparedSetSortOp() override { try { close(); } catch (...) {} }
    void setRowBound(std::optional<size_t> bound) {
        if(opened_)throw DbError("XX000","cannot change an open set sort bound");
        bound_=bound;
    }
    bool open() override {
        OpenInstrument instrument(this); clearError();
        runs_.reset(); current_ = Row{}; loaded_ = available_ = false;
        comparisons_.reset(columns_.size());
        try {
            runs_ = std::make_unique<Runs>(sortWorkMemBytes(),TypedSortRowCodec{arity_,projectKeys_?columns_.size():0,0,integerCache_?columns_.size():0},
                Less{this},bound_,[](std::vector<Row>& rows,const Less& less) {
                    interruptibleStableSort(rows.begin(),rows.end(),less);
                });
            opened_ = true;
            return child_->open() || fail("set sort child open failed");
        } catch (...) {
            const auto primary = std::current_exception();
            try { close(); } catch (...) {}
            std::rethrow_exception(primary);
        }
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if (!opened_ || !runs_) return false;
        try {
            checkForQueryInterrupt();
            if (!loaded_) {
                uint64_t ordinal = 0;
                std::string ignored;
                while (child_->next(ignored)) {
                    Row row;
                    if (!child_->lastStructuredValues(row.cells) || row.cells.size() != arity_)
                        throw DbError("XX000","set sort child lost typed output");
                    if (ordinal == std::numeric_limits<uint64_t>::max())
                        throw DbError("54000","too many set sort input rows");
                    row.ordinal = ordinal++;
                    if(projectKeys_) {
                        row.keys=projectKeys_(row.cells);
                        if(row.keys.size()!=columns_.size())throw DbError("XX000","VALUES sort lost its prepared keys");
                    }
                    if(integerCache_) {
                        row.integerKeys.resize(columns_.size());
                        for(size_t key=0;key<columns_.size();++key) {
                            const auto& value=projectKeys_?row.keys.at(columns_[key]):row.cells.at(columns_[key]);
                            if(value.isNull || enumComparisons_[key] ||
                               (value.typeName!="smallint" && value.typeName!="integer" && value.typeName!="bigint"))continue;
                            int64_t integer=0;const auto end=value.value.data()+value.value.size();
                            const auto parsed=std::from_chars(value.value.data(),end,integer);
                            if(parsed.ec==std::errc{} && parsed.ptr==end &&
                               (value.typeName!="smallint" || (integer>=INT16_MIN && integer<=INT16_MAX)) &&
                               (value.typeName!="integer" || (integer>=INT32_MIN && integer<=INT32_MAX)))
                                row.integerKeys[key]=integer;
                        }
                    }
                    runs_->add(std::move(row));
                }
                if (child_->hasError()) return fail("set sort child failed");
                runs_->finish(); loaded_ = true;
            }
            if (!runs_->next(current_)) { current_ = Row{}; available_ = false; return false; }
            available_ = true; display.clear();
            for (const auto& cell : current_.cells) display += cell.isNull ? "NULL " : cell.value + " ";
            instrument.emitted = true; return true;
        } catch (...) {
            const auto primary = std::current_exception();
            try { close(); } catch (...) {}
            std::rethrow_exception(primary);
        }
    }
    void close() override {
        runs_.reset(); current_ = Row{}; comparisons_.reset(); loaded_ = available_ = false;
        if (opened_) { opened_ = false; child_->close(); }
    }
    bool lastOrderValues(std::vector<ExprValue>& values) const {
        values.clear();if(!available_)return false;
        if(projectKeys_)values=current_.keys;
        else for(const auto column:columns_)values.push_back(current_.cells.at(column));
        return true;
    }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {
        row = current_.cells; return available_;
    }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        cells.clear(); nulls.clear();
        for (const auto& cell : current_.cells) { cells.push_back(cell.value); nulls.push_back(cell.isNull); }
        return available_;
    }
    std::string preparedPlanNodeName() const override { return "Sort"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
    bool supportsPreparedOuterRow() const override { return child_->supportsPreparedOuterRow(); }
    bool bindPreparedOuterRow(const RowContext& row) override { return child_->bindPreparedOuterRow(row); }
};
class PreparedSetTiesLimitOp final : public Operator {
    OpPtr child_;
    size_t limit_, emitted_=0;
    std::vector<size_t> keys_;
    std::vector<ExprValue> current_, boundary_, currentKeys_;
    PreparedSetSortOp* sorted_ = nullptr;
    std::vector<std::optional<QueryComparisonBinding>> enumComparisons_;
    SetOutputComparisons comparisons_;
    bool opened_=false, done_=false;
    ExprEvaluator evaluator_;
    bool tied() {
        for(size_t index=0;index<keys_.size();++index) {
            const auto key=keys_[index];
            const auto& a=sorted_?currentKeys_.at(index):current_.at(key);
            const auto& b=sorted_?boundary_.at(index):boundary_.at(key);
            if(a.isNull || b.isNull){if(a.isNull!=b.isNull)return false;continue;}
            if(!comparisons_.equal(index,evaluator_,a,b,enumComparisons_[index]))return false;
        }
        return true;
    }
public:
    PreparedSetTiesLimitOp(OpPtr child,size_t limit,std::vector<size_t> keys,
        const std::vector<SelectStmt::OrderByElem>& order,const std::string& database,PreparedSetSortOp* sorted=nullptr)
        :child_(std::move(child)),limit_(limit),keys_(std::move(keys)),sorted_(sorted) {
        if(keys_.size()!=order.size())throw DbError("XX000","set ties has no complete bound output keys");
        for(const auto& key:order)enumComparisons_.push_back(key.enumComparison);
        evaluator_.setCurrentDB(database);
    }
    ~PreparedSetTiesLimitOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();emitted_=0;current_.clear();boundary_.clear();currentKeys_.clear();done_=!limit_;
        comparisons_.reset(keys_.size());
        if(done_)return true;
        try {
            opened_=true;
            if(child_->open())return true;
            propagateChildError(child_.get(),"set ties child open failed");
            try{close();}catch(...){}
            return false;
        } catch(...) {
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if(done_ || !opened_){current_.clear();return false;}
        try {
            checkForQueryInterrupt();
            if(!child_->next(display)) {
                done_=true;current_.clear();
                if(!child_->hasError())return false;
                propagateChildError(child_.get(),"set ties child failed");
                try{close();}catch(...){}
                return false;
            }
            if(!child_->lastStructuredValues(current_))throw DbError("XX000","set ties lost typed output");
            if(sorted_ && !sorted_->lastOrderValues(currentKeys_))throw DbError("XX000","VALUES ties lost its actual sort keys");
            if(emitted_>=limit_ && !tied()){done_=true;current_.clear();return false;}
            if(++emitted_==limit_)boundary_=sorted_?currentKeys_:current_;
            instrument.emitted=true;return true;
        } catch(...) {
            // This wrapper can fail after Sort successfully returned a row.
            // Release the same child graph and its spool before propagating
            // the original cancellation/comparison/allocation exception.
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    void close() override {
        current_.clear();boundary_.clear();currentKeys_.clear();comparisons_.reset();done_=true;emitted_=0;
        if(opened_){opened_=false;child_->close();}
    }
    bool supportsStructuredRows() const override {return true;}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {row=current_;return !current_.empty();}
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        cells.clear();nulls.clear();for(const auto& cell:current_){cells.push_back(cell.value);nulls.push_back(cell.isNull);}return !current_.empty();
    }
    std::string preparedPlanNodeName() const override {return "Limit";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {return child_->bindPreparedOuterRow(row);}
};

// Expression counts belong to the genuine query execution. Evaluate them
// once per open, before opening the producer, and retain its actual sort keys
// for WITH TIES. No parameter frame or SQL child is owned by this wrapper.
class PreparedValuesCountOp final : public Operator {
    OpPtr child_;
    std::function<std::pair<std::optional<size_t>,size_t>()> counts_;
    std::optional<size_t> limit_;
    size_t offset_=0,emitted_=0;
    bool opened_=false,done_=false,ties_;
    PreparedSetSortOp* sorted_;
    std::vector<ExprValue> current_,boundary_,currentKeys_;
    std::vector<std::optional<QueryComparisonBinding>> enumComparisons_;
    SetOutputComparisons comparisons_;
    ExprEvaluator evaluator_;
    bool fail(const std::string& message) {
        propagateChildError(child_.get(),message);
        try{close();}catch(...){}
        return false;
    }
    bool tied() {
        for(size_t i=0;i<currentKeys_.size();++i) {
            const auto& a=currentKeys_[i];const auto& b=boundary_.at(i);
            if(a.isNull || b.isNull){if(a.isNull!=b.isNull)return false;continue;}
            if(!comparisons_.equal(i,evaluator_,a,b,enumComparisons_.at(i)))return false;
        }
        return true;
    }
public:
    PreparedValuesCountOp(OpPtr child,std::function<std::pair<std::optional<size_t>,size_t>()> counts,
        const SelectStmt& select,PreparedSetSortOp* sorted,const std::string& database)
        :child_(std::move(child)),counts_(std::move(counts)),ties_(select.withTies),sorted_(sorted) {
        for(const auto& key:select.orderBy)enumComparisons_.push_back(key.enumComparison);
        evaluator_.setCurrentDB(database);
    }
    ~PreparedValuesCountOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();close();done_=false;emitted_=0;
        comparisons_.reset(enumComparisons_.size());
        try {
            auto counts=counts_();limit_=counts.first;offset_=counts.second;
            if(limit_ && !*limit_){done_=true;return true;}
            opened_=true;
            if(child_->open())return true;
            propagateChildError(child_.get(),"VALUES count child open failed");
            try{close();}catch(...){}return false;
        } catch(...) {
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if(done_ || !opened_){current_.clear();return false;}
        try {
            checkForQueryInterrupt();
            while(offset_) {
                if(!child_->next(display)) {
                    done_=true;current_.clear();
                    if(child_->hasError())return fail("VALUES offset child failed");
                    return false;
                }
                --offset_;
            }
            if(limit_ && emitted_>=*limit_ && !ties_){done_=true;current_.clear();return false;}
            if(!child_->next(display)) {
                done_=true;current_.clear();
                if(child_->hasError())return fail("VALUES count child failed");
                return false;
            }
            if(!child_->lastStructuredValues(current_))throw DbError("XX000","VALUES count lost typed output");
            if(ties_) {
                if(!sorted_ || !sorted_->lastOrderValues(currentKeys_))throw DbError("XX000","VALUES count lost its sort keys");
                if(limit_ && emitted_>=*limit_ && !tied()){done_=true;current_.clear();return false;}
            }
            ++emitted_;
            if(ties_ && limit_ && emitted_==*limit_)boundary_=currentKeys_;
            instrument.emitted=true;return true;
        } catch(...) {
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    void close() override {
        current_.clear();boundary_.clear();currentKeys_.clear();comparisons_.reset();done_=true;emitted_=0;
        if(opened_){opened_=false;child_->close();}
    }
    bool supportsStructuredRows() const override {return true;}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {row=current_;return !current_.empty();}
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        cells.clear();nulls.clear();for(const auto& cell:current_){cells.push_back(cell.value);nulls.push_back(cell.isNull);}return !current_.empty();
    }
    std::string preparedPlanNodeName() const override {return "Limit";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {close();return child_->bindPreparedOuterRow(row);}
};

// The same execution owner that normally surrounds Append must also surround
// dynamic counts: count expressions run before either branch opens. Borrowed
// owners keep empty callbacks and are never replaced or closed here.
class PreparedSetExecutionOp final : public Operator {
    OpPtr child_;
    std::function<void()> begin_,finish_;
    std::shared_ptr<RowContext> caller_;
    bool active_=false,opened_=false;
    bool fail(const char* message) {
        propagateChildError(child_.get(),message);
        try{close();}catch(...){}return false;
    }
public:
    PreparedSetExecutionOp(OpPtr child,std::function<void()> begin,std::function<void()> finish,
        std::shared_ptr<RowContext> caller)
        :child_(std::move(child)),begin_(std::move(begin)),finish_(std::move(finish)),caller_(std::move(caller)) {}
    ~PreparedSetExecutionOp() override {try{close();}catch(...) {}}
    bool open() override {
        OpenInstrument instrument(this);clearError();close();
        try {
            active_=true;if(begin_)begin_();
            opened_=true;
            return child_->open() || fail("set count execution open failed");
        } catch(...) {
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    bool next(std::string& display) override {
        NextInstrument instrument(this);
        if(!opened_)return false;
        try {
            checkForQueryInterrupt();
            if(child_->next(display)){instrument.emitted=true;return true;}
            if(child_->hasError())return fail("set count execution failed");
            close();return false;
        } catch(...) {
            const auto primary=std::current_exception();try{close();}catch(...){}std::rethrow_exception(primary);
        }
    }
    void close() override {
        std::exception_ptr primary;
        if(opened_){opened_=false;try{child_->close();}catch(...){primary=std::current_exception();}}
        if(active_){active_=false;try{if(finish_)finish_();}catch(...){if(!primary)primary=std::current_exception();}}
        if(primary)std::rethrow_exception(primary);
    }
    bool supportsStructuredRows() const override {return child_->supportsStructuredRows();}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {return child_->lastStructuredValues(row);}
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        return child_->lastStructuredRow(cells,nulls);
    }
    std::string preparedPlanNodeName() const override {return "Result";}
    std::vector<Operator*> preparedPlanChildren() const override {return {child_.get()};}
    bool supportsPreparedOuterRow() const override {return child_->supportsPreparedOuterRow();}
    bool bindPreparedOuterRow(const RowContext& row) override {
        close();if(!child_->bindPreparedOuterRow(row))return false;
        *caller_=row;return true;
    }
};

std::function<std::pair<std::optional<size_t>,size_t>()> preparedRowCounts(
    SelectStmt* select,QueryPlanner::PreparedExecutionProvider executionProvider,
    std::function<RowContext()> contextProvider,PreparedSetSortOp* sorted) {
    return [select,executionProvider,contextProvider,sorted] {
        const auto actual=executionProvider();
        if(!actual)throw DbError("XX000","row count lost its live execution owner");
        auto context=actual->context(contextProvider());
        const auto count=[&](Expr* expression,std::optional<size_t> fallback,
                             std::optional<int64_t> signedCount,const char* clause,const char* state) {
            if(!expression) {
                if(signedCount && *signedCount<0)throw DbError(state,std::string(clause)+" must not be negative");
                return fallback;
            }
            actual->prepareExpression(expression);
            const auto value=actual->evaluate(expression,context);
            if(value.isNull)return std::optional<size_t>{};
            int64_t number=0;
            const auto parsed=std::from_chars(value.value.data(),value.value.data()+value.value.size(),number);
            if(parsed.ec!=std::errc{} || parsed.ptr!=value.value.data()+value.value.size())
                throw DbError("XX000","row count lost its bound bigint value");
            if(number<0)throw DbError(state,std::string(clause)+" must not be negative");
            if(static_cast<uint64_t>(number)>std::numeric_limits<size_t>::max())throw DbError("54000","row count is too large");
            return std::optional<size_t>{static_cast<size_t>(number)};
        };
        // OFFSET is evaluated first, and a failure must not prepare LIMIT.
        const auto offset=count(select->offsetExpr.get(),select->offset,select->signedOffsetCount,"OFFSET","2201X");
        const auto limit=count(select->limitExpr.get(),select->limit,
            select->fetchFirst?select->signedFetchCount:select->signedLimitCount,
            "LIMIT","2201W");
        if(sorted) {
            std::optional<size_t> bound;
            if(limit && *limit && !select->withTies && *limit<=std::numeric_limits<size_t>::max()-offset.value_or(0))
                bound=*limit+offset.value_or(0);
            sorted->setRowBound(bound);
        }
        return std::make_pair(limit,offset.value_or(0));
    };
}
}

bool QueryPlanner::supportsPreparedSetEquality(const QueryRowDescriptor& output) {
    return std::all_of(output.begin(), output.end(), [](const QueryOutputColumn& field) {
        switch (field.typeOid) {
            case 16: case 20: case 21: case 23: case 25: // bool, integers, text
            case 700: case 701: case 1042: case 1043:   // floats, char, varchar
            case 1700: case 2950:                      // numeric, uuid
                return true;
            default:
                return false;
        }
    });
}

OpPtr QueryPlanner::buildPreparedSetPlan(StorageEngine* engine,const std::string& database,
    std::shared_ptr<PreparedQuery> query,SelectStmt* select,PreparedSetBranchFactory branchFactory,
    PreparedChildExecutor reader,PreparedChildCursorFactory cursor,bool ownsScalarChildren,
    bool planRootConstants,PreparedExecutionProvider executionProvider,const RowContext& outerRow) {
    if(!engine || !query || !select || !branchFactory || select->setOp==SetOp::None || !select->setOpRhs)
        throw DbError("0A000","typed set plan requires genuine operands");
    const auto inputs=query->setOperationInputs.find(select);
    const auto output=query->statementOutputs.find(select);
    if(inputs==query->setOperationInputs.end() || output==query->statementOutputs.end() ||
        inputs->second.left.size()!=output->second.size() || inputs->second.right.size()!=output->second.size())
        throw DbError("XX000","typed set plan has no complete bound branch descriptors");
    const bool needsEquality = select->setOp != SetOp::Union || !select->setOpAll;
    if (needsEquality && !supportsPreparedSetEquality(output->second))
        throw DbError("0A000", "set comparison requires a supported output type");
    std::function<void()> begin,finish;
    if(!executionProvider) {
        if(!cursor) {
            cursor=[engine,database,query](const Stmt* child,const RowContext& row) {
                auto graph=buildPreparedQueryPlan(engine,database,query,child,row);
                return makePreparedCursor(std::move(graph),query->statementOutputs.at(child));
            };
            ownsScalarChildren=false; // physical fallback, not an explicit paired SQL host
        }
        // All bodies borrow this live slot. Reopening/rebinding the same
        // actual tree creates a fresh carrier; no initplan datum crosses an
        // execution and neither branch prematurely closes sibling cursors.
        auto slot=std::make_shared<std::shared_ptr<PreparedQueryExecution>>();
        auto started=std::make_shared<bool>(false);
        const auto prepare=[slot,engine,database,query,select,reader,cursor,ownsScalarChildren,planRootConstants] {
            *slot=std::make_shared<PreparedQueryExecution>(query,engine,database);
            (*slot)->setQueryExecutor(reader);(*slot)->setChildCursorFactory(cursor,ownsScalarChildren);
            if(planRootConstants)(*slot)->planStatementConstants(select);
        };
        prepare();
        executionProvider=[slot]{return *slot;};
        begin=[started,prepare]{if(*started)prepare();*started=true;};
        finish=[slot]{if(*slot)(*slot)->closeChildCursors();};
    }
    auto* left=select->setOpLhs?dynamic_cast<SelectStmt*>(select->setOpLhs.get()):select;
    auto* right=dynamic_cast<SelectStmt*>(select->setOpRhs.get());
    if(!left || !right)throw DbError("42601","set operands must be retained query statements");
    auto leftPlan=branchFactory(left,!select->setOpLhs,executionProvider);
    auto rightPlan=branchFactory(right,false,executionProvider);
    if(!leftPlan || !rightPlan)throw DbError("XX000","typed set branch factory returned no actual graph");
    const bool expressionCounts=bool(select->limitExpr || select->offsetExpr ||
        (select->signedLimitCount && *select->signedLimitCount<0) ||
        (select->signedOffsetCount && *select->signedOffsetCount<0));
    auto append=std::make_unique<PreparedAppendOp>(std::move(leftPlan),std::move(rightPlan),
        inputs->second.left,inputs->second.right,output->second,!needsEquality,
        expressionCounts?std::function<void()>{}:std::move(begin),
        expressionCounts?std::function<void()>{}:std::move(finish));
    OpPtr root;
    if (needsEquality)
        root = std::make_unique<PreparedSetMatchOp>(std::move(append), select->setOp, select->setOpAll, output->second);
    else
        root = std::move(append);
    PreparedSetSortOp* sorted=nullptr;
    if(!select->orderBy.empty()) {
        const auto keys=query->setOrderColumns.find(select);
        if(keys==query->setOrderColumns.end())throw DbError("XX000","set sort has no bound output ordinals");
        std::optional<size_t> bound;
        // Every branch output is still evaluated. Only the final ordinary
        // prefix may discard candidates; WITH TIES must retain every peer.
        if(!expressionCounts && select->limit && *select->limit && !select->withTies &&
            (!select->signedFetchCount || *select->signedFetchCount>=0)) {
            const size_t offset=select->offset.value_or(0);
            if(*select->limit<=std::numeric_limits<size_t>::max()-offset)bound=offset+*select->limit;
        }
        root=std::make_unique<PreparedSetSortOp>(std::move(root),keys->second,select->orderBy,
            output->second,bound,database);
        sorted=static_cast<PreparedSetSortOp*>(root.get());
    }
    if(expressionCounts) {
        auto caller=std::make_shared<RowContext>(outerRow);
        auto counts=preparedRowCounts(select,executionProvider,[caller]{return *caller;},sorted);
        root=std::make_unique<PreparedValuesCountOp>(std::move(root),std::move(counts),*select,sorted,database);
        return std::make_unique<PreparedSetExecutionOp>(std::move(root),std::move(begin),std::move(finish),std::move(caller));
    }
    if(select->offset && *select->offset)root=std::make_unique<OffsetOp>(std::move(root),*select->offset);
    if(select->signedFetchCount && *select->signedFetchCount<0)
        root=std::make_unique<PreparedNegativeFetchOp>(std::move(root));
    else if(select->fetchFirst && select->withTies && !select->limit && !select->signedFetchCount)
        root=std::make_unique<PreparedRowCountErrorOp>(std::move(root),"2201W","row count cannot be null in FETCH FIRST ... WITH TIES clause");
    else if(select->limit) {
        if(select->withTies)root=std::make_unique<PreparedSetTiesLimitOp>(std::move(root),*select->limit,query->setOrderColumns.at(select),select->orderBy,database,sorted);
        else root=std::make_unique<LimitOp>(std::move(root),*select->limit);
    }
    return root;
}

OpPtr QueryPlanner::applyPreparedValuesClauses(StorageEngine* engine,const std::string& database,
    std::shared_ptr<PreparedQuery> query,SelectStmt* select,OpPtr source,
    PreparedExecutionProvider executionProvider,std::function<RowContext()> contextProvider,bool selectBodyOnly) {
    if(!engine || !query || !select || !source || select->command!=SqlCommand::Values)
        throw DbError("XX000","VALUES clauses require their genuine producer");
    if(selectBodyOnly)return source;
    const bool expressionCounts=bool(select->limitExpr || select->offsetExpr);
    const auto output=query->statementOutputs.find(select);
    if(output==query->statementOutputs.end())throw DbError("XX000","VALUES has no bound output descriptor");
    std::vector<size_t> keys;
    PreparedSetSortOp* sorted=nullptr;
    if(!select->orderBy.empty()) {
        if(!executionProvider || !contextProvider)throw DbError("XX000","VALUES ORDER lost its actual execution context");
        const PreparedQuery::SourceRange* valuesRange=nullptr;
        for(const auto& range:query->sourceRanges)
            if(range.owner==select && !range.source && range.relationName.empty() && !range.cteStatement && !range.viewQuery) {
                if(valuesRange)throw DbError("XX000","VALUES has multiple output ranges");
                valuesRange=&range;
            }
        if(!valuesRange || valuesRange->columns.size()!=output->second.size())
            throw DbError("XX000","VALUES ORDER has no genuine output range");
        const size_t ordinal=valuesRange->ordinal;
        const size_t absent=std::numeric_limits<size_t>::max();
        std::vector<size_t> targets, priorKeys;
        std::vector<std::string> identities;
        for(const auto& order:select->orderBy) {
            size_t target=absent;
            if(const auto* literal=dynamic_cast<const LiteralExpr*>(order.expr.get())) {
                if(literal->typeName.empty() && !literal->preparedSubquery && !literal->value.empty() &&
                    std::all_of(literal->value.begin(),literal->value.end(),[](unsigned char c){return c>='0' && c<='9';})) {
                    size_t position=0;
                    for(char c:literal->value){if(position>output->second.size())break;position=position*10+size_t(c-'0');}
                    if(!position || position>output->second.size())throw DbError("42P10","ORDER BY position is not in select list");
                    target=position-1;
                }
            } else if(const auto* column=dynamic_cast<const ColumnRefExpr*>(order.expr.get())) {
                if(column->binding && !column->binding->scopeDepth && column->binding->sourceOrdinal==ordinal)
                    target=column->binding->columnOrdinal;
            }
            const auto identity=target!=absent?"output:"+std::to_string(target):
                ExprHelper::preparedSortExpressionIdentity(order.expr.get(),*query,[](const ColumnRefExpr& column) {
                    if(!column.binding)throw DbError("XX000","VALUES sort expression lost its source identity");
                    const auto& binding=*column.binding;
                    return std::to_string(binding.scopeDepth)+":"+std::to_string(binding.sourceOrdinal)+":"+
                        std::to_string(binding.columnOrdinal)+":"+binding.declaredType;
                },database,engine,false,select);
            const auto previous=std::find(identities.begin(),identities.end(),identity);
            priorKeys.push_back(previous==identities.end()?absent:static_cast<size_t>(previous-identities.begin()));
            identities.push_back(identity);targets.push_back(target);keys.push_back(keys.size());
        }
        bool needsContext=false;
        for(size_t i=0;i<targets.size();++i)needsContext=needsContext || (targets[i]==absent && priorKeys[i]==absent);
        auto projectKeys=[query,select,ordinal,targets=std::move(targets),priorKeys=std::move(priorKeys),executionProvider,contextProvider,absent,needsContext]
            (const std::vector<ExprValue>& cells) {
            // Bare output references already own their typed values. Only
            // expression keys need a bound output snapshot/context copy.
            std::shared_ptr<PreparedQueryExecution> actual;
            std::optional<RowContext> context;
            if(needsContext) {
                actual=executionProvider();
                if(!actual)throw DbError("XX000","VALUES ORDER lost its live execution owner");
                context=actual->context(contextProvider());
                actual->setSourceRow(*context,ordinal,cells);
            }
            std::vector<ExprValue> values;values.reserve(targets.size());
            for(size_t i=0;i<targets.size();++i) {
                if(priorKeys[i]!=absent)values.push_back(values.at(priorKeys[i]));
                else if(targets[i]!=absent)values.push_back(cells.at(targets[i]));
                else {
                    actual->prepareExpression(select->orderBy[i].expr.get());
                    values.push_back(actual->evaluate(select->orderBy[i].expr.get(),*context));
                }
            }
            return values;
        };
        std::optional<size_t> bound;
        if(!expressionCounts && select->limit && *select->limit && !select->withTies &&
            (!select->signedFetchCount || *select->signedFetchCount>=0)) {
            const size_t offset=select->offset.value_or(0);
            if(*select->limit<=std::numeric_limits<size_t>::max()-offset)bound=offset+*select->limit;
        }
        source=std::make_unique<PreparedSetSortOp>(std::move(source),keys,select->orderBy,
            output->second,bound,database,std::move(projectKeys));
        sorted=static_cast<PreparedSetSortOp*>(source.get());
    }
    if(select->withTies && !sorted)throw DbError("42601","WITH TIES cannot be specified without ORDER BY");
    if(expressionCounts) {
        if(!executionProvider || !contextProvider)throw DbError("XX000","VALUES count lost its execution context");
        auto counts=preparedRowCounts(select,executionProvider,contextProvider,sorted);
        return std::make_unique<PreparedValuesCountOp>(std::move(source),std::move(counts),*select,sorted,database);
    }
    // PostgreSQL checks OFFSET before LIMIT, even when LIMIT is zero.
    if(select->signedOffsetCount && *select->signedOffsetCount<0)
        return std::make_unique<PreparedRowCountErrorOp>(std::move(source),"2201X","OFFSET must not be negative");
    if(select->signedLimitCount && *select->signedLimitCount<0)
        return std::make_unique<PreparedRowCountErrorOp>(std::move(source),"2201W","LIMIT must not be negative");
    if(select->offset && *select->offset)source=std::make_unique<OffsetOp>(std::move(source),*select->offset);
    if(select->signedFetchCount && *select->signedFetchCount<0)
        source=std::make_unique<PreparedNegativeFetchOp>(std::move(source));
    else if(select->fetchFirst && select->withTies && !select->limit && !select->signedFetchCount)
        source=std::make_unique<PreparedRowCountErrorOp>(std::move(source),"2201W","row count cannot be null in FETCH FIRST ... WITH TIES clause");
    else if(select->limit) {
        if(select->withTies)source=std::make_unique<PreparedSetTiesLimitOp>(std::move(source),*select->limit,keys,select->orderBy,database,sorted);
        else source=std::make_unique<LimitOp>(std::move(source),*select->limit);
    }
    return source;
}

OpPtr QueryPlanner::buildPreparedSetReturningPlan(StorageEngine* engine,const std::string& database,
    std::shared_ptr<PreparedQuery> query,SelectStmt* select,const RowContext& outer,
    PreparedChildExecutor reader,PreparedChildCursorFactory cursor,bool planRootConstants,
    PreparedSetReturningReader providerReader) {
    if(!select)return {};
    bool returning=false;
    for(const auto& target:select->selectList)
        if(const auto* call=dynamic_cast<const FunctionCallExpr*>(target.expr.get());call && call->setReturning)returning=true;
    if(!returning && !select->functionSourceProducer)return {};
    if(select->command!=SqlCommand::Select || select->fromClause || select->setOp!=SetOp::None ||
        select->setOpLhs || select->setOpRhs || !select->groupBy.empty() || !select->groupByElems.empty() ||
        select->having || select->distinct || !select->distinctOn.empty() || !select->orderBy.empty() ||
        !select->windowDefs.empty() || !select->locking.empty() || select->withTies)
        throw DbError("0A000","set-returning SELECT requires additional relational lowering");
    if(!query)
        throw DbError("XX000","set-returning SELECT has no prepared query-host implementation");
    // PreparedQueryExecution validates each original child descriptor and
    // requires a real restartable provider for actual correlations. An
    // uncorrelated SQL child is not an unsupported source merely by existing.
    if(select->whereClause) {
        const auto type=ExprHelper::inferParsedResultType(select->whereClause.get(),{},database,engine);
        if(type!="boolean" && type!="unknown")
            throw DbError("42804","argument of WHERE must be type boolean");
        if(type=="unknown") {
            const auto* literal=dynamic_cast<const LiteralExpr*>(select->whereClause.get());
            if(!literal)throw DbError("42804","argument of WHERE must be type boolean");
            CastExpr cast;cast.typeName="boolean";cast.operand=std::make_unique<LiteralExpr>(*literal);
            ExprEvaluator pure;(void)pure.eval(&cast,RowContext{});
        }
    }
    OpPtr plan=std::make_unique<PreparedSetReturningOp>(engine,database,std::move(query),select,outer,
        std::move(reader),std::move(cursor),planRootConstants,std::move(providerReader));
    if(select->offset)plan=std::make_unique<OffsetOp>(std::move(plan),*select->offset);
    if(select->signedFetchCount && *select->signedFetchCount<0)
        plan=std::make_unique<PreparedNegativeFetchOp>(std::move(plan));
    else if(select->limit)plan=std::make_unique<LimitOp>(std::move(plan),*select->limit);
    return plan;
}

OpPtr QueryPlanner::buildPreparedQueryPlan(StorageEngine* engine,const std::string& database,
    std::shared_ptr<PreparedQuery> query,const Stmt* original,const RowContext& outer,
    PreparedExecutionProvider executionProvider,bool selectBodyOnly) {
    auto* select=const_cast<SelectStmt*>(dynamic_cast<const SelectStmt*>(original));
    if(!engine || !query || !select)throw DbError("42601","prepared query plan requires SELECT/VALUES");
    const auto output=query->statementOutputs.find(select);
    if(output==query->statementOutputs.end())throw DbError("XX000","prepared query has no output descriptor");
    const PreparedChildCursorFactory childFactory=[engine,database,query](const Stmt* child,const RowContext& row) {
        auto plan=buildPreparedQueryPlan(engine,database,query,child,row);
        return makePreparedCursor(std::move(plan),query->statementOutputs.at(child));
    };
    if(!selectBodyOnly && select->setOp!=SetOp::None)
        return buildPreparedSetPlan(engine,database,query,select,
            [engine,database,query,outer](SelectStmt* branch,bool body,PreparedExecutionProvider owner) {
                return buildPreparedQueryPlan(engine,database,query,branch,outer,std::move(owner),body);
            },{},childFactory,false,false,std::move(executionProvider),outer);
    if(!selectBodyOnly)if(auto set=buildPreparedSetReturningPlan(engine,database,query,select,outer,{},childFactory))return set;
    if(select->command==SqlCommand::Values) {
        if(select->whereClause || (!selectBodyOnly && select->setOp!=SetOp::None))
            throw DbError("0A000","prepared VALUES requires additional clause lowering");
        auto slot=std::make_shared<std::shared_ptr<PreparedQueryExecution>>();
        const auto prepare=[slot,executionProvider,query,engine,database,select,childFactory,selectBodyOnly] {
            *slot=executionProvider?executionProvider():std::make_shared<PreparedQueryExecution>(query,engine,database);
            if(!*slot)throw DbError("XX000","prepared VALUES lost its execution owner");
            if(!executionProvider)(*slot)->setChildCursorFactory(childFactory,false);
            for(auto& row:select->valuesRows)for(auto& cell:row)(*slot)->prepareExpression(cell.get());
            if(!selectBodyOnly)for(auto& order:select->orderBy)(*slot)->prepareExpression(order.expr.get());
            if(!selectBodyOnly) {
                if(select->limitExpr)(*slot)->prepareExpression(select->limitExpr.get());
                if(select->offsetExpr)(*slot)->prepareExpression(select->offsetExpr.get());
            }
            (*slot)->prepareChildCursors();
        };
        prepare();
        auto cells=std::make_shared<RowContext>((*slot)->context(outer));
        auto descriptor=selectBodyOnly?query->setOperationInputs.at(select).left:output->second;
        OpPtr source=std::make_unique<PreparedSourceRowsOp>(descriptor,
            [select,slot,executionProvider,cells,descriptor](size_t index,std::vector<ExprValue>& row) {
                if(index>=select->valuesRows.size())return false;
                row.clear();
                const auto actual=executionProvider?executionProvider():*slot;
                for(size_t i=0;i<select->valuesRows[index].size();++i) {
                    actual->prepareExpression(select->valuesRows[index][i].get());
                    auto value=actual->evaluate(select->valuesRows[index][i].get(),*cells);
                    value.typeName=descriptor.at(i).type;row.push_back(std::move(value));
                }
                return true;
            }, [cells,slot,executionProvider](const RowContext& caller) {
                const auto actual=executionProvider?executionProvider():*slot;
                *cells=actual->context(caller);
            });
        auto started=std::make_shared<bool>(false);
        const auto begin=[started,prepare] {if(*started)prepare();*started=true;};
        const auto finish=[slot,executionProvider] {if(!executionProvider && *slot)(*slot)->closeChildCursors();};
        if(!selectBodyOnly && (select->limitExpr || select->offsetExpr)) {
            // The existing VALUES owner begins this invocation before its
            // count expressions. Reopen refreshes its own carrier once;
            // LIMIT zero still avoids opening/evaluating the row producer.
            auto clauses=applyPreparedValuesClauses(engine,database,query,select,std::move(source),
                [slot,executionProvider]{return executionProvider?executionProvider():*slot;},
                [cells]{return *cells;},false);
            return std::make_unique<PreparedValuesExecutionOp>(std::move(clauses),begin,finish);
        }
        OpPtr owned=std::make_unique<PreparedValuesExecutionOp>(std::move(source),begin,finish);
        return applyPreparedValuesClauses(engine,database,query,select,std::move(owned),
            [slot,executionProvider]{return executionProvider?executionProvider():*slot;},
            [cells]{return *cells;},selectBodyOnly);
    }
    if(!supportsPreparedSelectShape(*select,true,false,selectBodyOnly))
        throw DbError("0A000","prepared query requires additional relational lowering");
    TableSchema schema;OpPtr source;
    if(!select->fromClause)source=std::make_unique<PreparedResultOp>();
    else {
        const auto found=std::find_if(query->sourceRanges.begin(),query->sourceRanges.end(),
            [&](const auto& range){return range.owner==select && range.source==select->fromClause.get() && !range.mergedUsing;});
        if(found==query->sourceRanges.end())throw DbError("XX000","prepared source has no range descriptor");
        if(found->cteStatement || select->fromClause->type==FromItem::Type::Subquery) {
            const auto* statement=found->cteStatement?found->cteStatement:select->fromClause->subquery.get();
            if(!dynamic_cast<const SelectStmt*>(statement))
                throw DbError("0A000","writing CTE requires the statement-owned source provider");
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;
                const auto& declaration = output.typeMetadata;
                if (declaration && declaration->kind == DatumKind::Array &&
                    declaration->actualOid == output.typeOid && declaration->arrayElement &&
                    common_type_detail::array(output.type)) {
                    // The child already owns each array and its declaration.
                    // This schema supplies the slot's shape to the parent.
                    column.dataType = output.type.substr(0, output.type.size() - 2);
                    column.isArray = true;
                    column.isVariableLength = true;
                    schema.append(column);
                    continue;
                }
                if((output.typeOid==2206 && output.type=="regtype") ||
                   (output.typeOid==2205 && output.type=="regclass")) {
                    // This source owns typed ExprValue cells, not heap bytes.
                    // Its genuine descriptor keeps OIDs without inventing a
                    // TypeRegistry storage codec for the displayed type name.
                    for(const auto& row:CatalogManager::builtinTypeRows())if(row.oid==output.typeOid) {
                        column.dataType=output.type;column.dsize=row.typlen;
                        column.isVariableLength=row.typlen<0;break;
                    }
                    schema.append(column);continue;
                }
                const auto error=TypeRegistry::instance().resolveColumnType(column,output.type,{},false);
                if(!error.empty())throw DbError("42704",error);schema.append(column);
            }
            struct Cache {
                std::unique_ptr<PreparedQueryCursor> cursor;
                std::vector<std::vector<ExprValue>> rows;
                bool eof=false;
            };
            auto cache=std::make_shared<Cache>();
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [cache,statement,outer,childFactory](size_t index,std::vector<ExprValue>& row) {
                    if(index<cache->rows.size()){row=cache->rows[index];return true;}
                    if(cache->eof)return false;
                    if(!cache->cursor)cache->cursor=childFactory(statement,outer);
                    if(!cache->cursor->next(row)){cache->cursor->close();cache->eof=true;return false;}
                    cache->rows.push_back(row);return true;
                });
        } else if(found->relationSchema=="pg_catalog" &&
                  (found->relationName=="pg_class" || found->relationName=="pg_attribute")) {
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;column.dataType=output.type;
                column.isVariableLength=true;schema.append(column);
            }
            const bool attributes=found->relationName=="pg_attribute";
            auto rows=std::make_shared<std::optional<PreparedQueryRows>>();
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [engine,database,rows,attributes](size_t index,std::vector<ExprValue>& row) {
                    if(!*rows) {
                        const auto metadata=engine->catalogService().metadataSnapshot(database);
                        *rows=attributes?system_catalog::attributeRows(metadata):system_catalog::classRows(metadata);
                    }
                    if(index>=(**rows).size())return false;
                    row=(**rows)[index];return true;
                });
        } else if(found->relationSchema=="pg_catalog" &&
                  (found->relationName=="pg_attrdef" || found->relationName=="pg_depend")) {
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;column.dataType=output.type;
                column.isVariableLength=true;schema.append(column);
            }
            const bool dependencies=found->relationName=="pg_depend";
            auto rows=std::make_shared<std::optional<PreparedQueryRows>>();
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [engine,database,rows,dependencies](size_t index,std::vector<ExprValue>& row) {
                    if(!*rows)*rows=default_catalog::rows(engine->catalogService().defaultCatalogSnapshot(database,
                        dependencies ? CatalogManager::DefaultCatalogKind::Depend : CatalogManager::DefaultCatalogKind::Attrdef),dependencies);
                    if(index>=(**rows).size())return false;
                    row=(**rows)[index];return true;
                });
        } else if(found->relationSchema=="pg_catalog" && found->relationName=="pg_sequence") {
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;column.dataType=output.type;
                column.isVariableLength=true;schema.append(column);
            }
            auto rows=std::make_shared<std::optional<PreparedQueryRows>>();
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [engine,database,rows](size_t index,std::vector<ExprValue>& row) {
                    if(!*rows)*rows=engine->getSequenceCatalogRows(database);
                    if(index>=(**rows).size())return false;
                    row=(**rows)[index];return true;
                });
        } else if(found->relationSchema=="pg_catalog" && found->relationName=="pg_stats") {
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;column.dataType=output.type;
                column.isVariableLength=true;schema.append(column);
            }
            auto rows=std::make_shared<std::optional<PreparedQueryRows>>();
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [engine,database,rows](size_t index,std::vector<ExprValue>& row) {
                    if(!*rows)*rows=engine->getAnalyzedStatisticsRows(database);
                    if(index>=(**rows).size())return false;
                    row=(**rows)[index];return true;
                });
        } else if(found->relationSchema=="pg_catalog" && found->relationName=="pg_namespace") {
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;column.dataType=output.type;
                column.isVariableLength=true;schema.append(column);
            }
            auto rows=std::make_shared<std::optional<std::vector<PgNamespaceRow>>>();
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [engine,database,rows](size_t index,std::vector<ExprValue>& row) {
                    if(!*rows)*rows=engine->catalogService().metadataSnapshot(database).namespaces;
                    if(index>=(**rows).size())return false;
                    row=ownedNamespaceCatalogCells((**rows)[index]);return true;
                });
        } else if(found->relationSchema=="pg_catalog" && found->relationName=="pg_type") {
            for(const auto& output:found->columns) {
                Column column;column.dataName=output.name;column.dataType=output.type;
                column.isVariableLength=true;schema.append(column);
            }
            auto rows = std::make_shared<TypeCatalogScan>(typeCatalogOidFilter(*found), true);
            source=std::make_unique<PreparedSourceRowsOp>(found->columns,
                [engine,database,rows](size_t index,std::vector<ExprValue>& row) {
                    return rows->read(engine->catalogService(), database, index, row);
                });
        } else {
            if(found->relationName.empty())
                throw DbError("0A000","prepared source requires a logical source provider");
            // These are canonical binder metadata, not SQL spelling. Storage
            // takes the physical identifier; quoting or parsing a rendered
            // qualified name here loses public/custom/temp relation forks.
            const auto* session=currentSession();
            const bool temporary=session &&
                (found->relationSchema==sessionTempSchemaName(*session) || found->relationSchema=="pg_temp");
            auto table=temporary?tempTablePrefix(*session,found->relationName):
                found->relationSchema.empty() || found->relationSchema=="public"?found->relationName:
                found->relationSchema+"__"+found->relationName;
            bool materializedSource = false;
            if (!temporary) {
                const auto materialized = engine->resolveMaterializedView(database,
                    found->relationSchema.empty() ? "public" : found->relationSchema,
                    found->relationName);
                if (materialized) {
                    if (!materialized->populated)
                        throw DbError("55000", "materialized view has not been populated");
                    table = materialized->backingTable;
                    materializedSource = true;
                }
            }
            schema=engine->getTableSchema(database,table);
            if (!temporary && !materializedSource && !found->viewQuery && !found->source->only &&
                select->ctes.empty() && found->relationSchema != "pg_catalog" &&
                found->relationSchema != "information_schema" && engine->inTransaction() &&
                engine->transactionDatabase() == database && session &&
                (sessionIsAdmin(*session) || engine->hasPermission(database, table,
                    effectiveSessionRole(*session), StorageEngine::TablePrivilege::Select))) {
                const auto descendants = inheritedTableDescendants(*engine, database, table);
                if (supportsInheritedTableSource(*engine, database, schema, descendants))
                    source = makeInheritedTableSource(*engine, database, schema, *found, descendants);
            }
            if (!source) source=std::make_unique<TableScanOp>(engine,database,table);
        }
    }
    return buildPreparedSelectPlan(engine,database,std::move(query),select,schema,std::move(source),outer,{},childFactory,false,false,
        std::move(executionProvider),selectBodyOnly);
}

TableScanOp::TableScanOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename)
    : engine_(engine), dbname_(dbname), tablename_(tablename) {}

TableScanOp::~TableScanOp() { try { close(); } catch (...) {} }

bool TableScanOp::open() {
    OpenInstrument startup(this);
    if (tableLockHeld_) close();
    clearError();
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rows_.clear();
    bufferedNulls_.clear();
    lastBufferedNullsValid_ = false;
    lastRid_ = 0;
    pos_ = 0; lastNulls_.clear(); cursor_.reset();
    const bool partitioned = tbl_.partitionType != TableSchema::PartitionType::None;
    const bool mayStream = !partitioned || scanPartitionsOnDemand_ || rangePruning_.has_value();
    const bool enforcePolicy = mayStream && engine_->rlsAppliesTo(tbl_);
    std::optional<std::vector<std::string>> partitions;
    if (rangePruning_ && !enforcePolicy && tbl_.physicalRelationId == rangePruning_->relationId &&
        tbl_.partitionKey == rangePruning_->column &&
        rangePruning_->columnOrdinal < tbl_.len() &&
        tbl_.cols[rangePruning_->columnOrdinal].dataName == rangePruning_->column &&
        StorageEngine::integerPartitionDomain(tbl_) == rangePruning_->domain) {
        partitions = StorageEngine::pruneIntegerPartitions(tbl_, rangePruning_->filter);
    }
    prunedPartitions_ = partitions.has_value();
    if ((!partitioned || scanPartitionsOnDemand_ || partitions) && !enforcePolicy) {
        cursor_ = engine_->beginHeapScanUsingSchema(dbname_, tablename_, tbl_,
            nullptr, partitioned, partitions ? &*partitions : nullptr);
        return true;
    }
    bool toastFailed = false;
    bool bitmapFailed = false;
    const bool capturePartitionNulls =
        partitioned && tbl_.formatVersion == DATA_FILE_FORMAT_VERSION;
    const bool hasVariableColumns = tbl_.hasVariableLength();
    const auto appendRow = [&](uint32_t pageId, uint16_t slotId,
                               const char* data, size_t len) {
        if (toastFailed || bitmapFailed) return;
        std::vector<bool> capturedNulls;
        std::optional<detail::ScopedStoredRowNullBitmap> nullOwner;
        if (capturePartitionNulls) {
            if (!StorageEngine::captureCurrentScanNullBitmap(
                    tbl_, data, len, capturedNulls) || capturedNulls.size() != tbl_.len()) {
                bitmapFailed = true;
                return;
            }
            // TOAST resolution must use this leaf's bitmap, not the parent
            // heap reached by an ambiguous page/slot RID.
            if (hasVariableColumns) nullOwner.emplace(tbl_, capturedNulls);
        }
        std::string row(data, len);
        if (hasVariableColumns) {
            bool toastOk = false;
            row = engine_->resolveToastValues(dbname_, tablename_, std::move(row), tbl_, &toastOk);
            if (!toastOk) {
                toastFailed = true;
                return;
            }
        }
        rows_.emplace_back(StorageEngine::encodeRid(pageId, slotId), std::move(row));
        if (capturePartitionNulls) bufferedNulls_.push_back(std::move(capturedNulls));
    };
    const bool scanOk = partitioned
        ? engine_->forEachVisibleRowUsingSchema(
            dbname_, tablename_, "SELECT", appendRow, nullptr, {}, &tbl_)
        : engine_->forEachVisibleRow(dbname_, tablename_, "SELECT", appendRow);
    if (!scanOk || toastFailed || bitmapFailed) {
        rows_.clear();
        bufferedNulls_.clear();
        setError(toastFailed ? "TOAST value read failed" :
                 bitmapFailed ? "partition tuple NULL bitmap unavailable" : "table scan failed");
        return false;
    }
    pos_ = 0;
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, rows_.size(), false, true);
        statsRecorded_ = true;
    }
    return true;
}

// ========================================================================
// UnnestOp: array literal -> one row per element
// ========================================================================
UnnestOp::UnnestOp(const std::string& arrayLiteral, const std::string& outputColumn)
    : literal_(arrayLiteral), column_(outputColumn) {}

std::vector<std::string> UnnestOp::splitArrayLiteral(const std::string& literal) {
    std::string arr = literal;
    // Case-insensitive helpers for prefix forms.
    auto lowerPrefix = [](const std::string& s, const char* pre) {
        size_t n = std::strlen(pre);
        if (s.size() < n) return false;
        for (size_t i = 0; i < n; ++i) {
            if (std::tolower(static_cast<unsigned char>(s[i])) != pre[i]) return false;
        }
        return true;
    };
    // ARRAY[1,2,3] (any case) -> keep bracketed list only
    size_t arrPrefixPos = std::string::npos;
    for (size_t i = 0; i + 6 <= arr.size(); ++i) {
        if (lowerPrefix(arr.substr(i, 6), "array[")) { arrPrefixPos = i; break; }
    }
    if (arrPrefixPos != std::string::npos) {
        arr = arr.substr(arrPrefixPos + 5);
    } else if (lowerPrefix(arr, "array_get(array,") ) {
        // sqlProcessor-mangled form: array_get(array, 1,2,3) -> 1,2,3
        size_t comma = arr.find(',');
        size_t close = arr.rfind(')');
        if (comma != std::string::npos && close != std::string::npos && close > comma) {
            arr = arr.substr(comma + 1, close - comma - 1);
        }
    }
    while (!arr.empty() && (arr.front() == ' ' || arr.front() == '\t')) arr.erase(arr.begin());
    while (!arr.empty() && (arr.back() == ' ' || arr.back() == '\t')) arr.pop_back();
    if (arr.size() >= 2 && (arr.front() == '{' || arr.front() == '[') &&
        (arr.back() == '}' || arr.back() == ']')) {
        arr = arr.substr(1, arr.size() - 2);
    }
    std::vector<std::string> elems;
    size_t ai = 0;
    while (ai < arr.size()) {
        while (ai < arr.size() && std::isspace(static_cast<unsigned char>(arr[ai]))) ++ai;
        if (ai >= arr.size()) break;
        std::string elem;
        if (arr[ai] == '\'' || arr[ai] == '"') {
            char q = arr[ai++];
            while (ai < arr.size() && arr[ai] != q) elem += arr[ai++];
            if (ai < arr.size()) ++ai;
        } else {
            while (ai < arr.size() && arr[ai] != ',') elem += arr[ai++];
        }
        // trim
        size_t b = 0, e = elem.size();
        while (b < e && std::isspace(static_cast<unsigned char>(elem[b]))) ++b;
        while (e > b && std::isspace(static_cast<unsigned char>(elem[e - 1]))) --e;
        elems.push_back(elem.substr(b, e - b));
        if (ai < arr.size() && arr[ai] == ',') ++ai;
    }
    return elems;
}

bool UnnestOp::open() {
    elements_ = splitArrayLiteral(literal_);
    pos_ = 0;
    return true;
}

bool UnnestOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= elements_.size()) return false;
    // Single-column row layout: "<value> " matching the engine's
    // space-terminated column convention.
    outRow = elements_[pos_] + " ";
    ++pos_;
    rtInstr_.emitted = true;
    return true;
}

void UnnestOp::close() {
    elements_.clear();
    pos_ = 0;
}

TableSchema unnestTableSchema() {
    TableSchema t;
    t.tablename = UnnestOp::kVirtualTableName;
    t.append(makeVarCharColumn("unnest", false, 256, false));
    return t;
}

bool TableScanOp::next(std::string& outRow) {
    return nextRow(outRow, nullptr, nullptr, nullptr, nullptr);
}

bool TableScanOp::nextForProjection(std::string& raw, const TableSchema& schema,
                                    const std::set<std::string>& selected,
                                    std::vector<std::string>& cells,
                                    std::vector<bool>& nulls, bool& decoded) {
    decoded = false;
    // A subclass may implement a different next()/owner contract. Never
    // bypass that virtual method or a filtered/indexed/buffered source.
    // A full projection cannot avoid any source field. Keep its existing
    // materialization path without building an extra selected-field plan.
    if (selected.empty() || selected.size() >= schema.len() ||
        typeid(*this) != typeid(TableScanOp) || !cursor_ ||
        tbl_.formatVersion != DATA_FILE_FORMAT_VERSION ||
        schema.formatVersion != tbl_.formatVersion || schema.len() != tbl_.len() ||
        schema.tablename != tablename_ || tbl_.tablename != tablename_)
        return next(raw);
    size_t matched = 0;
    for (size_t i = 0; i < tbl_.len(); ++i) {
        const auto& source = tbl_.cols[i];
        const auto& consumer = schema.cols[i];
        if (source.dataName != consumer.dataName || source.dataType != consumer.dataType ||
            source.dsize != consumer.dsize || source.isVariableLength != consumer.isVariableLength ||
            source.isNull != consumer.isNull || source.isArray != consumer.isArray ||
            source.generatedKind != consumer.generatedKind || source.storageCodec != consumer.storageCodec)
            return next(raw);
        if (selected.empty() || selected.count(source.dataName)) {
            ++matched;
            // The existing virtual evaluator constructs a complete physical
            // input context. It still needs the full materialization owner.
            if (source.generatedKind == 'v') return next(raw);
        }
    }
    if (!selected.empty() && matched != selected.size()) return next(raw);
    const bool good = nextRow(raw, &schema, &selected, &cells, &nulls);
    decoded = good;
    return good;
}

bool TableScanOp::nextRow(std::string& outRow, const TableSchema* projection,
                          const std::set<std::string>* selected,
                          std::vector<std::string>* cells, std::vector<bool>* nulls) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (cursor_) {
        HeapScanRow row;
        if (!cursor_->next(row)) {
            if (!statsRecorded_) {
                recordTableScan(dbname_, tablename_, pos_, false, !prunedPartitions_);
                statsRecorded_ = true;
            }
            return false;
        }
        lastRid_ = row.rid; lastNulls_ = std::move(row.nulls);
        StorageEngine::bindNullRow(engine_, dbname_, tablename_, lastRid_, tbl_.len());
        bool ok = true;
        if (readVisibleRowsOnly_) {
            outRow.clear();
        } else if (projection) {
            ok = detail::decodeStoredRowProjection(*engine_, dbname_, row.data,
                    *projection, lastNulls_, *selected, *cells, *nulls);
            outRow = std::move(row.data);
        } else {
            // The cursor already captured this exact tuple's physical NULL
            // bitmap. Full-row TOAST materialization must use it too instead
            // of fetching the heap again for each variable-width field.
            detail::ScopedStoredRowNullBitmap nullOwner(tbl_, lastNulls_);
            outRow = engine_->resolveToastValues(dbname_, tablename_, row.data, tbl_, &ok);
        }
        if (!ok) { setError("TOAST value read failed"); return false; }
        ++pos_; rtInstr_.emitted = true; return true;
    }
    if (pos_ >= rows_.size()) return false;
    lastRid_ = rows_[pos_].first;
    outRow = std::move(rows_[pos_].second);
    lastBufferedNullsValid_ = pos_ < bufferedNulls_.size();
    if (lastBufferedNullsValid_) lastNulls_ = std::move(bufferedNulls_[pos_]);
    else lastNulls_.clear();
    ++pos_;
    // Bind stored-NULL visibility for the row handed upstream so
    // projections see the heap null bitmap instead of zero values.
    StorageEngine::bindNullRow(engine_, dbname_, tablename_, lastRid_, tbl_.len());
    rtInstr_.emitted = true;
    return true;
}

bool TableScanOp::lastColumnIsNull(size_t colIdx) const {
    if ((cursor_ || lastBufferedNullsValid_) && colIdx < lastNulls_.size())
        return lastNulls_[colIdx];
    return detail::storedRowColumnIsNull(*engine_, dbname_, tablename_, tbl_, lastRid_, colIdx);
}

bool TableScanOp::lastStoredNullBitmap(std::vector<bool>& nulls) const {
    if ((!cursor_ && !lastBufferedNullsValid_) || !lastRid_) return false;
    nulls = lastNulls_; return true;
}

void TableScanOp::close() {
    if (!tableLockHeld_ && !cursor_ && rows_.empty()) return;
    const bool partialScan = cursor_ && !statsRecorded_;
    cursor_.reset(); lastNulls_.clear();
    bufferedNulls_.clear();
    lastBufferedNullsValid_ = false;
    rows_.clear();
    lastRid_ = 0;
    StorageEngine::unbindNullRow();
    std::exception_ptr statsFailure;
    if (partialScan) {
        // Publish before releasing the relation token: DROP/recreate must
        // not reset statistics between our unlock and this publication.
        try { recordTableScan(dbname_, tablename_, pos_, false, false); }
        catch (...) { statsFailure = std::current_exception(); }
    }
    if (tableLockHeld_) {
        engine_->getLockManager().unlock(tablename_);
        tableLockHeld_ = false;
    }
    statsRecorded_ = false; pos_ = 0;
    if (statsFailure) std::rethrow_exception(statsFailure);
}

// ========================================================================
// ParallelTableScanOp
// ========================================================================

ParallelTableScanOp::ParallelTableScanOp(StorageEngine* engine,
                                         const std::string& dbname,
                                         const std::string& tablename,
                                         int workers)
    : engine_(engine), dbname_(dbname), tablename_(tablename), workers_(workers) {}

bool ParallelTableScanOp::open() {
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rows_.clear();
    pos_ = 0;
    usedParallelWorkers_ = false;
    lastRid_ = 0;
    bool toastFailed = false;

    auto appendSequential = [this, &toastFailed]() {
        return engine_->forEachRow(dbname_, tablename_,
            [this, &toastFailed](uint32_t pageId, uint16_t slotId,
                                 const char* data, size_t len) {
                std::string row(data, len);
                bool toastOk = false;
                row = engine_->resolveToastValues(
                    dbname_, tablename_, row, tbl_, &toastOk);
                if (!toastOk) {
                    toastFailed = true;
                    return;
                }
                rows_.emplace_back(StorageEngine::encodeRid(pageId, slotId), std::move(row));
        });
    };
    auto recordScan = [this]() {
        if (!statsRecorded_) {
            recordTableScan(dbname_, tablename_, rows_.size(), false, true);
            statsRecorded_ = true;
        }
    };

    // Do not move transaction-local visibility/SSI state to anonymous worker
    // threads.  Partitioned relations also need their existing routing path.
    if (workers_ <= 1 || engine_->inTransaction() ||
        tbl_.partitionType != TableSchema::PartitionType::None) {
        if (!appendSequential() || toastFailed) {
            rows_.clear();
            setError(toastFailed ? "TOAST value read failed"
                                 : "parallel scan fallback failed");
            return false;
        }
        recordScan();
        return true;
    }

    const uint32_t pageCount = engine_->tableNumPages(dbname_, tablename_);
    if (pageCount <= 1) {
        recordScan();
        return true;
    }
    const int activeWorkers = std::min<int>(workers_, static_cast<int>(pageCount - 1));
    if (activeWorkers <= 1) {
        if (!appendSequential()) {
            setError("parallel scan fallback failed");
            return false;
        }
        recordScan();
        return true;
    }

    usedParallelWorkers_ = true;
    using Row = std::pair<int64_t, std::string>;
    std::vector<std::vector<Row>> local(static_cast<size_t>(activeWorkers));
    std::atomic<bool> scanFailed{false};
    const auto interruptState = currentQueryInterruptState();
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(activeWorkers));
    for (int worker = 0; worker < activeWorkers; ++worker) {
        const uint32_t begin = 1 + static_cast<uint32_t>(worker) * (pageCount - 1) /
                                   static_cast<uint32_t>(activeWorkers);
        const uint32_t end = 1 + static_cast<uint32_t>(worker + 1) * (pageCount - 1) /
                                 static_cast<uint32_t>(activeWorkers);
        threads.emplace_back([this, &local, &scanFailed, interruptState,
                              worker, begin, end]() {
            setCurrentQueryInterruptState(interruptState);
            auto& output = local[static_cast<size_t>(worker)];
            try {
                if (!engine_->forEachRowPageRange(
                    dbname_, tablename_, begin, end,
                    [this, &output](uint32_t pageId, uint16_t slotId,
                                    const char* data, size_t len) {
                        std::string row(data, len);
                        output.emplace_back(
                            StorageEngine::encodeRid(pageId, slotId),
                            std::move(row));
                    })) {
                    scanFailed.store(true, std::memory_order_relaxed);
                }
            } catch (...) {
                scanFailed.store(true, std::memory_order_relaxed);
            }
            setCurrentQueryInterruptState(nullptr);
        });
    }
    for (auto& thread : threads) thread.join();
    checkForQueryInterrupt();
    if (scanFailed.load(std::memory_order_relaxed)) {
        rows_.clear();
        usedParallelWorkers_ = false;
        setError("parallel page scan failed");
        return false;
    }
    for (auto& part : local) {
        for (auto& row : part) {
            bool toastOk = false;
            row.second = engine_->resolveToastValues(
                dbname_, tablename_, row.second, tbl_, &toastOk);
            if (!toastOk) {
                rows_.clear();
                usedParallelWorkers_ = false;
                setError("TOAST value read failed");
                return false;
            }
            rows_.push_back(std::move(row));
        }
    }
    recordScan();
    return true;
}

bool ParallelTableScanOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    lastRid_ = rows_[pos_].first;
    outRow = rows_[pos_++].second;
    rtInstr_.emitted = true;
    return true;
}

bool ParallelTableScanOp::lastColumnIsNull(size_t colIdx) const {
    return engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, colIdx);
}

void ParallelTableScanOp::close() {
    rows_.clear();
    pos_ = 0;
    lastRid_ = 0;
    statsRecorded_ = false;
    if (tableLockHeld_) {
        engine_->getLockManager().unlock(tablename_);
        tableLockHeld_ = false;
    }
}

// ========================================================================
// IndexScanOp
// ========================================================================

static bool isSingleColumnPrimaryKey(const TableSchema& table,
                                     const std::string& column) {
    if (!table.pkColIndices.empty()) {
        return table.pkColIndices.size() == 1 &&
               table.pkColIndices.front() < table.len() &&
               table.cols[table.pkColIndices.front()].dataName == column;
    }
    size_t primaryColumns = 0;
    bool matches = false;
    for (size_t i = 0; i < table.len(); ++i) {
        if (table.cols[i].isPrimaryKey) {
            ++primaryColumns;
            matches = table.cols[i].dataName == column;
        }
    }
    return primaryColumns == 1 && matches;
}

static std::string integerEqualityValue(const TableSchema& table,
                                       const std::string& column,
                                       const std::string& value,
                                       StorageEngine* engine=nullptr,
                                       const std::string& database={}) {
    for (size_t i = 0; i < table.len(); ++i) {
        if (table.cols[i].dataName != column) continue;
        const auto& type = table.cols[i].dataType;
        if(stored_regtype_detail::scalarType(type) && !table.cols[i].isArray && engine) {
            const auto catalog=regtype_detail::metadata(*engine,database);
            return stored_regtype_detail::input(value,false,catalog,currentSession(),type);
        }
        const bool integer = type == "tinyint" || type == "smallint" ||
            type == "int" || type == "integer" || type == "bigint" ||
            type == "long" || type == "int2" || type == "int4" ||
            type == "int8" || type == "tinyint unsigned" ||
            type == "smallint unsigned" || type == "int unsigned" ||
            type == "integer unsigned" || type == "bigint unsigned";
        if (integer && !table.cols[i].isArray) {
            // Heap extraction emits canonical integer text. A raw bound
            // literal (+0001/-0) must use that same key, without converting
            // through double and losing BIGINT precision. Leave other types
            // and unsupported literals to their existing comparison path.
            const int64_t parsed = StorageEngine::parseInt(value);
            if (parsed != INF) return std::to_string(parsed);
            try {
                const Numeric numeric(value);
                if (numeric.isFinite()) {
                    std::string canonical = numeric.toString();
                    const auto dot = canonical.find('.');
                    // Numeric preserves display scale, so 1.0 has scale 1
                    // despite being integral. Discard only an all-zero
                    // fraction; never round or truncate a non-integral key.
                    if (dot != std::string::npos) {
                        if (canonical.find_first_not_of('0', dot + 1) != std::string::npos)
                            return value;
                        canonical.resize(dot);
                    }
                    const int64_t exact = StorageEngine::parseInt(canonical);
                    if (exact != INF) return std::to_string(exact);
                }
            } catch (const std::invalid_argument&) {
                // Invalid or unsupported literals keep their existing path.
            }
        }
        break;
    }
    return value;
}

static bool equalityIndexKeyIsEmpty(const TableSchema& table,
                                    const StorageEngine::Condition& condition) {
    if (condition.op != "=") return false;
    for(size_t i=0;i<table.len();++i)
        if(table.cols[i].dataName==condition.colName && stored_regtype_detail::scalarType(table.cols[i].dataType) &&
           !table.cols[i].isArray)return false; // Every genuine OID key is nonempty.
    const auto value = integerEqualityValue(table, condition.colName, condition.value);
    return isSingleColumnPrimaryKey(table, condition.colName)
        ? table.buildPKValue({{condition.colName, value}}).empty()
        : table.columnIndexKey(condition.colName, value).empty();
}

static bool lookupBtreeKeyChecked(BPTree* index, const std::string& key,
                                  int64_t& rid) {
    const auto status = index->searchChecked(key, rid);
    if (status == BPTree::SearchResult::Error) {
        throw DbError("XX001", "invalid B-tree traversal in index \"" +
            index->filePath().filename().string() + "\"");
    }
    return status == BPTree::SearchResult::Found;
}

static std::vector<int64_t> lookupBtreeMultiChecked(
        BPTree* index, const std::string& key) {
    std::vector<int64_t> rids;
    if (!index->searchMultiChecked(key, rids)) {
        throw DbError("XX001", "invalid B-tree traversal in index \"" +
            index->filePath().filename().string() + "\"");
    }
    return rids;
}

IndexScanOp::IndexScanOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename, const std::string& colname,
                          const std::string& value,
                         std::optional<uint32_t> referenceInputOid)
    : engine_(engine), dbname_(dbname), tablename_(tablename),
      colname_(colname), value_(value), referenceInputOid_(referenceInputOid) {}

bool IndexScanOp::open() {
    usedIndex_ = false;
    referenceEqualityOid_.reset();
    struct ReferenceInputGuard {
        std::optional<uint32_t>& value;
        bool keep=false;
        ~ReferenceInputGuard(){if(!keep)value.reset();}
    } referenceGuard{referenceEqualityOid_};
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    for(size_t i=0;i<tbl_.len();++i)
        if(tbl_.cols[i].dataName==colname_ && !tbl_.cols[i].isArray &&
           stored_regtype_detail::scalarType(tbl_.cols[i].dataType)) {
            try {
                if(referenceInputOid_)referenceEqualityOid_=referenceInputOid_;
                else {
                    const auto catalog=regtype_detail::metadata(*engine_,dbname_);
                    referenceEqualityOid_=stored_regtype_detail::inputOid(value_,catalog,currentSession(),tbl_.cols[i].dataType);
                }
            } catch(...) {
                // Only this newly introduced conversion owns this cleanup.
                // Preserve the input exception and the old index lifecycle.
                const auto primary=std::current_exception();
                referenceEqualityOid_.reset();
                if(tableLockHeld_)try {
                    lockManager.unlock(tablename_);
                    tableLockHeld_=false;
                } catch(...) {}
                std::rethrow_exception(primary);
            }
            break;
        }
    rids_.clear();
    lastRid_ = 0;
    // Check if this is a PK scan
    size_t pkIdx = tbl_.len();
    for (size_t i = 0; i < tbl_.len(); ++i) {
        if (tbl_.cols[i].isPrimaryKey && tbl_.cols[i].dataName == colname_) {
            pkIdx = i; break;
        }
    }
    isPK_ = isSingleColumnPrimaryKey(tbl_, colname_);
    if (pkIdx < tbl_.len() && !isPK_) {
        const auto indexedColumns = engine_->getIndexedColumns(dbname_, tablename_);
        if (std::find(indexedColumns.begin(), indexedColumns.end(), colname_) ==
            indexedColumns.end()) {
            throw DbError("0A000", "single-column index scan cannot use a "
                "composite primary key");
        }
    }

    const std::string key = isPK_
        ? tbl_.buildPKValue({{colname_, (referenceEqualityOid_ ? std::to_string(*referenceEqualityOid_) :
            integerEqualityValue(tbl_, colname_, value_))}})
        : tbl_.columnIndexKey(colname_, (referenceEqualityOid_ ? std::to_string(*referenceEqualityOid_) :
            integerEqualityValue(tbl_, colname_, value_)));
    const auto heapFallback=[&]() {
        // Historical build/write paths omit empty keys. Direct callers need
        // the same complete heap fallback as the planner, with next() still
        // doing the typed equality and NULL recheck.
        const auto append = [&](uint32_t page, uint16_t slot, const char*, size_t) {
            rids_.push_back(StorageEngine::encodeRid(page, slot));
        };
        const bool ok = engine_->rlsAppliesTo(dbname_, tablename_)
            ? engine_->forEachVisibleRow(dbname_, tablename_, "SELECT", append)
            : engine_->forEachRow(dbname_, tablename_, append);
        if (!ok) {
            rids_.clear();
            setError(key.empty()?"empty-key heap fallback failed":"snapshot heap fallback failed");
            return false;
        }
        pos_ = 0;
        if (!statsRecorded_) {
            recordTableScan(dbname_, tablename_, rids_.size(), false, true);
            statsRecorded_ = true;
        }
        referenceGuard.keep=true;
        return true;
    };
    engine_->noteQuerySnapshot();
    const auto beforeFence=engine_->indexReadSnapshotFence(dbname_);
    if(key.empty())return heapFallback();
    BPTree* idx = isPK_
        ? engine_->getPKIndex(dbname_, tablename_)
        : engine_->getSecondaryIndex(dbname_, tablename_, colname_);
    if (!idx || !idx->isOpen()) {
        throw DbError("XX001", "could not open B-tree index for relation \"" +
            tablename_ + "\"");
    }
    if(!beforeFence)return heapFallback();
    if (isPK_ && primary_key_index::lookupNeedsHeap(tbl_, *idx, key)) return heapFallback();
    if (isPK_ && !primary_key_index::sharesPrefix(tbl_, key)) {
        int64_t rid = 0;
        if (lookupBtreeKeyChecked(idx, key, rid)) rids_.push_back(rid);
    } else {
        rids_ = lookupBtreeMultiChecked(idx, key);
    }
    const auto afterFence=engine_->indexReadSnapshotFence(dbname_);
    if(!afterFence || *afterFence!=*beforeFence) {
        rids_.clear();
        return heapFallback();
    }
    pos_ = 0;
    usedIndex_ = true;
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, rids_.size(), true, false);
        statsRecorded_ = true;
    }
    referenceGuard.keep=true;
    return true;
}

bool IndexScanOp::next(std::string& outRow) {
    const int priorExceptions=std::uncaught_exceptions();
    struct ReferenceErrorGuard {
        std::optional<uint32_t>& value; int prior;
        ~ReferenceErrorGuard(){if(std::uncaught_exceptions()>prior)value.reset();}
    } referenceGuard{referenceEqualityOid_,priorExceptions};
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    // Fetch indexed tuples directly. Scanning every heap tuple for each RID
    // turns an index lookup into O(index_matches * table_rows), which is
    // unacceptable for production workloads.
    while (pos_ < rids_.size()) {
        const int64_t rid = rids_[pos_++];
        std::string row;
        bool readFailed = false;
        if (!engine_->readIndexedRowByRid(dbname_, tablename_, rid, row, tbl_, nullptr,
                                          &readFailed)) {
            if (readFailed) {
                setError("indexed heap fetch failed");
                referenceEqualityOid_.reset();
                return false;
            }
            continue;
        }
        bool toastOk = false;
        outRow = engine_->resolveToastValues(
            dbname_, tablename_, row, tbl_, &toastOk);
        if (!toastOk) {
            setError("TOAST value read failed");
            referenceEqualityOid_.reset();
                return false;
        }
        lastRid_ = rid;
        StorageEngine::bindNullRow(
            engine_, dbname_, tablename_, lastRid_, tbl_.len());
        // Fixed-width B-tree keys can share a truncated prefix. The heap
        // tuple is only a candidate until the complete SQL value matches.
        // Keep the recheck inside IndexScan: the planner consumes its indexed
        // equality condition and does not retain a Filter above this node.
        size_t columnIndex = tbl_.len();
        for (size_t i = 0; i < tbl_.len(); ++i)
            if (tbl_.cols[i].dataName == colname_) { columnIndex = i; break; }
        if (columnIndex == tbl_.len()) {
            setError("indexed column does not exist");
            referenceEqualityOid_.reset();
                return false;
        }
        bool valueIsNull = false;
        const std::string actual = engine_->extractColumnValue(
            outRow, tbl_, columnIndex, dbname_, true, &valueIsNull);
        valueIsNull = valueIsNull ||
            (tbl_.cols[columnIndex].generatedKind != 'v' &&
             detail::storedRowColumnIsNull(*engine_, dbname_, tablename_, tbl_, rid, columnIndex));
        if (StorageEngine::compareValues(tbl_.cols[columnIndex], actual,
                valueIsNull, (referenceEqualityOid_ ? std::to_string(*referenceEqualityOid_) :
            integerEqualityValue(tbl_, colname_, value_)),
                false, "=") != StorageEngine::PredicateTruth::True)
            continue;
        rtInstr_.emitted = true;
        return true;
    }
    referenceEqualityOid_.reset();
    return false;
}

bool IndexScanOp::lastColumnIsNull(size_t colIdx) const {
    return lastRid_ > 0 && colIdx < tbl_.len() &&
        detail::storedRowColumnIsNull(*engine_, dbname_, tablename_, tbl_, lastRid_, colIdx);
}

void IndexScanOp::close() {
    referenceEqualityOid_.reset();
    rids_.clear();
    lastRid_ = 0;
    StorageEngine::unbindNullRow();
    statsRecorded_ = false;
    if (tableLockHeld_) {
        engine_->getLockManager().unlock(tablename_);
        tableLockHeld_ = false;
    }
}

static bool collectEqualityIndexCandidates(
    StorageEngine* engine, const std::string& dbname,
    const std::string& tablename, const TableSchema& tbl,
    const std::vector<std::string>& hashIndexedColumns,
    const StorageEngine::Condition& condition,
    std::set<int64_t>& candidates) {
    if (condition.op != "=" || equalityIndexKeyIsEmpty(tbl, condition)) return false;
    std::string value; bool reference=false;
    for(size_t i=0;i<tbl.len();++i)
        if(tbl.cols[i].dataName==condition.colName && !tbl.cols[i].isArray &&
           stored_regtype_detail::scalarType(tbl.cols[i].dataType))reference=true;
    if(reference && condition.referenceInputOid)value=std::to_string(*condition.referenceInputOid);
    else value = integerEqualityValue(
        tbl, condition.colName, condition.value, engine, dbname);

    if (isSingleColumnPrimaryKey(tbl, condition.colName)) {
        auto* index = engine->getPKIndex(dbname, tablename);
        if (!index) {
            throw DbError("XX001", "could not open B-tree index for relation \"" +
                tablename + "\"");
        }
        int64_t rid = 0;
        const std::string key = tbl.buildPKValue(
            std::map<std::string, std::string>{{condition.colName, value}});
        if (primary_key_index::lookupNeedsHeap(tbl, *index, key)) return false;
        if (primary_key_index::sharesPrefix(tbl, key)) {
            const auto matches = lookupBtreeMultiChecked(index, key);
            candidates.insert(matches.begin(), matches.end());
        } else if (lookupBtreeKeyChecked(index, key, rid)) {
            candidates.insert(rid);
        }
        return true;
    }

    // A primary key encodes the logical value exactly once above. Secondary
    // AMs store type-specific column keys (including money's biased hex),
    // not the original SQL spelling or display text.
    const std::string indexValue = tbl.columnIndexKey(condition.colName, value);

    if (std::find(hashIndexedColumns.begin(), hashIndexedColumns.end(),
                  condition.colName) != hashIndexedColumns.end()) {
        auto* hash = engine->getHashIndex(dbname, tablename, condition.colName);
        std::error_code fileError;
        // Hash/Bloom's legacy open() treats absence as an empty creation
        // mapping. Here metadata already declares a live index: absence (or
        // failed loading) is not an empty candidate set and must not make a
        // Bitmap/DNF branch silently discard matching heap rows.
        if (!hash || !hash->isOpen() ||
            !std::filesystem::is_regular_file(hash->filePath(), fileError) ||
            fileError) {
            throw DbError("XX001", "could not read Hash index for relation \"" +
                tablename + "\"");
        }
        for (int64_t rid : hash->search(indexValue)) candidates.insert(rid);
        return true;
    }

    const auto bloomIndexedColumns = engine->getBloomIndexedColumns(dbname, tablename);
    if (std::find(bloomIndexedColumns.begin(), bloomIndexedColumns.end(),
                  condition.colName) != bloomIndexedColumns.end()) {
        auto* bloom = engine->getBloomIndex(dbname, tablename, condition.colName);
        std::error_code fileError;
        if (!bloom || !bloom->isOpen() ||
            !std::filesystem::is_regular_file(bloom->filePath(), fileError) ||
            fileError) {
            throw DbError("XX001", "could not read Bloom index for relation \"" +
                tablename + "\"");
        }
        for (int64_t rid : bloom->search(indexValue)) candidates.insert(rid);
        return true;
    }

    auto* index = engine->getSecondaryIndex(dbname, tablename, condition.colName);
    if (!index) {
        const auto indexedColumns = engine->getIndexedColumns(dbname, tablename);
        if (std::find(indexedColumns.begin(), indexedColumns.end(),
                      condition.colName) != indexedColumns.end()) {
            throw DbError("XX001", "could not open B-tree index for relation \"" +
                tablename + "\"");
        }
        return false;
    }
    for (int64_t rid : lookupBtreeMultiChecked(index, indexValue))
        candidates.insert(rid);
    return true;
}

// ========================================================================
// BitmapHeapScanOp
// ========================================================================

static bool collectVisibleHeapCandidates(StorageEngine* engine,
    const std::string& dbname, const std::string& tablename,
    std::vector<int64_t>& rids) {
    rids.clear();
    const auto append = [&](uint32_t page, uint16_t slot, const char*, size_t) {
        rids.push_back(StorageEngine::encodeRid(page, slot));
    };
    const bool ok = engine->rlsAppliesTo(dbname, tablename)
        ? engine->forEachVisibleRow(dbname, tablename, "SELECT", append)
        : engine->forEachRow(dbname, tablename, append);
    if (!ok) rids.clear();
    return ok;
}

BitmapHeapScanOp::BitmapHeapScanOp(
    StorageEngine* engine, const std::string& dbname,
    const std::string& tablename,
    const std::vector<StorageEngine::Condition>& conds)
    : engine_(engine), dbname_(dbname), tablename_(tablename), conds_(conds) {}

bool BitmapHeapScanOp::open() {
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rids_.clear();
    rows_.clear();
    pos_ = 0;
    lastRid_ = 0;

    engine_->noteQuerySnapshot();
    const auto beforeFence = engine_->indexReadSnapshotFence(dbname_);
    std::set<std::string> indexedColumns;
    std::set<int64_t> matched;
    bool initialized = false;
    size_t indexedPredicates = 0;
    const auto hashIndexedColumns = engine_->getHashIndexedColumns(dbname_, tablename_);

    for (const auto& condition : conds_) {
        if (condition.op != "=" || !indexedColumns.insert(condition.colName).second)
            continue;

        std::set<int64_t> candidates;
        if (!collectEqualityIndexCandidates(engine_, dbname_, tablename_, tbl_,
                                            hashIndexedColumns, condition, candidates))
            continue;
        ++indexedPredicates;

        if (!initialized) {
            matched = std::move(candidates);
            initialized = true;
        } else {
            std::set<int64_t> intersection;
            std::set_intersection(matched.begin(), matched.end(),
                                  candidates.begin(), candidates.end(),
                                  std::inserter(intersection, intersection.end()));
            matched = std::move(intersection);
        }
    }

    // The planner only builds this node for two or more usable indexes.  A
    // false return is a defensive guard for direct callers, not a fallback
    // mechanism inside an already-open plan.
    if (indexedPredicates < 2 || !initialized) return false;
    rids_.assign(matched.begin(), matched.end());
    const auto afterFence = engine_->indexReadSnapshotFence(dbname_);
    const bool usedIndex = beforeFence && afterFence && *beforeFence == *afterFence;
    // Sidecars only contain current keys. A historical snapshot, own writes,
    // or a writer racing the probes needs the complete visible heap instead.
    // The Filter above this node retains every original predicate.
    if (!usedIndex && !collectVisibleHeapCandidates(engine_, dbname_, tablename_, rids_)) {
        setError("snapshot bitmap heap fallback failed");
        return false;
    }
    const size_t heapCandidates = rids_.size();

    bool readFailed = false;
    std::vector<int64_t> visibleRids;
    visibleRids.reserve(rids_.size());
    for (int64_t rid : rids_) {
        std::string row;
        if (!engine_->readIndexedRowByRid(dbname_, tablename_, rid, row, tbl_, nullptr,
                                          &readFailed)) {
            if (readFailed) {
                rows_.clear();
                setError("bitmap heap fetch failed");
                return false;
            }
            continue;
        }
        bool toastOk = false;
        row = engine_->resolveToastValues(
            dbname_, tablename_, row, tbl_, &toastOk);
        if (!toastOk) {
            rows_.clear();
            setError("TOAST value read failed");
            return false;
        }
        rows_.push_back(std::move(row));
        visibleRids.push_back(rid);
    }
    rids_ = std::move(visibleRids);
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, usedIndex ? rows_.size() : heapCandidates,
                        usedIndex, !usedIndex);
        statsRecorded_ = true;
    }
    return true;
}

bool BitmapHeapScanOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    lastRid_ = rids_[pos_];
    outRow = rows_[pos_++];
    StorageEngine::bindNullRow(
        engine_, dbname_, tablename_, lastRid_, tbl_.len());
    rtInstr_.emitted = true;
    return true;
}

bool BitmapHeapScanOp::lastColumnIsNull(size_t colIdx) const {
    return lastRid_ > 0 && colIdx < tbl_.len() &&
        engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, colIdx);
}

void BitmapHeapScanOp::close() {
    rids_.clear();
    rows_.clear();
    pos_ = 0;
    lastRid_ = 0;
    StorageEngine::unbindNullRow();
    statsRecorded_ = false;
    if (tableLockHeld_) {
        engine_->getLockManager().unlock(tablename_);
        tableLockHeld_ = false;
    }
}

// ========================================================================
// GiSTScanOp
// ========================================================================

static std::string gistScanSchemaShape(const TableSchema& schema) {
    std::string out;
    const auto field = [&](const std::string& text) { out += std::to_string(text.size())+":"+text; };
    field(std::to_string(schema.physicalRelationId)); field(std::to_string(schema.formatVersion));
    field(std::to_string(schema.len()));
    for (size_t i = 0; i < schema.len(); ++i) {
        const auto& c = schema.cols[i];
        for (const auto& text : {c.dataName,c.dataType,c.domainName,c.collation,c.resolvedCollation,c.generatedExpr}) field(text);
        for (const auto n : {int64_t(c.dsize),int64_t(c.typeMod),int64_t(c.storageCodec),int64_t(c.isArray),int64_t(c.isNull),int64_t(c.generatedKind),int64_t(c.isUnsigned)}) field(std::to_string(n));
        field(std::to_string(c.enumValues.size()));
        field(std::to_string(c.resolvedCollationIsBinary)); field(std::to_string(c.resolvedCollationUsesLocale));
        for (const auto& value : c.enumValues) field(value);
    }
    return out;
}

GiSTScanOp::GiSTScanOp(StorageEngine* engine, const std::string& dbname,
                       const std::string& tablename,
                       const std::vector<StorageEngine::Condition>& conds,
                       std::optional<TableSchema> plannedSchema, OpPtr fallback)
    : engine_(engine), dbname_(dbname), tablename_(tablename),
      plannedShape_(plannedSchema ? gistScanSchemaShape(*plannedSchema) : std::string{}),
      fallback_(std::move(fallback)), conds_(conds) {
    if (!fallback_) fallback_ = std::make_unique<TableScanOp>(engine, dbname, tablename);
}

std::string GiSTScanOp::describeConds() const {
    std::string out;
    for (const auto& c : conds_) {
        if (!out.empty()) out += " AND ";
        out += c.colName + c.op + c.value;
    }
    return out;
}

bool GiSTScanOp::open() {
    if (opened_) close();
    OpenInstrument instrumentation(this);
    clearError();
    opened_ = true;
    fallbackOpen_ = false;
    usedIndex_ = false;
    statsRecorded_ = false;
    rids_.clear(); rows_.clear(); pos_ = 0; lastRid_ = 0;
    auto& locks = engine_->getLockManager();
    locks.setResourceNamespace(dbname_);
    if (!locks.lockShared(tablename_))
        throw DbError("55P03", "could not obtain lock on relation \"" + tablename_ + "\"");
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    engine_->noteQuerySnapshot();
    const auto openHeap = [&]() {
        fallbackOpen_ = true;
        return fallback_->open() || propagateChildError(fallback_.get(), "GiST heap fallback failed");
    };
    if ((!plannedShape_.empty() && plannedShape_ != gistScanSchemaShape(tbl_)) ||
        tbl_.partitionType != TableSchema::PartitionType::None ||
        engine_->rlsAppliesTo(dbname_, tablename_) ||
        !engine_->getInheritedChildren(dbname_, tablename_).empty()) return openHeap();
    const auto beforeFence = engine_->indexReadSnapshotFence(dbname_);
    if (!beforeFence) return openHeap();
    const auto gistColumns = engine_->getGiSTIndexedColumns(dbname_, tablename_);
    const auto isGist = [&](const std::string& column) {
        return std::find(gistColumns.begin(), gistColumns.end(), column) != gistColumns.end();
    };
    const auto literalRhs = [&](const StorageEngine::Condition& condition) {
        return condition.decodedLiteralRhs || !std::any_of(tbl_.cols.begin(), tbl_.cols.end(),
            [&](const auto& column) { return column.dataName == condition.value; });
    };
    StorageEngine::GiSTBounds bounds;
    std::string column;
    std::optional<std::string> likePrefix;
    for (const auto& condition : conds_) {
        const auto& op = condition.op;
        if ((op != "=" && op != "<" && op != "<=" && op != ">" && op != ">=") ||
            !isGist(condition.colName) || !literalRhs(condition)) continue;
        if (column.empty()) column = condition.colName;
        if (condition.colName != column) continue;
        // Presence, strictness and datum bytes remain separate. Other
        // columns and any further conjuncts stay with the original filter.
        if ((op == "=" || op == ">" || op == ">=") && !bounds.low)
            bounds.low = StorageEngine::GiSTBound{condition.value, op != ">"};
        if ((op == "=" || op == "<" || op == "<=") && !bounds.high)
            bounds.high = StorageEngine::GiSTBound{condition.value, op != "<"};
    }
    if (column.empty()) {
        for (const auto& condition : conds_) {
            if (!isGist(condition.colName)) continue;
            const auto prefix = nativeLikePrefix(condition, tbl_);
            if (!prefix) continue;
            column = condition.colName;
            likePrefix = *prefix;
            break;
        }
    }
    if (column.empty()) return openHeap();
    auto candidates = likePrefix
        ? engine_->tryGiSTSearchLikePrefix(dbname_, tablename_, column, *likePrefix)
        : engine_->tryGiSTSearchOverlap(dbname_, tablename_, column, bounds);
    if (!candidates || engine_->indexReadSnapshotFence(dbname_) != beforeFence) return openHeap();
    usedIndex_ = true;
    rids_ = std::move(*candidates);
    bool readFailed = false;
    std::vector<int64_t> visible;
    visible.reserve(rids_.size());
    for (const auto rid : rids_) {
        std::string row;
        if (!engine_->readIndexedRowByRid(dbname_, tablename_, rid, row, tbl_, nullptr, &readFailed)) {
            if (readFailed) { rows_.clear(); setError("GiST heap fetch failed"); return false; }
            continue;
        }
        bool toastOk = false;
        row = engine_->resolveToastValues(dbname_, tablename_, row, tbl_, &toastOk);
        if (!toastOk) { rows_.clear(); setError("TOAST value read failed"); return false; }
        rows_.push_back(std::move(row)); visible.push_back(rid);
    }
    rids_ = std::move(visible);
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, rows_.size(), true, false);
        statsRecorded_ = true;
    }
    return true;
}

bool GiSTScanOp::next(std::string& outRow) {
    NextInstrument instrumentation(this);
    if (fallbackOpen_) {
        if (!fallback_->next(outRow)) {
            if (fallback_->hasError()) return propagateChildError(fallback_.get(), "GiST heap fallback failed");
            return false;
        }
    } else {
        if (pos_ >= rows_.size()) return false;
        lastRid_ = rids_[pos_]; outRow = rows_[pos_++];
        StorageEngine::bindNullRow(engine_, dbname_, tablename_, lastRid_, tbl_.len());
    }
    instrumentation.emitted = true;
    return true;
}

bool GiSTScanOp::lastColumnIsNull(size_t ordinal) const {
    if (fallbackOpen_) return fallback_->lastColumnIsNull(ordinal);
    return lastRid_ > 0 && ordinal < tbl_.len() &&
        engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, ordinal);
}

bool GiSTScanOp::lastStoredNullBitmap(std::vector<bool>& nulls) const {
    return fallbackOpen_ && fallback_->lastStoredNullBitmap(nulls);
}

void GiSTScanOp::close() {
    if (!opened_) return;
    opened_ = false;
    std::exception_ptr error;
    try { if (fallbackOpen_) fallback_->close(); } catch (...) { error = std::current_exception(); }
    fallbackOpen_ = false;
    rids_.clear(); rows_.clear(); pos_ = 0; lastRid_ = 0;
    StorageEngine::unbindNullRow(); statsRecorded_ = false;
    if (tableLockHeld_) {
        tableLockHeld_ = false;
        engine_->getLockManager().setResourceNamespace(dbname_);
        engine_->getLockManager().unlock(tablename_);
    }
    if (error) std::rethrow_exception(error);
}

// ========================================================================
// BitmapOrHeapScanOp
// ========================================================================

BitmapOrHeapScanOp::BitmapOrHeapScanOp(
    StorageEngine* engine, const std::string& dbname,
    const std::string& tablename,
    const std::vector<std::vector<StorageEngine::Condition>>& branches)
    : engine_(engine), dbname_(dbname), tablename_(tablename), branches_(branches) {}

bool BitmapOrHeapScanOp::open() {
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rows_.clear();
    rids_.clear();
    pos_ = 0;
    lastRid_ = 0;
    if (branches_.size() < 2) return false;

    engine_->noteQuerySnapshot();
    const auto beforeFence = engine_->indexReadSnapshotFence(dbname_);
    const auto hashIndexedColumns = engine_->getHashIndexedColumns(dbname_, tablename_);
    std::set<int64_t> unionRids;
    for (const auto& branch : branches_) {
        std::set<std::string> indexedColumns;
        std::set<int64_t> matched;
        bool initialized = false;
        size_t indexedPredicates = 0;

        for (const auto& condition : branch) {
            if (!indexedColumns.insert(condition.colName).second) continue;
            std::set<int64_t> candidates;
            if (!collectEqualityIndexCandidates(engine_, dbname_, tablename_, tbl_,
                                                hashIndexedColumns, condition, candidates))
                continue;
            ++indexedPredicates;
            if (!initialized) {
                matched = std::move(candidates);
                initialized = true;
            } else {
                std::set<int64_t> intersection;
                std::set_intersection(matched.begin(), matched.end(),
                                      candidates.begin(), candidates.end(),
                                      std::inserter(intersection, intersection.end()));
                matched = std::move(intersection);
            }
        }
        // A branch without an equality index cannot safely be represented by
        // this node; the planner must choose the normal DNF/legacy path.
        if (indexedPredicates == 0 || !initialized) return false;
        unionRids.insert(matched.begin(), matched.end());
    }

    const auto afterFence = engine_->indexReadSnapshotFence(dbname_);
    const bool usedIndex = beforeFence && afterFence && *beforeFence == *afterFence;
    if (!usedIndex) {
        std::vector<int64_t> visible;
        if (!collectVisibleHeapCandidates(engine_, dbname_, tablename_, visible)) {
            setError("snapshot bitmap heap fallback failed");
            return false;
        }
        unionRids = std::set<int64_t>(visible.begin(), visible.end());
    }
    const size_t heapCandidates = unionRids.size();
    bool readFailed = false;
    for (int64_t rid : unionRids) {
        std::string row;
        if (!engine_->readIndexedRowByRid(dbname_, tablename_, rid, row, tbl_, nullptr,
                                          &readFailed)) {
            if (readFailed) {
                rows_.clear();
                setError("bitmap heap fetch failed");
                return false;
            }
            continue;
        }
        bool toastOk = false;
        row = engine_->resolveToastValues(
            dbname_, tablename_, row, tbl_, &toastOk);
        if (!toastOk) {
            rows_.clear();
            setError("TOAST value read failed");
            return false;
        }

        struct PredicateNullBinding {
            StorageEngine::NullRowBindingState saved;
            ~PredicateNullBinding() {
                StorageEngine::restoreNullRowBinding(std::move(saved));
            }
        } nullBinding{StorageEngine::captureNullRowBinding()};
        StorageEngine::bindNullRow(engine_, dbname_, tablename_, rid, tbl_.len());
        bool matches = false;
        for (const auto& branch : branches_) {
            bool branchMatches = true;
            for (const auto& condition : branch) {
                if (!StorageEngine::evalConditionOnRow(condition, row, tbl_)) {
                    branchMatches = false;
                    break;
                }
            }
            if (branchMatches) {
                matches = true;
                break;
            }
        }
        if (matches) {
            rows_.push_back(std::move(row));
            rids_.push_back(rid);
        }
    }
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, usedIndex ? rows_.size() : heapCandidates,
                        usedIndex, !usedIndex);
        statsRecorded_ = true;
    }
    return true;
}

bool BitmapOrHeapScanOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_];
    lastRid_ = (pos_ < rids_.size()) ? rids_[pos_] : 0;
    ++pos_;
    if (lastRid_ > 0) {
        StorageEngine::bindNullRow(
            engine_, dbname_, tablename_, lastRid_, tbl_.len());
    }
    rtInstr_.emitted = true;
    return true;
}

bool BitmapOrHeapScanOp::lastColumnIsNull(size_t colIdx) const {
    if (lastRid_ <= 0 || colIdx >= tbl_.len()) return false;
    return engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, colIdx);
}

void BitmapOrHeapScanOp::close() {
    rows_.clear();
    rids_.clear();
    pos_ = 0;
    lastRid_ = 0;
    StorageEngine::unbindNullRow();
    statsRecorded_ = false;
    if (tableLockHeld_) {
        engine_->getLockManager().unlock(tablename_);
        tableLockHeld_ = false;
    }
}

// ========================================================================
// FilterOp
// ========================================================================

static std::vector<StorageEngine::Condition> prepareUnknownBitPredicateInputs(
        const TableSchema& table, const std::vector<StorageEngine::Condition>& inputs) {
    auto conditions = inputs;
    for (auto& condition : conditions) {
        if (condition.patternType != "unknown" || !condition.decodedLiteralRhs ||
            !(condition.op == "=" || condition.op == "<>" || condition.op == "!=" ||
              condition.op == "<" || condition.op == ">" || condition.op == "<=" || condition.op == ">=")) continue;
        for (size_t i = 0; i < table.len(); ++i) {
            const auto& column = table.cols[i];
            if (column.dataName != condition.colName) continue;
            if (column.isArray || (column.dataType != "bit" && column.dataType != "bit varying")) break;
            const auto binding = ExprEvaluator::resolveComparison(condition.op, column.dataType, "unknown");
            ExprEvaluator evaluator;
            const auto input = evaluator.coerceComparison(binding, ExprValue("unknown", condition.value, condition.patternIsNull), false);
            condition.value = input.value;
            condition.patternType = input.typeName;
            break;
        }
    }
    return conditions;
}

static bool nullScalarPredicate(const StorageEngine::Condition& condition) {
    return condition.patternIsNull && (condition.op=="typedexpr" || condition.op=="=" || condition.op=="<>" || condition.op=="!=" ||
        condition.op=="<" || condition.op==">" || condition.op=="<=" || condition.op==">=");
}

static bool nullPredicateBranch(const std::vector<StorageEngine::Condition>& conditions) {
    return std::any_of(conditions.begin(),conditions.end(),nullScalarPredicate);
}

static void validateNullPredicateColumns(const Expr* expression,const TableSchema& table) {
    const auto* binary=dynamic_cast<const BinaryOpExpr*>(expression);
    if(!binary) return;
    if(binary->op=="AND" || binary->op=="OR") {
        validateNullPredicateColumns(binary->left.get(),table);validateNullPredicateColumns(binary->right.get(),table);return;
    }
    if(binary->op!="=" && binary->op!="<>" && binary->op!="!=" && binary->op!="<" &&
       binary->op!=">" && binary->op!="<=" && binary->op!=">=") return;
    const auto isNull=[](const Expr* value) {
        const auto* literal=dynamic_cast<const LiteralExpr*>(value);
        return literal && literal->typeName.empty() && !literal->preparedSubquery && literal->value=="null";
    };
    const auto* reference=isNull(binary->right.get())?dynamic_cast<const ColumnRefExpr*>(binary->left.get()):
        isNull(binary->left.get())?dynamic_cast<const ColumnRefExpr*>(binary->right.get()):nullptr;
    if(!reference || !reference->table.empty() || !reference->schema.empty()) return;
    for(size_t i=0;i<table.len();++i) if(table.cols[i].dataName==reference->column) return;
    throw DbError("42703","column does not exist: "+reference->column);
}

static void validatePredicateTypes(const TableSchema& table,
        const std::vector<StorageEngine::Condition>& conditions,
        const std::string& database = {}, StorageEngine* owner = nullptr) {
    for (const auto& condition : conditions) {
        if (owner && (condition.op == "=" || condition.op == "<" || condition.op == "<=" ||
                      condition.op == ">" || condition.op == ">=") && condition.decodedLiteralRhs &&
            condition.patternType == "unknown" && !condition.patternIsNull) {
            for (size_t i = 0; i < table.len(); ++i) {
                if (table.cols[i].dataName == condition.colName)
                    validateStoredZonedClockComparisonInput(table.cols[i], condition.value);
            }
        }
        if (nullScalarPredicate(condition) && condition.op!="typedexpr") {
            bool found=false;
            for(size_t i=0;i<table.len();++i) if(table.cols[i].dataName==condition.colName) {
                found=true;
                (void)ExprEvaluator::resolveComparison(condition.op,table.cols[i].dataType+
                    (table.cols[i].isArray?"[]":""),"unknown");
                break;
            }
            if(!found) throw DbError("42703","column does not exist: "+condition.colName);
        }
        if (condition.op == "typedexpr") {
            // Only the planner owns database/routine metadata. A filter's
            // schema alone must not guess an already prepared UDF as TEXT.
            if (!owner) continue;
            SQLParser parser;
            auto parsed = parser.parse("SELECT " + condition.value);
            auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
            if (!select || select->selectList.size() != 1 || select->fromClause)
                throw DbError("42601", "invalid typed filter predicate");
            validateNullPredicateColumns(select->selectList.front().expr.get(),table);
            std::map<std::string, std::string> hints;
            for (size_t i = 0; i < table.len(); ++i)
                hints[table.cols[i].dataName] = table.cols[i].dataType +
                    (table.cols[i].isArray ? "[]" : "");
            ExprHelper::prepareArrayTypes(select->selectList.front().expr.get(),
                hints, database, owner);
            continue;
        }
        if (condition.patternType != "bit") continue;
        static const std::set<std::string> comparisons = {
            "=", "<>", "!=", "<", ">", "<=", ">=", "in", "notin", "between", "notbetween"};
        if (!comparisons.count(condition.op)) continue;
        for (size_t i = 0; i < table.len(); ++i) {
            if (table.cols[i].dataName != condition.colName) continue;
            const auto& column = table.cols[i];
            (void)ExprEvaluator::resolveComparison("=", column.dataType +
                (column.isArray ? "[]" : ""), "bit");
            if (condition.op=="in" || condition.op=="notin" ||
                condition.op=="between" || condition.op=="notbetween") {
                const auto tokens=SQLParser::tokenize(condition.value);
                const bool range=condition.op=="between" || condition.op=="notbetween";
                if(range && tokens.size()!=2)throw DbError("42601","BIT BETWEEN requires two operands");
                std::map<std::string,std::string> hints;
                for(size_t c=0;c<table.len();++c)
                    hints[table.cols[c].dataName]=table.cols[c].dataType+(table.cols[c].isArray?"[]":"");
                ExprEvaluator evaluator;
                for(size_t member=0;member<tokens.size();++member) {
                    SQLParser parser;auto parsed=parser.parse("SELECT "+tokens[member]);
                    const auto* select=parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
                    if(!select || select->selectList.size()!=1 || select->fromClause)
                        throw DbError("42601","invalid compact BIT predicate operand");
                    const auto* operand=select->selectList.front().expr.get();
                    if(const auto* reference=dynamic_cast<const ColumnRefExpr*>(operand);
                       reference && reference->table.empty() && !hints.count(reference->column))
                        throw DbError("42703","compact predicate column does not exist: "+reference->column);
                    const auto type=ExprHelper::inferParsedInputType(operand,hints);
                    const auto binding=ExprEvaluator::resolveComparison(range ? (member==0?">=":"<=") : "=",
                        column.dataType+(column.isArray?"[]":""),type);
                    if(const auto* literal=dynamic_cast<const LiteralExpr*>(operand);
                       literal && !literal->preparedSubquery) {
                        auto value=evaluator.eval(operand,{});value.typeName=type;
                        (void)evaluator.coerceComparison(binding,value,false);
                    }
                }
            }
            break;
        }
    }
}

FilterOp::FilterOp(OpPtr child, const TableSchema& tbl,
                    const std::vector<StorageEngine::Condition>& conds)
    : child_(std::move(child)), tbl_(tbl), conds_(prepareUnknownBitPredicateInputs(tbl, conds)) {}

FilterOp::FilterOp(
    OpPtr child, const TableSchema& tbl,
    const std::vector<std::vector<StorageEngine::Condition>>& branches)
    : child_(std::move(child)), tbl_(tbl) {
    for (const auto& branch : branches)
        branches_.push_back(prepareUnknownBitPredicateInputs(tbl, branch));
}

bool FilterOp::open() {
    validatePredicateTypes(tbl_, conds_);
    for (auto& condition : conds_) StorageEngine::bindStoredComparison(condition, tbl_);
    for (auto& branch : branches_) {
        validatePredicateTypes(tbl_, branch);
        for (auto& condition : branch) StorageEngine::bindStoredComparison(condition, tbl_);
    }
    return child_->open();
}

bool FilterOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    const auto* scan = dynamic_cast<const TableScanOp*>(child_.get());
    const bool sameTable = scan && scan->tableName() == tbl_.tablename;
    std::vector<bool> storedNulls;
    const auto matchesBranch = [&](
        const std::vector<StorageEngine::Condition>& conditions) {
        if (nullPredicateBranch(conditions)) return false;
        for (const auto& c : conditions) {
            using Kind = StorageEngine::Condition::ColumnComparison::Kind;
            const bool boundComparison = c.literalComparisons ||
                (c.columnComparison && c.columnComparison->kind != Kind::Unbound);
            if (sameTable && boundComparison &&
                scan->lastStoredNullBitmap(storedNulls) && storedNulls.size() == tbl_.len()) {
                if (!StorageEngine::evalConditionOnRow(c, outRow, tbl_, storedNulls)) return false;
                continue;
            }
            size_t colIdx = tbl_.len();
            for (size_t i = 0; i < tbl_.len(); ++i) {
                if (tbl_.cols[i].dataName == c.colName) {
                    colIdx = i;
                    break;
                }
            }
            const bool physicalColumn = colIdx < tbl_.len() &&
                tbl_.cols[colIdx].generatedKind != 'v';
            const bool isNull = physicalColumn && child_->lastColumnIsNull(colIdx);
            const bool conditionMatches =
                (physicalColumn && c.op == "isnull") ? isNull :
                (physicalColumn && c.op == "isnotnull") ? !isNull :
                !isNull && StorageEngine::evalConditionOnRow(c, outRow, tbl_);
            if (!conditionMatches) return false;
        }
        return true;
    };
    while (child_->next(outRow)) {
        std::vector<bool> partitionNulls;
        std::optional<detail::ScopedStoredRowNullBitmap> partitionNullOwner;
        if (sameTable && tbl_.partitionType != TableSchema::PartitionType::None &&
            scan->lastStoredNullBitmap(partitionNulls) && partitionNulls.size() == tbl_.len()) {
            // Generic conditions and virtual expressions may inspect more
            // than their named column; bind the same leaf's entire bitmap.
            partitionNullOwner.emplace(tbl_, partitionNulls);
        }
        bool match = false;
        if (branches_.empty()) {
            match = matchesBranch(conds_);
        } else {
            for (const auto& branch : branches_) {
                if (matchesBranch(branch)) {
                    match = true;
                    break;
                }
            }
        }
        if (match) {
            rtInstr_.emitted = true;
            return true;
        }
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "filter child failed");
    return false;
}

void FilterOp::close() {
    child_->close();
}

// ========================================================================
// SemiJoinOp / AntiJoinOp
// ========================================================================

SemiJoinOp::SemiJoinOp(OpPtr outer, OpPtr inner, const TableSchema& outerTbl,
                       const TableSchema& innerTbl,
                       const std::string& outerColumn,
                       const std::string& innerColumn, bool anti,
                       NullSemantics nullSemantics)
    : outer_(std::move(outer)), inner_(std::move(inner)), outerTbl_(outerTbl),
      innerTbl_(innerTbl), outerColumn_(outerColumn), innerColumn_(innerColumn),
      anti_(anti), nullSemantics_(nullSemantics) {}

SemiJoinOp::SemiJoinOp(OpPtr outer, OpPtr inner, const TableSchema& outerTbl,
                       const TableSchema& innerTbl,
                       std::vector<std::pair<std::string, std::string>> keys,
                       bool anti, NullSemantics nullSemantics)
    : outer_(std::move(outer)), inner_(std::move(inner)), outerTbl_(outerTbl),
      innerTbl_(innerTbl),
      outerColumn_(keys.empty() ? std::string() : keys.front().first),
      innerColumn_(keys.empty() ? std::string() : keys.front().second),
      keys_(std::move(keys)), anti_(anti), nullSemantics_(nullSemantics) {}

bool SemiJoinOp::open() {
    rows_.clear();
    nullRows_.clear();
    origins_.clear();
    pos_ = 0;

    const auto rememberOuterRow = [&](const std::string& row) {
        rows_.push_back(row);
        origins_.push_back(outer_->scanOrigin());
        std::vector<bool> nulls(outerTbl_.len(), false);
        for (size_t i = 0; i < outerTbl_.len(); ++i) {
            nulls[i] = outer_->lastColumnIsNull(i);
        }
        nullRows_.push_back(std::move(nulls));
    };

    if (!keys_.empty()) {
        // Multi-column semi/anti join. Keep fields structural so a delimiter
        // byte stored in one column cannot collide with a boundary between
        // columns. For IN/NOT IN, each inner row is compared as a SQL row
        // value: a definite unequal field makes that row comparison FALSE,
        // even if another field is NULL; UNKNOWN is retained only when no
        // field proves inequality. EXISTS correlations use the same equality
        // test but treat UNKNOWN as a non-match, as a WHERE clause does.
        std::vector<size_t> outerIdxs, innerIdxs;
        for (const auto& k : keys_) {
            size_t oi = outerTbl_.len(), ii = innerTbl_.len();
            for (size_t i = 0; i < outerTbl_.len(); ++i)
                if (outerTbl_.cols[i].dataName == k.first) { oi = i; break; }
            for (size_t i = 0; i < innerTbl_.len(); ++i)
                if (innerTbl_.cols[i].dataName == k.second) { ii = i; break; }
            if (oi >= outerTbl_.len() || ii >= innerTbl_.len()) return false;
            outerIdxs.push_back(oi);
            innerIdxs.push_back(ii);
        }
        if (!inner_->open()) return false;
        struct KeyValue {
            std::string text;
            bool isNull = false;
        };
        const auto rowKey = [](const std::string& row,
                               const TableSchema& table,
                               const std::vector<size_t>& indexes,
                               const Operator* source) {
            std::vector<KeyValue> key;
            key.reserve(indexes.size());
            for (const size_t index : indexes) {
                // The child operator carries the authoritative SQL NULL
                // bitmap. An empty text value is valid data and cannot be
                // inferred to be NULL from its rendered payload.
                const bool isNull = source->lastColumnIsNull(index);
                key.push_back({StorageEngine::extractColumnValueStatic(
                                   row, table, index),
                               isNull});
            }
            return key;
        };
        std::vector<std::vector<KeyValue>> innerRows;
        std::string row;
        while (inner_->next(row)) {
            innerRows.push_back(rowKey(row, innerTbl_, innerIdxs, inner_.get()));
        }
        if (inner_->hasError()) {
            return propagateChildError(inner_.get(), "semi-join inner scan failed");
        }
        inner_->close();
        if (!outer_->open()) return false;
        while (outer_->next(row)) {
            const std::vector<KeyValue> outerKey = rowKey(
                row, outerTbl_, outerIdxs, outer_.get());
            bool found = false;
            bool comparisonUnknown = false;
            for (const auto& innerKey : innerRows) {
                bool rowUnequal = false;
                bool rowUnknown = false;
                for (size_t index = 0; index < outerKey.size(); ++index) {
                    const auto equal = StorageEngine::compareValues(
                        outerTbl_.cols[outerIdxs[index]],
                        outerKey[index].text, outerKey[index].isNull,
                        innerKey[index].text, innerKey[index].isNull, "=");
                    if (equal == StorageEngine::PredicateTruth::False) {
                        rowUnequal = true;
                        break;
                    }
                    if (equal == StorageEngine::PredicateTruth::Unknown)
                        rowUnknown = true;
                }
                if (!rowUnequal && !rowUnknown) {
                    found = true;
                    break;
                }
                if (!rowUnequal && rowUnknown) comparisonUnknown = true;
            }
            const bool keep = nullSemantics_ == NullSemantics::ExistsCorrelation
                ? (anti_ ? !found : found)
                : (anti_
                    ? (innerRows.empty() || (!found && !comparisonUnknown))
                    : found);
            if (keep) rememberOuterRow(row);
        }
        if (outer_->hasError())
            return propagateChildError(outer_.get(), "semi-join outer scan failed");
        outer_->close();
        return true;
    }

    size_t outerIdx = outerTbl_.len();
    size_t innerIdx = innerTbl_.len();
    for (size_t i = 0; i < outerTbl_.len(); ++i) {
        if (outerTbl_.cols[i].dataName == outerColumn_) {
            outerIdx = i;
            break;
        }
    }
    for (size_t i = 0; i < innerTbl_.len(); ++i) {
        if (innerTbl_.cols[i].dataName == innerColumn_) {
            innerIdx = i;
            break;
        }
    }
    if (outerIdx >= outerTbl_.len() || innerIdx >= innerTbl_.len()) return false;
    if (!inner_->open()) return false;

    std::unordered_set<std::string> innerKeys;
    bool innerHasNull = false;
    bool innerHasRow = false;
    std::string row;
    while (inner_->next(row)) {
        innerHasRow = true;
        const std::string value =
            StorageEngine::extractColumnValueStatic(row, innerTbl_, innerIdx);
        if (inner_->lastColumnIsNull(innerIdx)) {
            innerHasNull = true;
        } else {
            innerKeys.insert(value);
        }
    }
    inner_->close();

    if (!outer_->open()) return false;
    while (outer_->next(row)) {
        const std::string value =
            StorageEngine::extractColumnValueStatic(row, outerTbl_, outerIdx);
        const bool outerIsNull = outer_->lastColumnIsNull(outerIdx);
        const bool found = !outerIsNull && innerKeys.find(value) != innerKeys.end();

        // SQL's three-valued logic matters for NOT IN: a NULL in the inner
        // relation makes every non-matching comparison UNKNOWN, not TRUE.
        // The empty set is the exception: NOT IN over it is TRUE even when
        // the outer value is NULL (there is no comparison that can be UNKNOWN).
        const bool keep = nullSemantics_ == NullSemantics::ExistsCorrelation
            ? (anti_ ? !found : found)
            : (anti_
                ? (!innerHasRow || (!outerIsNull && !found && !innerHasNull))
                : found);
        if (keep) rememberOuterRow(row);
    }
    outer_->close();
    return true;
}

bool SemiJoinOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    const size_t rowIndex = pos_++;
    outRow = rows_[rowIndex];
    const ScanOrigin origin = rowIndex < origins_.size()
        ? origins_[rowIndex] : ScanOrigin{};
    if (origin.engine && origin.rid > 0) {
        StorageEngine::bindNullRow(
            origin.engine, origin.dbname, origin.tablename, origin.rid,
            outerTbl_.len());
    } else {
        StorageEngine::unbindNullRow();
    }
    rtInstr_.emitted = true;
    return true;
}

bool SemiJoinOp::lastColumnIsNull(size_t colIdx) const {
    return pos_ > 0 && pos_ - 1 < nullRows_.size() &&
        colIdx < nullRows_[pos_ - 1].size() && nullRows_[pos_ - 1][colIdx];
}

Operator::ScanOrigin SemiJoinOp::scanOrigin() const {
    return pos_ > 0 && pos_ - 1 < origins_.size()
        ? origins_[pos_ - 1] : ScanOrigin{};
}

void SemiJoinOp::close() {
    rows_.clear();
    nullRows_.clear();
    origins_.clear();
    pos_ = 0;
    StorageEngine::unbindNullRow();
}

// ========================================================================
// QuantifiedSubqueryFilterOp
// ========================================================================

bool QuantifiedSubqueryFilterOp::open() {
    values_.clear();

    size_t outerIdx = outerTbl_.len();
    size_t innerIdx = innerTbl_.len();
    for (size_t i = 0; i < outerTbl_.len(); ++i) {
        if (outerTbl_.cols[i].dataName == outerColumn_) {
            outerIdx = i;
            break;
        }
    }
    for (size_t i = 0; i < innerTbl_.len(); ++i) {
        if (innerTbl_.cols[i].dataName == innerColumn_) {
            innerIdx = i;
            break;
        }
    }
    if (outerIdx >= outerTbl_.len() || innerIdx >= innerTbl_.len()) return false;
    if (!inner_->open()) return false;

    std::string row;
    while (inner_->next(row)) {
        values_.push_back({
            StorageEngine::extractColumnValueStatic(row, innerTbl_, innerIdx),
            inner_->lastColumnIsNull(innerIdx)});
    }
    if (inner_->hasError()) return propagateChildError(inner_.get(), "quantified subquery failed");
    inner_->close();
    return outer_->open();
}

bool QuantifiedSubqueryFilterOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    size_t outerIdx = outerTbl_.len();
    for (size_t i = 0; i < outerTbl_.len(); ++i) {
        if (outerTbl_.cols[i].dataName == outerColumn_) {
            outerIdx = i;
            break;
        }
    }
    if (outerIdx >= outerTbl_.len()) return false;

    while (outer_->next(outRow)) {
        const std::string left = StorageEngine::extractColumnValueStatic(
            outRow, outerTbl_, outerIdx);
        const bool leftIsNull = outer_->lastColumnIsNull(outerIdx);
        // ANY over an empty set is FALSE and ALL over an empty set is TRUE,
        // independently of the left operand (including SQL NULL).
        if (values_.empty()) {
            if (all_) {
                rtInstr_.emitted = true;
                return true;
            }
            continue;
        }
        if (leftIsNull) continue; // NULL <op> ANY/ALL is UNKNOWN.

        bool result = all_;
        bool hasUnknown = false;
        for (const auto& value : values_) {
            const auto truth = StorageEngine::compareValues(
                outerTbl_.cols[outerIdx], left, leftIsNull,
                value.text, value.isNull, op_);
            if (truth == StorageEngine::PredicateTruth::Unknown) {
                hasUnknown = true;
                continue;
            }
            if (all_ && truth == StorageEngine::PredicateTruth::False) {
                result = false;
                hasUnknown = false;
                break;
            }
            if (!all_ && truth == StorageEngine::PredicateTruth::True) {
                result = true;
                hasUnknown = false;
                break;
            }
        }
        if (hasUnknown) result = false; // UNKNOWN is filtered from WHERE.
        if (result) { rtInstr_.emitted = true; return true; }
    }
    if (outer_->hasError()) return propagateChildError(outer_.get(), "quantified outer scan failed");
    return false;
}

void QuantifiedSubqueryFilterOp::close() {
    outer_->close();
    values_.clear();
}

// ========================================================================
// ExistenceFilterOp
// ========================================================================

bool ExistenceFilterOp::open() {
    rows_.clear();
    origins_.clear();
    pos_ = 0;

    if (!inner_->open()) return false;
    std::string row;
    const bool innerHasRow = inner_->next(row);
    if (inner_->hasError()) return propagateChildError(inner_.get(), "existence subquery failed");
    inner_->close();

    const bool keepRows = anti_ ? !innerHasRow : innerHasRow;
    if (!keepRows) return true;
    if (!outer_->open()) return false;
    while (outer_->next(row)) {
        rows_.push_back(row);
        origins_.push_back(outer_->scanOrigin());
    }
    if (outer_->hasError()) return propagateChildError(outer_.get(), "existence outer scan failed");
    outer_->close();
    return true;
}

bool ExistenceFilterOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    const size_t rowIndex = pos_++;
    outRow = rows_[rowIndex];
    const ScanOrigin origin = rowIndex < origins_.size()
        ? origins_[rowIndex] : ScanOrigin{};
    if (origin.engine && origin.rid > 0) {
        StorageEngine::bindNullRow(
            origin.engine, origin.dbname, origin.tablename, origin.rid,
            std::numeric_limits<size_t>::max());
    } else {
        StorageEngine::unbindNullRow();
    }
    rtInstr_.emitted = true;
    return true;
}

bool ExistenceFilterOp::lastColumnIsNull(size_t colIdx) const {
    const ScanOrigin origin = scanOrigin();
    return origin.engine && origin.rid > 0 &&
        origin.engine->isColumnNullByRid(
            origin.dbname, origin.tablename, origin.rid, colIdx);
}

Operator::ScanOrigin ExistenceFilterOp::scanOrigin() const {
    return pos_ > 0 && pos_ - 1 < origins_.size()
        ? origins_[pos_ - 1] : ScanOrigin{};
}

void ExistenceFilterOp::close() {
    rows_.clear();
    origins_.clear();
    pos_ = 0;
    StorageEngine::unbindNullRow();
}

// ========================================================================
// ScalarSubqueryProjectOp
// ========================================================================

bool ScalarSubqueryProjectOp::open() {
    clearError();
    scalarValue_.clear();
    scalarIsNull_ = true;
    lastCells_.clear();
    lastNulls_.clear();

    size_t innerIdx = innerTbl_.len();
    for (size_t i = 0; i < innerTbl_.len(); ++i) {
        if (innerTbl_.cols[i].dataName == innerColumn_) {
            innerIdx = i;
            break;
        }
    }
    if (innerIdx >= innerTbl_.len()) {
        setError("scalar subquery column does not exist");
        return false;
    }
    bool innerCloseAttempted = false;
    const auto closeInner = [&] {
        if (!innerCloseAttempted) {
            innerCloseAttempted = true;
            inner_->close();
        }
    };
    try {
        if (!inner_->open()) {
            try { closeInner(); } catch (...) {}
            return propagateChildError(inner_.get(), "scalar subquery failed to open");
        }
        std::string row;
        if (inner_->next(row)) {
            scalarIsNull_ = inner_->lastColumnIsNull(innerIdx);
            if (!scalarIsNull_) {
                scalarValue_ = StorageEngine::extractColumnValueStatic(
                    row, innerTbl_, innerIdx);
            }
            if (inner_->next(row))
                throw DbError("21000", "more than one row returned by a subquery used as an expression");
        }
        if (inner_->hasError()) {
            try { closeInner(); } catch (...) {}
            return propagateChildError(inner_.get(), "scalar subquery failed");
        }
        closeInner();
        if (inner_->hasError()) return propagateChildError(inner_.get(), "scalar subquery failed");
    } catch (...) {
        // Cardinality/read failures retain their structured SQL identity even
        // if close also fails. Attempt inner cleanup once, before the checked
        // executor independently closes the outer plan.
        const auto failure = std::current_exception();
        try { closeInner(); } catch (...) {}
        std::rethrow_exception(failure);
    }
    return outer_->open();
}

bool ScalarSubqueryProjectOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    std::string row;
    if (!outer_->next(row)) {
        if (outer_->hasError()) return propagateChildError(outer_.get(), "scalar outer scan failed");
        return false;
    }

    outRow.clear();
    lastCells_.clear();
    lastNulls_.clear();
    lastCells_.reserve(targets_.size());
    lastNulls_.reserve(targets_.size());
    for (const auto& target : targets_) {
        std::string value;
        bool isNull = false;
        if (target.isScalar) {
            isNull = scalarIsNull_;
            value = isNull ? "NULL" : scalarValue_;
        } else {
            size_t outerIdx = outerTbl_.len();
            for (size_t i = 0; i < outerTbl_.len(); ++i) {
                if (outerTbl_.cols[i].dataName == target.column) {
                    outerIdx = i;
                    break;
                }
            }
            if (outerIdx >= outerTbl_.len()) {
                setError("scalar projection column does not exist");
                return false;
            }
            isNull = outer_->lastColumnIsNull(outerIdx);
            value = isNull ? "NULL" :
                StorageEngine::extractColumnValueStatic(row, outerTbl_, outerIdx);
        }
        lastCells_.push_back(isNull ? std::string{} : value);
        lastNulls_.push_back(isNull);
        outRow += value;
        outRow += ' ';
    }
    rtInstr_.emitted = true;
    return true;
}

bool ScalarSubqueryProjectOp::lastStructuredRow(
    std::vector<std::string>& cells, std::vector<bool>& nulls) const {
    if (lastCells_.size() != targets_.size() ||
        lastNulls_.size() != targets_.size()) {
        return false;
    }
    cells = lastCells_;
    nulls = lastNulls_;
    return true;
}

void ScalarSubqueryProjectOp::close() {
    outer_->close();
    lastCells_.clear();
    lastNulls_.clear();
}

// ========================================================================
// ProjectOp
// ========================================================================

ProjectOp::ProjectOp(OpPtr child, const TableSchema& tbl,
                      const std::set<std::string>& selectCols)
    : child_(std::move(child)), tbl_(tbl), selectCols_(selectCols) {}

bool ProjectOp::open() {
    clearError();
    lastCells_.clear();
    lastNulls_.clear();
    lastRowAvailable_ = false;
    return child_->open();
}

bool ProjectOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    std::string raw;
    auto* physicalScan = dynamic_cast<TableScanOp*>(child_.get());
    std::vector<std::string> projectedCells;
    std::vector<bool> projectedNulls;
    bool decoded = false;
    const bool good = physicalScan
        ? physicalScan->nextForProjection(raw, tbl_, selectCols_,
                                          projectedCells, projectedNulls, decoded)
        : child_->next(raw);
    if (!good) {
        if (child_->hasError()) return propagateChildError(child_.get(), "projection child failed");
        return false;
    }
    lastCells_.clear();
    lastNulls_.clear();
    const auto origin = child_->scanOrigin();
    if (origin.engine && origin.rid > 0)
        StorageEngine::bindNullRow(origin.engine, origin.dbname, origin.tablename,
                                  origin.rid, tbl_.len());
    if (decoded) {
        lastCells_ = std::move(projectedCells);
        lastNulls_ = std::move(projectedNulls);
        outRow.clear();
        for (size_t i = 0; i < lastCells_.size(); ++i) {
            const auto& value = lastCells_[i];
            if (lastNulls_[i]) outRow += "NULL ";
            else if (value.find(' ') != std::string::npos && selectCols_.size() != 1)
                outRow += "\"" + value + "\" ";
            else outRow += value + ' ';
        }
        lastRowAvailable_ = true;
        rtInstr_.emitted = true;
        return true;
    }
    // Borrow only the current physical cursor or a concrete sort's emitted
    // row, for this next() call. Other children keep their existing fallback.
    std::vector<bool> storedNulls;
    std::optional<detail::ScopedStoredRowNullBitmap> storedNullOwner;
    // Nonnullable physical targets never ask the stored-NULL fallback. A
    // selected virtual target can still demand nullable physical inputs.
    const auto needsStoredNulls = [&] {
        for (size_t i = 0; i < tbl_.len(); ++i) {
            const auto& column = tbl_.cols[i];
            if ((column.isNull || column.generatedKind == 'v') &&
                (selectCols_.empty() || selectCols_.count(column.dataName)))
                return true;
        }
        return false;
    };
    const auto matchingSort = [&]() -> SortOp* {
        if (typeid(*child_) != typeid(SortOp)) return nullptr;
        auto* sort = static_cast<SortOp*>(child_.get());
        const auto& source = sort->tbl_;
        if (source.tablename != tbl_.tablename || source.len() != tbl_.len() ||
            source.formatVersion != tbl_.formatVersion) return nullptr;
        for (size_t i = 0; i < tbl_.len(); ++i) {
            const auto& a = source.cols[i];
            const auto& b = tbl_.cols[i];
            if (a.dataName != b.dataName || a.dataType != b.dataType ||
                a.dsize != b.dsize || a.isVariableLength != b.isVariableLength ||
                a.isNull != b.isNull || a.isArray != b.isArray ||
                a.isUnsigned != b.isUnsigned || a.generatedKind != b.generatedKind ||
                a.generatedExpr != b.generatedExpr || a.storageCodec != b.storageCodec)
                return nullptr;
        }
        return sort;
    };
    if (physicalScan && origin.engine && !origin.dbname.empty() &&
        origin.rid > 0 && origin.rid == physicalScan->lastRid() &&
        origin.tablename == physicalScan->tableName() &&
        origin.tablename == tbl_.tablename &&
        tbl_.formatVersion == DATA_FILE_FORMAT_VERSION &&
        needsStoredNulls() &&
        physicalScan->lastStoredNullBitmap(storedNulls) &&
        storedNulls.size() == tbl_.len()) {
        storedNullOwner.emplace(tbl_, storedNulls);
    } else if (!physicalScan && origin.engine && !origin.dbname.empty() &&
               origin.rid > 0 && origin.tablename == tbl_.tablename &&
               tbl_.formatVersion == DATA_FILE_FORMAT_VERSION && needsStoredNulls()) {
        if (auto* sort = matchingSort(); sort &&
            sort->lastStoredNullBitmap(storedNulls) && storedNulls.size() == tbl_.len())
            storedNullOwner.emplace(tbl_, storedNulls);
        else if (tbl_.partitionType != TableSchema::PartitionType::None &&
                 typeid(*child_) == typeid(FilterOp) &&
                 child_->lastStoredNullBitmap(storedNulls) && storedNulls.size() == tbl_.len())
            storedNullOwner.emplace(tbl_, storedNulls);
    }
    outRow.clear();
    for (size_t i = 0; i < tbl_.len(); ++i) {
        if (!selectCols_.empty() && !selectCols_.count(tbl_.cols[i].dataName))
            continue;
        bool computedNull = false;
        std::string value = origin.engine
            ? origin.engine->extractColumnValue(raw, tbl_, i, origin.dbname,
                                                 true, &computedNull)
            : StorageEngine::extractColumnValueStatic(raw, tbl_, i);
        const bool isNull = computedNull ||
            (tbl_.cols[i].generatedKind != 'v' && child_->lastColumnIsNull(i));
        lastCells_.push_back(isNull ? std::string{} : value);
        lastNulls_.push_back(isNull);
        if (isNull) outRow += "NULL ";
        else if (value.find(' ') != std::string::npos && selectCols_.size() != 1)
            outRow += "\"" + value + "\" ";
        else outRow += value + ' ';
    }
    lastRowAvailable_ = true;
    rtInstr_.emitted = true;
    return true;
}

bool ProjectOp::lastStructuredRow(std::vector<std::string>& cells,
                                  std::vector<bool>& nulls) const {
    if (!lastRowAvailable_ || lastCells_.size() != lastNulls_.size()) return false;
    cells = lastCells_;
    nulls = lastNulls_;
    return true;
}

bool ProjectOp::storedOutputRoles(std::vector<StoredReferenceOutputRole>& roles) const {
    roles.clear();
    if (typeid(*this) != typeid(ProjectOp) || tbl_.len() > MAX_COLUMNS) return false;
    bool references = false;
    for (size_t i = 0; i < tbl_.len(); ++i) {
        const auto& column = tbl_.cols[i];
        if (!selectCols_.empty() && !selectCols_.count(column.dataName)) continue;
        if (storedReferenceOutputRoleForColumn(column).kind != StoredReferenceOutputRole::Kind::None) {
            references = true;
            break;
        }
    }
    // Primitive-only projections retain their existing allocation path.
    if (!references) return false;
    std::vector<StoredReferenceOutputRole> result;
    result.reserve(selectCols_.empty() ? tbl_.len() : selectCols_.size());
    for (size_t i = 0; i < tbl_.len(); ++i) {
        const auto& column = tbl_.cols[i];
        if (!selectCols_.empty() && !selectCols_.count(column.dataName)) continue;
        result.push_back(storedReferenceOutputRoleForColumn(column));
    }
    if (!selectCols_.empty() && result.size() != selectCols_.size()) return false;
    roles = std::move(result);
    return true;
}

bool ProjectOp::lastTypedRowKey(std::string& key) const {
    if (!lastRowAvailable_ || lastCells_.size() != lastNulls_.size()) return false;
    key.clear();
    size_t projected = 0;
    for (size_t i = 0; i < tbl_.len(); ++i) {
        if (!selectCols_.empty() && !selectCols_.count(tbl_.cols[i].dataName)) continue;
        if (projected >= lastCells_.size()) return false;
        key += StorageEngine::groupingValueKey(
            tbl_.cols[i], lastCells_[projected], lastNulls_[projected]);
        ++projected;
    }
    return projected == lastCells_.size();
}

void ProjectOp::close() {
    child_->close();
    lastCells_.clear();
    lastNulls_.clear();
    lastRowAvailable_ = false;
}

// ========================================================================
// WindowOp
// ========================================================================

namespace {

struct WindowInputRow {
    std::string raw;
    std::vector<std::string> values;
    std::vector<bool> nulls;
};

static size_t sortColIndex(const TableSchema& tbl, const std::string& name) {
    for (size_t i = 0; i < tbl.len(); ++i) {
        if (tbl.cols[i].dataName == name) return i;
    }
    return tbl.len();
}

// PG float8 output: shortest representation that round-trips
// (double digits, trailing zeros trimmed).  percent_rank()/cume_dist()
// render 0.5 / 0.4, not 0.5000.
static std::string float8Shortest(double v) {
    char buf[64];
    for (int prec = 1; prec <= 17; ++prec) {
        std::snprintf(buf, sizeof(buf), "%.*g", prec, v);
        if (std::strtod(buf, nullptr) == v) break;
    }
    return buf;
}

// SQL NULL ordering for window sorts: PostgreSQL default is
// NULLS LAST for ASC and NULLS FIRST for DESC.  Callers pass the
// direction so empty (NULL) values land on the PG side.
static int compareWindowValue(const std::string& left, const std::string& right,
                             bool nullsLast = true) {
    if (left.empty() && right.empty()) return 0;
    if (left.empty()) return nullsLast ? 1 : -1;
    if (right.empty()) return nullsLast ? -1 : 1;

    char* leftEnd = nullptr;
    char* rightEnd = nullptr;
    const double leftNumber = std::strtod(left.c_str(), &leftEnd);
    const double rightNumber = std::strtod(right.c_str(), &rightEnd);
    if (leftEnd != left.c_str() && *leftEnd == '\0' &&
        rightEnd != right.c_str() && *rightEnd == '\0') {
        if (leftNumber < rightNumber) return -1;
        if (leftNumber > rightNumber) return 1;
        return 0;
    }
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
}

// Compare values after the caller has already excluded SQL NULL.  Empty text
// remains a real value here; compareWindowValue intentionally cannot make that
// distinction because its legacy callers do not carry a NULL bitmap.
static int compareNonNullAggregateValue(const std::string& left,
                                        const std::string& right) {
    char* leftEnd = nullptr;
    char* rightEnd = nullptr;
    const double leftNumber = std::strtod(left.c_str(), &leftEnd);
    const double rightNumber = std::strtod(right.c_str(), &rightEnd);
    if (leftEnd != left.c_str() && *leftEnd == '\0' &&
        rightEnd != right.c_str() && *rightEnd == '\0') {
        if (leftNumber < rightNumber) return -1;
        if (leftNumber > rightNumber) return 1;
        return 0;
    }
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
}

// Plain-column MIN/MAX uses the same typed runtime-cell comparison as
// prepared aggregates. Numeric-looking text is still text; bigint/numeric
// must not round through double, and floating NaNs retain SQL ordering.
class DeclaredAggregateLess {
public:
    DeclaredAggregateLess(const Column& column, const ExprEvaluator& evaluator)
        : column_(column), evaluator_(evaluator),
          type_([&column] {
              std::string type = column.dataType;
              // Native/SQL schemas can already carry the array suffix.
              if (column.isArray && (type.size() < 2 || type.compare(type.size() - 2, 2, "[]") != 0))
                  type += "[]";
              return ExprHelper::canonicalResultTypeName(type);
          }()) {
        expression_.op = "<";
        auto left = std::make_unique<ParameterExpr>();
        left->slot = 0; left->origin = ParameterOrigin::RuntimeCell; left->declaredType = type_;
        auto right = std::make_unique<ParameterExpr>();
        right->slot = 1; right->origin = ParameterOrigin::RuntimeCell; right->declaredType = type_;
        expression_.left = std::move(left); expression_.right = std::move(right);
    }
    bool less(const std::string& left, const std::string& right) const {
        if (!column_.isArray && !column_.enumValues.empty()) {
            const auto rank = [&](const std::string& value) {
                const auto found = std::find(column_.enumValues.begin(), column_.enumValues.end(), value);
                if (found == column_.enumValues.end())
                    throw DbError("22P02", "invalid input value for enum: " + value);
                return found - column_.enumValues.begin();
            };
            return rank(left) < rank(right);
        }
        ExprValue first(type_, left), second(type_, right);
        first.collation = second.collation = column_.resolvedCollation.empty()
            ? column_.collation : column_.resolvedCollation;
        RowContext row; row.setParameters({std::move(first), std::move(second)});
        const auto result = evaluator_.eval(&expression_, row);
        if (result.isNull || ExprHelper::canonicalResultTypeName(result.typeName) != "boolean")
            throw DbError("XX000", "non-NULL aggregate comparison lost its boolean result");
        return result.asBool();
    }
private:
    const Column& column_;
    const ExprEvaluator& evaluator_;
    std::string type_;
    BinaryOpExpr expression_;
};

static int compareWindowCell(const std::string& left, bool leftIsNull,
                             const std::string& right, bool rightIsNull,
                             bool nullsLast = true) {
    if (leftIsNull && rightIsNull) return 0;
    if (leftIsNull) return nullsLast ? 1 : -1;
    if (rightIsNull) return nullsLast ? -1 : 1;
    return compareNonNullAggregateValue(left, right);
}

static size_t windowColumnIndex(const TableSchema& tbl, const std::string& name) {
    for (size_t i = 0; i < tbl.len(); ++i) {
        if (tbl.cols[i].dataName == name) return i;
    }
    return tbl.len();
}

static int compareDeclaredWindowCell(const Column& column,
    const std::string& left, bool leftNull, const std::string& right, bool rightNull) {
    if (leftNull != rightNull) return leftNull ? 1 : -1;
    if (leftNull) return 0;
    const auto less = StorageEngine::compareValues(column, left, false, right, false, "<");
    const auto greater = StorageEngine::compareValues(column, left, false, right, false, ">");
    if (less == StorageEngine::PredicateTruth::Unknown ||
        greater == StorageEngine::PredicateTruth::Unknown)
        throw DbError("0A000", "unsupported declared window comparison");
    return less == StorageEngine::PredicateTruth::True ? -1 :
        greater == StorageEngine::PredicateTruth::True ? 1 : 0;
}

static bool sameWindowPartition(const WindowInputRow& left,
                                const WindowInputRow& right,
    const std::vector<size_t>& columns, const TableSchema& schema) {
    for (size_t column : columns) {
        if (compareDeclaredWindowCell(schema.cols[column], left.values[column], left.nulls[column],
                right.values[column], right.nulls[column]) != 0) return false;
    }
    return true;
}

static bool sameWindowPeer(const WindowInputRow& left,
                           const WindowInputRow& right,
    size_t orderColumn,
    const TableSchema& schema) {
    return orderColumn >= schema.len() ||
           compareDeclaredWindowCell(schema.cols[orderColumn], left.values[orderColumn], left.nulls[orderColumn],
                             right.values[orderColumn], right.nulls[orderColumn]) == 0;
}

static bool parseWindowNumber(const std::string& value, double& out) {
    if (value.empty()) return false;
    char* end = nullptr;
    out = std::strtod(value.c_str(), &end);
    return end != value.c_str() && *end == '\0' && std::isfinite(out);
}

static std::string renderWindowArrayElement(const std::string& value,
                                            bool isNull) {
    if (isNull) return "NULL";
    bool quote = value.empty() || value == "NULL";
    for (unsigned char c : value) {
        if (std::isspace(c) || c == ',' || c == '{' || c == '}' ||
            c == '"' || c == '\\') {
            quote = true;
            break;
        }
    }
    if (!quote) return value;
    std::string result = "\"";
    for (char c : value) {
        if (c == '"' || c == '\\') result.push_back('\\');
        result.push_back(c);
    }
    result.push_back('"');
    return result;
}

} // namespace

WindowOp::WindowOp(OpPtr child, const TableSchema& tbl,
                   const std::vector<WindowTarget>& targets,
                   const std::vector<WindowFunctionSpec>& functions,
                    const std::string& finalOrderBy,
                    bool finalOrderAscending,
                    bool finalOrderNullsFirst,
                    bool hasExplicitFinalNullOrder)
    : child_(std::move(child)), tbl_(tbl), targets_(targets),
      functions_(functions), finalOrderBy_(finalOrderBy),
      finalOrderAscending_(finalOrderAscending),
      finalOrderNullsFirst_(hasExplicitFinalNullOrder ? finalOrderNullsFirst : !finalOrderAscending) {}

bool WindowOp::open() {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
    for(const auto& function:functions_)
        if(function.preparedFrame.prepared())function.preparedFrame.validateOffsets();
    if (!child_->open()) return false;

    std::vector<WindowInputRow> input;
    std::string raw;
    while (child_->next(raw)) {
        WindowInputRow row;
        row.raw = std::move(raw);
        row.values.reserve(tbl_.len());
        row.nulls.reserve(tbl_.len());
        for (size_t i = 0; i < tbl_.len(); ++i) {
            row.values.push_back(StorageEngine::extractColumnValueStatic(row.raw, tbl_, i));
            row.nulls.push_back(child_->lastColumnIsNull(i));
        }
        input.push_back(std::move(row));
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "window child failed");
    child_->close();

    std::vector<std::vector<std::string>> computed(
        input.size(), std::vector<std::string>(functions_.size()));
    std::vector<std::vector<bool>> computedNulls(
        input.size(), std::vector<bool>(functions_.size(), true));
    for (size_t functionIndex = 0; functionIndex < functions_.size(); ++functionIndex) {
        const auto& function = functions_[functionIndex];
        std::vector<size_t> partitionColumns;
        for (const auto& name : function.partitionBy) {
            const size_t column = windowColumnIndex(tbl_, name);
            if (column >= tbl_.len()) return false;
            partitionColumns.push_back(column);
        }
        const size_t orderColumn = function.orderBy.empty()
            ? tbl_.len() : windowColumnIndex(tbl_, function.orderBy);
        if (!function.orderBy.empty() && orderColumn >= tbl_.len()) return false;

        const size_t argumentColumn = (function.argument.empty() || function.argument == "*")
            ? tbl_.len() : windowColumnIndex(tbl_, function.argument);
        // Boolean aggregates accept comparison expressions ("v > 5") as
        // arguments; evaluated per row instead of a direct column read.
        const bool boolAggWithExpr =
            (function.name == "bool_and" || function.name == "bool_or" || function.name == "every") &&
            argumentColumn >= tbl_.len() && !function.argument.empty() &&
            function.argument != "*";
        const bool argumentRequired = function.name == "lag" || function.name == "lead" ||
            function.name == "sum" || function.name == "avg" || function.name == "min" ||
            function.name == "max" || function.name == "first_value" ||
            function.name == "last_value" || function.name == "bool_and" ||
            function.name == "bool_or" || function.name == "every";
        if (argumentRequired && argumentColumn >= tbl_.len() && !boolAggWithExpr) return false;

        std::vector<size_t> order(input.size());
        for (size_t i = 0; i < order.size(); ++i) order[i] = i;
        interruptibleStableSort(order.begin(), order.end(), [&](size_t left, size_t right) {
            const auto& leftRow = input[left];
            const auto& rightRow = input[right];
            for (size_t column : partitionColumns) {
                const int cmp = compareDeclaredWindowCell(tbl_.cols[column],
                    leftRow.values[column], leftRow.nulls[column],
                    rightRow.values[column], rightRow.nulls[column]);
                if (cmp != 0) return cmp < 0;
            }
            if (orderColumn < tbl_.len()) {
                const bool leftNull=leftRow.nulls[orderColumn];
                const bool rightNull=rightRow.nulls[orderColumn];
                if (leftNull!=rightNull) {
                    const bool nullsFirst=function.hasExplicitOrderNulls
                        ? function.orderByNullsFirst : !function.orderAscending;
                    return leftNull==nullsFirst;
                }
                const int cmp = compareDeclaredWindowCell(tbl_.cols[orderColumn],
                    leftRow.values[orderColumn], leftRow.nulls[orderColumn],
                    rightRow.values[orderColumn], rightRow.nulls[orderColumn]);
                if (cmp != 0) return function.orderAscending ? cmp < 0 : cmp > 0;
            }
            return left < right;
        });

        std::vector<size_t> partitionStartAt(order.size());
        std::vector<size_t> partitionEndAt(order.size());
        for (size_t position = 0; position < order.size();) {
            size_t end = position + 1;
            while (end < order.size() &&
                   sameWindowPartition(input[order[end]], input[order[position]], partitionColumns, tbl_)) {
                ++end;
            }
            for (size_t p = position; p < end; ++p) {
                partitionStartAt[p] = position;
                partitionEndAt[p] = end;
            }
            position = end;
        }

        std::vector<size_t> groupStartAt(order.size());
        std::vector<size_t> groupEndAt(order.size());
        for (size_t partitionStart = 0; partitionStart < order.size();) {
            const size_t partitionEnd = partitionEndAt[partitionStart];
            size_t groupStart = partitionStart;
            while (groupStart < partitionEnd) {
                size_t groupEnd = groupStart + 1;
                while (groupEnd < partitionEnd &&
                       sameWindowPeer(input[order[groupEnd]], input[order[groupStart]],
                                      orderColumn, tbl_)) {
                    ++groupEnd;
                }
                for (size_t p = groupStart; p < groupEnd; ++p) {
                    groupStartAt[p] = groupStart;
                    groupEndAt[p] = groupEnd;
                }
                groupStart = groupEnd;
            }
            partitionStart = partitionEnd;
        }

        std::vector<size_t> rankAt(order.size());
        std::vector<size_t> denseRankAt(order.size());
        for (size_t partitionStart = 0; partitionStart < order.size();) {
            const size_t partitionEnd = partitionEndAt[partitionStart];
            size_t denseRank = 1;
            for (size_t position = partitionStart; position < partitionEnd; ++position) {
                if (position > partitionStart && groupStartAt[position] == position) ++denseRank;
                rankAt[position] = groupStartAt[position] - partitionStart + 1;
                denseRankAt[position] = denseRank;
            }
            partitionStart = partitionEnd;
        }

        auto frameBounds = [&](size_t position) -> std::pair<size_t, size_t> {
            const size_t partitionStart = partitionStartAt[position];
            const size_t partitionEnd = partitionEndAt[position];
            if(function.preparedFrame.prepared()) {
                return function.preparedFrame.bounds(position,partitionStart,partitionEnd,
                    [&](size_t row){return std::make_pair(groupStartAt[row],groupEndAt[row]);},
                    [&](size_t row){
                        if(orderColumn>=tbl_.len())throw DbError("XX000","RANGE frame has no ORDER BY key");
                        return ExprValue(tbl_.cols[orderColumn].dataType,input[order[row]].values[orderColumn],
                            input[order[row]].nulls[orderColumn]);
                    },function.orderAscending,function.hasExplicitOrderNulls?function.orderByNullsFirst:!function.orderAscending);
            }
            if (!function.hasFrame) {
                if (orderColumn < tbl_.len()) return {partitionStart, groupEndAt[position]};
                return {partitionStart, partitionEnd};
            }
            if (function.frameType == WindowFunctionSpec::FrameType::GROUPS) {
                size_t begin = groupStartAt[position];
                size_t end = groupEndAt[position];
                if (function.frameStartOffset < 0) {
                    begin = partitionStart;
                } else {
                    size_t groups = static_cast<size_t>(function.frameStartOffset);
                    while (groups-- > 0 && begin > partitionStart) {
                        begin = groupStartAt[begin - 1];
                    }
                }
                if (function.frameEndOffset < 0) {
                    end = partitionEnd;
                } else {
                    size_t groups = static_cast<size_t>(function.frameEndOffset);
                    while (groups-- > 0 && end < partitionEnd) {
                        end = groupEndAt[end];
                    }
                }
                return {begin, end};
            }
            if (function.frameType == WindowFunctionSpec::FrameType::RANGE &&
                orderColumn < tbl_.len()) {
                double currentKey = 0.0;
                if (!parseWindowNumber(input[order[position]].values[orderColumn], currentKey)) {
                    // NULL current row: PG treats the RANGE frame end as the
                    // NULL peer group and an UNBOUNDED/PRECEDING start still
                    // spans all lower rows, so aggregate inputs remain visible
                    // (min OVER (... RANGE UNBOUNDED PRECEDING AND CURRENT ROW)
                    // yields the running minimum, not NULL).
                    return {partitionStart, groupEndAt[position]};
                }
                if (!function.orderAscending) currentKey = -currentKey;
                auto orderedKey = [&](size_t framePosition, double& key) {
                    if (!parseWindowNumber(input[order[framePosition]].values[orderColumn], key)) {
                        return false;
                    }
                    if (!function.orderAscending) key = -key;
                    return true;
                };
                size_t begin = partitionStart;
                size_t end = partitionEnd;
                if (function.frameStartOffset >= 0) {
                    if (function.frameStartOffset == 0) {
                        begin = groupStartAt[position];
                    } else {
                        const double threshold = currentKey - function.frameStartOffset;
                        while (begin < partitionEnd) {
                            double key = 0.0;
                            if (!orderedKey(begin, key) || key >= threshold) break;
                            ++begin;
                        }
                    }
                }
                if (function.frameEndOffset >= 0) {
                    if (function.frameEndOffset == 0) {
                        end = groupEndAt[position];
                    } else {
                        const double threshold = currentKey + function.frameEndOffset;
                        end = position;
                        while (end < partitionEnd) {
                            double key = 0.0;
                            if (!orderedKey(end, key) || key > threshold) break;
                            ++end;
                        }
                    }
                }
                return {begin, end};
            }
            size_t begin = partitionStart;
            size_t end = partitionEnd;
            const size_t partitionPosition = position - partitionStart;
            if (function.frameStartOffset >= 0) {
                const size_t preceding = static_cast<size_t>(function.frameStartOffset);
                begin = preceding > partitionPosition
                    ? partitionStart : position - preceding;
            }
            if (function.frameEndOffset >= 0) {
                const size_t following = static_cast<size_t>(function.frameEndOffset);
                end = std::min(partitionEnd, position + following + 1);
            }
            return {begin, end};
        };

        auto rowIsExcluded = [&](size_t framePosition, size_t currentPosition) {
            if (function.frameExclusion == "current row") return framePosition == currentPosition;
            if (function.frameExclusion == "group") {
                return framePosition >= groupStartAt[currentPosition] &&
                       framePosition < groupEndAt[currentPosition];
            }
            if (function.frameExclusion == "ties") {
                return framePosition >= groupStartAt[currentPosition] &&
                       framePosition < groupEndAt[currentPosition] &&
                       framePosition != currentPosition;
            }
            return false;
        };

        auto parseInteger = [](const std::string& value, int64_t& out) {
            try {
                size_t consumed = 0;
                out = std::stoll(value, &consumed);
                return consumed == value.size();
            } catch (...) {
                return false;
            }
        };

        for (size_t position = 0; position < order.size(); ++position) {
            const size_t rowIndex = order[position];
            const size_t partitionStart = partitionStartAt[position];
            const size_t partitionEnd = partitionEndAt[position];
            const size_t peerEnd = groupEndAt[position];
            const size_t rank = rankAt[position];
            const size_t denseRank = denseRankAt[position];

            if (function.name == "row_number") {
                computed[rowIndex][functionIndex] = std::to_string(position - partitionStart + 1);
                computedNulls[rowIndex][functionIndex] = false;
            } else if (function.name == "rank") {
                computed[rowIndex][functionIndex] = std::to_string(rank);
                computedNulls[rowIndex][functionIndex] = false;
            } else if (function.name == "dense_rank") {
                computed[rowIndex][functionIndex] = std::to_string(denseRank);
                computedNulls[rowIndex][functionIndex] = false;
            } else if (function.name == "lag" || function.name == "lead") {
                const int64_t distance = function.name == "lag"
                    ? -static_cast<int64_t>(function.offset) : static_cast<int64_t>(function.offset);
                const bool hasTarget = distance < 0
                    ? static_cast<uint64_t>(-distance) <= position - partitionStart
                    : static_cast<uint64_t>(distance) < partitionEnd - position;
                if (hasTarget) {
                    const size_t targetPosition = distance < 0
                        ? position - static_cast<size_t>(-distance) : position + static_cast<size_t>(distance);
                    const auto& targetRow = input[order[targetPosition]];
                    computed[rowIndex][functionIndex] =
                        targetRow.values[argumentColumn];
                    computedNulls[rowIndex][functionIndex] =
                        targetRow.nulls[argumentColumn];
                } else if (function.hasDefault) {
                    computed[rowIndex][functionIndex] = function.defaultValue;
                    computedNulls[rowIndex][functionIndex] =
                        function.defaultIsNull;
                } else {
                    computed[rowIndex][functionIndex].clear();
                    computedNulls[rowIndex][functionIndex] = true;
                }
            } else if (function.name == "ntile") {
                if (SQLParser::toLower(trimExec(function.argument)) == "null") {
                    computedNulls[rowIndex][functionIndex] = true;
                    continue;
                }
                int64_t bucketCount = 0;
                if (!parseInteger(function.argument, bucketCount))
                    throw DbError("22P02", "invalid input syntax for NTILE integer argument");
                if (bucketCount < std::numeric_limits<int32_t>::min() || bucketCount > std::numeric_limits<int32_t>::max())
                    throw DbError("22003", "NTILE integer argument out of range");
                if (bucketCount <= 0)
                    throw DbError("22014", "argument of ntile must be greater than zero");
                const size_t partitionSize = partitionEnd - partitionStart;
                const size_t index = position - partitionStart;
                const size_t buckets = static_cast<size_t>(bucketCount);
                size_t bucket = index + 1;
                if (buckets < partitionSize) {
                    const size_t base = partitionSize / buckets;
                    const size_t extra = partitionSize % buckets;
                    // The extra rows belong to the first extra buckets, not
                    // evenly spaced buckets. This product is at most rows;
                    // avoid index*buckets and the base+1 edge when extra=0.
                    const size_t leadingRows = extra ? (base + 1) * extra : 0;
                    bucket = index < leadingRows
                        ? index / (base + 1) + 1
                        : extra + (index - leadingRows) / base + 1;
                }
                computed[rowIndex][functionIndex] = std::to_string(bucket);
                computedNulls[rowIndex][functionIndex] = false;
            } else if (function.name == "percent_rank") {
                const double value = partitionEnd - partitionStart <= 1
                    ? 0.0 : static_cast<double>(rank - 1) /
                        static_cast<double>(partitionEnd - partitionStart - 1);
                computed[rowIndex][functionIndex] = float8Shortest(value);
                computedNulls[rowIndex][functionIndex] = false;
            } else if (function.name == "cume_dist") {
                const double value = static_cast<double>(peerEnd - partitionStart) /
                    static_cast<double>(std::max<size_t>(1, partitionEnd - partitionStart));
                computed[rowIndex][functionIndex] = float8Shortest(value);
                computedNulls[rowIndex][functionIndex] = false;
            } else {
                const auto [frameBegin, frameEnd] = frameBounds(position);
                int64_t count = 0;
                dbms::Numeric exactSum(0);
                bool hasValue = false;
                bool boolValue = function.name == "bool_and" || function.name == "every";
                bool boolSeen = false;
                std::string selected;
                bool selectedIsNull = true;
                std::vector<std::pair<std::string, bool>> arrayElements;
                for (size_t framePosition = frameBegin; framePosition < frameEnd; ++framePosition) {
                    if (rowIsExcluded(framePosition, position)) continue;
                    std::string value;
                    bool valueIsNull = false;
                    if (boolAggWithExpr) {
                        // Per-row comparison evaluation ("v > 5"): substitute
                        // column tokens with this row's values.
                        std::string synth;
                        std::string token;
                        const auto& rowVals = input[order[framePosition]].values;
                        auto flushTok = [&]() {
                            if (token.empty()) return;
                            size_t ci3 = 0;
                            for (; ci3 < tbl_.len(); ++ci3)
                                if (tbl_.cols[ci3].dataName == token) break;
                            if (ci3 < tbl_.len()) { synth += rowVals[ci3]; token.clear(); return; }
                            synth += token;
                            token.clear();
                        };
                        for (char ch2 : function.argument) {
                            if (ch2 == ' ') { flushTok(); synth += ' '; continue; }
                            token += ch2;
                        }
                        flushTok();
                        auto r2 = dbms::ExprHelper::evalString(synth, {}, {}, "");
                        value = (r2.ok && !r2.isNull) ? r2.value : std::string{};
                        valueIsNull = !r2.ok || r2.isNull;
                    } else {
                        value = function.argument == "*"
                            ? std::string{} : input[order[framePosition]].values[argumentColumn];
                        valueIsNull = function.argument != "*" &&
                            input[order[framePosition]].nulls[argumentColumn];
                    }
                    if (function.name == "count") {
                        if (function.argument == "*" || !valueIsNull) ++count;
                        continue;
                    }
                    if (function.name == "first_value" && !hasValue) {
                        selected = value;
                        selectedIsNull = valueIsNull;
                        hasValue = true;
                        continue;
                    }
                    if (function.name == "last_value") {
                        selected = value;
                        selectedIsNull = valueIsNull;
                        hasValue = true;
                        continue;
                    }
                    if (function.name == "bool_and" || function.name == "every" ||
                        function.name == "bool_or") {
                        if (valueIsNull) continue;
                        boolSeen = true;
                        const bool truthyW = value == "true" || value == "t" || value == "1";
                        if (function.name == "bool_or") boolValue = boolValue || truthyW;
                        else boolValue = boolValue && truthyW;
                        continue;
                    }
                    if (function.name == "array_agg") {
                        arrayElements.push_back({std::move(value), valueIsNull});
                        continue;
                    }
                    if (valueIsNull) continue;
                    if (function.name == "sum" || function.name == "avg") {
                        // BIGINT windows can exceed int64 even when the exact
                        // result is valid NUMERIC. Keep only the exact state;
                        // an arithmetic error must not become a partial sum.
                        exactSum = exactSum + dbms::Numeric(value);
                        ++count;
                    } else if (function.name == "min" || function.name == "max") {
                        if (!hasValue || (function.name == "min"
                                ? compareDeclaredWindowCell(tbl_.cols[argumentColumn], value, false, selected, false) < 0
                                : compareDeclaredWindowCell(tbl_.cols[argumentColumn], value, false, selected, false) > 0)) {
                            selected = value;
                            selectedIsNull = false;
                            hasValue = true;
                        }
                    }
                }
                if (function.name == "array_agg") {
                    if (arrayElements.empty()) {
                        computed[rowIndex][functionIndex].clear();
                        computedNulls[rowIndex][functionIndex] = true;
                    } else {
                        std::string rendered = "{";
                        for (size_t ei = 0; ei < arrayElements.size(); ++ei) {
                            if (ei > 0) rendered += ",";
                            rendered += renderWindowArrayElement(
                                arrayElements[ei].first,
                                arrayElements[ei].second);
                        }
                        rendered += "}";
                        computed[rowIndex][functionIndex] = std::move(rendered);
                        computedNulls[rowIndex][functionIndex] = false;
                    }
                } else if (function.name == "count") {
                    computed[rowIndex][functionIndex] = std::to_string(count);
                    computedNulls[rowIndex][functionIndex] = false;
                } else if (function.name == "sum") {
                    if (count == 0) {
                        computed[rowIndex][functionIndex].clear();
                        computedNulls[rowIndex][functionIndex] = true;
                    }
                    else {
                        computed[rowIndex][functionIndex] = exactSum.toString();
                        computedNulls[rowIndex][functionIndex] = false;
                    }
                } else if (function.name == "avg") {
                    if (count == 0) {
                        computed[rowIndex][functionIndex].clear();
                        computedNulls[rowIndex][functionIndex] = true;
                    } else {
                        // PG avg(numeric): exact division with select_div_scale
                        computed[rowIndex][functionIndex] =
                            (exactSum / dbms::Numeric(count)).toString();
                        computedNulls[rowIndex][functionIndex] = false;
                    }
                } else if (function.name == "bool_and" || function.name == "every" ||
                           function.name == "bool_or") {
                    computed[rowIndex][functionIndex] =
                        boolSeen ? (boolValue ? "t" : "f") : std::string{};
                    computedNulls[rowIndex][functionIndex] = !boolSeen;
                } else if (function.name == "first_value" || function.name == "last_value" ||
                           function.name == "min" || function.name == "max") {
                    computed[rowIndex][functionIndex] = hasValue
                        ? selected : std::string{};
                    computedNulls[rowIndex][functionIndex] =
                        !hasValue || selectedIsNull;
                } else {
                    computed[rowIndex][functionIndex].clear();
                    computedNulls[rowIndex][functionIndex] = true;
                }
            }
        }
    }

    struct OutputRow {
        std::string text;
        std::vector<std::string> cells;
        std::vector<bool> nulls;
        std::string sortKey;
        bool sortKeyNull = false;
        std::vector<std::string> keys;
        std::vector<bool> keyNulls;
    };
    std::vector<OutputRow> output;
    output.reserve(input.size());
    // PG sorts window output by the window ORDER BY when the query
    // has no outer ORDER BY: the planner feeds rows through the
    // window sort.  Use the first window function order as the
    // final order in that case.
    bool finalAsc = finalOrderAscending_;
    // (partition, ORDER BY) fallback sort keys: PG emits window
    // rows ordered by the window PARTITION BY then ORDER BY when
    // the query itself has no outer ORDER BY.
    std::vector<size_t> finalPartitionColumns;
    size_t finalOrderColumn = tbl_.len();
    if (finalOrderBy_.empty() && !functions_.empty()) {
        for (const auto& pc : functions_.front().partitionBy) {
            const size_t c = windowColumnIndex(tbl_, pc);
            if (c < tbl_.len()) finalPartitionColumns.push_back(c);
        }
        if (!functions_.front().orderBy.empty()) {
            finalOrderColumn = windowColumnIndex(tbl_, functions_.front().orderBy);
            finalAsc = functions_.front().orderAscending;
            finalOrderNullsFirst_=functions_.front().hasExplicitOrderNulls
                ? functions_.front().orderByNullsFirst : !finalAsc;
        }
    } else if (!finalOrderBy_.empty()) {
        finalOrderColumn = windowColumnIndex(tbl_, finalOrderBy_);
    }
    finalOrderAscending_ = finalAsc;
    if (!finalOrderBy_.empty() && finalOrderColumn >= tbl_.len()) return false;
    for (size_t rowIndex = 0; rowIndex < input.size(); ++rowIndex) {
        std::string line;
        std::vector<std::string> cells;
        std::vector<bool> nulls;
        cells.reserve(targets_.size());
        nulls.reserve(targets_.size());
        for (size_t targetIndex = 0; targetIndex < targets_.size(); ++targetIndex) {
            const auto& target = targets_[targetIndex];
            std::string value;
            bool isNull = false;
            if (target.isWindow) {
                if (target.windowIndex >= functions_.size()) return false;
                value = computed[rowIndex][target.windowIndex];
                isNull = computedNulls[rowIndex][target.windowIndex];
            } else {
                const size_t column = windowColumnIndex(tbl_, target.column);
                if (column >= tbl_.len()) return false;
                value = input[rowIndex].values[column];
                isNull = input[rowIndex].nulls[column];
            }
            if (targetIndex != 0) line.push_back(' ');
            line += isNull ? "NULL" : value;
            cells.push_back(std::move(value));
            nulls.push_back(isNull);
        }
        OutputRow row;
        row.text = std::move(line);
        row.cells = std::move(cells);
        row.nulls = std::move(nulls);
        if (finalOrderColumn < tbl_.len()) {
            row.sortKey = input[rowIndex].values[finalOrderColumn];
            row.sortKeyNull = input[rowIndex].nulls[finalOrderColumn];
        }
        row.keys = input[rowIndex].values;
        row.keyNulls = input[rowIndex].nulls;
        output.push_back(std::move(row));
    }
    if (finalOrderColumn < tbl_.len() || !finalPartitionColumns.empty()) {
        interruptibleStableSort(output.begin(), output.end(), [&](const OutputRow& left, const OutputRow& right) {
            for (size_t column : finalPartitionColumns) {
                const int cmp = compareDeclaredWindowCell(tbl_.cols[column],
                    left.keys[column], left.keyNulls[column],
                    right.keys[column], right.keyNulls[column]);
                if (cmp != 0) return cmp < 0;
            }
            if (finalOrderColumn >= tbl_.len()) return false;
            if (left.sortKeyNull!=right.sortKeyNull)
                return left.sortKeyNull==finalOrderNullsFirst_;
            const int cmp = compareDeclaredWindowCell(tbl_.cols[finalOrderColumn],
                left.sortKey, left.sortKeyNull,
                right.sortKey, right.sortKeyNull);
            return finalOrderAscending_ ? cmp < 0 : cmp > 0;
        });
    }
    for (auto& row : output) {
        rows_.push_back(std::move(row.text));
        structuredRows_.push_back(std::move(row.cells));
        structuredNulls_.push_back(std::move(row.nulls));
    }
    return true;
}

bool WindowOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_++];
    rtInstr_.emitted = true;
    return true;
}

bool WindowOp::lastStructuredRow(std::vector<std::string>& cells,
                                 std::vector<bool>& nulls) const {
    if (pos_ == 0 || pos_ > structuredRows_.size()) return false;
    cells = structuredRows_[pos_ - 1];
    nulls = structuredNulls_[pos_ - 1];
    return true;
}

void WindowOp::close() {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
}

// ========================================================================
// SortOp
// ========================================================================

static size_t effectiveWorkMemKb() {
    const auto* session = currentSession();
    return session ? session->workMem.effective(g_config.workMemKb) : g_config.workMemKb;
}

static size_t sortWorkMemBytes() {
    const auto kilobytes = effectiveWorkMemKb();
    return kilobytes > std::numeric_limits<size_t>::max() / 1024
        ? std::numeric_limits<size_t>::max() : std::max<size_t>(1, kilobytes) * 1024;
}

static std::optional<PreparedIntegerKeyOrder> openedCompositeOrder(const Operator* child) {
    while (const auto* filter = dynamic_cast<const FilterOp*>(child)) child = filter->child();
    if (const auto* scan = dynamic_cast<const CompositePrimaryKeyScanOp*>(child))
        return scan->openedKeyOrder();
    return {};
}

struct SortOp::State {
    struct Row {
        std::string raw, key, nulls;
        ScanOrigin origin;
        int64_t integer = 0;
        double floating = 0;
        Date date;
        uint64_t ordinal = 0;
        bool isNull = false;
        size_t bytes() const {
            return sizeof(Row) + raw.capacity() + key.capacity() + nulls.capacity() +
                origin.dbname.capacity() + origin.tablename.capacity();
        }
    };
    struct RecordHeader {
        uint64_t raw = 0, key = 0, database = 0, table = 0, ordinal = 0, engine = 0;
        int64_t integer = 0, rid = 0;
        double floating = 0;
        int32_t year = 0, month = 0, day = 0;
        uint8_t isNull = 0;
    };
    struct Input {
        std::unique_ptr<sort_detail::Reader> reader;
        Row row;
        uint64_t left;
    };
    std::vector<Row> rows;
    size_t position = 0, dynamicBytes = 0, maximumRowBytes = 1;
    size_t budget, bufferSize, nullBytes;
    Column column;
    bool haveColumn, ascending, nullsFirst;
    bool presortedInput = false;
    uint64_t total = 0, runCount = 0, extent = 0;
    std::unique_ptr<sort_detail::File> input, scratch;
    std::unique_ptr<sort_detail::Writer> writer;
    std::vector<Input> finalHeads;
    std::vector<size_t> finalHeap;
    size_t pendingHead = std::numeric_limits<size_t>::max();
    uint64_t outputRows = 0;
    StorageEngine::NullRowBindingState callerNullBinding;

    State(const TableSchema& schema, size_t ordinal, bool asc, bool first)
        : budget(sortWorkMemBytes()),
          bufferSize(std::max<size_t>(64, std::min<size_t>(65536, budget / 16))),
          nullBytes((schema.len() + 7) / 8), haveColumn(ordinal < schema.len()),
          ascending(asc), nullsFirst(first),
          callerNullBinding(StorageEngine::captureNullRowBinding()) {
        if (haveColumn) column = schema.cols[ordinal];
    }
    bool less(const Row& a, const Row& b) const {
        if (a.isNull != b.isNull) return a.isNull == nullsFirst;
        if (!a.isNull && haveColumn) {
            bool lower = false, greater = false;
            if (column.dataType == "char" || column.isVariableLength) {
                lower = StorageEngine::compareValues(column, a.key, false, b.key, false, "<") ==
                    StorageEngine::PredicateTruth::True;
                if (!lower) greater = StorageEngine::compareValues(column, b.key, false, a.key, false, "<") ==
                    StorageEngine::PredicateTruth::True;
            } else if (column.dataType == "date") {
                lower = a.date < b.date; greater = b.date < a.date;
            } else if (column.dataType == "float" || column.dataType == "double" || column.dataType == "decimal") {
                const bool an = std::isnan(a.floating), bn = std::isnan(b.floating);
                lower = !an && (bn || a.floating < b.floating);
                greater = !bn && (an || b.floating < a.floating);
            } else {
                lower = a.integer < b.integer; greater = b.integer < a.integer;
            }
            if (lower) return ascending;
            if (greater) return !ascending;
        }
        // One total peer order is shared by in-memory runs and merge passes.
        // SQL without an additional ORDER key still has no promised peer order.
        return a.ordinal < b.ordinal;
    }
    void key(Row& row, std::string value) const {
        if (column.dataType == "char" || column.isVariableLength) row.key = std::move(value);
        else if (column.dataType == "date") row.date = value.empty() ? Date{} : Date(value.c_str());
        else if (column.storageCodec == Column::StorageCodec::ZonedClock) {
            const auto clock = temporal_storage_detail::parseZonedClock(value);
            if (clock) row.integer = temporal_storage_detail::zonedClockOrderKey(*clock);
            else if (!row.isNull) throw DbError("22007", "invalid time with time zone sort key");
        } else if (column.storageCodec == Column::StorageCodec::TemporalMicros)
            row.integer = value.empty() ? 0 : (column.dataType == "time"
                ? temporal_storage_detail::parseTimeMicros(value).value_or(0)
                : temporal_storage_detail::parseTimestampMicros(value).value_or(0));
        else if (column.dataType == "timestamp") row.integer = value.empty() ? 0 : parseTimestampToSeconds(value);
        else if (column.dataType == "float" || column.dataType == "double" || column.dataType == "decimal") {
            try { row.floating = std::stod(value); } catch (...) { row.floating = 0; }
        } else row.integer = value.empty() ? 0 : StorageEngine::parseInt(value);
    }
    void writeRow(const Row& row, sort_detail::Writer& target) const {
        RecordHeader header{};
        header.raw = row.raw.size(); header.key = row.key.size();
        header.database = row.origin.dbname.size(); header.table = row.origin.tablename.size();
        header.ordinal = row.ordinal; header.engine = reinterpret_cast<uintptr_t>(row.origin.engine);
        header.rid = row.origin.rid; header.integer = row.integer; header.floating = row.floating;
        header.year = row.date.year; header.month = row.date.month; header.day = row.date.day;
        header.isNull = row.isNull;
        target.scalar(header);
        target.append(row.raw.data(), row.raw.size()); target.append(row.key.data(), row.key.size());
        target.append(row.origin.dbname.data(), row.origin.dbname.size());
        target.append(row.origin.tablename.data(), row.origin.tablename.size());
        target.append(row.nulls.data(), row.nulls.size());
    }
    Row readRow(sort_detail::Reader& reader) const {
        const auto header = reader.scalar<RecordHeader>();
        uint64_t remaining = reader.remaining();
        for (const auto length : {header.raw, header.key, header.database, header.table, uint64_t(nullBytes)}) {
            if (length > remaining || length > std::numeric_limits<size_t>::max())
                throw DbError("58030", "invalid sort temporary record extent");
            remaining -= length;
        }
        if (header.isNull > 1 || header.engine > std::numeric_limits<uintptr_t>::max())
            throw DbError("58030", "invalid sort temporary record");
        Row row;
        row.raw = reader.string(header.raw); row.key = reader.string(header.key);
        row.origin.dbname = reader.string(header.database); row.origin.tablename = reader.string(header.table);
        row.nulls = reader.string(nullBytes); row.origin.engine = reinterpret_cast<StorageEngine*>(uintptr_t(header.engine));
        row.origin.rid = header.rid; row.integer = header.integer; row.floating = header.floating;
        row.date.year = header.year; row.date.month = header.month; row.date.day = header.day;
        row.ordinal = header.ordinal; row.isNull = header.isNull;
        return row;
    }
    sort_detail::RunHeader runHeader(uint64_t offset) const {
        if (offset > extent || extent - offset < sizeof(sort_detail::RunHeader))
            throw DbError("58030", "invalid sort temporary run header");
        sort_detail::Reader reader(input->fd(), offset, offset + sizeof(sort_detail::RunHeader), sizeof(sort_detail::RunHeader));
        auto header = reader.scalar<sort_detail::RunHeader>();
        if (header.bytes < sizeof(header) || header.bytes > extent - offset || !header.rows)
            throw DbError("58030", "invalid sort temporary run extent");
        return header;
    }
    void flushRun() {
        if (rows.empty()) return;
        interruptibleSort(rows.begin(), rows.end(), [&](const Row& a, const Row& b) { return less(a,b); });
        if (!input) {
            input = std::make_unique<sort_detail::File>();
            writer = std::make_unique<sort_detail::Writer>(input->fd(), bufferSize);
        }
        const auto start = writer->position();
        writer->scalar(sort_detail::RunHeader{});
        for (const auto& row : rows) { checkForQueryInterrupt(); writeRow(row, *writer); }
        const sort_detail::RunHeader header{writer->position() - start, rows.size()};
        writer->flush();
        sort_detail::writeAt(input->fd(), start, reinterpret_cast<const char*>(&header), sizeof(header));
        ++runCount; extent = writer->position();
        std::vector<Row>().swap(rows); dynamicBytes = 0;
    }
    void add(Row row) {
        maximumRowBytes = std::max(maximumRowBytes, row.bytes());
        const size_t extra = row.bytes() - sizeof(Row);
        // Include vector capacity, strings and the captured NULL bitmap.
        // Leave room for I/O buffers. A single oversized row is permitted;
        // it becomes its own run rather than accumulating more oversized rows.
        const size_t capacity = rows.size() < rows.capacity() ? rows.capacity() :
            rows.capacity() > std::numeric_limits<size_t>::max() / 2 ? rows.size() + 1 :
            std::max<size_t>(1, rows.capacity() * 2);
        const size_t allowance = budget - budget / 4;
        if (!rows.empty() && (capacity > allowance / sizeof(Row) ||
            extra > allowance - std::min(allowance, capacity * sizeof(Row)) ||
            dynamicBytes > allowance - std::min(allowance, capacity * sizeof(Row) + extra))) flushRun();
        if (rows.size() == rows.capacity()) {
            const size_t next = std::max<size_t>(1, rows.capacity() * 2);
            rows.reserve(next);
        }
        dynamicBytes += extra;
        rows.push_back(std::move(row)); ++total;
        if (rows.capacity() > allowance / sizeof(Row) ||
            dynamicBytes >= allowance - std::min(allowance, rows.capacity() * sizeof(Row))) flushRun();
    }
    void finish() {
        if (!runCount) {
            interruptibleSort(rows.begin(), rows.end(), [&](const Row& a,const Row& b) { return less(a,b); });
            return;
        }
        flushRun(); writer.reset();
        // Bounded k-way merge: at most16 heads, fewer for wide records. Run
        // headers stay on disk, not in an input-sized directory or fd array.
        const size_t fanIn = std::max<size_t>(2, std::min<size_t>(16,
            budget / std::max<size_t>(1, maximumRowBytes + bufferSize)));
        // A forward-only consumer can merge the final bounded set of runs
        // directly. Materializing that last pass would write and read every
        // output row once more, even if the consumer closes early.
        while (runCount > fanIn) {
            checkForQueryInterrupt();
            if (!scratch) scratch = std::make_unique<sort_detail::File>();
            else scratch->truncate();
            sort_detail::Writer target(scratch->fd(), bufferSize);
            uint64_t offset = 0, newRuns = 0;
            while (offset < extent) {
                std::vector<Input> heads;
                heads.reserve(fanIn);
                uint64_t count = 0;
                for (size_t i = 0; i < fanIn && offset < extent; ++i) {
                    const auto header = runHeader(offset);
                    auto reader = std::make_unique<sort_detail::Reader>(input->fd(),
                        offset + sizeof(header), offset + header.bytes, bufferSize);
                    auto row = readRow(*reader);
                    heads.push_back({std::move(reader), std::move(row), header.rows - 1});
                    offset += header.bytes; count += header.rows;
                }
                std::vector<size_t> heap;
                for (size_t i = 0; i < heads.size(); ++i) heap.push_back(i);
                const auto later = [&](size_t a,size_t b) { return less(heads[b].row,heads[a].row); };
                std::make_heap(heap.begin(),heap.end(),later);
                const auto start = target.position();
                target.scalar(sort_detail::RunHeader{});
                while (!heap.empty()) {
                    checkForQueryInterrupt();
                    std::pop_heap(heap.begin(),heap.end(),later);
                    const auto index = heap.back(); heap.pop_back();
                    auto& head = heads[index]; writeRow(head.row,target);
                    if (head.left) {
                        head.row = readRow(*head.reader); --head.left;
                        heap.push_back(index); std::push_heap(heap.begin(),heap.end(),later);
                    } else if (head.reader->remaining()) throw DbError("58030", "invalid sort temporary run row count");
                }
                const sort_detail::RunHeader header{target.position() - start,count};
                target.flush();
                sort_detail::writeAt(scratch->fd(),start,reinterpret_cast<const char*>(&header),sizeof(header));
                ++newRuns;
            }
            extent = target.position(); runCount = newRuns; input.swap(scratch);
        }
        scratch.reset();
        uint64_t offset = 0, count = 0;
        finalHeads.reserve(runCount); finalHeap.reserve(runCount);
        while (offset < extent && finalHeads.size() < runCount) {
            checkForQueryInterrupt();
            const auto header = runHeader(offset);
            if (header.rows > total - count)
                throw DbError("58030", "sort temporary output count mismatch");
            auto reader = std::make_unique<sort_detail::Reader>(input->fd(),
                offset + sizeof(header), offset + header.bytes, bufferSize);
            auto row = readRow(*reader);
            finalHeap.push_back(finalHeads.size());
            finalHeads.push_back({std::move(reader), std::move(row), header.rows - 1});
            offset += header.bytes; count += header.rows;
        }
        if (offset != extent || finalHeads.size() != runCount || count != total)
            throw DbError("58030", "sort temporary output count mismatch");
        const auto later = [&](size_t a, size_t b) { return less(finalHeads[b].row, finalHeads[a].row); };
        std::make_heap(finalHeap.begin(), finalHeap.end(), later);
        outputRows = total;
    }
    bool next(Row& row) {
        if (!input) {
            if (position == rows.size()) return false;
            row = std::move(rows[position++]); return true;
        }
        const auto later = [&](size_t a, size_t b) { return less(finalHeads[b].row, finalHeads[a].row); };
        // Refill only when another row is requested. The previously returned
        // row must not depend on reading its successor from temporary storage.
        if (pendingHead != std::numeric_limits<size_t>::max()) {
            auto& head = finalHeads[pendingHead];
            if (head.left) {
                head.row = readRow(*head.reader); --head.left;
                finalHeap.push_back(pendingHead);
                std::push_heap(finalHeap.begin(), finalHeap.end(), later);
            } else if (head.reader->remaining())
                throw DbError("58030", "invalid sort temporary run row count");
            pendingHead = std::numeric_limits<size_t>::max();
        }
        if (finalHeap.empty()) {
            if (outputRows) throw DbError("58030", "sort temporary output count mismatch");
            return false;
        }
        if (!outputRows) throw DbError("58030", "sort temporary output count mismatch");
        std::pop_heap(finalHeap.begin(), finalHeap.end(), later);
        pendingHead = finalHeap.back(); finalHeap.pop_back();
        row = std::move(finalHeads[pendingHead].row); --outputRows; return true;
    }
};

SortOp::SortOp(OpPtr child, const TableSchema& tbl,
                const std::string& orderByCol, bool asc,
                bool nullsFirst, bool hasExplicitNullOrder)
    : child_(std::move(child)), tbl_(tbl), orderByCol_(orderByCol), asc_(asc),
      nullsFirst_(hasExplicitNullOrder ? nullsFirst : !asc) {}

SortOp::~SortOp() { try { close(); } catch (...) {} }

bool SortOp::open() {
    OpenInstrument startup(this);
    close(); clearError();
    size_t sortIndex = tbl_.len();
    for (size_t i = 0; i < tbl_.len(); ++i)
        if (tbl_.cols[i].dataName == orderByCol_) { sortIndex = i; break; }
    state_ = std::make_unique<State>(tbl_,sortIndex,asc_,nullsFirst_);
    try {
        childOpened_ = true;
        if (!child_->open()) {
            propagateChildError(child_.get(), "sort child could not open");
            try { close(); } catch (...) {} // Keep the child's diagnostic primary.
            return false;
        }
        const auto order = openedCompositeOrder(child_.get());
        if (order && order->columnOrdinal == sortIndex && order->ascending == asc_) {
            state_->presortedInput = true;
            return true;
        }
        std::string raw;
        while (child_->next(raw)) {
            checkForQueryInterrupt();
            State::Row row; row.raw = std::move(raw); row.origin = child_->scanOrigin();
            if (!row.origin.engine) StorageEngine::unbindNullRow();
            else StorageEngine::bindNullRow(row.origin.engine,row.origin.dbname,row.origin.tablename,row.origin.rid,tbl_.len());
            std::vector<bool> nulls;
            if (!child_->lastStoredNullBitmap(nulls) || nulls.size() != tbl_.len()) {
                nulls.assign(tbl_.len(),false);
                for (size_t i = 0; i < tbl_.len(); ++i)
                    nulls[i] = row.origin.engine ? child_->lastColumnIsNull(i) :
                        child_->lastColumnIsNull(i) || rawColumnIsNull(row.raw,tbl_,i);
            }
            row.nulls.assign(state_->nullBytes,'\0');
            for (size_t i = 0; i < nulls.size(); ++i)
                if (nulls[i]) row.nulls[i/8] |= static_cast<char>(1U << (i%8));
            if (sortIndex < tbl_.len()) {
                row.isNull = nulls[sortIndex];
                state_->key(row,StorageEngine::extractColumnValueStatic(row.raw,tbl_,sortIndex));
            }
            row.ordinal = state_->total;
            if (state_->total == std::numeric_limits<uint64_t>::max()) throw DbError("54000", "too many sort input rows");
            state_->add(std::move(row));
        }
        if (child_->hasError()) {
            propagateChildError(child_.get(),"sort child failed");
            try { close(); } catch (...) {}
            return false;
        }
        childOpened_ = false; child_->close();
        state_->finish();
        return true;
    } catch (...) {
        const auto failure = std::current_exception();
        try { close(); } catch (...) {}
        std::rethrow_exception(failure);
    }
}

bool SortOp::next(std::string& outRow) {
    NextInstrument instrumentation(this);
    try {
    checkForQueryInterrupt();
    if (state_ && state_->presortedInput) {
        if (!child_->next(outRow)) {
            if (child_->hasError()) return propagateChildError(child_.get(), "ordered sort child failed");
            return false;
        }
        lastOrigin_ = child_->scanOrigin();
        rowAvailable_ = true;
        instrumentation.emitted = true;
        return true;
    }
    State::Row row;
    if (!state_ || !state_->next(row)) return false;
    lastOrigin_ = std::move(row.origin); lastNulls_.assign(tbl_.len(),false);
    for (size_t i = 0; i < tbl_.len(); ++i)
        lastNulls_[i] = (static_cast<unsigned char>(row.nulls[i/8]) & (1U << (i%8))) != 0;
    if (lastOrigin_.engine && lastOrigin_.rid > 0)
        StorageEngine::bindNullRow(lastOrigin_.engine,lastOrigin_.dbname,lastOrigin_.tablename,lastOrigin_.rid,tbl_.len());
    else StorageEngine::unbindNullRow();
    outRow = std::move(row.raw); rowAvailable_ = true; instrumentation.emitted = true;
    return true;
    } catch (...) {
        const auto failure = std::current_exception();
        try { close(); } catch (...) {}
        std::rethrow_exception(failure);
    }
}

void SortOp::close() {
    std::optional<StorageEngine::NullRowBindingState> caller;
    if (state_) caller = std::move(state_->callerNullBinding);
    state_.reset(); rowAvailable_ = false; lastOrigin_ = {}; lastNulls_.clear();
    std::exception_ptr failure;
    if (childOpened_) {
        childOpened_ = false;
        try { child_->close(); } catch (...) { failure = std::current_exception(); }
    }
    if (caller) StorageEngine::restoreNullRowBinding(std::move(*caller));
    if (failure) std::rethrow_exception(failure);
}

bool SortOp::lastColumnIsNull(size_t colIdx) const {
    if (rowAvailable_ && state_ && state_->presortedInput) return child_->lastColumnIsNull(colIdx);
    return rowAvailable_ && colIdx < lastNulls_.size() && lastNulls_[colIdx];
}
bool SortOp::lastStoredNullBitmap(std::vector<bool>& nulls) const {
    if (!rowAvailable_) return false;
    if (state_ && state_->presortedInput) return child_->lastStoredNullBitmap(nulls);
    nulls = lastNulls_; return true;
}

// ========================================================================
// LimitOp / OffsetOp
// ========================================================================

LimitOp::LimitOp(OpPtr child, size_t limit)
    : child_(std::move(child)), limit_(limit) {}

bool LimitOp::open() {
    OpenInstrument startup(this);
    clearError(); count_ = 0; childOpened_ = false;
    // LIMIT 0 is no execution demand, including child startup. A sort or
    // OFFSET can evaluate volatile expressions during open().
    if (!limit_) return true;
    childOpened_ = true;
    return child_->open();
}

bool LimitOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (count_ >= limit_) return false;
    if (!child_->next(outRow)) {
        if (child_->hasError()) return propagateChildError(child_.get(), "limit child failed");
        return false;
    }
    ++count_;
    rtInstr_.emitted = true;
    return true;
}

void LimitOp::close() {
    count_ = 0;
    if (childOpened_) child_->close();
    childOpened_ = false;
}

OffsetOp::OffsetOp(OpPtr child, size_t offset)
    : child_(std::move(child)), offset_(offset) {}

bool OffsetOp::open() {
    OpenInstrument startup(this);
    skipped_ = 0;
    if (!child_->open()) return false;
    auto* projection = dynamic_cast<PreparedProjectOp*>(child_.get());
    const auto skipped = projection ? projection->skipUnprojectedRows(offset_) : std::nullopt;
    if (skipped) skipped_ = *skipped;
    else {
        std::string ignored;
        while (skipped_ < offset_ && child_->next(ignored)) ++skipped_;
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "offset child failed");
    return true;
}

bool OffsetOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (child_->next(outRow)) { rtInstr_.emitted = true; return true; }
    if (child_->hasError()) return propagateChildError(child_.get(), "offset child failed");
    return false;
}

void OffsetOp::close() {
    skipped_ = 0;
    child_->close();
}

// ========================================================================
// DistinctOp
// ========================================================================

bool LimitOp::storedOutputRoles(std::vector<StoredReferenceOutputRole>& roles) const {
    roles.clear();
    return typeid(*this) == typeid(LimitOp) && child_->storedOutputRoles(roles);
}

bool OffsetOp::storedOutputRoles(std::vector<StoredReferenceOutputRole>& roles) const {
    roles.clear();
    return typeid(*this) == typeid(OffsetOp) && child_->storedOutputRoles(roles);
}

bool DistinctOp::storedOutputRoles(std::vector<StoredReferenceOutputRole>& roles) const {
    roles.clear();
    return typeid(*this) == typeid(DistinctOp) && child_->storedOutputRoles(roles);
}

DistinctOp::DistinctOp(OpPtr child) : child_(std::move(child)) {}

bool DistinctOp::open() {
    clearError();
    seen_.clear();
    return child_->open();
}

bool DistinctOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    while (child_->next(outRow)) {
        std::string key = outRow;
        if (const auto* project = dynamic_cast<const ProjectOp*>(child_.get())) {
            if (!project->lastTypedRowKey(key)) {
                setError("DISTINCT projection lost typed row metadata");
                return false;
            }
        } else if (child_->supportsStructuredRows()) {
            std::vector<std::string> cells;
            std::vector<bool> nulls;
            if (!child_->lastStructuredRow(cells, nulls) || cells.size() != nulls.size()) {
                setError("DISTINCT child lost structured row metadata");
                return false;
            }
            key.clear();
            Column text;
            text.dataType = "text";
            for (size_t i = 0; i < cells.size(); ++i)
                key += StorageEngine::groupingValueKey(text, cells[i], nulls[i]);
        }
        if (seen_.insert(key).second) { rtInstr_.emitted = true; return true; }
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "distinct child failed");
    return false;
}

void DistinctOp::close() {
    seen_.clear();
    child_->close();
}

// ========================================================================
// SetOperationOp
// ========================================================================

SetOperationOp::SetOperationOp(OpPtr left, OpPtr right,
                               SetOperationType type, bool all)
    : left_(std::move(left)), right_(std::move(right)), type_(type), all_(all) {}

bool SetOperationOp::open() {
    rows_.clear();
    pos_ = 0;
    if (!left_ || !right_ || !left_->open()) return false;
    if (!right_->open()) {
        left_->close();
        return false;
    }

    std::vector<std::string> leftRows;
    std::vector<std::string> rightRows;
    std::string row;
    while (left_->next(row)) leftRows.push_back(row);
    while (right_->next(row)) rightRows.push_back(row);
    if (left_->hasError()) return propagateChildError(left_.get(), "set operation left child failed");
    if (right_->hasError()) return propagateChildError(right_.get(), "set operation right child failed");

    if (type_ == SetOperationType::Union) {
        if (all_) {
            rows_ = std::move(leftRows);
            rows_.insert(rows_.end(), rightRows.begin(), rightRows.end());
            return true;
        }
        std::set<std::string> seen;
        for (const auto& candidate : leftRows) {
            if (seen.insert(candidate).second) rows_.push_back(candidate);
        }
        for (const auto& candidate : rightRows) {
            if (seen.insert(candidate).second) rows_.push_back(candidate);
        }
        return true;
    }

    std::map<std::string, size_t> rightCounts;
    for (const auto& candidate : rightRows) ++rightCounts[candidate];
    std::set<std::string> emitted;
    for (const auto& candidate : leftRows) {
        auto it = rightCounts.find(candidate);
        const size_t available = it == rightCounts.end() ? 0 : it->second;
        if (type_ == SetOperationType::Intersect) {
            if (available == 0) continue;
            if (all_) {
                rows_.push_back(candidate);
                --it->second;
            } else if (emitted.insert(candidate).second) {
                rows_.push_back(candidate);
            }
            continue;
        }

        // EXCEPT: a row survives only when the right side has no remaining
        // matching occurrence.  EXCEPT ALL consumes one right occurrence.
        if (available > 0) {
            if (all_) --it->second;
            continue;
        }
        if (all_ || emitted.insert(candidate).second) rows_.push_back(candidate);
    }
    return true;
}

bool SetOperationOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_++];
    rtInstr_.emitted = true;
    return true;
}

void SetOperationOp::close() {
    rows_.clear();
    pos_ = 0;
    if (left_) left_->close();
    if (right_) right_->close();
}

// ========================================================================
// NestedLoopJoinOp
// ========================================================================

static size_t findJoinColumnIndex(const TableSchema& tbl,
                                  const std::string& colName) {
    for (size_t i = 0; i < tbl.len(); ++i) {
        if (tbl.cols[i].dataName == colName) return i;
    }
    return tbl.len();
}

NestedLoopJoinOp::NestedLoopJoinOp(StorageEngine* engine, const std::string& dbname,
                                    OpPtr left, OpPtr right,
                                    const std::string& leftTable,
                                    const std::string& rightTable,
                                    const std::string& leftCol,
                                    const std::string& rightCol)
    : engine_(engine), dbname_(dbname),
      left_(std::move(left)), right_(std::move(right)),
      leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol) {}

bool NestedLoopJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);
    leftColIdx_ = findJoinColumnIndex(leftTbl_, leftCol_);
    rightColIdx_ = findJoinColumnIndex(rightTbl_, rightCol_);
    if (leftColIdx_ >= leftTbl_.len() || rightColIdx_ >= rightTbl_.len()) {
        setError("nested-loop join column does not exist");
        return false;
    }
    if (!left_->open()) return false;
    hasLeft_ = left_->next(curLeftRow_);
    if (left_->hasError()) return propagateChildError(left_.get(), "nested-loop left child failed");
    curLeftKeyNull_ = hasLeft_ && left_->lastColumnIsNull(leftColIdx_);
    return right_->open();
}

bool NestedLoopJoinOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    std::string rightRow;
    while (hasLeft_) {
        while (right_->next(rightRow)) {
            if (curLeftKeyNull_ || right_->lastColumnIsNull(rightColIdx_)) {
                continue;
            }
            const std::string leftKey = StorageEngine::extractColumnValueStatic(
                curLeftRow_, leftTbl_, leftColIdx_);
            const std::string rightKey = StorageEngine::extractColumnValueStatic(
                rightRow, rightTbl_, rightColIdx_);
            if (leftKey == rightKey) {
                outRow = curLeftRow_ + rightRow;
                rtInstr_.emitted = true;
                return true;
            }
        }
        right_->close();
        hasLeft_ = left_->next(curLeftRow_);
        if (left_->hasError()) return propagateChildError(left_.get(), "nested-loop left child failed");
        curLeftKeyNull_ = hasLeft_ && left_->lastColumnIsNull(leftColIdx_);
        if (hasLeft_) right_->open();
    }
    return false;
}

void NestedLoopJoinOp::close() {
    left_->close();
    right_->close();
}

// ========================================================================
// Helper: extract join key value from raw row data as string
// ========================================================================
static std::string extractJoinKey(const std::string& row, const TableSchema& tbl, const std::string& colName) {
    size_t colIdx = findJoinColumnIndex(tbl, colName);
    if (colIdx >= tbl.len()) return "";
    return StorageEngine::extractColumnValueStatic(row, tbl, colIdx);
}

// ========================================================================
// HashJoinOp
// ========================================================================

HashJoinOp::HashJoinOp(StorageEngine* engine, const std::string& dbname,
                       OpPtr left, OpPtr right,
                       const std::string& leftTable, const std::string& rightTable,
                       const std::string& leftCol, const std::string& rightCol)
    : engine_(engine), dbname_(dbname),
      left_(std::move(left)), right_(std::move(right)),
      leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol) {}

bool HashJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);
    leftColIdx_ = findJoinColumnIndex(leftTbl_, leftCol_);
    rightColIdx_ = findJoinColumnIndex(rightTbl_, rightCol_);
    if (leftColIdx_ >= leftTbl_.len() || rightColIdx_ >= rightTbl_.len()) {
        setError("hash join column does not exist");
        return false;
    }
    rightHash_.clear();

    // Build hash table from right table
    if (!right_->open()) return false;
    std::string rightRow;
    while (right_->next(rightRow)) {
        if (right_->lastColumnIsNull(rightColIdx_)) continue;
        std::string key = extractJoinKey(rightRow, rightTbl_, rightCol_);
        rightHash_[key].push_back(std::move(rightRow));
    }
    if (right_->hasError()) return propagateChildError(right_.get(), "hash join right child failed");
    right_->close();

    // Start left table iteration
    if (!left_->open()) return false;
    hasLeft_ = left_->next(curLeftRow_);
    if (left_->hasError()) return propagateChildError(left_.get(), "hash join left child failed");
    curLeftKeyNull_ = hasLeft_ && left_->lastColumnIsNull(leftColIdx_);
    matchPos_ = 0;
    curRightMatches_.clear();
    if (hasLeft_ && !curLeftKeyNull_) {
        std::string key = extractJoinKey(curLeftRow_, leftTbl_, leftCol_);
        auto it = rightHash_.find(key);
        if (it != rightHash_.end()) curRightMatches_ = it->second;
    }
    return true;
}

// ========================================================================
// Collection aggregates share one implementation across serial and parallel
// grouping. Keep values and NULL bits separate all the way to finalization.
// ========================================================================
template <typename InputRow>
static std::string collectionAggregate(
    const TableSchema& table, const std::vector<InputRow>& input,
    const std::vector<size_t>& rowIds, const StorageEngine::AggItem& item,
    const std::string& function, bool* resultIsNull = nullptr) {
    SQLParser parser;
    auto parsed = parser.parse("SELECT " + function + "(" + item.arg + ")");
    const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
    const auto* call = select && select->selectList.size() == 1
        ? dynamic_cast<const FunctionCallExpr*>(select->selectList[0].expr.get())
        : nullptr;
    if (!parsed.success || !call) {
        throw std::runtime_error("invalid collection aggregate (SQLSTATE 42601)");
    }
    const bool array = function == "array_agg";
    if (call->args.size() != (array ? 1u : 2u)) {
        throw std::runtime_error("invalid aggregate argument count (SQLSTATE 42883)");
    }
    const std::string orderSql = item.orderBy.empty() ? call->orderBy : item.orderBy;
    auto parsedOrder = parser.parse("SELECT 1" +
        (orderSql.empty() ? std::string{} : " ORDER BY " + orderSql));
    const auto* order = dynamic_cast<const SelectStmt*>(parsedOrder.stmt.get());
    if (!parsedOrder.success || !order) {
        throw std::runtime_error("invalid aggregate ORDER BY (SQLSTATE 42601)");
    }
    ExprEvaluator evaluator;
    const auto filters = StorageEngine::parseConditions(item.filterConds);
    struct Entry {
        std::vector<ExprValue> args;
        std::vector<ExprValue> keys;
    };
    std::vector<Entry> entries;
    for (size_t rowId : rowIds) {
        const auto& row = input[rowId];
        bool passes = true;
        for (const auto& filter : filters) {
            size_t column = 0;
            while (column < table.len() && table.cols[column].dataName != filter.colName)
                ++column;
            const bool isNull = column < table.len() && row.nulls[column];
            const bool matched = filter.op == "isnull" ? isNull :
                filter.op == "isnotnull" ? !isNull :
                !isNull && StorageEngine::evalConditionOnRow(filter, row.raw, table);
            if (!matched) {
                passes = false;
                break;
            }
        }
        if (!passes) continue;
        RowContext context;
        for (size_t i = 0; i < table.len(); ++i) {
            const ExprValue value(table.cols[i].dataType, row.values[i], row.nulls[i]);
            context.set(table.cols[i].dataName, value);
            context.set(table.tablename + "." + table.cols[i].dataName, value);
        }
        Entry entry;
        const auto evaluate = [&](const Expr* expression) {
            auto value = evaluator.eval(expression, context);
            if (value.isUnknown())
                throw std::runtime_error("unsupported aggregate expression (SQLSTATE 0A000)");
            return value;
        };
        for (const auto& argument : call->args) entry.args.push_back(evaluate(argument.get()));
        if (!array && entry.args[0].isNull) continue;
        for (const auto& key : order->orderBy) entry.keys.push_back(evaluate(key.expr.get()));
        entries.push_back(std::move(entry));
    }
    // Use typed SQL comparison: text '10' sorts before '2', integer 10 after 2.
    BinaryOpExpr less;
    less.op = "<";
    auto left = std::make_unique<ColumnRefExpr>();
    left->column = "sort_left";
    auto right = std::make_unique<ColumnRefExpr>();
    right->column = "sort_right";
    less.left = std::move(left);
    less.right = std::move(right);
    interruptibleStableSort(entries.begin(), entries.end(), [&](const Entry& a, const Entry& b) {
        for (size_t i = 0; i < order->orderBy.size(); ++i) {
            const auto& key = order->orderBy[i];
            const auto& av = a.keys[i];
            const auto& bv = b.keys[i];
            if (av.isNull || bv.isNull) {
                if (av.isNull != bv.isNull) return av.isNull == key.nullsFirst;
                continue;
            }
            RowContext context;
            context.set("sort_left", av);
            context.set("sort_right", bv);
            if (evaluator.eval(&less, context).asBool()) return key.asc;
            context.set("sort_left", bv);
            context.set("sort_right", av);
            if (evaluator.eval(&less, context).asBool()) return !key.asc;
        }
        return false;
    });
    // PostgreSQL implements DISTINCT aggregates by sorting their aggregate
    // arguments before transition/deduplication when there is no explicit
    // aggregate ORDER BY.  Preserving scan order here made results such as
    // array_agg(DISTINCT int_column) observably incompatible.
    if (call->distinct && order->orderBy.empty()) {
        interruptibleStableSort(entries.begin(), entries.end(), [&](const Entry& a,
                                                              const Entry& b) {
            const size_t count = std::min(a.args.size(), b.args.size());
            for (size_t i = 0; i < count; ++i) {
                const auto& av = a.args[i];
                const auto& bv = b.args[i];
                if (av.isNull || bv.isNull) {
                    if (av.isNull != bv.isNull) return !av.isNull; // NULLS LAST
                    continue;
                }
                RowContext context;
                context.set("sort_left", av);
                context.set("sort_right", bv);
                if (evaluator.eval(&less, context).asBool()) return true;
                context.set("sort_left", bv);
                context.set("sort_right", av);
                if (evaluator.eval(&less, context).asBool()) return false;
            }
            return a.args.size() < b.args.size();
        });
    }
    const auto quoteElement = [](const ExprValue& value) {
        if (value.isNull) return std::string("NULL");
        std::string lower = value.value;
        for (char& ch : lower) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        const bool quote = value.value.empty() || lower == "null" ||
            value.value.find_first_of(",{}\"\\ \t\n\r\f\v") != std::string::npos;
        if (!quote) return value.value;
        std::string result = "\"";
        for (char ch : value.value) {
            if (ch == '\\' || ch == '"') result += '\\';
            result += ch;
        }
        return result + '"';
    };
    std::set<std::string> seen;
    std::string result;
    bool first = true;
    for (const auto& entry : entries) {
        if (call->distinct) {
            std::string key;
            for (const auto& value : entry.args)
                key += value.isNull ? "N;" : "V" + std::to_string(value.value.size()) + ":" + value.value;
            if (!seen.insert(key).second) continue;
        }
        if (!first) result += array ? "," : (entry.args[1].isNull ? "" : entry.args[1].value);
        result += array ? quoteElement(entry.args[0]) : entry.args[0].value;
        first = false;
    }
    if (first) {
        if (resultIsNull) *resultIsNull = true;
        return "NULL";
    }
    if (resultIsNull) *resultIsNull = false;
    return array ? "{" + result + "}" : result;
}

template <typename InputRow>
static std::vector<std::vector<ExprValue>> materializeGroupingKeys(
    const TableSchema& table, const std::vector<InputRow>& input,
    const std::vector<std::string>& keys) {
    std::vector<ParseResult> parsed;
    SQLParser parser;
    for (const auto& key : keys) {
        auto result = parser.parse("SELECT " + key);
        const auto* select = dynamic_cast<const SelectStmt*>(result.stmt.get());
        if (!result.success || !select || select->selectList.size() != 1 ||
            !select->selectList[0].expr) {
            throw std::runtime_error("invalid GROUP BY expression (SQLSTATE 42601)");
        }
        parsed.push_back(std::move(result));
    }
    // Evaluate before starting workers: expression errors are query failures,
    // never uncaught exceptions in a worker or silently-empty grouping keys.
    ExprEvaluator evaluator;
    std::vector<std::vector<ExprValue>> result;
    result.reserve(input.size());
    for (const auto& row : input) {
        RowContext context;
        for (size_t i = 0; i < table.len(); ++i) {
            const ExprValue value(table.cols[i].dataType, row.values[i], row.nulls[i]);
            context.set(table.cols[i].dataName, value);
            context.set(table.tablename + "." + table.cols[i].dataName, value);
        }
        std::vector<ExprValue> values;
        for (const auto& expression : parsed) {
            const auto* select = static_cast<const SelectStmt*>(expression.stmt.get());
            auto value = evaluator.eval(select->selectList[0].expr, context);
            if (value.isUnknown())
                throw std::runtime_error("unsupported GROUP BY expression (SQLSTATE 0A000)");
            values.push_back(std::move(value));
        }
        result.push_back(std::move(values));
    }
    return result;
}

static std::string encodeGroupingKey(const ExprValue& value) {
    Column column;
    column.dataType = SQLParser::toLower(value.typeName);
    if (column.dataType == "double precision") column.dataType = "double";
    if (column.dataType == "real") column.dataType = "float";
    return StorageEngine::groupingValueKey(column, value.value, value.isNull);
}

// ========================================================================
// ParallelGroupAggregateOp
// ========================================================================
static bool exactAggregateColumn(const TableSchema& table, size_t index) {
    if (index >= table.len() || table.cols[index].isArray) return false;
    const auto* type = TypeRegistry::instance().findType(table.cols[index].dataType);
    if (!type) return false;
    const std::string& name = type->canonicalName;
    return name == "smallint" || name == "integer" ||
           name == "bigint" || name == "numeric";
}

ParallelGroupAggregateOp::ParallelGroupAggregateOp(
    OpPtr child, const TableSchema& tbl,
    const std::vector<std::string>& groupByCols,
    const std::vector<StorageEngine::AggItem>& items,
    const std::vector<std::string>& havingConds, int workers)
    : child_(std::move(child)), tbl_(tbl), groupByCols_(groupByCols),
      items_(items), havingConds_(havingConds),
      workers_(workers < 1 ? 1 : workers) {}

bool ParallelGroupAggregateOp::open() try {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
    usedParallelWorkers_ = false;
    if (!child_->open()) return false;
    const auto expressionOrigin = child_->scanOrigin();
    ExprEvaluator aggregateComparator;
    aggregateComparator.setCurrentDB(expressionOrigin.dbname);

    struct InputRow {
        std::string raw;
        std::vector<std::string> values;
        std::vector<bool> nulls;
    };
    std::vector<InputRow> input;
    std::string raw;
    while (child_->next(raw)) {
        InputRow row;
        row.raw = std::move(raw);
        row.values.reserve(tbl_.len());
        for (size_t i = 0; i < tbl_.len(); ++i) {
            row.values.push_back(StorageEngine::extractColumnValueStatic(row.raw, tbl_, i));
            row.nulls.push_back(child_->lastColumnIsNull(i));
        }
        input.push_back(std::move(row));
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "aggregate child failed");
    child_->close();

    auto columnIndex = [&](const std::string& name) {
        for (size_t i = 0; i < tbl_.len(); ++i) {
            if (tbl_.cols[i].dataName == name) return i;
        }
        return tbl_.len();
    };
    const auto groupingKeys = materializeGroupingKeys(tbl_, input, groupByCols_);
    const auto groupKeyValue = [&](size_t rowId, size_t keyPos) -> const ExprValue& {
        return groupingKeys.at(rowId).at(keyPos);
    };

    // ---- parallel partition into local group buckets ----
    using Buckets = std::map<std::string, std::vector<size_t>>;
    const size_t n = input.size();
    int activeWorkers = 1;
    // Workers only partition the already-buffered input rows — no engine
    // calls inside the threads — so this is safe during transactions too.
    if (workers_ > 1 && n >= 256) {
        activeWorkers = static_cast<int>(std::min<size_t>(
            static_cast<size_t>(workers_),
            (n + 255) / 256));
    }
    std::vector<Buckets> localB(static_cast<size_t>(activeWorkers));
    if (activeWorkers > 1) {
        usedParallelWorkers_ = true;
        const auto interruptState = currentQueryInterruptState();
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(activeWorkers));
        const size_t chunk = (n + activeWorkers - 1) / activeWorkers;
        for (int w = 0; w < activeWorkers; ++w) {
            const size_t begin = static_cast<size_t>(w) * chunk;
            const size_t end = std::min(n, begin + chunk);
            threads.emplace_back([&, interruptState, w, begin, end]() {
                setCurrentQueryInterruptState(interruptState);
                auto& buckets = localB[static_cast<size_t>(w)];
                for (size_t rowId = begin; rowId < end; ++rowId) {
                    if (queryInterruptPending()) break;
                    std::string key;
                    for (size_t ki = 0; ki < groupByCols_.size(); ++ki) {
                        const auto& value = groupKeyValue(rowId, ki);
                        key += encodeGroupingKey(value);
                    }
                    buckets[key].push_back(rowId);
                }
                setCurrentQueryInterruptState(nullptr);
            });
        }
        for (auto& t : threads) t.join();
        checkForQueryInterrupt();
    } else {
        for (size_t rowId = 0; rowId < n; ++rowId) {
            std::string key;
            for (size_t ki = 0; ki < groupByCols_.size(); ++ki) {
                const auto& value = groupKeyValue(rowId, ki);
                key += encodeGroupingKey(value);
            }
            localB[0][key].push_back(rowId);
        }
    }

    // ---- merge local buckets (order preserved within each group) ----
    Buckets groups;
    if (groupByCols_.empty() && input.empty()) groups[""] = {};
    for (const auto& buckets : localB) {
        for (const auto& kv : buckets) {
            auto& merged = groups[kv.first];
            merged.insert(merged.end(), kv.second.begin(), kv.second.end());
        }
    }

    // ---- finalize each group on this thread (aggregates + HAVING) ----
    static const std::set<std::string> supported = {
        "count", "sum", "avg", "min", "max", "bool_and", "bool_or", "every",
        "string_agg", "array_agg"
    };
    auto parseNumber = [](const std::string& value, long double& out) {
        try {
            size_t consumed = 0;
            out = std::stold(value, &consumed);
            return consumed == value.size();
        } catch (...) {
            return false;
        }
    };
    auto formatNumber = [](long double value) {
        if (std::floor(value) == value &&
            value >= static_cast<long double>(std::numeric_limits<int64_t>::min()) &&
            value <= static_cast<long double>(std::numeric_limits<int64_t>::max())) {
            return std::to_string(static_cast<int64_t>(value));
        }
        std::ostringstream out;
        out << std::setprecision(15) << static_cast<double>(value);
        return out.str();
    };
    auto computeAggregate = [&](const std::vector<size_t>& rowIds,
                                const StorageEngine::AggItem& item,
                                bool* resultIsNull = nullptr) -> std::string {
        std::string func = item.func;
        for (char& c : func) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (func == "string_agg" || func == "array_agg")
            return collectionAggregate(
                tbl_, input, rowIds, item, func, resultIsNull);
        const bool distinct = func == "count" && item.arg.size() > 9 &&
            item.arg.substr(0, 9) == "distinct ";
        std::string arg = distinct ? item.arg.substr(9) : item.arg;
        const size_t argIndex = arg == "*" ? tbl_.len() : columnIndex(arg);
        std::optional<aggregate_type_detail::BooleanArgument> booleanArgument;
        if (argIndex >= tbl_.len() && arg != "*" && !arg.empty() &&
            (func == "bool_and" || func == "bool_or" || func == "every"))
            booleanArgument.emplace(tbl_, expressionOrigin.engine, expressionOrigin.dbname, arg);
        // Aggregate over an arithmetic expression ("sum(amt * 2)"): resolve
        // operands per row when the arg is not a bare column.
        const bool argIsExpr = argIndex >= tbl_.len() && arg != "*" &&
            (func == "sum" || func == "avg" || func == "min" ||
             func == "max" || func == "count") && !arg.empty();
        auto exprVal = [&](const std::string& raw,
                           const std::vector<std::string>& rowVals) -> std::string {
            double acc = 0.0;
            bool accSet = false;
            char pendingOp = 0;
            std::string token;
            auto resolve = [&](const std::string& tok) -> std::string {
                for (size_t ci = 0; ci < tbl_.len(); ++ci)
                    if (tbl_.cols[ci].dataName == tok) return rowVals[ci];
                if (tok.size() >= 2 && tok.front() == '\'' && tok.back() == '\'')
                    return tok.substr(1, tok.size() - 2);
                return tok;
            };
            auto flush = [&]() {
                if (token.empty()) return;
                std::string v = resolve(token);
                if (!accSet) {
                    try { acc = std::stod(v); accSet = true; }
                    catch (...) { token = "\x01"; return; }
                } else {
                    double rhs = 0.0;
                    try { rhs = std::stod(v); }
                    catch (...) { token = "\x01"; return; }
                    switch (pendingOp) {
                        case '+': acc += rhs; break;
                        case '-': acc -= rhs; break;
                        case '*': acc *= rhs; break;
                        case '/': if (rhs == 0) { token = "\x01"; return; } acc /= rhs; break;
                        case '%': { auto l = static_cast<int64_t>(acc), r = static_cast<int64_t>(rhs);
                                   if (r == 0) { token = "\x01"; return; }
                                   acc = static_cast<double>(l % r); break; }
                        default: token = "\x01"; return;
                    }
                }
                token.clear();
            };
            for (char ch : raw) {
                if (ch == ' ') { flush(); if (token == "\x01") return ""; continue; }
                if (accSet && pendingOp == 0 &&
                    std::string("+-*/%").find(ch) != std::string::npos && !token.empty()) {
                    flush();
                    if (token == "\x01") return "";
                    pendingOp = ch;
                    continue;
                }
                token += ch;
            }
            flush();
            if (token == "\x01" || !accSet) return "";
            if (acc == static_cast<double>(static_cast<int64_t>(acc)))
                return std::to_string(static_cast<int64_t>(acc));
            std::string s = std::to_string(acc);
            s.erase(s.find_last_not_of('0') + 1, std::string::npos);
            if (!s.empty() && s.back() == '.') s.pop_back();
            return s;
        };
        const auto filters = StorageEngine::parseConditions(item.filterConds);
        std::set<std::string> distinctValues;
        int64_t count = 0;
        long double sum = 0;
        dbms::Numeric exactSum(0);
        bool exactSumOk = true;
        const bool exactInput = !argIsExpr && exactAggregateColumn(tbl_, argIndex);
        bool hasValue = false;
        std::string selected;
        bool boolSeen = false;
        bool boolValue = func == "bool_and" || func == "every";
        std::optional<DeclaredAggregateLess> declaredOrder;
        if ((func == "min" || func == "max") && argIndex < tbl_.len())
            declaredOrder.emplace(tbl_.cols[argIndex], aggregateComparator);
        for (size_t rowId : rowIds) {
            const auto& row = input[rowId];
            bool passes = true;
            for (const auto& filter : filters) {
                if (!StorageEngine::evalConditionOnRow(filter, row.raw, tbl_, row.nulls)) {
                    passes = false; break;
                }
            }
            if (!passes) continue;
            std::string value;
            bool valueIsNull = false;
            if (booleanArgument) {
                const auto evaluated = booleanArgument->evaluate(row);
                value = evaluated.value; valueIsNull = evaluated.isNull;
            } else if (argIsExpr) {
                value = exprVal(arg, row.values);
                valueIsNull = value.empty();
            } else {
                value = argIndex < tbl_.len() ? row.values[argIndex] : "";
                valueIsNull = argIndex < row.nulls.size() && row.nulls[argIndex];
            }
            if (func == "count") {
                if (distinct) {
                    if (!valueIsNull)
                        distinctValues.insert(argIndex < tbl_.len()
                            ? tbl_.columnIndexKey(tbl_.cols[argIndex].dataName, value)
                            : value);
                } else if (arg == "*" || !valueIsNull) {
                    ++count;
                }
                continue;
            }
            if (valueIsNull) continue;
            if (func == "sum" || func == "avg") {
                if (exactInput) {
                    exactSum = exactSum + dbms::Numeric(value);
                    ++count;
                    continue;
                }
                long double number = 0;
                if (!parseNumber(value, number)) continue;
                sum += number; ++count;
                if (exactSumOk) {
                    try {
                        exactSum = exactSum + dbms::Numeric(value);
                    } catch (...) {
                        exactSumOk = false;
                    }
                }
            } else if (func == "min" || func == "max") {
                const bool replaces = hasValue && (declaredOrder
                    ? (func == "min" ? declaredOrder->less(value, selected)
                                     : declaredOrder->less(selected, value))
                    : (func == "min" ? compareNonNullAggregateValue(value, selected) < 0
                                     : compareNonNullAggregateValue(value, selected) > 0));
                if (!hasValue || replaces) {
                    selected = value; hasValue = true;
                }
            } else if (func == "bool_and" || func == "every" || func == "bool_or") {
                boolSeen = true;
                // Accept both "t"/"f" (evaluator booleans) and "true"/"false".
                const bool truthy = value == "true" || value == "t" || value == "1";
                if (func == "bool_or") boolValue = boolValue || truthy;
                else boolValue = boolValue && truthy;
            }
        }
        if (func == "count") {
            if (resultIsNull) *resultIsNull = false;
            return distinct ? std::to_string(distinctValues.size())
                            : std::to_string(count);
        }
        if (func == "sum") {
            if (resultIsNull) *resultIsNull = count == 0;
            if (count == 0) return "NULL";
            return exactInput ? exactSum.toString() : formatNumber(sum);
        }
        if (func == "avg") {
            if (resultIsNull) *resultIsNull = count == 0;
            if (count == 0) return "NULL";
            // PG avg(numeric) = numeric division of the exact sum by the
            // row count, carrying the select_div_scale digit rules.
            if (exactSumOk) {
                try {
                    return (exactSum / dbms::Numeric(static_cast<int64_t>(count))).toString();
                } catch (...) {
                }
            }
            return std::to_string(static_cast<double>(sum / count));
        }
        if (func == "min" || func == "max") {
            if (resultIsNull) *resultIsNull = !hasValue;
            return hasValue ? selected : "NULL";
        }
        if (func == "bool_and" || func == "every" || func == "bool_or") {
            if (resultIsNull) *resultIsNull = !boolSeen;
            return boolSeen ? (boolValue ? "t" : "f") : "NULL";
        }
        if (resultIsNull) *resultIsNull = true;
        return "NULL";
    };
    for (const auto& item : items_) {
        std::string func = item.func;
        for (char& c : func) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (!supported.count(func)) return false;
        if (func == "count" && item.arg == "*") continue;
        std::string arg = item.arg;
        if (arg.size() > 9 && arg.substr(0, 9) == "distinct ") arg = arg.substr(9);
        // Boolean aggregates accept comparison-expression arguments
        // ("v > 5"), evaluated per row by computeAggregate.
        const bool boolExprArg =
            (func == "bool_and" || func == "bool_or" || func == "every") &&
            arg != "*" && arg.find(' ') != std::string::npos;
    }
    for (const auto& group : groups) {
        std::vector<std::string> values;
        std::vector<bool> nulls;
        values.reserve(groupByCols_.size() + items_.size());
        nulls.reserve(groupByCols_.size() + items_.size());
        for (size_t gi = 0; gi < groupByCols_.size(); ++gi) {
            const bool isNull = group.second.empty() ||
                groupKeyValue(group.second.front(), gi).isNull;
            values.push_back(isNull ? std::string{} :
                groupKeyValue(group.second.front(), gi).value);
            nulls.push_back(isNull);
        }
        for (const auto& item : items_) {
            bool isNull = false;
            std::string value = computeAggregate(
                group.second, item, &isNull);
            values.push_back(isNull ? std::string{} : std::move(value));
            nulls.push_back(isNull);
        }
        std::string output;
        for (size_t i = 0; i < values.size(); ++i) {
            if (i != 0) output.push_back(' ');
            output += nulls[i] ? "NULL" : values[i];
        }
        rows_.push_back(std::move(output));
        structuredRows_.push_back(std::move(values));
        structuredNulls_.push_back(std::move(nulls));
    }
    return true;
}

catch (const DbError&) {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    try { child_->close(); } catch (...) {}
    throw;
}
catch (const std::exception& error) {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    child_->close();
    setError(error.what());
    return false;
}

bool ParallelGroupAggregateOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_++];
    rtInstr_.emitted = true;
    return true;
}

bool ParallelGroupAggregateOp::lastStructuredRow(
    std::vector<std::string>& cells, std::vector<bool>& nulls) const {
    if (pos_ == 0 || pos_ > structuredRows_.size() ||
        pos_ > structuredNulls_.size()) {
        return false;
    }
    cells = structuredRows_[pos_ - 1];
    nulls = structuredNulls_[pos_ - 1];
    return true;
}

void ParallelGroupAggregateOp::close() {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
}

// ========================================================================
// ParallelHashJoinOp
// ========================================================================
ParallelHashJoinOp::ParallelHashJoinOp(
    StorageEngine* engine, const std::string& dbname,
    OpPtr left, OpPtr right,
    const std::string& leftTable, const std::string& rightTable,
    const std::string& leftCol, const std::string& rightCol, int workers)
    : engine_(engine), dbname_(dbname), left_(std::move(left)),
      right_(std::move(right)), leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol),
      workers_(workers < 1 ? 1 : workers) {}

bool ParallelHashJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);
    leftColIdx_ = findJoinColumnIndex(leftTbl_, leftCol_);
    rightColIdx_ = findJoinColumnIndex(rightTbl_, rightCol_);
    if (leftColIdx_ >= leftTbl_.len() || rightColIdx_ >= rightTbl_.len()) {
        setError("parallel hash join column does not exist");
        return false;
    }
    rightHash_.clear();

    // Decide whether the build side can be hashed by worker threads.
    const uint32_t pageCount = engine_->tableNumPages(dbname_, rightTable_);
    bool parallelBuild = workers_ > 1 && !engine_->inTransaction() &&
                         rightTbl_.partitionType == TableSchema::PartitionType::None &&
                         pageCount > 2;
    if (parallelBuild) {
        const int activeWorkers = static_cast<int>(std::min<size_t>(
            static_cast<size_t>(workers_), static_cast<size_t>(pageCount - 1)));
        using Shard = std::map<std::string, std::vector<std::pair<int64_t, std::string>>>;
        std::vector<Shard> shards(static_cast<size_t>(activeWorkers));
        std::atomic<bool> failed{false};
        const auto interruptState = currentQueryInterruptState();
        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(activeWorkers));
        for (int w = 0; w < activeWorkers; ++w) {
            const uint32_t begin = 1 + static_cast<uint32_t>(w) * (pageCount - 1) /
                                       static_cast<uint32_t>(activeWorkers);
            const uint32_t end = 1 + static_cast<uint32_t>(w + 1) * (pageCount - 1) /
                                       static_cast<uint32_t>(activeWorkers);
            threads.emplace_back([this, &shards, &failed, interruptState,
                                  w, begin, end]() {
                setCurrentQueryInterruptState(interruptState);
                auto& shard = shards[static_cast<size_t>(w)];
                try {
                    if (!engine_->forEachRowPageRange(dbname_, rightTable_, begin, end,
                        [this, &shard, &failed](uint32_t pageId, uint16_t slotId,
                                               const char* data, size_t len) {
                            std::string row(data, len);
                            bool toastOk = false;
                            row = engine_->resolveToastValues(
                                dbname_, rightTable_, row, rightTbl_, &toastOk);
                            if (!toastOk) {
                                failed.store(true, std::memory_order_relaxed);
                                return;
                            }
                            const int64_t rid =
                                StorageEngine::encodeRid(pageId, slotId);
                            if (engine_->isColumnNullByRid(
                                    dbname_, rightTable_, rid, rightColIdx_)) {
                                return;
                            }
                            std::string key = extractJoinKey(row, rightTbl_, rightCol_);
                            shard[key].emplace_back(rid, std::move(row));
                        })) {
                        failed.store(true, std::memory_order_relaxed);
                    }
                } catch (...) {
                    failed.store(true, std::memory_order_relaxed);
                }
                setCurrentQueryInterruptState(nullptr);
            });
        }
        for (auto& t : threads) t.join();
        checkForQueryInterrupt();
        if (failed.load(std::memory_order_relaxed)) {
            setError("parallel hash join build failed");
            return false;
        }
        // merge shards; keep per-key chains ordered by rid for determinism
        for (auto& shard : shards) {
            for (auto& kv : shard) {
                auto& chain = rightHash_[kv.first];
                chain.insert(chain.end(),
                             std::make_move_iterator(kv.second.begin()),
                             std::make_move_iterator(kv.second.end()));
            }
        }
        for (auto& kv : rightHash_) {
            interruptibleSort(kv.second.begin(), kv.second.end(),
                      [](const auto& a, const auto& b) { return a.first < b.first; });
        }
        usedParallelWorkers_ = true;
        // the right child was not used; close it politely if it opened nothing
        right_->close();
    } else {
        if (!right_->open()) return false;
        std::string rightRow;
        while (right_->next(rightRow)) {
            if (right_->lastColumnIsNull(rightColIdx_)) continue;
            std::string key = extractJoinKey(rightRow, rightTbl_, rightCol_);
            rightHash_[key].emplace_back(0, std::move(rightRow));
        }
        if (right_->hasError()) {
            return propagateChildError(right_.get(), "hash join right child failed");
        }
        right_->close();
    }

    if (!left_->open()) return false;
    hasLeft_ = left_->next(curLeftRow_);
    if (left_->hasError()) {
        return propagateChildError(left_.get(), "hash join left child failed");
    }
    curLeftKeyNull_ = hasLeft_ && left_->lastColumnIsNull(leftColIdx_);
    matchPos_ = 0;
    curRightMatches_.clear();
    if (hasLeft_ && !curLeftKeyNull_) {
        std::string key = extractJoinKey(curLeftRow_, leftTbl_, leftCol_);
        auto it = rightHash_.find(key);
        if (it != rightHash_.end()) curRightMatches_ = it->second;
    }
    return true;
}

bool ParallelHashJoinOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    while (hasLeft_) {
        while (matchPos_ < curRightMatches_.size()) {
            outRow = curLeftRow_ + curRightMatches_[matchPos_].second;
            ++matchPos_;
            rtInstr_.emitted = true;
            return true;
        }
        hasLeft_ = left_->next(curLeftRow_);
        if (left_->hasError()) {
            return propagateChildError(left_.get(), "hash join left child failed");
        }
        if (!hasLeft_) break;
        curLeftKeyNull_ = left_->lastColumnIsNull(leftColIdx_);
        if (curLeftKeyNull_) {
            curRightMatches_.clear();
            matchPos_ = 0;
            continue;
        }
        std::string key = extractJoinKey(curLeftRow_, leftTbl_, leftCol_);
        auto it = rightHash_.find(key);
        curRightMatches_ = (it == rightHash_.end())
            ? std::vector<std::pair<int64_t, std::string>>{} : it->second;
        matchPos_ = 0;
    }
    return false;
}

void ParallelHashJoinOp::close() {
    left_->close();
    right_->close();
    rightHash_.clear();
    hasLeft_ = false;
    curRightMatches_.clear();
    matchPos_ = 0;
}

// ========================================================================
// GatherMergeOp
// ========================================================================
GatherMergeOp::GatherMergeOp(std::vector<OpPtr> children, const TableSchema& tbl,
                             const std::string& sortCol, bool asc)
    : children_(std::move(children)), tbl_(tbl), sortCol_(sortCol), asc_(asc) {
    colIdx_ = tbl_.len();
    for (size_t i = 0; i < tbl_.len(); ++i) {
        if (tbl_.cols[i].dataName == sortCol_) { colIdx_ = i; break; }
    }
}

bool GatherMergeOp::open() {
    heads_.assign(children_.size(), {});
    done_.assign(children_.size(), true);
    bool anyOk = false;
    for (size_t i = 0; i < children_.size(); ++i) {
        if (!children_[i]->open()) {
            if (children_[i]->hasError()) {
                return propagateChildError(children_[i].get(), "gather merge child failed");
            }
            done_[i] = true;
            continue;
        }
        anyOk = true;
        std::string row;
        if (children_[i]->next(row)) {
            heads_[i] = std::move(row);
            done_[i] = false;
        } else {
            if (children_[i]->hasError()) {
                return propagateChildError(children_[i].get(), "gather merge child failed");
            }
            done_[i] = true;
            children_[i]->close();
        }
    }
    return anyOk || children_.empty();
}

bool GatherMergeOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    // pick the smallest/largest head among live streams
    ssize_t pick = -1;
    for (size_t i = 0; i < children_.size(); ++i) {
        if (done_[i]) continue;
        if (pick < 0) { pick = static_cast<ssize_t>(i); continue; }
        const std::string a = StorageEngine::extractColumnValueStatic(heads_[i], tbl_, colIdx_);
        const std::string b = StorageEngine::extractColumnValueStatic(heads_[pick], tbl_, colIdx_);
        const int cmp = compareWindowValue(a, b);
        if ((asc_ && cmp < 0) || (!asc_ && cmp > 0)) pick = static_cast<ssize_t>(i);
    }
    if (pick < 0) return false;
    outRow = heads_[static_cast<size_t>(pick)];
    std::string row;
    if (children_[static_cast<size_t>(pick)]->next(row)) {
        heads_[static_cast<size_t>(pick)] = std::move(row);
    } else {
        done_[static_cast<size_t>(pick)] = true;
        children_[static_cast<size_t>(pick)]->close();
    }
    rtInstr_.emitted = true;
    return true;
}

void GatherMergeOp::close() {
    for (auto& c : children_) c->close();
    heads_.clear();
    done_.clear();
}

bool HashJoinOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    while (hasLeft_) {
        // If we have pending matches for current left row, output them
        while (matchPos_ < curRightMatches_.size()) {
            outRow = curLeftRow_ + curRightMatches_[matchPos_];
            ++matchPos_;
            rtInstr_.emitted = true;
    return true;
        }

        // Move to next left row
        hasLeft_ = left_->next(curLeftRow_);
        if (left_->hasError()) return propagateChildError(left_.get(), "hash join left child failed");
        if (!hasLeft_) break;
        curLeftKeyNull_ = left_->lastColumnIsNull(leftColIdx_);
        if (curLeftKeyNull_) {
            curRightMatches_.clear();
            matchPos_ = 0;
            continue;
        }

        std::string key = extractJoinKey(curLeftRow_, leftTbl_, leftCol_);
        auto it = rightHash_.find(key);
        if (it != rightHash_.end()) {
            curRightMatches_ = it->second;
            matchPos_ = 0;
        } else {
            curRightMatches_.clear();
            matchPos_ = 0;
        }
    }
    return false;
}

void HashJoinOp::close() {
    left_->close();
    rightHash_.clear();
    curRightMatches_.clear();
}

// ========================================================================
// MergeJoinOp
// ========================================================================

MergeJoinOp::MergeJoinOp(StorageEngine* engine, const std::string& dbname,
                         OpPtr left, OpPtr right,
                         const std::string& leftTable, const std::string& rightTable,
                         const std::string& leftCol, const std::string& rightCol)
    : engine_(engine), dbname_(dbname),
      left_(std::move(left)), right_(std::move(right)),
      leftTable_(leftTable), rightTable_(rightTable),
      leftCol_(leftCol), rightCol_(rightCol) {}

bool MergeJoinOp::open() {
    leftTbl_ = engine_->getTableSchema(dbname_, leftTable_);
    rightTbl_ = engine_->getTableSchema(dbname_, rightTable_);
    const size_t leftColIdx = findJoinColumnIndex(leftTbl_, leftCol_);
    const size_t rightColIdx = findJoinColumnIndex(rightTbl_, rightCol_);
    if (leftColIdx >= leftTbl_.len() || rightColIdx >= rightTbl_.len()) {
        setError("merge join column does not exist");
        return false;
    }
    leftRows_.clear();
    rightRows_.clear();

    // Read all left rows
    if (!left_->open()) return false;
    std::string row;
    while (left_->next(row)) {
        if (!left_->lastColumnIsNull(leftColIdx)) {
            leftRows_.push_back(std::move(row));
        }
    }
    if (left_->hasError()) return propagateChildError(left_.get(), "merge join left child failed");
    left_->close();

    // Read all right rows
    if (!right_->open()) return false;
    while (right_->next(row)) {
        if (!right_->lastColumnIsNull(rightColIdx)) {
            rightRows_.push_back(std::move(row));
        }
    }
    if (right_->hasError()) return propagateChildError(right_.get(), "merge join right child failed");
    right_->close();

    // Sort both by join key
    auto leftCmp = [&](const std::string& a, const std::string& b) {
        return extractJoinKey(a, leftTbl_, leftCol_) < extractJoinKey(b, leftTbl_, leftCol_);
    };
    auto rightCmp = [&](const std::string& a, const std::string& b) {
        return extractJoinKey(a, rightTbl_, rightCol_) < extractJoinKey(b, rightTbl_, rightCol_);
    };
    interruptibleSort(leftRows_.begin(), leftRows_.end(), leftCmp);
    interruptibleSort(rightRows_.begin(), rightRows_.end(), rightCmp);

    leftPos_ = 0;
    rightPos_ = 0;
    leftGroupEnd_ = 0;
    rightGroupBegin_ = 0;
    rightGroupEnd_ = 0;
    emittingGroup_ = false;
    return true;
}

bool MergeJoinOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    while (true) {
        if (emittingGroup_) {
            outRow = leftRows_[leftPos_] + rightRows_[rightPos_];
            ++rightPos_;
            if (rightPos_ >= rightGroupEnd_) {
                rightPos_ = rightGroupBegin_;
                ++leftPos_;
                if (leftPos_ >= leftGroupEnd_) {
                    emittingGroup_ = false;
                    rightPos_ = rightGroupEnd_;
                }
            }
            rtInstr_.emitted = true;
            return true;
        }

        if (leftPos_ >= leftRows_.size() || rightPos_ >= rightRows_.size()) {
            return false;
        }
        std::string lk = extractJoinKey(leftRows_[leftPos_], leftTbl_, leftCol_);
        std::string rk = extractJoinKey(rightRows_[rightPos_], rightTbl_, rightCol_);
        if (lk == rk) {
            leftGroupEnd_ = leftPos_ + 1;
            while (leftGroupEnd_ < leftRows_.size() &&
                   extractJoinKey(leftRows_[leftGroupEnd_], leftTbl_, leftCol_) ==
                       lk) {
                ++leftGroupEnd_;
            }
            rightGroupBegin_ = rightPos_;
            rightGroupEnd_ = rightPos_ + 1;
            while (rightGroupEnd_ < rightRows_.size() &&
                   extractJoinKey(rightRows_[rightGroupEnd_], rightTbl_,
                                  rightCol_) == rk) {
                ++rightGroupEnd_;
            }
            emittingGroup_ = true;
        } else if (lk < rk) {
            ++leftPos_;
        } else {
            ++rightPos_;
        }
    }
}

void MergeJoinOp::close() {
    leftRows_.clear();
    rightRows_.clear();
    leftPos_ = 0;
    rightPos_ = 0;
    emittingGroup_ = false;
}

// ========================================================================
// GroupAggregateOp
// ========================================================================

GroupAggregateOp::GroupAggregateOp(
    OpPtr child, const TableSchema& tbl,
    const std::vector<std::string>& groupByCols,
    const std::vector<std::vector<std::string>>& groupingSets,
    const std::vector<StorageEngine::AggItem>& items,
    const std::vector<std::string>& havingConds)
    : child_(std::move(child)), tbl_(tbl), groupByCols_(groupByCols),
      groupingSets_(groupingSets), items_(items), havingConds_(havingConds) {}

bool GroupAggregateOp::open() try {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
    if (!child_->open()) return false;
    const auto expressionOrigin = child_->scanOrigin();
    ExprEvaluator aggregateComparator;
    aggregateComparator.setCurrentDB(expressionOrigin.dbname);

    struct InputRow {
        std::string raw;
        std::vector<std::string> values;
        std::vector<bool> nulls;
    };
    std::vector<InputRow> input;
    std::string raw;
    while (child_->next(raw)) {
        InputRow row;
        row.raw = std::move(raw);
        row.values.reserve(tbl_.len());
        row.nulls.reserve(tbl_.len());
        for (size_t i = 0; i < tbl_.len(); ++i) {
            row.values.push_back(StorageEngine::extractColumnValueStatic(row.raw, tbl_, i));
            row.nulls.push_back(child_->lastColumnIsNull(i));
        }
        input.push_back(std::move(row));
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "aggregate child failed");
    child_->close();

    auto columnIndex = [&](const std::string& name) {
        for (size_t i = 0; i < tbl_.len(); ++i) {
            if (tbl_.cols[i].dataName == name) return i;
        }
        return tbl_.len();
    };
    // GROUP BY expressions: non-column keys are evaluated per row
    // (same semantics as GroupAggregateOp).
    const auto groupingKeys = materializeGroupingKeys(tbl_, input, groupByCols_);
    const auto groupKeyValue = [&](size_t rowId, size_t keyPos) -> const ExprValue& {
        return groupingKeys.at(rowId).at(keyPos);
    };

    static const std::set<std::string> supported = {
        "count", "sum", "avg", "min", "max", "bool_and", "bool_or", "every",
        "string_agg", "array_agg"
    };
    for (const auto& item : items_) {
        std::string func = item.func;
        for (char& c : func) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (!supported.count(func)) return false;
        if (func == "count" && item.arg == "*") continue;
        std::string arg = item.arg;
        if (arg.size() > 9 && arg.substr(0, 9) == "distinct ") arg = arg.substr(9);
        // Boolean aggregates accept comparison-expression arguments
        // ("v > 5"), evaluated per row by computeAggregate.
        const bool boolExprArg =
            (func == "bool_and" || func == "bool_or" || func == "every") &&
            arg != "*" && arg.find(' ') != std::string::npos;
    }

    std::vector<std::vector<std::string>> effectiveSets = groupingSets_;
    if (effectiveSets.empty()) effectiveSets.push_back(groupByCols_);

    auto parseNumber = [](const std::string& value, long double& out) {
        try {
            size_t consumed = 0;
            out = std::stold(value, &consumed);
            return consumed == value.size();
        } catch (...) {
            return false;
        }
    };
    auto formatNumber = [](long double value) {
        if (std::floor(value) == value &&
            value >= static_cast<long double>(std::numeric_limits<int64_t>::min()) &&
            value <= static_cast<long double>(std::numeric_limits<int64_t>::max())) {
            return std::to_string(static_cast<int64_t>(value));
        }
        std::ostringstream out;
        out << std::setprecision(15) << static_cast<double>(value);
        return out.str();
    };

    auto computeAggregate = [&](const std::vector<size_t>& rowIds,
                                const StorageEngine::AggItem& item,
                                bool* resultIsNull = nullptr) -> std::string {
        std::string func = item.func;
        for (char& c : func) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        const bool distinct = func == "count" && item.arg.size() > 9 &&
            item.arg.substr(0, 9) == "distinct ";
        std::string arg = distinct ? item.arg.substr(9) : item.arg;
        const size_t argIndex = arg == "*" ? tbl_.len() : columnIndex(arg);
        const auto filters = StorageEngine::parseConditions(item.filterConds);
        std::optional<aggregate_type_detail::BooleanArgument> booleanArgument;
        if (argIndex >= tbl_.len() && arg != "*" && !arg.empty() &&
            (func == "bool_and" || func == "bool_or" || func == "every"))
            booleanArgument.emplace(tbl_, expressionOrigin.engine, expressionOrigin.dbname, arg);
        if (func == "string_agg" || func == "array_agg")
            return collectionAggregate(
                tbl_, input, rowIds, item, func, resultIsNull);
        std::set<std::string> distinctValues;
        int64_t count = 0;
        long double sum = 0;
        dbms::Numeric exactSum(0);
        bool exactSumOk = true;
        const bool exactInput = exactAggregateColumn(tbl_, argIndex);
        bool hasValue = false;
        std::string selected;
        bool boolSeen = false;
        bool boolValue = func == "bool_and" || func == "every";
        std::optional<DeclaredAggregateLess> declaredOrder;
        if ((func == "min" || func == "max") && argIndex < tbl_.len())
            declaredOrder.emplace(tbl_.cols[argIndex], aggregateComparator);

        for (size_t rowId : rowIds) {
            const auto& row = input[rowId];
            bool passes = true;
            for (const auto& filter : filters) {
                if (!StorageEngine::evalConditionOnRow(filter, row.raw, tbl_, row.nulls)) {
                    passes = false;
                    break;
                }
            }
            if (!passes) continue;

            std::string value;
            bool valueIsNull = false;
            if (booleanArgument) {
                const auto evaluated = booleanArgument->evaluate(row);
                value = evaluated.value; valueIsNull = evaluated.isNull;
            } else {
                value = argIndex < tbl_.len() ? row.values[argIndex] : std::string{};
                valueIsNull = argIndex < tbl_.len()
                    ? (argIndex < row.nulls.size() && row.nulls[argIndex]) : value.empty();
            }
            if (func == "count") {
                if (distinct) {
                    if (!valueIsNull)
                        distinctValues.insert(argIndex < tbl_.len()
                            ? tbl_.columnIndexKey(tbl_.cols[argIndex].dataName, value)
                            : value);
                } else if (arg == "*") {
                    ++count;
                } else if (!valueIsNull) {
                    ++count;
                }
                continue;
            }
            if (valueIsNull) continue;
            if (func == "sum" || func == "avg") {
                if (exactInput) {
                    exactSum = exactSum + dbms::Numeric(value);
                    ++count;
                    continue;
                }
                long double number = 0;
                if (!parseNumber(value, number)) continue;
                sum += number;
                ++count;
                if (exactSumOk) {
                    try { exactSum = exactSum + dbms::Numeric(value); }
                    catch (...) { exactSumOk = false; }
                }
            } else if (func == "min" || func == "max") {
                const bool replaces = hasValue && (declaredOrder
                    ? (func == "min" ? declaredOrder->less(value, selected)
                                     : declaredOrder->less(selected, value))
                    : (func == "min" ? compareNonNullAggregateValue(value, selected) < 0
                                     : compareNonNullAggregateValue(value, selected) > 0));
                if (!hasValue || replaces) {
                    selected = value;
                    hasValue = true;
                }
            } else if (func == "bool_and" || func == "every" || func == "bool_or") {
                boolSeen = true;
                // Accept both "t"/"f" (evaluator booleans) and "true"/"false".
                const bool truthy = value == "true" || value == "t" || value == "1";
                if (func == "bool_or") boolValue = boolValue || truthy;
                else boolValue = boolValue && truthy;
            }
        }

        if (func == "count") {
            if (resultIsNull) *resultIsNull = false;
            return distinct ? std::to_string(distinctValues.size()) : std::to_string(count);
        }
        if (func == "sum") {
            if (resultIsNull) *resultIsNull = count == 0;
            if (count == 0) return "NULL";
            return exactInput ? exactSum.toString() : formatNumber(sum);
        }
        if (func == "avg") {
            if (resultIsNull) *resultIsNull = count == 0;
            if (count == 0) return "NULL";
            // PG avg(numeric) = numeric division of the exact sum by the
            // row count, carrying the select_div_scale digit rules.
            if (exactSumOk) {
                try {
                    return (exactSum / dbms::Numeric(static_cast<int64_t>(count))).toString();
                } catch (...) {
                }
            }
            return std::to_string(static_cast<double>(sum / count));
        }
        if (func == "min" || func == "max") {
            if (resultIsNull) *resultIsNull = !hasValue;
            return hasValue ? selected : "NULL";
        }
        if (func == "bool_and" || func == "every" || func == "bool_or") {
            if (resultIsNull) *resultIsNull = !boolSeen;
            return boolSeen ? (boolValue ? "t" : "f") : "NULL";
        }
        if (resultIsNull) *resultIsNull = true;
        return "NULL";
    };

    auto havingPasses = [&](const std::vector<size_t>& rowIds) {
        for (const auto& condition : havingConds_) {
            const std::string expression = trimExec(condition);
            const size_t leftParen = expression.find('(');
            const size_t rightParen = expression.find(')', leftParen == std::string::npos ? 0 : leftParen + 1);
            if (leftParen == std::string::npos || rightParen == std::string::npos) return false;
            size_t opStart = rightParen + 1;
            while (opStart < expression.size() && std::isspace(static_cast<unsigned char>(expression[opStart]))) ++opStart;
            size_t opEnd = opStart;
            while (opEnd < expression.size() &&
                   (expression[opEnd] == '<' || expression[opEnd] == '>' ||
                    expression[opEnd] == '=' || expression[opEnd] == '!')) ++opEnd;
            if (opEnd == opStart) return false;
            const std::string op = expression.substr(opStart, opEnd - opStart);
            const std::string expected = trimExec(expression.substr(opEnd));
            StorageEngine::AggItem item;
            item.func = trimExec(expression.substr(0, leftParen));
            item.arg = trimExec(expression.substr(leftParen + 1, rightParen - leftParen - 1));
            const std::string actual = computeAggregate(rowIds, item);
            if (op == "=" || op == "!=") {
                const bool equal = actual == expected;
                if ((op == "=" && !equal) || (op == "!=" && equal)) return false;
                continue;
            }
            long double actualNumber = 0, expectedNumber = 0;
            if (!parseNumber(actual, actualNumber) || !parseNumber(expected, expectedNumber)) return false;
            if ((op == ">" && !(actualNumber > expectedNumber)) ||
                (op == ">=" && !(actualNumber >= expectedNumber)) ||
                (op == "<" && !(actualNumber < expectedNumber)) ||
                (op == "<=" && !(actualNumber <= expectedNumber))) return false;
        }
        return true;
    };

    for (const auto& groupingSet : effectiveSets) {
        std::vector<size_t> setIndices;
        std::vector<size_t> setKeyPos;
        for (const auto& name : groupingSet) {
            size_t keyPos = static_cast<size_t>(-1);
            for (size_t gp = 0; gp < groupByCols_.size(); ++gp)
                if (groupByCols_[gp] == name) { keyPos = gp; break; }
            const size_t index = columnIndex(name);
            if (keyPos == static_cast<size_t>(-1)) {
                throw std::runtime_error("GROUPING SET key missing from GROUP BY (SQLSTATE 42601)");
            }
            setIndices.push_back(index);
            setKeyPos.push_back(keyPos);
        }

        std::map<std::string, std::vector<size_t>> groups;
        if (setIndices.empty()) groups[""] = {};
        for (size_t rowId = 0; rowId < input.size(); ++rowId) {
            std::string key;
            for (size_t ki = 0; ki < setIndices.size(); ++ki) {
                const auto& value = groupKeyValue(rowId, setKeyPos[ki]);
                key += encodeGroupingKey(value);
            }
            groups[key].push_back(rowId);
        }

        for (const auto& group : groups) {
            if (!havingPasses(group.second)) continue;
            std::vector<std::string> values;
            std::vector<bool> nulls;
            values.reserve(groupByCols_.size() + items_.size());
            nulls.reserve(groupByCols_.size() + items_.size());
            for (size_t gi = 0; gi < groupByCols_.size(); ++gi) {
                auto setIt = std::find(groupingSet.begin(), groupingSet.end(), groupByCols_[gi]);
                const bool isNull = setIt == groupingSet.end() ||
                    group.second.empty() ||
                    groupKeyValue(group.second.front(), gi).isNull;
                values.push_back(isNull ? std::string{} :
                    groupKeyValue(group.second.front(), gi).value);
                nulls.push_back(isNull);
            }
            for (const auto& item : items_) {
                bool isNull = false;
                std::string value = computeAggregate(
                    group.second, item, &isNull);
                values.push_back(isNull ? std::string{} : std::move(value));
                nulls.push_back(isNull);
            }
            std::string output;
            for (size_t i = 0; i < values.size(); ++i) {
                if (i != 0) output.push_back(' ');
                output += nulls[i] ? "NULL" : values[i];
            }
            rows_.push_back(std::move(output));
            structuredRows_.push_back(std::move(values));
            structuredNulls_.push_back(std::move(nulls));
        }
    }
    return true;
}

catch (const DbError&) {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    try { child_->close(); } catch (...) {}
    throw;
}
catch (const std::exception& error) {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    child_->close();
    setError(error.what());
    return false;
}

bool GroupAggregateOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    outRow = rows_[pos_++];
    rtInstr_.emitted = true;
    return true;
}

bool GroupAggregateOp::lastStructuredRow(
    std::vector<std::string>& cells, std::vector<bool>& nulls) const {
    if (pos_ == 0 || pos_ > structuredRows_.size() ||
        pos_ > structuredNulls_.size()) {
        return false;
    }
    cells = structuredRows_[pos_ - 1];
    nulls = structuredNulls_[pos_ - 1];
    return true;
}

void GroupAggregateOp::close() {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
}

// ========================================================================
// QueryPlanner
// ========================================================================

namespace {
// One planning call owns this descriptor. Schema and access-method discovery
// do not need repeated catalog validation/copies for each candidate or node.
// Nothing escapes into a retained plan, session cache, or operator execution.
struct PlanningRelationMetadata {
    StorageEngine* engine;
    const PlanContext& context;
    const TableSchema& table;
    std::optional<std::vector<std::string>> btrees{}, hashes{}, blooms{}, gists{};

    const std::vector<std::string>& btreeColumns() {
        if (!btrees) btrees = engine->getIndexedColumns(context.dbname, context.tablename);
        return *btrees;
    }
    const std::vector<std::string>& hashColumns() {
        if (!hashes) hashes = engine->getHashIndexedColumns(context.dbname, context.tablename);
        return *hashes;
    }
    const std::vector<std::string>& bloomColumns() {
        if (!blooms) blooms = engine->getBloomIndexedColumns(context.dbname, context.tablename);
        return *blooms;
    }
    const std::vector<std::string>& gistColumns() {
        if (!gists) gists = engine->getGiSTIndexedColumns(context.dbname, context.tablename);
        return *gists;
    }
    bool hasBtreeColumn(const std::string& name) {
        const auto& columns = btreeColumns();
        return std::find(columns.begin(), columns.end(), name) != columns.end();
    }
    bool hasEqualityIndex(const StorageEngine::Condition& condition) {
        if (condition.op != "=" || equalityIndexKeyIsEmpty(table, condition)) return false;
        if (isSingleColumnPrimaryKey(table, condition.colName)) return true;
        const auto contains = [&](const auto& columns) {
            return std::find(columns.begin(), columns.end(), condition.colName) != columns.end();
        };
        return contains(btreeColumns()) || contains(hashColumns()) || contains(bloomColumns());
    }
};
} // namespace

// A GiST-servable predicate exists: a range (>, >=, <, <=) pair or single
// bound, or an anchored prefix LIKE 'abc%', whose column carries a .gist
// sidecar index.
static bool canUseGiSTScan(PlanningRelationMetadata& relation) {
    if (relation.table.partitionType != TableSchema::PartitionType::None) return false;
    auto isGist = [&](const std::string& col) {
        const auto& columns = relation.gistColumns();
        return std::find(columns.begin(), columns.end(), col) != columns.end();
    };
    for (const auto& condition : relation.context.conds) {
        // A binary text >= empty probe keeps every non-NULL key. It is
        // not an independent reason to replace the heap with a full index
        // traversal. Other useful conjuncts can still select this source.
        if (condition.op == ">=" && condition.value.empty()) {
            const auto end = relation.table.cols.end();
            const auto key = std::find_if(relation.table.cols.begin(), end,
                [&](const auto& c) { return c.dataName == condition.colName; });
            if (key != end && !key->isArray && key->domainName.empty() &&
                key->enumValues.empty() && !key->collation.empty() &&
                key->resolvedCollationIsBinary && !key->resolvedCollationUsesLocale) {
                const auto type = ExprHelper::canonicalResultTypeName(key->dataType);
                if (type == "text" || type == "character varying") continue;
            }
        }
        const bool isRange = condition.op == "<" || condition.op == "<=" ||
                             condition.op == ">" || condition.op == ">=";
        if ((isRange || (condition.op == "=" && !relation.hasEqualityIndex(condition))) &&
            isGist(condition.colName)) return true;
        if (const auto prefix = nativeLikePrefix(condition, relation.table);
            prefix && isGist(condition.colName) &&
            !densePrefixPrefersHeap(relation.engine, relation.context.dbname,
                relation.context.tablename, condition.colName, *prefix)) {
            return true;
        }
    }
    return false;
}

static bool canUseBitmapHeapScan(PlanningRelationMetadata& relation) {
    if (relation.table.partitionType != TableSchema::PartitionType::None ||
        relation.context.conds.size() < 2) return false;
    std::set<std::string> seenColumns;
    size_t indexedPredicates = 0;
    for (const auto& condition : relation.context.conds) {
        if (!seenColumns.insert(condition.colName).second) continue;
        if (relation.hasEqualityIndex(condition)) ++indexedPredicates;
    }
    return indexedPredicates >= 2;
}

static bool canUseBitmapOrScan(
    PlanningRelationMetadata& relation,
    const std::vector<std::vector<StorageEngine::Condition>>& branches) {
    if (relation.table.partitionType != TableSchema::PartitionType::None || branches.size() < 2)
        return false;
    for (const auto& branch : branches) {
        std::set<std::string> seenColumns;
        bool indexed = false;
        for (const auto& condition : branch) {
            if (!seenColumns.insert(condition.colName).second) continue;
            if (relation.hasEqualityIndex(condition)) {
                indexed = true;
                break;
            }
        }
        if (!indexed) return false;
    }
    return true;
}

// Ordinary literal access-path costing only. Prepared/retained parameter
// plans, bitmap/GiST priority and provider lifetimes retain their owners.
static std::optional<double> analyzedEqualityFraction(
    const Column& column,const StorageEngine::ColumnStats& statistics,
    double analyzedRows,const std::string& probe) {
    const double sourceRows = statistics.analyzedRows ? *statistics.analyzedRows : analyzedRows;
    return planner_statistics::equalityFraction(statistics,sourceRows,
        [&](const std::string& value)->std::optional<bool> {
            if(value==probe)return true;
            try {
                const auto truth=StorageEngine::compareValues(column,value,false,probe,false,"=");
                if(truth==StorageEngine::PredicateTruth::Unknown)return {};
                return truth==StorageEngine::PredicateTruth::True;
            } catch(const DbError& error) {
                // Cost estimation may decline an unknown input; the existing
                // selected executor still owns its input error and phase.
                if(error.sqlState().rfind("22",0)==0)return {};
                throw;
            }
        });
}

static std::optional<double> analyzedEqualityFraction(
    const Column& column, const analyzed_statistics::Column& statistics,
    double populationRows, const std::string& probe) {
    return planner_statistics::analyzedEqualityFraction(statistics, populationRows,
        [&](const ExprValue& value) -> std::optional<bool> {
            if (value.value == probe) return true;
            try {
                const auto truth = StorageEngine::compareValues(column, value.value, false, probe, false, "=");
                if (truth == StorageEngine::PredicateTruth::Unknown) return {};
                return truth == StorageEngine::PredicateTruth::True;
            } catch (const DbError& error) {
                if (error.sqlState().rfind("22", 0) == 0) return {};
                throw;
            }
        });
}

static std::optional<size_t> chooseEqualityIndexCondition(
    StorageEngine* engine,const PlanContext& context,const TableSchema& table,
    const std::vector<size_t>& candidates) {
    if(candidates.empty())return {};
    const size_t fallback=candidates.front();
    size_t analyzedRows=0;
    double analyzedPopulation=0;
    std::map<std::string,std::shared_ptr<const analyzed_statistics::Column>> typed;
    for (const auto position : candidates) {
        const auto& name=context.conds[position].colName;
        if (isSingleColumnPrimaryKey(table,name)) continue;
        const auto entry=typed.emplace(name,nullptr);
        if(entry.second)entry.first->second=engine->getAnalyzedColumnStatistics(context.dbname,context.tablename,name);
        if(!analyzedPopulation && entry.first->second)analyzedPopulation=entry.first->second->totalRows;
    }
    if(!analyzedPopulation) {
        if(!engine->tryGetTableRowCount(context.dbname,context.tablename,analyzedRows) || !analyzedRows)
            return fallback;
        analyzedPopulation=analyzedRows;
    }
    uint64_t runtimeRows=0;
    const double rows=getRuntimeLiveRowEstimate(context.dbname,context.tablename,runtimeRows)
        ?double(runtimeRows):analyzedPopulation;
    // Local to this one plan: no cross-session/cache invalidation contract.
    std::map<std::string,StorageEngine::ColumnStats> statistics;
    std::optional<double> bestCost;
    size_t best=fallback;
    for(const auto position:candidates) {
        const auto& condition=context.conds[position];
        const Column* column=nullptr;
        for(size_t i=0;i<table.len();++i)if(table.cols[i].dataName==condition.colName){column=&table.cols[i];break;}
        if(!column)return fallback;
        double matches=rows>0?1.0:0;
        // A genuine single-column primary key already proves at most one
        // match. Do not read/parse MCVs to rediscover that uniqueness on
        // every ordinary point lookup; still compare actual scan costs.
        if(!isSingleColumnPrimaryKey(table,condition.colName)) {
            const auto probe=condition.referenceInputOid
                ?std::to_string(*condition.referenceInputOid):condition.value;
            std::optional<double> fraction;
            if(const auto evidence=typed.at(condition.colName))
                fraction=analyzedEqualityFraction(*column,*evidence,rows,probe);
            if(!fraction) {
                if(!analyzedRows && (!engine->tryGetTableRowCount(context.dbname,context.tablename,analyzedRows) || !analyzedRows))
                    return fallback;
                auto entry=statistics.find(condition.colName);
                if(entry==statistics.end())entry=statistics.emplace(condition.colName,
                    engine->getColumnStats(context.dbname,context.tablename,condition.colName)).first;
                fraction=analyzedEqualityFraction(*column,entry->second,analyzedRows,probe);
            }
            if(!fraction)return fallback;
            matches=rows>0?std::max(1.0,rows**fraction):0;
        }
        const double cost=QueryPlanner::costScan("index_scan",matches,0);
        if(!std::isfinite(cost))return fallback;
        if(!bestCost || cost<*bestCost){best=position;bestCost=cost;}
    }
    // This existing physical extent accessor honors the actual tablespace;
    // it neither visits tuples nor substitutes a guessed row width.
    const auto pages=engine->tableNumPages(context.dbname,context.tablename);
    if(!pages)return fallback;
    const double sequential=QueryPlanner::costScan("seq_scan",rows,pages);
    if(!std::isfinite(sequential))return fallback;
    return *bestCost<sequential?std::optional<size_t>(best):std::nullopt;
}

// The ordinary frontend lowers simple predicates to Condition cells rather
// than the bound AST used above. Inspect only literal/physical-column cells;
// keep the original Filter as the sole predicate evaluator on candidate rows.
static OpPtr ordinaryBrinTableSource(StorageEngine* engine, const PlanContext& context,
                                    const TableSchema& schema) {
    if (schema.partitionType != TableSchema::PartitionType::None || context.conds.empty()) return {};
    struct Probe { std::string column, op, value; };
    std::vector<Probe> probes;
    const auto comparison = [](const std::string& op) {
        return op == "=" || op == "<" || op == "<=" || op == ">" || op == ">=";
    };
    const auto inspect = [&](const auto& self, const Expr* node) -> void {
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node)) {
            if (binary->op == "AND") { self(self, binary->left.get()); self(self, binary->right.get()); return; }
            if (!comparison(binary->op) || binary->comparison) return;
            const auto* column = dynamic_cast<const ColumnRefExpr*>(binary->left.get());
            const Expr* input = binary->right.get(); std::string op = binary->op;
            if (!column) {
                column = dynamic_cast<const ColumnRefExpr*>(binary->right.get()); input = binary->left.get();
                if (op == "<") op = ">"; else if (op == "<=") op = ">=";
                else if (op == ">") op = "<"; else if (op == ">=") op = "<=";
            }
            if (!column || !column->table.empty() || !column->schema.empty()) return;
            if (const auto value = brinLiteralInput(input)) probes.push_back({column->column, op, *value});
        } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(node);
                   unary && (unary->op == "IS NULL" || unary->op == "IS NOT NULL")) {
            const auto* column = dynamic_cast<const ColumnRefExpr*>(unary->operand.get());
            if (column && column->table.empty() && column->schema.empty())
                probes.push_back({column->column, unary->op, ""});
        }
    };
    for (const auto& condition : context.conds) {
        if (condition.patternIsNull || condition.referenceInputOid) continue;
        if (condition.op == "typedexpr") {
            SQLParser parser; auto parsed = parser.parseForBinding("SELECT " + condition.value);
            const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
            if (select && !select->fromClause && select->selectList.size() == 1)
                inspect(inspect, select->selectList.front().expr.get());
        } else if (condition.op == "isnull" || condition.op == "isnotnull") {
            if (!engine->hasBrinIndex(context.dbname, context.tablename, condition.colName)) continue;
            probes.push_back({condition.colName, condition.op == "isnull" ? "IS NULL" : "IS NOT NULL", ""});
        } else if (comparison(condition.op)) {
            if (!engine->hasBrinIndex(context.dbname, context.tablename, condition.colName)) continue;
            if (condition.decodedLiteralRhs) probes.push_back({condition.colName, condition.op, condition.value});
            else {
                SQLParser parser; auto parsed = parser.parseForBinding("SELECT " + condition.value);
                const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
                if (select && !select->fromClause && select->selectList.size() == 1)
                    if (const auto value = brinLiteralInput(select->selectList.front().expr.get()))
                        probes.push_back({condition.colName, condition.op, *value});
            }
        }
    }
    std::optional<double> bestCost; std::optional<uint32_t> extent;
    Probe best; size_t bestPages = 0;
    for (const auto& probe : probes) {
        const Column* key = nullptr;
        for (size_t i = 0; i < schema.len(); ++i)
            if (schema.cols[i].dataName == probe.column) { key = &schema.cols[i]; break; }
        if (!key || key->generatedKind || !key->domainName.empty() || !key->enumValues.empty()) continue;
        const bool nullTest = probe.op == "IS NULL" || probe.op == "IS NOT NULL";
        const auto* type = TypeRegistry::instance().findType(key->dataType);
        if (!type || type->category == TypeCategory::Composite || type->category == TypeCategory::Pseudo ||
            (!nullTest && (key->isArray || key->isUnsigned ||
                (!key->collation.empty() && !key->resolvedCollationIsBinary))) ||
            (probe.op == "=" && isSingleColumnPrimaryKey(schema, key->dataName)) ||
            !engine->hasBrinIndex(context.dbname, context.tablename, key->dataName)) continue;
        if (!nullTest) {
            const auto canonical = ExprHelper::canonicalResultTypeName(key->dataType);
            try {
                const auto role = ExprEvaluator::resolveComparison(probe.op, canonical, canonical);
                if (!role.strict || role.identity.rfind("builtin-comparison:", 0) != 0) continue;
            } catch (const DbError& error) { if (error.sqlState() == "42883") continue; throw; }
        }
        if (!extent) {
            if (!engine->getInheritedChildren(context.dbname, context.tablename).empty()) return {};
            extent = engine->tableNumPages(context.dbname, context.tablename);
            if (*extent <= 1) return {};
        }
        const auto ranges = engine->tryBrinPageRanges(context.dbname, context.tablename,
            probe.column, probe.op, probe.value, false);
        if (!ranges) continue;
        uint64_t pages = 0;
        for (const auto& range : *ranges) pages += uint64_t(range.second) - range.first + 1;
        const double cost = QueryPlanner::costScan("seq_scan", 0, pages) + QueryPlanner::costScan("index_scan", 1, 0);
        const double sequential = QueryPlanner::costScan("seq_scan", 0, *extent - 1);
        if (!std::isfinite(cost) || !std::isfinite(sequential) || cost >= sequential ||
            (bestCost && cost >= *bestCost)) continue;
        best = probe; bestPages = pages; bestCost = cost;
    }
    if (!bestCost) return {};
    return std::make_unique<BrinScanOp>(engine, context.dbname, context.tablename,
        best.column, best.op, best.value,
        std::make_unique<TableScanOp>(engine, context.dbname, context.tablename),
        schema, bestPages, bestPages * 32.0, *bestCost);
}

static std::vector<CompositeEqualityInput> compactCompositeInputs(
        const TableSchema& table, const std::vector<StorageEngine::Condition>& conditions,
        std::vector<CompositeSuffixBound>* bounds = nullptr, std::optional<size_t> suffix = {}) {
    const auto columns = compositePrimaryColumns(table);
    if (columns.empty() || conditions.empty()) return {};
    std::map<size_t, CompositeEqualityInput> matches;
    for (const auto index : columns) {
        if (index >= table.len()) return {};
        const auto& column = table.cols[index];
        const auto type = compositeEqualityType(column);
        if (type.empty()) return {};
        const auto condition = std::find_if(conditions.begin(), conditions.end(),
            [&](const auto& item) {
                return item.colName == column.dataName && item.op == "=" &&
                    item.decodedLiteralRhs && !item.patternIsNull && !item.referenceInputOid &&
                    (item.patternType.empty() || item.patternType == "unknown");
            });
        if (condition == conditions.end()) continue;
        const auto value = compositeEqualityValue(type, condition->value);
        if (value)
            matches.emplace(index, CompositeEqualityInput{index, column.dataName, type, *value, {}});
    }
    if (matches.size() < columns.size()) {
        for (const auto& condition : conditions) {
            if (bounds && suffix && condition.colName == table.cols[*suffix].dataName &&
                condition.decodedLiteralRhs && !condition.patternIsNull &&
                !condition.referenceInputOid && !condition.columnComparison &&
                (condition.patternType.empty() || condition.patternType == "unknown") &&
                (condition.op == "<" || condition.op == "<=" ||
                 condition.op == ">" || condition.op == ">=")) {
                if (const auto value = compositeEqualityValue("bigint", condition.value))
                    bounds->push_back({{*suffix, condition.colName, "bigint", *value, {}},
                        condition.op.front() == '>', condition.op.size() == 2});
            }
            if (condition.op != "typedexpr") continue;
            SQLParser parser;
            auto parsed = parser.parseForBinding("SELECT " + condition.value);
            const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
            if (!select || select->selectList.size() != 1 || select->fromClause) continue;
            collectCompositePredicates(table, columns, select->selectList.front().expr.get(),
                                       {}, matches, bounds, suffix);
        }
    }
    return orderedCompositeInputs(columns, matches, suffix.has_value());
}

std::map<std::string, std::string> composite_key_lookup::compactValues(
        const TableSchema& table, const std::vector<StorageEngine::Condition>& conditions) {
    std::map<std::string, std::string> values;
    for (const auto& input : compactCompositeInputs(table, conditions))
        values.emplace(input.name, input.value);
    return values;
}

std::map<std::string, std::string> composite_key_lookup::boundMutationValues(
        const TableSchema& table, const Expr* predicate, size_t target) {
    const auto columns = compositePrimaryColumns(table);
    if (columns.empty() || !predicate) return {};
    std::map<size_t, CompositeEqualityInput> matches;
    // Keep expressions outside the exact key conjunction on their existing
    // demand path. The compact binding has no slot owner for runtime inputs.
    const auto count = collectCompositePredicates(table, columns, predicate, target, matches);
    if (count != columns.size() || matches.size() != columns.size()) return {};
    std::map<std::string, std::string> values;
    for (const auto& [ordinal, input] : matches) {
        if (input.slot) return {};
        values.emplace(input.name, input.value);
    }
    return values;
}

// An ordered LIMIT can stop the scan before reaching later predicates. Only
// allow that for comparisons whose remaining rows cannot call a routine or
// raise a conversion error. Other queries keep the existing sort barrier.
static bool safeOrderedCompositePredicate(
        const TableSchema& table, const StorageEngine::Condition& condition,
        const std::vector<CompositeEqualityInput>& leading, bool& leadingOnly) {
    const auto isLeading = [&](size_t column) {
        return std::any_of(leading.begin(), leading.end(),
            [&](const auto& input) { return input.column == column; });
    };
    if (condition.op == "typedexpr") {
        auto bound = condition;
        StorageEngine::bindStoredComparison(bound, table);
        if (!bound.literalComparisons) return false;
        leadingOnly = leadingOnly && bound.literalComparisons->size() == 1 &&
            bound.literalComparisons->front().operation == "=" &&
            isLeading(bound.literalComparisons->front().column);
        return true;
    }
    if (!condition.decodedLiteralRhs || condition.patternIsNull || condition.referenceInputOid ||
        condition.columnComparison ||
        (!condition.patternType.empty() && condition.patternType != "unknown")) return false;
    for (size_t i = 0; i < table.len(); ++i) {
        if (table.cols[i].dataName != condition.colName) continue;
        const auto type = compositeEqualityType(table.cols[i]);
        if (type.empty()) return false;
        leadingOnly = leadingOnly && condition.op == "=" && isLeading(i);
        if (condition.op == "=" || condition.op == "<>" || condition.op == "!=")
            return compositeEqualityValue(type, condition.value).has_value();
        return type != "text" && (condition.op == "<" || condition.op == "<=" ||
            condition.op == ">" || condition.op == ">=") &&
            compositeEqualityValue(type, condition.value).has_value();
    }
    return false;
}

static std::unique_ptr<CompositePrimaryKeyScanOp> compactCompositePrimarySource(StorageEngine* engine,
        const PlanContext& context, const TableSchema& table) {
    auto suffix = primary_key_index::orderedSuffixColumn(table);
    std::vector<CompositeSuffixBound> bounds;
    auto inputs = compactCompositeInputs(table, context.conds, &bounds, suffix);
    if (inputs.empty()) return {};
    if (suffix && inputs.back().column == *suffix) suffix.reset();
    std::optional<CompositeSuffixOrder> order;
    bool leadingOnly = context.conds.size() == inputs.size();
    if (suffix && table.cols[*suffix].dataName == context.orderByCol &&
        !context.distinct && context.disjunctiveConds.empty() && context.aggregateItems.empty() &&
        context.groupByCols.empty() && context.groupingSets.empty() &&
        context.windowFunctions.empty() && context.semiJoins.empty() &&
        context.existenceFilters.empty() && context.quantifiedSubqueries.empty() &&
        std::all_of(context.conds.begin(), context.conds.end(),
            [&](const auto& condition) {
                return safeOrderedCompositePredicate(table, condition, inputs, leadingOnly);
            })) {
        order = CompositeSuffixOrder{context.orderByAsc, std::nullopt, {}};
        if (leadingOnly && context.limit &&
            context.limit <= std::numeric_limits<size_t>::max() - context.offset)
            order->fetchCount = context.limit + context.offset;
    }
    auto fallback = std::make_unique<TableScanOp>(engine, context.dbname, context.tablename);
    return std::make_unique<CompositePrimaryKeyScanOp>(engine, context.dbname, table,
        std::move(inputs), std::move(fallback), nullptr, suffix, std::move(bounds), order);
}

OpPtr QueryPlanner::buildSelectPlan(StorageEngine* engine, const PlanContext& inputContext) {
    auto ctx = inputContext;
    const auto table = engine->getTableSchema(ctx.dbname, ctx.tablename);
    PlanningRelationMetadata relation{engine, ctx, table};
    // Prepare once before selecting an access path: both the index key and
    // its heap/residual predicate must consume the same canonical datum.
    ctx.conds = prepareUnknownBitPredicateInputs(table, ctx.conds);
    for (auto& branch : ctx.disjunctiveConds)
        branch = prepareUnknownBitPredicateInputs(table, branch);
    validatePredicateTypes(table, ctx.conds, ctx.dbname, engine);
    for (const auto& branch : ctx.disjunctiveConds)
        validatePredicateTypes(table, branch, ctx.dbname, engine);
    const bool allNullBranches=!ctx.disjunctiveConds.empty() &&
        std::all_of(ctx.disjunctiveConds.begin(),ctx.disjunctiveConds.end(),nullPredicateBranch);
    ctx.disjunctiveConds.erase(std::remove_if(ctx.disjunctiveConds.begin(),ctx.disjunctiveConds.end(),nullPredicateBranch),
        ctx.disjunctiveConds.end());
    OpPtr root;
    if (nullPredicateBranch(ctx.conds) || allNullBranches)
        root=std::make_unique<MaterializedRowsOp>(std::vector<std::string>{});

    // Choose between the protected IndexScan and TableScan paths.
    std::vector<StorageEngine::Condition> remainingConds = ctx.conds;
    // RLS is a relation-level security boundary.  Do not let an index,
    // bitmap, or parallel access path duplicate policy evaluation: use the
    // policy-aware TableScanOp and keep user predicates above it.
    const bool rlsApplies = engine->rlsAppliesTo(ctx.dbname, ctx.tablename);
    std::unique_ptr<CompositePrimaryKeyScanOp> compositeRange;
    if (!root && !rlsApplies) {
        compositeRange = compactCompositePrimarySource(engine, ctx, table);
        if (compositeRange && !compositeRange->readsSuffixRange()) root = std::move(compositeRange);
    }
    const bool useBitmap = !root && !rlsApplies && canUseBitmapHeapScan(relation);
    // GiST acceleration: a range or anchored-prefix predicate on a
    // .gist-indexed column narrows candidates via the sidecar overlap
    // scan.  All predicates stay in FilterOp above as the recheck
    // boundary, identical to the bitmap path.
    const bool useGiST = !root && !rlsApplies && canUseGiSTScan(relation);
    if (useBitmap) {
        // Keep all predicates for FilterOp's heap recheck.  The bitmap node
        // only narrows the candidate RID set; it is not a correctness filter.
        root = std::make_unique<BitmapHeapScanOp>(
            engine, ctx.dbname, ctx.tablename, ctx.conds);
    } else if (useGiST) {
        root = std::make_unique<GiSTScanOp>(
            engine, ctx.dbname, ctx.tablename, ctx.conds);
    } else if (!root && !rlsApplies && !remainingConds.empty() &&
               table.partitionType==TableSchema::PartitionType::None) {
        std::vector<size_t> candidates;
        for(size_t i=0;i<ctx.conds.size();++i) {
            const auto& condition=ctx.conds[i];
            if(condition.op!="=" || equalityIndexKeyIsEmpty(table,condition))continue;
            const bool primary=isSingleColumnPrimaryKey(table,condition.colName);
            if(primary || relation.hasBtreeColumn(condition.colName))
                candidates.push_back(i);
        }
        const auto selected=chooseEqualityIndexCondition(engine,ctx,table,candidates);
        if(selected) {
            const auto condition=ctx.conds[*selected];
            // Keep the existing heap/MVCC/SSI/full-value recheck inside
            // IndexScan. Partitioned row locators remain unsupported.
            root=std::make_unique<IndexScanOp>(engine,ctx.dbname,ctx.tablename,
                condition.colName,condition.value,condition.referenceInputOid);
            remainingConds.erase(remainingConds.begin()+*selected);
        }
    }

    if (!root && !rlsApplies) root = ordinaryBrinTableSource(engine, ctx, table);
    if (!root && !rlsApplies && ctx.disjunctiveConds.empty()) {
        if (auto range = storedZonedClockRange(*engine, ctx.dbname, table, remainingConds)) {
            auto fallback = std::make_unique<TableScanOp>(engine, ctx.dbname, ctx.tablename);
            root = std::make_unique<ZonedClockRangeScanOp>(*engine, ctx.dbname, table, std::move(*range), std::move(fallback));
        }
    }
    if (!root && compositeRange) root = std::move(compositeRange);
    if (!root) {
        // Keep the original typed predicate above a candidate-only lookup.
        // This also covers wire parameters expanded to explicit builtin
        // casts by the legacy frontend, rather than direct ParameterExprs.
        if (!rlsApplies && remainingConds.size() == 1 && remainingConds.front().op == "typedexpr") {
            SQLParser parser;
            auto parsed = parser.parseForBinding("SELECT " + remainingConds.front().value);
            const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
            const auto* equality = select && select->selectList.size() == 1 && !select->fromClause
                ? dynamic_cast<const BinaryOpExpr*>(select->selectList.front().expr.get()) : nullptr;
            if (equality && equality->op == "=") {
                const auto* column = dynamic_cast<const ColumnRefExpr*>(equality->left.get());
                const Expr* input = equality->right.get();
                if (!column) { column = dynamic_cast<const ColumnRefExpr*>(equality->right.get()); input = equality->left.get(); }
                if (column && column->schema.empty() && column->table.empty() &&
                    isSingleColumnPrimaryKey(table, column->column)) {
                    for (size_t i = 0; i < table.len(); ++i) {
                        const auto& key = table.cols[i];
                        const auto type = ExprHelper::canonicalResultTypeName(key.dataType);
                        if (key.dataName != column->column || key.isArray || key.generatedKind ||
                            !key.domainName.empty() || key.isUnsigned ||
                            (type != "smallint" && type != "integer" && type != "bigint")) continue;
                        const auto value = constantIntegerIndexInput(input, type);
                        if (!value) continue;
                        auto frozen = std::make_shared<PreparedQuery>();
                        frozen->parameters.push_back(*value);
                        root = std::make_unique<PreparedParameterIndexScanOp>(engine, ctx.dbname,
                            ctx.tablename, key.dataName, type, std::move(frozen), 0,
                            std::make_unique<TableScanOp>(engine, ctx.dbname, ctx.tablename));
                        break;
                    }
                }
            }
        }
    }
    if (!root) {
        if (!rlsApplies && parallelWorkers_ > 1) {
            root = std::make_unique<ParallelTableScanOp>(
                engine, ctx.dbname, ctx.tablename, parallelWorkers_);
        } else {
            root = std::make_unique<TableScanOp>(engine, ctx.dbname, ctx.tablename);
        }
    }

    // Add Filter if there are remaining conditions
    if (!remainingConds.empty()) {
        const auto& tbl = table;
        root = std::make_unique<FilterOp>(std::move(root), tbl, remainingConds);
    }
    if (!ctx.disjunctiveConds.empty()) {
        const auto& tbl = table;
        root = std::make_unique<FilterOp>(
            std::move(root), tbl, ctx.disjunctiveConds);
    }

    // Lower uncorrelated IN/NOT IN predicates after the outer filter and
    // before projection/aggregation.  The join only carries outer rows, so
    // it does not change the schema visible to all downstream operators.
    if (!ctx.semiJoins.empty()) {
        const auto& outerTbl = table;
        for (const auto& spec : ctx.semiJoins) {
            const std::string innerDb = spec.dbname.empty() ? ctx.dbname : spec.dbname;
            const TableSchema innerTbl = engine->getTableSchema(innerDb, spec.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, spec.tablename);
            if (!spec.innerConds.empty()) {
                validatePredicateTypes(innerTbl, spec.innerConds, innerDb, engine);
                inner = std::make_unique<FilterOp>(
                    std::move(inner), innerTbl, spec.innerConds);
            }
            if (!spec.correlations.empty()) {
                root = std::make_unique<SemiJoinOp>(
                    std::move(root), std::move(inner), outerTbl, innerTbl,
                    spec.correlations, spec.anti);
            } else {
                root = std::make_unique<SemiJoinOp>(
                    std::move(root), std::move(inner), outerTbl, innerTbl,
                    spec.outerColumn, spec.innerColumn, spec.anti);
            }
        }
    }

    // Lower uncorrelated EXISTS/NOT EXISTS after the outer filter.  The
    // existence predicate is independent of each outer row, so the inner
    // plan can be opened once and the outer row shape remains unchanged.
    if (!ctx.existenceFilters.empty()) {
        const auto& outerTbl = table;
        for (const auto& spec : ctx.existenceFilters) {
            const std::string innerDb = spec.dbname.empty() ? ctx.dbname : spec.dbname;
            const TableSchema innerTbl = engine->getTableSchema(innerDb, spec.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, spec.tablename);
            if (!spec.innerConds.empty()) {
                validatePredicateTypes(innerTbl, spec.innerConds, innerDb, engine);
                inner = std::make_unique<FilterOp>(
                    std::move(inner), innerTbl, spec.innerConds);
            }
            if (!spec.correlations.empty()) {
                // Correlated EXISTS: equality keys to the outer row lower
                // to semi-join keys (anti for NOT EXISTS); multiple
                // correlations join on a composite key.
                root = std::make_unique<SemiJoinOp>(
                    std::move(root), std::move(inner), outerTbl, innerTbl,
                    spec.correlations, spec.anti,
                    SemiJoinOp::NullSemantics::ExistsCorrelation);
            } else if (!spec.outerColumn.empty() && !spec.innerColumn.empty()) {
                // Correlated EXISTS: the equality to the outer row lowers
                // to a semi-join key (anti for NOT EXISTS).
                root = std::make_unique<SemiJoinOp>(
                    std::move(root), std::move(inner), outerTbl, innerTbl,
                    spec.outerColumn, spec.innerColumn, spec.anti,
                    SemiJoinOp::NullSemantics::ExistsCorrelation);
            } else {
                root = std::make_unique<ExistenceFilterOp>(
                    std::move(root), std::move(inner), spec.anti);
            }
        }
    }

    // Lower uncorrelated quantified subqueries after the outer filter.  The
    // inner value set is initialized once and the outer stream is evaluated
    // with SQL's ANY/ALL three-valued logic.
    if (!ctx.quantifiedSubqueries.empty()) {
        const auto& outerTbl = table;
        for (const auto& spec : ctx.quantifiedSubqueries) {
            const std::string innerDb = spec.dbname.empty() ? ctx.dbname : spec.dbname;
            const TableSchema innerTbl = engine->getTableSchema(innerDb, spec.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, spec.tablename);
            if (!spec.innerConds.empty()) {
                validatePredicateTypes(innerTbl, spec.innerConds, innerDb, engine);
                inner = std::make_unique<FilterOp>(
                    std::move(inner), innerTbl, spec.innerConds);
            }
            root = std::make_unique<QuantifiedSubqueryFilterOp>(
                std::move(root), std::move(inner), outerTbl, innerTbl,
                spec.outerColumn, spec.innerColumn, spec.op, spec.all);
        }
    }

    if (!ctx.groupByCols.empty()) {
        const auto& tbl = table;
        if (ctx.groupingSets.empty() && !rlsApplies && parallelWorkers_ > 1) {
            // Worker threads partition the buffered child rows into local
            // group buckets; results are merged and finalized once.
            root = std::make_unique<ParallelGroupAggregateOp>(
                std::move(root), tbl, ctx.groupByCols,
                ctx.aggregateItems, ctx.havingConds, parallelWorkers_);
        } else {
            root = std::make_unique<GroupAggregateOp>(
                std::move(root), tbl, ctx.groupByCols, ctx.groupingSets,
                ctx.aggregateItems, ctx.havingConds);
        }
    } else if (!ctx.aggregateItems.empty()) {
        const auto& tbl = table;
        if (!rlsApplies && parallelWorkers_ > 1) {
            root = std::make_unique<ParallelGroupAggregateOp>(
                std::move(root), tbl, std::vector<std::string>{},
                ctx.aggregateItems, ctx.havingConds, parallelWorkers_);
        } else {
            // A plain aggregate is one implicit empty grouping set.  Reusing
            // the same node keeps filtering, visibility, FILTER, DISTINCT
            // arguments, and aggregate formatting on one structured path.
            root = std::make_unique<GroupAggregateOp>(
                std::move(root), tbl, std::vector<std::string>{},
                std::vector<std::vector<std::string>>{}, ctx.aggregateItems,
                ctx.havingConds);
        }
    } else if (!ctx.windowFunctions.empty()) {
        const auto& tbl = table;
        root = std::make_unique<WindowOp>(std::move(root), tbl,
                                          ctx.windowTargets, ctx.windowFunctions,
                                          ctx.orderByCol, ctx.orderByAsc,
                                          ctx.orderByNullsFirst,ctx.hasExplicitOrderNulls);
    } else {
        // Add Sort if ORDER BY.  WindowOp sorts each window internally and
        // applies the final query ordering after it computes the values.
        if (!ctx.orderByCol.empty()) {
            const auto& tbl = table;
            // Parallel path: worker threads sort disjoint rid partitions and
            // GatherMerge k-way merges the sorted streams (PG Gather Merge).
            const uint32_t pageCount = engine->tableNumPages(ctx.dbname, ctx.tablename);
            if (!rlsApplies && !ctx.hasExplicitOrderNulls &&
                parallelWorkers_ > 1 && !engine->inTransaction() &&
                pageCount > 2 && ctx.conds.empty()) {
                const int activeWorkers = static_cast<int>(
                    std::min<size_t>(static_cast<size_t>(parallelWorkers_),
                                     static_cast<size_t>(pageCount - 1)));
                using Part = std::vector<std::pair<int64_t, std::string>>;
                std::vector<Part> parts(static_cast<size_t>(activeWorkers));
                std::atomic<bool> failed{false};
                const auto interruptState = currentQueryInterruptState();
                std::vector<std::thread> sorters;
                sorters.reserve(static_cast<size_t>(activeWorkers));
                for (int w = 0; w < activeWorkers; ++w) {
                    const uint32_t begin =
                        1 + static_cast<uint32_t>(w) * (pageCount - 1) /
                                static_cast<uint32_t>(activeWorkers);
                    const uint32_t end =
                        1 + static_cast<uint32_t>(w + 1) * (pageCount - 1) /
                                static_cast<uint32_t>(activeWorkers);
                    sorters.emplace_back([engine, &ctx, &tbl, &parts, &failed,
                                          interruptState, w,
                                          begin, end]() {
                        setCurrentQueryInterruptState(interruptState);
                        auto& part = parts[static_cast<size_t>(w)];
                        try {
                            if (!engine->forEachRowPageRange(
                                ctx.dbname, ctx.tablename, begin, end,
                                [&part](uint32_t pageId, uint16_t slotId,
                                        const char* data, size_t len) {
                                    part.emplace_back(0, std::string(data, len));
                                })) {
                                failed.store(true, std::memory_order_relaxed);
                                setCurrentQueryInterruptState(nullptr);
                                return;
                            }
                            checkForQueryInterrupt();
                            interruptibleSort(part.begin(), part.end(),
                                      [&tbl, &ctx](const std::pair<int64_t, std::string>& a,
                                                   const std::pair<int64_t, std::string>& b) {
                                          const std::string va =
                                              StorageEngine::extractColumnValueStatic(a.second, tbl, sortColIndex(tbl, ctx.orderByCol));
                                          const std::string vb =
                                              StorageEngine::extractColumnValueStatic(b.second, tbl, sortColIndex(tbl, ctx.orderByCol));
                                          return ctx.orderByAsc ? compareWindowValue(va, vb) < 0
                                                                : compareWindowValue(va, vb) > 0;
                                      });
                        } catch (...) {
                            failed.store(true, std::memory_order_relaxed);
                        }
                        setCurrentQueryInterruptState(nullptr);
                    });
                }
                for (auto& t : sorters) t.join();
                checkForQueryInterrupt();
                if (failed.load(std::memory_order_relaxed)) {
                    root = std::make_unique<SortOp>(std::move(root), tbl,
                        ctx.orderByCol, ctx.orderByAsc,
                        ctx.orderByNullsFirst, ctx.hasExplicitOrderNulls);
                } else {
                    std::vector<OpPtr> runs;
                    runs.reserve(parts.size());
                    for (auto& part : parts) {
                        std::vector<std::string> rows;
                        rows.reserve(part.size());
                        for (auto& pr : part) rows.push_back(std::move(pr.second));
                        runs.push_back(std::make_unique<MaterializedRowsOp>(std::move(rows)));
                    }
                    root = std::make_unique<GatherMergeOp>(
                        std::move(runs), tbl, ctx.orderByCol, ctx.orderByAsc);
                }
            } else {
                root = std::make_unique<SortOp>(std::move(root), tbl,
                    ctx.orderByCol, ctx.orderByAsc,
                    ctx.orderByNullsFirst, ctx.hasExplicitOrderNulls);
            }
        }

        // Always add a Project so the output is formatted text (not raw binary).
        // When selectCols is empty, Project emits all columns (SELECT * semantics).
        const auto& tbl = table;
        if (!ctx.projectionTargets.empty()) {
            const std::string innerDb = ctx.scalarSubquery.dbname.empty()
                ? ctx.dbname : ctx.scalarSubquery.dbname;
            const TableSchema innerTbl = engine->getTableSchema(
                innerDb, ctx.scalarSubquery.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, ctx.scalarSubquery.tablename);
            if (!ctx.scalarSubquery.innerConds.empty()) {
                validatePredicateTypes(innerTbl, ctx.scalarSubquery.innerConds, innerDb, engine);
                inner = std::make_unique<FilterOp>(
                    std::move(inner), innerTbl, ctx.scalarSubquery.innerConds);
            }
            root = std::make_unique<ScalarSubqueryProjectOp>(
                std::move(root), std::move(inner), tbl, innerTbl,
                ctx.projectionTargets, ctx.scalarSubquery.column);
        } else {
            root = std::make_unique<ProjectOp>(std::move(root), tbl, ctx.selectCols);
        }
    }

    // DISTINCT applies to the projected target list, not the hidden columns
    // carried by the scan.  Keeping it after Project is important for
    // queries such as SELECT DISTINCT department FROM employees.
    if (ctx.distinct) {
        root = std::make_unique<DistinctOp>(std::move(root));
    }

    // OFFSET is applied after projection/distinct and before LIMIT, matching
    // SQL's result-window semantics.
    if (ctx.offset > 0) {
        root = std::make_unique<OffsetOp>(std::move(root), ctx.offset);
    }

    // Add Limit
    if (ctx.limit > 0) {
        root = std::make_unique<LimitOp>(std::move(root), ctx.limit);
    }

    return root;
}

OpPtr QueryPlanner::buildDisjunctiveSelectPlan(
    StorageEngine* engine, const PlanContext& ctx,
    const std::vector<std::vector<StorageEngine::Condition>>& inputBranches) {
    const auto table = engine->getTableSchema(ctx.dbname, ctx.tablename);
    PlanningRelationMetadata relation{engine, ctx, table};
    std::vector<std::vector<StorageEngine::Condition>> branches;
    for (const auto& branch : inputBranches)
        branches.push_back(prepareUnknownBitPredicateInputs(table, branch));
    for (const auto& branch : branches)
        validatePredicateTypes(table, branch, ctx.dbname, engine);
    if (branches.empty()) return nullptr;
    const auto firstBranch=branches.front();
    branches.erase(std::remove_if(branches.begin(),branches.end(),nullPredicateBranch),branches.end());
    if (branches.empty()) {
        PlanContext empty=ctx;empty.conds=firstBranch;empty.disjunctiveConds.clear();
        return buildSelectPlan(engine,empty);
    }
    if(branches.size()==1 && inputBranches.size()>1) {
        PlanContext single=ctx;single.conds=branches.front();single.disjunctiveConds.clear();
        return buildSelectPlan(engine,single);
    }
    if (engine->rlsAppliesTo(ctx.dbname, ctx.tablename) ||
        !canUseBitmapOrScan(relation, branches)) return nullptr;

    OpPtr root = std::make_unique<BitmapOrHeapScanOp>(
        engine, ctx.dbname, ctx.tablename, branches);
    const auto& tbl = table;

    if (!ctx.orderByCol.empty()) {
        root = std::make_unique<SortOp>(std::move(root), tbl,
            ctx.orderByCol, ctx.orderByAsc,
            ctx.orderByNullsFirst, ctx.hasExplicitOrderNulls);
    }
    root = std::make_unique<ProjectOp>(std::move(root), tbl, ctx.selectCols);
    if (ctx.distinct) root = std::make_unique<DistinctOp>(std::move(root));
    if (ctx.offset > 0) root = std::make_unique<OffsetOp>(std::move(root), ctx.offset);
    if (ctx.limit > 0) root = std::make_unique<LimitOp>(std::move(root), ctx.limit);
    return root;
}

OpPtr QueryPlanner::buildAggregatePlan(StorageEngine* engine, const PlanContext& ctx,
                                        const std::vector<StorageEngine::AggItem>& items) {
    PlanContext aggregateContext = ctx;
    aggregateContext.aggregateItems = items;
    aggregateContext.groupByCols.clear();
    aggregateContext.groupingSets.clear();
    return buildSelectPlan(engine, aggregateContext);
}

OpPtr QueryPlanner::buildSetOperationPlan(OpPtr left, OpPtr right,
                                          SetOperationType type, bool all) {
    return std::make_unique<SetOperationOp>(std::move(left), std::move(right), type, all);
}

// Estimate the cost of a join algorithm for given table sizes & index availability.
static double estimateJoinCost(size_t leftRows, size_t rightRows,
                                bool rightIndexed,
                                const std::string& algo) {
    return QueryPlanner::costJoinAlgorithm(
        algo, static_cast<double>(leftRows), static_cast<double>(rightRows),
        rightIndexed);
}

void QueryPlanner::setCostParameter(const std::string& name, double value) {
    if (value < 0) return;
    if (name == "seq_page_cost") costModel_.seqPageCost = value;
    else if (name == "random_page_cost") costModel_.randomPageCost = value;
    else if (name == "cpu_tuple_cost") costModel_.cpuTupleCost = value;
    else if (name == "cpu_index_tuple_cost") costModel_.cpuIndexTupleCost = value;
    else if (name == "cpu_operator_cost") costModel_.cpuOperatorCost = value;
}

void QueryPlanner::setCostEnable(const std::string& algo, bool on) {
    if (algo == "nestloop" || algo == "nlj") costModel_.enableNestloop = on;
    else if (algo == "hashjoin" || algo == "hash") costModel_.enableHashJoin = on;
    else if (algo == "mergejoin" || algo == "merge") costModel_.enableMergeJoin = on;
}

double QueryPlanner::costJoinAlgorithm(const std::string& algo,
                                       double leftRows, double rightRows,
                                       bool rightIndexed) {
    // Custom hook wins when installed and it accepts the algorithm.
    if (costModel_.customCost) {
        double custom = costModel_.customCost(algo, leftRows, rightRows,
                                              rightIndexed);
        if (custom >= 0) return custom;
    }
    // Disabled algorithms price themselves out of consideration.
    if (algo == "nlj" && !costModel_.enableNestloop) return 1e18;
    if (algo == "hash" && !costModel_.enableHashJoin) return 1e18;
    if (algo == "merge" && !costModel_.enableMergeJoin) return 1e18;
    const CostModel& cm = costModel_;
    // Costs stay on the row-touch scale so the planner's absolute
    // thresholds (small-table NLJ shortcut, selectivity override) keep
    // their meaning; the GUC parameters modulate relative preferences.
    const double pageRatio = cm.randomPageCost / std::max(1e-9, cm.seqPageCost);
    if (algo == "nlj") {
        // NLJ: O(left * right) without index; O(left * log(right)) with
        // index on right.  Inner index probes scatter across the heap, so
        // they pay the random/sequential page ratio.
        double perLeft = rightRows;
        if (rightIndexed) {
            perLeft = std::max(1.0, rightRows / 10) * pageRatio;
        }
        return leftRows * perLeft * (1.0 + cm.cpuOperatorCost);
    }
    if (algo == "merge") {
        // Merge: O(left + right) sorted merge over sequential pages.
        return (leftRows + rightRows) * (1.0 + cm.cpuTupleCost);
    }
    if (algo == "hash") {
        // Hash: one build pass plus a probe pass; the build side pays the
        // CPU tuple cost twice (insert + probe hash).
        return (leftRows + rightRows * 1.2) * (1.0 + cm.cpuTupleCost);
    }
    return leftRows * rightRows;  // fallback (worst case cartesian)
}

double QueryPlanner::costScan(const std::string& strategy,
                              double tuples, double pages) {
    if (costModel_.customCost) {
        double custom = costModel_.customCost(strategy, tuples, 0.0, false);
        if (custom >= 0) return custom;
    }
    const CostModel& cm = costModel_;
    if (strategy == "seq_scan") {
        return pages * cm.seqPageCost + tuples * cm.cpuTupleCost;
    }
    if (strategy == "index_scan") {
        // Matching tuples cost an index-tuple CPU each; nonsequential heap
        // fetches pay the random-page price.
        return tuples * (cm.cpuIndexTupleCost + cm.randomPageCost);
    }
    return pages * cm.seqPageCost + tuples * cm.cpuTupleCost;
}

// Runtime statistics are useful only after an exact complete scan (and are
// invalidated when a relation is recreated/truncated).  Keep the durable
// ANALYZE count as the fallback so a partial/index scan cannot supply a
// lower-bound estimate. No evidence is distinct from an analyzed empty
// relation: use a nonzero project heuristic rather than assert zero rows.
static size_t plannerRowEstimate(StorageEngine* engine,
                                 const std::string& dbname,
                                 const std::string& tablename) {
    uint64_t rows = 0;
    if (getRuntimeLiveRowEstimate(dbname, tablename, rows)) {
        return static_cast<size_t>(rows);
    }
    size_t analyzedRows = 0;
    if (engine->tryGetTableRowCount(dbname, tablename, analyzedRows)) return analyzedRows;
    return 1000;
}

OpPtr QueryPlanner::buildJoinPlan(StorageEngine* engine, const std::string& dbname,
                                   const std::string& leftTable, const std::string& rightTable,
                                   const std::string& leftCol, const std::string& rightCol,
                                   const std::vector<StorageEngine::Condition>& conds,
                                   const std::set<std::string>& selectCols,
                                   const TableSchema* leftSchema,
                                   const TableSchema* rightSchema) {
    (void)conds;
    (void)selectCols;

    // Get table sizes for join ordering optimization
    size_t leftRows = plannerRowEstimate(engine, dbname, leftTable);
    size_t rightRows = plannerRowEstimate(engine, dbname, rightTable);

    // Check if join keys are indexed (candidate for MergeJoin)
    auto leftIdxCols = engine->getIndexedColumns(dbname, leftTable);
    auto rightIdxCols = engine->getIndexedColumns(dbname, rightTable);
    bool leftColIndexed = (std::find(leftIdxCols.begin(), leftIdxCols.end(), leftCol) != leftIdxCols.end());
    bool rightColIndexed = (std::find(rightIdxCols.begin(), rightIdxCols.end(), rightCol) != rightIdxCols.end());
    std::optional<TableSchema> loadedLeft, loadedRight;
    if (!leftSchema) leftSchema = &loadedLeft.emplace(engine->getTableSchema(dbname, leftTable));
    if (!rightSchema) rightSchema = &loadedRight.emplace(engine->getTableSchema(dbname, rightTable));
    const auto hasPrimaryKey = [](const TableSchema& schema, const std::string& name) {
        return std::any_of(schema.cols.begin(), schema.cols.end(), [&](const auto& column) {
            return column.dataName == name && column.isPrimaryKey;
        });
    };
    leftColIndexed = leftColIndexed || hasPrimaryKey(*leftSchema, leftCol);
    rightColIndexed = rightColIndexed || hasPrimaryKey(*rightSchema, rightCol);

    // Join selectivity from column statistics: an equality join emits about
    // max(ndistinct(left), ndistinct(right)) groups over the cartesian
    // product, so sel = 1 / max(nd_l, nd_r) (PostgreSQL's eqjoinsel for
    // the uniform case). Without stats the old heuristic costs stand.
    auto leftStats = engine->getColumnStats(dbname, leftTable, leftCol);
    auto rightStats = engine->getColumnStats(dbname, rightTable, rightCol);
    double ndL = static_cast<double>(leftStats.cardinality);
    double ndR = static_cast<double>(rightStats.cardinality);
    double joinSel = 0.0;
    bool haveJoinStats = ndL > 0 && ndR > 0;
    if (haveJoinStats) {
        joinSel = 1.0 / std::max(ndL, ndR);
        // Estimated output cardinality — recorded for cost comparison below.
        // NLJ with an inner index benefits the most from selectivity: its
        // per-outer cost is a lookup, and only matching rows carry forward.
    }
    double estJoinRows = haveJoinStats
        ? static_cast<double>(leftRows) * static_cast<double>(rightRows) * joinSel
        : 1e18;

    // Cost-based algorithm choice: try all three, pick the cheapest.
    double costNLJ = estimateJoinCost(leftRows, rightRows, rightColIndexed, "nlj");
    double costMerge = (leftColIndexed && rightColIndexed)
        ? estimateJoinCost(leftRows, rightRows, true, "merge")
        : 1e18;
    double costHash = estimateJoinCost(leftRows, rightRows, false, "hash");
    // With join stats, NLJ only pays off when its output is small: charge
    // NLJ with the materialization of its result when selectivity is known.
    if (costNLJ < 1e18 && haveJoinStats && !rightColIndexed) {
        costNLJ = static_cast<double>(leftRows) * static_cast<double>(rightRows)
                  * 0.5 + estJoinRows;
    }

    // A small-table shortcut is only valid while nested loop remains a
    // candidate. enable_nestloop prices the method out above; do not override
    // that explicit planner setting when another legal join is available.
    std::string chosenAlgo;
    if (leftRows < 50 && rightRows < 50 && costNLJ < 1e18) {
        chosenAlgo = "nlj";
    } else if (costMerge <= costHash && costMerge < costNLJ) {
        chosenAlgo = "merge";
    } else if (costHash < costNLJ * 0.8) {
        chosenAlgo = "hash";
    } else {
        chosenAlgo = "nlj";
    }

    // JOIN order optimization: put smaller table in the more expensive position.
    bool shouldSwap = false;
    if (chosenAlgo == "nlj" && rightRows < leftRows) {
        shouldSwap = true;  // Outer loop should be smaller for NLJ
    } else if (chosenAlgo == "hash" && rightRows > leftRows) {
        shouldSwap = true;  // Build side (right) should be smaller for HashJoin
    } else if (chosenAlgo == "hash" && rightRows == leftRows && haveJoinStats
               && ndL > ndR) {
        // Equal sizes: build on the denser-key side (fewer chain collisions
        // when probing the sparser side).
        shouldSwap = true;
    }

    std::string lTbl = shouldSwap ? rightTable : leftTable;
    std::string rTbl = shouldSwap ? leftTable : rightTable;
    std::string lCol = shouldSwap ? rightCol : leftCol;
    std::string rCol = shouldSwap ? leftCol : rightCol;

    auto leftScan = std::make_unique<TableScanOp>(engine, dbname, lTbl);
    auto rightScan = std::make_unique<TableScanOp>(engine, dbname, rTbl);

    if (chosenAlgo == "nlj") {
        return std::make_unique<NestedLoopJoinOp>(
            engine, dbname, std::move(leftScan), std::move(rightScan),
            lTbl, rTbl, lCol, rCol);
    }
    if (chosenAlgo == "hash" && !engine->inTransaction() &&
        parallelWorkers_ > 1) {
        return std::make_unique<ParallelHashJoinOp>(
            engine, dbname, std::move(leftScan), std::move(rightScan),
            lTbl, rTbl, lCol, rCol, parallelWorkers_);
    }
    if (chosenAlgo == "merge") {
        return std::make_unique<MergeJoinOp>(
            engine, dbname, std::move(leftScan), std::move(rightScan),
            lTbl, rTbl, lCol, rCol);
    }
    return std::make_unique<HashJoinOp>(
        engine, dbname, std::move(leftScan), std::move(rightScan),
        lTbl, rTbl, lCol, rCol);
}

// ========================================================================
// EXPLAIN with cost estimation
// ========================================================================

struct CostEstimate {
    double rows = 0;
    double cost = 0;
};

// Numeric-aware comparison for histogram bucketing (text compare falls back
// to lexicographic, which is correct for ISO dates and zero-padded numbers).
static bool statLess(const std::string& a, const std::string& b) {
    bool aNum = !a.empty() && a.find_first_not_of("0123456789.-") == std::string::npos;
    bool bNum = !b.empty() && b.find_first_not_of("0123456789.-") == std::string::npos;
    if (aNum && bNum) {
        try { return std::stod(a) < std::stod(b); } catch (...) {}
    }
    return a < b;
}

namespace {
// One Filter estimate can consider a column many times, including once for
// every value in an IN list. Keep evidence only for this filter's estimate.
class FilterSelectivity {
public:
    FilterSelectivity(StorageEngine* engine, const std::string& database, const std::string& table)
        : engine_(engine), database_(database), table_(table) {}

    double estimate(const StorageEngine::Condition& condition);

private:
    struct Evidence {
        std::optional<std::shared_ptr<const analyzed_statistics::Column>> typed;
        std::optional<StorageEngine::ColumnStats> legacy;
        std::optional<const Column*> column;
    };

    double estimate(const StorageEngine::Condition& condition, Evidence& evidence);
    const Column* column(const std::string& name, Evidence& evidence) {
        if (!evidence.column) {
            if (!schema_) schema_ = engine_->getTableSchema(database_, table_);
            evidence.column = nullptr;
            for (size_t i = 0; i < schema_->len(); ++i) {
                if (schema_->cols[i].dataName == name) {
                    evidence.column = &schema_->cols[i];
                    break;
                }
            }
        }
        return *evidence.column;
    }

    size_t analyzedRows() {
        if (!analyzedRows_) analyzedRows_ = engine_->getTableRowCount(database_, table_);
        return *analyzedRows_;
    }

    StorageEngine* engine_;
    const std::string& database_;
    const std::string& table_;
    std::map<std::string, Evidence> columns_;
    std::optional<TableSchema> schema_;
    std::optional<size_t> analyzedRows_;
    std::optional<size_t> estimatedRows_;
};

double FilterSelectivity::estimate(const StorageEngine::Condition& condition) {
    return estimate(condition, columns_[condition.colName]);
}

// Selectivity estimation consuming ANALYZE statistics:
//   '='  : MCV hit -> exact frequency; else 1/ndistinct (default 0.1)
//   '!=' : 1 - '=' selectivity
//   '<' '<=' '>' '>=' : equidepth histogram interpolation between bucket
//                       boundaries, clamped by min/max (default 0.3)
double FilterSelectivity::estimate(const StorageEngine::Condition& cond, Evidence& evidence) {
    if(cond.op=="isnull" || cond.op=="isnotnull" || cond.op=="=" || cond.op=="!=") {
        if (!evidence.typed)
            evidence.typed = engine_->getAnalyzedColumnStatistics(database_, table_, cond.colName);
        if (const auto* typed = evidence.typed->get()) {
            if(cond.op=="isnull")return typed->nullFraction;
            if(cond.op=="isnotnull")return 1.0-typed->nullFraction;
            if (const auto* field = column(cond.colName, evidence)) {
                const auto probe=cond.referenceInputOid?std::to_string(*cond.referenceInputOid):cond.value;
                if (!estimatedRows_) estimatedRows_ = plannerRowEstimate(engine_, database_, table_);
                if(const auto fraction=analyzedEqualityFraction(*field,*typed,*estimatedRows_,probe))
                    return cond.op=="="?*fraction:std::max(0.0,1.0-typed->nullFraction-*fraction);
            }
        }
    }
    if (cond.op == "in" || cond.op == "notin") {
        // PG scalargtsub/eqsel-sum semantics: sum the per-value equality
        // selectivities (MCV-exact where available), clamp IN to at most
        // 0.5..1 like PG's HALF/ONE defaults; NOT IN takes the complement.
        std::istringstream iss(cond.value);
        StorageEngine::Condition eq;
        eq.op = "=";
        eq.colName = cond.colName;
        double sum = 0.0;
        size_t k = 0;
        while (iss >> eq.value) {
            sum += estimate(eq, evidence);
            ++k;
        }
        if (k == 0) return cond.op == "in" ? 0.0 : 1.0;
        if (sum > 1.0) sum = 1.0;
        double sel = (cond.op == "in") ? sum : (1.0 - sum);
        if (sel <= 0.0) sel = 0.001;
        if (sel >= 1.0) sel = 0.999;
        return sel;
    }
    if (!evidence.legacy) evidence.legacy = engine_->getColumnStats(database_, table_, cond.colName);
    const auto& stats = *evidence.legacy;
    const double totalRows = static_cast<double>(stats.analyzedRows ? *stats.analyzedRows : analyzedRows());
    // NULL fraction (pg null_frac): 0 when stats predate nullCount.
    const double nullFrac = (totalRows > 0 && stats.nullCount > 0)
        ? std::min(1.0, static_cast<double>(stats.nullCount) / totalRows)
        : 0.0;
    if (cond.op == "isnull") {
        if (totalRows > 0 && stats.nullCount > 0) return nullFrac;
        return 0.1;   // historical default-null guess
    }
    if (cond.op == "isnotnull") {
        if (totalRows > 0 && stats.nullCount > 0) return 1.0 - nullFrac;
        return 0.9;
    }
    if (cond.op == "=") {
        // Most Common Values first: mcv entries are (value, row count), so
        // the selectivity of a hot value is count / table rows — exact.
        // NULLs never satisfy equality; the count already excludes them.
        const auto* field = column(cond.colName, evidence);
        if(field) {
            const auto probe=cond.referenceInputOid?std::to_string(*cond.referenceInputOid):cond.value;
            if(const auto fraction=analyzedEqualityFraction(*field,stats,totalRows,probe))return *fraction;
        }
        for (const auto& m : stats.mcv) {
            if (m.first == cond.value || (field && StorageEngine::compareValues(
                    *field, m.first, false, cond.value, false, "=")
                    == StorageEngine::PredicateTruth::True)) {
                if (totalRows > 0) return static_cast<double>(m.second) / totalRows;
                return 1.0 / static_cast<double>(stats.cardinality);
            }
        }
        if (stats.cardinality > 0) {
            // Non-null fraction times uniform 1/ndistinct (PG semantics).
            double s = (1.0 - nullFrac) / static_cast<double>(stats.cardinality);
            if (s <= 0.0) s = 0.001;
            return s;
        }
        return 0.1;
    }
    if (cond.op == "!=") {
        StorageEngine::Condition eqCond;
        eqCond.op = "=";
        eqCond.colName = cond.colName;
        eqCond.value = cond.value;
        return 1.0 - estimate(eqCond, evidence);
    }
    if (cond.op == "like") {
        // PG-style likesel: a literal prefix (LIKE 'abc%') bounds the
        // estimate by the fraction of values ordering below the prefix's
        // upper edge (prefix++ via last-char increment, histogram-driven).
        // Without a literal prefix keep the historical flat guess.
        std::string pat = cond.value;
        // Callers hand either the bare pattern or a quoted literal; accept
        // both (parseConditions strips, raw PlanContext conds may not).
        if (pat.size() >= 2 && pat.front() == '\'' && pat.back() == '\'') {
            pat = pat.substr(1, pat.size() - 2);
        }
        std::string prefix;
        for (size_t i = 0; i < pat.size(); ++i) {
            char ch = pat[i];
            if (ch == '%' || ch == '_') break;
            if (ch == '\\' && i + 1 < pat.size()) {   // escaped wildcard
                prefix += pat[++i];
                continue;
            }
            prefix += ch;
        }
        if (prefix.empty()) return 0.2;
        // prefix++ : increment the last character; carries saturate by
        // appending (max char -> grow the string), like PG's
        // make_greater_string in spirit.
        std::string upper = prefix;
        bool carry = true;
        for (int i = static_cast<int>(upper.size()) - 1; i >= 0 && carry; --i) {
            if (static_cast<unsigned char>(upper[i]) < 255) {
                ++upper[i];
                carry = false;
            } else {
                upper[i] = '\x01';
            }
        }
        if (carry) upper += '\x01';
        // sel(LIKE 'p%') ~ sel(prefix <= v < prefix++) via the histogram.
        StorageEngine::Condition lo;
        lo.op = ">=";
        lo.colName = cond.colName;
        lo.value = prefix;
        StorageEngine::Condition hi;
        hi.op = "<";
        hi.colName = cond.colName;
        hi.value = upper;
        double s = estimate(lo, evidence) * estimate(hi, evidence);
        // MCV correction: hot values inside the prefix range give exact
        // frequencies that the uniform histogram misses.
        double rows = static_cast<double>(analyzedRows());
        double mcvInside = 0.0;
        for (const auto& m : stats.mcv) {
            const std::string& v = m.first;
            bool ge = !statLess(v, prefix);
            bool lt = statLess(v, upper);
            if (ge && lt && rows > 0) mcvInside += static_cast<double>(m.second) / rows;
        }
        if (mcvInside > s) s = mcvInside;
        if (s <= 0.0) s = 0.001;
        if (s >= 1.0) s = 0.999;
        return s;
    }
    if (cond.op == "<" || cond.op == "<=" || cond.op == ">" || cond.op == ">=") {
        // Histogram interpolation: fraction of buckets whose range lies
        // below (or above) the probe value, with linear position inside the
        // straddling bucket.
        if (!stats.histogram.empty() && !cond.value.empty()) {
            const std::string& v = cond.value;
            bool below = (cond.op == "<" || cond.op == "<=");
            double total = 0.0;
            double n = static_cast<double>(stats.histogram.size());
            for (size_t i = 0; i < stats.histogram.size(); ++i) {
                const auto& b = stats.histogram[i];
                bool bucketBelow = below ? statLess(b.second, v) : statLess(v, b.first);
                bool straddle = !statLess(v, b.first) && !statLess(b.second, v);
                if (bucketBelow) {
                    total += 1.0;
                } else if (straddle) {
                    // linear position within the bucket
                    double lo = 0.0, hi = 0.0, probe = 0.0;
                    try {
                        lo = std::stod(b.first); hi = std::stod(b.second); probe = std::stod(v);
                    } catch (...) {
                        // text: use relative length position as a rough proxy
                        lo = 0.0;
                        hi = static_cast<double>(std::max(b.first.size(), b.second.size()));
                        probe = static_cast<double>(v.size());
                    }
                    double frac = (hi > lo) ? (probe - lo) / (hi - lo) : 0.5;
                    if (frac < 0) frac = 0;
                    if (frac > 1) frac = 1;
                    total += below ? frac : (1.0 - frac);
                }
            }
            double sel = total / n;
            if (sel <= 0.0) sel = 0.001;   // keep a floor for empty sides
            if (sel >= 1.0) sel = 0.999;
            return sel;
        }
        return 0.3;
    }
    return 0.3;
}

static double explainFilterSelectivity(const FilterOp& filter, StorageEngine* engine,
                                       const std::string& database) {
    const auto* child = filter.child();
    std::string table;
    if (const auto* scan = dynamic_cast<const TableScanOp*>(child)) table = scan->tableName();
    else if (const auto* scan = dynamic_cast<const ParallelTableScanOp*>(child)) table = scan->tableName();
    else if (const auto* scan = dynamic_cast<const IndexScanOp*>(child)) table = scan->tableName();
    else if (const auto* scan = dynamic_cast<const BitmapHeapScanOp*>(child)) table = scan->scanOrigin().tablename;
    else if (const auto* scan = dynamic_cast<const BitmapOrHeapScanOp*>(child)) table = scan->scanOrigin().tablename;

    FilterSelectivity estimator(engine, database, table);
    double selectivity = 1.0;
    for (const auto& condition : filter.conditions())
        selectivity *= estimator.estimate(condition);
    return selectivity;
}
} // namespace

// Stats-driven equality-join selectivity: 1 / max(ndistinct_l, ndistinct_r),
// defaulting to the historical 0.1 without ANALYZE stats.
static double estimateJoinSel(StorageEngine* engine, const std::string& dbname,
                              const std::string& leftTable, const std::string& leftCol,
                              const std::string& rightTable, const std::string& rightCol) {
    auto ls = engine->getColumnStats(dbname, leftTable, leftCol);
    auto rs = engine->getColumnStats(dbname, rightTable, rightCol);
    if (ls.cardinality > 0 && rs.cardinality > 0) {
        return 1.0 / std::max(static_cast<double>(ls.cardinality),
                              static_cast<double>(rs.cardinality));
    }
    return 0.1;
}

static double estimateEqualityIndexRows(StorageEngine* engine,const std::string& database,
                                       const IndexScanOp& index) {
    const auto table=engine->getTableSchema(database,index.tableName());
    const double rows=plannerRowEstimate(engine,database,index.tableName());
    if(!isSingleColumnPrimaryKey(table,index.colName()))
        if(const auto typed=engine->getAnalyzedColumnStatistics(database,index.tableName(),index.colName()))
            for(size_t i=0;i<table.len();++i)if(table.cols[i].dataName==index.colName()) {
                if(const auto fraction=analyzedEqualityFraction(table.cols[i],*typed,rows,index.value()))
                    return rows>0?std::max(1.0,rows**fraction):0;
                break;
            }
    const auto statistics=engine->getColumnStats(database,index.tableName(),index.colName());
    size_t analyzedRows=0;
    const bool analyzed=engine->tryGetTableRowCount(database,index.tableName(),analyzedRows);
    double fallback=index.colName().empty()?1.0:5.0;
    for(size_t i=0;i<table.len();++i)if(table.cols[i].dataName==index.colName()) {
        if(table.cols[i].isPrimaryKey)fallback=1.0;
        if(analyzed)if(const auto fraction=analyzedEqualityFraction(table.cols[i],statistics,analyzedRows,index.value()))
            return rows>0?std::max(1.0,rows**fraction):0;
        break;
    }
    return statistics.cardinality?std::max(1.0,rows/statistics.cardinality):fallback;
}

static std::string costRowsStr(const CostEstimate& est, const QueryPlanner::ExplainOptions& opts) {
    if (!opts.costs) return "";
    return "  cost=" + std::to_string(static_cast<int>(est.cost)) +
           "  rows=" + std::to_string(static_cast<int>(est.rows));
}

// Append EXPLAIN ANALYZE actuals to the last emitted plan line: PG-style
// " (actual time=X rows=N loops=M)". Only instrumented operators report;
// non-instrumented ones keep their estimate-only line.
static void appendActuals(std::string& out, const IOperator* op,
                          const QueryPlanner::ExplainOptions& opts) {
    if (!opts.analyze || !op || op->runtimeLoops() == 0) return;
    // strip the trailing newline of the last line, append, restore
    if (out.empty() || out.back() != '\n') return;
    out.pop_back();
    char buf[96];
    if (opts.timing) {
        std::snprintf(buf, sizeof(buf), "  (actual time=%.3f rows=%llu loops=%llu)",
                      op->runtimeMs(), (unsigned long long)op->runtimeRows(),
                      (unsigned long long)op->runtimeLoops());
    } else {
        std::snprintf(buf, sizeof(buf), "  (actual rows=%llu loops=%llu)",
                      (unsigned long long)op->runtimeRows(),
                      (unsigned long long)op->runtimeLoops());
    }
    out += buf;
    out += "\n";
}

// Bitmap operators collect their index RID sets internally rather than
// exposing child plan operators. Until AM-specific bitmap selectivity/cost
// callbacks exist, retain the full relation as a conservative candidate bound
// and use the existing index-access cost model; never substitute Unknown/zero.
static CostEstimate bitmapExplainEstimate(StorageEngine* engine,
                                           const std::string& dbname,
                                           const std::string& table) {
    CostEstimate estimate;
    estimate.rows = static_cast<double>(plannerRowEstimate(engine, dbname, table));
    estimate.cost = QueryPlanner::costScan("index_scan", estimate.rows, 0.0);
    return estimate;
}

static CostEstimate explainOp(Operator* op, int indent,
                              StorageEngine* engine,
                              const std::string& dbname,
                              std::string& out,
                              const QueryPlanner::ExplainOptions& opts) {
    std::string prefix(indent * 2, ' ');
    CostEstimate est;

    if (auto* brin = dynamic_cast<BrinScanOp*>(op)) {
        est.rows = brin->estimatedRows(); est.cost = brin->estimatedCost();
        out += prefix + "BrinScan(table=" + brin->table() + ", " + brin->condition() +
               ", pages=" + std::to_string(brin->plannedPages()) + ")" + costRowsStr(est, opts) + "\n";
        for (auto* child : brin->preparedPlanChildren())
            explainOp(child, indent + 1, engine, dbname, out, opts);
    } else if (const auto name = op->preparedPlanNodeName(); !name.empty()) {
        const auto children = op->preparedPlanChildren();
        est.rows = children.empty() ? 1.0 : 0.0;
        for (auto* child : children) {
            const auto estimate = explainOp(child, indent + 1, engine, dbname, out, opts);
            est.rows += estimate.rows; est.cost += estimate.cost;
        }
        est.cost += est.rows * 0.1;
        out += prefix + name;
        const auto attributes=op->preparedPlanAttributes();
        if(!attributes.empty()) {
            out += " (";bool first=true;
            for(const auto& field:attributes) {
                if(!first)out+=", ";
                first=false;
                out+=field.first+"="+field.second;
            }
            out += ")";
        }
        out += costRowsStr(est, opts) + "\n";
    } else if (auto* pscan = dynamic_cast<ParallelTableScanOp*>(op)) {
        double rows = static_cast<double>(plannerRowEstimate(
            engine, dbname, pscan->tableName()));
        est.rows = rows;
        est.cost = rows / std::max(1, pscan->workers());
        out += prefix + "ParallelTableScan(table=" + pscan->tableName() +
               ", workers=" + std::to_string(pscan->workers()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* pagg = dynamic_cast<ParallelGroupAggregateOp*>(op)) {
        CostEstimate child = explainOp(pagg->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows / 10.0;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = child.cost / std::max(1, pagg->workers());
        out += prefix + "ParallelAggregate(workers=" +
               std::to_string(pagg->workers()) +
               (pagg->usedParallelWorkers() ? ", used" : ", fallback") + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* pjoin = dynamic_cast<ParallelHashJoinOp*>(op)) {
        CostEstimate left = explainOp(pjoin->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(pjoin->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows
                   * estimateJoinSel(engine, dbname, pjoin->leftTable(), pjoin->leftColumn(),
                                     pjoin->rightTable(), pjoin->rightColumn());
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + right.rows * 2.0 / std::max(1, pjoin->workers());
        out += prefix + "ParallelHashJoin(" + pjoin->leftTable() + ", " +
               pjoin->rightTable() + ", workers=" + std::to_string(pjoin->workers()) +
               (pjoin->usedParallelWorkers() ? ", used" : ", fallback") + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* gm = dynamic_cast<GatherMergeOp*>(op)) {
        double rows = 0, cost = 0;
        for (size_t i = 0; i < gm->inputCount(); ++i) {
            CostEstimate child = explainOp(gm->childAt(i), indent + 1, engine, dbname, out, opts);
            rows += child.rows;
            cost = std::max(cost, child.cost);
        }
        est.rows = rows;
        est.cost = cost + rows * 0.01;
        out += prefix + "GatherMerge(sort=" + gm->sortColumn() +
               (gm->ascending() ? " ASC" : " DESC") +
               ", streams=" + std::to_string(gm->inputCount()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* scan = dynamic_cast<TableScanOp*>(op)) {
        double rows = static_cast<double>(plannerRowEstimate(
            engine, dbname, scan->tableName()));
        est.rows = rows;
        est.cost = rows * 1.0;
        out += prefix + "TableScan(table=" + scan->tableName() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* idx = dynamic_cast<IndexScanOp*>(op)) {
        const double rows=estimateEqualityIndexRows(engine,dbname,*idx);
        est.rows = rows;
        est.cost = rows * 2.0;
        out += prefix + "IndexScan(table=" + idx->tableName() +
               ", col=" + idx->colName() + ", val=" + idx->value() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* bitmap = dynamic_cast<BitmapHeapScanOp*>(op)) {
        const auto table = bitmap->scanOrigin().tablename;
        est = bitmapExplainEstimate(engine, dbname, table);
        out += prefix + "BitmapHeapScan(table=" + table + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* bitmapOr = dynamic_cast<BitmapOrHeapScanOp*>(op)) {
        const auto table = bitmapOr->scanOrigin().tablename;
        est = bitmapExplainEstimate(engine, dbname, table);
        out += prefix + "BitmapOrHeapScan(table=" + table + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* gist = dynamic_cast<GiSTScanOp*>(op)) {
        // The sidecar overlap returns candidates, not final rows; the
        // table row count is the safest upper bound without bucket stats.
        double rows = static_cast<double>(
            plannerRowEstimate(engine, dbname, gist->tableName()));
        if (rows < 1.0) rows = 1.0;
        est.rows = rows;
        est.cost = rows * 3.0;   // random-ish heap fetches, heavier than seq
        out += prefix + "GiSTScan(table=" + gist->tableName() +
               ", " + gist->describeConds() + ")" + costRowsStr(est, opts) + "\n";

    } else if (auto* filt = dynamic_cast<FilterOp*>(op)) {
        CostEstimate child = explainOp(filt->child(), indent + 1, engine, dbname, out, opts);
        const double sel = explainFilterSelectivity(*filt, engine, dbname);
        est.rows = child.rows>0?std::max(1.0,child.rows*sel):0;
        est.cost = child.cost + est.rows * 0.5;
        out += prefix + "Filter" + costRowsStr(est, opts) + "\n";

    } else if (auto* semi = dynamic_cast<SemiJoinOp*>(op)) {
        CostEstimate outer = explainOp(semi->outerChild(), indent + 1,
                                       engine, dbname, out, opts);
        CostEstimate inner = explainOp(semi->innerChild(), indent + 1,
                                       engine, dbname, out, opts);
        est.rows = outer.rows * 0.5;
        if (est.rows < 1.0 && outer.rows > 0.0) est.rows = 1.0;
        est.cost = outer.cost + inner.cost + inner.rows;
        out += prefix + (semi->isAnti() ? "AntiJoin" : "SemiJoin") +
               "(outer_col=" + semi->outerColumn() +
               ", inner_col=" + semi->innerColumn() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* quantified = dynamic_cast<QuantifiedSubqueryFilterOp*>(op)) {
        CostEstimate outer = explainOp(quantified->outerChild(), indent + 1,
                                       engine, dbname, out, opts);
        CostEstimate inner = explainOp(quantified->innerChild(), indent + 1,
                                       engine, dbname, out, opts);
        est.rows = outer.rows * 0.5;
        if (est.rows < 1.0 && outer.rows > 0.0) est.rows = 1.0;
        est.cost = outer.cost + inner.cost + outer.rows * inner.rows;
        out += prefix + "QuantifiedSubqueryFilter(outer_col=" +
               quantified->outerColumn() + ", op=" + quantified->op() +
               ", quantifier=" + (quantified->isAll() ? "ALL" : "ANY") + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* exists = dynamic_cast<ExistenceFilterOp*>(op)) {
        CostEstimate outer = explainOp(exists->outerChild(), indent + 1,
                                       engine, dbname, out, opts);
        CostEstimate inner = explainOp(exists->innerChild(), indent + 1,
                                       engine, dbname, out, opts);
        est.rows = outer.rows * 0.5;
        if (est.rows < 1.0 && outer.rows > 0.0) est.rows = 1.0;
        est.cost = outer.cost + inner.cost + inner.rows;
        out += prefix + (exists->isAnti() ? "AntiExistenceFilter" : "ExistenceFilter") +
               costRowsStr(est, opts) + "\n";

    } else if (auto* scalar = dynamic_cast<ScalarSubqueryProjectOp*>(op)) {
        CostEstimate outer = explainOp(scalar->outerChild(), indent + 1,
                                       engine, dbname, out, opts);
        CostEstimate inner = explainOp(scalar->innerChild(), indent + 1,
                                       engine, dbname, out, opts);
        est.rows = outer.rows;
        est.cost = outer.cost + inner.cost + outer.rows * 0.1;
        out += prefix + "ScalarSubqueryProject(inner_col=" +
               scalar->innerColumn() + ")" + costRowsStr(est, opts) + "\n";

    } else if (auto* proj = dynamic_cast<ProjectOp*>(op)) {
        CostEstimate child = explainOp(proj->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows;
        est.cost = child.cost + child.rows * 0.1;
        out += prefix + "Project" + costRowsStr(est, opts) + "\n";

    } else if (auto* window = dynamic_cast<WindowOp*>(op)) {
        CostEstimate child = explainOp(window->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows;
        const double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        out += prefix + "WindowAgg(functions=" +
               std::to_string(window->functions().size()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* group = dynamic_cast<GroupAggregateOp*>(op)) {
        CostEstimate child = explainOp(group->child(), indent + 1, engine, dbname, out, opts);
        const double sets = static_cast<double>(group->groupingSetCount());
        est.rows = std::max(1.0, child.rows / std::max(1.0, sets));
        est.cost = child.cost + child.rows * 0.75;
        out += prefix + "GroupAggregate(grouping_sets=" +
               std::to_string(group->groupingSetCount()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* sort = dynamic_cast<SortOp*>(op)) {
        CostEstimate child = explainOp(sort->child(), indent + 1, engine, dbname, out, opts);
        const bool presorted = openedCompositeOrder(sort->child()).has_value();
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = presorted ? child.cost : child.cost + child.rows * logFactor * 0.1;
        out += prefix + (presorted ? "Sort(presorted=true)" : "Sort") + costRowsStr(est, opts) + "\n";

    } else if (auto* off = dynamic_cast<OffsetOp*>(op)) {
        CostEstimate child = explainOp(off->child(), indent + 1, engine, dbname, out, opts);
        est.rows = std::max(0.0, child.rows - static_cast<double>(off->offset()));
        est.cost = child.cost + est.rows * 0.01;
        out += prefix + "Offset(offset=" + std::to_string(off->offset()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* lim = dynamic_cast<LimitOp*>(op)) {
        CostEstimate child = explainOp(lim->child(), indent + 1, engine, dbname, out, opts);
        est.rows = std::min(child.rows, static_cast<double>(lim->limit()));
        est.cost = child.cost + est.rows * 0.01;
        out += prefix + "Limit(limit=" + std::to_string(lim->limit()) + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* dist = dynamic_cast<DistinctOp*>(op)) {
        CostEstimate child = explainOp(dist->child(), indent + 1, engine, dbname, out, opts);
        est.rows = child.rows * 0.5;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = child.cost + child.rows * 0.5;
        out += prefix + "Distinct" + costRowsStr(est, opts) + "\n";

    } else if (auto* join = dynamic_cast<NestedLoopJoinOp*>(op)) {
        CostEstimate left = explainOp(join->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(join->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows
                   * estimateJoinSel(engine, dbname, join->leftTable(), join->leftColumn(),
                                     join->rightTable(), join->rightColumn());
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + left.rows * right.cost;
        out += prefix + "NestedLoopJoin(" + join->leftTable() + ", " + join->rightTable() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* hjoin = dynamic_cast<HashJoinOp*>(op)) {
        CostEstimate left = explainOp(hjoin->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(hjoin->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows
                   * estimateJoinSel(engine, dbname, hjoin->leftTable(), hjoin->leftColumn(),
                                     hjoin->rightTable(), hjoin->rightColumn());
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + right.rows * 2.0;
        out += prefix + "HashJoin(" + hjoin->leftTable() + ", " + hjoin->rightTable() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* mjoin = dynamic_cast<MergeJoinOp*>(op)) {
        CostEstimate left = explainOp(mjoin->leftChild(), indent + 1, engine, dbname, out, opts);
        CostEstimate right = explainOp(mjoin->rightChild(), indent + 1, engine, dbname, out, opts);
        est.rows = left.rows * right.rows
                   * estimateJoinSel(engine, dbname, mjoin->leftTable(), mjoin->leftColumn(),
                                     mjoin->rightTable(), mjoin->rightColumn());
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + left.rows * std::log2(left.rows + 1) * 0.1
                   + right.rows * std::log2(right.rows + 1) * 0.1;
        out += prefix + "MergeJoin(" + mjoin->leftTable() + ", " + mjoin->rightTable() + ")" +
               costRowsStr(est, opts) + "\n";

    } else if (auto* mat = dynamic_cast<MaterializedRowsOp*>(op)) {
        est.rows = static_cast<double>(mat->rowCount());
        est.cost = est.rows * 0.01;
        out += prefix + "MaterializedRows" + costRowsStr(est, opts) + "\n";

    } else {
        out += prefix + "Unknown\n";
    }

    appendActuals(out, op, opts);
    return est;
}

std::string QueryPlanner::explain(OpPtr& plan, StorageEngine* engine,
                                  const std::string& dbname) {
    ExplainOptions opts;
    return explain(plan, engine, dbname, opts);
}

std::string QueryPlanner::explain(OpPtr& plan, StorageEngine* engine,
                                  const std::string& dbname,
                                  const ExplainOptions& opts) {
    std::string result;
    CostEstimate total = explainOp(plan.get(), 0, engine, dbname, result, opts);
    if (opts.settings) {
        auto cfg = g_config;
        result += "Settings: work_mem=" + std::to_string(effectiveWorkMemKb()) + "kB";
        result += ", enable_seqscan=" + std::string(cfg.enableSeqScan ? "on" : "off");
        result += ", enable_hashjoin=" + std::string(cfg.enableHashJoin ? "on" : "off");
        result += ", enable_mergejoin=" + std::string(cfg.enableMergeJoin ? "on" : "off");
        result += ", max_parallel_workers_per_gather=" +
                  std::to_string(cfg.maxParallelWorkersPerGather);
        result += ", checkpoint_interval=" + std::to_string(cfg.checkpointInterval) + "\n";
    }
    if (opts.costs) {
        result += "\nTotal estimated cost: " + std::to_string(static_cast<int>(total.cost));
        result += ", estimated rows: " + std::to_string(static_cast<int>(total.rows)) + "\n";
    }
    if (opts.buffers) {
        auto bpStats = engine->getBufferPoolStats();
        result += "Buffers: shared_hit=" + std::to_string(bpStats.totalHits);
        result += " shared_read=" + std::to_string(bpStats.totalMisses);
        result += " hit_rate=" + std::to_string(static_cast<int>(bpStats.hitRate)) + "%\n";
    }
    return result;
}

// ========================================================================
// EXPLAIN FORMAT JSON
// ========================================================================

static std::string jsonEscape(const std::string& s) {
    std::string out;
    for (char c : s) {
        if (c == '"') out += "\\\"";
        else if (c == '\\') out += "\\\\";
        else if (c == '\b') out += "\\b";
        else if (c == '\f') out += "\\f";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c == '\t') out += "\\t";
        else if (static_cast<unsigned char>(c) < 0x20) {
            static constexpr char hex[] = "0123456789abcdef";
            out += "\\u00";
            out += hex[static_cast<unsigned char>(c) >> 4];
            out += hex[static_cast<unsigned char>(c) & 15];
        }
        else out += c;
    }
    return out;
}

static std::string jsonCostRows(const CostEstimate& est, const QueryPlanner::ExplainOptions& opts) {
    if (!opts.costs) return "";
    return "\"cost\":" + std::to_string(static_cast<int>(est.cost)) + ","
         + "\"rows\":" + std::to_string(static_cast<int>(est.rows)) + ",";
}

static std::pair<std::string, CostEstimate> explainOpJson(Operator* op,
                                                            StorageEngine* engine,
                                                            const std::string& dbname,
                                                            const QueryPlanner::ExplainOptions& opts) {
    std::string json = "{";
    CostEstimate est;

    if (auto* brin = dynamic_cast<BrinScanOp*>(op)) {
        est.rows = brin->estimatedRows(); est.cost = brin->estimatedCost();
        json += "\"nodeType\":\"BrinScan\",\"table\":\"" + jsonEscape(brin->table()) + "\",";
        json += "\"indexCond\":\"" + jsonEscape(brin->condition()) + "\",";
        json += "\"candidatePages\":" + std::to_string(brin->plannedPages()) + ",";
        json += "\"usedSummary\":" + std::string(brin->usedSummary() ? "true" : "false") + ",";
        json += jsonCostRows(est, opts); json += "\"children\":[";
        bool first = true;
        for (auto* child : brin->preparedPlanChildren()) {
            if (!first) json += ","; first = false;
            json += explainOpJson(child, engine, dbname, opts).first;
        }
        json += "]";
    } else if (const auto name = op->preparedPlanNodeName(); !name.empty()) {
        const auto children = op->preparedPlanChildren();
        est.rows = children.empty() ? 1.0 : 0.0;
        std::string childJson;
        for (auto* child : children) {
            auto [text, estimate] = explainOpJson(child, engine, dbname, opts);
            if (!childJson.empty()) childJson += ",";
            childJson += text; est.rows += estimate.rows; est.cost += estimate.cost;
        }
        est.cost += est.rows * 0.1;
        json += "\"nodeType\":\"" + jsonEscape(name) + "\"," + jsonCostRows(est, opts);
        for(const auto& field:op->preparedPlanAttributes())
            json += "\""+jsonEscape(field.first)+"\":\""+jsonEscape(field.second)+"\",";
        json += "\"children\":[" + childJson + "]";
    } else if (auto* pscan = dynamic_cast<ParallelTableScanOp*>(op)) {
        double rows = static_cast<double>(plannerRowEstimate(
            engine, dbname, pscan->tableName()));
        est.rows = rows;
        est.cost = rows / std::max(1, pscan->workers());
        json += "\"nodeType\":\"Gather\",";
        json += "\"parallelWorkers\":" + std::to_string(pscan->workers()) + ",";
        json += "\"table\":\"" + jsonEscape(pscan->tableName()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* scan = dynamic_cast<TableScanOp*>(op)) {
        double rows = static_cast<double>(plannerRowEstimate(
            engine, dbname, scan->tableName()));
        est.rows = rows;
        est.cost = rows * 1.0;
        json += "\"nodeType\":\"TableScan\",";
        json += "\"table\":\"" + jsonEscape(scan->tableName()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* idx = dynamic_cast<IndexScanOp*>(op)) {
        const double rows=estimateEqualityIndexRows(engine,dbname,*idx);
        est.rows = rows;
        est.cost = rows * 2.0;
        json += "\"nodeType\":\"IndexScan\",";
        json += "\"table\":\"" + jsonEscape(idx->tableName()) + "\",";
        json += "\"column\":\"" + jsonEscape(idx->colName()) + "\",";
        json += "\"value\":\"" + jsonEscape(idx->value()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* bitmap = dynamic_cast<BitmapHeapScanOp*>(op)) {
        const auto table = bitmap->scanOrigin().tablename;
        est = bitmapExplainEstimate(engine, dbname, table);
        json += "\"nodeType\":\"BitmapHeapScan\",";
        json += "\"table\":\"" + jsonEscape(table) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* bitmapOr = dynamic_cast<BitmapOrHeapScanOp*>(op)) {
        const auto table = bitmapOr->scanOrigin().tablename;
        est = bitmapExplainEstimate(engine, dbname, table);
        json += "\"nodeType\":\"BitmapOrHeapScan\",";
        json += "\"table\":\"" + jsonEscape(table) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* gist = dynamic_cast<GiSTScanOp*>(op)) {
        est.rows = std::max(1.0, static_cast<double>(
            plannerRowEstimate(engine, dbname, gist->tableName())));
        est.cost = est.rows * 3.0;
        json += "\"nodeType\":\"GiSTScan\",";
        json += "\"table\":\"" + jsonEscape(gist->tableName()) + "\",";
        json += "\"usedIndex\":" + std::string(gist->usedIndex() ? "true" : "false") + ",";
        json += "\"indexCond\":\"" + jsonEscape(gist->describeConds()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[]";

    } else if (auto* filt = dynamic_cast<FilterOp*>(op)) {
        auto [childJson, child] = explainOpJson(filt->child(), engine, dbname, opts);
        const double sel = explainFilterSelectivity(*filt, engine, dbname);
        est.rows = child.rows>0?std::max(1.0,child.rows*sel):0;
        est.cost = child.cost + est.rows * 0.5;
        json += "\"nodeType\":\"Filter\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* semi = dynamic_cast<SemiJoinOp*>(op)) {
        auto [outerJson, outer] = explainOpJson(semi->outerChild(), engine,
                                                dbname, opts);
        auto [innerJson, inner] = explainOpJson(semi->innerChild(), engine,
                                                dbname, opts);
        est.rows = outer.rows * 0.5;
        if (est.rows < 1.0 && outer.rows > 0.0) est.rows = 1.0;
        est.cost = outer.cost + inner.cost + inner.rows;
        json += "\"nodeType\":\"" +
                std::string(semi->isAnti() ? "AntiJoin" : "SemiJoin") + "\",";
        json += "\"outerColumn\":\"" + jsonEscape(semi->outerColumn()) + "\",";
        json += "\"innerColumn\":\"" + jsonEscape(semi->innerColumn()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + outerJson + "," + innerJson + "]";

    } else if (auto* quantified = dynamic_cast<QuantifiedSubqueryFilterOp*>(op)) {
        auto [outerJson, outer] = explainOpJson(quantified->outerChild(), engine,
                                                dbname, opts);
        auto [innerJson, inner] = explainOpJson(quantified->innerChild(), engine,
                                                dbname, opts);
        est.rows = outer.rows * 0.5;
        if (est.rows < 1.0 && outer.rows > 0.0) est.rows = 1.0;
        est.cost = outer.cost + inner.cost + outer.rows * inner.rows;
        json += "\"nodeType\":\"QuantifiedSubqueryFilter\",";
        json += "\"outerColumn\":\"" + jsonEscape(quantified->outerColumn()) + "\",";
        json += "\"operator\":\"" + jsonEscape(quantified->op()) + "\",";
        json += "\"quantifier\":\"" +
                std::string(quantified->isAll() ? "ALL" : "ANY") + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + outerJson + "," + innerJson + "]";

    } else if (auto* exists = dynamic_cast<ExistenceFilterOp*>(op)) {
        auto [outerJson, outer] = explainOpJson(exists->outerChild(), engine,
                                                dbname, opts);
        auto [innerJson, inner] = explainOpJson(exists->innerChild(), engine,
                                                dbname, opts);
        est.rows = outer.rows * 0.5;
        if (est.rows < 1.0 && outer.rows > 0.0) est.rows = 1.0;
        est.cost = outer.cost + inner.cost + inner.rows;
        json += "\"nodeType\":\"" +
                std::string(exists->isAnti() ? "AntiExistenceFilter" : "ExistenceFilter") + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + outerJson + "," + innerJson + "]";

    } else if (auto* scalar = dynamic_cast<ScalarSubqueryProjectOp*>(op)) {
        auto [outerJson, outer] = explainOpJson(scalar->outerChild(), engine,
                                                dbname, opts);
        auto [innerJson, inner] = explainOpJson(scalar->innerChild(), engine,
                                                dbname, opts);
        est.rows = outer.rows;
        est.cost = outer.cost + inner.cost + outer.rows * 0.1;
        json += "\"nodeType\":\"ScalarSubqueryProject\",";
        json += "\"innerColumn\":\"" + jsonEscape(scalar->innerColumn()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + outerJson + "," + innerJson + "]";

    } else if (auto* proj = dynamic_cast<ProjectOp*>(op)) {
        auto [childJson, child] = explainOpJson(proj->child(), engine, dbname, opts);
        est.rows = child.rows;
        est.cost = child.cost + child.rows * 0.1;
        json += "\"nodeType\":\"Project\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* window = dynamic_cast<WindowOp*>(op)) {
        auto [childJson, child] = explainOpJson(window->child(), engine, dbname, opts);
        est.rows = child.rows;
        const double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        json += "\"nodeType\":\"WindowAgg\",";
        json += "\"functions\":" + std::to_string(window->functions().size()) + ",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* group = dynamic_cast<GroupAggregateOp*>(op)) {
        auto [childJson, child] = explainOpJson(group->child(), engine, dbname, opts);
        const double sets = static_cast<double>(group->groupingSetCount());
        est.rows = std::max(1.0, child.rows / std::max(1.0, sets));
        est.cost = child.cost + child.rows * 0.75;
        json += "\"nodeType\":\"GroupAggregate\",";
        json += "\"groupingSets\":" + std::to_string(group->groupingSetCount()) + ",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* sort = dynamic_cast<SortOp*>(op)) {
        auto [childJson, child] = explainOpJson(sort->child(), engine, dbname, opts);
        const bool presorted = openedCompositeOrder(sort->child()).has_value();
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = presorted ? child.cost : child.cost + child.rows * logFactor * 0.1;
        json += "\"nodeType\":\"Sort\",";
        if (presorted) json += "\"presortedInput\":true,";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* off = dynamic_cast<OffsetOp*>(op)) {
        auto [childJson, child] = explainOpJson(off->child(), engine, dbname, opts);
        est.rows = std::max(0.0, child.rows - static_cast<double>(off->offset()));
        est.cost = child.cost + est.rows * 0.01;
        json += "\"nodeType\":\"Offset\",";
        json += "\"offset\":" + std::to_string(off->offset()) + ",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* lim = dynamic_cast<LimitOp*>(op)) {
        auto [childJson, child] = explainOpJson(lim->child(), engine, dbname, opts);
        est.rows = std::min(child.rows, static_cast<double>(lim->limit()));
        est.cost = child.cost + est.rows * 0.01;
        json += "\"nodeType\":\"Limit\",";
        json += "\"limit\":" + std::to_string(lim->limit()) + ",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* dist = dynamic_cast<DistinctOp*>(op)) {
        auto [childJson, child] = explainOpJson(dist->child(), engine, dbname, opts);
        est.rows = child.rows * 0.5;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = child.cost + child.rows * 0.5;
        json += "\"nodeType\":\"Distinct\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + childJson + "]";

    } else if (auto* join = dynamic_cast<NestedLoopJoinOp*>(op)) {
        auto [leftJson, left] = explainOpJson(join->leftChild(), engine, dbname, opts);
        auto [rightJson, right] = explainOpJson(join->rightChild(), engine, dbname, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + left.rows * right.cost;
        json += "\"nodeType\":\"NestedLoopJoin\",";
        json += "\"leftTable\":\"" + jsonEscape(join->leftTable()) + "\",";
        json += "\"rightTable\":\"" + jsonEscape(join->rightTable()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + leftJson + "," + rightJson + "]";

    } else if (auto* hjoin = dynamic_cast<HashJoinOp*>(op)) {
        auto [leftJson, left] = explainOpJson(hjoin->leftChild(), engine, dbname, opts);
        auto [rightJson, right] = explainOpJson(hjoin->rightChild(), engine, dbname, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + right.rows * 2.0;
        json += "\"nodeType\":\"HashJoin\",";
        json += "\"leftTable\":\"" + jsonEscape(hjoin->leftTable()) + "\",";
        json += "\"rightTable\":\"" + jsonEscape(hjoin->rightTable()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + leftJson + "," + rightJson + "]";

    } else if (auto* mjoin = dynamic_cast<MergeJoinOp*>(op)) {
        auto [leftJson, left] = explainOpJson(mjoin->leftChild(), engine, dbname, opts);
        auto [rightJson, right] = explainOpJson(mjoin->rightChild(), engine, dbname, opts);
        est.rows = left.rows * right.rows * 0.1;
        if (est.rows < 1.0) est.rows = 1.0;
        est.cost = left.cost + right.cost + left.rows * std::log2(left.rows + 1) * 0.1
                   + right.rows * std::log2(right.rows + 1) * 0.1;
        json += "\"nodeType\":\"MergeJoin\",";
        json += "\"leftTable\":\"" + jsonEscape(mjoin->leftTable()) + "\",";
        json += "\"rightTable\":\"" + jsonEscape(mjoin->rightTable()) + "\",";
        json += jsonCostRows(est, opts);
        json += "\"children\":[" + leftJson + "," + rightJson + "]";

    } else {
        json += "\"nodeType\":\"Unknown\",";
        json += "\"children\":[]";
    }

    if (opts.analyze && op && op->runtimeLoops() != 0) {
        json += ",\"actualRows\":" + std::to_string(op->runtimeRows());
        json += ",\"actualLoops\":" + std::to_string(op->runtimeLoops());
        if (opts.timing) {
            json += ",\"actualTimeMs\":" + std::to_string(op->runtimeMs());
        }
    }
    json += "}";
    return {json, est};
}

std::string QueryPlanner::explainJson(OpPtr& plan, StorageEngine* engine,
                                      const std::string& dbname) {
    ExplainOptions opts;
    return explainJson(plan, engine, dbname, opts);
}

static std::string explainJsonDocument(Operator* plan, StorageEngine* engine,
                                       const std::string& dbname,
                                       const QueryPlanner::ExplainOptions& opts,
                                       const QueryPlanner::ExplainExecutionStats* execution) {
    auto [planJson, total] = explainOpJson(plan, engine, dbname, opts);
    std::string result = "{\n";
    result += "  \"plan\": " + planJson;
    if (opts.costs) {
        result += ",\n  \"totalCost\": " + std::to_string(static_cast<int>(total.cost)) + ",\n";
        result += "  \"totalRows\": " + std::to_string(static_cast<int>(total.rows));
    }
    if (opts.settings) {
        auto cfg = g_config;
        result += ",\n  \"settings\": {\n";
        result += "    \"workMemKb\": " + std::to_string(effectiveWorkMemKb()) + ",\n";
        result += "    \"enableSeqScan\": " + std::string(cfg.enableSeqScan ? "true" : "false") + ",\n";
        result += "    \"enableHashJoin\": " + std::string(cfg.enableHashJoin ? "true" : "false") + ",\n";
        result += "    \"enableMergeJoin\": " + std::string(cfg.enableMergeJoin ? "true" : "false") + ",\n";
        result += "    \"maxParallelWorkersPerGather\": " +
                  std::to_string(cfg.maxParallelWorkersPerGather) + ",\n";
        result += "    \"checkpointInterval\": " + std::to_string(cfg.checkpointInterval) + "\n";
        result += "  }";
    }
    if (opts.analyze && execution) {
        result += ",\n  \"actualRows\": " + std::to_string(execution->actualRows);
        if (opts.timing) {
            result += ",\n  \"executionTimeMs\": " + std::to_string(execution->executionTimeMs);
        }
    }
    if (opts.buffers) {
        StorageEngine::BufferPoolStats bpStats;
        if (opts.analyze && execution) {
            bpStats.totalHits = execution->sharedHits;
            bpStats.totalMisses = execution->sharedReads;
            const double totalAccesses = static_cast<double>(bpStats.totalHits) +
                                         static_cast<double>(bpStats.totalMisses);
            bpStats.hitRate = totalAccesses == 0.0 ? 0.0 :
                100.0 * static_cast<double>(bpStats.totalHits) / totalAccesses;
        } else {
            bpStats = engine->getBufferPoolStats();
        }
        result += ",\n  \"buffers\": {\n";
        result += "    \"sharedHit\": " + std::to_string(bpStats.totalHits) + ",\n";
        result += "    \"sharedRead\": " + std::to_string(bpStats.totalMisses) + ",\n";
        result += "    \"hitRate\": " + std::to_string(static_cast<int>(bpStats.hitRate)) + "\n";
        result += "  }";
    }
    result += "\n}\n";
    return result;
}

std::string QueryPlanner::explainJson(OpPtr& plan, StorageEngine* engine,
                                      const std::string& dbname,
                                      const ExplainOptions& opts) {
    return explainJsonDocument(plan.get(), engine, dbname, opts, nullptr);
}

std::string QueryPlanner::explainJson(OpPtr& plan, StorageEngine* engine,
                                      const std::string& dbname,
                                      const ExplainOptions& opts,
                                      const ExplainExecutionStats& execution) {
    return explainJsonDocument(plan.get(), engine, dbname, opts, &execution);
}

namespace {
class PreparedOperatorCursor final : public PreparedQueryCursor {
    OpPtr plan_;
    QueryRowDescriptor descriptor_;
    std::vector<std::string> canonicalTypes_;
    bool opened_ = false, closed_ = false, eof_ = false;
public:
    PreparedOperatorCursor(OpPtr plan, QueryRowDescriptor descriptor)
        : plan_(std::move(plan)), descriptor_(std::move(descriptor)) {
        if (!plan_) throw DbError("XX000", "prepared cursor received a null plan");
        canonicalTypes_.reserve(descriptor_.size());
        for (const auto& column : descriptor_)
            canonicalTypes_.push_back(ExprHelper::canonicalResultTypeName(column.type));
    }
    ~PreparedOperatorCursor() override {
        try { close(); } catch (...) {}
    }
    const QueryRowDescriptor& descriptor() const override { return descriptor_; }
    Operator* plan() const override { return plan_.get(); }
    bool supportsRestart() const override { return plan_->supportsPreparedOuterRow(); }
    void restart(const RowContext& row) override {
        if(!supportsRestart())throw DbError("0A000","prepared cursor source cannot rebind its caller row");
        close();
        if(!plan_->bindPreparedOuterRow(row))throw DbError("XX000","prepared cursor failed to bind its caller row");
        opened_=closed_=eof_=false;
    }
    void close() override {
        if (closed_) return;
        closed_ = true; // Even a throwing close is attempted only once.
        plan_->close();
    }
    bool next(std::vector<ExprValue>& row) override {
        row.clear();
        if (eof_) return false;
        if (closed_) throw DbError("XX000", "prepared cursor has already been closed");
        try {
            checkForQueryInterrupt();
            if (!opened_) {
                opened_ = true;
                for (size_t i = 0; i < descriptor_.size(); ++i)
                    if (descriptor_[i].type.empty() || canonicalTypes_[i] == "unknown")
                        throw DbError("XX000", "prepared cursor requires a declared output type");
                if (!plan_->supportsStructuredRows())
                    throw DbError("0A000", "prepared cursor requires a typed structured plan");
                if (!plan_->open())
                    throw DbError("XX000", plan_->errorMessage().empty()
                        ? "failed to open prepared cursor plan" : plan_->errorMessage());
            }
            std::string raw;
            if (!plan_->next(raw)) {
                if (plan_->hasError())
                    throw DbError("XX000", plan_->errorMessage().empty()
                        ? "failed while reading prepared cursor plan" : plan_->errorMessage());
                close();
                eof_ = true;
                return false;
            }
            checkForQueryInterrupt();
            if (plan_->lastStructuredValues(row)) {
                if (row.size() != descriptor_.size())
                    throw DbError("XX000", "prepared cursor plan lost its typed descriptor width");
                for (size_t i = 0; i < row.size(); ++i) {
                    const auto& value = row[i];
                    if (value.typeName == canonicalTypes_[i]) continue;
                    const auto physicalType = ExprHelper::canonicalResultTypeName(value.typeName);
                    if (physicalType == canonicalTypes_[i]) continue;

                    // An array domain uses its base type for storage and keeps
                    // the domain declaration on the owning value.
                    const auto& column = descriptor_[i];
                    const auto& declaration = column.typeMetadata;
                    const auto& data = value.compound;
                    if (declaration && declaration->kind == DatumKind::Array &&
                        data && data->type && data->validatedArrayDomainOid &&
                        column.typeOid == declaration->actualOid &&
                        data->validatedArrayDomainOid == declaration->actualOid &&
                        data->type->actualOid == declaration->actualOid &&
                        data->type->codecOid == declaration->codecOid &&
                        data->type->effectiveTypeMod == declaration->effectiveTypeMod &&
                        mapBuiltinTypeNameToOid(physicalType) == declaration->codecOid &&
                        value.isNull != data->arrayElements.has_value()) continue;
                    throw DbError("XX000", "prepared cursor plan changed its declared output type");
                }
                return true;
            }
            std::vector<std::string> cells;
            std::vector<bool> nulls;
            if (!plan_->lastStructuredRow(cells, nulls) ||
                cells.size() != descriptor_.size() || nulls.size() != cells.size())
                throw DbError("XX000", "prepared cursor plan lost its structured descriptor");
            for (size_t i = 0; i < cells.size(); ++i)
                row.emplace_back(canonicalTypes_[i], std::move(cells[i]), nulls[i]);
            return true;
        } catch (...) {
            row.clear();
            const auto failure = std::current_exception();
            try { close(); } catch (...) {}
            std::rethrow_exception(failure);
        }
    }
};
}

std::unique_ptr<PreparedQueryCursor> QueryPlanner::makePreparedCursor(
    OpPtr plan, QueryRowDescriptor descriptor) {
    return std::make_unique<PreparedOperatorCursor>(std::move(plan), std::move(descriptor));
}

static PlanExecutionResult executePlanResult(OpPtr plan, size_t maxRows, bool retainPrefix, bool captureTyped) {
    PlanExecutionResult result;
    if (!plan) {
        result.ok = false;
        result.error = "executor received a null plan";
        result.errorSqlState = "XX000";
        result.errorMessage = result.error;
        return result;
    }
    bool closeAttempted = false;
    const auto closePlan = [&] {
        if (!closeAttempted) {
            closeAttempted = true;
            plan->close();
        }
    };
    try {
        checkForQueryInterrupt();
        result.structuredRowsAvailable = plan->supportsStructuredRows();
        result.typedRowsAvailable = captureTyped && result.structuredRowsAvailable;
        if (!plan->open()) {
            result.ok = false;
            result.error = plan->errorMessage();
            if (result.error.empty()) result.error = "executor failed to open plan";
            result.errorSqlState = "XX000";
            result.errorMessage = result.error;
            result.structuredRowsAvailable = false;
            result.typedRowsAvailable = false;
            // Preserve the failure that caused cleanup, including when an
            // operator's close itself fails.
            try { closePlan(); } catch (...) {}
            return result;
        }
        if (result.structuredRowsAvailable)
            result.storedOutputRolesAvailable = plan->storedOutputRoles(result.storedOutputRoles);
        std::string row;
        while ((!maxRows || result.rows.size() < maxRows) && plan->next(row)) {
            checkForQueryInterrupt();
            result.rows.push_back(row);
            if (result.structuredRowsAvailable) {
                std::vector<std::string> cells;
                std::vector<bool> nulls;
                if (!plan->lastStructuredRow(cells, nulls) ||
                    cells.size() != nulls.size()) {
                    result.structuredRowsAvailable = false;
                    result.structuredRows.clear();
                    result.structuredNulls.clear();
                    result.storedOutputRolesAvailable = false;
                    result.storedOutputRoles.clear();
                } else {
                    if (result.storedOutputRolesAvailable &&
                        result.storedOutputRoles.size() != cells.size()) {
                        result.storedOutputRolesAvailable = false;
                        result.storedOutputRoles.clear();
                    }
                    result.structuredRows.push_back(std::move(cells));
                    result.structuredNulls.push_back(std::move(nulls));
                }
            }
            // Observe the same already-executed row, never evaluate a target
            // again. A missing owning carrier stays unavailable; the actual
            // binary cell receiver must not reconstruct compound display text.
            if (captureTyped && result.typedRowsAvailable) {
                std::vector<ExprValue> values;
                if (!result.structuredRowsAvailable || !plan->lastStructuredValues(values) ||
                    values.size() != result.structuredRows.back().size()) {
                    result.typedRowsAvailable = false;
                    result.typedRows.clear();
                } else result.typedRows.push_back(std::move(values));
            }
        }
        if (plan->hasError()) {
            result.ok = false;
            result.error = plan->errorMessage();
            if (result.error.empty()) result.error = "executor failed while reading plan";
            result.errorSqlState = "XX000";
            result.errorMessage = result.error;
            try { closePlan(); } catch (...) {}
        } else {
            closePlan();
        }
    } catch (const DbError& error) {
        result.ok = false;
        result.error = error.what();
        result.errorSqlState = error.sqlState();
        result.errorMessage = error.message();
        result.errorException = std::current_exception();
        try { closePlan(); } catch (...) {}
    } catch (...) {
        // Non-SQL exceptions keep their original type and identity. Cleanup
        // is attempted once and cannot replace the primary exception.
        const auto failure = std::current_exception();
        try { closePlan(); } catch (...) {}
        std::rethrow_exception(failure);
    }
    if (!result.ok && !retainPrefix) {
        result.rows.clear();
        result.structuredRows.clear();
        result.structuredNulls.clear();
        result.structuredRowsAvailable = false;
        result.typedRows.clear();
        result.typedRowsAvailable = false;
        result.storedOutputRoles.clear();
        result.storedOutputRolesAvailable = false;
    }
    return result;
}

PlanExecutionResult QueryPlanner::executePlanChecked(OpPtr plan, size_t maxRows, bool captureTyped) {
    return executePlanResult(std::move(plan), maxRows, false, captureTyped);
}

PlanExecutionResult QueryPlanner::executePlanWithPrefix(OpPtr plan, size_t maxRows, bool captureTyped) {
    return executePlanResult(std::move(plan), maxRows, true, captureTyped);
}

// Check if an index provides the required pathkey ordering.
static bool indexProvidesOrdering(StorageEngine* engine, const std::string& dbname,
                                   const std::string& tablename,
                                   const std::string& orderCol) {
    // Primary key index provides ordering on PK columns.
    TableSchema tbl = engine->getTableSchema(dbname, tablename);
    for (size_t i = 0; i < tbl.len(); ++i) {
        if (tbl.cols[i].dataName == orderCol && tbl.cols[i].isPrimaryKey)
            return true;
    }
    // Secondary index provides ordering on indexed columns.
    auto idxCols = engine->getIndexedColumns(dbname, tablename);
    return std::find(idxCols.begin(), idxCols.end(), orderCol) != idxCols.end();
}

// Build plan with pathkey awareness: if an index can provide the required
// ordering, use IndexScan to avoid a separate Sort step.
OpPtr QueryPlanner::buildSelectPlan(StorageEngine* engine, const PlanContext& ctx,
                                      const std::vector<PathKey>& requiredPathkeys,
                                      const std::vector<EquivalenceClass>& eqClasses) {
    (void)eqClasses;
    // Use equivalence classes to find additional filter conditions.
    // If t1.id = t2.fk is an equivalence class and we're scanning t1 with
    // WHERE t1.id = 5, we can also infer t2.fk = 5 for a subsequent join.
    // For now, the eqClasses are used to validate join conditions.

    // Check if ORDER BY can be satisfied by an index (pathkey optimization).
    bool useIndexForOrdering = false;
    if (!requiredPathkeys.empty() && !ctx.orderByCol.empty()) {
        const auto& pk = requiredPathkeys[0];
        if (pk.expr == ctx.orderByCol &&
            indexProvidesOrdering(engine, ctx.dbname, ctx.tablename, ctx.orderByCol)) {
            useIndexForOrdering = true;
        }
    }

    // Build the basic plan first.
    OpPtr plan = buildSelectPlan(engine, ctx);

    // If ordering is provided by the index, remove the SortOp (last in chain).
    if (useIndexForOrdering) {
        // Walk the operator tree and remove the topmost SortOp.
        OpPtr* cur = &plan;
        OpPtr prev;
        while (*cur) {
            // Check if this is a SortOp by dynamic_cast
            // Since we can't easily identify type without RTTI on the interface,
            // we rely on the fact that Sort is always the last operator before Limit.
            // For now, skip the optimization if we can't safely identify the Sort.
            break;  // Safe fallback: keep the Sort.
        }
    }

    return plan;
}

} // namespace dbms
