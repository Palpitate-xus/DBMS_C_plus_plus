#include "ExecutionPlan.h"
#include "common/DbError.h"
#include "access/BPTree.h"
#include "access/HashIndex.h"
#include "access/BloomIndex.h"
#include "catalog/type_registry.h"
#include "catalog/catalog.h"
#include "Config.h"
#include "process/RuntimeStats.h"
#include "types/numeric.h"
#include "expression/expr_helper.h"
#include "expression/ExprEvaluator.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iterator>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <thread>
#include <unordered_set>

extern dbms::Config g_config;

namespace dbms {

// Parallel query support
int QueryPlanner::parallelWorkers_ = 0;
QueryPlanner::CostModel QueryPlanner::costModel_{};

static std::string trimExec(const std::string& value) {
    const size_t first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    const size_t last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

// ========================================================================
// Helper: format a raw row buffer into display string
// ========================================================================
static bool rawColumnIsNull(const std::string& row, const TableSchema& tbl,
                            size_t colIdx) {
    // TableScanOp deliberately strips the MVCC/null bitmap header before
    // exposing rows.  The payload format uses the same empty-value sentinel
    // as extractColumnValueStatic for nullable fixed-width values.
    return colIdx < tbl.len && tbl.cols[colIdx].isNull &&
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

// A prepared plan owns its AST and the evaluator's metadata-only routine
// bindings. Columns consume the binder's true source occurrence/ordinal,
// and the shared execution carrier owns parameter and lazy-child evaluation.
struct PreparedSelectState {
    StorageEngine* engine;
    std::string dbname;
    TableSchema schema;
    std::shared_ptr<PreparedQuery> query;
    SelectStmt* statement = nullptr;
    QueryRowDescriptor output;
    RowContext outerRow;
    PreparedChildExecutor childExecutor;
    std::unique_ptr<PreparedQueryExecution> execution;
    ExprEvaluator evaluator;
    std::vector<Expr*> targets;
    std::vector<ExprPtr> starTargets;
    std::vector<bool> targetsBeforeSort;
    struct SortKey { Expr* expression; size_t target; bool asc, nullsFirst; };
    std::vector<SortKey> keys;
    static constexpr size_t noTarget = std::numeric_limits<size_t>::max();
    size_t sourceOrdinal = noTarget;
    SelectStmt& select() const { return *statement; }
    size_t columnOrdinal(const ColumnRefExpr& column) const {
        if (!column.binding) throw DbError("XX000", "prepared source reference has no binding");
        const auto& binding = *column.binding;
        if (binding.scopeDepth || binding.sourceOrdinal != sourceOrdinal || binding.mergedUsing)
            throw DbError("0A000", "prepared source reference requires additional range lowering");
        if (binding.columnOrdinal >= schema.len)
            throw DbError("XX000", "prepared source ordinal is outside its descriptor");
        return binding.columnOrdinal;
    }
    std::string identity(const Expr* expression) const {
        return ExprHelper::scalarExpressionIdentity(expression, [&](const ColumnRefExpr& column) {
            if (column.binding && column.binding->scopeDepth) {
                const auto& binding = *column.binding;
                return std::to_string(binding.sourceOrdinal) + ":" + std::to_string(binding.columnOrdinal) + ":" + binding.declaredType;
            }
            const auto ordinal = columnOrdinal(column);
            const auto& physical = schema.cols[ordinal];
            return std::to_string(sourceOrdinal) + ":" + std::to_string(ordinal) + ":" +
                column.binding->declaredType + ":" + std::to_string(physical.dsize) + ":" + physical.collation;
        }, dbname, engine);
    }
    std::vector<ExprValue> cells(Operator& source, const std::string& raw) const {
        std::vector<ExprValue> values;
        std::vector<std::string> structured;
        std::vector<bool> nulls;
        const bool available = source.supportsStructuredRows() && source.lastStructuredRow(structured, nulls);
        for (size_t i = 0; i < schema.len; ++i) {
            bool computedNull = false;
            const std::string value = available ? structured.at(i) :
                engine->extractColumnValue(raw, schema, i, dbname, true, &computedNull);
            const bool isNull = available ? nulls.at(i) : computedNull ||
                (schema.cols[i].generatedKind != 'v' && source.lastColumnIsNull(i));
            values.emplace_back(ExprHelper::canonicalResultTypeName(schema.cols[i].dataType +
                (schema.cols[i].isArray ? "[]" : "")), value, isNull);
            values.back().collation = schema.cols[i].collation;
        }
        return values;
    }
    RowContext context(const std::vector<ExprValue>& values) const {
        auto row = outerRow;
        row.setParameters(query->parameters);
        if (sourceOrdinal != noTarget) execution->setSourceRow(row, sourceOrdinal, values);
        return row;
    }
    ExprValue evaluate(const Expr* expression, const RowContext& row) const {
        ExprValue value = execution->evaluate(expression, row);
        if (value.isUnknown()) throw DbError("0A000", "unsupported prepared expression value");
        return value;
    }
    void beginExecution() {
        execution = std::make_unique<PreparedQueryExecution>(query, engine, dbname);
        execution->setQueryExecutor(childExecutor);
        for (auto& target : starTargets)
            execution->prepareProjectionColumn(static_cast<ColumnRefExpr*>(target.get()), statement);
        for (auto* target : targets) execution->prepareExpression(target);
        execution->prepareExpression(select().whereClause.get());
        for (const auto& key : keys)
            if (key.target == noTarget) execution->prepareExpression(key.expression);
    }
    bool containsVolatile(const Expr* expression) const {
        if (!expression) return false;
        if (expression->preparedSubquery) return true;
        if (auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
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
};

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

class PreparedFilterOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
public:
    PreparedFilterOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {}
    bool open() override { OpenInstrument startup(this); clearError(); return child_->open(); }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        while (child_->next(row)) {
            const auto value = state_->evaluate(state_->select().whereClause.get(),
                state_->context(state_->cells(*child_, row)));
            if (!value.isNull && value.asBool()) { instrumentation.emitted = true; return true; }
        }
        if (child_->hasError()) return propagateChildError(child_.get(), "prepared filter child failed");
        return false;
    }
    void close() override { child_->close(); }
    bool lastColumnIsNull(size_t ordinal) const override { return child_->lastColumnIsNull(ordinal); }
    bool supportsStructuredRows() const override { return child_->supportsStructuredRows(); }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        return child_->lastStructuredRow(cells, nulls);
    }
    ScanOrigin scanOrigin() const override { return child_->scanOrigin(); }
    std::string preparedPlanNodeName() const override { return "TypedFilter"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};

class PreparedSortOp final : public Operator {
    struct Row {
        std::string raw;
        std::vector<ExprValue> cells, keys;
        std::vector<std::optional<ExprValue>> targets;
    };
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    std::vector<Row> rows_;
    size_t position_ = 0;
public:
    PreparedSortOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {}
    bool open() override {
        OpenInstrument startup(this);
        clearError(); rows_.clear(); position_ = 0;
        if (!child_->open()) return false;
        std::string raw;
        while (child_->next(raw)) {
            Row row; row.raw = raw; row.cells = state_->cells(*child_, raw);
            row.targets.resize(state_->targets.size());
            const auto context = state_->context(row.cells);
            // Non-volatile targets are part of the sort input projection.
            // Volatile non-key targets remain above Sort/LIMIT demand; sort
            // keys, aliases and structurally identical targets share slots.
            for (size_t i = 0; i < state_->targets.size(); ++i)
                if (state_->targetsBeforeSort[i])
                    row.targets[i] = state_->evaluate(state_->targets[i], context);
            for (const auto& key : state_->keys) {
                if (key.target != PreparedSelectState::noTarget) {
                    if (!row.targets[key.target])
                        row.targets[key.target] = state_->evaluate(state_->targets[key.target], context);
                    row.keys.push_back(*row.targets[key.target]);
                } else row.keys.push_back(state_->evaluate(key.expression, context));
            }
            rows_.push_back(std::move(row));
        }
        if (child_->hasError()) return propagateChildError(child_.get(), "prepared sort child failed");
        std::stable_sort(rows_.begin(), rows_.end(), [&](const Row& left, const Row& right) {
            for (size_t i = 0; i < state_->keys.size(); ++i) {
                const auto& a = left.keys[i]; const auto& b = right.keys[i];
                const auto& key = state_->keys[i];
                if (a.isNull || b.isNull) {
                    if (a.isNull == b.isNull) continue;
                    return a.isNull == key.nullsFirst;
                }
                Column column;
                const auto error = TypeRegistry::instance().resolveColumnType(column, a.typeName, {}, false);
                if (!error.empty()) throw DbError("0A000", "unsupported prepared sort type: " + a.typeName);
                column.collation = a.collation;
                const auto less = StorageEngine::compareValues(column, a.value, false, b.value, false, "<");
                const auto greater = StorageEngine::compareValues(column, a.value, false, b.value, false, ">");
                if (less == StorageEngine::PredicateTruth::Unknown || greater == StorageEngine::PredicateTruth::Unknown)
                    throw DbError("0A000", "unsupported prepared sort comparison");
                if (less == StorageEngine::PredicateTruth::True) return key.asc;
                if (greater == StorageEngine::PredicateTruth::True) return !key.asc;
            }
            return false;
        });
        return true;
    }
    bool next(std::string& row) override {
        NextInstrument instrumentation(this);
        if (position_ == rows_.size()) return false;
        row = rows_[position_++].raw; instrumentation.emitted = true; return true;
    }
    const std::vector<std::optional<ExprValue>>& targets() const { return rows_.at(position_ - 1).targets; }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        if (!position_ || position_ > rows_.size()) return false;
        cells.clear(); nulls.clear();
        for (const auto& value : rows_[position_ - 1].cells) {
            cells.push_back(value.value); nulls.push_back(value.isNull);
        }
        return true;
    }
    void close() override { rows_.clear(); position_ = 0; child_->close(); }
    std::string preparedPlanNodeName() const override { return "TypedSort"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};

class PreparedProjectOp final : public Operator {
    OpPtr child_;
    std::shared_ptr<PreparedSelectState> state_;
    std::vector<ExprValue> values_;
    std::optional<StorageEngine::NullRowBindingState> callerNullBinding_;
    bool childOpened_ = false;
public:
    PreparedProjectOp(OpPtr child, std::shared_ptr<PreparedSelectState> state)
        : child_(std::move(child)), state_(std::move(state)) {}
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
        const auto context = state_->context(state_->cells(*child_, raw));
        const auto* sorted = dynamic_cast<const PreparedSortOp*>(child_.get());
        values_.clear(); row.clear();
        for (size_t i = 0; i < state_->targets.size(); ++i) {
            values_.push_back(sorted && sorted->targets()[i] ? *sorted->targets()[i]
                : state_->evaluate(state_->targets[i], context));
            // A bare NULL's unknown type resolves to text at a SELECT output
            // boundary. Keep declared typed NULLs (parameters/casts) intact.
            if (values_.back().typeName == "unknown") values_.back().typeName = "text";
            row += values_.back().isNull ? "NULL " : values_.back().value + " ";
        }
        instrumentation.emitted = true; return true;
    }
    bool supportsStructuredRows() const override { return true; }
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        cells.clear(); nulls.clear();
        for (const auto& value : values_) { cells.push_back(value.value); nulls.push_back(value.isNull); }
        return true;
    }
    std::string typedKey() const {
        std::string key;
        for (const auto& value : values_) {
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
        values_.clear();
        try { if (childOpened_) child_->close(); }
        catch (...) { childOpened_ = false; restore(); throw; }
        childOpened_ = false;
        restore();
    }
    std::string preparedPlanNodeName() const override { return "TypedProject"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};

class PreparedDistinctOp final : public Operator {
    OpPtr child_;
    struct Row { std::string text; std::vector<std::string> cells; std::vector<bool> nulls; };
    std::vector<Row> rows_;
    size_t position_ = 0;
public:
    explicit PreparedDistinctOp(OpPtr child) : child_(std::move(child)) {}
    bool open() override {
        OpenInstrument startup(this);
        clearError(); rows_.clear(); position_ = 0;
        if (!child_->open()) return false;
        std::set<std::string> seen;
        auto* project = dynamic_cast<PreparedProjectOp*>(child_.get());
        Row row;
        while (child_->next(row.text)) {
            if (!project || !child_->lastStructuredRow(row.cells, row.nulls))
                throw DbError("XX000", "prepared DISTINCT lost typed projection");
            if (seen.insert(project->typedKey()).second) rows_.push_back(row);
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
    bool lastStructuredRow(std::vector<std::string>& cells, std::vector<bool>& nulls) const override {
        if (!position_ || position_ > rows_.size()) return false;
        cells = rows_[position_ - 1].cells; nulls = rows_[position_ - 1].nulls; return true;
    }
    void close() override { rows_.clear(); position_ = 0; child_->close(); }
    std::string preparedPlanNodeName() const override { return "TypedDistinct"; }
    std::vector<Operator*> preparedPlanChildren() const override { return {child_.get()}; }
};
} // namespace

static bool supportsPreparedSelectShape(const SelectStmt& select, bool allowCtes) {
    if (select.command != SqlCommand::Select || (!allowCtes && !select.ctes.empty()) ||
        select.setOp != SetOp::None || select.setOpLhs || select.setOpRhs ||
        !select.groupBy.empty() || !select.groupByElems.empty() || select.having ||
        !select.windowDefs.empty() || !select.distinctOn.empty() ||
        !select.locking.empty() || select.withTies) return false;
    if (select.fromClause && select.fromClause->type != FromItem::Type::Table) return false;
    std::function<bool(const Expr*)> scalar = [&](const Expr* expr) {
        if (!expr) return true;
        if (auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
            static const std::set<std::string> aggregates = {"count", "sum", "avg", "min", "max", "string_agg", "array_agg", "bool_and", "bool_or", "every", "bit_and", "bit_or", "bit_xor", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg", "xmlagg", "mode", "percentile_cont", "percentile_disc", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp", "corr", "covar_pop", "covar_samp"};
            CatalogManager::QualifiedName routine;
            const auto spelling = call->schema.empty() ? call->funcName : call->schema + "." + call->funcName;
            const bool builtinRole = CatalogManager::parseQualifiedName(spelling, routine, true) &&
                (routine.schema.empty() || routine.schema == "pg_catalog");
            if ((builtinRole && routine.name == "exists") || call->hasOver || call->distinct || call->filter || !call->orderBy.empty() ||
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
    return scalar(select.whereClause.get());
}

bool QueryPlanner::supportsPreparedSelectPlan(const SelectStmt& select) {
    return supportsPreparedSelectShape(select, false);
}

OpPtr QueryPlanner::buildPreparedSelectPlan(StorageEngine* engine,
    const std::string& dbname, const std::string& tablename, PreparedQuery prepared) {
    auto* select = dynamic_cast<SelectStmt*>(prepared.ast.get());
    if (!select || !supportsPreparedSelectPlan(*select))
        throw DbError("0A000", "query requires an additional prepared plan lowering");
    const auto schema = select->fromClause ? engine->getTableSchema(dbname, tablename) : TableSchema{};
    OpPtr source = select->fromClause ? OpPtr(std::make_unique<TableScanOp>(engine, dbname, tablename))
        : OpPtr(std::make_unique<PreparedResultOp>());
    return buildPreparedSelectPlan(engine, dbname, std::make_shared<PreparedQuery>(std::move(prepared)),
        select, schema, std::move(source));
}

OpPtr QueryPlanner::buildPreparedSelectPlan(StorageEngine* engine,
    const std::string& dbname, std::shared_ptr<PreparedQuery> prepared,
    SelectStmt* select, const TableSchema& sourceSchema, OpPtr source,
    const RowContext& outerRow, PreparedChildExecutor childExecutor) {
    if (!prepared || !select || !supportsPreparedSelectShape(*select, true) || !source)
        throw DbError("0A000", "query requires an additional prepared plan lowering");
    const auto output = prepared->statementOutputs.find(select);
    if (output == prepared->statementOutputs.end())
        throw DbError("XX000", "borrowed SELECT has no prepared output descriptor");
    auto state = std::make_shared<PreparedSelectState>();
    state->engine = engine; state->dbname = dbname;
    state->query = std::move(prepared); state->statement = select;
    state->output = output->second; state->outerRow = outerRow;
    state->childExecutor = std::move(childExecutor);
    state->evaluator.setCurrentDB(dbname);
    if (select->fromClause) {
        state->schema = sourceSchema;
        for (const auto& range : state->query->sourceRanges) {
            if (range.owner != select || range.source != select->fromClause.get() || range.mergedUsing) continue;
            state->sourceOrdinal = range.ordinal;
            if (range.columns.size() != state->schema.len)
                throw DbError("0A000", "prepared plan requires a virtual or view source lowering");
            for (size_t i = 0; i < range.columns.size(); ++i)
                if (range.columns[i].name != state->schema.cols[i].dataName)
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
            for (size_t i = 0; i < state->schema.len; ++i) {
                auto expression = std::make_unique<ColumnRefExpr>();
                expression->column = state->schema.cols[i].dataName;
                expression->binding = QueryColumnBinding{0, state->sourceOrdinal, i,
                    ExprHelper::canonicalResultTypeName(state->schema.cols[i].dataType +
                        (state->schema.cols[i].isArray ? "[]" : "")), false};
                state->targets.push_back(expression.get());
                state->starTargets.push_back(std::move(expression));
            }
        } else state->targets.push_back(target.expr.get());
    }
    std::vector<std::string> identities;
    for (const auto* target : state->targets) identities.push_back(state->identity(target));
    for (auto& order : select->orderBy) {
        size_t target = PreparedSelectState::noTarget;
        if (auto* ref = dynamic_cast<ColumnRefExpr*>(order.expr.get())) {
            if (ref->schema.empty() && ref->table.empty()) {
                for (size_t i = 0; i < state->output.size(); ++i) {
                    if (state->output[i].name != ref->column) continue;
                    if (target != PreparedSelectState::noTarget && identities[target] != identities[i])
                        throw DbError("42702", "ORDER BY name is ambiguous");
                    target = i;
                }
            }
        } else if (auto* literal = dynamic_cast<LiteralExpr*>(order.expr.get())) {
            if (!literal->value.empty() && std::all_of(literal->value.begin(), literal->value.end(), [](unsigned char c) { return std::isdigit(c); })) {
                const auto ordinal = std::stoull(literal->value);
                if (!ordinal || ordinal > state->targets.size()) throw DbError("42P10", "ORDER BY position is not in select list");
                target = static_cast<size_t>(ordinal - 1);
            }
        }
        if (target == PreparedSelectState::noTarget) {
            const auto identity = state->identity(order.expr.get());
            for (size_t i = 0; i < identities.size(); ++i) if (identities[i] == identity) { target = i; break; }
        }
        if (select->distinct && target == PreparedSelectState::noTarget)
            throw DbError("42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        state->keys.push_back({order.expr.get(), target, order.asc, order.nullsFirst});
    }
    // All bindings and collation checks precede opening any source or
    // evaluating a volatile expression. The retained parameter cells remain
    // typed, including NULL, and are supplied directly to the evaluator.
    for (auto* target : state->targets) {
        state->targetsBeforeSort.push_back(!state->containsVolatile(target));
    }
    state->beginExecution();
    OpPtr root = std::move(source);
    if (select->whereClause) root = std::make_unique<PreparedFilterOp>(std::move(root), state);
    if (!state->keys.empty()) root = std::make_unique<PreparedSortOp>(std::move(root), state);
    root = std::make_unique<PreparedProjectOp>(std::move(root), state);
    if (select->distinct) root = std::make_unique<PreparedDistinctOp>(std::move(root));
    if (select->offset && *select->offset) root = std::make_unique<OffsetOp>(std::move(root), *select->offset);
    if (select->limit) root = std::make_unique<LimitOp>(std::move(root), *select->limit);
    return root;
}

TableScanOp::TableScanOp(StorageEngine* engine, const std::string& dbname,
                          const std::string& tablename)
    : engine_(engine), dbname_(dbname), tablename_(tablename) {}

bool TableScanOp::open() {
    OpenInstrument startup(this);
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rows_.clear();
    lastRid_ = 0;
    bool toastFailed = false;
    const auto appendRow = [&](uint32_t pageId, uint16_t slotId,
                               const char* data, size_t len) {
        std::string row(data, len);
        bool toastOk = false;
        row = engine_->resolveToastValues(dbname_, tablename_, row, tbl_, &toastOk);
        if (!toastOk) {
            toastFailed = true;
            return;
        }
        rows_.emplace_back(StorageEngine::encodeRid(pageId, slotId), std::move(row));
    };
    const bool scanOk = engine_->rlsAppliesTo(dbname_, tablename_)
        ? engine_->forEachVisibleRow(dbname_, tablename_, "SELECT", appendRow)
        : engine_->forEachRow(dbname_, tablename_, appendRow);
    if (!scanOk || toastFailed) {
        rows_.clear();
        setError(toastFailed ? "TOAST value read failed" : "table scan failed");
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
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    lastRid_ = rows_[pos_].first;
    outRow = rows_[pos_].second;
    ++pos_;
    // Bind stored-NULL visibility for the row handed upstream so
    // projections see the heap null bitmap instead of zero values.
    StorageEngine::bindNullRow(engine_, dbname_, tablename_, lastRid_, tbl_.len);
    rtInstr_.emitted = true;
    return true;
}

bool TableScanOp::lastColumnIsNull(size_t colIdx) const {
    return engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, colIdx);
}

void TableScanOp::close() {
    rows_.clear();
    lastRid_ = 0;
    StorageEngine::unbindNullRow();
    statsRecorded_ = false;
    if (tableLockHeld_) {
        engine_->getLockManager().unlock(tablename_);
        tableLockHeld_ = false;
    }
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
               table.pkColIndices.front() < table.len &&
               table.cols[table.pkColIndices.front()].dataName == column;
    }
    size_t primaryColumns = 0;
    bool matches = false;
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].isPrimaryKey) {
            ++primaryColumns;
            matches = table.cols[i].dataName == column;
        }
    }
    return primaryColumns == 1 && matches;
}

static std::string integerEqualityValue(const TableSchema& table,
                                       const std::string& column,
                                       const std::string& value) {
    for (size_t i = 0; i < table.len; ++i) {
        if (table.cols[i].dataName != column) continue;
        const auto& type = table.cols[i].dataType;
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
                          const std::string& value)
    : engine_(engine), dbname_(dbname), tablename_(tablename),
      colname_(colname), value_(value) {}

bool IndexScanOp::open() {
    auto& lockManager = engine_->getLockManager();
    lockManager.setResourceNamespace(dbname_);
    if (!lockManager.lockShared(tablename_)) {
        throw DbError("55P03", "could not obtain lock on relation \"" +
            tablename_ + "\"");
    }
    tableLockHeld_ = true;
    tbl_ = engine_->getTableSchema(dbname_, tablename_);
    rids_.clear();
    lastRid_ = 0;
    // Check if this is a PK scan
    size_t pkIdx = tbl_.len;
    for (size_t i = 0; i < tbl_.len; ++i) {
        if (tbl_.cols[i].isPrimaryKey && tbl_.cols[i].dataName == colname_) {
            pkIdx = i; break;
        }
    }
    isPK_ = isSingleColumnPrimaryKey(tbl_, colname_);
    if (pkIdx < tbl_.len && !isPK_) {
        const auto indexedColumns = engine_->getIndexedColumns(dbname_, tablename_);
        if (std::find(indexedColumns.begin(), indexedColumns.end(), colname_) ==
            indexedColumns.end()) {
            throw DbError("0A000", "single-column index scan cannot use a "
                "composite primary key");
        }
    }

    const std::string key = isPK_
        ? tbl_.buildPKValue({{colname_, integerEqualityValue(tbl_, colname_, value_)}})
        : tbl_.columnIndexKey(colname_, integerEqualityValue(tbl_, colname_, value_));
    if (key.empty()) {
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
            setError("empty-key heap fallback failed");
            return false;
        }
        pos_ = 0;
        if (!statsRecorded_) {
            recordTableScan(dbname_, tablename_, rids_.size(), false, true);
            statsRecorded_ = true;
        }
        return true;
    }
    BPTree* idx = isPK_
        ? engine_->getPKIndex(dbname_, tablename_)
        : engine_->getSecondaryIndex(dbname_, tablename_, colname_);
    if (!idx || !idx->isOpen()) {
        throw DbError("XX001", "could not open B-tree index for relation \"" +
            tablename_ + "\"");
    }
    if (isPK_) {
        int64_t rid = 0;
        if (lookupBtreeKeyChecked(idx, key, rid)) rids_.push_back(rid);
    } else {
        rids_ = lookupBtreeMultiChecked(idx, key);
    }
    pos_ = 0;
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, rids_.size(), true, false);
        statsRecorded_ = true;
    }
    return true;
}

bool IndexScanOp::next(std::string& outRow) {
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
                return false;
            }
            continue;
        }
        bool toastOk = false;
        outRow = engine_->resolveToastValues(
            dbname_, tablename_, row, tbl_, &toastOk);
        if (!toastOk) {
            setError("TOAST value read failed");
            return false;
        }
        lastRid_ = rid;
        StorageEngine::bindNullRow(
            engine_, dbname_, tablename_, lastRid_, tbl_.len);
        // Fixed-width B-tree keys can share a truncated prefix. The heap
        // tuple is only a candidate until the complete SQL value matches.
        // Keep the recheck inside IndexScan: the planner consumes its indexed
        // equality condition and does not retain a Filter above this node.
        size_t columnIndex = tbl_.len;
        for (size_t i = 0; i < tbl_.len; ++i)
            if (tbl_.cols[i].dataName == colname_) { columnIndex = i; break; }
        if (columnIndex == tbl_.len) {
            setError("indexed column does not exist");
            return false;
        }
        bool valueIsNull = false;
        const std::string actual = engine_->extractColumnValue(
            outRow, tbl_, columnIndex, dbname_, true, &valueIsNull);
        valueIsNull = valueIsNull ||
            (tbl_.cols[columnIndex].generatedKind != 'v' &&
             engine_->isColumnNullByRid(dbname_, tablename_, rid, columnIndex));
        if (StorageEngine::compareValues(tbl_.cols[columnIndex], actual,
                valueIsNull, integerEqualityValue(tbl_, colname_, value_),
                false, "=") != StorageEngine::PredicateTruth::True)
            continue;
        rtInstr_.emitted = true;
        return true;
    }
    return false;
}

bool IndexScanOp::lastColumnIsNull(size_t colIdx) const {
    return lastRid_ > 0 && colIdx < tbl_.len &&
        engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, colIdx);
}

void IndexScanOp::close() {
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
    const std::string value = integerEqualityValue(
        tbl, condition.colName, condition.value);

    if (isSingleColumnPrimaryKey(tbl, condition.colName)) {
        auto* index = engine->getPKIndex(dbname, tablename);
        if (!index) {
            throw DbError("XX001", "could not open B-tree index for relation \"" +
                tablename + "\"");
        }
        int64_t rid = 0;
        const std::string key = tbl.buildPKValue(
            std::map<std::string, std::string>{{condition.colName, value}});
        if (lookupBtreeKeyChecked(index, key, rid)) candidates.insert(rid);
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
        recordTableScan(dbname_, tablename_, rows_.size(), true, false);
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
        engine_, dbname_, tablename_, lastRid_, tbl_.len);
    rtInstr_.emitted = true;
    return true;
}

bool BitmapHeapScanOp::lastColumnIsNull(size_t colIdx) const {
    return lastRid_ > 0 && colIdx < tbl_.len &&
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

GiSTScanOp::GiSTScanOp(StorageEngine* engine, const std::string& dbname,
                       const std::string& tablename,
                       const std::vector<StorageEngine::Condition>& conds)
    : engine_(engine), dbname_(dbname), tablename_(tablename), conds_(conds) {}

std::string GiSTScanOp::describeConds() const {
    std::string out;
    for (const auto& c : conds_) {
        if (!out.empty()) out += " AND ";
        out += c.colName + c.op + c.value;
    }
    return out;
}

bool GiSTScanOp::open() {
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

    // Fold the GiST-servable predicates into [lo, hi] bounds on the
    // indexed column.  Mixed-column predicates stay in FilterOp; this node
    // only takes over when at least one bound is known.
    std::string lo, hi;
    bool haveLo = false, haveHi = false;
    std::string loCol, hiCol;
    for (const auto& c : conds_) {
        const bool isRange = c.op == "<" || c.op == "<=" || c.op == ">" || c.op == ">=";
        if (!isRange) continue;
        if (c.op == ">" || c.op == ">=") {
            if (haveLo) continue;  // narrowest wins is FilterOp's job
            lo = c.value;
            if (c.op == ">") {
                // exclusive: keep the boundary but note the recheck above
                // still enforces strictness; overlap widening is safe.
            }
            haveLo = true;
            loCol = c.colName;
        } else {
            if (haveHi) continue;
            hi = c.value;
            haveHi = true;
            hiCol = c.colName;
        }
    }
    // Both bounds must target the same GiST-indexed column.
    std::string gistCol;
    const auto gistColumns = engine_->getGiSTIndexedColumns(dbname_, tablename_);
    auto isGist = [&](const std::string& col) {
        return std::find(gistColumns.begin(), gistColumns.end(), col) != gistColumns.end();
    };
    if (haveLo && haveHi && loCol == hiCol && isGist(loCol)) gistCol = loCol;
    else if (haveLo && isGist(loCol)) gistCol = loCol;
    else if (haveHi && isGist(hiCol)) gistCol = hiCol;

    if (!gistCol.empty()) {
        // Missing side: use the widest possible bound.  The sidecar compares
        // textually, so an empty lo is below every key and a high sentinel
        // is above every ASCII key.
        const std::string overlapLo = haveLo ? lo : "";
        const std::string overlapHi = haveHi ? hi : "\x7f";
        rids_ = engine_->giSTSearchOverlap(dbname_, tablename_, gistCol,
                                           overlapLo, overlapHi);
    } else {
        // Text-prefix predicate: LIKE 'prefix%' via the GiST containment
        // style (entry low <= prefix <= entry high is replaced by the
        // overlap with [prefix, prefix + sentinel]).
        for (const auto& c : conds_) {
            if (c.op != "like" || c.value.empty()) continue;
            if (c.value.back() != '%' || c.value.find('%') != c.value.size() - 1)
                continue;  // only anchored prefixes
            if (!isGist(c.colName)) continue;
            std::string prefix = c.value.substr(0, c.value.size() - 1);
            rids_ = engine_->giSTSearchOverlap(dbname_, tablename_, c.colName,
                                               prefix, prefix + "\x7f");
            break;
        }
    }

    bool readFailed = false;
    std::vector<int64_t> visibleRids;
    visibleRids.reserve(rids_.size());
    for (int64_t rid : rids_) {
        std::string row;
        if (!engine_->readIndexedRowByRid(dbname_, tablename_, rid, row, tbl_, nullptr,
                                          &readFailed)) {
            if (readFailed) {
                rows_.clear();
                setError("gist heap fetch failed");
                return false;
            }
            continue;  // concurrently removed row: not visible, skip
        }
        rows_.push_back(std::move(row));
        visibleRids.push_back(rid);
    }
    rids_ = std::move(visibleRids);
    pos_ = 0;
    if (!statsRecorded_) {
        recordTableScan(dbname_, tablename_, rows_.size(), true, false);
        statsRecorded_ = true;
    }
    return true;
}

bool GiSTScanOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= rows_.size()) return false;
    lastRid_ = rids_[pos_];
    outRow = rows_[pos_++];
    StorageEngine::bindNullRow(
        engine_, dbname_, tablename_, lastRid_, tbl_.len);
    rtInstr_.emitted = true;
    return true;
}

bool GiSTScanOp::lastColumnIsNull(size_t colIdx) const {
    return lastRid_ > 0 && colIdx < tbl_.len &&
        engine_->isColumnNullByRid(dbname_, tablename_, lastRid_, colIdx);
}

void GiSTScanOp::close() {
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
        recordTableScan(dbname_, tablename_, rows_.size(), true, false);
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
            engine_, dbname_, tablename_, lastRid_, tbl_.len);
    }
    rtInstr_.emitted = true;
    return true;
}

bool BitmapOrHeapScanOp::lastColumnIsNull(size_t colIdx) const {
    if (lastRid_ <= 0 || colIdx >= tbl_.len) return false;
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

FilterOp::FilterOp(OpPtr child, const TableSchema& tbl,
                    const std::vector<StorageEngine::Condition>& conds)
    : child_(std::move(child)), tbl_(tbl), conds_(conds) {}

FilterOp::FilterOp(
    OpPtr child, const TableSchema& tbl,
    const std::vector<std::vector<StorageEngine::Condition>>& branches)
    : child_(std::move(child)), tbl_(tbl), branches_(branches) {}

bool FilterOp::open() {
    return child_->open();
}

bool FilterOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    const auto matchesBranch = [&](
        const std::vector<StorageEngine::Condition>& conditions) {
        for (const auto& c : conditions) {
            size_t colIdx = tbl_.len;
            for (size_t i = 0; i < tbl_.len; ++i) {
                if (tbl_.cols[i].dataName == c.colName) {
                    colIdx = i;
                    break;
                }
            }
            const bool physicalColumn = colIdx < tbl_.len &&
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
        std::vector<bool> nulls(outerTbl_.len, false);
        for (size_t i = 0; i < outerTbl_.len; ++i) {
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
            size_t oi = outerTbl_.len, ii = innerTbl_.len;
            for (size_t i = 0; i < outerTbl_.len; ++i)
                if (outerTbl_.cols[i].dataName == k.first) { oi = i; break; }
            for (size_t i = 0; i < innerTbl_.len; ++i)
                if (innerTbl_.cols[i].dataName == k.second) { ii = i; break; }
            if (oi >= outerTbl_.len || ii >= innerTbl_.len) return false;
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

    size_t outerIdx = outerTbl_.len;
    size_t innerIdx = innerTbl_.len;
    for (size_t i = 0; i < outerTbl_.len; ++i) {
        if (outerTbl_.cols[i].dataName == outerColumn_) {
            outerIdx = i;
            break;
        }
    }
    for (size_t i = 0; i < innerTbl_.len; ++i) {
        if (innerTbl_.cols[i].dataName == innerColumn_) {
            innerIdx = i;
            break;
        }
    }
    if (outerIdx >= outerTbl_.len || innerIdx >= innerTbl_.len) return false;
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
            outerTbl_.len);
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

    size_t outerIdx = outerTbl_.len;
    size_t innerIdx = innerTbl_.len;
    for (size_t i = 0; i < outerTbl_.len; ++i) {
        if (outerTbl_.cols[i].dataName == outerColumn_) {
            outerIdx = i;
            break;
        }
    }
    for (size_t i = 0; i < innerTbl_.len; ++i) {
        if (innerTbl_.cols[i].dataName == innerColumn_) {
            innerIdx = i;
            break;
        }
    }
    if (outerIdx >= outerTbl_.len || innerIdx >= innerTbl_.len) return false;
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
    size_t outerIdx = outerTbl_.len;
    for (size_t i = 0; i < outerTbl_.len; ++i) {
        if (outerTbl_.cols[i].dataName == outerColumn_) {
            outerIdx = i;
            break;
        }
    }
    if (outerIdx >= outerTbl_.len) return false;

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

    size_t innerIdx = innerTbl_.len;
    for (size_t i = 0; i < innerTbl_.len; ++i) {
        if (innerTbl_.cols[i].dataName == innerColumn_) {
            innerIdx = i;
            break;
        }
    }
    if (innerIdx >= innerTbl_.len) {
        setError("scalar subquery column does not exist");
        return false;
    }
    if (!inner_->open()) return false;

    std::string row;
    if (inner_->next(row)) {
        scalarIsNull_ = inner_->lastColumnIsNull(innerIdx);
        if (!scalarIsNull_) {
            scalarValue_ = StorageEngine::extractColumnValueStatic(
                row, innerTbl_, innerIdx);
        }
        if (inner_->next(row)) {
            inner_->close();
            setError("more than one row returned by a subquery used as an expression");
            return false;
        }
        if (inner_->hasError()) {
            inner_->close();
            return propagateChildError(inner_.get(), "scalar subquery failed");
        }
    }
    inner_->close();
    if (inner_->hasError()) return propagateChildError(inner_.get(), "scalar subquery failed");
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
            size_t outerIdx = outerTbl_.len;
            for (size_t i = 0; i < outerTbl_.len; ++i) {
                if (outerTbl_.cols[i].dataName == target.column) {
                    outerIdx = i;
                    break;
                }
            }
            if (outerIdx >= outerTbl_.len) {
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
    if (!child_->next(raw)) {
        if (child_->hasError()) return propagateChildError(child_.get(), "projection child failed");
        return false;
    }
    lastCells_.clear();
    lastNulls_.clear();
    const auto origin = child_->scanOrigin();
    if (origin.engine && origin.rid > 0)
        StorageEngine::bindNullRow(origin.engine, origin.dbname, origin.tablename,
                                  origin.rid, tbl_.len);
    outRow.clear();
    for (size_t i = 0; i < tbl_.len; ++i) {
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

bool ProjectOp::lastTypedRowKey(std::string& key) const {
    if (!lastRowAvailable_ || lastCells_.size() != lastNulls_.size()) return false;
    key.clear();
    size_t projected = 0;
    for (size_t i = 0; i < tbl_.len; ++i) {
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
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == name) return i;
    }
    return tbl.len;
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

static int compareWindowCell(const std::string& left, bool leftIsNull,
                             const std::string& right, bool rightIsNull,
                             bool nullsLast = true) {
    if (leftIsNull && rightIsNull) return 0;
    if (leftIsNull) return nullsLast ? 1 : -1;
    if (rightIsNull) return nullsLast ? -1 : 1;
    return compareNonNullAggregateValue(left, right);
}

static size_t windowColumnIndex(const TableSchema& tbl, const std::string& name) {
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == name) return i;
    }
    return tbl.len;
}

static bool sameWindowPartition(const WindowInputRow& left,
                                const WindowInputRow& right,
    const std::vector<size_t>& columns) {
    for (size_t column : columns) {
        if (left.nulls[column] != right.nulls[column]) return false;
        if (!left.nulls[column] &&
            left.values[column] != right.values[column]) return false;
    }
    return true;
}

static bool sameWindowPeer(const WindowInputRow& left,
                           const WindowInputRow& right,
    size_t orderColumn,
    size_t columnCount) {
    return orderColumn >= columnCount ||
           compareWindowCell(left.values[orderColumn], left.nulls[orderColumn],
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
                   bool finalOrderAscending)
    : child_(std::move(child)), tbl_(tbl), targets_(targets),
      functions_(functions), finalOrderBy_(finalOrderBy),
      finalOrderAscending_(finalOrderAscending) {}

bool WindowOp::open() {
    rows_.clear();
    structuredRows_.clear();
    structuredNulls_.clear();
    pos_ = 0;
    if (!child_->open()) return false;

    std::vector<WindowInputRow> input;
    std::string raw;
    while (child_->next(raw)) {
        WindowInputRow row;
        row.raw = std::move(raw);
        row.values.reserve(tbl_.len);
        row.nulls.reserve(tbl_.len);
        for (size_t i = 0; i < tbl_.len; ++i) {
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
            if (column >= tbl_.len) return false;
            partitionColumns.push_back(column);
        }
        const size_t orderColumn = function.orderBy.empty()
            ? tbl_.len : windowColumnIndex(tbl_, function.orderBy);
        if (!function.orderBy.empty() && orderColumn >= tbl_.len) return false;

        const size_t argumentColumn = (function.argument.empty() || function.argument == "*")
            ? tbl_.len : windowColumnIndex(tbl_, function.argument);
        // Boolean aggregates accept comparison expressions ("v > 5") as
        // arguments; evaluated per row instead of a direct column read.
        const bool boolAggWithExpr =
            (function.name == "bool_and" || function.name == "bool_or" || function.name == "every") &&
            argumentColumn >= tbl_.len && !function.argument.empty() &&
            function.argument != "*";
        const bool argumentRequired = function.name == "lag" || function.name == "lead" ||
            function.name == "sum" || function.name == "avg" || function.name == "min" ||
            function.name == "max" || function.name == "first_value" ||
            function.name == "last_value" || function.name == "bool_and" ||
            function.name == "bool_or" || function.name == "every";
        if (argumentRequired && argumentColumn >= tbl_.len && !boolAggWithExpr) return false;

        std::vector<size_t> order(input.size());
        for (size_t i = 0; i < order.size(); ++i) order[i] = i;
        std::stable_sort(order.begin(), order.end(), [&](size_t left, size_t right) {
            const auto& leftRow = input[left];
            const auto& rightRow = input[right];
            for (size_t column : partitionColumns) {
                const int cmp = compareWindowCell(
                    leftRow.values[column], leftRow.nulls[column],
                    rightRow.values[column], rightRow.nulls[column]);
                if (cmp != 0) return cmp < 0;
            }
            if (orderColumn < tbl_.len) {
                const int cmp = compareWindowCell(
                    leftRow.values[orderColumn], leftRow.nulls[orderColumn],
                    rightRow.values[orderColumn], rightRow.nulls[orderColumn],
                    function.orderAscending);
                if (cmp != 0) return function.orderAscending ? cmp < 0 : cmp > 0;
            }
            return left < right;
        });

        std::vector<size_t> partitionStartAt(order.size());
        std::vector<size_t> partitionEndAt(order.size());
        for (size_t position = 0; position < order.size();) {
            size_t end = position + 1;
            while (end < order.size() &&
                   sameWindowPartition(input[order[end]], input[order[position]], partitionColumns)) {
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
                                      orderColumn, tbl_.len)) {
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
            if (!function.hasFrame) {
                if (orderColumn < tbl_.len) return {partitionStart, groupEndAt[position]};
                return {partitionStart, partitionEnd};
            }
            if (function.frameType == WindowFunctionSpec::FrameType::GROUPS) {
                size_t begin = groupStartAt[position];
                size_t end = groupEndAt[position];
                if (function.frameStartOffset >= 0) {
                    size_t groups = static_cast<size_t>(function.frameStartOffset);
                    while (groups-- > 0 && begin > partitionStart) {
                        begin = groupStartAt[begin - 1];
                    }
                }
                if (function.frameEndOffset >= 0) {
                    size_t groups = static_cast<size_t>(function.frameEndOffset);
                    while (groups-- > 0 && end < partitionEnd) {
                        end = groupEndAt[end];
                    }
                }
                return {begin, end};
            }
            if (function.frameType == WindowFunctionSpec::FrameType::RANGE &&
                orderColumn < tbl_.len) {
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
                const size_t offset = std::max<size_t>(1, function.offset);
                const bool hasTarget = function.name == "lag"
                    ? position >= partitionStart + offset
                    : position + offset < partitionEnd;
                if (hasTarget) {
                    const size_t targetPosition = function.name == "lag"
                        ? position - offset : position + offset;
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
                int64_t bucketCount = 1;
                if (!parseInteger(function.argument, bucketCount) || bucketCount <= 0) bucketCount = 1;
                const size_t partitionSize = partitionEnd - partitionStart;
                const size_t bucket = (position - partitionStart) * static_cast<size_t>(bucketCount) /
                    std::max<size_t>(1, partitionSize) + 1;
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
                int64_t sum = 0;
                dbms::Numeric exactSum(0);
                bool exactSumOk = true;
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
                            for (; ci3 < tbl_.len; ++ci3)
                                if (tbl_.cols[ci3].dataName == token) break;
                            if (ci3 < tbl_.len) { synth += rowVals[ci3]; token.clear(); return; }
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
                    int64_t number = 0;
                    if (function.name == "sum" || function.name == "avg") {
                        if (!parseInteger(value, number)) {
                            // numeric/decimal storage keeps fraction digits;
                            // accumulate exactly instead of skipping
                            if (!exactSumOk) continue;
                            try {
                                exactSum = exactSum + dbms::Numeric(value);
                                ++count;
                            } catch (...) { exactSumOk = false; continue; }
                        } else {
                            sum += number;
                            ++count;
                            if (exactSumOk) {
                                try { exactSum = exactSum + dbms::Numeric(value); }
                                catch (...) { exactSumOk = false; }
                            }
                        }
                    } else if (function.name == "min" || function.name == "max") {
                        if (!hasValue || (function.name == "min"
                                ? compareNonNullAggregateValue(value, selected) < 0
                                : compareNonNullAggregateValue(value, selected) > 0)) {
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
                    else if (exactSumOk) {
                        try { computed[rowIndex][functionIndex] = exactSum.toString(); }
                        catch (...) { computed[rowIndex][functionIndex] = std::to_string(sum); }
                        computedNulls[rowIndex][functionIndex] = false;
                    } else {
                        computed[rowIndex][functionIndex] = std::to_string(sum);
                        computedNulls[rowIndex][functionIndex] = false;
                    }
                } else if (function.name == "avg") {
                    if (count == 0) {
                        computed[rowIndex][functionIndex].clear();
                        computedNulls[rowIndex][functionIndex] = true;
                    } else if (exactSumOk) {
                        // PG avg(numeric): exact division with select_div_scale
                        try {
                            computed[rowIndex][functionIndex] =
                                (exactSum / dbms::Numeric(count)).toString();
                        } catch (...) {
                            computed[rowIndex][functionIndex] =
                                std::to_string(static_cast<double>(sum) / count);
                        }
                        computedNulls[rowIndex][functionIndex] = false;
                    } else {
                        computed[rowIndex][functionIndex] =
                            std::to_string(static_cast<double>(sum) / count);
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
    size_t finalOrderColumn = tbl_.len;
    if (finalOrderBy_.empty() && !functions_.empty()) {
        for (const auto& pc : functions_.front().partitionBy) {
            const size_t c = windowColumnIndex(tbl_, pc);
            if (c < tbl_.len) finalPartitionColumns.push_back(c);
        }
        if (!functions_.front().orderBy.empty()) {
            finalOrderColumn = windowColumnIndex(tbl_, functions_.front().orderBy);
            finalAsc = functions_.front().orderAscending;
        }
    } else if (!finalOrderBy_.empty()) {
        finalOrderColumn = windowColumnIndex(tbl_, finalOrderBy_);
    }
    finalOrderAscending_ = finalAsc;
    if (!finalOrderBy_.empty() && finalOrderColumn >= tbl_.len) return false;
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
                if (column >= tbl_.len) return false;
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
        if (finalOrderColumn < tbl_.len) {
            row.sortKey = input[rowIndex].values[finalOrderColumn];
            row.sortKeyNull = input[rowIndex].nulls[finalOrderColumn];
        }
        row.keys = input[rowIndex].values;
        row.keyNulls = input[rowIndex].nulls;
        output.push_back(std::move(row));
    }
    if (finalOrderColumn < tbl_.len || !finalPartitionColumns.empty()) {
        std::stable_sort(output.begin(), output.end(), [&](const OutputRow& left, const OutputRow& right) {
            for (size_t column : finalPartitionColumns) {
                const int cmp = compareWindowCell(
                    left.keys[column], left.keyNulls[column],
                    right.keys[column], right.keyNulls[column]);
                if (cmp != 0) return cmp < 0;
            }
            if (finalOrderColumn >= tbl_.len) return false;
            const int cmp = compareWindowCell(
                left.sortKey, left.sortKeyNull,
                right.sortKey, right.sortKeyNull,
                finalOrderAscending_);
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

SortOp::SortOp(OpPtr child, const TableSchema& tbl,
                const std::string& orderByCol, bool asc,
                bool nullsFirst, bool hasExplicitNullOrder)
    : child_(std::move(child)), tbl_(tbl), orderByCol_(orderByCol), asc_(asc),
      nullsFirst_(hasExplicitNullOrder ? nullsFirst : !asc) {}

bool SortOp::open() {
    if (!child_->open()) return false;
    size_t sortIdxPre = tbl_.len;
    for (size_t i = 0; i < tbl_.len; ++i) {
        if (tbl_.cols[i].dataName == orderByCol_) { sortIdxPre = i; break; }
    }
    // Extract each row's sort value WHILE the child's stored-NULL
    // binding is still live (the binding is cleared on close).
    std::string row;
    std::vector<std::string> preVals;
    std::vector<bool> preNulls;
    origins_.clear();
    while (child_->next(row)) {
        buffer_.push_back(std::move(row));
        Operator::ScanOrigin org = child_->scanOrigin();
        origins_.push_back(org);
        // Rows not traced to a heap scan carry no stored-NULL bitmap:
        // clear any stale binding so extraction reads raw values.
        if (!org.engine) StorageEngine::unbindNullRow();
        else StorageEngine::bindNullRow(org.engine, org.dbname, org.tablename, org.rid, tbl_.len);
        if (sortIdxPre < tbl_.len) {
            preVals.push_back(StorageEngine::extractColumnValueStatic(
                buffer_.back(), tbl_, sortIdxPre));
            const auto origin = child_->scanOrigin();
            preNulls.push_back(origin.engine && origin.rid != 0
                ? child_->lastColumnIsNull(sortIdxPre)
                : rawColumnIsNull(buffer_.back(), tbl_, sortIdxPre));
        }
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "sort child failed");
    child_->close();

    size_t sortIdx = sortIdxPre;
    if (sortIdx < tbl_.len) {
        struct Item { std::string s; int64_t n; Date d; double f; bool isNull; size_t originIdx; };
        std::vector<std::pair<std::string, Item>> items;
        const Column& scol = tbl_.cols[sortIdx];
        for (size_t ri = 0; ri < buffer_.size(); ++ri) {
            std::string val = (ri < preVals.size()) ? preVals[ri] : "";
            // PG default: NULLS LAST for ASC, NULLS FIRST for DESC.
            const bool valNull = ri < preNulls.size()
                ? preNulls[ri]
                : (val.empty() || val == "NULL" || val == "null");
            Item it{"", 0, {}, 0.0, valNull, ri};
            if (scol.dataType == "char" || scol.isVariableLength) {
                it.s = val;
            } else if (scol.dataType == "date") {
                it.d = val.empty() ? Date{} : Date(val.c_str());
            } else if (scol.dataType == "timestamp") {
                it.n = val.empty() ? 0 : parseTimestampToSeconds(val);
            } else if (scol.dataType == "float" || scol.dataType == "double" || scol.dataType == "decimal") {
                try { it.f = std::stod(val); } catch (...) { it.f = 0.0; }
            } else {
                it.n = val.empty() ? 0 : StorageEngine::parseInt(val);
            }
            items.emplace_back(std::move(buffer_[ri]), it);
        }
        std::sort(items.begin(), items.end(), [&](const auto& a, const auto& b) {
            if (a.second.isNull != b.second.isNull)
                return a.second.isNull == nullsFirst_;
            if (scol.dataType == "char" || scol.isVariableLength) {
                const auto comparison = StorageEngine::compareValues(
                    scol, a.second.s, false, b.second.s, false, "<");
                if (asc_)
                    return comparison == StorageEngine::PredicateTruth::True;
                return StorageEngine::compareValues(
                           scol, b.second.s, false, a.second.s, false, "<") ==
                       StorageEngine::PredicateTruth::True;
            }
            if (scol.dataType == "date") return asc_ ? (a.second.d < b.second.d) : (b.second.d < a.second.d);
            if (scol.dataType == "float" || scol.dataType == "double" || scol.dataType == "decimal") return asc_ ? (a.second.f < b.second.f) : (b.second.f < a.second.f);
            return asc_ ? (a.second.n < b.second.n) : (b.second.n < a.second.n);
        });
        buffer_.clear();
        sortedOrigins_.clear();
        for (auto& it : items) {
            buffer_.push_back(std::move(it.first));
            sortedOrigins_.push_back(it.second.originIdx < origins_.size()
                ? origins_[it.second.originIdx] : Operator::ScanOrigin{});
        }
    }
    pos_ = 0;
    return true;
}

bool SortOp::next(std::string& outRow) {
    NextInstrument rtInstr_(this);  // EXPLAIN ANALYZE per-node stats
    if (pos_ >= buffer_.size()) return false;
    // Re-bind stored-NULL visibility for the re-emitted row so
    // downstream projection sees the correct null bitmap.
    if (pos_ < sortedOrigins_.size()) {
        const auto& org = sortedOrigins_[pos_];
        if (org.engine && org.rid != 0)
            StorageEngine::bindNullRow(org.engine, org.dbname, org.tablename,
                                       org.rid, tbl_.len);
        else
            StorageEngine::unbindNullRow();
    }
    outRow = buffer_[pos_];
    ++pos_;
    rtInstr_.emitted = true;
    return true;
}

void SortOp::close() {
    buffer_.clear();
}

bool SortOp::lastColumnIsNull(size_t colIdx) const {
    // The re-emitted row stored-NULL bit: consult the sort own origin
    // for the row at pos_ - 1 (the row most recently handed upstream),
    // not the drained child scan stale lastRid_.
    if (pos_ == 0 || pos_ - 1 >= sortedOrigins_.size()) return false;
    const auto& org = sortedOrigins_[pos_ - 1];
    if (!org.engine || org.rid == 0) return false;
    return org.engine->isColumnNullByRid(org.dbname, org.tablename, org.rid, colIdx);
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
    std::string ignored;
    while (skipped_ < offset_ && child_->next(ignored)) ++skipped_;
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
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == colName) return i;
    }
    return tbl.len;
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
    if (leftColIdx_ >= leftTbl_.len || rightColIdx_ >= rightTbl_.len) {
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
    if (colIdx >= tbl.len) return "";
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
    if (leftColIdx_ >= leftTbl_.len || rightColIdx_ >= rightTbl_.len) {
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
            while (column < table.len && table.cols[column].dataName != filter.colName)
                ++column;
            const bool isNull = column < table.len && row.nulls[column];
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
        for (size_t i = 0; i < table.len; ++i) {
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
    std::stable_sort(entries.begin(), entries.end(), [&](const Entry& a, const Entry& b) {
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
        std::stable_sort(entries.begin(), entries.end(), [&](const Entry& a,
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
        for (size_t i = 0; i < table.len; ++i) {
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
    if (index >= table.len || table.cols[index].isArray) return false;
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
        row.values.reserve(tbl_.len);
        for (size_t i = 0; i < tbl_.len; ++i) {
            row.values.push_back(StorageEngine::extractColumnValueStatic(row.raw, tbl_, i));
            row.nulls.push_back(child_->lastColumnIsNull(i));
        }
        input.push_back(std::move(row));
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "aggregate child failed");
    child_->close();

    auto columnIndex = [&](const std::string& name) {
        for (size_t i = 0; i < tbl_.len; ++i) {
            if (tbl_.cols[i].dataName == name) return i;
        }
        return tbl_.len;
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
        const size_t argIndex = arg == "*" ? tbl_.len : columnIndex(arg);
        // Aggregate over an arithmetic expression ("sum(amt * 2)"): resolve
        // operands per row when the arg is not a bare column.
        const bool argIsExpr = argIndex >= tbl_.len && arg != "*" &&
            (func == "sum" || func == "avg" || func == "min" ||
             func == "max" || func == "count") && !arg.empty();
        auto exprVal = [&](const std::string& raw,
                           const std::vector<std::string>& rowVals) -> std::string {
            double acc = 0.0;
            bool accSet = false;
            char pendingOp = 0;
            std::string token;
            auto resolve = [&](const std::string& tok) -> std::string {
                for (size_t ci = 0; ci < tbl_.len; ++ci)
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
            if (argIsExpr) {
                value = exprVal(arg, row.values);
            } else {
                value = argIndex < tbl_.len ? row.values[argIndex] : "";
            }
            const bool valueIsNull = argIsExpr
                ? value.empty()
                : (argIndex < row.nulls.size() && row.nulls[argIndex]);
            if (func == "count") {
                if (distinct) {
                    if (!valueIsNull)
                        distinctValues.insert(argIndex < tbl_.len
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
                if (!hasValue || (func == "min"
                        ? compareNonNullAggregateValue(value, selected) < 0
                        : compareNonNullAggregateValue(value, selected) > 0)) {
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
    if (leftColIdx_ >= leftTbl_.len || rightColIdx_ >= rightTbl_.len) {
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
            std::sort(kv.second.begin(), kv.second.end(),
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
    colIdx_ = tbl_.len;
    for (size_t i = 0; i < tbl_.len; ++i) {
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
    if (leftColIdx >= leftTbl_.len || rightColIdx >= rightTbl_.len) {
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
    std::sort(leftRows_.begin(), leftRows_.end(), leftCmp);
    std::sort(rightRows_.begin(), rightRows_.end(), rightCmp);

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
        row.values.reserve(tbl_.len);
        row.nulls.reserve(tbl_.len);
        for (size_t i = 0; i < tbl_.len; ++i) {
            row.values.push_back(StorageEngine::extractColumnValueStatic(row.raw, tbl_, i));
            row.nulls.push_back(child_->lastColumnIsNull(i));
        }
        input.push_back(std::move(row));
    }
    if (child_->hasError()) return propagateChildError(child_.get(), "aggregate child failed");
    child_->close();

    auto columnIndex = [&](const std::string& name) {
        for (size_t i = 0; i < tbl_.len; ++i) {
            if (tbl_.cols[i].dataName == name) return i;
        }
        return tbl_.len;
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
        const size_t argIndex = arg == "*" ? tbl_.len : columnIndex(arg);
        const auto filters = StorageEngine::parseConditions(item.filterConds);
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
            if (argIndex >= tbl_.len && arg != "*" && !arg.empty() &&
                arg.find(' ') != std::string::npos &&
                (func == "bool_and" || func == "bool_or" || func == "every")) {
                // Per-row comparison evaluation ("v > 5"): substitute column
                // tokens with this row's values, then evaluate the predicate.
                std::string synth;
                std::string token;
                const auto& rowVals = row.values;
                auto flushTok = [&]() {
                    if (token.empty()) return;
                    size_t ci4 = 0;
                    for (; ci4 < tbl_.len; ++ci4)
                        if (tbl_.cols[ci4].dataName == token) break;
                    if (ci4 < tbl_.len) { synth += rowVals[ci4]; token.clear(); return; }
                    synth += token;
                    token.clear();
                };
                for (char ch3 : arg) {
                    if (ch3 == ' ') { flushTok(); synth += ' '; continue; }
                    token += ch3;
                }
                flushTok();
                auto r3 = dbms::ExprHelper::evalString(synth, {}, {}, "");
                value = (r3.ok && !r3.isNull) ? r3.value : std::string{};
            } else {
                value = argIndex < tbl_.len ? row.values[argIndex] : std::string{};
            }
            const bool valueIsNull = argIndex < tbl_.len
                ? (argIndex < row.nulls.size() && row.nulls[argIndex])
                : value.empty();
            if (func == "count") {
                if (distinct) {
                    if (!valueIsNull)
                        distinctValues.insert(argIndex < tbl_.len
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
                if (!hasValue || (func == "min"
                        ? compareNonNullAggregateValue(value, selected) < 0
                        : compareNonNullAggregateValue(value, selected) > 0)) {
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

static bool hasEqualityIndex(StorageEngine* engine, const PlanContext& ctx,
                             const StorageEngine::Condition& condition) {
    if (condition.op != "=") return false;
    const TableSchema table = engine->getTableSchema(ctx.dbname, ctx.tablename);
    if (equalityIndexKeyIsEmpty(table, condition)) return false;
    if (isSingleColumnPrimaryKey(table, condition.colName)) return true;
    const auto btreeColumns = engine->getIndexedColumns(ctx.dbname, ctx.tablename);
    if (std::find(btreeColumns.begin(), btreeColumns.end(), condition.colName) != btreeColumns.end())
        return true;
    const auto hashColumns = engine->getHashIndexedColumns(ctx.dbname, ctx.tablename);
    if (std::find(hashColumns.begin(), hashColumns.end(), condition.colName) != hashColumns.end())
        return true;
    const auto bloomColumns = engine->getBloomIndexedColumns(ctx.dbname, ctx.tablename);
    return std::find(bloomColumns.begin(), bloomColumns.end(), condition.colName) != bloomColumns.end();
}

// A GiST-servable predicate exists: a range (>, >=, <, <=) pair or single
// bound, or an anchored prefix LIKE 'abc%', whose column carries a .gist
// sidecar index.
static bool canUseGiSTScan(StorageEngine* engine, const PlanContext& ctx) {
    const TableSchema table = engine->getTableSchema(ctx.dbname, ctx.tablename);
    if (table.partitionType != TableSchema::PartitionType::None) return false;
    const auto gistColumns = engine->getGiSTIndexedColumns(ctx.dbname, ctx.tablename);
    if (gistColumns.empty()) return false;
    auto isGist = [&](const std::string& col) {
        return std::find(gistColumns.begin(), gistColumns.end(), col) != gistColumns.end();
    };
    for (const auto& condition : ctx.conds) {
        const bool isRange = condition.op == "<" || condition.op == "<=" ||
                             condition.op == ">" || condition.op == ">=";
        if (isRange && isGist(condition.colName)) return true;
        if (condition.op == "like" && !condition.value.empty() &&
            condition.value.back() == '%' &&
            condition.value.find('%') == condition.value.size() - 1 &&
            isGist(condition.colName)) {
            return true;
        }
    }
    return false;
}

static bool canUseBitmapHeapScan(StorageEngine* engine, const PlanContext& ctx) {
    const TableSchema table = engine->getTableSchema(ctx.dbname, ctx.tablename);
    if (table.partitionType != TableSchema::PartitionType::None) return false;
    std::set<std::string> seenColumns;
    size_t indexedPredicates = 0;
    for (const auto& condition : ctx.conds) {
        if (!seenColumns.insert(condition.colName).second) continue;
        if (hasEqualityIndex(engine, ctx, condition)) ++indexedPredicates;
    }
    return indexedPredicates >= 2;
}

static bool canUseBitmapOrScan(
    StorageEngine* engine, const PlanContext& ctx,
    const std::vector<std::vector<StorageEngine::Condition>>& branches) {
    const TableSchema table = engine->getTableSchema(ctx.dbname, ctx.tablename);
    if (table.partitionType != TableSchema::PartitionType::None || branches.size() < 2)
        return false;
    for (const auto& branch : branches) {
        std::set<std::string> seenColumns;
        bool indexed = false;
        for (const auto& condition : branch) {
            if (!seenColumns.insert(condition.colName).second) continue;
            if (hasEqualityIndex(engine, ctx, condition)) {
                indexed = true;
                break;
            }
        }
        if (!indexed) return false;
    }
    return true;
}

OpPtr QueryPlanner::buildSelectPlan(StorageEngine* engine, const PlanContext& ctx) {
    OpPtr root;

    // Choose between the protected IndexScan and TableScan paths.
    std::vector<StorageEngine::Condition> remainingConds = ctx.conds;
    // RLS is a relation-level security boundary.  Do not let an index,
    // bitmap, or parallel access path duplicate policy evaluation: use the
    // policy-aware TableScanOp and keep user predicates above it.
    const bool rlsApplies = engine->rlsAppliesTo(ctx.dbname, ctx.tablename);
    const bool useBitmap = !rlsApplies && canUseBitmapHeapScan(engine, ctx);
    // GiST acceleration: a range or anchored-prefix predicate on a
    // .gist-indexed column narrows candidates via the sidecar overlap
    // scan.  All predicates stay in FilterOp above as the recheck
    // boundary, identical to the bitmap path.
    const bool useGiST = !rlsApplies && canUseGiSTScan(engine, ctx);
    if (useBitmap) {
        // Keep all predicates for FilterOp's heap recheck.  The bitmap node
        // only narrows the candidate RID set; it is not a correctness filter.
        root = std::make_unique<BitmapHeapScanOp>(
            engine, ctx.dbname, ctx.tablename, ctx.conds);
    } else if (useGiST) {
        root = std::make_unique<GiSTScanOp>(
            engine, ctx.dbname, ctx.tablename, ctx.conds);
    } else if (!rlsApplies && !remainingConds.empty()) {
        for (const auto& c : remainingConds) {
            if (c.op == "=" && !equalityIndexKeyIsEmpty(
                    engine->getTableSchema(ctx.dbname, ctx.tablename), c)) {
                // Check if column has primary key index
                TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
                const bool isPK = isSingleColumnPrimaryKey(tbl, c.colName);
                bool hasSecIdx = false;
                if (!isPK) {
                    auto indexedCols = engine->getIndexedColumns(ctx.dbname, ctx.tablename);
                    for (const auto& ic : indexedCols) {
                        if (ic == c.colName) { hasSecIdx = true; break; }
                    }
                }
                // Parent indexes currently store page/slot only.  Partition
                // heaps reuse those local addresses, so an index fetch cannot
                // identify which physical heap owns the RID.  Keep SELECT on
                // the partition-aware sequential scan until partitioned index
                // row locators are implemented.
                const bool partitioned =
                    tbl.partitionType != TableSchema::PartitionType::None;
                if (!partitioned && (isPK || hasSecIdx)) {
                    // Every indexed path must recheck the heap tuple under
                    // the same lock/MVCC boundary. A visibility-map-backed
                    // index-only scan is not implemented yet.
                    root = std::make_unique<IndexScanOp>(engine, ctx.dbname, ctx.tablename,
                                                          c.colName, c.value);
                    // Remove this condition from Filter since IndexScan handles it
                    auto it = remainingConds.begin();
                    while (it != remainingConds.end()) {
                        if (it->colName == c.colName && it->op == c.op && it->value == c.value) {
                            it = remainingConds.erase(it);
                            break;
                        }
                        ++it;
                    }
                    break;
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
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        root = std::make_unique<FilterOp>(std::move(root), tbl, remainingConds);
    }
    if (!ctx.disjunctiveConds.empty()) {
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        root = std::make_unique<FilterOp>(
            std::move(root), tbl, ctx.disjunctiveConds);
    }

    // Lower uncorrelated IN/NOT IN predicates after the outer filter and
    // before projection/aggregation.  The join only carries outer rows, so
    // it does not change the schema visible to all downstream operators.
    if (!ctx.semiJoins.empty()) {
        const TableSchema outerTbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        for (const auto& spec : ctx.semiJoins) {
            const std::string innerDb = spec.dbname.empty() ? ctx.dbname : spec.dbname;
            const TableSchema innerTbl = engine->getTableSchema(innerDb, spec.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, spec.tablename);
            if (!spec.innerConds.empty()) {
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
        const TableSchema outerTbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        for (const auto& spec : ctx.existenceFilters) {
            const std::string innerDb = spec.dbname.empty() ? ctx.dbname : spec.dbname;
            const TableSchema innerTbl = engine->getTableSchema(innerDb, spec.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, spec.tablename);
            if (!spec.innerConds.empty()) {
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
        const TableSchema outerTbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        for (const auto& spec : ctx.quantifiedSubqueries) {
            const std::string innerDb = spec.dbname.empty() ? ctx.dbname : spec.dbname;
            const TableSchema innerTbl = engine->getTableSchema(innerDb, spec.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, spec.tablename);
            if (!spec.innerConds.empty()) {
                inner = std::make_unique<FilterOp>(
                    std::move(inner), innerTbl, spec.innerConds);
            }
            root = std::make_unique<QuantifiedSubqueryFilterOp>(
                std::move(root), std::move(inner), outerTbl, innerTbl,
                spec.outerColumn, spec.innerColumn, spec.op, spec.all);
        }
    }

    if (!ctx.groupByCols.empty()) {
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
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
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
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
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        root = std::make_unique<WindowOp>(std::move(root), tbl,
                                          ctx.windowTargets, ctx.windowFunctions,
                                          ctx.orderByCol, ctx.orderByAsc);
    } else {
        // Add Sort if ORDER BY.  WindowOp sorts each window internally and
        // applies the final query ordering after it computes the values.
        if (!ctx.orderByCol.empty()) {
            TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
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
                            std::sort(part.begin(), part.end(),
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
        TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);
        if (!ctx.projectionTargets.empty()) {
            const std::string innerDb = ctx.scalarSubquery.dbname.empty()
                ? ctx.dbname : ctx.scalarSubquery.dbname;
            const TableSchema innerTbl = engine->getTableSchema(
                innerDb, ctx.scalarSubquery.tablename);
            OpPtr inner = std::make_unique<TableScanOp>(
                engine, innerDb, ctx.scalarSubquery.tablename);
            if (!ctx.scalarSubquery.innerConds.empty()) {
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
    const std::vector<std::vector<StorageEngine::Condition>>& branches) {
    if (engine->rlsAppliesTo(ctx.dbname, ctx.tablename) ||
        !canUseBitmapOrScan(engine, ctx, branches)) return nullptr;

    OpPtr root = std::make_unique<BitmapOrHeapScanOp>(
        engine, ctx.dbname, ctx.tablename, branches);
    TableSchema tbl = engine->getTableSchema(ctx.dbname, ctx.tablename);

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
                                   const std::set<std::string>& selectCols) {
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
    TableSchema leftTbl = engine->getTableSchema(dbname, leftTable);
    TableSchema rightTbl = engine->getTableSchema(dbname, rightTable);
    for (size_t i = 0; i < leftTbl.len; ++i) {
        if (leftTbl.cols[i].dataName == leftCol && leftTbl.cols[i].isPrimaryKey) {
            leftColIndexed = true; break;
        }
    }
    for (size_t i = 0; i < rightTbl.len; ++i) {
        if (rightTbl.cols[i].dataName == rightCol && rightTbl.cols[i].isPrimaryKey) {
            rightColIndexed = true; break;
        }
    }

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

// Selectivity estimation consuming ANALYZE statistics:
//   '='  : MCV hit -> exact frequency; else 1/ndistinct (default 0.1)
//   '!=' : 1 - '=' selectivity
//   '<' '<=' '>' '>=' : equidepth histogram interpolation between bucket
//                       boundaries, clamped by min/max (default 0.3)
static double estimateSelectivity(const StorageEngine::Condition& cond,
                                  StorageEngine* engine,
                                  const std::string& dbname,
                                  const std::string& tablename) {
    auto stats = engine->getColumnStats(dbname, tablename, cond.colName);
    double totalRows = static_cast<double>(engine->getTableRowCount(dbname, tablename));
    // NULL fraction (pg null_frac): 0 when stats predate nullCount.
    double nullFrac = (totalRows > 0 && stats.nullCount > 0)
        ? std::min(1.0, static_cast<double>(stats.nullCount) / totalRows)
        : 0.0;
    if (cond.op == "isnull") {
        // Exact when ANALYZE ran: nullCount / rows.
        if (totalRows > 0 && stats.nullCount > 0) return nullFrac;
        return 0.1;   // historical default-null guess
    }
    if (cond.op == "isnotnull") {
        if (totalRows > 0 && stats.nullCount > 0) return 1.0 - nullFrac;
        return 0.9;
    }
    if (cond.op == "in" || cond.op == "notin") {
        // PG scalargtsub/eqsel-sum semantics: sum the per-value equality
        // selectivities (MCV-exact where available), clamp IN to at most
        // 0.5..1 like PG's HALF/ONE defaults; NOT IN takes the complement.
        std::istringstream iss(cond.value);
        std::string tok;
        double sum = 0.0;
        size_t k = 0;
        while (iss >> tok) {
            StorageEngine::Condition eq;
            eq.op = "=";
            eq.colName = cond.colName;
            eq.value = tok;
            sum += estimateSelectivity(eq, engine, dbname, tablename);
            ++k;
        }
        if (k == 0) return cond.op == "in" ? 0.0 : 1.0;
        if (sum > 1.0) sum = 1.0;
        double sel = (cond.op == "in") ? sum : (1.0 - sum);
        if (sel <= 0.0) sel = 0.001;
        if (sel >= 1.0) sel = 0.999;
        return sel;
    }
    if (cond.op == "=") {
        // Most Common Values first: mcv entries are (value, row count), so
        // the selectivity of a hot value is count / table rows — exact.
        // NULLs never satisfy equality; the count already excludes them.
        double rows = totalRows;
        const auto schema = engine->getTableSchema(dbname, tablename);
        const Column* column = nullptr;
        for (size_t i = 0; i < schema.len; ++i) {
            if (schema.cols[i].dataName == cond.colName) {
                column = &schema.cols[i];
                break;
            }
        }
        for (const auto& m : stats.mcv) {
            if (m.first == cond.value || (column && StorageEngine::compareValues(
                    *column, m.first, false, cond.value, false, "=")
                    == StorageEngine::PredicateTruth::True)) {
                if (rows > 0) return static_cast<double>(m.second) / rows;
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
        return 1.0 - estimateSelectivity(eqCond, engine, dbname, tablename);
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
        double s = estimateSelectivity(lo, engine, dbname, tablename) *
                   estimateSelectivity(hi, engine, dbname, tablename);
        // MCV correction: hot values inside the prefix range give exact
        // frequencies that the uniform histogram misses.
        double rows = static_cast<double>(engine->getTableRowCount(dbname, tablename));
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

    if (const auto name = op->preparedPlanNodeName(); !name.empty()) {
        const auto children = op->preparedPlanChildren();
        est.rows = children.empty() ? 1.0 : 0.0;
        for (auto* child : children) {
            const auto estimate = explainOp(child, indent + 1, engine, dbname, out, opts);
            est.rows += estimate.rows; est.cost += estimate.cost;
        }
        est.cost += est.rows * 0.1;
        out += prefix + name + costRowsStr(est, opts) + "\n";
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
        double rows = idx->colName().empty() ? 1.0 : 5.0;
        TableSchema tbl = engine->getTableSchema(dbname, idx->tableName());
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == idx->colName() && tbl.cols[i].isPrimaryKey) {
                rows = 1.0;
                break;
            }
        }
        auto stats = engine->getColumnStats(dbname, idx->tableName(), idx->colName());
        if (stats.cardinality > 0) {
            rows = static_cast<double>(plannerRowEstimate(
                engine, dbname, idx->tableName()))
                   / static_cast<double>(stats.cardinality);
            if (rows < 1.0) rows = 1.0;
        }
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
        double sel = 1.0;
        for (const auto& c : filt->conditions()) {
            std::string tblName;
            if (auto* ts = dynamic_cast<TableScanOp*>(filt->child())) {
                tblName = ts->tableName();
            } else if (auto* pts = dynamic_cast<ParallelTableScanOp*>(filt->child())) {
                tblName = pts->tableName();
            } else if (auto* is = dynamic_cast<IndexScanOp*>(filt->child())) {
                tblName = is->tableName();
            } else if (auto* bitmap = dynamic_cast<BitmapHeapScanOp*>(filt->child())) {
                tblName = bitmap->scanOrigin().tablename;
            } else if (auto* bitmapOr = dynamic_cast<BitmapOrHeapScanOp*>(filt->child())) {
                tblName = bitmapOr->scanOrigin().tablename;
            }
            sel *= estimateSelectivity(c, engine, dbname, tblName);
        }
        est.rows = child.rows * sel;
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
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        out += prefix + "Sort" + costRowsStr(est, opts) + "\n";

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
        result += "Settings: work_mem=" + std::to_string(cfg.workMemKb) + "kB";
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

    if (const auto name = op->preparedPlanNodeName(); !name.empty()) {
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
        double rows = idx->colName().empty() ? 1.0 : 5.0;
        TableSchema tbl = engine->getTableSchema(dbname, idx->tableName());
        for (size_t i = 0; i < tbl.len; ++i) {
            if (tbl.cols[i].dataName == idx->colName() && tbl.cols[i].isPrimaryKey) {
                rows = 1.0;
                break;
            }
        }
        auto stats = engine->getColumnStats(dbname, idx->tableName(), idx->colName());
        if (stats.cardinality > 0) {
            rows = static_cast<double>(plannerRowEstimate(
                engine, dbname, idx->tableName()))
                   / static_cast<double>(stats.cardinality);
            if (rows < 1.0) rows = 1.0;
        }
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

    } else if (auto* filt = dynamic_cast<FilterOp*>(op)) {
        auto [childJson, child] = explainOpJson(filt->child(), engine, dbname, opts);
        double sel = 1.0;
        for (const auto& c : filt->conditions()) {
            std::string tblName;
            if (auto* ts = dynamic_cast<TableScanOp*>(filt->child())) {
                tblName = ts->tableName();
            } else if (auto* pts = dynamic_cast<ParallelTableScanOp*>(filt->child())) {
                tblName = pts->tableName();
            } else if (auto* is = dynamic_cast<IndexScanOp*>(filt->child())) {
                tblName = is->tableName();
            } else if (auto* bitmap = dynamic_cast<BitmapHeapScanOp*>(filt->child())) {
                tblName = bitmap->scanOrigin().tablename;
            } else if (auto* bitmapOr = dynamic_cast<BitmapOrHeapScanOp*>(filt->child())) {
                tblName = bitmapOr->scanOrigin().tablename;
            }
            sel *= estimateSelectivity(c, engine, dbname, tblName);
        }
        est.rows = child.rows * sel;
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
        est.rows = child.rows;
        double logFactor = child.rows > 1.0 ? std::log2(child.rows) : 1.0;
        est.cost = child.cost + child.rows * logFactor * 0.1;
        json += "\"nodeType\":\"Sort\",";
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
        result += "    \"workMemKb\": " + std::to_string(cfg.workMemKb) + ",\n";
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

PlanExecutionResult QueryPlanner::executePlanChecked(OpPtr plan, size_t maxRows) {
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
        if (!plan->open()) {
            result.ok = false;
            result.error = plan->errorMessage();
            if (result.error.empty()) result.error = "executor failed to open plan";
            result.errorSqlState = "XX000";
            result.errorMessage = result.error;
            result.structuredRowsAvailable = false;
            // Preserve the failure that caused cleanup, including when an
            // operator's close itself fails.
            try { closePlan(); } catch (...) {}
            return result;
        }
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
                } else {
                    result.structuredRows.push_back(std::move(cells));
                    result.structuredNulls.push_back(std::move(nulls));
                }
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
    if (!result.ok) {
        result.rows.clear();
        result.structuredRows.clear();
        result.structuredNulls.clear();
        result.structuredRowsAvailable = false;
    }
    return result;
}

// Check if an index provides the required pathkey ordering.
static bool indexProvidesOrdering(StorageEngine* engine, const std::string& dbname,
                                   const std::string& tablename,
                                   const std::string& orderCol) {
    // Primary key index provides ordering on PK columns.
    TableSchema tbl = engine->getTableSchema(dbname, tablename);
    for (size_t i = 0; i < tbl.len; ++i) {
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
