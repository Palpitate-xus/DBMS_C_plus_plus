// ============================================================================
// Expression Evaluator — Phase 4 Wave 0.2
//
// 基于 Parser AST 的表达式求值器，为 CHECK / DEFAULT / GENERATED / WHERE /
// USING / WITH CHECK 提供统一求值入口。
//
// 当前实现覆盖：
//   - 字面量、列引用、NULL
//   - 一元 / 二元运算、比较、逻辑运算
//   - BETWEEN / IN / LIKE / ILIKE / SIMILAR TO
//   - CASE / COALESCE / NULLIF / GREATEST / LEAST
//   - CAST(expr AS type)
//   - 简单函数调用（内置常用函数子集）
// ============================================================================

#pragma once

#include "parser/ast.h"
#include "catalog/type_registry.h"
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

// Forward declaration so sequence builtins can delegate to the engine.
class StorageEngine;

// ----------------------------------------------------------------------------
// 表达式求值结果
// ----------------------------------------------------------------------------
struct ExprValue {
    std::string typeName;   // 规范类型名（如 "integer" / "character varying"）
    std::string value;      // 文本表示；空字符串与 NULL 用 isNull 区分
    bool isNull = false;
    std::string collation;  // Explicit COLLATE on this expression, if any.

    ExprValue() = default;
    ExprValue(std::string typeName_, std::string value_, bool isNull_ = false)
        : typeName(std::move(typeName_)), value(std::move(value_)), isNull(isNull_) {}

    bool isUnknown() const { return typeName.empty() && value.empty() && !isNull; }
    bool asBool() const;
    int64_t asInt() const;
    double asDouble() const;
};

// ----------------------------------------------------------------------------
// 行上下文：为列引用提供值
// ----------------------------------------------------------------------------
class RowContext {
public:
    RowContext() = default;

    // 直接构造（用于测试或简单场景）
    explicit RowContext(std::map<std::string, ExprValue> values)
        : values_(std::move(values)) {}

    void set(const std::string& name, ExprValue val) { values_[normalize(name)] = std::move(val); }
    std::optional<ExprValue> get(const std::string& name) const;
    bool has(const std::string& name) const { return get(name).has_value(); }
    void setParameters(std::vector<ExprValue> cells) { parameters_ = std::move(cells); }
    const ExprValue& parameter(size_t slot) const;
    // Prepared SQL uses query-owned occurrence/descriptor identities, never
    // RowContext's case-insensitive legacy identifier map.
    void setBoundColumn(size_t sourceOrdinal, size_t columnOrdinal, ExprValue cell) {
        boundColumns_[{sourceOrdinal, columnOrdinal}] = std::move(cell);
    }
    const ExprValue& boundColumn(size_t sourceOrdinal, size_t columnOrdinal) const;

private:
    std::map<std::string, ExprValue> values_;
    std::vector<ExprValue> parameters_;
    std::map<std::pair<size_t, size_t>, ExprValue> boundColumns_;
    static std::string normalize(const std::string& s);
};

// ----------------------------------------------------------------------------
// 函数回调签名
// ----------------------------------------------------------------------------
using ScalarFunction = std::function<ExprValue(const std::vector<ExprValue>&)>;
using ScalarSubqueryExecutor = std::function<ExprValue(const Expr*, const RowContext&)>;

// ----------------------------------------------------------------------------
// 表达式求值器
// ----------------------------------------------------------------------------
class ExprEvaluator {
public:
    ExprEvaluator();

    // 主入口
    ExprValue eval(const Expr* expr, const RowContext& ctx) const;
    ExprValue eval(const ExprPtr& expr, const RowContext& ctx) const {
        return expr ? eval(expr.get(), ctx) : ExprValue{};
    }

    // Analyze collation-bearing result arms without executing the expression.
    // Conflicting explicit collations raise SQLSTATE 42P21 even for zero rows.
    static std::string analyzeExplicitResultCollation(const Expr* expr);

    // 注册 / 查找标量函数
    void registerFunction(const std::string& name, ScalarFunction fn);
    void registerFunction(const std::string& name, ScalarFunction fn, char volatility);
    bool hasFunction(const std::string& name) const;
    char volatility(const std::string& name) const;

    // Resolve canonical SQL function names from metadata only. Preparation
    // binds stored calls to private registry slots; it never executes them.
    bool hasScalarFunction(const FunctionCallExpr* call,
                           StorageEngine* engine = nullptr) const;
    char scalarFunctionVolatility(const FunctionCallExpr* call,
                                  StorageEngine* engine = nullptr) const;
    // Empty for builtins whose result type is inferred from their operands.
    std::string scalarFunctionResultType(const FunctionCallExpr* call,
                                         StorageEngine* engine = nullptr) const;
    // Internal metadata identity, not a SQL name or an evaluated datum.
    std::string scalarFunctionIdentity(const FunctionCallExpr* call,
                                       StorageEngine* engine = nullptr) const;
    void bindScalarFunctions(Expr* expression, StorageEngine* engine = nullptr);

    // 设置当前数据库，供 nextval/currval/lastval 等内置函数使用
    void setCurrentDB(const std::string& db) { currentDB_ = db; }
    // The caller owns the prepared statement/query context. This callback is
    // invoked only when evaluation reaches that prepared child expression.
    void setScalarSubqueryExecutor(ScalarSubqueryExecutor executor) {
        scalarSubqueryExecutor_ = std::move(executor);
    }

private:
    std::map<std::string, ScalarFunction, std::less<>> functions_;
    std::map<std::string, char, std::less<>> volatility_;
    std::string currentDB_;
    ScalarSubqueryExecutor scalarSubqueryExecutor_;

    void registerBuiltins();

    ExprValue evalLiteral(const LiteralExpr* e) const;
    ExprValue evalColumnRef(const ColumnRefExpr* e, const RowContext& ctx) const;
    ExprValue evalUnaryOp(const UnaryOpExpr* e, const RowContext& ctx) const;
    ExprValue evalBinaryOp(const BinaryOpExpr* e, const RowContext& ctx) const;
    ExprValue evalCase(const CaseExpr* e, const RowContext& ctx) const;
    ExprValue evalCast(const CastExpr* e, const RowContext& ctx) const;
    ExprValue evalCast(const Expr* /*unused*/, const RowContext& /*unused*/,
                       const ExprValue& v, const std::string& targetTypeName) const;
    ExprValue evalFunctionCall(const FunctionCallExpr* e, const RowContext& ctx) const;
    ExprValue evalArrayExpr(const ArrayExpr* e, const RowContext& ctx) const;
    ExprValue evalRowExpr(const RowExpr* e, const RowContext& ctx) const;

    // 辅助
    static int compareValues(const ExprValue& a, const ExprValue& b);
    static ExprValue applyComparison(const std::string& op, const ExprValue& l, const ExprValue& r);
    static ExprValue applyArithmetic(const std::string& op, const ExprValue& l, const ExprValue& r);
    static std::optional<bool> rangesOverlap(const ExprValue& left,
                                             const ExprValue& right);
    static std::optional<bool> rangeContains(const ExprValue& container,
                                             const ExprValue& contained);
    static bool likeMatch(const std::string& text, const std::string& pattern);
    static bool similarToMatch(const std::string& text, const std::string& pattern);
};

} // namespace dbms
