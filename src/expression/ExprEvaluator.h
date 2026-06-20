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

// ----------------------------------------------------------------------------
// 表达式求值结果
// ----------------------------------------------------------------------------
struct ExprValue {
    std::string typeName;   // 规范类型名（如 "integer" / "character varying"）
    std::string value;      // 文本表示；空字符串与 NULL 用 isNull 区分
    bool isNull = false;

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

private:
    std::map<std::string, ExprValue> values_;
    static std::string normalize(const std::string& s);
};

// ----------------------------------------------------------------------------
// 函数回调签名
// ----------------------------------------------------------------------------
using ScalarFunction = std::function<ExprValue(const std::vector<ExprValue>&)>;

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

    // 注册 / 查找标量函数
    void registerFunction(const std::string& name, ScalarFunction fn);
    bool hasFunction(const std::string& name) const;

private:
    std::map<std::string, ScalarFunction, std::less<>> functions_;

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
    static bool likeMatch(const std::string& text, const std::string& pattern);
    static bool similarToMatch(const std::string& text, const std::string& pattern);
};

} // namespace dbms
