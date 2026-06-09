#pragma once

#include "dbms_defs.h"
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// 执行引擎接口 (Volcano Iterator Model)
// Phase 5 逐步实现：所有执行算子实现 IOperator
// ============================================================================

class IOperator;

// 单行数据（列名 -> 值）
using Tuple = std::map<std::string, std::string>;

// 表达式求值接口
class IExpr {
public:
    virtual ~IExpr() = default;
    virtual std::string eval(const Tuple& row) const = 0;
    virtual std::string toString() const = 0;
};

// 执行算子基类
class IOperator {
public:
    virtual ~IOperator() = default;

    // 初始化：分配资源、打开子算子
    virtual void open() = 0;

    // 获取下一行；返回 true 表示有数据，false 表示流结束
    virtual bool next(Tuple& out) = 0;

    // 关闭：释放资源
    virtual void close() = 0;

    // 估计行数
    virtual double estimatedRows() const { return 0; }

    // 估计成本
    virtual double estimatedCost() const { return 0; }

    // 子算子
    virtual std::vector<IOperator*> children() { return {}; }
};

// ============================================================================
// 执行计划接口
// ============================================================================

struct PlanNode {
    enum Type {
        SEQ_SCAN,
        INDEX_SCAN,
        INDEX_ONLY_SCAN,
        BITMAP_HEAP_SCAN,
        NESTED_LOOP_JOIN,
        HASH_JOIN,
        MERGE_JOIN,
        FILTER,
        PROJECT,
        AGGREGATE,
        SORT,
        LIMIT,
        DISTINCT,
        UNION,
        SETOP,
        CTE_SCAN,
        WORKTABLE_SCAN,
        FUNCTION_SCAN
    };

    Type type;
    std::vector<std::unique_ptr<PlanNode>> children;
    double cost;
    double rows;
    int width;
};

class IExecutionPlanner {
public:
    virtual ~IExecutionPlanner() = default;

    // 生成物理执行计划
    virtual std::unique_ptr<PlanNode> plan(
        const std::string& sql,
        const Snapshot* snapshot) = 0;

    // 执行计划
    virtual std::unique_ptr<IOperator> buildExecutor(
        const PlanNode* plan) = 0;

    // EXPLAIN 输出
    virtual std::string explain(const PlanNode* plan, bool analyze) const = 0;
};

} // namespace dbms
