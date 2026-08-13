#pragma once

#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// 执行引擎接口 (Volcano Iterator Model)
// Concrete Volcano operators live in src/executor/ExecutionPlan.cpp and
// implement the lifecycle contract exposed here.
// ============================================================================

// 执行算子基类
class IOperator {
public:
    virtual ~IOperator() = default;

    // 初始化：分配资源、打开子算子；返回 true 表示成功
    virtual bool open() = 0;

    // 获取下一行；返回 true 表示有数据，false 表示流结束
    virtual bool next(std::string& out) = 0;

    // 错误状态与正常 EOF 分离。调用方必须在 next/open 返回 false 后
    // 检查该状态，不能把页面 I/O、锁冲突或策略求值失败当成空结果。
    virtual bool hasError() const { return false; }
    virtual std::string errorMessage() const { return {}; }

    // 关闭：释放资源
    virtual void close() = 0;

    // 估计行数
    virtual double estimatedRows() const { return 0; }

    // 估计成本
    virtual double estimatedCost() const { return 0; }

    // 子算子
    virtual std::vector<IOperator*> children() { return {}; }
};

} // namespace dbms
