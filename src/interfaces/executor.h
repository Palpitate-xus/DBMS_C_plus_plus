#pragma once

#include <chrono>
#include <cstdint>

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

    // ------------------------------------------------------------------
    // Runtime instrumentation for EXPLAIN ANALYZE.
    // Operators call instrumentNextStart() at the top of next() and
    // instrumentNextEnd(true/false) before returning; the counters below
    // accumulate actual time (ms), next() invocations (loops) and emitted
    // rows. Reading them is always safe; non-instrumented operators stay
    // zeroed and are simply reported without actuals.
    // ------------------------------------------------------------------
    double runtimeMs() const { return runtimeMs_; }
    uint64_t runtimeLoops() const { return runtimeLoops_; }
    uint64_t runtimeRows() const { return runtimeRows_; }
    void resetRuntimeStats() {
        runtimeMs_ = 0;
        runtimeLoops_ = 0;
        runtimeRows_ = 0;
    }

protected:
    // RAII helper: construct at the top of next(); on ANY return it stops
    // the clock, counts one loop and, when `emitted` was assigned true,
    // one output row.
    struct NextInstrument {
        IOperator* self;
        bool emitted = false;
        std::chrono::steady_clock::time_point tick;
        explicit NextInstrument(IOperator* s)
            : self(s), tick(std::chrono::steady_clock::now()) {}
        ~NextInstrument() {
            self->runtimeMs_ += std::chrono::duration<double, std::milli>(
                                    std::chrono::steady_clock::now() - tick)
                                    .count();
            ++self->runtimeLoops_;
            if (emitted) ++self->runtimeRows_;
        }
    };

private:
    double runtimeMs_ = 0;
    uint64_t runtimeLoops_ = 0;
    uint64_t runtimeRows_ = 0;
    bool rtClockActive_ = false;
    std::chrono::steady_clock::time_point rtTick_{};
};

} // namespace dbms
