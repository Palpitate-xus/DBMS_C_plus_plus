#pragma once

#include "dbms_defs.h"
#include <atomic>
#include <mutex>
#include <string>

namespace dbms {

// ============================================================================
// OID 分配器（PostgreSQL 兼容）
//
// PostgreSQL 的 OID 空间：
//   0      = InvalidOid
//   1~9999 = 系统保留（bootstrap 对象）
//   10000+ = 用户对象（实际从 16384 开始分配更常见）
//
// 当前策略：
//   1. 从 10000 开始单调递增
//   2. 持久化到单个计数器文件
//   3. 支持预留批量 OID（减少文件 IO）
// ============================================================================

class OidGenerator {
public:
    static constexpr Oid kFirstUserOid = 10000;

    // 初始化（从持久化文件加载当前值）
    explicit OidGenerator(const std::string& persistPath);

    // 分配单个 OID
    Oid allocate();

    // 批量预留 OID（返回起始 OID，连续 count 个）
    Oid allocateBatch(uint32_t count);

    // 获取下一个将分配的 OID（不分配）
    Oid peekNext() const;

    // 显式持久化当前计数器（通常在事务提交时调用）
    void persist();

    // 设置下一个 OID（用于恢复或测试）
    void setNext(Oid next);

private:
    std::atomic<Oid> nextOid_{kFirstUserOid};
    std::string persistPath_;
    mutable std::mutex mutex_;
};

} // namespace dbms
