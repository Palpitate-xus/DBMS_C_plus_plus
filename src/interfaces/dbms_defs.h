#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// 核心标识符类型 (对标 PostgreSQL)
// ============================================================================

using TxnId     = uint64_t;      // 事务ID
using PageId    = uint32_t;      // 页号
using RowId     = int64_t;       // 行号 / ItemPointer
using Oid       = uint32_t;      // 对象标识符
using Lsn       = uint64_t;      // WAL日志序列号
using OffsetNumber = uint16_t;   // 页内行偏移号

// 非法值常量
constexpr TxnId  INVALID_TXN_ID  = 0;
constexpr PageId INVALID_PAGE_ID = 0xFFFFFFFFu;
constexpr RowId  INVALID_ROW_ID  = -1;
constexpr Oid    INVALID_OID     = 0;
constexpr Lsn    INVALID_LSN     = 0;

// ============================================================================
// 行定位器 (ItemPointer / CTID)
// ============================================================================

struct ItemPointer {
    PageId pageId;
    OffsetNumber offset;

    bool operator==(const ItemPointer& other) const {
        return pageId == other.pageId && offset == other.offset;
    }
    bool operator!=(const ItemPointer& other) const {
        return !(*this == other);
    }
};

// ============================================================================
// 数据值 (Datum)
// ============================================================================

using Datum = uintptr_t;

constexpr Datum PointerGetDatum(const void* p) {
    return reinterpret_cast<Datum>(p);
}
constexpr void* DatumGetPointer(Datum d) {
    return reinterpret_cast<void*>(d);
}

// ============================================================================
// 快照 / 可见性
// ============================================================================

enum class IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
};

struct Snapshot {
    TxnId xmin;           // 所有小于 xid 的事务已提交/回滚
    TxnId xmax;           // 所有大于等于 xid 的事务未开始或活跃
    std::vector<TxnId> activeXids; // 活跃事务列表
    TxnId curCid;         // 当前命令ID
};

// ============================================================================
// 页常量
// ============================================================================

constexpr uint32_t BLCKSZ = 8192;           // 默认页大小 8KB
constexpr uint32_t MAXALIGN = 8;            // 最大对齐边界

// ============================================================================
// 错误码
// ============================================================================

enum class DBStatus {
    OK = 0,
    NOT_FOUND,
    ALREADY_EXISTS,
    PERMISSION_DENIED,
    OUT_OF_MEMORY,
    IO_ERROR,
    CORRUPTED_DATA,
    INVALID_ARGUMENT,
    INTERNAL_ERROR
};

} // namespace dbms
