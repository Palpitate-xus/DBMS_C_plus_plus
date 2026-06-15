#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
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
    uint32_t version = 1;            // 序列化格式版本
    TxnId xmin = 0;                  // 所有小于 xid 的事务已提交/回滚
    TxnId xmax = 0;                  // 所有大于等于 xid 的事务未开始或活跃
    std::vector<TxnId> activeXids;   // 活跃事务列表
    std::vector<TxnId> subxip;       // 子事务进行中列表
    TxnId curCid = 0;                // 当前命令ID

    // 二进制序列化（小端，稳定格式 v1）
    std::string exportToBytes() const;

    // 反序列化；失败返回 std::nullopt
    static std::optional<Snapshot> importFromBytes(const std::string& bytes);
};

// Snapshot v1 二进制格式（小端）：
// [0]   uint32 magic = 0x534E4150  // "SNAP"
// [4]   uint32 version = 1
// [8]   uint64 xmin
// [16]  uint64 xmax
// [24]  uint64 curCid
// [32]  uint32 activeXids count (N)
// [36]  uint32 subxip count (M)
// [40]  N * uint64 activeXids
// [40+N*8] M * uint64 subxip
inline std::string Snapshot::exportToBytes() const {
    std::string out;
    out.reserve(40 + (activeXids.size() + subxip.size()) * sizeof(TxnId));

    auto appendU32 = [&out](uint32_t v) {
        out.append(reinterpret_cast<const char*>(&v), sizeof(v));
    };
    auto appendU64 = [&out](uint64_t v) {
        out.append(reinterpret_cast<const char*>(&v), sizeof(v));
    };

    appendU32(0x534E4150u); // "SNAP"
    appendU32(version);
    appendU64(xmin);
    appendU64(xmax);
    appendU64(curCid);
    appendU32(static_cast<uint32_t>(activeXids.size()));
    appendU32(static_cast<uint32_t>(subxip.size()));
    for (TxnId xid : activeXids) appendU64(xid);
    for (TxnId xid : subxip) appendU64(xid);
    return out;
}

inline std::optional<Snapshot> Snapshot::importFromBytes(const std::string& bytes) {
    if (bytes.size() < 40) return std::nullopt;
    size_t pos = 0;

    auto readU32 = [&bytes, &pos]() -> uint32_t {
        uint32_t v = 0;
        std::memcpy(&v, bytes.data() + pos, sizeof(v));
        pos += sizeof(v);
        return v;
    };
    auto readU64 = [&bytes, &pos]() -> uint64_t {
        uint64_t v = 0;
        std::memcpy(&v, bytes.data() + pos, sizeof(v));
        pos += sizeof(v);
        return v;
    };

    uint32_t magic = readU32();
    if (magic != 0x534E4150u) return std::nullopt;
    uint32_t version = readU32();
    if (version != 1) return std::nullopt;

    Snapshot snap;
    snap.version = version;
    snap.xmin = readU64();
    snap.xmax = readU64();
    snap.curCid = readU64();
    uint32_t activeCount = readU32();
    uint32_t subCount = readU32();

    size_t needed = static_cast<size_t>(activeCount + subCount) * sizeof(TxnId);
    if (bytes.size() - pos < needed) return std::nullopt;

    snap.activeXids.resize(activeCount);
    for (uint32_t i = 0; i < activeCount; ++i) snap.activeXids[i] = readU64();
    snap.subxip.resize(subCount);
    for (uint32_t i = 0; i < subCount; ++i) snap.subxip[i] = readU64();
    return snap;
}

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
    INTERNAL_ERROR,
    // Storage-engine specific codes (unified from the old DBStatus enum)
    TABLE_NOT_FOUND,
    DATABASE_NOT_FOUND,
    TABLE_ALREADY_EXISTS,
    INVALID_VALUE,
    NULL_NOT_ALLOWED,
    SYNTAX_ERROR,
    DUPLICATE_KEY,
    LOCK_CONFLICT,
    SERIALIZATION_FAILURE
};

} // namespace dbms
