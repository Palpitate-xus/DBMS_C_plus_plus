#pragma once

#include "dbms_defs.h"
#include <cstdint>
#include <cstring>

namespace dbms {

// ============================================================================
// PostgreSQL 风格 HeapTupleHeader
// ============================================================================
//
// 行头位于每行数据的最前面，大小为 t_hoff（通常 23+ bytes）。
// 格式：
//   [HeapTupleFields: t_xmin, t_xmax, t_cid]  (12 bytes)
//   [ItemPointerData: t_ctid]                  (6 bytes)
//   uint16_t t_infomask2
//   uint16_t t_infomask
//   uint8_t  t_hoff     // header 总大小
//   [t_bits: null bitmap]  // ((natts + 7) / 8) bytes，可选
//   [padding to MAXALIGN]
//   [实际列数据]
//
// 注意：为了兼容现有 16 字节 header（creatorTxnId + rollbackPtr），
// 新表使用 formatVersion=2 时启用本 header；旧表保持原格式。

#pragma pack(push, 1)

struct HeapTupleFields {
    uint32_t t_xmin;    // 插入事务 ID
    uint32_t t_xmax;    // 删除/锁定事务 ID
    union {
        uint32_t t_cid;     // 插入/删除的命令 ID（同一事务内）
        uint32_t t_xvac;    // VACUUM FULL 时移动此行的 xid
    };
};

struct HeapTupleHeaderData {
    HeapTupleFields t_fields;   // 12 bytes
    // t_ctid 紧跟在 t_fields 之后
    uint32_t t_ctid_page;       // ctid 页号
    uint16_t t_ctid_offset;     // ctid 偏移
    uint16_t t_infomask2;       // 属性数量 + 标志
    uint16_t t_infomask;        // 状态标志
    uint8_t  t_hoff;            // header 总大小（包括 t_bits）
    // t_bits[] 紧跟在后面（变长）
};

#pragma pack(pop)

// t_infomask2 标志
static constexpr uint16_t HEAP_NATTS_MASK    = 0x07FF;  // 低 11 bits = 属性数量
static constexpr uint16_t HEAP_KEYS_UPDATED  = 0x2000;  // key columns updated
static constexpr uint16_t HEAP_HOT_UPDATED   = 0x4000;  // 是 HOT update
static constexpr uint16_t HEAP_ONLY_TUPLE    = 0x8000;  // 是 HOT 链中的 only tuple

// t_infomask 标志
static constexpr uint16_t HEAP_HASNULL       = 0x0001;  // 有 null 属性
static constexpr uint16_t HEAP_HASVARWIDTH   = 0x0002;  // 有变宽属性
static constexpr uint16_t HEAP_HASEXTERNAL   = 0x0004;  // 有外部（TOAST）属性
static constexpr uint16_t HEAP_HASOID_OLD    = 0x0008;  // 有 OID（旧版本）
static constexpr uint16_t HEAP_XMAX_KEYSHR_LOCK = 0x0010;  // xmax 是 KEY SHARE 锁
static constexpr uint16_t HEAP_COMBOCID      = 0x0020;  // t_cid 是 combo CID
static constexpr uint16_t HEAP_XMAX_EXCL_LOCK   = 0x0040;  // xmax 是 exclusive 锁
static constexpr uint16_t HEAP_XMAX_LOCK_ONLY   = 0x0080;  // xmax 仅锁定
static constexpr uint16_t HEAP_XMIN_COMMITTED   = 0x0100;  // xmin 已提交（hint）
static constexpr uint16_t HEAP_XMIN_INVALID     = 0x0200;  // xmin 无效/终止（hint）
static constexpr uint16_t HEAP_XMAX_COMMITTED   = 0x0400;  // xmax 已提交（hint）
static constexpr uint16_t HEAP_XMAX_INVALID     = 0x0800;  // xmax 无效/终止（hint）
static constexpr uint16_t HEAP_XMAX_IS_MULTI    = 0x1000;  // xmax 是多事务
static constexpr uint16_t HEAP_UPDATED          = 0x2000;  // 此行被更新
static constexpr uint16_t HEAP_MOVED_OFF        = 0x4000;  // 移动到新行（旧版本）
static constexpr uint16_t HEAP_MOVED_IN         = 0x8000;  // 移动到此行（新版本）

// ============================================================================
// HeapTupleHeader 辅助函数
// ============================================================================

inline HeapTupleHeaderData* castHeapHeader(char* data) {
    return reinterpret_cast<HeapTupleHeaderData*>(data);
}
inline const HeapTupleHeaderData* castHeapHeader(const char* data) {
    return reinterpret_cast<const HeapTupleHeaderData*>(data);
}

// 获取/设置 ctid（指向最新版本的 ItemPointer）
inline ItemPointer getCtid(const HeapTupleHeaderData* htup) {
    return { htup->t_ctid_page, htup->t_ctid_offset };
}
inline void setCtid(HeapTupleHeaderData* htup, const ItemPointer& ctid) {
    htup->t_ctid_page = ctid.pageId;
    htup->t_ctid_offset = ctid.offset;
}

// 计算 header 总大小（包括 null bitmap）
inline uint8_t computeHeapHeaderSize(int natts) {
    size_t size = sizeof(HeapTupleHeaderData);
    if (natts > 0) {
        size += (natts + 7) / 8; // null bitmap
    }
    // 对齐到 MAXALIGN (8)
    size = (size + MAXALIGN - 1) & ~(MAXALIGN - 1);
    return static_cast<uint8_t>(size);
}

// 初始化 HeapTupleHeader
inline void initHeapTupleHeader(HeapTupleHeaderData* htup, uint32_t xmin,
                                uint16_t natts, bool hasNull = false,
                                bool hasVarWidth = false) {
    htup->t_fields.t_xmin = static_cast<uint32_t>(xmin);
    htup->t_fields.t_xmax = 0;
    htup->t_fields.t_cid = 0;
    htup->t_ctid_page = 0;
    htup->t_ctid_offset = 0;
    htup->t_infomask2 = natts & HEAP_NATTS_MASK;
    htup->t_infomask = 0;
    if (hasNull) htup->t_infomask |= HEAP_HASNULL;
    if (hasVarWidth) htup->t_infomask |= HEAP_HASVARWIDTH;
    htup->t_hoff = computeHeapHeaderSize(static_cast<int>(natts));
}

// 获取 null bitmap 指针（紧跟在固定 header 之后）
inline uint8_t* getNullBitmap(HeapTupleHeaderData* htup) {
    return reinterpret_cast<uint8_t*>(htup) + sizeof(HeapTupleHeaderData);
}
inline const uint8_t* getNullBitmap(const HeapTupleHeaderData* htup) {
    return reinterpret_cast<const uint8_t*>(htup) + sizeof(HeapTupleHeaderData);
}

// 检查某列是否为 null
inline bool isNull(const HeapTupleHeaderData* htup, int attnum) {
    if ((htup->t_infomask & HEAP_HASNULL) == 0) return false;
    const uint8_t* bits = getNullBitmap(htup);
    return (bits[attnum >> 3] & (1 << (attnum & 0x07))) == 0;
}

// 设置某列为 null
inline void setNull(HeapTupleHeaderData* htup, int attnum) {
    uint8_t* bits = getNullBitmap(htup);
    bits[attnum >> 3] &= ~(1 << (attnum & 0x07));
}

// 设置某列为非 null
inline void setNotNull(HeapTupleHeaderData* htup, int attnum) {
    uint8_t* bits = getNullBitmap(htup);
    bits[attnum >> 3] |= (1 << (attnum & 0x07));
}

// 检查 xmax 是否为锁标记（非删除）
inline bool xmaxIsLock(const HeapTupleHeaderData* htup) {
    return (htup->t_infomask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY)) != 0;
}

// 检查 xmax 是否已提交（hint bit）
inline bool xmaxCommitted(const HeapTupleHeaderData* htup) {
    return (htup->t_infomask & HEAP_XMAX_COMMITTED) != 0;
}
inline bool xmaxInvalid(const HeapTupleHeaderData* htup) {
    return (htup->t_infomask & HEAP_XMAX_INVALID) != 0;
}
inline bool xminCommitted(const HeapTupleHeaderData* htup) {
    return (htup->t_infomask & HEAP_XMIN_COMMITTED) != 0;
}
inline bool xminInvalid(const HeapTupleHeaderData* htup) {
    return (htup->t_infomask & HEAP_XMIN_INVALID) != 0;
}

// 设置/清除 hint bits
inline void setXminCommitted(HeapTupleHeaderData* htup) {
    htup->t_infomask |= HEAP_XMIN_COMMITTED;
    htup->t_infomask &= ~HEAP_XMIN_INVALID;
}
inline void setXminInvalid(HeapTupleHeaderData* htup) {
    htup->t_infomask |= HEAP_XMIN_INVALID;
    htup->t_infomask &= ~HEAP_XMIN_COMMITTED;
}
inline void setXmaxCommitted(HeapTupleHeaderData* htup) {
    htup->t_infomask |= HEAP_XMAX_COMMITTED;
    htup->t_infomask &= ~HEAP_XMAX_INVALID;
}
inline void setXmaxInvalid(HeapTupleHeaderData* htup) {
    htup->t_infomask |= HEAP_XMAX_INVALID;
    htup->t_infomask &= ~HEAP_XMAX_COMMITTED;
}

// 获取实际列数据起始地址
inline char* getTupleData(HeapTupleHeaderData* htup) {
    return reinterpret_cast<char*>(htup) + htup->t_hoff;
}
inline const char* getTupleData(const HeapTupleHeaderData* htup) {
    return reinterpret_cast<const char*>(htup) + htup->t_hoff;
}

} // namespace dbms
