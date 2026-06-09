#pragma once

#include "dbms_defs.h"
#include <cstdint>
#include <cstring>
#include <cassert>

namespace dbms {

// ============================================================================
// PostgreSQL 风格页格式（8KB）
// ============================================================================
//
// 页布局（从低地址到高地址）：
//   +----------------------------------+
//   | PageHeaderData (24 bytes)        |
//   +----------------------------------+
//   | ItemIdData[0] | ItemIdData[1]... |  <-- pd_lower 之前，向下增长
//   +----------------------------------+
//   |            free space            |
//   +----------------------------------+
//   | Tuple N ... Tuple 0              |  <-- pd_upper 之后，向上增长
//   +----------------------------------+
//   | special space (index pages only) |  <-- pd_special 之后，向下增长
//   +----------------------------------+
//
// freeSpace = pd_upper - pd_lower
//
// 兼容策略：
//   FileHeader.formatVersion = 0/1 -> 使用旧 Page 类（4096 bytes）
//   FileHeader.formatVersion = 2   -> 使用本 PgPage 类（8192 bytes）

class PgPage {
public:
    static constexpr size_t PAGE_SIZE = BLCKSZ;           // 8192
    static constexpr size_t MAXALIGN = 8;
    static constexpr uint16_t PG_PAGE_LAYOUT_VERSION = 4; // PostgreSQL 页布局版本号

    // ------------------------------------------------------------------------
    // PageHeaderData - 页头，固定 24 字节
    // ------------------------------------------------------------------------
#pragma pack(push, 1)
    struct PageHeaderData {
        Lsn      pd_lsn;          // 8 bytes: 最后修改此页的 XLOG 记录 LSN
        uint16_t pd_checksum;     // 2 bytes: 页校验和
        uint16_t pd_flags;        // 2 bytes: 标志位
        uint16_t pd_lower;        // 2 bytes: free space 起始偏移
        uint16_t pd_upper;        // 2 bytes: free space 结束偏移（tuple 起始）
        uint16_t pd_special;      // 2 bytes: special space 起始偏移
        uint16_t pd_pagesize_version; // 2 bytes: 高8位=页大小版本，低8位=布局版本
        uint32_t pd_prune_xid;    // 4 bytes: 最老的未修剪 XMAX
    };
#pragma pack(pop)

    // 页标志位
    static constexpr uint16_t PD_HAS_FREE_LINES = 0x0001;   // 有未使用的 line pointer
    static constexpr uint16_t PD_PAGE_FULL      = 0x0002;   // 页面已满提示
    static constexpr uint16_t PD_ALL_VISIBLE    = 0x0004;   // 所有行对所有事务可见

    // ------------------------------------------------------------------------
    // ItemIdData - Line Pointer，每个 tuple 一个，4 字节
    // ------------------------------------------------------------------------
#pragma pack(push, 1)
    struct ItemIdData {
        uint16_t lp_off;    // 15 bits: tuple 在页内的字节偏移
        uint16_t lp_len;    // 15 bits: tuple 字节长度；高 2 bits 为 lp_flags
    };
#pragma pack(pop)

    // lp_flags 编码在 lp_len 的高 2 bits
    static constexpr uint16_t LP_UNUSED   = 0;  // 未使用
    static constexpr uint16_t LP_NORMAL   = 1;  // 正常使用的行
    static constexpr uint16_t LP_REDIRECT = 2;  // HOT 重定向
    static constexpr uint16_t LP_DEAD     = 3;  // 死行（HOT 链中）

    static constexpr uint16_t LP_FLAG_SHIFT = 14;
    static constexpr uint16_t LP_LEN_MASK   = 0x3FFF;
    static constexpr uint16_t LP_OFF_MASK   = 0x7FFF;

    // ------------------------------------------------------------------------
    // 构造 / 初始化
    // ------------------------------------------------------------------------
    explicit PgPage(char* buf) : buf_(buf) {}

    void init(PageId pageId);

    // Free-list chaining (stored in special space, 4 bytes at end of page)
    uint32_t nextPage() const;
    void setNextPage(uint32_t next);

    // ------------------------------------------------------------------------
    // 行操作
    // ------------------------------------------------------------------------
    // 插入一行数据，返回 line pointer 编号（1-based，0 表示失败）
    bool insert(const char* data, size_t len, OffsetNumber& linePtr);

    // 删除一行（标记为 unused，空间不回收，等待 VACUUM）
    bool remove(OffsetNumber linePtr);

    // 恢复一行（将 LP_UNUSED 改回 LP_NORMAL，用于 ROLLBACK）
    bool restore(OffsetNumber linePtr);

    // 读取一行
    bool get(OffsetNumber linePtr, const char*& data, size_t& len) const;

    // 更新一行：若空间足够原位更新，否则删除+插入
    bool update(OffsetNumber linePtr, const char* data, size_t len,
                OffsetNumber* newLinePtr = nullptr);

    // 压缩页：移动活tuple到一起，重建 line pointer 数组
    void compact();

    // ------------------------------------------------------------------------
    // 空闲空间管理
    // ------------------------------------------------------------------------
    size_t freeSpace() const;
    size_t freeSpaceWithFrag() const; // 包含可重用的已删除 slot 空间
    bool canFit(size_t len) const;

    // ------------------------------------------------------------------------
    // 访问器
    // ------------------------------------------------------------------------
    PageHeaderData* header() const {
        return reinterpret_cast<PageHeaderData*>(buf_);
    }
    ItemIdData* itemId(OffsetNumber n) const {
        assert(n >= 1 && n <= maxLinePointers());
        return reinterpret_cast<ItemIdData*>(buf_ + sizeof(PageHeaderData)
                                               + (n - 1) * sizeof(ItemIdData));
    }
    uint16_t numLinePointers() const;
    uint16_t maxLinePointers() const {
        return static_cast<uint16_t>((PAGE_SIZE - sizeof(PageHeaderData)) / sizeof(ItemIdData));
    }

    // 特殊空间（索引页用）
    char* specialSpace() const {
        return buf_ + header()->pd_special;
    }
    size_t specialSpaceSize() const {
        return PAGE_SIZE - header()->pd_special;
    }

    // ------------------------------------------------------------------------
    // 校验和
    // ------------------------------------------------------------------------
    static uint16_t computeChecksum(const char* data, size_t len);
    void writeChecksum();
    bool verifyChecksum() const;

    // ------------------------------------------------------------------------
    // 行计数
    // ------------------------------------------------------------------------
    uint16_t liveCount() const;

    // 遍历所有活行：fn(linePtr, data, len)
    template<typename Fn>
    void forEachLive(Fn&& fn) const {
        uint16_t n = numLinePointers();
        for (uint16_t i = 1; i <= n; ++i) {
            const ItemIdData* id = itemId(i);
            uint16_t flags = id->lp_len >> LP_FLAG_SHIFT;
            if (flags == LP_NORMAL) {
                uint16_t off = id->lp_off & LP_OFF_MASK;
                uint16_t len = id->lp_len & LP_LEN_MASK;
                fn(i, buf_ + off, len);
            }
        }
    }

    // ------------------------------------------------------------------------
    // 设置/获取标志
    // ------------------------------------------------------------------------
    bool hasFlag(uint16_t flag) const { return (header()->pd_flags & flag) != 0; }
    void setFlag(uint16_t flag) { header()->pd_flags |= flag; }
    void clearFlag(uint16_t flag) { header()->pd_flags &= ~flag; }

private:
    char* buf_;

    // lp_flags 辅助
    static uint16_t getLpFlags(const ItemIdData* id) {
        return id->lp_len >> LP_FLAG_SHIFT;
    }
    static void setLpFlags(ItemIdData* id, uint16_t flags) {
        id->lp_len = (id->lp_len & LP_LEN_MASK) | (flags << LP_FLAG_SHIFT);
    }
    static uint16_t getLpLen(const ItemIdData* id) {
        return id->lp_len & LP_LEN_MASK;
    }
    static void setLpLen(ItemIdData* id, uint16_t len) {
        uint16_t flags = getLpFlags(id);
        id->lp_len = (len & LP_LEN_MASK) | (flags << LP_FLAG_SHIFT);
    }
    static uint16_t getLpOff(const ItemIdData* id) {
        return id->lp_off & LP_OFF_MASK;
    }
    static void setLpOff(ItemIdData* id, uint16_t off) {
        id->lp_off = off & LP_OFF_MASK;
    }
};

} // namespace dbms
