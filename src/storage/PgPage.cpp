#include "PgPage.h"

#include <algorithm>
#include <cstring>

namespace dbms {

// ============================================================================
// Checksum (Fletcher-16)
// ============================================================================

uint16_t PgPage::computeChecksum(const char* data, size_t len) {
    uint8_t sum1 = 0, sum2 = 0;
    for (size_t i = 0; i < len; ++i) {
        sum1 = (sum1 + static_cast<uint8_t>(data[i])) % 255;
        sum2 = (sum2 + sum1) % 255;
    }
    return (static_cast<uint16_t>(sum2) << 8) | sum1;
}

void PgPage::writeChecksum() {
    PageHeaderData* h = header();
    h->pd_checksum = 0;
    const uint16_t computed = computeChecksum(buf_, PAGE_SIZE);
    // Zero is reserved for an unchecked/corrupt page in the current format.
    // Normalize the rare legitimate zero result so a valid page can never be
    // mistaken for an uninitialized page.
    h->pd_checksum = computed == 0 ? 0xFFFF : computed;
}

bool PgPage::verifyChecksum() const {
    const PageHeaderData* h = header();
    uint16_t saved = h->pd_checksum;
    if (saved == 0) return false;
    uint8_t sum1 = 0;
    uint8_t sum2 = 0;
    const auto* bytes = reinterpret_cast<const uint8_t*>(buf_);
    constexpr size_t checksumOffset = offsetof(PageHeaderData, pd_checksum);
    for (size_t i = 0; i < PAGE_SIZE; ++i) {
        const uint8_t value = (i == checksumOffset || i == checksumOffset + 1)
            ? 0 : bytes[i];
        sum1 = static_cast<uint8_t>((sum1 + value) % 255);
        sum2 = static_cast<uint8_t>((sum2 + sum1) % 255);
    }
    const uint16_t computed = (static_cast<uint16_t>(sum2) << 8) | sum1;
    return saved == (computed == 0 ? 0xFFFF : computed);
}

bool PgPage::isValid() const {
    const PageHeaderData* h = header();
    const uint16_t expectedPageSizeVersion =
        static_cast<uint16_t>((PAGE_SIZE / 512) << 8) | PG_PAGE_LAYOUT_VERSION;
    if (!verifyChecksum() || h->pd_pagesize_version != expectedPageSizeVersion ||
        h->pd_lower < sizeof(PageHeaderData) || h->pd_lower > h->pd_upper ||
        h->pd_upper > h->pd_special || h->pd_special > PAGE_SIZE ||
        ((h->pd_lower - sizeof(PageHeaderData)) % sizeof(ItemIdData)) != 0) {
        return false;
    }

    const uint16_t slots = numLinePointers();
    for (uint16_t i = 1; i <= slots; ++i) {
        const ItemIdData* id = itemId(i);
        const uint16_t flags = getLpFlags(id);
        const uint16_t off = getLpOff(id);
        const uint16_t len = getLpLen(id);
        if (flags == LP_UNUSED) {
            // A removed tuple may retain its location for transaction
            // rollback; an unused slot created by compaction is zeroed.
            if ((off != 0 || len != 0) &&
                (len == 0 || off < h->pd_upper ||
                 static_cast<size_t>(off) + len > h->pd_special)) return false;
        } else if (flags == LP_NORMAL) {
            if (len == 0 || off < h->pd_upper ||
                static_cast<size_t>(off) + len > h->pd_special) return false;
        } else if (flags == LP_REDIRECT) {
            if (len != 0 || off == 0 || off > slots) return false;
        } else if (flags == LP_DEAD) {
            if (len != 0 || off != 0) return false;
        } else {
            return false;
        }
    }
    return true;
}

// ============================================================================
// Initialization
// ============================================================================

void PgPage::init(PageId pageId) {
    (void)pageId; // pageId 不在页头中存储，由外部页号管理
    std::memset(buf_, 0, PAGE_SIZE);
    PageHeaderData* h = header();
    h->pd_lsn = INVALID_LSN;
    h->pd_checksum = 0;
    h->pd_flags = 0;
    h->pd_lower = sizeof(PageHeaderData);
    h->pd_upper = static_cast<uint16_t>(PAGE_SIZE - sizeof(uint32_t)); // reserve special space for nextPage
    h->pd_special = static_cast<uint16_t>(PAGE_SIZE - sizeof(uint32_t));
    h->pd_pagesize_version = (static_cast<uint16_t>(PAGE_SIZE / 512) << 8) | PG_PAGE_LAYOUT_VERSION;
    h->pd_prune_xid = 0;
    writeChecksum();
}

uint32_t PgPage::nextPage() const {
    uint32_t val = 0;
    std::memcpy(&val, buf_ + PAGE_SIZE - sizeof(uint32_t), sizeof(uint32_t));
    return val;
}

void PgPage::setNextPage(uint32_t next) {
    std::memcpy(buf_ + PAGE_SIZE - sizeof(uint32_t), &next, sizeof(uint32_t));
    writeChecksum();
}

// ============================================================================
// Insert
// ============================================================================

bool PgPage::insert(const char* data, size_t len, OffsetNumber& linePtr) {
    if (len == 0 || len > PAGE_SIZE - sizeof(PageHeaderData) - sizeof(ItemIdData)) {
        return false;
    }

    PageHeaderData* h = header();

    // 1. 尝试重用已删除的 line pointer
    uint16_t n = numLinePointers();
    for (uint16_t i = 1; i <= n; ++i) {
        ItemIdData* id = itemId(i);
        if (getLpFlags(id) == LP_UNUSED) {
            // 检查空间是否足够
            if (len <= static_cast<size_t>(h->pd_upper - h->pd_lower - sizeof(ItemIdData))) {
                h->pd_upper -= static_cast<uint16_t>(len);
                std::memcpy(buf_ + h->pd_upper, data, len);
                setLpOff(id, h->pd_upper);
                setLpLen(id, static_cast<uint16_t>(len));
                setLpFlags(id, LP_NORMAL);
                linePtr = i;
                writeChecksum();
                return true;
            }
        }
    }

    // 2. 需要新的 line pointer
    size_t need = sizeof(ItemIdData) + len;
    if (static_cast<size_t>(h->pd_upper) < h->pd_lower + need) {
        return false;
    }

    // 写入数据（从底部向上）
    h->pd_upper -= static_cast<uint16_t>(len);
    std::memcpy(buf_ + h->pd_upper, data, len);

    // 分配新的 line pointer
    OffsetNumber newLp = static_cast<OffsetNumber>(n + 1);
    ItemIdData* id = itemId(newLp);
    setLpOff(id, h->pd_upper);
    setLpLen(id, static_cast<uint16_t>(len));
    setLpFlags(id, LP_NORMAL);

    h->pd_lower += sizeof(ItemIdData);
    linePtr = newLp;
    writeChecksum();
    return true;
}

// ============================================================================
// Remove (mark as UNUSED)
// ============================================================================

bool PgPage::remove(OffsetNumber linePtr) {
    if (linePtr == 0) return false;
    uint16_t n = numLinePointers();
    if (linePtr > n) return false;

    ItemIdData* id = itemId(linePtr);
    if (getLpFlags(id) != LP_NORMAL) {
        return false;
    }

    setLpFlags(id, LP_UNUSED);

    header()->pd_flags |= PD_HAS_FREE_LINES;
    writeChecksum();
    return true;
}

// ============================================================================
// Restore (undo remove: LP_UNUSED -> LP_NORMAL)
// ============================================================================

bool PgPage::restore(OffsetNumber linePtr) {
    if (linePtr == 0) return false;
    uint16_t n = numLinePointers();
    if (linePtr > n) return false;

    ItemIdData* id = itemId(linePtr);
    if (getLpFlags(id) != LP_UNUSED) return false;
    if (getLpLen(id) == 0) return false;

    setLpFlags(id, LP_NORMAL);
    writeChecksum();
    return true;
}

// ============================================================================
// HOT redirect: LP_NORMAL -> LP_REDIRECT
// ============================================================================

bool PgPage::redirect(OffsetNumber fromLinePtr, OffsetNumber toLinePtr) {
    if (fromLinePtr == 0 || toLinePtr == 0) return false;
    uint16_t n = numLinePointers();
    if (fromLinePtr > n || toLinePtr > n) return false;

    ItemIdData* fromId = itemId(fromLinePtr);
    if (getLpFlags(fromId) != LP_NORMAL) return false;

    ItemIdData* toId = itemId(toLinePtr);
    if (getLpFlags(toId) != LP_NORMAL) return false;

    // Redirect: store target offset in lp_off, set flag to LP_REDIRECT, len = 0
    setLpOff(fromId, toLinePtr);
    setLpLen(fromId, 0);
    setLpFlags(fromId, LP_REDIRECT);
    writeChecksum();
    return true;
}

// ============================================================================
// Get
// ============================================================================

bool PgPage::get(OffsetNumber linePtr, const char*& data, size_t& len) const {
    if (linePtr == 0) return false;
    uint16_t n = numLinePointers();
    if (linePtr > n) return false;

    const ItemIdData* id = itemId(linePtr);
    uint16_t flags = getLpFlags(id);

    // Follow HOT redirect chain (with simple cycle guard)
    if (flags == LP_REDIRECT) {
        OffsetNumber target = getLpOff(id);
        for (int guard = 0; guard < 16 && target != 0 && target <= n; ++guard) {
            const ItemIdData* tid = itemId(target);
            if (getLpFlags(tid) != LP_REDIRECT) {
                id = tid;
                flags = getLpFlags(id);
                linePtr = target;
                break;
            }
            target = getLpOff(tid);
        }
        if (flags == LP_REDIRECT) return false;
    }

    if (flags != LP_NORMAL) return false;

    uint16_t off = getLpOff(id);
    uint16_t l = getLpLen(id);
    data = buf_ + off;
    len = l;
    return true;
}

// ============================================================================
// Update
// ============================================================================

bool PgPage::update(OffsetNumber linePtr, const char* data, size_t len,
                    OffsetNumber* newLinePtr) {
    if (linePtr == 0) return false;
    uint16_t n = numLinePointers();
    if (linePtr > n) return false;

    ItemIdData* id = itemId(linePtr);
    if (getLpFlags(id) != LP_NORMAL) return false;

    uint16_t oldLen = getLpLen(id);

    // 原位更新：新长度 <= 旧长度
    if (len <= oldLen) {
        uint16_t off = getLpOff(id);
        std::memcpy(buf_ + off, data, len);
        if (len < oldLen) {
            std::memset(buf_ + off + len, 0, oldLen - len);
        }
        setLpLen(id, static_cast<uint16_t>(len));
        writeChecksum();
        if (newLinePtr) *newLinePtr = linePtr;
        return true;
    }

    // 空间不够：删除旧 + 插入新
    if (!remove(linePtr)) return false;

    OffsetNumber insertedLp = 0;
    if (!insert(data, len, insertedLp)) {
        // 插入失败：恢复旧行（best effort）
        setLpFlags(id, LP_NORMAL);
        writeChecksum();
        if (newLinePtr) *newLinePtr = linePtr;
        return false;
    }

    if (newLinePtr) *newLinePtr = insertedLp;
    return true;
}

// ============================================================================
// Compact: defragment page
// ============================================================================

void PgPage::compact() {
    PageHeaderData* h = header();
    uint16_t n = numLinePointers();
    if (n == 0) return;

    // 收集所有活行
    struct LiveItem {
        OffsetNumber linePtr;
        uint16_t off;
        uint16_t len;
    };
    LiveItem items[256];
    uint16_t liveCount = 0;

    for (uint16_t i = 1; i <= n; ++i) {
        ItemIdData* id = itemId(i);
        if (getLpFlags(id) == LP_NORMAL && liveCount < 256) {
            items[liveCount++] = {i, getLpOff(id), getLpLen(id)};
        }
    }

    if (liveCount == 0) {
        // 全部删除：重置页
        init(0);
        return;
    }

    // 按偏移量降序排序（从底部到顶部）
    std::sort(items, items + liveCount,
              [](const LiveItem& a, const LiveItem& b) {
                  return a.off > b.off;
              });

    // 重新排列数据
    // Keep the reserved special space outside the heap tuple area.  Starting
    // at PAGE_SIZE would place compacted tuples over the free-list pointer
    // stored in the final four bytes and makes the page structurally invalid.
    uint16_t newUpper = h->pd_special;
    for (uint16_t i = 0; i < liveCount; ++i) {
        newUpper -= items[i].len;
        std::memmove(buf_ + newUpper, buf_ + items[i].off, items[i].len);
        // 更新 line pointer
        ItemIdData* id = itemId(items[i].linePtr);
        setLpOff(id, newUpper);
    }

    // 清理 trailing unused line pointers
    while (n > 0) {
        ItemIdData* id = itemId(n);
        if (getLpFlags(id) != LP_UNUSED) break;
        setLpOff(id, 0);
        setLpLen(id, 0);
        --n;
    }

    h->pd_lower = static_cast<uint16_t>(sizeof(PageHeaderData) + n * sizeof(ItemIdData));
    h->pd_upper = newUpper;
    h->pd_flags &= ~PD_HAS_FREE_LINES;
    writeChecksum();
}

// ============================================================================
// Utilities
// ============================================================================

size_t PgPage::freeSpace() const {
    const PageHeaderData* h = header();
    if (h->pd_upper <= h->pd_lower) return 0;
    return h->pd_upper - h->pd_lower;
}

size_t PgPage::freeSpaceWithFrag() const {
    const PageHeaderData* h = header();
    if (h->pd_upper <= h->pd_lower) return 0;

    size_t free = h->pd_upper - h->pd_lower;

    // 加上所有 UNUSED line pointer 对应的空间（可重用）
    uint16_t n = numLinePointers();
    for (uint16_t i = 1; i <= n; ++i) {
        const ItemIdData* id = itemId(i);
        if (getLpFlags(id) == LP_UNUSED) {
            free += sizeof(ItemIdData);
        }
    }
    return free;
}

bool PgPage::canFit(size_t len) const {
    const PageHeaderData* h = header();
    size_t need = sizeof(ItemIdData) + len;
    return static_cast<size_t>(h->pd_upper) >= h->pd_lower + need;
}

uint16_t PgPage::numLinePointers() const {
    const PageHeaderData* h = header();
    if (h->pd_lower <= sizeof(PageHeaderData)) return 0;
    return static_cast<uint16_t>((h->pd_lower - sizeof(PageHeaderData)) / sizeof(ItemIdData));
}

uint16_t PgPage::liveCount() const {
    uint16_t count = 0;
    uint16_t n = numLinePointers();
    for (uint16_t i = 1; i <= n; ++i) {
        if (getLpFlags(itemId(i)) == LP_NORMAL) {
            ++count;
        }
    }
    return count;
}

} // namespace dbms
