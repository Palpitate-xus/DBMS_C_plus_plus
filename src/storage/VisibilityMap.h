#pragma once

#include <cstdint>
#include <cstring>
#include <fstream>
#include <mutex>
#include <vector>

namespace dbms {

// ========================================================================
// VisibilityMap - 记录每个数据页的"全部可见"状态
// ========================================================================
// 文件格式：bitmap，每个数据页对应 1 bit
//   bit = 1  -> 该页所有行对所有事务可见（AllVisible）
//   bit = 0  -> 该页可能有不可见行（需要检查）
//
// 用途：
//   1. 加速 VACUUM：跳过 AllVisible 页面
//   2. 只读查询优化：Index-only scan 可跳过 heap 访问
//
// 每 8 个页面占 1 字节。pageId N 对应的字节索引 = N/8，位索引 = N%8。
//
// 线程安全：内部互斥锁保护内存缓存与磁盘文件（bgwriter/vacuum 与
// 前台 DML 可并发访问）。

class VisibilityMap {
public:
    explicit VisibilityMap(const std::string& filename);
    ~VisibilityMap();

    bool open();
    void close();
    bool isOpen() const;

    // 获取指定页的 AllVisible 状态
    bool isAllVisible(uint32_t pageId) const;

    // 设置指定页的 AllVisible 状态
    void setAllVisible(uint32_t pageId, bool visible);

    // 获取当前支持的最大页数
    uint32_t capacity() const;

    // 将内存缓存刷盘
    void flush();

private:
    std::string filename_;
    mutable std::fstream f_;
    mutable std::vector<uint8_t> cache_;
    mutable bool dirty_ = false;
    mutable std::mutex mutex_;

    void ensureSizeLocked(uint32_t pageId);
    void loadFromDiskLocked() const;
    void writeToDiskLocked() const;

    static size_t byteIndex(uint32_t pageId) { return pageId / 8; }
    static uint8_t bitMask(uint32_t pageId) { return static_cast<uint8_t>(1) << (pageId % 8); }
};

} // namespace dbms
