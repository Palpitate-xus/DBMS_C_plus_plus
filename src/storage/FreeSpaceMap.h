#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

namespace dbms {

// ========================================================================
// FreeSpaceMap - 记录每个数据页的空闲空间百分比
// ========================================================================
// 文件格式：每个数据页对应 1 字节
//   0..100  = 空闲空间百分比 (freeSpace * 100 / pageSize)
//   255     = 未知 / 未初始化
//
// 用途：INSERT 时快速定位有足够空间的页面，避免逐页扫描。

class FreeSpaceMap {
public:
    explicit FreeSpaceMap(const std::string& filename);
    ~FreeSpaceMap();

    bool open();
    void close();
    bool isOpen() const { return f_.is_open(); }

    // 获取指定页的空闲空间百分比（0~100），未知返回 255
    uint8_t getFreePercent(uint32_t pageId) const;

    // 设置指定页的空闲空间百分比
    void setFreePercent(uint32_t pageId, uint8_t percent);

    // 查找第一个空闲空间 >= minPercent 的页面（从 startPage 开始）
    // 返回 pageId，找不到返回 0
    uint32_t findPage(uint8_t minPercent, uint32_t startPage = 1) const;

    // 获取当前记录的总页数
    uint32_t numPages() const { return numPages_; }

    // 将内存缓存刷盘
    void flush();

private:
    std::string filename_;
    mutable std::fstream f_;
    mutable std::vector<uint8_t> cache_;
    mutable bool dirty_ = false;
    mutable uint32_t numPages_ = 0;

    void ensureSize(uint32_t pageId);
    void loadFromDisk() const;
    void writeToDisk() const;
};

} // namespace dbms
