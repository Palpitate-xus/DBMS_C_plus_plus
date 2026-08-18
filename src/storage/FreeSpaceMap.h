#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
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
//
// 线程安全：内部互斥锁保护内存缓存与磁盘文件。调用方不再需要为并发
// 的 get/set/find/flush 串行化（bgwriter 与前台 DML 可同时访问）。

class FreeSpaceMap {
public:
    explicit FreeSpaceMap(const std::string& filename);
    ~FreeSpaceMap();

    bool open();
    void close();
    bool isOpen() const;

    // 获取指定页的空闲空间百分比（0~100），未知返回 255
    uint8_t getFreePercent(uint32_t pageId) const;

    // 设置指定页的空闲空间百分比
    void setFreePercent(uint32_t pageId, uint8_t percent);

    // 查找第一个空闲空间 >= minPercent 的页面（从 startPage 开始）
    // 返回 pageId，找不到返回 0
    uint32_t findPage(uint8_t minPercent, uint32_t startPage = 1) const;

    // 获取当前记录的总页数
    uint32_t numPages() const;

    // 将内存缓存刷盘
    void flush();

private:
    std::string filename_;
    mutable std::fstream f_;
    mutable std::vector<uint8_t> cache_;
    mutable bool dirty_ = false;
    mutable uint32_t numPages_ = 0;
    mutable std::mutex mutex_;

    void ensureSizeLocked(uint32_t pageId);
    void loadFromDiskLocked() const;
    void writeToDiskLocked() const;
};

} // namespace dbms
