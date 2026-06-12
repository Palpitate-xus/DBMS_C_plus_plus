#pragma once

#include "dbms_defs.h"
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbms {

// ============================================================================
// PostgreSQL 风格 CLOG（Commit Log）
// ============================================================================
//
// 每个事务 ID 占用 2 bits，记录事务状态：
//   00 = IN_PROGRESS
//   01 = COMMITTED
//   10 = ABORTED
//   11 = SUB_COMMITTED（用于子事务，当前简化处理）
//
// 文件布局（与 PG 兼容）：
//   <dataDir>/pg_xact/0000    // 覆盖 xid [0, kXidsPerSegment)
//   <dataDir>/pg_xact/0001    // 覆盖 xid [kXidsPerSegment, 2*kXidsPerSegment)
//   ...
//
// 每段文件大小 = kXidsPerSegment / 4 bytes
//
// 使用方式：
//   CommitLog clog(dataDir);
//   clog.setStatus(xid, CommitLog::Status::Committed);
//   auto s = clog.getStatus(xid);
//
// 注意：
//   - 调用方负责在事务提交/回滚时调用 setStatus
//   - 调用方定期进行 truncate 以回收旧段文件
// ============================================================================

class CommitLog {
public:
    enum class Status : uint8_t {
        InProgress   = 0,
        Committed    = 1,
        Aborted      = 2,
        SubCommitted = 3
    };

    // 每段覆盖的事务数（必须是 4 的倍数）
    static constexpr uint64_t kXidsPerSegment  = 1024 * 1024; // 1M xids
    static constexpr uint64_t kSegmentFileSize = kXidsPerSegment / 4; // 256KB

    explicit CommitLog(const std::string& dataDir);
    ~CommitLog();

    // 禁止拷贝
    CommitLog(const CommitLog&) = delete;
    CommitLog& operator=(const CommitLog&) = delete;

    // 获取事务状态（未找到段文件时返回 InProgress）
    Status getStatus(TxnId xid) const;

    // 设置单个事务状态
    void setStatus(TxnId xid, Status status);

    // 批量设置并持久化
    void setStatuses(const std::vector<std::pair<TxnId, Status>>& entries);

    // 删除所有 <= oldestXid 的段文件（VACUUM / checkpoint 后调用）
    void truncate(TxnId oldestXid);

    // 将所有脏段刷盘
    void flush();

    // 解析状态为可读字符串
    static const char* statusName(Status s);

private:
    std::string dataDir_;
    mutable std::mutex mutex_;

    struct Segment {
        std::vector<uint8_t> data;
        bool dirty = false;
    };
    mutable std::unordered_map<uint64_t, Segment> segments_;

    std::string segmentPath(uint64_t segNo) const;
    void loadSegment(uint64_t segNo) const;
    void saveSegment(uint64_t segNo);
    void ensureSegment(uint64_t segNo) const;

    static uint64_t segmentNumber(TxnId xid);
    static size_t byteOffset(TxnId xid);
    static uint8_t shiftForXid(TxnId xid);
    static uint8_t statusBits(Status s);
    static Status bitsToStatus(uint8_t bits);
};

} // namespace dbms
