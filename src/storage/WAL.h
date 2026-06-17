#pragma once

#include "dbms_defs.h"
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// PostgreSQL-style WAL (Write-Ahead Log) manager
// ============================================================================
//
// LSN format: 64-bit byte offset in the logical WAL stream.
// Segment files are 16 MiB and named:
//   <walDir>/000000010000000000000000
//   <walDir>/000000010000000000000001
//   ...
//
// Each record has a fixed 24-byte header followed by payload data.
// Records are aligned to MAXALIGN (8) bytes.
//
// Record header (28 bytes):
//   uint64_t xl_prev     - LSN of previous record
//   uint32_t xl_tot_len  - total length including header
//   uint32_t xl_info     - (rmid << 8) | info flags
//   uint64_t xl_xid      - transaction id
//   uint32_t xl_crc      - CRC32C of the record
//
// Resource managers (rmid):
//   RM_HEAP_ID     - heap insert/update/delete/clean
//   RM_XACT_ID     - transaction commit/abort
//   RM_SMGR_ID     - storage manager create/drop/truncate
//   RM_CHECKPOINT  - checkpoint records

// Resource manager IDs
constexpr uint8_t RM_HEAP_ID     = 10;
constexpr uint8_t RM_XACT_ID     = 11;
constexpr uint8_t RM_SMGR_ID     = 12;
constexpr uint8_t RM_CHECKPOINT_ID = 13;

// XACT info flags
constexpr uint8_t XLOG_XACT_COMMIT  = 0x00;
constexpr uint8_t XLOG_XACT_ABORT   = 0x01;
constexpr uint8_t XLOG_XACT_PREPARE = 0x02;

// HEAP info flags
constexpr uint8_t XLOG_HEAP_INSERT     = 0x00;
constexpr uint8_t XLOG_HEAP_DELETE     = 0x01;
constexpr uint8_t XLOG_HEAP_UPDATE     = 0x02;
constexpr uint8_t XLOG_HEAP_HOT_UPDATE = 0x03;
constexpr uint8_t XLOG_HEAP_CLEAN      = 0x04;
constexpr uint8_t XLOG_HEAP_PAGE_BEFORE = 0x10; // full-page image before modification (undo)
constexpr uint8_t XLOG_HEAP_PAGE_AFTER  = 0x11; // full-page image after modification (redo)

// SMGR info flags
constexpr uint8_t XLOG_SMGR_CREATE     = 0x00;
constexpr uint8_t XLOG_SMGR_TRUNCATE   = 0x01;

// CHECKPOINT info flags
constexpr uint8_t XLOG_CHECKPOINT_SHUTDOWN = 0x00;
constexpr uint8_t XLOG_CHECKPOINT_ONLINE   = 0x01;

#pragma pack(push, 1)
struct XLogRecHeader {
    uint64_t xl_prev;     // LSN of previous record
    uint32_t xl_tot_len;  // total length (header + data), aligned
    uint32_t xl_info;     // (rmid << 8) | info
    uint64_t xl_xid;      // transaction id
    uint32_t xl_crc;      // CRC32C over the whole record
};
#pragma pack(pop)

static_assert(sizeof(XLogRecHeader) == 28, "XLogRecHeader size mismatch");

// A decoded WAL record
struct XLogRecord {
    XLogRecHeader header;
    std::vector<char> data;

    uint8_t rmid() const { return static_cast<uint8_t>(header.xl_info >> 8); }
    uint8_t info() const { return static_cast<uint8_t>(header.xl_info & 0xFF); }
};

// Block reference used by full-page writes and redo
struct XLogBlockRef {
    uint32_t rnodeSpcNode; // tablespace / database tag (simplified)
    uint32_t rnodeDbNode;
    uint32_t rnodeRelNode;
    uint32_t forkNum;
    uint32_t blockNum;
};

// ============================================================================
// WALManager
// ============================================================================
class WALManager {
public:
    explicit WALManager(const std::filesystem::path& walDir);
    ~WALManager();

    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;

    // Insert a WAL record. Returns the LSN of the new record.
    Lsn XLogInsert(uint8_t rmid, uint8_t info, uint64_t xid,
                   const std::vector<char>& data);

    // Ensure all records up to targetLsn are fsync'd to disk.
    void XLogFlush(Lsn targetLsn);

    // Read a record at the given LSN. Returns nullopt if not found/malformed.
    std::optional<XLogRecord> ReadRecord(Lsn lsn) const;

    // Read the next record after the given LSN (skips the record at lsn).
    // If lsn == 0, reads the first record.
    std::optional<XLogRecord> ReadNextRecord(Lsn lsn) const;

    // Get the LSN where the next record will be written.
    Lsn currentWriteLsn() const { return currentLsn_; }

    // Find the LSN of the latest checkpoint record, if any.
    std::optional<Lsn> findLastCheckpointLsn() const;

    // Read the checkpoint record at the given LSN.
    std::optional<XLogRecord> readCheckpoint(Lsn lsn) const;

    // Reset the WAL (delete all segment files). Used after checkpoint.
    void reset();

    // Ensure walDir exists.
    bool ensureOpen();

    // Validate that the WAL directory is usable.
    bool isOpen() const { return open_; }

private:
    std::filesystem::path walDir_;
    bool open_ = false;
    Lsn currentLsn_ = 0;

    static constexpr uint64_t kSegmentSize = 16 * 1024 * 1024; // 16 MiB

    uint32_t segmentNumber(Lsn lsn) const {
        return static_cast<uint32_t>(lsn / kSegmentSize);
    }
    uint32_t segmentOffset(Lsn lsn) const {
        return static_cast<uint32_t>(lsn % kSegmentSize);
    }
    std::filesystem::path segmentPath(uint32_t segNo) const;

    void advanceCurrentLsn(uint32_t len);

    uint32_t computeCrc(const char* data, size_t len) const;
    bool verifyCrc(const XLogRecord& rec) const;

    // Read raw bytes from WAL stream at [offset, offset+len).
    bool readBytes(uint64_t offset, char* buf, size_t len) const;

    // Ensure a segment file exists up to the given segment number.
    bool ensureSegment(uint32_t segNo);

    // Append bytes to the WAL stream at currentLsn_.
    bool appendBytes(const char* buf, size_t len);

    // fsync a segment file.
    bool syncSegment(uint32_t segNo);
};

// ============================================================================
// Redo dispatcher
// ============================================================================
using RedoFunc = std::function<bool(const XLogRecord& rec)>;

// Apply a single WAL record. Returns false on error.
// Callbacks receive the decoded table name as first argument.
bool RedoApplyRecord(const XLogRecord& rec,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t blockNum, uint32_t forkNum,
                                              const char* pageData, size_t pageLen)>& redoPageImage,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t blockNum, uint32_t forkNum,
                                              uint16_t slotId, const char* rowData, size_t rowLen)>& redoInsert,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t blockNum, uint32_t forkNum,
                                              uint16_t slotId, uint64_t xmax)>& redoDelete,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t oldBlock, uint32_t oldFork,
                                              uint16_t oldSlot,
                                              uint32_t newBlock, uint32_t newFork,
                                              uint16_t newSlot,
                                              const char* rowData, size_t rowLen)>& redoUpdate);

} // namespace dbms
