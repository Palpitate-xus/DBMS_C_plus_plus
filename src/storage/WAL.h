#pragma once

#include "dbms_defs.h"
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
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
// where the leading 8 hex digits are the timeline ID (TLI).
//
// Each record has a fixed 28-byte header followed by payload data.
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
constexpr uint8_t RM_CATALOG_ID    = 14; // DDL catalog changes
constexpr uint8_t RM_INDEX_ID      = 15; // B-tree/Hash full-file images

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

// CATALOG info flags
constexpr uint8_t XLOG_CATALOG_CREATE = 0x00;
constexpr uint8_t XLOG_CATALOG_DROP   = 0x01;
constexpr uint8_t XLOG_CATALOG_UPDATE = 0x02;

// INDEX info flags. The payload stores a path and the complete durable file
// image. Full-file images are intentionally limited to the B-tree/Hash access
// methods until page-level WAL exists for the remaining access methods.
constexpr uint8_t XLOG_INDEX_FILE_BEFORE = 0x10;
constexpr uint8_t XLOG_INDEX_FILE_AFTER  = 0x11;

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
    // Segment size is part of the on-disk WAL format.
    static constexpr uint64_t kSegmentSize = 16 * 1024 * 1024; // 16 MiB
    // A single record may span segments, but it must remain bounded so a
    // damaged length field cannot force unbounded recovery allocation.
    static constexpr uint32_t kMaxRecordPayload = 256 * 1024 * 1024;

    explicit WALManager(const std::filesystem::path& walDir);
    ~WALManager();

    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;

    // Insert a WAL record. Returns the LSN of the new record.
    Lsn XLogInsert(uint8_t rmid, uint8_t info, uint64_t xid,
                   const std::vector<char>& data);

    // Ensure all records up to targetLsn are fsync'd to disk.
    // Returns false when the WAL stream or any segment cannot be synced.
    bool XLogFlush(Lsn targetLsn);

    // Read a record at the given LSN. Returns nullopt if not found/malformed.
    std::optional<XLogRecord> ReadRecord(Lsn lsn) const;

    // Read the next record after the given LSN (skips the record at lsn).
    // If lsn == 0, reads the first record.
    std::optional<XLogRecord> ReadNextRecord(Lsn lsn) const;

    // Get the LSN where the next record will be written.
    Lsn currentWriteLsn() const { return currentLsn_; }

    // Timeline ID (TLI). Default is 1. Used in segment file names.
    uint32_t timelineId() const { return timelineId_; }
    bool setTimeline(uint32_t tli);

    // Archive status helpers. PostgreSQL keeps .ready / .done files in
    // pg_wal/archive_status/ to track which segments need archiving / have been
    // archived. These methods mimic that behaviour.
    bool markSegmentReadyForArchive(uint32_t segNo);
    bool markSegmentArchived(uint32_t segNo);
    bool isSegmentArchived(uint32_t segNo) const;
    std::vector<uint32_t> pendingArchiveSegments() const;
    bool archiveSegment(uint32_t segNo, const std::filesystem::path& archiveDir);
    bool archivePendingSegments(const std::filesystem::path& archiveDir);

    // Mark all complete segments strictly before lsn as ready for archiving.
    bool markSegmentsReadyBefore(Lsn lsn);

    // Remove complete segments strictly before lsn after they have been
    // archived. The segment containing lsn is always retained.
    bool truncateBefore(Lsn lsn);

    // Return the first segment still present on the current timeline. A new
    // WAL directory, or an empty directory, starts at LSN 0.
    Lsn earliestAvailableLsn() const;

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

    // Path to a specific segment file (useful for tests / archiving).
    std::filesystem::path segmentPath(uint32_t segNo) const;

private:
    std::filesystem::path walDir_;
    bool open_ = false;
    Lsn currentLsn_ = 0;
    uint32_t timelineId_ = 1;

    // ---- Incremental append state ---------------------------------------
    // Maintained in memory under dirMutex_/the WAL file lock so a normal
    // XLogInsert/XLogFlush does not rescan the segment directory or re-walk
    // the record chain. refreshCurrentLsnFromDisk()/validateRecordsOnDisk()
    // still rebuild this state at open and whenever another process is
    // detected appending to the same directory (validateAppendOffset).
    Lsn lastRecordLsn_ = 0;         // LSN of the last inserted record
    bool hasLastRecord_ = false;    // false = no record yet (LSN 0 is valid)
    uint32_t lastRecordTotLen_ = 0; // xl_tot_len of that record
    Lsn lastSyncedLsn_ = 0;         // bytes [0, lastSyncedLsn_) are fsync'd
    mutable uint32_t earliestSeg_ = 0; // first segment present on this timeline
    mutable bool earliestSegValid_ = false;
    // Long-lived descriptors: the cross-process wal.lock fd and the write fd
    // of the segment currently being appended to. Reopened only on segment
    // switch instead of per record.
    mutable int lockFd_ = -1;
    int writeFd_ = -1;
    uint32_t writeFdSeg_ = 0;
    bool writeFdValid_ = false;

    // In-process mutex shared by every WALManager instance that targets the
    // same WAL directory (several embedded StorageEngines may coexist). It
    // replaces the former process-global mutex so different databases no
    // longer serialize each other.
    std::shared_ptr<std::mutex> dirMutex_;

    uint32_t segmentNumber(Lsn lsn) const {
        return static_cast<uint32_t>(lsn / kSegmentSize);
    }
    uint32_t segmentOffset(Lsn lsn) const {
        return static_cast<uint32_t>(lsn % kSegmentSize);
    }

    std::filesystem::path timelinePath() const { return walDir_ / "timeline"; }
    std::filesystem::path lockPath() const { return walDir_ / "wal.lock"; }
    bool loadTimeline();
    bool persistTimeline();
    bool refreshCurrentLsnFromDisk();
    bool validateRecordsOnDisk() const;
    std::optional<Lsn> lastRecordLsn() const;

    // Verify the on-disk append offset still matches currentLsn_ with a
    // single stat (called under the WAL file lock). When another process
    // appended in between, the caller rebuilds state from disk.
    bool validateAppendOffset();
    // Walk the retained chain once, filling lastRecordLsn_/lastRecordTotLen_.
    bool scanWalTail();

    int acquireWalFileLock() const;
    static void releaseWalFileLock(int fd);

    std::filesystem::path archiveStatusDir() const { return walDir_ / "archive_status"; }
    std::filesystem::path readyPath(uint32_t segNo) const;
    std::filesystem::path donePath(uint32_t segNo) const;

    bool markSegmentReadyForArchiveLocked(uint32_t segNo);
    bool markSegmentArchivedLocked(uint32_t segNo);
    bool archiveSegmentLocked(uint32_t segNo,
                              const std::filesystem::path& archiveDir);

    void advanceCurrentLsn(uint32_t len);

    // Ensure writeFd_ targets the given segment, opening/switching as needed.
    int segmentWriteFd(uint32_t segNo);
    bool closeWriteFd();

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
