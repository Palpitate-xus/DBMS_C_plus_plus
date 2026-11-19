#pragma once

#include "storage/WAL.h"

namespace dbms {

// Read at lsn, advancing it to the next record if it points at zero padding
// written by SWITCH WAL. An empty result on success means end of stream.
// A malformed record, nonzero padding or missing segment is an error.
bool readWalRecord(const WALManager& wal, Lsn& lsn,
                   std::optional<XLogRecord>& record);

enum class SnapshotWalCoverage { Retained, Archived, Unverified };

// Verify that the saved WAL is a prefix of this timeline. If part of the
// interval after the snapshot was recycled, return its transaction records
// newest first. The caller must make a terminal decision before using it.
SnapshotWalCoverage readSnapshotTransactionWal(
    const WALManager& wal, const std::filesystem::path& snapshotWal,
    const std::filesystem::path& archive, uint64_t xid,
    std::vector<XLogRecord>& records);

}  // namespace dbms
