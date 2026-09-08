#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace dbms {

// ============================================================================
// Logical decoding (P2-5)
//
// Write paths buffer logical changes per transaction (table, op, old/new
// row image); at commit the buffer is decoded and streamed into every
// logical replication slot whose publication includes the table.  Rollback
// discards the buffer, so subscribers only ever see committed changes —
// the same contract PostgreSQL's decode-at-commit gives.
//
// Output plugins: "dbms_pgoutput_preview" emits a project-specific compact
// stream (it deliberately does not claim PostgreSQL's reserved pgoutput
// identity); "dbms_test_decoding" emits a readable debugging format.
// ============================================================================

struct LogicalChange {
    enum class Op { Insert, Update, Delete, Truncate };
    Op op = Op::Insert;
    std::string table;
    // Row images as bar-separated column values (the storage layer's
    // stripped row text format).
    std::string oldRow;
    std::string newRow;
    uint64_t xid = 0;
    uint64_t commitLsn = 0;
};

// Decoded stream handed to output plugins.
struct LogicalChangeBatch {
    uint64_t xid = 0;
    uint64_t commitLsn = 0;
    std::vector<LogicalChange> changes;
};

class LogicalDecoder {
public:
    // Render a committed batch with the named output plugin.
    // Returns false for unknown plugins.
    static bool format(const std::string& plugin, const LogicalChangeBatch& batch,
                       std::string& out);

    // Plugin registry (fixed set; PG loads shared objects here).
    static std::vector<std::string> availablePlugins();
};

// A publication: the set of tables whose changes stream to subscribers.
struct Publication {
    std::string name;
    std::string owner;
    bool publishInsert = true;
    bool publishUpdate = true;
    bool publishDelete = true;
    bool publishTruncate = true;
    bool publishAllTables = false;  // FOR ALL TABLES
    std::vector<std::string> tables;
};

// Catalog of publications per database, persisted as
//   <db>/<name>.publication   (one JSON-ish line per table/flag)
class PublicationCatalog {
public:
    static PublicationCatalog& instance();

    bool create(const std::string& dbname, const Publication& pub, std::string& error);
    bool update(const std::string& dbname, const Publication& pub,
                std::string& error);
    bool rename(const std::string& dbname, const std::string& oldName,
                const std::string& newName, std::string& error);
    bool drop(const std::string& dbname, const std::string& name, std::string& error);
    bool dropMany(const std::string& dbname,
                  const std::vector<std::string>& names, bool ifExists,
                  std::vector<std::string>& missing, std::string& error);
    bool exists(const std::string& dbname, const std::string& name) const;
    std::vector<Publication> list(const std::string& dbname) const;
    // Retarget explicit publication membership when a relation is renamed.
    // All affected publication files are updated atomically as a batch.
    bool renameTable(const std::string& dbname, const std::string& oldName,
                     const std::string& newName, std::string& error);
    // Remove explicit membership for a relation that is being dropped.
    bool removeTable(const std::string& dbname, const std::string& tableName,
                     std::string& error);
    // Does this publication stream changes of (dbname, table)?
    bool publishes(const std::string& dbname, const std::string& table) const;
    // Operation-aware membership check used by DML capture.  A table can be
    // a publication member while a specific change kind is disabled.
    bool publishes(const std::string& dbname, const std::string& table,
                   LogicalChange::Op operation) const;

private:
    PublicationCatalog() = default;
    mutable std::mutex mutex_;
};

// In-memory change stream per logical slot (bounded; subscribers drain via
// peek/acknowledge which advances the slot's confirmed LSN).
class LogicalChangeStore {
public:
    static LogicalChangeStore& instance();

    // Append a committed batch for a slot (called under the slot's lock).
    void append(const std::string& slotName, const LogicalChangeBatch& batch);

    // Read up to maxChanges changes from fromLsn (exclusive).  Returns the
    // next LSN to resume from.
    struct PeekResult {
        std::vector<LogicalChangeBatch> batches;
        uint64_t nextLsn = 0;
        bool hitEnd = true;
    };
    PeekResult peek(const std::string& slotName, uint64_t fromLsn,
                    size_t maxChanges) const;

    // Drop everything up to (and including) confirmedLsn.
    void acknowledge(const std::string& slotName, uint64_t confirmedLsn);

    // Remove every retained batch when the owning slot is dropped.  A later
    // slot reusing the name must not inherit the previous slot's stream.
    void discard(const std::string& slotName);

    // Bounded retention: at most kMaxRetained per slot (oldest dropped).
    static constexpr size_t kMaxRetained = 4096;

    size_t depth(const std::string& slotName) const;

private:
    LogicalChangeStore() = default;
    mutable std::mutex mutex_;
    struct Entry {
        uint64_t startLsn;  // LSN of the first change (batch key)
        uint64_t endLsn;
        LogicalChangeBatch batch;
    };
    std::map<std::string, std::deque<Entry>> streams_;
};

}  // namespace dbms
