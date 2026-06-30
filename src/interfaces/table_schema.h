#pragma once

#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

namespace dbms {

constexpr size_t MAX_COLUMNS = 30;
constexpr size_t MAX_TABLE_NAME_LEN = 15;
constexpr size_t MAX_TYPE_NAME_LEN = 12;
constexpr size_t MAX_COL_NAME_LEN = 15;
constexpr size_t DATE_SIZE = 12;
constexpr size_t TIMESTAMP_SIZE = 8;
constexpr int64_t INF = 0x8000000000000000LL;

struct Column {
    bool isNull = false;
    bool isPrimaryKey = false;
    bool isVariableLength = false;  // true for VARCHAR, false for fixed-length types
    bool isUnique = false;          // UNIQUE constraint
    bool isAutoIncrement = false;   // AUTO_INCREMENT / SERIAL
    bool isUnsigned = false;        // UNSIGNED for numeric types
    bool isArray = false;           // true for array types (INT[], VARCHAR[])
    std::string dataType;
    std::string dataName;
    size_t dsize = 0;  // For VARCHAR: max length; for fixed: actual bytes
    std::string defaultValue;       // DEFAULT value
    std::string checkExpr;          // CHECK constraint expression
    std::string checkConstraintName; // Name of the CHECK constraint
    bool deferrable = false;        // CHECK constraint is deferrable
    bool initiallyDeferred = false; // Deferrable constraint initially deferred
    std::string generatedExpr;      // GENERATED ALWAYS AS (expr)
    char generatedKind = 0;         // 0=none, 's'=STORED, 'v'=VIRTUAL
    std::string collation;          // COLLATE (e.g. "C", "POSIX", "en_US.utf8")
    std::vector<std::string> enumValues;  // ENUM('a','b','c') values
    std::string domainName;         // If type is a DOMAIN, the domain name

    void print() const;
};

struct SequenceInfo {
    int64_t start = 1;
    int64_t increment = 1;
    int64_t minValue = 1;
    int64_t maxValue = std::numeric_limits<int64_t>::max();
    int64_t cache = 1;
    bool cycle = false;
    bool hasMinValue = false;   // user explicitly wrote MINVALUE
    bool hasMaxValue = false;   // user explicitly wrote MAXVALUE
    bool noMinValue = false;    // user wrote NO MINVALUE
    bool noMaxValue = false;    // user wrote NO MAXVALUE
    std::string ownedByTable;   // OWNED BY table.column
    std::string ownedByColumn;

    // Explicit-set flags (used by ALTER SEQUENCE to know what changed).
    bool startSpecified = false;
    bool incrementSpecified = false;
    bool cacheSpecified = false;
    bool cycleSpecified = false;  // true when CYCLE or NO CYCLE was written
    bool ownedBySpecified = false; // true when OWNED BY was written

    // Compute PG-compatible defaults based on increment direction.
    void applyDefaults() {
        if (increment > 0) {
            if (!hasMinValue && !noMinValue) minValue = 1;
            if (!hasMaxValue && !noMaxValue) maxValue = std::numeric_limits<int64_t>::max();
        } else if (increment < 0) {
            if (!hasMaxValue && !noMaxValue) maxValue = -1;
            if (!hasMinValue && !noMinValue) minValue = -std::numeric_limits<int64_t>::max();
        }
    }
};

struct ForeignKey {
    std::vector<std::string> colNames;     // local columns (composite FK)
    std::vector<std::string> refCols;      // referenced columns
    std::string refTable;                  // referenced table
    std::string onDelete = "restrict";     // restrict | cascade | setnull
    std::string onUpdate = "restrict";     // restrict | cascade | setnull
    std::string name;                      // constraint name

    // Back-compat helpers
    bool isSingleColumn() const { return colNames.size() <= 1; }
    std::string singleColName() const { return colNames.empty() ? "" : colNames[0]; }
    std::string singleRefCol() const { return refCols.empty() ? "" : refCols[0]; }
};

struct TableSchema {
    std::string tablename;
    Column cols[MAX_COLUMNS];
    size_t len = 0;
    ForeignKey fks[MAX_COLUMNS];
    size_t fkLen = 0;

    // Composite primary key column indices (empty = use single-column isPrimaryKey)
    std::vector<size_t> pkColIndices;
    // Composite UNIQUE constraints: each inner vector is column indices
    std::vector<std::vector<size_t>> uniqueConstraints;
    std::vector<std::string> uniqueConstraintNames; // names parallel to uniqueConstraints

    // Partitioning
    enum class PartitionType { None, Range, List, Hash };
    PartitionType partitionType = PartitionType::None;
    std::string partitionKey;  // column name for partitioning
    std::vector<std::pair<std::string, std::string>> rangePartitions;  // name -> upper bound
    std::vector<std::pair<std::string, std::vector<std::string>>> listPartitions;  // name -> values
    size_t hashPartitions = 0;  // number of hash partitions
    std::string defaultPartitionName; // DEFAULT partition for LIST partitioning

    // Subpartitioning (two-level partitioning)
    PartitionType subPartitionType = PartitionType::None;
    std::string subPartitionKey;  // column name for sub-partitioning
    size_t subHashPartitions = 0; // number of hash sub-partitions

    bool isUnlogged = false;    // UNLOGGED table: no WAL, truncated on crash
    bool rowLevelSecurity = false; // ENABLE ROW LEVEL SECURITY
    bool forceRowLevelSecurity = false; // FORCE ROW LEVEL SECURITY (applies to table owner too)
    std::map<std::string, std::string> storageParams; // WITH (fillfactor=70, autovacuum_enabled=off)

    // Storage format version: 0 = legacy 4KB, 1 = 4KB with MVCC, 2 = PostgreSQL 8KB
    uint32_t formatVersion = 0;

    // Tablespace: physical location for data files (default = "pg_default")
    std::string tablespace = "pg_default";

    void append(const Column& ncol);
    void appendFK(const ForeignKey& fk);
    void print() const;
    size_t rowSize() const;

    // PK helpers
    bool hasPrimaryKey() const;
    std::string buildPKValue(const std::string& rowBuffer) const;
    std::string buildPKValue(const std::map<std::string, std::string>& values) const;

    // Variable-length helpers
    bool hasVariableLength() const;
    size_t fixedDataSize() const;       // sum of dsize of all fixed-length columns
    size_t varColCount() const;         // number of variable-length columns
    size_t getVarColIndex(size_t colIdx) const;  // index among var columns
    size_t getFixedColOffset(size_t colIdx) const; // offset within fixed data region
};

} // namespace dbms
