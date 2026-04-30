#pragma once

#include <cstdint>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "DateType.h"

namespace dbms {

constexpr size_t MAX_COLUMNS = 30;
constexpr size_t MAX_TABLE_NAME_LEN = 15;
constexpr size_t MAX_TYPE_NAME_LEN = 4;
constexpr size_t MAX_COL_NAME_LEN = 15;
constexpr size_t DATE_SIZE = 12;
constexpr int64_t INF = 0x8000000000000000LL;

struct Column {
    bool isNull = false;
    bool isPrimaryKey = false;
    std::string dataType;
    std::string dataName;
    size_t dsize = 0;

    void print() const;
};

struct TableSchema {
    std::string tablename;
    Column cols[MAX_COLUMNS];
    size_t len = 0;

    void append(const Column& ncol);
    void print() const;
    size_t rowSize() const;
};

// Result code for data operations
enum class OpResult {
    Success = 0,
    TableNotExist,
    DatabaseNotExist,
    TableAlreadyExist,
    InvalidValue,
    NullNotAllowed,
    SyntaxError,
    DuplicateKey,
};

class StorageEngine {
public:
    StorageEngine();

    // Database operations
    OpResult createDatabase(const std::string& dbname);
    OpResult dropDatabase(const std::string& dbname);
    bool databaseExists(const std::string& dbname) const;

    // Table operations
    OpResult createTable(const std::string& dbname, const TableSchema& tbl);
    OpResult createTable(const std::string& dbname, const std::string& tablename, const TableSchema& tbl);
    OpResult dropTable(const std::string& dbname, const std::string& tablename);
    OpResult alterTableAddColumn(const std::string& dbname, const std::string& tablename,
                                  const Column& col);
    OpResult alterTableDropColumn(const std::string& dbname, const std::string& tablename,
                                   const std::string& colName);
    bool tableExists(const std::string& dbname, const std::string& tablename) const;
    std::vector<std::string> getTableNames(const std::string& dbname) const;
    TableSchema getTableSchema(const std::string& dbname, const std::string& tablename) const;

    // Data operations
    OpResult insert(const std::string& dbname, const std::string& tablename,
                    const std::map<std::string, std::string>& values);
    OpResult update(const std::string& dbname, const std::string& tablename,
                    const std::map<std::string, std::string>& updates,
                    const std::vector<std::string>& conditions);
    OpResult remove(const std::string& dbname, const std::string& tablename,
                    const std::vector<std::string>& conditions);
    std::vector<std::string> query(const std::string& dbname, const std::string& tablename,
                                   const std::vector<std::string>& conditions,
                                   const std::set<std::string>& selectCols,
                                   const std::string& orderByCol = "",
                                   bool orderByAsc = true);

    // Aggregate query: items = {("count","*"), ("max","score"), ...}
    std::vector<std::string> aggregate(const std::string& dbname, const std::string& tablename,
                                       const std::vector<std::string>& conditions,
                                       const std::vector<std::pair<std::string, std::string>>& items);

private:
    std::filesystem::path dbPath(const std::string& dbname) const;
    std::filesystem::path schemaPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path dataPath(const std::string& dbname, const std::string& tablename) const;
    std::filesystem::path tableListPath(const std::string& dbname) const;

    void writeSchema(std::ostream& out, const TableSchema& tbl);
    TableSchema readSchema(std::istream& in, const std::string& tablename) const;

    // Primary key index: "dbname/tablename" -> "pkValue" -> rowIdx
    std::map<std::string, std::map<std::string, int64_t>> pkIndexes_;
    std::string pkIndexKey(const std::string& dbname, const std::string& tablename) const {
        return dbname + "/" + tablename;
    }
    void updatePKIndexOnInsert(const std::string& dbname, const std::string& tablename,
                                int64_t rowIdx, const std::string& rowBuffer);
    std::set<int64_t> lookupPKIndex(const std::string& dbname, const std::string& tablename,
                                     const TableSchema& tbl, const std::string& colName,
                                     const std::string& value);

    // Parse condition strings like "<col value", "=col value", ">col value"
    struct Condition {
        std::string op;  // "<", ">", "=", "<=", ">=", "!="
        std::string colName;
        std::string value;
    };
    static std::vector<Condition> parseConditions(const std::vector<std::string>& cstr);

    // Evaluate a single row against conditions, returning matching row indices
    std::set<int64_t> filterRows(const std::string& dbname, const std::string& tablename,
                                 const std::vector<Condition>& conds);

    // Indexing
    void rebuildPKIndex(const std::string& dbname, const std::string& tablename);

    // Helpers
    static int64_t parseInt(const std::string& s);
    static bool stringToBuffer(const std::string& src, char* dst, size_t len);
};

// Column type constructors
Column makeIntColumn(const std::string& name, bool isNull, int scale, bool isPK = false);
Column makeStringColumn(const std::string& name, bool isNull, size_t length, bool isPK = false);
Column makeDateColumn(const std::string& name, bool isNull, bool isPK = false);

} // namespace dbms
