#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("schema_write_atomicity");
    fs::remove_all(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", true, 2));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    const fs::path schemaPath = fs::path(db) / "items.stc";
    const fs::path savedPath = fs::path(db) / "items.stc.saved";
    const std::string originalBytes = readFile(schemaPath);
    assert(!originalBytes.empty());

    // Prime the parsed-schema cache, then substitute a directory with the
    // exact same timestamp. This keeps the table readable for the operation
    // while forcing the final atomic rename to fail on every user account,
    // including root.
    const auto originalSchema = g_engine.getTableSchema(db, "items");
    assert(originalSchema.len == 1);
    const auto originalTimestamp = fs::last_write_time(schemaPath);
    fs::rename(schemaPath, savedPath);
    assert(fs::create_directory(schemaPath));
    fs::last_write_time(schemaPath, originalTimestamp);

    const dbms::DBStatus failed = g_engine.alterTableSetDefault(
        db, "items", "id", "42");
    assert(failed == dbms::DBStatus::IO_ERROR);
    assert(fs::is_directory(schemaPath));

    fs::remove(schemaPath);
    fs::rename(savedPath, schemaPath);
    assert(readFile(schemaPath) == originalBytes);

    dbms::StorageEngine reloaded;
    const auto afterFailure = reloaded.getTableSchema(db, "items");
    assert(afterFailure.len == 1);
    assert(afterFailure.cols[0].defaultValue.empty());

    assert(g_engine.alterTableSetDefault(
               db, "items", "id", "42") == dbms::DBStatus::OK);
    dbms::StorageEngine persisted;
    const auto afterSuccess = persisted.getTableSchema(db, "items");
    assert(afterSuccess.len == 1);
    assert(afterSuccess.cols[0].defaultValue == "42");

    g_engine.catalogService().evict(db);
    fs::remove_all(db);
    std::cout << "[SCHEMA WRITE] atomic failure propagation OK" << std::endl;
    return 0;
}
