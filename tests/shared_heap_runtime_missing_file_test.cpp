#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "storage/PageAllocator.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main() {
    using namespace dbms;
    const std::string database = "__t_shared_heap_missing_file";
    StorageEngine first, second;
    assert(first.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 2, false));
    assert(first.createTable(database, table) == DBStatus::OK);
    assert(first.insert(database, "items", {{"id", "1"}}) == DBStatus::OK);
    auto* allocator = first.getPageAllocator(database, "items");
    assert(allocator && allocator->flush());
    assert(second.getPageAllocator(database, "items"));
    const auto path = std::filesystem::path(database) / "items.dt";
    const auto saved = std::filesystem::path(database) / "items.dt.saved";
    std::filesystem::rename(path, saved);
    const auto unexpected = second.getPageAllocator(database, "items");
    std::cerr << "[MISSING HEAP] allocator=" << (unexpected != nullptr)
              << " recreated=" << std::filesystem::exists(path) << '\n';
    assert(unexpected == nullptr);
    assert(!std::filesystem::exists(path));
    std::filesystem::rename(saved, path);
    const auto rows = second.query(database, "items", {}, {"id"});
    assert(rows.size() == 1 && rows.front() == "1 ");
    std::cout << "[MISSING HEAP] fail-closed getter and restored original rows passed\n";
}
