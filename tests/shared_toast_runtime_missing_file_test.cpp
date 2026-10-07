#include "commands/TableManage.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    const std::string db = "__t_shared_toast_missing_file";
    StorageEngine first, second;
    assert(first.createDatabase(db) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 4, false));
    table.append(makeVarCharColumn("payload", false, 12000));
    assert(first.createTable(db, table) == DBStatus::OK);
    const std::string value(10000, 'x');
    assert(first.insert(db, "items", {{"id", "1"}, {"payload", value}}) == DBStatus::OK);
    const auto before = second.query(db, "items", {}, {"payload"});
    assert(before.size() == 1 && before.front() == value + " ");
    const auto path = std::filesystem::path(db) / "items.toast.dt";
    const auto saved = std::filesystem::path(db) / "items.toast.dt.saved";
    std::filesystem::rename(path, saved);
    (void)second.query(db, "items", {}, {"payload"});
    const bool recreated = std::filesystem::exists(path);
    std::cerr << "[MISSING TOAST] recreated=" << recreated << '\n';
    assert(!recreated);
    std::filesystem::rename(saved, path);
    const auto restored = second.query(db, "items", {}, {"payload"});
    assert(restored.size() == 1 && restored.front() == value + " ");
    std::cout << "[MISSING TOAST] no replacement is invented and original payload remains recoverable\n";
}
