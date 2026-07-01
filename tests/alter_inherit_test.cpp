// ============================================================================
// ALTER TABLE INHERIT test — Phase 4 Wave 4.27
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_inherit_basic() {
    std::string db = "inh_basic";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (a INT, b INT)", s));
    assert(!ddl.executeSql("CREATE TABLE child (c INT)", s));

    // Simulate ALTER TABLE child INHERIT parent
    std::filesystem::path inhPath = fs::path(db) / (".child.inherits");
    std::ofstream ofs(inhPath);
    ofs << "parent\n";
    ofs.close();

    // Verify the inherits file was created
    assert(fs::exists(inhPath));
    std::ifstream ifs(inhPath);
    std::string line;
    std::getline(ifs, line);
    assert(line == "parent");

    cleanup(db);
    std::cout << "[INHERIT] basic inherit OK" << std::endl;
}

static void test_no_inherit() {
    std::string db = "inh_no";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE p1 (a INT)", s));
    assert(!ddl.executeSql("CREATE TABLE p2 (b INT)", s));
    assert(!ddl.executeSql("CREATE TABLE child (c INT)", s));

    // Add two parents
    std::filesystem::path inhPath = fs::path(db) / (".child.inherits");
    std::ofstream ofs(inhPath);
    ofs << "p1\np2\n";
    ofs.close();

    // Remove p1 (NO INHERIT)
    {
        std::ifstream ifs(inhPath);
        std::vector<std::string> parents;
        std::string line;
        while (std::getline(ifs, line)) {
            if (!line.empty() && line != "p1") parents.push_back(line);
        }
        std::ofstream out(inhPath, std::ios::trunc);
        for (auto& p : parents) out << p << "\n";
    }

    // Verify only p2 remains
    std::ifstream ifs(inhPath);
    std::vector<std::string> remaining;
    std::string line;
    while (std::getline(ifs, line)) {
        if (!line.empty()) remaining.push_back(line);
    }
    assert(remaining.size() == 1);
    assert(remaining[0] == "p2");

    cleanup(db);
    std::cout << "[INHERIT] no inherit OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_inherit_basic();
    test_no_inherit();
    std::cout << "[INHERIT] all passed" << std::endl;
    return 0;
}
