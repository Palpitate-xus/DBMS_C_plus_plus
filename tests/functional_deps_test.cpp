#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <cmath>
#include <filesystem>
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

static bool approx(double a, double b) { return std::fabs(a - b) < 1e-9; }

static void test_dependencies() {
    std::string db = "fdeps";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE addr (zip INT, city VARCHAR(20), note VARCHAR(20))", s));

    // zip fully determines city; note is unrelated within a zip group.
    auto ins = [&](const std::string& zip, const std::string& city, const std::string& note) {
        std::map<std::string, std::string> v{{"zip", zip}, {"city", city}, {"note", note}};
        assert(g_engine.insert(db, "addr", v) == dbms::DBStatus::OK);
    };
    ins("1", "A", "x");
    ins("1", "A", "y");
    ins("2", "B", "z");
    ins("2", "B", "w");

    auto deps = g_engine.computeFunctionalDependencies(db, "addr", {"zip", "city", "note"});
    // All ordered pairs present (3 cols -> 6 pairs).
    assert(deps.size() == 6);

    // zip -> city is a perfect functional dependency.
    assert(approx(deps["zip=>city"], 1.0));
    // city -> zip is also perfect here.
    assert(approx(deps["city=>zip"], 1.0));
    // zip -> note: each zip group has two distinct notes, best=1 per group ->
    // consistent = 2 of 4 rows -> 0.5.
    assert(approx(deps["zip=>note"], 0.5));

    cleanup(db);
    std::cout << "[FDEPS] degrees OK" << std::endl;
}

static void test_dependencies_guards() {
    std::string db = "fdeps_guard";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (a INT, b INT)", s));
    // Empty table -> no degrees.
    auto empty = g_engine.computeFunctionalDependencies(db, "t", {"a", "b"});
    assert(empty.empty());
    // Fewer than two columns -> no degrees.
    auto one = g_engine.computeFunctionalDependencies(db, "t", {"a"});
    assert(one.empty());
    // Unknown column -> no degrees.
    auto bad = g_engine.computeFunctionalDependencies(db, "t", {"a", "nope"});
    assert(bad.empty());

    cleanup(db);
    std::cout << "[FDEPS] guards OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_dependencies();
    test_dependencies_guards();
    std::cout << "[FDEPS] all passed" << std::endl;
    return 0;
}
