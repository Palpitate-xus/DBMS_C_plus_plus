// ============================================================================
// Geometric type test — Phase 4 Wave 4.7
// line / lseg / box / path / polygon / circle: string-backed canonical text
// storage with structural validation (coordinate count, char set, brackets)
// and canonicalization (whitespace, box corner ordering, open/closed path).
// `point` keeps its packed-binary representation and is exercised separately.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "common/GeometryValue.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

static void test_line_lseg_circle() {
    std::string db = testDbPath("geo_a");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, ln LINE, ls LSEG, c CIRCLE)", s));

    // Whitespace normalized; canonical brackets emitted.
    assert(g_engine.insert(db, "g", {{"id","1"}, {"ln","{1, 2, 3}"}, {"ls","[ (0,0), (1,1) ]"}, {"c","<(0,0), 5>"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "ln") == "{1,2,3}");
    assert(fetchOne(db, "g", {"=id 1"}, "ls") == "[(0,0),(1,1)]");
    assert(fetchOne(db, "g", {"=id 1"}, "c") == "<(0,0),5>");

    // line with A=B=0 invalid; circle negative radius invalid; wrong count invalid.
    assert(g_engine.insert(db, "g", {{"id","2"}, {"ln","{0,0,3}"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","3"}, {"ln","{1,2,3}"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),-5>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","4"}, {"ln","{1,2,3}"}, {"ls","[(0,0),(1,1),(2,2)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Garbage characters rejected.
    assert(g_engine.insert(db, "g", {{"id","5"}, {"ln","{1,2,x}"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    // Coordinate counts alone are insufficient: delimiter kind, balance and
    // nesting are part of PostgreSQL's geometric input grammar.
    assert(g_engine.insert(db, "g", {{"id","6"}, {"ln","{1,2,3)"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","7"}, {"ln","{1,2,3}"}, {"ls","{(0,0),(1,1)}"}, {"c","<(0,0),5>"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","8"}, {"ln","[(0,0),(2,2)]"}, {"ls","[(0,0),(1,1)]"}, {"c","<(0,0),NaN>"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 8"}, "ln") == "{1,-1,0}");
    assert(fetchOne(db, "g", {"=id 8"}, "c") == "<(0,0),NaN>");

    auto rows = g_engine.query(db, "g", {}, {"id"}, {});
    assert(rows.size() == 2);
    cleanup(db);
    std::cout << "[GEO] line/lseg/circle OK" << std::endl;
}

static void test_box_corner_ordering() {
    std::string db = testDbPath("geo_box");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, b BOX)", s));

    // Corners reorder to (high-right),(low-left) regardless of input order.
    assert(g_engine.insert(db, "g", {{"id","1"}, {"b","(0,0),(2,3)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "b") == "(2,3),(0,0)");
    assert(g_engine.insert(db, "g", {{"id","2"}, {"b","(2,3),(0,0)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 2"}, "b") == "(2,3),(0,0)");

    cleanup(db);
    std::cout << "[GEO] box corner ordering OK" << std::endl;
}

static void test_path_polygon() {
    std::string db = testDbPath("geo_path");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, po PATH, pc PATH, pg POLYGON)", s));

    // Open path preserves '[...]'; closed path and polygon use '(...)'.
    assert(g_engine.insert(db, "g", {{"id","1"},
        {"po","[(0,0),(1,1),(2,0)]"},
        {"pc","((0,0),(1,1),(2,0))"},
        {"pg","((0,0),(4,0),(2,3))"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "po") == "[(0,0),(1,1),(2,0)]");
    assert(fetchOne(db, "g", {"=id 1"}, "pc") == "((0,0),(1,1),(2,0))");
    assert(fetchOne(db, "g", {"=id 1"}, "pg") == "((0,0),(4,0),(2,3))");

    // Odd number of coordinates rejected.
    assert(g_engine.insert(db, "g", {{"id","2"},
        {"po","[(0,0),(1,1),(2)]"},
        {"pc","((0,0),(1,1))"},
        {"pg","((0,0),(1,1),(2,2))"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","3"},
        {"po","[(0,0),(1,1))"},
        {"pc","((0,0),(1,1))"},
        {"pg","((0,0),(1,1),(2,2))"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "g", {{"id","4"},
        {"po","[(0,0),(1,1)]"},
        {"pc","((0,0),(1,1))"},
        {"pg","[(0,0),(1,1),(2,2)]"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[GEO] path/polygon open/closed OK" << std::endl;
}

static void test_binary_and_catalog_contract() {
    using dbms::decodeGeometryBinary;
    using dbms::encodeGeometryBinary;
    using dbms::mapBuiltinTypeNameToOid;

    assert(mapBuiltinTypeNameToOid("point") == 600);
    assert(mapBuiltinTypeNameToOid("lseg") == 601);
    assert(mapBuiltinTypeNameToOid("path") == 602);
    assert(mapBuiltinTypeNameToOid("box") == 603);
    assert(mapBuiltinTypeNameToOid("polygon") == 604);
    assert(mapBuiltinTypeNameToOid("line") == 628);
    assert(mapBuiltinTypeNameToOid("circle") == 718);
    assert(mapBuiltinTypeNameToOid("circle[]") == 719);

    struct RoundTrip {
        uint32_t oid;
        const char* input;
        const char* canonical;
        size_t bytes;
    };
    const std::vector<RoundTrip> cases = {
        {600, "1,2", "(1,2)", 16},
        {601, "[(0,0),(1,2)]", "[(0,0),(1,2)]", 32},
        {602, "[(0,0),(1,2)]", "[(0,0),(1,2)]", 37},
        {603, "(0,0),(2,3)", "(2,3),(0,0)", 32},
        {604, "((0,0),(1,0),(0,1))", "((0,0),(1,0),(0,1))", 52},
        {628, "{1,2,3}", "{1,2,3}", 24},
        {718, "<(1,2),3>", "<(1,2),3>", 24},
    };
    for (const auto& test : cases) {
        std::vector<uint8_t> binary;
        assert(encodeGeometryBinary(test.input, test.oid, binary));
        assert(binary.size() == test.bytes);
        std::string decoded;
        assert(decodeGeometryBinary(test.oid, binary, decoded));
        assert(decoded == test.canonical);
    }

    std::string decoded;
    assert(!decodeGeometryBinary(600, std::vector<uint8_t>(15), decoded));
    std::vector<uint8_t> nanPoint(16, 0);
    nanPoint[0] = 0x7f;
    nanPoint[1] = 0xf8;
    assert(!decodeGeometryBinary(600, nanPoint, decoded));
    assert(!decodeGeometryBinary(602, {2, 0, 0, 0, 1,
                                      0, 0, 0, 0, 0, 0, 0, 0,
                                      0, 0, 0, 0, 0, 0, 0, 0}, decoded));
    assert(!decodeGeometryBinary(604, {0, 0, 0, 2}, decoded));
    std::cout << "[GEO] OID/binary contract OK" << std::endl;
}

static void test_geo_update() {
    std::string db = testDbPath("geo_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE g (id INT PRIMARY KEY, b BOX)", s));

    assert(g_engine.insert(db, "g", {{"id","1"}, {"b","(0,0),(2,2)"}}) == dbms::DBStatus::OK);
    // Valid update canonicalizes.
    assert(g_engine.update(db, "g", {{"b","(1,1),(5,5)"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "b") == "(5,5),(1,1)");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "g", {{"b","(1,1)"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "g", {"=id 1"}, "b") == "(5,5),(1,1)");

    cleanup(db);
    std::cout << "[GEO] update enforce/canonicalize OK" << std::endl;
}

static void test_point_still_works() {
    std::string db = testDbPath("geo_point");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE g (id INT PRIMARY KEY, p POINT, note VARCHAR(20))", s));

    // POINT must retain both coordinates even when the tuple also has a
    // variable-length column (the mixed-layout writer previously treated it
    // as an integer).  max_digits10 keeps adjacent doubles distinct.
    const std::string p1 =
        "1.0000000000000002,2.0000000000000004";
    const std::string p2 =
        "1.0000000000000004,2.000000000000001";
    assert(g_engine.insert(db, "g", {{"id","1"}, {"p","3,4"}, {"note","a"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "g", {{"id","2"}, {"p",p2}, {"note","b"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "g", {{"id","3"}, {"p","0,0"}, {"note","c"}}) ==
           dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "p") == "3,4");
    assert(fetchOne(db, "g", {"=id 2"}, "p") == p2);

    assert(g_engine.update(db, "g", {{"p", "POINT (" + p1 + ")"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(fetchOne(db, "g", {"=id 1"}, "p") == p1);
    assert(g_engine.update(db, "g", {{"p", "3,4 trailing"}}, {"=id 2"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "g", {"=id 2"}, "p") == p2);

    for (const std::string invalid :
         {"missing-comma", "1oops,2", "nan,2", "1,2,3"}) {
        assert(g_engine.insert(db, "g", {{"id","4"}, {"p",invalid}}) ==
               dbms::DBStatus::INVALID_VALUE);
    }

    // This implementation's SP-GiST opclass is POINT-only.
    assert(g_engine.createSPGiSTIndex(db, "g", "id") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(!ddl.executeSql("CREATE INDEX g_p_spgist ON g USING SPGIST (p)", s));

    const fs::path sidecar = fs::path(db) / "g_p.spgist";
    std::ifstream input(sidecar, std::ios::binary);
    assert(input);
    const std::string validSidecar{
        std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    assert(validSidecar.find(p1) != std::string::npos);
    assert(validSidecar.find(p2) != std::string::npos);

    // A malformed sidecar is rejected and, crucially, is not cached as an
    // empty valid tree; restoring the file makes the next lookup succeed.
    {
        std::ofstream broken(sidecar, std::ios::binary | std::ios::trunc);
        broken << "not-a-rid not-a-point\n";
    }
    assert(g_engine.spGiSTSearch(db, "g", "p", "=", p1).empty());
    {
        std::ofstream restored(sidecar, std::ios::binary | std::ios::trunc);
        restored.write(validSidecar.data(),
                       static_cast<std::streamsize>(validSidecar.size()));
        assert(restored);
    }
    assert(g_engine.spGiSTSearch(db, "g", "p", "=", p1).size() == 1);
    assert(g_engine.spGiSTSearch(db, "g", "p", "=", p2).size() == 1);
    // Invalid predicates must not be interpreted as the origin.
    assert(g_engine.spGiSTSearch(db, "g", "p", "=", "not-a-point").empty());
    assert(g_engine.spGiSTSearch(db, "g", "p", "=", "0,0 trailing").empty());

    cleanup(db);
    std::cout << "[GEO] point validation/precision/SP-GiST reload OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_line_lseg_circle();
    test_box_corner_ordering();
    test_path_polygon();
    test_geo_update();
    test_point_still_works();
    test_binary_and_catalog_contract();
    std::cout << "[GEO] all passed" << std::endl;
    return 0;
}
