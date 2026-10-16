// ============================================================================
// inet / cidr type test — Phase 4 Wave 4.8b
// Strict IPv4/IPv6 address parsing (rejecting bad octets, bad groups and
// out-of-range prefixes) with IPv6 storage now functional, plus UPDATE support.
// Output is the engine's canonical decoded form (full IPv6 groups).
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/common_type.h"
#include "parser/parser.h"
#include <cassert>
#include <filesystem>
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

static void test_inet_ipv4() {
    std::string db = testDbPath("inet_v4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET, c CIDR)", s));

    // Host address (no prefix) and network with prefix round-trip.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","192.168.1.1"}, {"c","10.0.0.0/8"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "192.168.1.1");
    assert(fetchOne(db, "t", {"=id 1"}, "c") == "10.0.0.0/8");

    // inet with explicit prefix keeps it.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","172.16.5.4/24"}, {"c","192.168.0.0/16"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "172.16.5.4/24");

    cleanup(db);
    std::cout << "[INET] IPv4 round-trip OK" << std::endl;
}

static void test_inet_ipv6() {
    std::string db = testDbPath("inet_v6");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET)", s));

    // PostgreSQL canonical output uses inet_ntop-style zero compression.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","::1"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "::1");

    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","2001:db8::1"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "2001:db8::1");
    assert(g_engine.insert(db, "t", {{"id","3"}, {"a","::ffff:192.0.2.128"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "a") == "::ffff:192.0.2.128");

    cleanup(db);
    std::cout << "[INET] IPv6 storage OK" << std::endl;
}

static void test_cidr_host_bits_and_network_operators() {
    std::string db = testDbPath("cidr_semantics");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, c CIDR)", s));
    assert(g_engine.insert(db, "t", {{"id","1"}, {"c","192.168.1.0/24"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id","2"}, {"c","192.168.1.1/24"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","3"}, {"c","2001:db8::1/64"}}) == dbms::DBStatus::INVALID_VALUE);

    dbms::SQLParser parser;
    const auto parsed = parser.parse(
        "SELECT '192.168.1.7/24'::inet << '192.168.0.0/16'::cidr, "
        "'192.168.1.7/24'::inet <<= '192.168.1.7/24'::inet, "
        "'192.168.1.7/24'::inet << '192.168.1.7/24'::inet, "
        "'192.168.0.0/16'::cidr >> '192.168.1.7/24'::inet, "
        "'2001:db8::1/64'::inet && '2001:db8::/48'::cidr, "
        "'10.0.0.1'::inet < '::1'::inet");
    assert(parsed.success);
    const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
    assert(select && select->selectList.size() == 6);
    const std::vector<std::string> expected = {"t", "t", "f", "t", "t", "t"};
    dbms::ExprEvaluator evaluator;
    for (size_t i = 0; i < expected.size(); ++i) {
        const dbms::ExprValue value = evaluator.eval(select->selectList[i].expr.get(), {});
        assert(value.typeName == "boolean" && value.value == expected[i]);
    }
    const auto bad = parser.parse("SELECT '192.168.1.1/24'::cidr");
    assert(bad.success);
    const auto* badSelect = dynamic_cast<const dbms::SelectStmt*>(bad.stmt.get());
    bool rejected = false;
    try {
        (void)evaluator.eval(badSelect->selectList[0].expr.get(), {});
    } catch (const dbms::DbError& error) {
        rejected = error.sqlState() == "22P02";
    }
    assert(rejected);

    assert(dbms::mapBuiltinTypeNameToOid("cidr") == 650);
    assert(dbms::mapBuiltinTypeNameToOid("inet") == 869);
    assert(dbms::mapBuiltinTypeNameToOid("macaddr") == 829);
    assert(dbms::mapBuiltinTypeNameToOid("macaddr8") == 774);
    cleanup(db);
    std::cout << "[INET] cidr/operators/OIDs OK" << std::endl;
}

static void test_inet_invalid() {
    std::string db = testDbPath("inet_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET)", s));

    // Octet out of range.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","256.1.1.1"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too few octets.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","192.168.1"}}) == dbms::DBStatus::INVALID_VALUE);
    // Prefix out of range for IPv4.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"a","192.168.1.1/33"}}) == dbms::DBStatus::INVALID_VALUE);
    // Non-numeric prefix.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"a","192.168.1.1/x"}}) == dbms::DBStatus::INVALID_VALUE);
    // Pure garbage.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"a","not-an-ip"}}) == dbms::DBStatus::INVALID_VALUE);
    // IPv6 group too long / double "::".
    assert(g_engine.insert(db, "t", {{"id","6"}, {"a","12345::"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","7"}, {"a","2001::db8::1"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[INET] invalid rejected OK" << std::endl;
}

static void test_inet_update() {
    std::string db = testDbPath("inet_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","192.168.1.1"}}) == dbms::DBStatus::OK);
    // Valid update (previously the int-parse catch-all rejected all inet UPDATEs).
    assert(g_engine.update(db, "t", {{"a","10.1.2.3/24"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "10.1.2.3/24");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"a","300.0.0.1"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "10.1.2.3/24");

    cleanup(db);
    std::cout << "[INET] update enforce/reject OK" << std::endl;
}

static void test_typed_network_casts() {
    struct Case {
        const char* source;
        const char* value;
        bool null;
        const char* target;
        const char* expected;
        const char* state;
    };
    const Case cases[] = {
        {"name","192.0.2.129/24",false,"inet","192.0.2.129/24",""},
        {"name","192.0.2.0/24",false,"cidr","192.0.2.0/24",""},
        {"pg_catalog.name","192.0.2.129/24",false,"inet","192.0.2.129/24",""},
        {"pg_catalog.name","192.0.2.0/24",false,"cidr","192.0.2.0/24",""},
        {"\"pg_catalog\".\"name\"","2001:db8::/32",false,"inet","2001:db8::/32",""},
        {"\"pg_catalog\".\"name\"","2001:db8::/32",false,"pg_catalog.\"cidr\"","2001:db8::/32",""},
        {"\"pg_catalog\".\"name\"","",true,"inet","",""},
        {"\"pg_catalog\".\"name\"","",true,"cidr","",""},
        {"\"pg_catalog\".\"name\"","invalid",false,"inet","","22P02"},
        {"\"pg_catalog\".\"name\"","invalid",false,"cidr","","22P02"},
        {"\"pg_catalog\".\"name\"","192.0.2.129/24",false,"cidr","","22P02"},
        {"bytea","192.0.2.129/24",false,"inet","","42846"},
        {"integer","",true,"cidr","","42846"},
        // Raw SDK spellings must not impersonate the catalog's builtin NAME.
        {"other.name","192.0.2.129/24",false,"inet","","42846"},
        {"pg_catalog.\"Name\"","",true,"cidr","","42846"},
        {"inet","192.0.2.129/24",false,"cidr","192.0.2.0/24",""},
        {"inet","192.0.2.129/25",false,"cidr","192.0.2.128/25",""},
        {"inet","192.0.2.129/31",false,"cidr","192.0.2.128/31",""},
        {"inet","255.255.255.255/0",false,"cidr","0.0.0.0/0",""},
        {"inet","192.0.2.129",false,"cidr","192.0.2.129/32",""},
        {"inet","2001:db8::1234/64",false,"cidr","2001:db8::/64",""},
        {"inet","2001:db8::1235/127",false,"cidr","2001:db8::1234/127",""},
        {"inet","2001:db8::1234/0",false,"cidr","::/0",""},
        {"inet","2001:db8::1234",false,"cidr","2001:db8::1234/128",""},
        {"inet","::ffff:192.0.2.129/120",false,"cidr","::ffff:192.0.2.0/120",""},
        {"cidr","192.0.2.129/32",false,"inet","192.0.2.129",""},
        {"cidr","2001:db8::1234/128",false,"inet","2001:db8::1234",""},
        {"inet","",true,"cidr","",""},
        {"cidr","",true,"inet","",""},
        {"text","192.0.2.129/24",false,"cidr","","22P02"},
        {"name","192.0.2.129/24",false,"cidr","","22P02"},
        {"integer","",true,"cidr","","42846"},
    };
    dbms::ExprEvaluator evaluator;
    for (const auto& item : cases) {
        dbms::CastExpr cast;
        cast.typeName=item.target;
        auto parameter=std::make_unique<dbms::ParameterExpr>();
        parameter->slot=0; parameter->declaredType=item.source;
        cast.operand=std::move(parameter);
        dbms::RowContext row;
        row.setParameters({dbms::ExprValue(item.source,item.value,item.null)});
        std::string state;
        try {
            const auto result=evaluator.eval(&cast,row);
            assert(result.isNull==item.null);
            assert(result.value==item.expected);
            if (item.null) assert(result.typeName==item.target);
        } catch(const dbms::DbError& error) { state=error.sqlState(); }
        assert(state==item.state);
    }
    dbms::SQLParser parser;
    const auto parsed=parser.parse(
        "SELECT COALESCE(NULL::inet,'192.0.2.129/32'::cidr), "
        "COALESCE(NULL::cidr,NULL::inet), COALESCE(NULL::cidr,NULL::cidr), "
        "COALESCE('192.0.2.129/32'::cidr,NULL::inet), "
        "COALESCE('2001:db8::1234/128'::cidr,NULL::inet)");
    assert(parsed.success);
    const auto* select=dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
    assert(select && select->selectList.size()==5);
    const std::vector<std::string> coalesceValues={"192.0.2.129","","","192.0.2.129","2001:db8::1234"};
    for (size_t i=0;i<5;++i) {
        const auto value=evaluator.eval(select->selectList[i].expr.get(),{});
        assert(value.typeName==(i==2?"cidr":"inet"));
        assert(value.isNull==(i==1 || i==2));
        assert(value.value==coalesceValues[i]);
    }
    int laterCalls=0;
    evaluator.registerFunction("later_network_input",[&](const std::vector<dbms::ExprValue>&) -> dbms::ExprValue {
        ++laterCalls;
        throw dbms::DbError("XX000","unselected network input executed");
    });
    const auto lazy=parser.parse("SELECT COALESCE('192.0.2.129'::inet,later_network_input()::inet)");
    assert(lazy.success);
    const auto* lazySelect=dynamic_cast<const dbms::SelectStmt*>(lazy.stmt.get());
    assert(lazySelect && lazySelect->selectList.size()==1);
    const auto lazyValue=evaluator.eval(lazySelect->selectList[0].expr.get(),{});
    assert(lazyValue.typeName=="inet" && lazyValue.value=="192.0.2.129" && laterCalls==0);
    assert(dbms::common_type_detail::implicit("cidr","inet"));
    assert(!dbms::common_type_detail::implicit("inet","cidr"));
    assert(dbms::selectCommonType({"cidr","inet"},"VALUES")=="inet");
    assert(dbms::selectCommonType({"inet","cidr"},"VALUES")=="inet");
    std::cout << "[INET] typed network CAST/NULL/errors/NAME (32 cases), COALESCE(5)/lazy and cast graph OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_inet_ipv4();
    test_inet_ipv6();
    test_inet_invalid();
    test_inet_update();
    test_cidr_host_bits_and_network_operators();
    test_typed_network_casts();
    std::cout << "[INET] all passed" << std::endl;
    return 0;
}
