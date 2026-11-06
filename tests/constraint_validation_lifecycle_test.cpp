// Genuine CHECK/FK NOT VALID -> VALIDATE lifecycle and scan ownership.
// test_link_flags: -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
// Compile against matching before/final public headers, core61 and fresh stub.
// A runner must nm-prove separate-TU fetchPage references/definition before run.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/sha256.h"
#include "storage/PageAllocator.h"
#include "utils/Session.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <spawn.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
namespace fs = std::filesystem;
using Cell = std::optional<std::string>;
using Cells = std::vector<Cell>;
using RowInput = StorageEngine::SqlRow;
const std::string database = "constraint_lifecycle";
size_t checks = 0, failures = 0, groups = 0, fetchProbes = 0, zeroFetch = 0, positiveFetch = 0;
thread_local std::array<PageAllocator*, 2> watched{};
thread_local std::array<size_t, 2> watchedCounts{};
thread_local bool armed = false;
void check(bool condition, const std::string& label) {
    ++checks;
    if (!condition) { ++failures; std::cerr << "CONSTRAINT_LIFECYCLE_FAIL " << label << '\n'; }
}
void need(bool condition, const std::string& label) {
    check(condition, label);
    if (!condition) throw std::runtime_error("unsafe fixture continuation: " + label);
}
struct Watch {
    explicit Watch(PageAllocator* a, PageAllocator* b = nullptr) {
        need(!armed && a, "actual target allocator and nonnested watch");
        watched = {a, b}; watchedCounts = {}; armed = true;
    }
    ~Watch() { armed = false; watched = {}; }
};
struct ActualRow { int64_t rid = 0; std::vector<std::string> values; std::vector<bool> nulls; size_t rawBytes = 0; };
using Rows = std::map<int, ActualRow>;
bool sameRows(const Rows& a, const Rows& b) {
    if (a.size() != b.size()) return false;
    auto x = a.begin(), y = b.begin();
    for (; x != a.end(); ++x, ++y)
        if (x->first != y->first || x->second.rid != y->second.rid ||
            x->second.values != y->second.values || x->second.nulls != y->second.nulls) return false;
    return true;
}
std::string bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary); need(bool(in), "read actual file " + path.string());
    std::string value{std::istreambuf_iterator<char>(in), {}};
    need(!in.bad(), "complete actual file read"); return value;
}
Rows rows(const std::string& table) {
    const auto schema = g_engine.getTableSchema(database, table);
    auto scan = g_engine.beginHeapScan(database, table, schema);
    need(bool(scan), "real heap cursor " + table);
    Rows actual; HeapScanRow row;
    while (scan->next(row)) {
        need(row.nulls.size() == schema.len, "real schema-width NULL bitmap");
        ActualRow value; value.rid = row.rid; value.rawBytes = row.data.size();
        detail::decodeStoredRowValues(g_engine, database, row.data, schema, row.nulls, value.values, value.nulls);
        need(value.values.size() == schema.len && value.nulls == row.nulls && !value.nulls.front(),
             "physical decoder agrees with actual cursor NULL owner");
        size_t used = 0; const int id = std::stoi(value.values.front(), &used);
        need(used == value.values.front().size() && value.rid > 0 && actual.emplace(id, value).second,
             "complete genuine unique PK/RID");
        uint32_t page = 0; uint16_t slot = 0;
        StorageEngine::decodeRid(row.rid, page, slot);
        need(page > 0 && StorageEngine::encodeRid(page, slot) == row.rid, "actual physical RID roundtrip");
    }
    return actual;
}
void expected(const Rows& actual, const std::map<int, Cells>& wanted, const std::string& label) {
    check(actual.size() == wanted.size(), label + " complete row count");
    for (const auto& item : wanted) {
        const auto found = actual.find(item.first);
        need(found != actual.end(), label + " genuine expected PK");
        const auto& value = found->second;
        need(value.values.size() == item.second.size(), label + " actual width");
        for (size_t i = 0; i < item.second.size(); ++i)
            check(value.nulls[i] == !item.second[i] && (!item.second[i] || value.values[i] == *item.second[i]),
                  label + " full value/NULL PK=" + std::to_string(item.first) + " col=" + std::to_string(i));
        std::cout << "CONSTRAINT_LIFECYCLE_ROW " << label << " id=" << item.first << " rid=" << value.rid;
        for (size_t i = 0; i < value.values.size(); ++i)
            std::cout << ' ' << value.nulls[i] << ' ' << std::quoted(value.values[i]);
        std::cout << '\n';
    }
}
struct SqlResult { bool success = false; std::string state; };
Session session;
SqlResult execute(const std::string& sql) {
    SqlResult value;
    try { DdlExecutor ddl; value.success = !ddl.executeSql(sql, session); }
    catch (const DbError& error) { value.state = error.sqlState(); }
    std::cout << "CONSTRAINT_LIFECYCLE_SQL success=" << value.success << " state=" << std::quoted(value.state)
              << " sql=" << std::quoted(sql) << '\n';
    return value;
}
void ddl(const std::string& sql) { need(execute(sql).success, "real DDL succeeds: " + sql); }
void sqlState(const std::string& sql, const std::string& state) {
    const auto result = execute(sql);
    check(!result.success && result.state == state, "real DDL SQLSTATE " + state + ": " + sql);
}
void insert(const std::string& table, const RowInput& values, DBStatus status = DBStatus::OK) {
    const auto actual = g_engine.insertRow(database, table, values);
    std::cout << "CONSTRAINT_LIFECYCLE_INSERT table=" << table << " expected=" << sqlstateForDBStatus(status)
              << " actual=" << sqlstateForDBStatus(actual) << '\n';
    need(actual == status, "native INSERT exact status " + table);
}
void update(const std::string& table, const RowInput& values, int id, DBStatus status = DBStatus::OK) {
    const auto actual = g_engine.updateRows(database, table, values, {"=id " + std::to_string(id)});
    std::cout << "CONSTRAINT_LIFECYCLE_UPDATE table=" << table << " id=" << id
              << " expected=" << sqlstateForDBStatus(status) << " actual=" << sqlstateForDBStatus(actual) << '\n';
    need(actual == status, "native UPDATE exact status " + table);
}
void flags(const std::string& table, const std::string& constraint, bool validated) {
    const auto params = g_engine.getStorageParams(database, table);
    const auto prefix = "constraint." + constraint + ".";
    const auto a = params.find(prefix + "validated"), b = params.find(prefix + "not_valid");
    need(a != params.end() && b != params.end(), "both actual persisted validation flags exist");
    check(a->second == (validated ? "1" : "0") && b->second == (validated ? "0" : "1"),
          "actual flags " + table + "." + constraint);
    const auto raw = bytes(fs::path(database) / (table + ".params"));
    check(raw.find(prefix + "validated=" + (validated ? "1" : "0") + "\n") != std::string::npos &&
          raw.find(prefix + "not_valid=" + (validated ? "0" : "1") + "\n") != std::string::npos,
          "real params file contains same validated/not_valid pair");
}
PageAllocator* target(const std::string& table) {
    const auto schema = g_engine.getTableSchema(database, table);
    need(schema.len > 0, "warm actual schema");
    auto* allocator = g_engine.getPageAllocator(database, table);
    need(allocator != nullptr, "actual target allocator"); return allocator;
}
void positive(const std::string& table) {
    auto* allocator = target(table); size_t count = 0; bool ok = false;
    { Watch watch(allocator); ok = g_engine.forEachRow(database, table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }); }
    ++fetchProbes; ++positiveFetch;
    need(ok && count > 0 && watchedCounts[0] > 0, "external real heap fetch wrapper positive control: " + table);
    std::cout << "CONSTRAINT_LIFECYCLE_FETCH positive=" << table << " rows=" << count << " fetch=" << watchedCounts[0] << '\n';
}
SqlResult watchedSql(const std::string& sql, const std::string& a, const std::string& b,
    bool mustScan, const std::string& state = "") {
    auto* pa = target(a); auto* pb = b.empty() ? nullptr : target(b);
    SqlResult result;
    { Watch watch(pa, pb); result = execute(sql); }
    ++fetchProbes;
    std::cout << "CONSTRAINT_LIFECYCLE_FETCH sql=" << std::quoted(sql)
              << " a=" << watchedCounts[0] << " b=" << watchedCounts[1] << '\n';
    if (mustScan) {
        ++positiveFetch; check(watchedCounts[0] > 0, "actual validation scans first target");
        if (pb) check(watchedCounts[1] > 0, "actual FK validation scans second target");
    } else {
        ++zeroFetch; check(watchedCounts[0] == 0 && watchedCounts[1] == 0,
                          "NOT VALID/definition/already-valid performs zero target heap fetch");
    }
    if (state.empty()) need(result.success, "watched statement succeeds");
    else need(!result.success && result.state == state, "watched statement preserves actual SQLSTATE " + state);
    return result;
}
template<class F> void group(const std::string& label, F&& run) {
    ++groups; std::cout << "CONSTRAINT_LIFECYCLE_GROUP " << groups << ' ' << label << '\n';
    const size_t oldFailures = failures;
    try { run(); }
    catch (const std::exception& error) {
        if (failures == oldFailures) { ++checks; ++failures; }
        std::cerr << "CONSTRAINT_LIFECYCLE_GROUP_STOP " << label << ' ' << error.what() << '\n';
        if (g_engine.inTransaction())
            check(g_engine.rollbackTransaction() == DBStatus::OK, "failing independent group releases its actual transaction");
    }
}
std::map<std::string, std::map<int, Cells>> wantedCold;
void lifecycle() {
    group("definition", [&] {
        ddl("CREATE TABLE cv_definition(id INT PRIMARY KEY,n INT,tag TEXT)");
        for (const auto& definition : std::vector<std::pair<std::string,std::string>>{
            {"missing > 0", "42703"}, {"n", "42804"}, {"no_such_check_fn(n)", "42883"},
            {"sum(n) > 0", "42803"}, {"row_number() OVER () > 0", "42P20"},
            {"(SELECT 1) > 0", "0A000"}, {"tag + n > 0", "42883"}}) {
            watchedSql("ALTER TABLE cv_definition ADD CONSTRAINT cv_bad CHECK (" + definition.first + ") NOT VALID",
                       "cv_definition", "", false, definition.second);
            const auto schema = g_engine.getTableSchema(database,"cv_definition");
            check(schema.additionalCheckConstraints.empty() && schema.cols[1].checkExpr.empty(),
                  "failed definition leaves no advertised CHECK");
        }
        ddl("ALTER TABLE cv_definition ADD CONSTRAINT cv_false CHECK (FALSE) NOT VALID");
        ddl("ALTER TABLE cv_definition ADD CONSTRAINT cv_null CHECK (NULL) NOT VALID");
        flags("cv_definition","cv_false",false); flags("cv_definition","cv_null",false);
        insert("cv_definition", {{"id","1"},{"n","1"},{"tag","ok"}}, DBStatus::CHECK_VIOLATION);
        ddl("ALTER TABLE cv_definition DROP CONSTRAINT cv_false");
        insert("cv_definition", {{"id","1"},{"n","1"},{"tag",""}});
        ddl("ALTER TABLE cv_definition VALIDATE CONSTRAINT cv_null"); flags("cv_definition","cv_null",true);
    });
    group("CHECK-lifecycle", [&] {
        ddl("CREATE TABLE cv_check(id INT PRIMARY KEY,n INT,pad TEXT)");
        insert("cv_check",{{"id","1"},{"n","-1"},{"pad","old-bad"}});
        insert("cv_check",{{"id","2"},{"n",std::nullopt},{"pad",""}});
        const auto before = rows("cv_check"); positive("cv_check");
        watchedSql("ALTER TABLE cv_check ADD CONSTRAINT cv_positive CHECK (n > 0) NOT VALID","cv_check","",false);
        flags("cv_check","cv_positive",false); check(sameRows(before,rows("cv_check")),"NV preserves every old value/RID/NULL");
        insert("cv_check",{{"id","3"},{"n","-8"},{"pad","new-bad"}},DBStatus::CHECK_VIOLATION);
        update("cv_check",{{"pad","unrelated"}},1,DBStatus::CHECK_VIOLATION);
        check(sameRows(before,rows("cv_check")),"rejected writes preserve complete old heap");
        const auto priorParams = bytes(database+"/cv_check.params");
        watchedSql("ALTER TABLE cv_check VALIDATE CONSTRAINT cv_positive","cv_check","",true,"23514");
        flags("cv_check","cv_positive",false);
        check(bytes(database+"/cv_check.params")==priorParams && sameRows(before,rows("cv_check")),"failed scan never publishes true or mutates heap");
        update("cv_check",{{"n","7"}},1);
        watchedSql("ALTER TABLE cv_check VALIDATE CONSTRAINT cv_positive","cv_check","",true);
        flags("cv_check","cv_positive",true);
        watchedSql("ALTER TABLE cv_check VALIDATE CONSTRAINT cv_positive","cv_check","",false);
        wantedCold["cv_check"]={{1,{{"1"},{"7"},{"old-bad"}}},{2,{{"2"},std::nullopt,{""}}}};
        expected(rows("cv_check"),wantedCold.at("cv_check"),"check-repaired");
    });
    group("CHECK-empty-quoted-multicol-TOAST", [&] {
        ddl("CREATE TABLE cv_text(id INT PRIMARY KEY,\"Limit\" INT,n INT,tag TEXT)");
        const std::string toast=std::string(7000,'T')+"λ";
        insert("cv_text",{{"id","1"},{"Limit","9"},{"n","4"},{"tag",""}});
        insert("cv_text",{{"id","2"},{"Limit","9"},{"n","5"},{"tag",std::nullopt}});
        insert("cv_text",{{"id","3"},{"Limit","9"},{"n","6"},{"tag","NULL"}});
        insert("cv_text",{{"id","4"},{"Limit","9"},{"n","7"},{"tag",toast}});
        const auto before=rows("cv_text");positive("cv_text");
        need(before.at(4).rawBytes<toast.size(),"real long datum externalized from actual heap tuple");
        watchedSql("ALTER TABLE cv_text ADD CONSTRAINT cv_nonempty CHECK (tag <> '') NOT VALID","cv_text","",false);
        watchedSql("ALTER TABLE cv_text ADD CONSTRAINT cv_pair CHECK (n < \"Limit\" AND id > 0) NOT VALID","cv_text","",false);
        check(sameRows(before,rows("cv_text")),"quoted/multicol NV has no old-row mutation");
        insert("cv_text",{{"id","5"},{"Limit","9"},{"n","1"},{"tag",""}},DBStatus::CHECK_VIOLATION);
        watchedSql("ALTER TABLE cv_text VALIDATE CONSTRAINT cv_nonempty","cv_text","",true,"23514");
        flags("cv_text","cv_nonempty",false);
        update("cv_text",{{"tag","fixed"}},1);
        watchedSql("ALTER TABLE cv_text VALIDATE CONSTRAINT cv_nonempty","cv_text","",true);
        watchedSql("ALTER TABLE cv_text VALIDATE CONSTRAINT cv_pair","cv_text","",true);
        flags("cv_text","cv_nonempty",true);flags("cv_text","cv_pair",true);
        insert("cv_text",{{"id","6"},{"Limit","9"},{"n","1"},{"tag",std::nullopt}});
        wantedCold["cv_text"]={{1,{{"1"},{"9"},{"4"},{"fixed"}}},{2,{{"2"},{"9"},{"5"},std::nullopt}},
            {3,{{"3"},{"9"},{"6"},{"NULL"}}},{4,{{"4"},{"9"},{"7"},{toast}}},{6,{{"6"},{"9"},{"1"},std::nullopt}}};
        expected(rows("cv_text"),wantedCold.at("cv_text"),"text-validated");
    });
    group("FK-lifecycle", [&] {
        ddl("CREATE TABLE cv_parent(id INT PRIMARY KEY,pad TEXT)");
        ddl("CREATE TABLE cv_child(id INT PRIMARY KEY,pid INT,pad TEXT)");
        insert("cv_parent",{{"id","1"},{"pad","parent"}});
        insert("cv_child",{{"id","1"},{"pid","999"},{"pad","old-orphan"}});
        insert("cv_child",{{"id","2"},{"pid",std::nullopt},{"pad",""}});
        const auto old=rows("cv_child"), parents=rows("cv_parent");positive("cv_child");positive("cv_parent");
        watchedSql("ALTER TABLE cv_child ADD CONSTRAINT cv_fk FOREIGN KEY(pid) REFERENCES cv_parent(id) NOT VALID","cv_child","cv_parent",false);
        flags("cv_child","cv_fk",false);check(sameRows(old,rows("cv_child"))&&sameRows(parents,rows("cv_parent")),"FK NV leaves real child/parent untouched");
        update("cv_child",{{"pad","changed-only-pad"}},1);
        update("cv_child",{{"pid","999"}},1);
        update("cv_child",{{"pid",std::nullopt}},2);
        update("cv_child",{{"pid","998"}},1,DBStatus::FOREIGN_KEY_VIOLATION);
        insert("cv_child",{{"id","3"},{"pid","998"},{"pad","bad"}},DBStatus::FOREIGN_KEY_VIOLATION);
        const auto before=rows("cv_child");const auto raw=bytes(database+"/cv_child.params");
        watchedSql("ALTER TABLE cv_child VALIDATE CONSTRAINT cv_fk","cv_child","cv_parent",true,"23503");
        flags("cv_child","cv_fk",false);check(bytes(database+"/cv_child.params")==raw&&sameRows(before,rows("cv_child")),"failed FK validation leaves flags/heap exact");
        insert("cv_parent",{{"id","999"},{"pad","repair"}});
        watchedSql("ALTER TABLE cv_child VALIDATE CONSTRAINT cv_fk","cv_child","cv_parent",true);
        flags("cv_child","cv_fk",true);
        watchedSql("ALTER TABLE cv_child VALIDATE CONSTRAINT cv_fk","cv_child","cv_parent",false);
        wantedCold["cv_child"]={{1,{{"1"},{"999"},{"changed-only-pad"}}},{2,{{"2"},std::nullopt,{""}}}};
        wantedCold["cv_parent"]={{1,{{"1"},{"parent"}}},{999,{{"999"},{"repair"}}}};
        expected(rows("cv_child"),wantedCold.at("cv_child"),"child-validated");
    });
    group("FK-empty-composite-and-old-native-default", [&] {
        ddl("CREATE TABLE cv_text_parent(id INT PRIMARY KEY,k TEXT UNIQUE)");
        ddl("CREATE TABLE cv_text_child(id INT PRIMARY KEY,k TEXT)");
        insert("cv_text_parent",{{"id","1"},{"k","present"}});
        insert("cv_text_child",{{"id","1"},{"k",""}});insert("cv_text_child",{{"id","2"},{"k",std::nullopt}});
        watchedSql("ALTER TABLE cv_text_child ADD CONSTRAINT cv_text_fk FOREIGN KEY(k) REFERENCES cv_text_parent(k) NOT VALID","cv_text_child","cv_text_parent",false);
        watchedSql("ALTER TABLE cv_text_child VALIDATE CONSTRAINT cv_text_fk","cv_text_child","cv_text_parent",true,"23503");
        insert("cv_text_parent",{{"id","2"},{"k",""}});
        watchedSql("ALTER TABLE cv_text_child VALIDATE CONSTRAINT cv_text_fk","cv_text_child","cv_text_parent",true);
        insert("cv_text_child",{{"id","3"},{"k",""}});insert("cv_text_child",{{"id","4"},{"k",std::nullopt}});
        ddl("CREATE TABLE cv_pair_parent(id INT PRIMARY KEY,a INT,b INT,UNIQUE(a,b))");
        ddl("CREATE TABLE cv_pair_child(id INT PRIMARY KEY,a INT,b INT,pad TEXT)");
        insert("cv_pair_parent",{{"id","1"},{"a","1"},{"b","2"}});
        insert("cv_pair_child",{{"id","1"},{"a","8"},{"b","9"},{"pad","orphan"}});
        ddl("ALTER TABLE cv_pair_child ADD CONSTRAINT cv_pair_fk FOREIGN KEY(a,b) REFERENCES cv_pair_parent(a,b) NOT VALID");
        update("cv_pair_child",{{"a","8"},{"b","9"},{"pad","same-key"}},1);
        update("cv_pair_child",{{"a","8"},{"b","10"}},1,DBStatus::FOREIGN_KEY_VIOLATION);
        watchedSql("ALTER TABLE cv_pair_child VALIDATE CONSTRAINT cv_pair_fk","cv_pair_child","cv_pair_parent",true,"23503");
        insert("cv_pair_parent",{{"id","2"},{"a","8"},{"b","9"}});
        watchedSql("ALTER TABLE cv_pair_child VALIDATE CONSTRAINT cv_pair_fk","cv_pair_child","cv_pair_parent",true);
        ddl("CREATE TABLE cv_default(id INT PRIMARY KEY,n INT,pid INT)");
        insert("cv_default",{{"id","1"},{"n","-1"},{"pid","998"}});
        check(g_engine.alterTableAddCheckConstraint(database,"cv_default","cv_default_ck","n > 0")==DBStatus::CHECK_VIOLATION,
              "old four-argument native CHECK API still validates history by default");
        check(g_engine.alterTableAddFKConstraint(database,"cv_default","cv_default_fk",{"pid"},"cv_parent",{"id"})==DBStatus::INVALID_VALUE,
              "old native FK argument list preserves historical-orphan INVALID_VALUE contract");
        check(g_engine.alterTableAddCheckConstraint(database,"cv_default","cv_default_malformed","n >")==DBStatus::INVALID_VALUE,
              "old native CHECK malformed-definition INVALID_VALUE contract");
    });
    group("user-routine-demand", [&] {
        ddl("CREATE TABLE cv_routine(id INT PRIMARY KEY,n INT)");
        for(int id=1;id<=4;++id)insert("cv_routine",{{"id",std::to_string(id)},{"n",std::to_string(id)}});
        ddl("CREATE SEQUENCE cv_calls");
        need(g_engine.setval(database,"cv_calls",100,true)==100,"real sequence counter seeded");
        ddl("CREATE FUNCTION cv_count(value integer) RETURNS boolean LANGUAGE SQL VOLATILE AS $$ SELECT nextval('cv_calls') > 0 AND value > 0 $$");
        watchedSql("ALTER TABLE cv_routine ADD CONSTRAINT cv_routine_ck CHECK (cv_count(n)) NOT VALID","cv_routine","",false);
        check(g_engine.currval(database,"cv_calls")==100,"NOT VALID definition invokes no actual routine");
        watchedSql("ALTER TABLE cv_routine VALIDATE CONSTRAINT cv_routine_ck","cv_routine","",true);
        check(g_engine.currval(database,"cv_calls")==104,"validation evaluates original actual routine once per visible row");
        watchedSql("ALTER TABLE cv_routine VALIDATE CONSTRAINT cv_routine_ck","cv_routine","",false);
        check(g_engine.currval(database,"cv_calls")==104,"already-valid validation executes no routine or definition expression");
    });
    group("transaction-savepoint-flags", [&] {
        ddl("CREATE TABLE cv_tx(id INT PRIMARY KEY,n INT)");insert("cv_tx",{{"id","1"},{"n","2"}});
        ddl("ALTER TABLE cv_tx ADD CONSTRAINT cv_tx_ck CHECK (n > 0) NOT VALID");flags("cv_tx","cv_tx_ck",false);
        const auto raw=bytes(database+"/cv_tx.params");const auto initial=rows("cv_tx");
        need(g_engine.beginTransaction(database,true)==DBStatus::OK,"real exclusive snapshot transaction starts");
        ddl("ALTER TABLE cv_tx VALIDATE CONSTRAINT cv_tx_ck");flags("cv_tx","cv_tx_ck",true);
        need(g_engine.rollbackTransaction()==DBStatus::OK,"actual transaction rollback");
        flags("cv_tx","cv_tx_ck",false);check(bytes(database+"/cv_tx.params")==raw&&sameRows(initial,rows("cv_tx")),"rollback restores raw params and exact heap/RID");
        need(g_engine.beginTransaction(database,true)==DBStatus::OK,"real snapshot transaction for user savepoint");
        need(g_engine.savepoint("cv_user_sp")==DBStatus::OK,"real user savepoint created");
        ddl("ALTER TABLE cv_tx VALIDATE CONSTRAINT cv_tx_ck");flags("cv_tx","cv_tx_ck",true);
        need(g_engine.rollbackToSavepoint("cv_user_sp")==DBStatus::OK,"actual rollback to user savepoint");
        flags("cv_tx","cv_tx_ck",false);check(bytes(database+"/cv_tx.params")==raw,"savepoint restores original params byteexact");
        need(g_engine.releaseSavepoint("cv_user_sp")==DBStatus::OK&&g_engine.commitTransaction()==DBStatus::OK,"release and normal transaction commit");
        flags("cv_tx","cv_tx_ck",false);
        wantedCold["cv_tx"]={{1,{{"1"},{"2"}}}};
    });
    group("rename-drop", [&] {
        ddl("CREATE TABLE cv_names(id INT PRIMARY KEY,n INT)");insert("cv_names",{{"id","1"},{"n","3"}});
        ddl("ALTER TABLE cv_names ADD CONSTRAINT cv_names_ck CHECK (n > 0) NOT VALID");
        ddl("ALTER TABLE cv_names RENAME CONSTRAINT cv_names_ck TO cv_names_new");flags("cv_names","cv_names_new",false);
        const auto params=g_engine.getStorageParams(database,"cv_names");
        check(!params.count("constraint.cv_names_ck.validated")&&!params.count("constraint.cv_names_ck.not_valid"),"rename does not leave old validation identity");
        ddl("ALTER TABLE cv_names RENAME COLUMN n TO amount");
        watchedSql("ALTER TABLE cv_names VALIDATE CONSTRAINT cv_names_new","cv_names","",true);flags("cv_names","cv_names_new",true);
        ddl("ALTER TABLE cv_names RENAME TO cv_renamed");flags("cv_renamed","cv_names_new",true);
        insert("cv_renamed",{{"id","2"},{"amount","-1"}},DBStatus::CHECK_VIOLATION);
        ddl("ALTER TABLE cv_renamed DROP CONSTRAINT cv_names_new");
        const auto dropped=g_engine.getStorageParams(database,"cv_renamed");
        check(!dropped.count("constraint.cv_names_new.validated")&&!dropped.count("constraint.cv_names_new.not_valid"),"DROP removes only dropped constraint flags");
        insert("cv_renamed",{{"id","2"},{"amount","-1"}});
        wantedCold["cv_renamed"]={{1,{{"1"},{"3"}}},{2,{{"2"},{"-1"}}}};
    });
}
void saveCold(const fs::path& path) {
    need(!fs::exists(path),"fresh cold reference file");std::ofstream out(path,std::ios::binary);
    for(const auto& table:wantedCold) {
        const auto actual=rows(table.first);expected(actual,table.second,"pre-cold-"+table.first);
        out<<"TABLE "<<std::quoted(table.first)<<' '<<actual.size()<<'\n';
        for(const auto& row:actual) {out<<row.first<<' '<<row.second.rid<<' '<<row.second.values.size();
            for(size_t i=0;i<row.second.values.size();++i)out<<' '<<row.second.nulls[i]<<' '<<std::quoted(row.second.values[i]);out<<'\n';}
        out<<"PARAMS "<<std::quoted(bytes(fs::path(database)/(table.first+".params")))<<'\n';
    }
    out.close();need(!out.fail(),"complete independent cold reference write");
}
void cold(const fs::path& path) {
    std::ifstream in(path,std::ios::binary);need(bool(in),"read actual closed prior worker reference");
    std::string word;
    while(in>>word) {
        need(word=="TABLE","strict cold reference record");std::string table;size_t count=0;in>>std::quoted(table)>>count;Rows expectedRows;
        for(size_t i=0;i<count;++i) {int id=0;ActualRow row;size_t width=0;in>>id>>row.rid>>width;
            for(size_t c=0;c<width;++c){bool null=false;std::string value;in>>null>>std::quoted(value);row.nulls.push_back(null);row.values.push_back(std::move(value));}
            need(bool(in)&&expectedRows.emplace(id,std::move(row)).second,"complete cold row source");}
        std::string raw;in>>word>>std::quoted(raw);need(bool(in)&&word=="PARAMS","actual params cold source");
        check(sameRows(rows(table),expectedRows),"fresh process exact full value/NULL/RID: "+table);
        check(bytes(fs::path(database)/(table+".params"))==raw,"fresh process raw validation metadata exact: "+table);
    }
    need(in.eof(),"complete prior worker reference consumed");
    flags("cv_check","cv_positive",true);flags("cv_child","cv_fk",true);flags("cv_tx","cv_tx_ck",false);
    insert("cv_check",{{"id","91"},{"n","-1"},{"pad","cold-bad"}},DBStatus::CHECK_VIOLATION);
    insert("cv_text",{{"id","91"},{"Limit","9"},{"n","1"},{"tag",""}},DBStatus::CHECK_VIOLATION);
    insert("cv_child",{{"id","91"},{"pid","998"},{"pad","cold-orphan"}},DBStatus::FOREIGN_KEY_VIOLATION);
    positive("cv_check");positive("cv_child");positive("cv_parent");
    watchedSql("ALTER TABLE cv_check VALIDATE CONSTRAINT cv_positive","cv_check","",false);
    watchedSql("ALTER TABLE cv_child VALIDATE CONSTRAINT cv_fk","cv_child","cv_parent",false);
}
void worker(const std::string& phase,const fs::path& root) {
    const auto cluster=fs::canonical(fs::current_path());const auto* env=std::getenv("DBMS_DATA_DIR");
    need(env&&fs::equivalent(cluster,env)&&cluster==root/"cluster","real cwd/env set before exec/global engine");
    TypeRegistry::instance().bootstrap();session.username="admin";session.permission=1;session.currentDB=database;
    struct RestoreSession {Session* old;~RestoreSession(){setCurrentSession(old);}} restore{currentSession()};setCurrentSession(&session);
    if(phase=="lifecycle") {
        need(!g_engine.databaseExists(database)&&g_engine.createDatabase(database,"utf8")==DBStatus::OK,"fresh private real database");
        lifecycle();
        if(!failures) {need(wantedCold.size()==6,"all independent cold fixtures present");saveCold(root/"cold-reference.txt");}
    } else {need(phase=="cold"&&g_engine.databaseExists(database),"fixed actual existing cold phase");cold(root/"cold-reference.txt");}
    if(g_engine.inTransaction()) {const auto status=g_engine.rollbackTransaction();check(status==DBStatus::OK,"unclosed failing fixture transaction explicitly rolled back");}
    std::cout<<"CONSTRAINT_LIFECYCLE_WORKER phase="<<phase<<" groups="<<groups<<" checks="<<checks<<" failures="<<failures
             <<" fetch_probes="<<fetchProbes<<" zero_fetch="<<zeroFetch<<" positive_fetch="<<positiveFetch<<'\n';
}
void spawn(const fs::path& exe,const fs::path& root,const std::string& phase) {
    const auto cluster=root/"cluster";
    std::vector<std::string> environment;for(char** p=environ;*p;++p)if(std::string(*p).rfind("DBMS_",0)!=0)environment.emplace_back(*p);
    environment.push_back("DBMS_DATA_DIR="+cluster.string());std::vector<char*> env;for(auto& x:environment)env.push_back(x.data());env.push_back(nullptr);
    std::vector<std::string> arguments={exe.string(),"--worker",phase,root.string()};std::vector<char*> args;for(auto& x:arguments)args.push_back(x.data());args.push_back(nullptr);
    posix_spawn_file_actions_t actions;need(posix_spawn_file_actions_init(&actions)==0,"actual spawn actions");
    need(posix_spawn_file_actions_addchdir_np(&actions,cluster.c_str())==0,"child real cwd before global engine initialization");
    const auto out=root/(phase+".stdout"),err=root/(phase+".stderr");
    need(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,out.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0&&
         posix_spawn_file_actions_addopen(&actions,STDERR_FILENO,err.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0,"new child raw output only");
    posix_spawnattr_t attributes;need(posix_spawnattr_init(&attributes)==0,"actual spawn attrs");
    need(posix_spawnattr_setflags(&attributes,POSIX_SPAWN_SETPGROUP)==0&&posix_spawnattr_setpgroup(&attributes,0)==0,"owned child process group");pid_t pid=0;
    const int error=posix_spawn(&pid,exe.c_str(),&actions,&attributes,args.data(),env.data());posix_spawn_file_actions_destroy(&actions);posix_spawnattr_destroy(&attributes);
    need(error==0&&pid>0,"actual fresh worker exec");
    struct Child {pid_t pid;bool reaped=false;~Child(){if(reaped)return;const bool own=getpgid(pid)==pid;if(own)(void)kill(-pid,SIGTERM);else(void)kill(pid,SIGTERM);
        int status=0;for(int i=0;i<100;++i){const auto w=waitpid(pid,&status,WNOHANG);if(w==pid||(w<0&&errno==ECHILD))return;std::this_thread::sleep_for(std::chrono::milliseconds(5));}
        if(own)(void)kill(-pid,SIGKILL);else(void)kill(pid,SIGKILL);while(waitpid(pid,&status,0)<0&&errno==EINTR){}}} child{pid};
    std::ifstream stat("/proc/"+std::to_string(pid)+"/stat");std::string line;std::getline(stat,line);const auto close=line.rfind(')');
    need(close!=std::string::npos,"real child start identity");std::istringstream fields(line.substr(close+2));std::string ticks;for(int i=0;i<20;++i)fields>>ticks;
    std::cout<<"CONSTRAINT_LIFECYCLE_CHILD phase="<<phase<<" pid="<<pid<<" start_ticks="<<ticks<<'\n'<<std::flush;
    int status=0;pid_t waited=0;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(180);
    for(;;){waited=waitpid(pid,&status,WNOHANG);if(waited==pid)break;if(waited<0&&errno!=EINTR)break;
        if(std::chrono::steady_clock::now()>=deadline)throw std::runtime_error("owned worker timeout (not normal): "+phase);std::this_thread::sleep_for(std::chrono::milliseconds(5));}
    child.reaped=waited==pid;std::cout<<"CONSTRAINT_LIFECYCLE_CHILD_TERMINAL phase="<<phase<<" status="<<status<<" stdout="<<out<<" stderr="<<err<<'\n'<<std::flush;
    need(waited==pid&&WIFEXITED(status)&&WEXITSTATUS(status)==0,"strict fresh worker normal0: "+phase);
    need(kill(pid,0)==-1&&errno==ESRCH&&kill(-pid,0)==-1&&errno==ESRCH,"actual child/group gone");
}
// Parent-only evidence capture after the actual child has exited normally.
// A later cold close can never replace an earlier phase's captured bytes.
void captureClosedPhase(const fs::path& root, const std::string& phase) {
    need(!armed && (phase == "lifecycle" || phase == "cold"), "phase capture outside fetch watch");
    const auto source = root / "cluster", imageRoot = root / "images", destination = imageRoot / phase;
    need(fs::is_directory(source) && !fs::is_symlink(source), "actual closed phase cluster");
    if (!fs::exists(imageRoot)) need(fs::create_directory(imageRoot), "new parent-owned image root");
    need(fs::is_directory(imageRoot) && !fs::is_symlink(imageRoot) && !fs::exists(destination), "fresh phase-specific image");
    need(fs::create_directory(destination), "new exact phase image directory");
    std::vector<fs::path> directories, files;
    for (const auto& entry : fs::recursive_directory_iterator(source)) {
        const auto status = entry.symlink_status();
        need(!fs::is_symlink(status), "reject symlink in closed phase inventory");
        const auto relative = entry.path().lexically_relative(source);
        need(!relative.empty() && !relative.is_absolute(), "actual relative phase path");
        for (const auto& component : relative) need(component != "..", "phase path stays under owned cluster");
        if (fs::is_directory(status)) directories.push_back(relative);
        else { need(fs::is_regular_file(status), "closed evidence is a regular file or directory"); files.push_back(relative); }
    }
    std::sort(directories.begin(), directories.end()); std::sort(files.begin(), files.end());
    const auto manifest = root / (phase + "-image-inventory-v1.txt");
    need(!fs::exists(manifest), "new fixed phase inventory path");
    std::ofstream inventory(manifest, std::ios::binary);
    need(bool(inventory), "open real phase inventory");
    inventory << "CONSTRAINT_LIFECYCLE_PHASE_IMAGE_V1 " << std::quoted(phase) << ' '
              << files.size() << ' ' << directories.size() << '\n';
    for (const auto& relative : directories) {
        need(fs::create_directories(destination / relative) || fs::is_directory(destination / relative), "preserve actual empty/nonempty directory");
        inventory << "D " << std::quoted(relative.generic_string()) << '\n';
    }
    uintmax_t totalBytes = 0;
    std::array<char, 64 * 1024> buffer{};
    for (const auto& relative : files) {
        const auto inputPath = source / relative, outputPath = destination / relative;
        const uintmax_t originalLength = fs::file_size(inputPath);
        need(!fs::exists(outputPath), "never overwrite an existing phase file");
        std::ifstream input(inputPath, std::ios::binary);
        std::ofstream output(outputPath, std::ios::binary);
        need(bool(input) && bool(output), "open exact source and new phase destination");
        SHA256 digest; uintmax_t count = 0;
        while (input) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const auto length = input.gcount();
            if (length > 0) { digest.append(buffer.data(), static_cast<size_t>(length)); output.write(buffer.data(), length); count += static_cast<uintmax_t>(length); }
        }
        need(input.eof() && !input.bad() && count == originalLength && fs::file_size(inputPath) == originalLength,
             "complete stable closed source bytes, including zero-length files");
        output.close(); need(!output.fail(), "complete actual phase copy write");
        const auto sha = digest.finishHex();
        std::ifstream copied(outputPath, std::ios::binary); need(bool(copied), "read back actual phase destination");
        SHA256 copiedDigest; uintmax_t copiedCount = 0;
        while (copied) {
            copied.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const auto length = copied.gcount();
            if (length > 0) { copiedDigest.append(buffer.data(), static_cast<size_t>(length)); copiedCount += static_cast<uintmax_t>(length); }
        }
        need(copied.eof() && !copied.bad() && copiedCount == count && copiedDigest.finishHex() == sha,
             "copied phase full bytes/SHA equal captured source");
        totalBytes += count;
        const auto imageRelative = outputPath.lexically_relative(root).generic_string();
        inventory << "F " << std::quoted(relative.generic_string()) << ' ' << count << ' ' << std::quoted(sha)
                  << ' ' << std::quoted(imageRelative) << '\n';
        std::cout << "CONSTRAINT_LIFECYCLE_PHASE_FILE phase=" << std::quoted(phase)
                  << " source_relative=" << std::quoted(relative.generic_string())
                  << " image_relative=" << std::quoted(imageRelative) << " bytes=" << count << " sha256=" << sha << '\n';
    }
    inventory.close(); need(!inventory.fail(), "complete machine-readable phase inventory");
    std::cout << "CONSTRAINT_LIFECYCLE_PHASE_IMAGE phase=" << std::quoted(phase)
              << " source_relative=\"cluster\" image_relative=" << std::quoted(destination.lexically_relative(root).generic_string())
              << " manifest_relative=" << std::quoted(manifest.lexically_relative(root).generic_string())
              << " files=" << files.size() << " directories=" << directories.size() << " raw_bytes=" << totalBytes << '\n' << std::flush;
}

} // namespace
extern "C" char* realConstraintFetch(dbms::PageAllocator*,uint32_t) asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapConstraintFetch(dbms::PageAllocator*,uint32_t) asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapConstraintFetch(dbms::PageAllocator* allocator,uint32_t page) {
    if(armed)for(size_t i=0;i<watched.size();++i)if(allocator==watched[i])++watchedCounts[i];
    return realConstraintFetch(allocator,page);
}
int main(int argc,char** argv) {
    try {
        if(argc==4&&std::string(argv[1])=="--worker") {
            worker(argv[2],fs::canonical(argv[3]));
            std::cout<<"Tests run: "<<checks<<"\nTests failed: "<<failures<<'\n';return failures?1:0;
        }
        need(argc==1,"fixed standalone self-exec interface");std::string pattern=(fs::temp_directory_path()/"dbms-constraint-lifecycle-XXXXXX").string();
        std::vector<char> name(pattern.begin(),pattern.end());name.push_back(0);const auto* made=mkdtemp(name.data());need(made,"fresh unique owned experiment root");
        const auto root=fs::canonical(made);need(fs::create_directory(root/"cluster"),"fresh genuine worker cluster");
        std::cout<<"CONSTRAINT_LIFECYCLE_OWNED_ROOT "<<root<<'\n'<<std::flush;
        const auto exe=fs::canonical("/proc/self/exe");
        spawn(exe,root,"lifecycle");captureClosedPhase(root,"lifecycle");
        spawn(exe,root,"cold");captureClosedPhase(root,"cold");
        std::cout<<"CONSTRAINT_LIFECYCLE_COMPLETE workers=2 no_fixture_retry=1 phase_images=2 owned="<<root<<'\n';
        std::cout<<"Tests run: "<<checks<<"\nTests failed: "<<failures<<'\n';return failures?1:0;
    } catch(const std::exception& error) {
        if(!failures)++failures;std::cerr<<"CONSTRAINT_LIFECYCLE_FATAL "<<error.what()<<'\n';
        std::cout<<"Tests run: "<<checks<<"\nTests failed: "<<failures<<'\n';return 1;
    }
}
