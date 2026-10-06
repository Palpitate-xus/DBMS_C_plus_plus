#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <set>

extern dbms::StorageEngine g_engine;

int main() {
    using Engine = dbms::StorageEngine;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "join_range_identity";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE ranges(id INT,payload TEXT)", session));
    const std::vector<std::optional<std::string>> payloads{
        std::string("a b"), std::nullopt, std::string("NULL"), std::string{}};
    for (size_t i = 0; i < payloads.size(); ++i)
        assert(g_engine.insertRow(db, "ranges", {{"id", std::to_string(i + 1)},
            {"payload", payloads[i]}}) == dbms::DBStatus::OK);
    const Engine::JoinRangeNames labels{"L", "R"};
    const std::set<std::string> keys{"L.id", "R.id"};
    std::vector<std::vector<std::string>> cells;
    std::vector<std::vector<bool>> nulls;
    auto rows = g_engine.crossJoin(db, "ranges", "ranges", {"=L.id 1", "=R.id 2"},
                                   keys, &cells, &nulls, labels);
    assert(rows == std::vector<std::string>{"1 2 "});
    assert(cells == (std::vector<std::vector<std::string>>{{"1", "2"}}));
    assert(nulls == (std::vector<std::vector<bool>>{{false, false}}));
    rows = g_engine.join(db, "ranges", "ranges", "id", "id", {"=L.id 1"},
                         keys, &cells, &nulls, {}, labels);
    assert(rows == std::vector<std::string>{"1 1 "});
    const std::vector<std::string> on{"=L.id 1"};
    rows = g_engine.leftJoin(db, "ranges", "ranges", "id", "id", {}, keys,
                             &cells, &nulls, on, labels);
    assert(rows == (std::vector<std::string>{"1 1 ", "2 NULL ", "3 NULL ", "4 NULL "}));
    assert(nulls[1] == (std::vector<bool>{false, true}));
    rows = g_engine.rightJoin(db, "ranges", "ranges", "id", "id", {}, keys,
                              &cells, &nulls, on, labels);
    assert(rows == (std::vector<std::string>{"1 1 ", "NULL 2 ", "NULL 3 ", "NULL 4 "}));
    assert(nulls[1] == (std::vector<bool>{true, false}));
    rows = g_engine.fullOuterJoin(db, "ranges", "ranges", "id", "id", {}, keys,
                                  &cells, &nulls, on, labels);
    assert(rows == (std::vector<std::string>{"1 1 ", "2 NULL ", "3 NULL ", "4 NULL ",
                                           "NULL 2 ", "NULL 3 ", "NULL 4 "}));
    const std::set<std::string> texts{"L.payload", "R.payload"};
    g_engine.crossJoin(db, "ranges", "ranges", {"=L.id 1", "=R.id 2"},
                       texts, &cells, &nulls, labels);
    assert(cells == (std::vector<std::vector<std::string>>{{"a b", ""}}));
    assert(nulls == (std::vector<std::vector<bool>>{{false, true}}));
    g_engine.crossJoin(db, "ranges", "ranges", {"=L.id 3", "=R.id 4"},
                       texts, &cells, &nulls, labels);
    assert(cells == (std::vector<std::vector<std::string>>{{"NULL", ""}}));
    assert(nulls == (std::vector<std::vector<bool>>{{false, false}}));
    // Non-equality INNER fallback must forward range names into CROSS.
    rows = g_engine.join(db, "ranges", "ranges", "", "", {}, keys,
                         &cells, &nulls, {"<L.id R.id"}, labels);
    assert(rows.size() == 6);
    for (const auto& row : cells) assert(std::stoi(row[0]) < std::stoi(row[1]));
    assert(!ddl.executeSql("CREATE TABLE range_bag(id INT,payload TEXT)", session));
    for (const auto& text : {"left", "left", "right"})
        assert(g_engine.insert(db, "range_bag", {{"id", "1"}, {"payload", text}}) == dbms::DBStatus::OK);
    g_engine.fullOuterJoin(db, "range_bag", "range_bag", "id", "id", {}, texts,
                           &cells, &nulls, {}, labels);
    std::multiset<std::vector<std::string>> expected;
    for (const auto& left : {"left", "left", "right"})
        for (const auto& right : {"left", "left", "right"}) expected.insert({left, right});
    assert(std::multiset<std::vector<std::string>>(cells.begin(), cells.end()) == expected);
    assert(!ddl.executeSql("CREATE TABLE range_literals(id INT,payload TEXT)", session));
    assert(g_engine.insert(db, "range_literals", {{"id", "1"}, {"payload", "L.id"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "range_literals", {{"id", "2"}, {"payload", "R.id"}}) == dbms::DBStatus::OK);
    g_engine.crossJoin(db, "range_literals", "range_literals",
                       {"=L.payload 'L.id'", "=R.payload 'R.id'"}, texts, &cells, &nulls, labels);
    assert(cells == (std::vector<std::vector<std::string>>{{"L.id", "R.id"}}));
    g_engine.crossJoin(db, "ranges", "ranges", {"=L.payload NULL"}, texts, &cells, &nulls, labels);
    assert(cells.empty());
    g_engine.crossJoin(db, "ranges", "ranges", {"=L.payload 'NULL'", "=R.id 2"}, texts, &cells, &nulls, labels);
    assert(cells == (std::vector<std::vector<std::string>>{{"NULL", ""}}));
    assert(nulls == (std::vector<std::vector<bool>>{{false, true}}));
    const Engine::JoinRangeNames encodedLabels{"L", "R", true};
    const auto encoded = [](const std::string& side, const std::string& column) {
        return Engine::joinRangeColumnKey(side, column, true);
    };
    assert(encoded("L", "id") != encoded("L", "_6964"));
    assert(!ddl.executeSql("CREATE TABLE range_delimited(id INT,\"Value Name\" INT,\"a\"\"b\" INT)", session));
    assert(g_engine.insertRow(db, "range_delimited", {{"id", "1"}, {"Value Name", "11"}, {"a\"b", "111"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, "range_delimited", {{"id", "2"}, {"Value Name", "22"}, {"a\"b", "222"}}) == dbms::DBStatus::OK);
    g_engine.crossJoin(db, "range_delimited", "range_delimited",
                       {"=" + encoded("L", "Value Name") + " 11", "=" + encoded("R", "a\"b") + " 222"},
                       {encoded("L", "id"), encoded("R", "id")}, &cells, &nulls, encodedLabels);
    assert(cells == (std::vector<std::vector<std::string>>{{"1", "2"}}));
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[JOIN RANGE IDENTITY] passed\n";
}
