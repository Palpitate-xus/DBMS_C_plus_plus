#include "BPTree.h"
#include "Config.h"
#include "TableManage.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

dbms::Config g_config;

using namespace dbms;

namespace {

void assertCount(BPTree* index, const std::string& key, size_t expected) {
    assert(index != nullptr);
    const size_t actual = index->searchMulti(key).size();
    if (actual != expected) {
        std::cerr << "index key '" << key << "': expected " << expected
                  << ", got " << actual << '\n';
    }
    assert(actual == expected);
}

} // namespace

int main() {
    const std::string dbname = "partial_index_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    StorageEngine engine;
    assert(engine.createDatabase(dbname) == DBStatus::OK);

    TableSchema table;
    table.tablename = "accounts";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("email", false, 64, false));
    table.append(makeBooleanColumn("active", false, false));
    table.append(makeVarCharColumn("tenant", false, 32, false));
    assert(engine.createTable(dbname, table) == DBStatus::OK);

    assert(engine.insert(dbname, "accounts",
                         {{"id", "1"}, {"email", "same@example.test"},
                          {"active", "true"}, {"tenant", "a"}}) ==
           DBStatus::OK);
    assert(engine.insert(dbname, "accounts",
                         {{"id", "2"}, {"email", "same@example.test"},
                          {"active", "false"}, {"tenant", "b"}}) ==
           DBStatus::OK);

    assert(engine.createIndex(dbname, "accounts", "email", true, {},
                              "active = true") == DBStatus::OK);
    BPTree* emailIndex = engine.getSecondaryIndex(
        dbname, "accounts", "email");
    assertCount(emailIndex, "same@example.test", 1);

    assert(engine.insert(dbname, "accounts",
                         {{"id", "3"}, {"email", "same@example.test"},
                          {"active", "false"}, {"tenant", "c"}}) ==
           DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);
    assert(engine.insert(dbname, "accounts",
                         {{"id", "4"}, {"email", "same@example.test"},
                          {"active", "true"}, {"tenant", "d"}}) ==
           DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 2);

    // A query that does not imply the partial predicate must not use the
    // subset as though it were a complete index.
    assert(engine.query(dbname, "accounts", {"=email same@example.test"},
                        {"id"}).size() == 4);
    assert(engine.query(dbname, "accounts",
                        {"=email same@example.test", "=active true"},
                        {"id"}).size() == 2);

    assert(engine.update(dbname, "accounts", {{"active", "true"}},
                         {"=id 2"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 3);
    assert(engine.update(dbname, "accounts", {{"active", "false"}},
                         {"=id 1"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 2);
    assert(engine.update(dbname, "accounts", {{"email", "moved@example.test"}},
                         {"=id 2"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);
    assertCount(emailIndex, "moved@example.test", 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.update(dbname, "accounts", {{"active", "false"}},
                         {"=id 2"}) == DBStatus::OK);
    assertCount(emailIndex, "moved@example.test", 0);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(emailIndex, "moved@example.test", 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.savepoint("before_partial_change") == DBStatus::OK);
    assert(engine.update(dbname, "accounts", {{"active", "false"}},
                         {"=id 4"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 0);
    assert(engine.rollbackToSavepoint("before_partial_change") == DBStatus::OK);
    assert(engine.commitTransaction() == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.remove(dbname, "accounts", {"=id 4"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 0);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);

    // Rollback must not restore a key for an OLD row that was outside the
    // predicate, nor for a deleted non-member.
    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.update(dbname, "accounts", {{"active", "true"}},
                         {"=id 3"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 2);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.savepoint("before_entering_predicate") == DBStatus::OK);
    assert(engine.update(dbname, "accounts", {{"active", "true"}},
                         {"=id 3"}) == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 2);
    assert(engine.rollbackToSavepoint("before_entering_predicate") ==
           DBStatus::OK);
    assert(engine.commitTransaction() == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.remove(dbname, "accounts", {"=id 3"}) == DBStatus::OK);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.insert(dbname, "accounts",
                         {{"id", "5"}, {"email", "same@example.test"},
                          {"active", "false"}, {"tenant", "e"}}) ==
           DBStatus::OK);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(emailIndex, "same@example.test", 1);

    // INCLUDE used to hide the WHERE clause from composite metadata parsing.
    assert(engine.createCompositeIndex(
               dbname, "accounts", {"tenant", "email"}, "tenant_email_active",
               {"id"}, "active = true") == DBStatus::OK);
    const auto composites = engine.getCompositeIndexes(dbname, "accounts");
    bool foundComposite = false;
    for (const auto& composite : composites) {
        if (composite.name != "tenant_email_active") continue;
        foundComposite = true;
        assert(composite.columns.size() == 2);
        assert(composite.columns[0] == "tenant");
        assert(composite.columns[1] == "email");
        assert(composite.whereCondition == "active = true");
    }
    assert(foundComposite);
    BPTree* compositeIndex = engine.getCompositeIndexTree(
        dbname, "accounts", "tenant_email_active");
    assertCount(compositeIndex, std::string("b\x01moved@example.test", 20), 1);
    assertCount(compositeIndex, std::string("a\x01same@example.test", 19), 0);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.update(dbname, "accounts", {{"active", "true"}},
                         {"=id 3"}) == DBStatus::OK);
    assertCount(compositeIndex, std::string("c\x01same@example.test", 19), 1);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(compositeIndex, std::string("c\x01same@example.test", 19), 0);

    assert(engine.createIndex(dbname, "accounts", "email", true, {},
                              "active = true", "UPPER(email)") == DBStatus::OK);
    BPTree* expressionIndex = engine.getSecondaryIndex(
        dbname, "accounts", "UPPER(email)");
    assertCount(expressionIndex, "SAME@EXAMPLE.TEST", 1);
    assertCount(expressionIndex, "MOVED@EXAMPLE.TEST", 1);
    assert(engine.query(dbname, "accounts",
                        {"=UPPER(email) SAME@EXAMPLE.TEST"}, {"id"}).size() == 3);
    assert(engine.query(dbname, "accounts",
                        {"=UPPER(email) SAME@EXAMPLE.TEST", "=active true"},
                        {"id"}).size() == 1);

    assert(engine.beginTransaction(dbname) == DBStatus::OK);
    assert(engine.update(dbname, "accounts", {{"active", "true"}},
                         {"=id 3"}) == DBStatus::OK);
    assertCount(expressionIndex, "SAME@EXAMPLE.TEST", 2);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertCount(expressionIndex, "SAME@EXAMPLE.TEST", 1);

    assert(engine.createIndex(dbname, "accounts", "tenant", true, {},
                              "active =") == DBStatus::INVALID_VALUE);

    assert(engine.reindex(dbname, "accounts") == DBStatus::OK);
    emailIndex = engine.getSecondaryIndex(dbname, "accounts", "email");
    assertCount(emailIndex, "same@example.test", 1);
    assertCount(emailIndex, "moved@example.test", 1);
    expressionIndex = engine.getSecondaryIndex(
        dbname, "accounts", "UPPER(email)");
    assertCount(expressionIndex, "SAME@EXAMPLE.TEST", 1);
    assertCount(expressionIndex, "MOVED@EXAMPLE.TEST", 1);

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[PARTIAL INDEX] all passed\n";
    return 0;
}
