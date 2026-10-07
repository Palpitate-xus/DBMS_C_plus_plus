#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "access/HashIndex.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>

extern dbms::StorageEngine g_engine;

namespace {
void checkKey(const std::string& db, const std::string& value,
              const std::vector<std::string>& ids) {
    using namespace dbms;
    const auto* index = g_engine.getHashIndex(db, "ranks", "r");
    assert(index);
    const auto rids = index->search(value);
    std::vector<std::string> actual;
    const auto schema = g_engine.getTableSchema(db, "ranks");
    assert(g_engine.forEachRow(db, "ranks",
        [&](uint32_t page, uint16_t slot, const char* bytes, size_t length) {
            const auto rid = StorageEngine::encodeRid(page, slot);
            if (std::find(rids.begin(),rids.end(),rid) == rids.end()) return;
            assert(!g_engine.isColumnNullByRid(db, "ranks", rid, 1));
            const std::string row(bytes,length);
            assert(g_engine.extractColumnValue(row,schema,1,db) == value);
            actual.push_back(g_engine.extractColumnValue(row,schema,0,db));
        }));
    auto expected = ids; std::sort(expected.begin(),expected.end());
    std::sort(actual.begin(),actual.end());
    std::cout << "ENUM_HASH_KEY value=" << value << " raw_rids=" << rids.size()
              << " visible_ids=";
    for (const auto& id:actual) std::cout << id << ',';
    std::cout << " expected_ids=";
    for (const auto& id:expected) std::cout << id << ',';
    std::cout << std::endl;
    assert(actual == expected && rids.size() == expected.size());
}
}

int main() {
    using namespace dbms;
    using SqlRow = StorageEngine::SqlRow;
    TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("enum_empty_hash_key");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session; session.username="testuser"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TYPE rank_type AS ENUM ('zeta','','alpha','NULL','it''s')", session));
    assert(!ddl.executeSql("CREATE TABLE ranks (id INT PRIMARY KEY, r rank_type)", session));
    assert((g_engine.getTableSchema(db,"ranks").cols[1].enumValues ==
            std::vector<std::string>{"zeta","","alpha","NULL","it's"}));
    assert(g_engine.insertRow(db, "ranks", {{"id", "1"}, {"r", "zeta"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "2"}, {"r", ""}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "3"}, {"r", "alpha"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "4"}, {"r", "NULL"}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", std::map<std::string, std::optional<std::string>>{{"id", "5"}, {"r", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insertRow(db, "ranks", {{"id", "6"}, {"r", "it's"}}) == DBStatus::OK);
    assert(g_engine.createHashIndex(db, "ranks", "r") == DBStatus::OK);
    const auto* index = g_engine.getHashIndex(db, "ranks", "r");
    assert(index);
    std::cout << "ENUM_EMPTY_HASH_ACTUAL_RIDS=" << index->search("").size() << std::endl;
    // Original exact raw index assertion, never replaced with SQL fallback.
    assert(index->search("").size() == 1);
    checkKey(db,"",{"2"}); checkKey(db,"NULL",{"4"});
    assert(g_engine.insertRow(db,"ranks",{{"id","7"},{"r",""}}) == DBStatus::OK);
    checkKey(db,"",{"2","7"});
    assert(g_engine.update(db,"ranks",{{"r",""}},{"=id 1"}) == DBStatus::OK);
    checkKey(db,"",{"1","2","7"}); checkKey(db,"zeta",{});
    assert(g_engine.updateRows(db,"ranks",std::map<std::string,std::optional<std::string>>{{"r",std::nullopt}},{"=id 2"}) == DBStatus::OK);
    checkKey(db,"",{"1","7"});
    assert(g_engine.update(db,"ranks",{{"r",""}},{"=id 5"}) == DBStatus::OK);
    checkKey(db,"",{"1","5","7"});
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.savepoint("hash_empty_undo") == DBStatus::OK);
    assert(g_engine.insertRow(db,"ranks",{{"id","8"},{"r",""}}) == DBStatus::OK);
    assert(g_engine.update(db,"ranks",{{"r","alpha"}},{"=id 7"}) == DBStatus::OK);
    assert(g_engine.remove(db,"ranks",{"=id 1"}) == DBStatus::OK);
    checkKey(db,"",{"5","8"});
    assert(g_engine.rollbackToSavepoint("hash_empty_undo") == DBStatus::OK);
    checkKey(db,"",{"1","5","7"}); checkKey(db,"alpha",{"3"});
    assert(g_engine.insertRow(db,"ranks",{{"id","9"},{"r",""}}) == DBStatus::OK);
    assert(g_engine.update(db,"ranks",{{"r","alpha"}},{"=id 5"}) == DBStatus::OK);
    assert(g_engine.remove(db,"ranks",{"=id 7"}) == DBStatus::OK);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    checkKey(db,"",{"1","5","7"}); checkKey(db,"alpha",{"3"});
    assert(g_engine.remove(db,"ranks",{"=id 7"}) == DBStatus::OK);
    checkKey(db,"",{"1","5"});
    {
        StorageEngine reopened;
        const auto* cold = reopened.getHashIndex(db, "ranks", "r");
        assert(cold && cold->search("").size() == 2);
    }
    // Actual historical hash files omitted empty keys. Upgrade a real such
    // key/RID omission through unchanged-value UPDATE, without pretending a
    // missing nonempty key is valid or trusting this index for empty reads.
    auto* legacy = g_engine.getHashIndex(db,"ranks","r");
    for (const auto rid : legacy->search("")) assert(legacy->remove("",rid));
    assert(legacy->flush() && legacy->search("").empty());
    assert(g_engine.update(db,"ranks",{{"r",""}},{"=id 1"}) == DBStatus::OK);
    checkKey(db,"",{"1"});
    assert(g_engine.update(db,"ranks",{{"r",""}},{"=id 5"}) == DBStatus::OK);
    checkKey(db,"",{"1","5"});
    // A physical rewrite must repopulate the real empty key at the new RIDs.
    assert(g_engine.vacuumFull(db,"ranks") > 0);
    checkKey(db,"",{"1","5"}); checkKey(db,"alpha",{"3"});
    auto renamed = g_engine.getEnumType(db,"rank_type");
    renamed.labels[1] = "blank";
    assert(g_engine.updateEnumType(db,renamed) == DBStatus::OK);
    checkKey(db,"",{}); checkKey(db,"blank",{"1","5"});
    {
        StorageEngine reopened;
        const auto* cold = reopened.getHashIndex(db,"ranks","r");
        assert(cold && cold->search("").empty() && cold->search("blank").size() == 2);
        assert(reopened.getTableSchema(db,"ranks").cols[1].enumValues == renamed.labels);
    }
    // The shared hash consumer must also distinguish an ordinary empty TEXT
    // datum from SQL NULL, without altering its nonempty key encoding.
    TableSchema text; text.tablename="text_control";
    text.append(makeIntColumn("id",false,4,true));
    text.append(makeTextColumn("txt",true));
    assert(g_engine.createTable(db,text) == DBStatus::OK);
    assert(g_engine.insertRow(db,"text_control",{{"id","1"},{"txt",""}}) == DBStatus::OK);
    assert(g_engine.insertRow(db,"text_control",SqlRow{{"id","2"},{"txt",std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insertRow(db,"text_control",{{"id","3"},{"txt","NULL"}}) == DBStatus::OK);
    assert(g_engine.createHashIndex(db,"text_control","txt") == DBStatus::OK);
    auto textCount = [&] {
        const auto* key = g_engine.getHashIndex(db,"text_control","txt");
        assert(key && key->search("NULL").size() == 1);
        return key->search("").size();
    };
    assert(textCount() == 1);
    assert(g_engine.updateRows(db,"text_control",SqlRow{{"txt",""}},{"=id 2"}) == DBStatus::OK);
    assert(textCount() == 2);
    assert(g_engine.updateRows(db,"text_control",SqlRow{{"txt",std::nullopt}},{"=id 1"}) == DBStatus::OK);
    assert(textCount() == 1);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insertRow(db,"text_control",SqlRow{{"id","4"},{"txt",""}}) == DBStatus::OK);
    assert(g_engine.insertRow(db,"text_control",SqlRow{{"id","5"},{"txt",std::nullopt}}) == DBStatus::OK);
    assert(textCount() == 2);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(textCount() == 1);
    // A shared real empty-key bucket must not be mistaken for any NULL
    // row's OLD index image. Keep the exact RID bucket unchanged through
    // unchanged-NULL UPDATE and DELETE with many unrelated empty values.
    for(int i=100;i<164;++i)
        assert(g_engine.insertRow(db,"text_control",SqlRow{{"id",std::to_string(i)},{"txt",""}}) == DBStatus::OK);
    for(int i=200;i<216;++i)
        assert(g_engine.insertRow(db,"text_control",SqlRow{{"id",std::to_string(i)},{"txt",std::nullopt}}) == DBStatus::OK);
    const auto emptyBefore=g_engine.getHashIndex(db,"text_control","txt")->search("");
    assert(emptyBefore.size()==65);
    for(int i=200;i<216;++i) {
        assert(g_engine.updateRows(db,"text_control",SqlRow{{"txt",std::nullopt}},{"=id "+std::to_string(i)}) == DBStatus::OK);
        assert(g_engine.remove(db,"text_control",{"=id "+std::to_string(i)}) == DBStatus::OK);
    }
    assert(g_engine.getHashIndex(db,"text_control","txt")->search("")==emptyBefore);
    std::cout << "[ENUM EMPTY HASH KEY] exact physical empty key/NULL, INSERT/UPDATE/DELETE, savepoint/full rollback, vacuum/native rename and cold storage passed\n";
}
