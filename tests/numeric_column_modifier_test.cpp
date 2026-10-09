#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "utils/numeric_type.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    for (const auto& modifiers : {std::vector<std::string>{"5","2"},
         std::vector<std::string>{"3","-2"}, std::vector<std::string>{"2","4"},
         std::vector<std::string>{"1000","-1000"}, std::vector<std::string>{"1000","1000"}})
        assert(numeric_type_detail::unpack(numeric_type_detail::pack(modifiers)) == modifiers);
    assert(numeric_type_detail::pack({}) == -1);
    assert(numeric_type_detail::unpack(-1).empty());
    const auto db = testDbPath("numeric_column_modifier");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT PRIMARY KEY,v NUMERIC(5,2),n NUMERIC(3,-2),a NUMERIC(4,1)[],u NUMERIC)", session));
    const auto schema = g_engine.getTableSchema(db, "t");
    assert(schema.len == 5 && schema.cols[1].typeMod == (5 << 16) + 6 &&
        schema.cols[2].typeMod == (3 << 16) + 2050 &&
        schema.cols[3].typeMod == (4 << 16) + 5 && schema.cols[4].typeMod == -1);
    StorageEngine cold;
    const auto loaded = cold.getTableSchema(db, "t");
    assert(loaded.len == 5 && loaded.cols[1].typeMod == schema.cols[1].typeMod &&
        loaded.cols[2].typeMod == schema.cols[2].typeMod &&
        loaded.cols[3].typeMod == schema.cols[3].typeMod && loaded.cols[4].typeMod == -1);
    assert(g_engine.insert(db, "t", {{"id","1"},{"v","12.345"},{"n","12345"},{"a","{12.35,NULL,-12.35}"},{"u","12"}}) == DBStatus::OK);
    assert(g_engine.query(db,"t",{},{"v"},{}) == std::vector<std::string>{"12.35 "});
    assert(g_engine.query(db,"t",{},{"n"},{}) == std::vector<std::string>{"12300 "});
    assert(g_engine.query(db,"t",{},{"a"},{}) == std::vector<std::string>{"{12.4,NULL,-12.4} "});
    assert(g_engine.query(db,"t",{},{"u"},{}) == std::vector<std::string>{"12 "});
    assert(g_engine.update(db,"t",{{"v","10"},{"a","{1.25,NULL}"}},{"=id 1"}) == DBStatus::OK);
    assert(g_engine.query(db,"t",{},{"v"},{}) == std::vector<std::string>{"10.00 "});
    assert(g_engine.query(db,"t",{},{"a"},{}) == std::vector<std::string>{"{1.3,NULL} "});
    assert(g_engine.insert(db,"t",{{"id","2"},{"v","1000"}}) == DBStatus::INVALID_VALUE);
    assert(g_engine.update(db,"t",{{"v","1000"}},{"=id 1"}) == DBStatus::INVALID_VALUE);
    assert(g_engine.query(db,"t",{},{"v"},{}) == std::vector<std::string>{"10.00 "});
    assert(!ddl.executeSql("ALTER TABLE t ALTER COLUMN v TYPE NUMERIC(6,3)",session));
    assert(g_engine.query(db,"t",{},{"v"},{}) == std::vector<std::string>{"10.000 "});
    StorageEngine afterAlter;
    assert(afterAlter.getTableSchema(db,"t").cols[1].typeMod == (6 << 16) + 7);
    assert(!ddl.executeSql("CREATE TABLE old_numeric(v NUMERIC)",session));
    StorageEngine oldReader;
    assert(oldReader.getTableSchema(db,"old_numeric").cols[0].typeMod == -1);
    std::cout << "[NUMERIC COLUMN MODIFIER] native metadata/cold load/assignment/rewrite/unbounded passed\n";
}
