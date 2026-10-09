#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const std::string name="group_having_column_identity";
    const std::string db=testDbPath(name);
    assert(owner.createDatabase(db,"utf8")==DBStatus::OK);
    for (const auto& columnName : {"id", "ID", "value key", "x.y", "a\"b", "x>y"}) {
        TableSchema table;
        table.tablename="items";
        table.append(makeIntColumn(columnName,true,4));
        assert(owner.createTable(db,table)==DBStatus::OK);
        for (const auto& value : {std::optional<std::string>{"1"},
                                  std::optional<std::string>{"2"},std::optional<std::string>{}})
            assert(owner.insertRow(db,"items",{{columnName,value}})==DBStatus::OK);
        std::string quoted="\"";
        for (char c : std::string(columnName)) { quoted+=c;if(c=='"')quoted+='"'; }
        quoted+='"';
        const auto check=[&](const std::string& predicate,size_t rows,const std::string& prefix) {
            const auto actual=owner.groupAggregate(db,"items",{},{},{columnName},{predicate});
            std::cout << "GROUP_HAVING_IDENTITY " << predicate << " actual=" << actual.size() << '\n';
            assert(actual.size()==rows);
            if (rows) assert(actual.front()==prefix+" ");
        };
        check(quoted+"=1",1,"1");
        check(quoted+">1",1,"2");
        check(quoted+"<>1",1,"2");
        check(quoted+"=NULL",0,"");
        assert(owner.dropTable(db,"items")==DBStatus::OK);
    }
    assert(owner.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[GROUP HAVING COLUMN IDENTITY] passed\n";
}
