#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const std::string name="legacy_scalar_routine_arguments";
    const std::string db=testDbPath(name);
    assert(owner.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema table;
    table.tablename="rows";
    table.append(makeIntColumn("id",true,8));
    table.cols[0].dataType="bigint";
    table.append(makeTextColumn("txt",true));
    assert(owner.createTable(db,table)==DBStatus::OK);
    assert(owner.insertRow(db,"rows",{{"id","2147483648"},{"txt","a b"}})==DBStatus::OK);
    assert(owner.insertRow(db,"rows",{{"id",std::nullopt},{"txt",std::nullopt}})==DBStatus::OK);
    assert(owner.createUDF(db,"LegacyCalc",{"x"},{"bigint"},"SELECT x*2",'i',"sql","bigint",false)==DBStatus::OK);
    assert(owner.createUDF(db,"legacy_echo",{"x"},{"text"},"SELECT x",'i',"sql","text")==DBStatus::OK);
    const auto check=[&](const std::string& function,const std::string& argument,
                         const std::string& value,bool null) {
        StorageEngine::SelectExpr expression;
        expression.isScalar=true;
        expression.funcName=function;
        expression.funcArgs={argument};
        std::vector<std::vector<std::string>> cells;
        std::vector<std::vector<bool>> nulls;
        const auto rows=owner.queryExpr(db,"rows",{"isnotnull id"},{expression},{},&cells,&nulls);
        assert(rows.size()==1 && cells.size()==1 && nulls.size()==1 &&
            cells.front().size()==1 && nulls.front().size()==1);
        assert(nulls.front().front()==null);
        if (!null) assert(cells.front().front()==value);
    };
    check("LegacyCalc","id+1","4294967298",false);
    check("LegacyCalc","NULL::bigint","",true);
    check("legacy_echo","txt","a b",false);
    check("legacy_echo","'NULL'","NULL",false);
    check("legacy_echo","''","",false);
    check("legacy_echo","'O''Brien'","O'Brien",false);
    StorageEngine::SelectExpr expression;
    expression.isScalar=true;
    expression.funcName="legacy_echo";
    expression.funcArgs={"txt"};
    std::vector<std::vector<std::string>> cells;
    std::vector<std::vector<bool>> nulls;
    owner.queryExpr(db,"rows",{"isnull id"},{expression},{},&cells,&nulls);
    assert(cells.size()==1 && nulls.size()==1 && nulls.front().front());
    assert(owner.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[LEGACY SCALAR ROUTINE ARGUMENTS] passed\n";
}
