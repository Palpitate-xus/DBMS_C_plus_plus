#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); StorageEngine engine;
    const std::string name="prepared_primitive_assignment_input"; cleanupTestDb(name);
    const auto db=testDbPath(name); assert(engine.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema schema; schema.len=1;
    schema.cols[0].dataName="id"; schema.cols[0].dataType="int"; schema.cols[0].dsize=4;
    assert(engine.createTable(db,"assignment_rows",schema)==DBStatus::OK);
    assert(engine.insert(db,"assignment_rows",{{"id","1"}})==DBStatus::OK);
    assert(engine.createSequence(db,"assignment_effects",1,1)==DBStatus::OK);
    assert(engine.createUDF(db,"assignment_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('assignment_effects'); RETURN p; END;",'v',"plpgsql","integer")==DBStatus::OK);
    const std::vector<std::pair<std::string,std::string>> cases={
        {"INSERT INTO assignment_rows VALUES('bad')","22P02"},
        {"INSERT INTO assignment_rows VALUES('2147483648')","22003"},
        {"INSERT INTO assignment_rows VALUES('bad'),(assignment_writer(2))","22P02"},
        {"INSERT INTO assignment_rows VALUES('bad',missing_assignment_function(1))","42883"},
        {"INSERT INTO assignment_rows VALUES('bad') RETURNING missing_assignment_function(1)","22P02"},
        {"INSERT INTO assignment_rows SELECT 'bad' WHERE false","22P02"},
        {"INSERT INTO assignment_rows SELECT 'bad' WHERE missing_assignment_function(1)=1","42883"},
        {"UPDATE assignment_rows SET id='bad' WHERE false","22P02"},
        {"UPDATE assignment_rows SET id='2147483648' WHERE false","22003"},
        {"UPDATE assignment_rows SET id='bad' WHERE missing_assignment_function(1)=1","42883"},
        {"UPDATE assignment_rows SET id='bad' RETURNING missing_assignment_function(1)","42883"},
        {"UPDATE assignment_rows SET id='bad' WHERE 1","42804"},
        {"UPDATE assignment_rows SET id=assignment_writer(2) WHERE 'bad'","22P02"},
        {"WITH ins AS(INSERT INTO assignment_rows VALUES(assignment_writer(2)) RETURNING id) UPDATE assignment_rows SET id='bad' WHERE false","22P02"},
        {"UPDATE assignment_rows SET id='2' WHERE false",""},
        {"UPDATE assignment_rows SET id=NULL WHERE false",""},
        {"INSERT INTO assignment_rows VALUES('2')",""},
        {"INSERT INTO assignment_rows SELECT '2' WHERE false",""},
        {"UPDATE assignment_rows SET id=CAST(2147483648 AS INT) WHERE false",""}, // no numeric narrowing in analysis
        {"UPDATE assignment_rows SET id=1/0 WHERE false",""}, // no arithmetic in analysis
    };
    size_t failures=0;
    for(const auto& control:cases) {
        std::string actual;
        try{(void)engine.prepareBoundQuery(db,control.first);}
        catch(const DbError& error){actual=error.sqlState();}
        std::cout<<"PRIMITIVE ASSIGNMENT "<<control.first<<" actual="<<actual<<" expected="<<control.second<<std::endl;
        if(actual!=control.second)++failures;
    }
    assert(engine.nextval(db,"assignment_effects")==1); // metadata only, never invoke writers
    assert(engine.query(db,"assignment_rows",{}, {"id"})==std::vector<std::string>{"1 "});
    assert(failures==0);
    cleanupTestDb(name);
    std::cout<<"[PREPARED PRIMITIVE ASSIGNMENT INPUT] passed\n";
}
