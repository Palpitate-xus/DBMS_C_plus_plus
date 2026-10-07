#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/SqlArrayText.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    std::vector<std::string> failures;
    const auto check=[&](const std::string& name,bool good){if(!good){failures.push_back(name);std::cerr<<"ARRAY_BOUNDS_NATIVE_FAILURE "<<name<<'\n';}};
    const std::vector<std::pair<std::string,std::string>> valid={
        {"[0:2]={1,NULL,3}","[0:2]={1,NULL,3}"},
        {"[-2:-1][3:4]={{1,NULL},{3,4}}","[-2:-1][3:4]={{1,NULL},{3,4}}"},
        {"[+01:+02]={01,02}","{01,02}"},
        {"{{},{}}","{}"},
        {"[-2147483648:-2147483648]={1}","[-2147483648:-2147483648]={1}"},
        {"[2147483646:2147483646]={1}","[2147483646:2147483646]={1}"},
        {"[0:3]={\"NULL\",\"\",NULL,\"a,b\"}","[0:3]={\"NULL\",\"\",NULL,\"a,b\"}"},
    };
    for(const auto& control:valid) {
        try{check("parsed envelope "+control.first,sql_array_text::render(sql_array_text::parse(control.first))==control.second);}
        catch(const DbError& error){check("valid parser SQLSTATE "+error.sqlState(),false);}
    }
    const std::vector<std::pair<std::string,std::string>> invalid={
        {"[0:1]={1}","22P02"},{"[0:1][2:3]={1,2}","22P02"},{"[2:1]={}","2202E"},
        {"[2147483648:2147483648]={1}","54000"},{"[2147483647:2147483647]={1}","54000"},
        {"[-2147483648:0]={1}","54000"},{"[0 :1]={1,2}","22P02"},{"[0:1]{1,2}","22P02"},
        {"[0:1]={{1,2},{3}}","22P02"},{"[1][1][1][1][1][1][1]={{{{{{{1}}}}}}}","54000"},
    };
    for(const auto& control:invalid) {
        std::string state;try{(void)sql_array_text::parse(control.first);}catch(const DbError& error){state=error.sqlState();}
        check("invalid bounds "+control.first,state==control.second);
    }
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"'[0:2]={1,NULL,3}'::INT[]","[0:2]={1,NULL,3}"},
        {"'[-2:-1][3:4]={{1,NULL},{3,4}}'::INT[]","[-2:-1][3:4]={{1,NULL},{3,4}}"},
        {"array_dims('[-2:-1][3:4]={{1,NULL},{3,4}}'::INT[])","[-2:-1][3:4]"},
        {"array_lower('[0:2]={1,NULL,3}'::INT[],1)","0"},
        {"array_upper('[0:2]={1,NULL,3}'::INT[],1)","2"},
        {"('[0:2]={1,NULL,3}'::INT[])[0]","1"},
        {"('[-2:-1][3:4]={{1,NULL},{3,4}}'::INT[])[-1][4]","4"},
        {"('[0:2]={1,NULL,3}'::INT[])[0:1]","{1,NULL}"},
        {"array_position('[0:2]={1,NULL,3}'::INT[],NULL)","1"},
        {"array_append('[0:2]={1,NULL,3}'::INT[],4)","[0:3]={1,NULL,3,4}"},
        {"array_prepend(4,'[0:2]={1,NULL,3}'::INT[])","[0:3]={4,1,NULL,3}"},
        {"'[0:1]={1,2}'::INT[]||'[5:6]={3,4}'::INT[]","[0:3]={1,2,3,4}"},
        {"ARRAY['[0:1]={1,2}'::INT[],'[0:1]={3,4}'::INT[]]","[1:2][0:1]={{1,2},{3,4}}"},
        {"'[0:1]={abcd,éééé}'::VARCHAR(3)[]","[0:1]={abc,ééé}"},
        {"'[0:1]={a,é}'::CHAR(3)[]","[0:1]={\"a  \",\"é  \"}"},
    }) {
        const auto value=ExprHelper::evalString(control.first,{});
        check("real evaluator "+control.first,value.ok && !value.isNull && value.value==control.second);
    }
    for(const auto* expression:{"('[0:2]={1,NULL,3}'::INT[])[1]","('[0:2]={1,NULL,3}'::INT[])[-1]","('[-2:-1][3:4]={{1,NULL},{3,4}}'::INT[])[-2]"}) {
        const auto value=ExprHelper::evalString(expression,{});check(std::string("actual element NULL ")+expression,value.ok && value.isNull && value.typeName=="integer");
    }
    const auto database=testDbPath("explicit_array_bounds");
    std::map<std::string,std::optional<std::string>> expected;
    {
        StorageEngine engine;assert(engine.createDatabase(database,"utf8")==DBStatus::OK);
        TableSchema schema;schema.tablename="target";schema.append(makeIntColumn("id",false,2,true));
        auto array=makeIntColumn("a",true,2);array.isArray=true;schema.append(array);
        assert(engine.createTable(database,schema)==DBStatus::OK);
        check("actual native integer array type",engine.getTableSchema(database,"target").cols[1].dataType=="int" && engine.getTableSchema(database,"target").cols[1].isArray);
        const std::vector<std::optional<std::string>> values={"[0:2]={1,NULL,3}","[-2:-1][3:4]={{1,NULL},{3,4}}",std::nullopt,"{}"};
        for(size_t i=0;i<values.size();++i) {
            const std::string id=std::to_string(i+1);const auto status=engine.insertRow(database,"target",{{"id",id},{"a",values[i]}});
            check("actual native bounded insert "+id,status==DBStatus::OK);expected[id]=values[i];
        }
        assert(engine.createIndex(database,"target","a")==DBStatus::OK);
        for(const auto& control:invalid)check("native rejects without leaking owner "+control.first,
            engine.insertRow(database,"target",{{"id","99"},{"a",control.first}})==DBStatus::INVALID_VALUE && !engine.inTransaction());
        check("native element overflow atomic",engine.insertRow(database,"target",{{"id","99"},{"a","[0:1]={1,2147483648}"}})==DBStatus::INVALID_VALUE);
        auto bigint=makeIntColumn("a",true,3);bigint.isArray=true;
        check("native actual rewrite",engine.alterTableAlterColumnType(database,"target","a",bigint)==DBStatus::OK);
        check("native rewritten type metadata",engine.getTableSchema(database,"target").cols[1].dataType=="bigint" && engine.getTableSchema(database,"target").cols[1].isArray);
    }
    {
        StorageEngine reader;
        std::vector<std::vector<std::string>> rows;std::vector<std::vector<bool>> nulls;
        (void)reader.query(database,"target",{},{"a","id"},{{"id",true}},false,false,false,0,{},&rows,&nulls);
        check("cold complete rows",rows.size()==expected.size() && nulls.size()==expected.size());
        for(size_t i=0;i<rows.size();++i) {
            check("cold lower bounds values NULL roundtrip "+rows[i][0],expected.count(rows[i][0]) &&
                nulls[i][1]==!expected.at(rows[i][0]) && rows[i][1]==expected.at(rows[i][0]).value_or(""));
        }
        std::vector<std::vector<std::string>> indexedRows;std::vector<std::vector<bool>> indexedNulls;
        (void)reader.query(database,"target",{"=a [0:2]={1,NULL,3}"},{"a","id"},{},false,false,false,0,{},&indexedRows,&indexedNulls);
        check("cold indexed exact array value",indexedRows==std::vector<std::vector<std::string>>({{"1","[0:2]={1,NULL,3}"}}) && indexedNulls==std::vector<std::vector<bool>>({{false,false}}));
        assert(reader.dropDatabase(database)==DBStatus::OK);
    }
    std::cout<<"ARRAY_BOUNDS_NATIVE_FAILURE_COUNT="<<failures.size()<<std::endl;assert(failures.empty());
}
