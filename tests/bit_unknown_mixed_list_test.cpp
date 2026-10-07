#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <memory>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto database=testDbPath("bit_unknown_mixed_list");
    assert(owner.createDatabase(database,"utf8")==DBStatus::OK);
    size_t controls=0;
    const auto cursor=[&](const std::string& sql,const std::vector<QueryBindingDatum>& datums,
                          std::optional<bool> expected) {
        auto prepared=std::make_shared<PreparedQuery>(owner.prepareBoundQuery(database,sql,datums));
        auto rows=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(
            &owner,database,prepared,prepared->ast.get()),prepared->output);
        std::vector<ExprValue> row;
        assert(rows->next(row) && row.size()==1 && row[0].typeName=="boolean");
        assert(row[0].isNull==!expected.has_value() && (!expected || row[0].asBool()==*expected));
        assert(!rows->next(row));rows->close();
    };
    for(const auto& input:std::vector<std::pair<std::string,std::optional<bool>>>{
        {"'0'",false},{"'1'",true},{"NULL",{}},{"'2'",{}},{"''",{}},
        {"'01'",true},{"'001'",true},{"'11'",false}}) {
        const bool invalid=input.first=="'2'" || input.first=="''";
        for(const auto& list:{"B'01',1","1,B'01'","B'01',1,NULL","NULL,1,B'01'"})
            for(const auto& operation:{"IN","NOT IN"}) {
                const std::string expression=input.first+" "+operation+"("+list+")";
                auto expected=input.second;
                if(expected && !*expected && std::string(list).find("NULL")!=std::string::npos)expected.reset();
                if(expected && std::string(operation)=="NOT IN")expected=!*expected;
                const auto value=ExprHelper::evalString(expression,{});
                assert(value.ok==!invalid);
                if(invalid)assert(value.sqlState=="22P02");
                else assert(value.typeName=="boolean" && value.isNull==!expected &&
                    (!expected || ExprValue(value.typeName,value.value).asBool()==*expected));
                ++controls;
                bool rejected=false;
                try {cursor("SELECT "+expression+" AS value",{},expected);}
                catch(const DbError& error){rejected=true;assert(invalid && error.sqlState()=="22P02");}
                assert(rejected==invalid);++controls;
            }
    }
    for(const auto& source:{"1","01","001","0001"})
        for(const auto& bit:std::vector<std::pair<std::string,std::string>>{{"B'1'","1"},{"B'01'","01"},{"X'1'","0001"}})
            for(const auto& list:{bit.first+",0","0,"+bit.first})
                for(const auto& operation:{"IN","NOT IN"}) {
                    const bool matches=std::string(source)==bit.second;
                    const bool expected=std::string(operation)=="IN"?matches:!matches;
                    const std::string expression="'"+std::string(source)+"' "+operation+"("+list+")";
                    const auto value=ExprHelper::evalString(expression,{});
                    assert(value.ok && value.typeName=="boolean" && !value.isNull && ExprValue(value.typeName,value.value).asBool()==expected);
                    ++controls;cursor("SELECT "+expression,{},expected);++controls;
                }
    for(const auto& list:{"B'01',1","1,B'01'"}) {
        bool rejected=false;
        try {(void)owner.prepareBoundQuery(database,std::string("SELECT $1 IN(")+list+")",
            {{"mixed-list-parameter","needle","unknown",{},true,"1",1}});}
        catch(const DbError& error){rejected=true;assert(error.sqlState()=="42P08");}
        assert(rejected);++controls;
    }
    for(const auto& control:std::vector<std::pair<std::string,std::optional<bool>>>{
        {"1",false},{"01",true},{"",false}}) {
        cursor("SELECT $1 IN(B'01','01') AS value",{{"homogeneous-list-parameter","needle","unknown",{},true,control.first,1}},control.second);
        ++controls;
    }
    cursor("SELECT $1 IN(NULL,B'01') AS value",{{"null-list-parameter","needle","unknown",{},true,std::nullopt,1}},{});++controls;
    TableSchema sink;sink.tablename="calls";sink.append(makeIntColumn("id",false,4));
    assert(owner.createTable(database,sink)==DBStatus::OK);
    TableSchema empty;empty.tablename="empty_bits";empty.append(makeIntColumn("id",false,4));
    Column bit;bit.dataName="v";bit.isNull=true;
    assert(TypeRegistry::instance().resolveColumnType(bit,"varbit",{},false).empty());empty.append(bit);
    assert(owner.createTable(database,empty)==DBStatus::OK);
    assert(owner.createUDF(database,"mixed_list_writer",{"p"},{"varbit"},
        "BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","varbit")==DBStatus::OK);
    const auto calls=[&] {return owner.query(database,"calls",{}, {"id"},{}).size();};
    for(const auto& sql:{"SELECT mixed_list_writer(B'01') IN(B'01',1)",
                         "SELECT id FROM empty_bits WHERE mixed_list_writer(v) IN(B'01',1)"}) {
        bool rejected=false;
        try {(void)owner.prepareBoundQuery(database,sql);}
        catch(const DbError& error){rejected=true;assert(error.sqlState()=="42883");}
        assert(rejected && calls()==0);++controls;
    }
    size_t total=0;
    for(const auto& control:std::vector<std::pair<std::string,std::optional<bool>>>{
        {"B'0'",false},{"B'01'",true},{"NULL::varbit",{}}}) {
        const auto sql="SELECT mixed_list_writer("+control.first+") IN(B'1','01') AS value";
        auto prepared=owner.prepareBoundQuery(database,sql);
        assert(calls()==total); // metadata preparation never invokes the writer
        cursor(sql,{},control.second);++total;
        assert(calls()==total);++controls;
    }
    assert(owner.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[BIT UNKNOWN MIXED LIST] all "<<controls<<" real helper/binder/cursor/parameter/once/NULL/early-error controls passed\n";
}
