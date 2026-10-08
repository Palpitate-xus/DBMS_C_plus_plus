#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t checked=0,failed=0;
    std::cout<<std::unitbuf;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;
        std::cout<<"DECLARED_TYPE_CONSTANT "<<role<<" pass="<<pass<<'\n';
    };
    struct Case {std::string sql,type,value,catalogName;};
    const std::vector<Case> cases={
        {"VARBIT 'b01'","bit varying","01","varbit"},
        {"BIT VARYING 'b01'","bit varying","01","varbit"},
        {"pg_catalog.varbit 'b01'","bit varying","01","varbit"},
        {"\"pg_catalog\".\"varbit\"(2) 'b111'","bit varying","11","varbit"},
        {"\"varbit\" 'b01'","bit varying","01","varbit"},
        {"VARBIT(4) 'b01'","bit varying","01","varbit"},
        {"BIT 'b01'","bit","01","bit"},
        {"BIT ''","bit","","bit"},
        {"BIT(4) 'b01'","bit","0100","bit"},
        {"VARBIT 'X0aF'","bit varying","000010101111","varbit"},
        {"CAST('b01' AS pg_catalog.varbit)","bit varying","01","varbit"},
        {"'b111'::\"pg_catalog\".\"varbit\"(2)","bit varying","11","varbit"},
        {"TEXT 'it''s VARBIT ''b01'''","text","it's VARBIT 'b01'","text"},
        {"pg_catalog.text 'it''s'","text","it's","text"},
        {"INTEGER '12'","integer","12","int4"},
        {"NUMERIC(4,2) '1.2'","numeric","1.20","numeric"},
        {"UUID '12345678-1234-1234-1234-123456789012'","uuid","12345678-1234-1234-1234-123456789012","uuid"}
    };
    SQLParser parser;
    ExprEvaluator evaluator;
    for(const auto& item:cases) {
        auto parsed=parser.parse("SELECT "+item.sql+" AS result");
        auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
        try {
            const auto value=select?evaluator.eval(select->selectList.at(0).expr.get(),RowContext{}):ExprValue{};
            require(select && !value.isNull && value.typeName==item.type && value.value==item.value,"runtime "+item.sql);
        } catch(const DbError& error) {require(false,"runtime "+item.sql+" "+error.sqlState());}
        const auto legacy=ExprHelper::evalString(item.sql,{},{});
        require(legacy.ok && !legacy.isNull && legacy.value==item.value,"stored-expression "+item.sql);
    }
    const std::vector<std::pair<std::string,std::string>> errors={
        {"not_a_type 'x'","42704"},{"not_a_namespace.varbit 'b01'","3F000"},
        {"\"VARBIT\" 'b01'","42704"},{"CAST('x' AS not_a_type)","42704"},
        {"'x'::not_a_type","42704"},{"VARBIT 'b02'","22P02"},
        {"VARBIT 'xg'","22P02"},{"VARBIT(0) 'b01'","22023"},
        {"VARBIT(-2) 'b01'","22023"},{"VARBIT(2,3) 'b01'","22023"},
        {"VARBIT(+2) 'b01'","42601"}
    };
    for(const auto& item:errors) {
        bool good=false;
        try {
            auto parsed=parser.parse("SELECT "+item.first);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(select)(void)evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
        } catch(const DbError& error) {good=error.sqlState()==item.second;}
        require(good,"input-error "+item.first);
    }
    // Real production bootstrap owns the OIDs. Preparation is metadata-only;
    // no guessed user type/OID or value-dependent descriptor is installed.
    const auto database=testDbPath("declared_type_constants");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    // createDatabase alone creates storage. Use the actual production catalog
    // initialization path before testing copied, read-only type metadata.
    (void)g_engine.catalogService().get(database);
    Session session;session.username="testuser";session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{previous};
    const auto catalog=g_engine.catalogService().metadataSnapshot(database);
    for(const auto& item:cases) {
        try {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+item.sql+" AS result WHERE false");
            Oid expected=INVALID_OID;
            for(const auto& type:catalog.types)
                if(type.typnamespace==11 && type.typname==item.catalogName)expected=type.oid;
            require(query.output.size()==1 && expected && query.output[0].type==item.type &&
                query.output[0].typeOid==expected,"pure catalog descriptor "+item.sql);
        } catch(const DbError& error) {require(false,"pure catalog descriptor "+item.sql+" "+error.sqlState());}
    }
    for(const auto& item:errors) {
        if(item.second!="42704" && item.second!="3F000" && item.second!="42601")continue;
        bool good=false;
        try {(void)g_engine.prepareBoundQuery(database,"SELECT "+item.first+" WHERE false");}
        catch(const DbError& error) {good=error.sqlState()==item.second;}
        require(good,"empty-source type-error "+item.first);
    }
    std::cout<<"DECLARED_TYPE_CONSTANT_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return failed?1:0;
}
