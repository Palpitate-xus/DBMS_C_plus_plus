#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <array>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
struct Checks {
    size_t controls=0,failures=0;
    template<class Action> void test(const std::string& label,Action action) {
        ++controls;
        try {action();}
        catch(const std::exception& error) {
            ++failures;std::cerr<<"CUSTOM_SCALAR_ROUTINE_DECLARATION_FAIL "<<controls<<' '<<label<<" : "<<error.what()<<'\n';
        }
        catch(...) {++failures;std::cerr<<"CUSTOM_SCALAR_ROUTINE_DECLARATION_FAIL "<<controls<<' '<<label<<" : nonstandard exception\n";}
    }
};
void require(bool condition,const std::string& message) {if(!condition)throw std::runtime_error(message);}
std::string quote(const std::string& name) {
    std::string result="\"";for(const char byte:name){result+=byte;if(byte=='"')result+=byte;}return result+'"';
}
std::string identity(const std::string& schema,const std::string& name) {return quote(schema)+"."+quote(name);}
struct DdlResult {bool failed=false;std::string state,output;};
DdlResult execute(DdlExecutor& ddl,Session& session,const std::string& sql) {
    DdlResult result;std::ostringstream output;
    {
        ScopedOutputCapture capture(output);
        try {result.failed=ddl.executeSql(sql,session);}
        catch(const DbError& error) {result.failed=true;result.state=error.sqlState();}
    }
    result.output=output.str();
    if(result.state.empty()) {
        const auto at=result.output.find("SQLSTATE ");
        if(at!=std::string::npos)result.state=result.output.substr(at+9,5);
    }
    return result;
}
void succeeds(DdlExecutor& ddl,Session& session,const std::string& sql) {
    const auto result=execute(ddl,session,sql);
    require(!result.failed,"DDL failed ["+result.state+"]: "+result.output);
}
void rejected(DdlExecutor& ddl,Session& session,const std::string& name,
    const std::string& sql,const std::string& expected) {
    const auto result=execute(ddl,session,sql);
    require(result.failed && result.state==expected,"expected SQLSTATE "+expected+", got "+result.state+": "+result.output);
    require(!g_engine.udfExists(session.currentDB,name,"public"),"rejected declaration installed a routine");
    require(!g_engine.inTransaction(),"rejected declaration leaked a transaction owner");
}
struct Kind {char key;std::string name,mixed,escaped;};
const std::array<Kind,3> kinds={{{'e',"e_kind","Mixed.E","Esc\"E"},
    {'d',"d_kind","Mixed.D","Esc\"D"},{'c',"c_kind","Mixed.C","Esc\"C"}}};
std::string typeDdl(const Kind& kind,const std::string& type) {
    if(kind.key=='e')return "CREATE TYPE "+type+" AS ENUM('red','blue')";
    if(kind.key=='d')return "CREATE DOMAIN "+type+" AS INTEGER";
    return "CREATE TYPE "+type+" AS (v INTEGER)";
}
struct Spelling {std::string sql,owner,path;};
std::vector<Spelling> spellings(const Kind& kind) {
    return {{kind.name,identity("public",kind.name),"public, pg_catalog"},
        {"public."+kind.name,identity("public",kind.name),"r164_b, public, pg_catalog"},
        {"r164_a."+kind.name,identity("r164_a",kind.name),"r164_b, public, pg_catalog"},
        {kind.name,identity("r164_a",kind.name),"r164_a, r164_b, public, pg_catalog"},
        {"r164_b."+kind.name,identity("r164_b",kind.name),"r164_a, public, pg_catalog"},
        {kind.name,identity("r164_b",kind.name),"r164_b, r164_a, public, pg_catalog"},
        {identity("R164.Scope",kind.mixed),identity("R164.Scope",kind.mixed),"public, pg_catalog"},
        {identity("R164.Scope",kind.escaped),identity("R164.Scope",kind.escaped),"public, pg_catalog"}};
}
enum Position {Argument,Result,Both};
std::string body(const std::string& language,Position position,const std::string& type) {
    if(language=="sql")return position==Argument?"SELECT 'ok'::text":
        position==Result?"SELECT NULL::"+type:"SELECT p";
    return position==Argument?"BEGIN RETURN 'ok'::text; END;":
        position==Result?"BEGIN RETURN NULL; END;":"BEGIN RETURN p; END;";
}
std::string declaration(const std::string& name,const std::string& language,Position position,
    const std::string& type,bool replace=false) {
    return std::string(replace?"CREATE OR REPLACE FUNCTION public.":"CREATE FUNCTION public.")+name+
        (position==Result?"()":"(p "+type+")")+" RETURNS "+(position==Argument?"TEXT":type)+
        " LANGUAGE "+language+" IMMUTABLE STRICT AS $$"+body(language,position,type)+"$$";
}
void metadata(const std::string& database,const std::string& name,const std::string& language,
    Position position,const std::string& owner) {
    const auto routine=g_engine.getUDF(database,name,"public");
    require(!routine.expression.empty(),"missing real stored routine");
    const std::vector<std::string> expected=position==Result?std::vector<std::string>{}:std::vector<std::string>{owner};
    require(routine.paramTypes==expected,"parameter declaration lost namespace identity");
    require(routine.returnType==(position==Argument?"text":owner),"return declaration lost namespace identity: "+routine.returnType);
    require(routine.language==language && routine.strict && routine.provolatile=='i',"routine attributes changed");
}
}

int main() {
    using namespace dbms;
    Checks checks;TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("custom_scalar_routine_declaration");
    Session session;session.username="admin";session.permission=1;session.currentDB=database;session.searchPath="public, pg_catalog";
    auto* prior=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{prior};
    DdlExecutor ddl;
    checks.test("private database",[&]{require(g_engine.createDatabase(database)==DBStatus::OK,"createDatabase failed");});
    for(const auto* schema:{"r164_a","r164_b","\"R164.Scope\""})
        checks.test(std::string("real schema ")+schema,[&]{succeeds(ddl,session,"CREATE SCHEMA "+std::string(schema));});
    for(const auto& kind:kinds) {
        for(const auto* schema:{"public","r164_a","r164_b"})
            checks.test("real scalar "+std::string(schema)+"."+kind.name,[&]{succeeds(ddl,session,typeDdl(kind,std::string(schema)+"."+kind.name));});
        for(const auto& name:{kind.mixed,kind.escaped})
            checks.test("real quoted scalar "+name,[&]{succeeds(ddl,session,typeDdl(kind,identity("R164.Scope",name)));});
    }
    checks.test("purity sequence",[&]{succeeds(ddl,session,"CREATE SEQUENCE public.r164_effects");});
    // Different kinds with the same basename prove namespace order is
    // considered before kind. No catalog identity is synthesized.
    checks.test("first namespace enum shadow",[&]{succeeds(ddl,session,"CREATE TYPE r164_a.r164_shadow AS ENUM('red','blue')");});
    checks.test("second namespace domain shadow",[&]{succeeds(ddl,session,"CREATE DOMAIN r164_b.r164_shadow AS INTEGER");});
    checks.test("public composite shadow",[&]{succeeds(ddl,session,"CREATE TYPE public.r164_shadow AS (v INTEGER)");});
    const auto beforeCatalog=g_engine.catalogService().metadataSnapshot(database);
    const auto beforeEnums=g_engine.getEnumTypeNames(database);
    const auto beforeDomains=g_engine.getDomainNames(database);
    const auto beforeComposites=g_engine.getCompositeTypeNames(database);

    // Full core: 3 kinds * 8 spellings * 2 languages * 3 positions = 144.
    // The second control checks actual stored declarations, not a guessed
    // result type. Composites intentionally keep their real .types owner;
    // adding their absent pg_type/value codec is outside this DDL repair.
    size_t core=0;
    for(const auto& kind:kinds)for(const auto& spelling:spellings(kind))
        for(const auto* language:{"sql","plpgsql"})for(const auto position:{Argument,Result,Both}) {
            const auto name="r164_"+std::to_string(core++);session.searchPath=spelling.path;
            checks.test("core DDL "+name,[&]{succeeds(ddl,session,declaration(name,language,position,spelling.sql));});
            checks.test("core actual owner "+name,[&]{metadata(database,name,language,position,spelling.owner);});
            if(position==Result)checks.test("pure noarg result declaration "+name,[&]{
                require(ExprHelper::inferResultType("public."+name+"()",{},database,&g_engine)==spelling.owner,"pure metadata selected another type owner");
            });
        }

    // Alias-equivalent OR REPLACE must preserve both input and output owner.
    size_t replacement=0;
    for(const auto& kind:kinds)for(const auto* language:{"sql","plpgsql"})for(size_t variant:{size_t(0),size_t(3),size_t(6)}) {
        const auto choices=spellings(kind);const auto& original=choices[variant];
        const auto index=static_cast<size_t>(&kind-&kinds[0])*48+variant*6+(std::string(language)=="sql"?0:3)+2;
        const auto name="r164_"+std::to_string(index);std::string alias;
        if(variant==0){session.searchPath="public, pg_catalog";alias="public."+kind.name;}
        else if(variant==3){session.searchPath="r164_b, public, pg_catalog";alias="r164_a."+kind.name;}
        else {session.searchPath="\"R164.Scope\", public, pg_catalog";alias=quote(kind.mixed);}
        checks.test("equivalent replacement "+std::to_string(replacement),[&]{succeeds(ddl,session,declaration(name,language,Both,alias,true));});
        checks.test("equivalent replacement owner "+std::to_string(replacement++),[&]{metadata(database,name,language,Both,original.owner);});
    }

    size_t ownerCase=0;
    for(const auto& kind:kinds)for(const auto* language:{"sql","plpgsql"}) {
        const auto name="r164_owner_"+std::to_string(ownerCase++);session.searchPath="r164_a, r164_b, public, pg_catalog";
        checks.test("initial bare result owner "+name,[&]{succeeds(ddl,session,declaration(name,language,Result,kind.name));});
        session.searchPath="r164_b, r164_a, public, pg_catalog";
        checks.test("qualified original owner survives path switch "+name,[&]{succeeds(ddl,session,declaration(name,language,Result,"r164_a."+kind.name,true));});
        const auto before=g_engine.getUDF(database,name,"public");
        checks.test("other return owner rejected "+name,[&]{
            const auto result=execute(ddl,session,declaration(name,language,Result,kind.name,true));
            require(result.failed && result.state=="42P13","different return owner was accepted: "+result.state);
            require(!g_engine.inTransaction(),"replacement rejection leaked transaction");
        });
        checks.test("rejected replacement preserved callable "+name,[&]{
            metadata(database,name,language,Result,identity("r164_a",kind.name));
            const auto after=g_engine.getUDF(database,name,"public");require(after.expression==before.expression,"failed replacement changed body");
        });
    }

    size_t invalid=0;session.searchPath="public, pg_catalog";
    for(const auto& kind:kinds)for(const auto* language:{"sql","plpgsql"})for(const auto position:{Argument,Result,Both})
        for(int which=0;which<4;++which) {
            const auto name="r164_invalid_"+std::to_string(invalid++);
            const auto type=which==0?"absent_"+kind.name:which==1?"public.absent_"+kind.name:
                which==2?"r164_absent_schema."+kind.name:"public."+quote(std::string(1,static_cast<char>(kind.key-'a'+'A'))+"_KIND");
            checks.test("invalid scalar declaration "+name,[&]{rejected(ddl,session,name,declaration(name,language,position,type),which==2?"3F000":"42704");});
        }
    // These local unsupported-boundary controls are not PG acceptance:
    // R151's array producer and user-array codecs remain explicitly deferred.
    size_t arrayGuard=0;
    for(const auto& kind:kinds)for(const auto* language:{"sql","plpgsql"})for(const auto position:{Argument,Result,Both}) {
        const auto name="r164_userarray_"+std::to_string(arrayGuard++);
        checks.test("user array remains unsupported "+name,[&]{rejected(ddl,session,name,declaration(name,language,position,"public."+kind.name+"[]"),"42704");});
    }
    size_t builtin=0;
    for(const auto* type:{"INTEGER[]","TEXT[]"})for(const auto* language:{"sql","plpgsql"})for(const auto position:{Argument,Result,Both}) {
        const auto name="r164_builtin_"+std::to_string(builtin++);const auto owner=std::string(type)=="INTEGER[]"?"integer[]":"text[]";
        checks.test("existing builtin array DDL "+name,[&]{succeeds(ddl,session,declaration(name,language,position,type));});
        checks.test("existing builtin array metadata "+name,[&]{metadata(database,name,language,position,owner);});
    }
    for(const auto* language:{"sql","plpgsql"}) {
        const auto name="r164_void_"+std::string(language);
        checks.test("void result remains supported "+name,[&]{succeeds(ddl,session,declaration(name,language,Result,"VOID"));});
        checks.test("void result metadata "+name,[&]{metadata(database,name,language,Result,"void");});
        const auto input="r164_void_arg_"+std::string(language);
        checks.test("void argument remains unsupported "+input,[&]{rejected(ddl,session,input,declaration(input,language,Argument,"VOID"),"42704");});
        for(const auto position:{Argument,Result}) {
            const auto record="r164_record_"+std::string(language)+std::to_string(position);
            // RECORD is registered as Composite in this project's builtin
            // registry. Preserve that existing declaration policy; neither
            // this guard nor R164 claims full PostgreSQL RECORD execution.
            checks.test("existing builtin RECORD declaration policy "+record,[&]{
                succeeds(ddl,session,declaration(record,language,position,"RECORD"));
                metadata(database,record,language,position,"record");
            });
        }
    }
    size_t shadow=0;
    for(const auto* schema:{"r164_a","r164_b","public"}) {
        const auto name="r164_shadow_"+std::to_string(shadow++);session.searchPath=std::string(schema)+", public, r164_b, r164_a, pg_catalog";
        checks.test("namespace before kind "+name,[&]{succeeds(ddl,session,declaration(name,"sql",Both,"r164_shadow"));});
        checks.test("namespace before kind actual owner "+name,[&]{metadata(database,name,"sql",Both,identity(schema,"r164_shadow"));});
    }

    const auto coldDatabase=testDbPath("custom_scalar_routine_declaration_cold");
    checks.test("cold real database",[&]{require(g_engine.createDatabase(coldDatabase)==DBStatus::OK,"cold createDatabase failed");});
    session.currentDB=coldDatabase;session.searchPath="public, pg_catalog";
    checks.test("cold real sidecar composite DDL",[&]{succeeds(ddl,session,"CREATE TYPE public.r164_cold AS (v INTEGER)");});
    checks.test("cold composite has no fabricated catalog",[&]{require(!g_engine.catalogService().has(coldDatabase),"composite DDL bootstrapped catalog");require(g_engine.isCompositeType(coldDatabase,"r164_cold"),"missing real composite sidecar");});
    checks.test("cold qualified scalar composite routine",[&]{succeeds(ddl,session,declaration("r164_cold_routine","sql",Both,"public.r164_cold"));});
    checks.test("cold routine actual identity without bootstrap",[&]{metadata(coldDatabase,"r164_cold_routine","sql",Both,identity("public","r164_cold"));require(!g_engine.catalogService().has(coldDatabase) && !g_engine.inTransaction(),"declaration fabricated catalog or leaked owner");});
    checks.test("cold database cleanup",[&]{require(g_engine.dropDatabase(coldDatabase)==DBStatus::OK,"cold dropDatabase failed");});
    session.currentDB=database;session.searchPath="public, pg_catalog";

    size_t effect=0;
    for(const auto& kind:kinds)for(const auto* language:{"sql","plpgsql"}) {
        const auto name="r164_effect_"+std::to_string(effect++);
        const auto effectBody=std::string(language)=="sql"?"SELECT nextval('public.r164_effects')::text":
            "BEGIN PERFORM nextval('public.r164_effects'); RETURN 'effect'::text; END;";
        const auto sql="CREATE FUNCTION public."+name+"(p public."+kind.name+") RETURNS TEXT LANGUAGE "+language+" VOLATILE AS $$"+effectBody+"$$";
        checks.test("effect body stored lazily "+name,[&]{succeeds(ddl,session,sql);});
        checks.test("effect metadata remains pure "+name,[&]{
            FunctionCallExpr call;call.schema="public";call.funcName=name;
            auto parameter=std::make_unique<ParameterExpr>();parameter->declaredType=identity("public",kind.name);parameter->origin=ParameterOrigin::MetadataPlaceholder;call.args.push_back(std::move(parameter));
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            require(evaluator.hasScalarFunction(&call,&g_engine) && evaluator.scalarFunctionResultType(&call,&g_engine)=="text" && evaluator.scalarFunctionVolatility(&call,&g_engine)=='v',"metadata lost actual routine declaration");
        });
    }
    checks.test("DDL and metadata never execute stored bodies",[&]{require(g_engine.nextval(database,"r164_effects")==1,"DDL/metadata executed a function body");});
    checks.test("routine DDL does not manufacture type identities",[&]{
        const auto after=g_engine.catalogService().metadataSnapshot(database);
        require(after.types.size()==beforeCatalog.types.size() && after.enumLabels.size()==beforeCatalog.enumLabels.size(),"routine declaration changed type catalog");
        require(g_engine.getEnumTypeNames(database)==beforeEnums && g_engine.getDomainNames(database)==beforeDomains && g_engine.getCompositeTypeNames(database)==beforeComposites,"routine declaration changed real scalar metadata");
    });
    checks.test("private database cleanup",[&]{require(g_engine.dropDatabase(database)==DBStatus::OK,"dropDatabase failed");});
    // Setup23 + core144*2 + noarg metadata48 + equivalent replacement18*2
    // + result-owner6*4 + missing72 + userarray18 + builtin arrays12*2
    // + pseudo policy10 + cross-kind3*2 + cold6 + effect13 + no type writes1
    // + cleanup1 = 570. Preserve every control even after setup failures.
    constexpr size_t expected=570;
    if(checks.controls!=expected){++checks.failures;std::cerr<<"CUSTOM_SCALAR_ROUTINE_DECLARATION_INVENTORY expected "<<expected<<" got "<<checks.controls<<'\n';}
    std::cout<<"CUSTOM_SCALAR_ROUTINE_DECLARATION_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expected<<" FAILURES "<<checks.failures<<'\n';
    return checks.failures?1:0;
}
