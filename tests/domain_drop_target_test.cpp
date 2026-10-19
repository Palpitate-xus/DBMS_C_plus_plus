#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
struct Checks {
    size_t controls=0,failures=0;
    template<class Action> void test(const std::string& label,Action action) {
        ++controls;
        try {action();}
        catch(const std::exception& error) {
            ++failures;std::cerr<<"DOMAIN_DROP_TARGET_FAIL "<<controls<<' '<<label<<" : "<<error.what()<<'\n';
        }
        catch(...) {++failures;std::cerr<<"DOMAIN_DROP_TARGET_FAIL "<<controls<<' '<<label<<" : nonstandard exception\n";}
    }
};
void require(bool value,const std::string& message) {if(!value)throw std::runtime_error(message);}
std::string quote(const std::string& name) {
    std::string result="\"";for(char byte:name){result+=byte;if(byte=='"')result+=byte;}return result+'"';
}
std::string identity(const std::string& schema,const std::string& name) {return quote(schema)+"."+quote(name);}
struct Result {bool failed=false;std::string state,output;};
template<class Action> Result capture(Action action) {
    Result result;std::ostringstream output;
    {
        ScopedOutputCapture capture(output);
        try {result.failed=action();}
        catch(const DbError& error){result.failed=true;result.state=error.sqlState();}
    }
    result.output=output.str();
    if(result.state.empty()) {
        const auto at=result.output.find("SQLSTATE ");
        if(at!=std::string::npos)result.state=result.output.substr(at+9,5);
    }
    return result;
}
Result execute(DdlExecutor& ddl,Session& session,const std::string& sql) {
    return capture([&]{return ddl.executeSql(sql,session);});
}
void success(DdlExecutor& ddl,Session& session,const std::string& sql) {
    const auto result=execute(ddl,session,sql);
    require(!result.failed,"DDL failed ["+result.state+"]: "+result.output);
    require(!g_engine.inTransaction(),"successful statement leaked transaction owner");
}
void state(const Result& result,const std::string& wanted) {
    require(result.failed && result.state==wanted,"expected "+wanted+", got "+result.state+": "+result.output);
}
std::vector<std::string> malformed(const std::string& target,const std::string& other,
    const std::string& schema,const std::string& name) {
    // PG18 Parse-only evidence accepts unquoted cascade/restrict/if names;
    // standalone exists is reserved and is a syntax error.
    return {"DROP DOMAIN","DROP DOMAIN IF EXISTS","DROP DOMAIN ,"+target,
        "DROP DOMAIN "+target+",","DROP DOMAIN "+target+",,"+other,
        "DROP DOMAIN "+target+" "+other,"DROP DOMAIN "+schema+".",
        "DROP DOMAIN ."+name,"DROP DOMAIN "+schema+".."+name,
        "DROP DOMAIN "+target+".extra","DROP DOMAIN "+schema+".,"+other,
        "DROP DOMAIN "+target+" CASCADE RESTRICT","DROP DOMAIN "+target+" RESTRICT CASCADE",
        "DROP DOMAIN "+target+" CASCADE CASCADE","DROP DOMAIN "+target+" RESTRICT RESTRICT",
        "DROP DOMAIN "+target+" CASCADE garbage","DROP DOMAIN "+target+" RESTRICT garbage",
        "DROP DOMAIN "+target+" CASCADE,"+other,"DROP DOMAIN "+target+"()",
        "DROP DOMAIN "+target+"[]","DROP DOMAIN "+target+"=1","DROP DOMAIN 'not_an_identifier'",
        "DROP DOMAIN 7","DROP DOMAIN \"\"","DROP DOMAIN "+target+",,",
        "DROP DOMAIN "+target+" RESTRICT "+other,"DROP DOMAIN IF EXISTS "+target+",",
        "DROP DOMAIN IF EXISTS ,"+target,"DROP DOMAIN IF EXISTS "+schema+".",
        "DROP DOMAIN "+target+" IF EXISTS","DROP DOMAIN "+target+"; garbage",
        "DROP DOMAIN "+target+" CASCADE; garbage","DROP DOMAIN exists"};
}
struct ParsedCase {std::string sql;std::vector<std::pair<std::string,std::string>> names;bool ifExists=false,cascade=false;};
void isolatedParserEntry(Checks& checks) {
    // First executed test entry: no registry bootstrap, current session,
    // catalog creation, database/global-engine calls, or global cleanup.
    const std::vector<ParsedCase> valid={
        {"DROP DOMAIN d",{{"","d"}}},
        {"DROP DOMAIN PUBLIC.Name",{{"public","name"}}},
        {"DROP DOMAIN public . name",{{"public","name"}}},
        {"DROP DOMAIN \"public\".\"Mixed\"",{{"public","Mixed"}}},
        {"DROP DOMAIN \"S.X\".\"D.Y\"",{{"S.X","D.Y"}}},
        {"DROP DOMAIN \"S\"\"Q\".\"D\"\"Q\"",{{"S\"Q","D\"Q"}}},
        {"DROP DOMAIN \"域\".\"型\"",{{"域","型"}}},
        {"DROP DOMAIN \"123\"",{{"","123"}}},
        {"DROP DOMAIN \"cascade\"",{{"","cascade"}}},
        {"DROP DOMAIN \"restrict\"",{{"","restrict"}}},
        {"DROP DOMAIN IF EXISTS public.d RESTRICT",{{"public","d"}},true,false},
        {"DROP DOMAIN IF EXISTS \"S.X\".\"D.Y\" CASCADE",{{"S.X","D.Y"}},true,true},
        {"DROP DOMAIN a,b",{{"","a"},{"","b"}}},
        {"DROP DOMAIN \"S.X\".a,public.b CASCADE",{{"S.X","a"},{"public","b"}},false,true},
        {"DROP DOMAIN public /* gap */ . d ;",{{"public","d"}}},
        {"DROP DOMAIN cascade",{{"","cascade"}}},
        {"DROP DOMAIN restrict",{{"","restrict"}}},
        {"DROP DOMAIN if",{{"","if"}}},
        {"DROP DOMAIN cascade.d",{{"cascade","d"}}},
        {"DROP DOMAIN if.d",{{"if","d"}}}};
    for(const auto& item:valid)checks.test("isolated parse "+item.sql,[&]{
        for(bool binding:{false,true}) {
            auto parsed=binding?SQLParser().parseForBinding(item.sql):SQLParser().parse(item.sql);
            const auto* drop=parsed.isValid()?dynamic_cast<const DropStmt*>(parsed.stmt.get()):nullptr;
            require(drop && drop->command==SqlCommand::DropDomain && drop->objectType=="DOMAIN","wrong/missing DROP DOMAIN AST");
            require(drop->objectNames.size()==item.names.size(),"qualified target split into tokens or list lost");
            require(drop->ifExists==item.ifExists && drop->cascade==item.cascade,"options moved into targets");
            for(size_t index=0;index<item.names.size();++index) {
                CatalogManager::QualifiedName decoded;
                require(CatalogManager::parseQualifiedName(drop->objectNames[index],decoded,true),"target cannot be decoded");
                require(decoded.schema==item.names[index].first && decoded.name==item.names[index].second,"quoted target components changed");
            }
        }
    });
    for(const auto& sql:malformed("public.guard","other.guard","public","guard"))
        checks.test("isolated malformed "+sql,[&]{
            // PG accepts these target spellings at Parse, then rejects
            // the cross-database/non-domain identity during execution.
            if (sql=="DROP DOMAIN public.guard.extra" || sql=="DROP DOMAIN public.guard[]") {
                require(SQLParser().parse(sql).isValid() && SQLParser().parseForBinding(sql).isValid(),"semantic target rejected as syntax");
                return;
            }
            require(!SQLParser().parse(sql).isValid() && !SQLParser().parseForBinding(sql).isValid(),"malformed target/tail accepted");
        });
}
std::string domainBytes(const std::string& database) {
    std::ifstream input(g_engine.dbPath(database)/".domains",std::ios::binary);
    require(bool(input),"actual .domains metadata missing");
    return {std::istreambuf_iterator<char>(input),std::istreambuf_iterator<char>()};
}
}

int main() {
    using namespace dbms;
    Checks checks;
    isolatedParserEntry(checks);
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("domain_drop_target_"+std::to_string(static_cast<long long>(getpid())));
    Session session;session.username="admin";session.permission=1;session.currentDB=database;session.searchPath="public,pg_catalog";
    auto* prior=currentSession();setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{prior};
    DdlExecutor ddl;
    checks.test("private test database",[&]{require(g_engine.createDatabase(database)==DBStatus::OK,"createDatabase failed");});
    for(const auto& schema:{"r167_first","r167_second","R167.Scope","R167\"Quote"})
        checks.test("schema "+std::string(schema),[&]{success(ddl,session,"CREATE SCHEMA "+quote(schema));});
    for(const auto& schema:{"public","r167_first","r167_second"})
        checks.test("same-name guard "+std::string(schema),[&]{success(ddl,session,"CREATE DOMAIN "+identity(schema,"shared")+" AS INTEGER DEFAULT 7");});
    for(const auto& name:{"list_a","list_b"})checks.test("list guard "+std::string(name),[&]{success(ddl,session,"CREATE DOMAIN public."+std::string(name)+" AS INTEGER");});

    struct Target {std::string schema,name,spelling,path;};
    const std::vector<Target> targets={
        {"public","bare_native","bare_native","public,pg_catalog"},
        {"public","qualified_native","PUBLIC.QUALIFIED_NATIVE","r167_first,public,pg_catalog"},
        {"public","spaced_native","public . spaced_native","r167_second,public,pg_catalog"},
        {"r167_first","private_native","r167_first.private_native","r167_second,public,pg_catalog"},
        {"R167.Scope","Mixed.Type",identity("R167.Scope","Mixed.Type"),"public,pg_catalog"},
        {"R167\"Quote","D\"Quote",identity("R167\"Quote","D\"Quote"),"public,pg_catalog"},
        {"public","Dot.Name",identity("public","Dot.Name"),"public,pg_catalog"},
        {"r167_first","cascade",identity("r167_first","cascade"),"public,pg_catalog"},
        {"r167_second","restrict",identity("r167_second","restrict"),"public,pg_catalog"},
        {"R167.Scope","123",identity("R167.Scope","123"),"public,pg_catalog"},
        {"r167_first","cascade","cascade","r167_first,public,pg_catalog"},
        {"r167_second","restrict","restrict","r167_second,public,pg_catalog"},
        {"r167_first","if","if","r167_first,public,pg_catalog"}};
    for(size_t index=0;index<targets.size();++index) {
        const auto& target=targets[index];const auto canonical=identity(target.schema,target.name);session.searchPath=target.path;
        checks.test("real domain "+canonical,[&]{success(ddl,session,"CREATE DOMAIN "+canonical+" AS INTEGER DEFAULT 19");});
        checks.test("actual existing target "+canonical,[&]{require(g_engine.getDomain(database,canonical).defaultValue=="19","actual target missing before DROP");});
        const auto options=index%3==0?"":index%3==1?" RESTRICT":" CASCADE";
        checks.test("drop decoded target "+canonical,[&]{success(ddl,session,std::string("DROP DOMAIN ")+(index%2?"IF EXISTS ":"")+target.spelling+options);});
        checks.test("actual missing target "+canonical,[&]{
            require(g_engine.getDomain(database,canonical).name.empty(),"DROP did not remove actual target");
            state(execute(ddl,session,"DROP DOMAIN "+canonical),"42704");require(!g_engine.inTransaction(),"missing target leaked owner");
        });
        checks.test("IF EXISTS missing target "+canonical,[&]{success(ddl,session,"DROP DOMAIN IF EXISTS "+canonical+options);});
    }

    session.searchPath="public,r167_first,r167_second,pg_catalog";
    const auto unchanged=[&](const std::string& bytes){
        require(domainBytes(database)==bytes,"rejected DROP changed actual domain records");
        for(const auto& schema:{"public","r167_first","r167_second"})
            require(g_engine.getDomain(database,identity(schema,"shared")).defaultValue=="7","rejected DROP lost same-name guard");
        require(!g_engine.inTransaction(),"rejected DROP leaked transaction owner");
    };
    for(const auto& sql:malformed("public.shared","r167_first.shared","public","shared"))
        checks.test("malformed actual SQL "+sql,[&]{
            const auto bytes=domainBytes(database);
            const std::string wanted=sql=="DROP DOMAIN public.shared.extra"?"0A000":
                sql=="DROP DOMAIN public.shared[]"?"42809":"42601";
            state(execute(ddl,session,sql),wanted);unchanged(bytes);
        });

    // Product policy only: PostgreSQL supports an atomic multi-target DROP.
    // Until that owner exists here, reject before the first target mutates.
    const std::vector<std::string> multiple={"DROP DOMAIN public.list_a,public.list_b",
        "DROP DOMAIN public.list_a , public.list_b RESTRICT","DROP DOMAIN public.list_a,public.list_b CASCADE",
        "DROP DOMAIN IF EXISTS public.list_a,public.list_b","DROP DOMAIN public.absent,public.list_a",
        "DROP DOMAIN IF EXISTS public.list_a,public.absent CASCADE"};
    for(const auto& sql:multiple)checks.test("local multi-target policy "+sql,[&]{
        const auto bytes=domainBytes(database);state(execute(ddl,session,sql),"0A000");unchanged(bytes);
    });
    for(bool ifExists:{false,true})checks.test("forged multi-target AST "+std::to_string(ifExists),[&]{
        const auto bytes=domainBytes(database);StmtPtr stmt=std::make_unique<DropStmt>(SqlCommand::DropDomain);
        auto* drop=static_cast<DropStmt*>(stmt.get());drop->objectType="DOMAIN";drop->objectNames={"public.list_a","public.list_b"};drop->ifExists=ifExists;
        state(capture([&]{return ddl.execute(stmt,session);}),"0A000");unchanged(bytes);
    });
    for(const auto& target:{"absent","public.absent","r167_first.absent","\"R167.Scope\".\"mixed.type\""})
        checks.test("actual absent object "+std::string(target),[&]{const auto bytes=domainBytes(database);state(execute(ddl,session,"DROP DOMAIN "+std::string(target)),"42704");unchanged(bytes);});
    for(const auto& suffix:{"absent","public.absent RESTRICT","r167_first.absent CASCADE","\"R167.Scope\".\"mixed.type\""})
        checks.test("missing IF EXISTS "+std::string(suffix),[&]{const auto bytes=domainBytes(database);success(ddl,session,"DROP DOMAIN IF EXISTS "+std::string(suffix));unchanged(bytes);});

    const auto retained=[&](const std::string& absent) {
        for(const auto& schema:{"public","r167_first","r167_second"})
            require(g_engine.getDomain(database,identity(schema,"shared")).name.empty()==(schema==absent),"DROP selected a different namespace owner");
    };
    for(const auto& owner:{"r167_first","r167_second","public","r167_first"}) {
        session.searchPath=std::string(owner)+",r167_second,r167_first,public,pg_catalog";
        const std::string spelling=std::string(owner)=="public"?"public.shared":std::string(owner)=="r167_first"?"shared":"r167_second.shared";
        checks.test("actual canonical owner "+std::string(owner),[&]{success(ddl,session,"DROP DOMAIN "+spelling);retained(owner);});
        checks.test("owner recovery "+std::string(owner),[&]{success(ddl,session,"CREATE DOMAIN "+identity(owner,"shared")+" AS INTEGER DEFAULT 7");});
    }
    for(const auto& names:std::vector<std::vector<std::string>>{{},{"public."},{"public..shared"}})
        checks.test("forged malformed target AST "+std::to_string(names.size()),[&]{
            const auto bytes=domainBytes(database);StmtPtr stmt=std::make_unique<DropStmt>(SqlCommand::DropDomain);
            auto* drop=static_cast<DropStmt*>(stmt.get());drop->objectType="DOMAIN";drop->objectNames=names;
            state(capture([&]{return ddl.execute(stmt,session);}),"42601");unchanged(bytes);
        });
    checks.test("private database cleanup",[&]{require(g_engine.dropDatabase(database)==DBStatus::OK,"dropDatabase failed");});
    // Isolated parser20+33 + setup10 + valid target13*5 + malformed SQL33
    // + local multi-target6+2 + missing4+4 + path4*2 + forged malformed3 + cleanup1 = 189.
    constexpr size_t expected=189;
    if(checks.controls!=expected){++checks.failures;std::cerr<<"DOMAIN_DROP_TARGET_INVENTORY expected "<<expected<<" got "<<checks.controls<<'\n';}
    std::cout<<"DOMAIN_DROP_TARGET_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expected<<" FAILURES "<<checks.failures<<'\n';
    return checks.failures?1:0;
}
