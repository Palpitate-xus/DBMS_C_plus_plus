#include "Session.h"
#include "common/DbError.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/SequenceStorageName.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <optional>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string db=testDbPath("temp_sequence_lifecycle");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    DdlExecutor ddl;SQLParser parser;int controls=0,failures=0;
    auto check=[&](const std::string& label,bool good) {
        ++controls;if(!good)++failures;
        std::cout<<"TEMP_SEQUENCE_LIFECYCLE "<<controls<<' '<<label<<" match "<<good<<std::endl;
    };
    auto initialize=[&](Session& s,uint64_t pid) {
        s.currentDB=db;s.username="admin";s.permission=1;s.pid=pid;
    };
    struct Result {bool failed;std::string output;};
    auto execute=[&](Session& s,const std::string& sql) {
        std::ostringstream output;bool failed;
        {ScopedOutputCapture capture(output);failed=ddl.executeSql(sql,s);}
        return Result{failed,output.str()};
    };
    auto relation=[&](const Session& s,const std::string& name)->std::optional<PgClassRow> {
        auto& catalog=g_engine.catalogService().get(db);
        const auto ns=catalog.getTempNamespace(s.pid);
        return ns?catalog.getClassByName(name,ns->oid):std::nullopt;
    };
    auto allocated=[&](const std::string& name,int64_t expected) {
        return g_engine.sequenceExists(db,name) && g_engine.nextval(db,name)==expected;
    };
    for(const std::string prefix:{"CREATE TEMP SEQUENCE","CREATE TEMPORARY SEQUENCE",
        "CREATE LOCAL TEMP SEQUENCE","CrEaTe TeMp SeQuEnCe",
        "/* lead */ CREATE /* scope */ TEMPORARY SEQUENCE","CREATE\nTEMP\tSEQUENCE"}) {
        const auto parsed=parser.parseForBinding(prefix+" r309_parser START WITH 7");
        const auto* stmt=parsed.isValid()?dynamic_cast<const CreateObjectStmt*>(parsed.stmt.get()):nullptr;
        check("parser valid "+prefix,parsed.isValid());
        check("parser sequence identity "+prefix,stmt && stmt->command==SqlCommand::CreateSequence);
        check("parser logical name "+prefix,stmt && stmt->objectName=="r309_parser");
        check("parser temporary modifier "+prefix,stmt && stmt->options.count("temporary") && stmt->options.at("temporary")=="true");
    }
    for(const std::string prefix:{"CREATE SEQUENCE","CREATE /* ordinary */ SEQUENCE"}) {
        const auto parsed=parser.parseForBinding(prefix+" r309_plain START WITH 7");
        const auto* stmt=parsed.isValid()?dynamic_cast<const CreateObjectStmt*>(parsed.stmt.get()):nullptr;
        check("ordinary parser valid",parsed.isValid());
        check("ordinary parser not temporary",stmt && !stmt->options.count("temporary"));
        check("ordinary parser name",stmt && stmt->objectName=="r309_plain");
    }
    int index=0;
    for(const std::string prefix:{"CREATE TEMP SEQUENCE","CREATE TEMPORARY SEQUENCE","CREATE LOCAL TEMP SEQUENCE"}) {
        Session a,b;initialize(a,309001+index*2);initialize(b,309002+index*2);
        const std::string name="native_temp_"+std::to_string(index++);
        const std::string aStorage=sequenceStorageName(sessionTempSchemaName(a),name);
        const std::string bStorage=sequenceStorageName(sessionTempSchemaName(b),name);
        check("public control create",!execute(a,"CREATE SEQUENCE public."+name+" START WITH 700").failed);
        check("owner temporary create",!execute(a,prefix+" "+name+" START WITH 11").failed);
        check("owner real namespace",g_engine.catalogService().get(db).getTempNamespace(a.pid).has_value());
        const auto ar=relation(a,name);
        check("owner class belongs to namespace",ar.has_value());
        check("owner class sequence",ar && ar->relkind=='S');
        check("owner persistence temporary",ar && ar->relpersistence=='t');
        check("owner cleanup flag",a.tempNamespaceCreated);
        check("owner physical sequence",g_engine.sequenceExists(db,aStorage));
        check("public physical control retained",g_engine.sequenceExists(db,name));
        check("owner temporary allocation",allocated(aStorage,11));
        check("public independent allocation",allocated(name,700));
        check("peer same temporary name create",!execute(b,prefix+" "+name+" START WITH 101").failed);
        check("peer real namespace",g_engine.catalogService().get(db).getTempNamespace(b.pid).has_value());
        const auto br=relation(b,name);
        check("peer distinct class identity",ar && br && ar->oid!=br->oid && ar->relnamespace!=br->relnamespace);
        check("peer physical sequence",g_engine.sequenceExists(db,bStorage));
        check("peer temporary allocation",allocated(bStorage,101));
        check("owner after peer allocation",allocated(aStorage,12));
        check("peer cleanup succeeds",g_engine.dropSessionTemporaryObjects(db,b.pid));
        check("peer catalog and file removed",!relation(b,name) && !g_engine.sequenceExists(db,bStorage));
        check("owner file unaffected by peer cleanup",g_engine.sequenceExists(db,aStorage));
        check("owner allocation after peer cleanup",allocated(aStorage,13));
        check("owner cleanup succeeds",g_engine.dropSessionTemporaryObjects(db,a.pid));
        check("owner catalog and file removed",!relation(a,name) && !g_engine.sequenceExists(db,aStorage));
        check("public allocation survives cleanup",allocated(name,701));
        check("public control drop",!execute(a,"DROP SEQUENCE public."+name).failed);
    }
    Session c;initialize(c,309100);
    auto result=execute(c,"CREATE TEMP SEQUENCE public.r309_forbidden");
    check("TEMP public schema rejected",result.failed);
    check("TEMP public schema SQLSTATE",result.output.find("SQLSTATE 42P16")!=std::string::npos);
    check("TEMP public schema no physical side effect",!g_engine.sequenceExists(db,"r309_forbidden"));
    if(g_engine.sequenceExists(db,"r309_forbidden"))execute(c,"DROP SEQUENCE public.r309_forbidden");
    result=execute(c,"CREATE TEMP SEQUENCE r309_native_missing.q");
    check("missing explicit schema rejected",result.failed);
    check("missing explicit schema SQLSTATE",result.output.find("SQLSTATE 3F000")!=std::string::npos);
    check("invalid schema did not create temp namespace",!g_engine.catalogService().get(db).getTempNamespace(c.pid));
    for(int i=0;i<2;++i) {
        const std::string name="native_alias_"+std::to_string(i);
        const std::string prefix=i?"CREATE TEMP SEQUENCE ":"CREATE SEQUENCE ";
        check("pg_temp alias create",!execute(c,prefix+"pg_temp."+name+" START WITH "+std::to_string(21+i*10)).failed);
        check("pg_temp alias real namespace",g_engine.catalogService().get(db).getTempNamespace(c.pid).has_value());
        const auto row=relation(c,name);
        check("pg_temp alias class",row && row->relkind=='S');
        check("pg_temp alias persistence",row && row->relpersistence=='t');
        const std::string physical=sequenceStorageName(sessionTempSchemaName(c),name);
        check("pg_temp alias physical sequence",g_engine.sequenceExists(db,physical));
        check("pg_temp alias no public file",!g_engine.sequenceExists(db,name));
        check("pg_temp alias allocation",allocated(physical,21+i*10));
        check("pg_temp alias cleanup flag",c.tempNamespaceCreated);
    }
    assert(g_engine.dropSessionTemporaryObjects(db,c.pid));
    Session d;initialize(d,309200);
    check("conflict existing TEMP table create",!execute(d,"CREATE TEMP TABLE native_collision(id integer)").failed);
    const auto before=relation(d,"native_collision");
    result=execute(d,"CREATE TEMP SEQUENCE native_collision START WITH 999");
    check("first TEMP table sequence conflict rejected",result.failed);
    check("TEMP table conflict SQLSTATE",result.output.find("SQLSTATE 42P07")!=std::string::npos);
    const auto after=relation(d,"native_collision");
    check("TEMP table identity unchanged",before && after && before->oid==after->oid && after->relkind=='r');
    check("TEMP table conflict no public sequence",!g_engine.sequenceExists(db,"native_collision"));
    check("TEMP table conflict no temp sequence",!g_engine.sequenceExists(db,sequenceStorageName(sessionTempSchemaName(d),"native_collision")));
    const auto attribute=after?g_engine.catalogService().get(db).getAttribute(after->oid,"id"):std::nullopt;
    check("TEMP table definition unchanged",attribute && attribute->atttypid==23 && attribute->attnum==1);
    assert(g_engine.dropSessionTemporaryObjects(db,d.pid));
    if(g_engine.sequenceExists(db,"native_collision"))execute(d,"DROP SEQUENCE public.native_collision");
    Session e;initialize(e,309300);
    const std::string cleanupStorage=sequenceStorageName(sessionTempSchemaName(e),"cleanup_only");
    check("preserved namespace standalone create",!execute(e,"CREATE TEMP SEQUENCE cleanup_only").failed);
    check("preserved namespace standalone file",g_engine.sequenceExists(db,cleanupStorage));
    check("preserved namespace standalone catalog",relation(e,"cleanup_only").has_value());
    check("preserved namespace cleanup succeeds",g_engine.dropSessionTemporaryObjects(db,e.pid,true));
    check("preserved namespace still exists",g_engine.catalogService().get(db).getTempNamespace(e.pid).has_value());
    check("preserved namespace class removed",!relation(e,"cleanup_only"));
    check("preserved namespace file removed",!g_engine.sequenceExists(db,cleanupStorage));
    check("preserved namespace repeat cleanup succeeds",g_engine.dropSessionTemporaryObjects(db,e.pid,true));
    bool missing=false;
    {
        struct RestoreSession {Session* previous;~RestoreSession(){setCurrentSession(previous);}} restore{currentSession()};
        setCurrentSession(&e);
        try { (void)g_engine.nextval(db,"cleanup_only"); }
        catch(const DbError& error) { missing=error.sqlState()=="42P01"; }
    }
    check("preserved namespace SQL lookup reports missing",missing);
    const std::string aliasStorage=sequenceStorageName(sessionTempSchemaName(e),"drop_alias");
    check("alias drop standalone create",!execute(e,"CREATE TEMP SEQUENCE drop_alias START WITH 7").failed);
    check("alias drop DDL succeeds",!execute(e,"DROP SEQUENCE pg_temp.drop_alias").failed);
    check("alias drop class removed",!relation(e,"drop_alias"));
    check("alias drop file removed",!g_engine.sequenceExists(db,aliasStorage));
    check("alias drop namespace retained",g_engine.catalogService().get(db).getTempNamespace(e.pid).has_value());
    assert(g_engine.dropSessionTemporaryObjects(db,e.pid));
    assert(controls==148);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout<<"TEMP_SEQUENCE_LIFECYCLE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
