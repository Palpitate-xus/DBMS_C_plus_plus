#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/SequenceStorageName.h"
#include "commands/TableManage.h"
#include "process/OutputCapture.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("sequence_duplicate_sqlstate");
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username="admin";session.permission=1;session.currentDB=db;
    DdlExecutor ddl;
    int controls=0,failures=0;
    auto check=[&](const std::string& label,bool good) {
        ++controls;if(!good)++failures;
        std::cout<<"SEQUENCE_DUPLICATE_SQLSTATE "<<controls<<' '<<label<<" match "<<good<<'\n';
    };
    struct Result { bool failed;std::string output; };
    auto execute=[&](const std::string& sql) {
        std::ostringstream text;bool failed;
        { ScopedOutputCapture capture(text);failed=ddl.executeSql(sql,session); }
        return Result{failed,text.str()};
    };
    assert(!execute("CREATE SCHEMA r307_native").failed);
    for(const std::string schema:{"public","r307_native"}) {
        for(const char kind:{'S','r','v'}) {
            const std::string name="duplicate_"+std::string(1,kind=='S'?'s':kind);
            const std::string target=schema+"."+name;
            const std::string storage=sequenceStorageName(schema,name);
            const std::string label=target+" ";
            const std::string create=kind=='S'?"CREATE SEQUENCE "+target+" START WITH 11":
                kind=='r'?"CREATE TABLE "+target+"(id integer)":
                "CREATE VIEW "+target+" AS SELECT 9 AS id";
            check(label+"setup",!execute(create).failed);
            auto& catalog=g_engine.catalogService().get(db);
            const auto ns=catalog.getNamespaceByName(schema);assert(ns);
            const auto before=catalog.getClassByName(name,ns->oid);
            check(label+"catalog present",before.has_value());assert(before);
            const Oid oid=before->oid;
            int64_t expected=11;
            auto probe=[&] {
                if(kind=='S')return g_engine.nextval(db,storage)==expected++;
                const auto attribute=g_engine.catalogService().get(db).getAttribute(oid,"id");
                return attribute && attribute->atttypid==23 && attribute->attnum==1;
            };
            auto unchanged=[&] {
                if(kind!='S')return !g_engine.sequenceExists(db,storage);
                SequenceInfo info;
                return g_engine.getSequenceInfo(db,storage,info)==DBStatus::OK && info.increment==1 && info.start==11;
            };
            check(label+"initial object",probe());
            auto result=execute("CREATE SEQUENCE "+target+" START WITH 999");
            check(label+"duplicate failed",result.failed);
            check(label+"duplicate SQLSTATE",result.output.find("SQLSTATE 42P07")!=std::string::npos);
            check(label+"conflict diagnostic",result.output.find("already exists")!=std::string::npos);
            auto row=g_engine.catalogService().get(db).getClassByName(name,ns->oid);
            check(label+"duplicate OID unchanged",row && row->oid==oid);
            check(label+"duplicate kind unchanged",row && row->relkind==kind);
            check(label+"duplicate storage unchanged",unchanged());
            check(label+"duplicate object usable",probe());
            result=execute("CREATE SEQUENCE IF NOT EXISTS "+target+" START WITH 999");
            check(label+"IF NOT EXISTS succeeded",!result.failed);
            check(label+"IF NOT EXISTS notice",result.output.find("NOTICE:")!=std::string::npos);
            check(label+"notice identifies conflict",result.output.find("already exists")!=std::string::npos);
            check(label+"notice is not error",result.output.find("ERROR:")==std::string::npos);
            row=g_engine.catalogService().get(db).getClassByName(name,ns->oid);
            check(label+"notice OID unchanged",row && row->oid==oid);
            check(label+"notice kind unchanged",row && row->relkind==kind);
            check(label+"notice storage unchanged",unchanged());
            check(label+"notice object usable",probe());
        }
    }
    assert(controls==108);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout<<"SEQUENCE_DUPLICATE_SQLSTATE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
