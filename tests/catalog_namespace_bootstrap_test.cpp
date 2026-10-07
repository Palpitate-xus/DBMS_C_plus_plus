#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sys/wait.h>
#include <unistd.h>
extern dbms::StorageEngine g_engine;

static std::string fileBytes(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);assert(file);
    return std::string(std::istreambuf_iterator<char>(file), {});
}

static int freshProcess(const std::string& database, dbms::Oid expectedOid) {
    using namespace dbms;
    Session session;session.username="namespace_bootstrap";session.permission=1;
    session.currentDB=database;session.searchPath="public";setCurrentSession(&session);
    const auto path=g_engine.dbPath(database)/"pg_catalog"/"pg_namespace.cat";
    const auto before=fileBytes(path);
    auto& catalog=g_engine.catalogService().get(database);
    size_t count=0;for(const auto& row:catalog.metadataSnapshot().namespaces)
        if(row.nspname=="public")++count;
    const auto* actual=catalog.findNamespaceByName("public");
    bool valid=expectedOid==INVALID_OID ? count==0 && !actual :
        count==1 && actual && actual->oid==expectedOid;
    if(expectedOid!=INVALID_OID) {
        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT id FROM public.kept"));
        const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,prepared,prepared->ast.get()));
        result.throwIfFailed();
        valid=valid && result.structuredRows==std::vector<std::vector<std::string>>{{"7"}};
    }
    valid=valid && catalog.persistAll() && fileBytes(path)==before;
    setCurrentSession(nullptr);
    std::cout<<"[NAMESPACE BOOTSTRAP] fresh exec actual namespace/rows/bytes "<<(valid?"PASS":"FAIL")<<std::endl;
    return valid?0:1;
}

int main(int argc,char** argv) {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    if(argc==4 && std::string(argv[1])=="cold")
        return freshProcess(argv[2],static_cast<Oid>(std::stoul(argv[3])));
    Session session;session.username="namespace_bootstrap";session.permission=1;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;int failures=0;
    const auto check=[&](bool good,const std::string& label) {
        std::cout<<"[NAMESPACE BOOTSTRAP] "<<label<<' '<<(good?"PASS":"FAIL")<<std::endl;
        if(!good)++failures;
    };
    const auto publicCount=[](const CatalogManager::MetadataSnapshot& rows) {
        size_t count=0;for(const auto& row:rows.namespaces)if(row.nspname=="public")++count;return count;
    };
    for(bool recreate:{false,true}) {
        const auto name=recreate?"catalog_public_recreate":"catalog_public_drop";
        const auto database=testDbPath(name);cleanupTestDb(name);
        assert(g_engine.createDatabase(database)==DBStatus::OK);session.currentDB=database;session.searchPath="public";
        auto& initial=g_engine.catalogService().get(database);
        check(publicCount(initial.metadataSnapshot())==1,"fresh bootstrap creates public exactly once");
        assert(!ddl.executeSql("DROP SCHEMA public",session));
        Oid expectedOid=INVALID_OID;
        if(recreate) {
            assert(!ddl.executeSql("CREATE SCHEMA public",session));
            const auto* actual=g_engine.catalogService().get(database).findNamespaceByName("public");assert(actual);
            expectedOid=actual->oid;assert(expectedOid!=2200);
            assert(!ddl.executeSql("CREATE TABLE public.kept(id INT)",session));
            assert(g_engine.insertRow(database,"kept",{{"id","7"}})==DBStatus::OK);
        }
        assert(g_engine.catalogService().persistAll());
        const auto path=g_engine.dbPath(database)/"pg_catalog"/"pg_namespace.cat";
        const auto before=fileBytes(path);
        const auto child=::fork();assert(child>=0);
        if(child==0) {
            const auto oid=std::to_string(expectedOid);
            ::execl(argv[0],argv[0],"cold",database.c_str(),oid.c_str(),nullptr);
            _exit(127);
        }
        int childStatus=0;assert(::waitpid(child,&childStatus,0)==child);
        check(WIFEXITED(childStatus) && WEXITSTATUS(childStatus)==0,"independent exec preserves committed namespace state");
        g_engine.catalogService().evict(database);
        auto& cold=g_engine.catalogService().get(database);
        const auto snapshot=cold.metadataSnapshot();
        check(publicCount(snapshot)==(recreate?1u:0u),"cold bootstrap preserves actual public cardinality");
        const auto* actual=cold.findNamespaceByName("public");
        check(recreate?(actual && actual->oid==expectedOid):!actual,"cold bootstrap preserves actual public identity/absence");
        assert(cold.persistAll());
        check(fileBytes(path)==before,"cold bootstrap does not rewrite namespace snapshot");
        if(recreate) {
            check(cold.findClassByName("kept",expectedOid)!=nullptr,"actual relation still belongs to recreated public");
            check(g_engine.query(database,"kept",{},{"id"}).size()==1,"actual physical row remains present");
            std::string state;std::vector<std::vector<std::string>> rows;
            try {
                auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT id FROM public.kept"));
                const auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,prepared,prepared->ast.get()));
                result.throwIfFailed();rows=result.structuredRows;
            } catch(const DbError& error){state=error.sqlState();}
            check(state.empty() && rows==std::vector<std::vector<std::string>>{{"7"}},"actual bound SELECT after cold reopen state="+state);
        } else {
            for(const auto* sql:{"CREATE DOMAIN public.rejected_domain AS INT","CREATE FUNCTION public.rejected_function() RETURNS INT LANGUAGE SQL AS $$SELECT 1$$"}) {
                std::string state;try{(void)ddl.executeSql(sql,session);}catch(const DbError& error){state=error.sqlState();}
                check(state=="3F000","actual declaration after committed DROP state="+state);
            }
            check(!std::filesystem::exists(g_engine.dbPath(database)/".domains"),"no rejected domain artifact");
            check(!g_engine.udfExists(database,"rejected_function","public"),"no rejected routine artifact");
            check(!g_engine.inTransaction(),"no declaration owner leak");
        }
        assert(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    }
    {
        CatalogManager catalog("standalone_bootstrap");
        catalog.createNamespace("custom_before_bootstrap",10);
        catalog.bootstrapSystemNamespaces();
        check(publicCount(catalog.metadataSnapshot())==1,"standalone first initialization remains compatible");
        assert(catalog.dropNamespace(2200));
        catalog.bootstrapSystemNamespaces();
        check(publicCount(catalog.metadataSnapshot())==0,"repeat bootstrap does not resurrect an in-memory DROP");
        assert(catalog.persistAll());
    }
    {
        CatalogManager catalog("standalone_existing_public");
        const auto oid=catalog.createNamespace("public",10);assert(oid!=2200);
        catalog.bootstrapSystemNamespaces();
        const auto* actual=catalog.findNamespaceByName("public");
        check(publicCount(catalog.metadataSnapshot())==1 && actual && actual->oid==oid,
              "first bootstrap preserves an already-created public identity");
        assert(catalog.persistAll());
    }
    std::cout<<"[NAMESPACE BOOTSTRAP] failed="<<failures<<std::endl;
    return failures?1:0;
}
