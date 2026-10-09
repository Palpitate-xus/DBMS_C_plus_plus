#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checks=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checks;failures+=!pass;std::cout<<"NAMED_ARRAY_MODIFIER "<<role<<" pass="<<pass<<'\n';};
    const auto db=testDbPath("named_array_modifier");
    require(g_engine.createDatabase(db)==DBStatus::OK,"create isolated database");
    struct Case {std::string type,inputType,value,expected;Oid oid;};
    const std::vector<Case> cases={
        {"pg_catalog._varchar(3)","character varying(3)[]","{abcdef,xy,NULL}","{abc,xy,NULL}",1015},
        {"pg_catalog._numeric(5,2)","numeric(5,2)[]","{1.235,2,NULL}","{1.24,2.00,NULL}",1231},
        {"pg_catalog._bpchar(3)","character(3)[]","{a,abcd,NULL}","{\"a  \",abc,NULL}",1014},
        {"pg_catalog._varchar(3)","character varying(3)[]","[0:1]={abcdef,xy}","[0:1]={abc,xy}",1015}
    };
    for(bool warm:{false,true}) {
        if(warm)(void)g_engine.catalogService().get(db);
        const auto snapshot=warm?g_engine.catalogService().metadataSnapshot(db):CatalogManager::MetadataSnapshot{};
        for(const auto& item:cases) {
            const auto type=resolveDeclaredTypeName(item.type,warm?&snapshot:nullptr,nullptr);
            require(type.inputType==item.inputType && type.typeOid==item.oid,(warm?"warm ":"cold ")+item.type+" modifier precedes array suffix");
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT CAST('"+item.value+"' AS "+item.type+") AS value"));
            require(query->output.at(0).typeOid==item.oid,"actual static array identity");
            auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
            auto result=QueryPlanner::executePlanChecked(std::move(plan));result.throwIfFailed();
            require(result.structuredRows==std::vector<std::vector<std::string>>{{item.expected}},"actual element input/rounding/padding/lower bounds");
        }
    }
    std::cout<<"NAMED_ARRAY_MODIFIER_CHECKED="<<checks<<" FAILED="<<failures<<'\n';
    return checks==25 && !failures?0:1;
}
