#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name="geometric_cast_result_type",db=testDbPath(name);
    cleanupTestDb(name);assert(engine.createDatabase(db)==DBStatus::OK);
    assert(engine.createSequence(db,"geometry_metadata_effects",1,1)==DBStatus::OK);
    size_t failures=0;
    for(const auto& kind:std::vector<std::string>{"point","line","lseg","box","path","polygon","circle"}) {
        for(const auto& spelling:std::vector<std::string>{kind,"pg_catalog."+kind,"pg_catalog.\""+kind+"\""}) {
            for(const auto& expression:std::vector<std::string>{"CAST(NULL AS "+spelling+")","NULL::"+spelling}) {
                auto prepared=prepareQuery("SELECT "+expression,{},{});
                auto* select=dynamic_cast<SelectStmt*>(prepared.ast.get());assert(select);
                const auto inferred=ExprHelper::inferParsedResultType(select->selectList[0].expr.get(),{},db,&engine);
                const auto descriptor=prepared.output.at(0).type;
                const auto label=prepared.output.at(0).name;
                std::cout<<"GEOMETRY CAST METADATA "<<expression<<" descriptor="<<descriptor<<" inferred="<<inferred<<" label="<<label<<" expected="<<kind<<std::endl;
                if(descriptor!=kind || inferred!=kind || label!=kind ||
                    ExprHelper::canonicalResultTypeName(spelling)!=kind)++failures;
            }
        }
    }
    // Distinct quoted/custom namespaces must not become the pg_catalog type.
    for(const auto& spelling:std::vector<std::string>{"\"Path\"","public.path","\"pg_catalog.path\""})
        assert(ExprHelper::canonicalResultTypeName(spelling)!="path");
    assert(engine.nextval(db,"geometry_metadata_effects")==1); // no type discovery by execution
    assert(failures==0);
    cleanupTestDb(name);std::cout<<"[GEOMETRY CAST RESULT TYPE] passed\n";
}
