#include "parser/query_binding.h"
#include "expression/expr_helper.h"
#include "common/DbError.h"
#include "catalog/catalog.h"
#include <iostream>

int main() {
    using namespace dbms; TypeRegistry::instance().bootstrap();
    size_t controls=0, failures=0, descriptions=0, defaults=0;
    QueryBindingMetadata metadata;
    metadata.relation=[&](const std::string& name) {
        ++descriptions;
        if(name!="target")throw DbError("42P01","missing relation");
        return QueryRelationMetadata{"public","target",{{"id","integer"}}, {}};
    };
    const auto compare=[&](const std::string& left,const std::string& right,bool expected) {
        auto query=prepareQuery("SELECT(SELECT 1 "+left+") ORDER BY(SELECT 1 "+right+")",{},metadata);
        auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
        const auto column=[](const ColumnRefExpr&)->std::string{throw DbError("XX000","unexpected column resolver");};
        const auto first=ExprHelper::preparedSortExpressionIdentity(select->selectList[0].expr.get(),query,column);
        const auto second=ExprHelper::preparedSortExpressionIdentity(select->orderBy[0].expr.get(),query,column);
        const bool actual=first==second; ++controls; failures+=actual!=expected;
        std::cout<<"SIGNED_FETCH_IDENTITY left="<<left<<" right="<<right<<" actual="<<actual<<" expected="<<expected<<std::endl;
        for(const auto* value:{select->selectList[0].expr.get(),select->orderBy[0].expr.get()}) {
            const auto* child=dynamic_cast<const SelectStmt*>(value->preparedSubquery.get());
            if(!child || !child->signedFetchCount || (*child->signedFetchCount<0 && child->limit))++failures;
        }
    };
    compare("FETCH FIRST -1 ROW ONLY","FETCH FIRST -2 ROWS ONLY",false);
    compare("FETCH FIRST -1 ROW ONLY","FETCH FIRST -01 ROW ONLY",true);
    compare("FETCH FIRST -9223372036854775808 ROWS ONLY","FETCH FIRST -9223372036854775807 ROWS ONLY",false);
    compare("FETCH FIRST +1 ROW ONLY","FETCH FIRST 01 ROW ONLY",true);
    compare("FETCH FIRST -0 ROWS ONLY","FETCH FIRST +0 ROWS ONLY",true);
    compare("FETCH FIRST -1 ROW ONLY","FETCH NEXT -1 ROW ONLY",true);
    compare("ORDER BY 1 FETCH FIRST -1 ROW WITH TIES","ORDER BY 1 FETCH FIRST -1 ROW ONLY",false);
    if(descriptions)++failures; // whole prepared identities are pure and source-free
    for(const auto& definition:{"1","1 FETCH FIRST -1 ROW ONLY","1 FETCH FIRST -9223372036854775808 ROWS ONLY",
                               "1 FETCH FIRST +0 ROWS ONLY","1 FETCH FIRST 1 ROW ONLY"}) {
        metadata.updateDefault=[&](const std::string& schema,const std::string& relation,const std::string& column)->std::optional<std::string> {
            ++defaults;if(schema!="public" || relation!="target" || column!="id")++failures;return definition;
        };
        std::string state;
        try{(void)prepareQuery("INSERT INTO target(id) VALUES(DEFAULT)",{},metadata);}
        catch(const DbError& error){state=error.sqlState();}
        const std::string expected=std::string(definition)=="1"?"":"XX001";
        ++controls;failures+=state!=expected;
        std::cout<<"SIGNED_FETCH_DEFAULT definition="<<definition<<" actual="<<state<<" expected="<<expected<<std::endl;
    }
    if(defaults!=5 || descriptions!=5)++failures;
    std::cout<<"[SIGNED FETCH IDENTITY] complete controls="<<controls<<" failures="<<failures<<std::endl;
    return failures?1:0;
}
