#include "parser/query_binding.h"
#include "common/DbError.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t described=0;
    QueryBindingMetadata metadata;
    metadata.relation=[](const std::string& name) {
        if(name!="source")throw DbError("42P01","missing table");
        return QueryRelationMetadata{"public","source",{{"id","integer"}}, {}};
    };
    metadata.functionType=[&](const FunctionCallExpr* call) {
        ++described;
        if(call->funcName!="writer")throw DbError("42883","missing function");
        return "integer";
    };
    {
        const std::string sql="SELECT writer(10) AS n UNION ALL SELECT writer(2) ORDER BY n LIMIT 1";
        auto query=prepareQuery(sql,{},metadata);
        auto* root=static_cast<SelectStmt*>(query.ast.get());
        assert(root->setOpLhs && root->setOpRhs && root->selectList.empty());
        auto* left=static_cast<SelectStmt*>(root->setOpLhs.get());
        auto* right=static_cast<SelectStmt*>(root->setOpRhs.get());
        assert(root->limit==1 && root->orderBy.size()==1);
        assert(!left->limit && left->orderBy.empty() && !right->limit && right->orderBy.empty());
        assert(query.setOrderColumns.at(root)==std::vector<size_t>{0});
        assert(query.output.front().name=="n" && query.output.front().type=="integer");
        const auto* ref=static_cast<ColumnRefExpr*>(root->orderBy.front().expr.get());
        assert(!ref->binding && sql.substr(ref->sourceBegin,ref->sourceEnd-ref->sourceBegin)=="n");
        assert(query.sourceRanges.empty() && described==2);
    }
    {
        auto query=prepareQuery("(SELECT 3 AS n ORDER BY n LIMIT 1) UNION ALL (SELECT 2 LIMIT 0) ORDER BY n OFFSET 1 LIMIT 2",{},metadata);
        const auto* root=static_cast<SelectStmt*>(query.ast.get());
        const auto* left=static_cast<SelectStmt*>(root->setOpLhs.get());
        const auto* right=static_cast<SelectStmt*>(root->setOpRhs.get());
        assert(root->limit==2 && root->offset==1 && root->orderBy.size()==1);
        assert(left->limit==1 && left->orderBy.size()==1 && right->limit==0);
        assert(query.setOrderColumns.at(root)==std::vector<size_t>{0});
    }
    {
        auto query=prepareQuery("SELECT 3 AS n UNION ALL SELECT 2 UNION ALL SELECT 1 ORDER BY n LIMIT 1",{},metadata);
        const auto* root=static_cast<SelectStmt*>(query.ast.get());
        const auto* left=static_cast<SelectStmt*>(root->setOpLhs.get());
        assert(left->setOp==SetOp::Union && left->setOpRhs && left->orderBy.empty() && !left->limit);
        assert(root->limit==1 && root->orderBy.size()==1);
        assert(query.setOperationInputs.size()==2);
    }
    {
        auto query=prepareQuery("WITH c AS(SELECT 3 AS n) SELECT n FROM c UNION ALL SELECT n FROM c ORDER BY n LIMIT 1",{},metadata);
        const auto* root=static_cast<SelectStmt*>(query.ast.get());
        assert(root->ctes.size()==1 && root->setOpLhs && root->setOpRhs);
        assert(query.sourceRanges.size()==2 && query.sourceRanges[0].cteStatement==root->ctes[0].query.get());
        assert(query.sourceRanges[1].cteStatement==root->ctes[0].query.get());
    }
    for(const auto& [sql,state]:std::vector<std::pair<std::string,std::string>>{
        {"SELECT 1 AS n UNION ALL SELECT 2 ORDER BY missing","42703"},
        {"SELECT 1 AS n UNION ALL SELECT 2 AS rhs ORDER BY rhs","42703"},
        {"SELECT id AS n FROM source UNION ALL SELECT 2 ORDER BY source.id","42P01"},
        {"SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n+1","0A000"},
        {"SELECT 1 AS n UNION ALL SELECT 2 ORDER BY 0","42P10"},
        {"SELECT 1 AS n UNION ALL SELECT 2 ORDER BY 2","42P10"},
        {"SELECT 1 AS x,2 AS x UNION ALL SELECT 3,4 ORDER BY x","42702"},
        {"SELECT 1 LIMIT 1 UNION ALL SELECT 2","42601"},
        {"SELECT 1 ORDER BY 1 UNION ALL SELECT 2","42601"},
        {"SELECT writer(1) AS n UNION ALL SELECT 2 ORDER BY missing","42703"}
    }) {
        std::string actual;
        try{(void)prepareQuery(sql,{},metadata);}catch(const DbError& error){actual=error.sqlState();}
        std::cout<<sql<<" state="<<actual<<std::endl;
        assert(actual==state);
    }
    assert(described==3); // metadata signatures only; no execution hook exists
}
