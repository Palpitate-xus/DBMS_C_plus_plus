#include "parser/parser.h"
#include <iostream>
#include <string>
#include <vector>

namespace {
size_t checks=0,failures=0;
void check(bool valid,const std::string& label) {
    ++checks;
    if(!valid) {++failures;std::cerr<<"DERIVED_QUERY_SOURCE_PARSE_FAIL "<<label<<'\n';}
}
dbms::ParseResult parse(const std::string& sql,bool binding) {
    try {dbms::SQLParser parser;return binding?parser.parseForBinding(sql):parser.parse(sql);}
    catch(const std::exception& error) {dbms::ParseResult result;result.error=error.what();return result;}
}
const dbms::SelectStmt* query(const dbms::Stmt* stmt) {return dynamic_cast<const dbms::SelectStmt*>(stmt);}
const dbms::SelectStmt* left(const dbms::SelectStmt* stmt) {
    while(stmt && stmt->setOpLhs)stmt=query(stmt->setOpLhs.get());
    return stmt;
}
struct Case {
    std::string sql;
    dbms::SqlCommand classification,command;
    size_t ctes;
    int rows;
    int setTarget; // 0 none, 1 root, 2 first CTE
    bool recursive=false,nested=false;
};
}

int main() {
    using C=dbms::SqlCommand;
    const std::vector<Case> cases={
        {"VALUES(1::integer),(2::integer)",C::Values,C::Values,0,2,0},
        {"WITH c AS(SELECT 1::integer AS v) VALUES(2::integer)",C::Values,C::Values,1,1,0},
        {"WITH rows AS(SELECT 1::integer AS v) VALUES(2::integer)",C::Values,C::Values,1,1,0},
        {"WITH values AS(SELECT 1::integer AS v) SELECT v FROM values",C::Select,C::Select,1,0,0},
        {"WITH RECURSIVE c(n) AS(VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<3) SELECT n FROM c",C::Select,C::Select,1,0,2,true},
        {"VALUES(1::integer) UNION ALL VALUES(2::integer)",C::Values,C::Unknown,0,-1,1},
        {"VALUES(1::integer) UNION ALL SELECT 2::integer ORDER BY 1",C::Values,C::Unknown,0,-1,1},
        {"(VALUES(1::integer) UNION ALL SELECT 2::integer)",C::Values,C::Unknown,0,-1,1},
        {"WITH c AS(VALUES(1::integer) UNION ALL VALUES(2::integer)) SELECT * FROM c",C::Select,C::Select,1,0,2},
        {"WITH c AS(WITH d AS(SELECT 1::integer AS v) VALUES(2::integer)) SELECT * FROM c",C::Select,C::Select,1,0,0,false,true},
        {"WITH \"select\" AS(SELECT 1::integer AS v) VALUES(2::integer)",C::Values,C::Values,1,1,0},
        {"WITH c(x) AS NOT MATERIALIZED(SELECT 1::integer AS v),d AS(VALUES(2::integer)) VALUES(3::integer)",C::Values,C::Values,2,1,0}
    };
    for(const auto& item:cases)for(bool binding:{false,true}) {
        const auto label=std::to_string(binding)+":"+item.sql;
        auto result=parse(item.sql,binding);const auto* root=query(result.stmt.get());
        const auto* child=root && !root->ctes.empty()?query(root->ctes.front().query.get()):nullptr;
        const auto* target=item.setTarget==2?child:root;
        check(dbms::SQLParser::classify(item.sql)==item.classification,label+":classify");
        check(result.isValid(),label+":valid "+result.error);
        check(root && (item.command==C::Unknown?(root->command==C::Select || root->command==C::Values):root->command==item.command),label+":query command");
        check(root && root->ctes.size()==item.ctes,label+":root CTE owner");
        check(root && (item.rows<0?(left(target) && left(target)->valuesRows.size()==1):root->valuesRows.size()==size_t(item.rows)),label+":VALUES rows");
        check(root && (item.ctes?(!root->ctes.empty() && root->ctes.front().recursive==item.recursive):root->ctes.empty()),label+":recursive declaration");
        check(target && (item.setTarget?(target->setOp==dbms::SetOp::Union && target->setOpAll && target->setOpRhs):target->setOp==dbms::SetOp::None),label+":set-operation owner");
        bool owned=root!=nullptr;
        if(root)for(const auto& cte:root->ctes)owned=owned && bool(cte.query);
        if(item.setTarget)owned=owned && left(target) && left(target)->valuesRows.size()==1;
        if(item.nested)owned=owned && child && child->command==C::Values && child->ctes.size()==1;
        if(item.command==C::Values)owned=owned && root && root->selectList.empty();
        check(owned,label+":retained query tree");
    }
    const std::vector<std::string> aliases={"rows","materialized","values","between","filter","\"Q Alias\"","\"select\"","q"};
    for(const auto& alias:aliases)for(bool binding:{false,true}) {
        const std::string sql="SELECT * FROM(SELECT 1::integer AS v) "+alias;
        auto result=parse(sql,binding);const auto* root=query(result.stmt.get());const auto* source=root?root->fromClause.get():nullptr;
        check(result.isValid(),sql+":valid "+result.error);
        check(source && source->type==dbms::FromItem::Type::Subquery,sql+":source role");
        check(source && source->alias==alias,sql+":alias identity");
        check(source && query(source->subquery.get()) && source->columnAliases.empty(),sql+":child owner");
    }
    const std::vector<std::string> anonymous={"SELECT v FROM(SELECT 1::integer AS v)","SELECT v FROM(WITH c AS(SELECT 1::integer AS v) SELECT v FROM c)"};
    for(const auto& sql:anonymous)for(bool binding:{false,true}) {
        auto result=parse(sql,binding);const auto* root=query(result.stmt.get());const auto* source=root?root->fromClause.get():nullptr;
        check(result.isValid(),sql+":anonymous valid");
        check(source && source->type==dbms::FromItem::Type::Subquery,sql+":anonymous query");
        check(source && source->alias.empty(),sql+":no synthetic SQL-visible AST alias");
        check(source && query(source->subquery.get()),sql+":anonymous owner");
    }
    const std::string provenance="WITH c AS(SELECT 1::integer AS v) VALUES(2::integer)";
    auto result=parse(provenance,true);const auto* root=query(result.stmt.get());
    const auto* value=root && !root->valuesRows.empty() && !root->valuesRows[0].empty()?root->valuesRows[0][0].get():nullptr;
    const size_t at=provenance.find("2::integer");
    check(root && root->command==C::Values,"VALUES source provenance owner");
    check(value && value->sourceBegin==at,"VALUES original source begin");
    check(value && value->sourceEnd==at+std::string("2::integer").size(),"VALUES original source end");
    check(root && !root->ctes.empty() && query(root->ctes[0].query.get()) && root->ctes[0].query.get()!=root,"VALUES independent CTE statement identity");
    const size_t expected=2*(cases.size()*8+aliases.size()*4+anonymous.size()*4)+4;
    std::cout<<"DERIVED_QUERY_SOURCE_PARSE_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    if(checks!=expected)return 2;
    return failures?1:0;
}
