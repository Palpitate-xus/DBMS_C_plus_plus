#include "parser/parser.h"

#include <iostream>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
using namespace dbms;
size_t controls=0,failures=0;
void check(bool valid,const std::string& label){
    ++controls;failures+=!valid;
    if(!valid)std::cout<<"QUERY_ROOT_COUNT_GRAMMAR_FAILED "<<label<<'\n';
}
ParseResult parse(const std::string& sql,bool binding){
    try{SQLParser parser;return binding?parser.parseForBinding(sql):parser.parse(sql);}
    catch(const std::exception& error){ParseResult result;result.error=error.what();return result;}
}
template<class T,class=void>struct CountErrorRole{static bool get(const T&){return false;}};
template<class T>struct CountErrorRole<T,std::void_t<decltype(std::declval<T>().rowCountSyntaxError)>>{
    static bool get(const T& result){return result.rowCountSyntaxError;}
};
std::string shape(const Expr* expr){
    if(!expr)return "missing";
    if(const auto* value=dynamic_cast<const LiteralExpr*>(expr))return "L:"+value->value;
    if(const auto* column=dynamic_cast<const ColumnRefExpr*>(expr))return "C:"+column->toString();
    if(const auto* parameter=dynamic_cast<const ParameterExpr*>(expr))
        return "P:"+std::to_string(parameter->slot+1)+(parameter->origin==ParameterOrigin::StatementInput?":input":":wrong-owner");
    if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(expr))return "U:"+unary->op+"("+shape(unary->operand.get())+")";
    if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(expr))return "B:"+binary->op+"("+shape(binary->left.get())+","+shape(binary->right.get())+")";
    if(const auto* cast=dynamic_cast<const CastExpr*>(expr))return "CAST:"+cast->typeName+"("+shape(cast->operand.get())+")";
    if(const auto* call=dynamic_cast<const FunctionCallExpr*>(expr)){
        std::string result="F:"+call->funcName+"(";
        for(size_t i=0;i<call->args.size();++i)result+=(i?",":"")+shape(call->args[i].get());
        return result+")";
    }
    return "other-expression";
}
const SelectStmt* query(const Stmt* stmt){return dynamic_cast<const SelectStmt*>(stmt);}
bool originalRootRole(const SelectStmt* root,size_t form){
    if(!root || root->orderBy.size()!=1 || !root->ctes.empty() || !root->hasRowCountClause)return false;
    if(form==0)return root->command==SqlCommand::Values && root->setOp==SetOp::None &&
        root->valuesRows.size()==4 && !root->setOpLhs && !root->setOpRhs;
    if(root->setOp!=SetOp::Union || !root->setOpAll || !root->setOpRhs || root->setOpRhs.get()==root)return false;
    if(form==2)return root->command==SqlCommand::Values && root->valuesRows.size()==2 && !root->setOpLhs &&
        query(root->setOpRhs.get()) && query(root->setOpRhs.get())->valuesRows.size()==2;
    return root->command==SqlCommand::Select && root->valuesRows.empty() && root->setOpLhs &&
        root->setOpLhs.get()!=root && root->setOpLhs.get()!=root->setOpRhs.get();
}
bool span(const Expr* expr,const std::string& sql,const std::string& clause,const std::string& bytes,bool binding){
    if(!expr)return bytes.empty();
    if(bytes.empty())return false;
    if(!binding)return expr->sourceBegin==std::string::npos && expr->sourceEnd==std::string::npos;
    const auto at=sql.find(" "+clause+" ");
    const auto begin=at==std::string::npos?std::string::npos:sql.find(bytes,at+clause.size()+2);
    return begin!=std::string::npos && expr->sourceBegin==begin && expr->sourceEnd==begin+bytes.size() &&
        sql.substr(expr->sourceBegin,expr->sourceEnd-expr->sourceBegin)==bytes;
}
struct Valid {
    std::string tail,limit="missing",offset="missing",limitBytes={},offsetBytes={};
    bool fetch=false,ties=false;
    std::optional<size_t> cap={},skip={};
    std::optional<int64_t> signedFetch={},signedLimit={},signedOffset={};
};
}

int main(){
    const std::vector<std::string> roots{
        "VALUES(3),(1),(1),(2) ORDER BY column1",
        "VALUES(3),(1) UNION ALL VALUES(1),(2) ORDER BY 1",
        "((VALUES(3),(1)) UNION ALL VALUES(1),(2)) ORDER BY 1",
        "SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY 1"
    };
    const std::vector<Valid> valid{
        {"LIMIT (1+1)","B:+(L:1,L:1)","missing","1+1"},
        {"OFFSET ($1) ROWS LIMIT 2","missing","P:1:input","","$1",false,false,2,{}, {},2},
        {"LIMIT $1 OFFSET ($2+1)","P:1:input","B:+(P:2:input,L:1)","$1","$2+1"},
        {"FETCH FIRST (1+1) ROWS ONLY","B:+(L:1,L:1)","missing","1+1","",true},
        {"FETCH NEXT $1 ROWS WITH TIES","P:1:input","missing","$1","",true,true},
        {"FETCH FIRST abs(-1) ROWS ONLY","F:abs(U:-(L:1))","missing","abs(-1)","",true},
        {"FETCH FIRST +1.5 ROWS ONLY","U:+(L:1.5)","missing","+1.5","",true},
        {"FETCH FIRST NULL ROWS ONLY","missing","missing","","",true},
        {"FETCH FIRST ((NULL)) ROWS WITH TIES","missing","missing","","",true,true},
        {"FETCH FIRST (NULL::bigint) ROWS WITH TIES","B:::(L:null,L:bigint)","missing","NULL::bigint","",true,true},
        {"FETCH FIRST CAST(NULL AS bigint) ROWS WITH TIES","CAST:bigint(L:null)","missing","CAST(NULL AS bigint)","",true,true},
        {"OFFSET 1+rows LIMIT $1","P:1:input","B:+(L:1,C:rows)","$1","1+rows"},
        {"FETCH FIRST -1 ROWS ONLY","missing","missing","","",true,false,{}, {},-1},
        {"LIMIT -1 OFFSET -2","missing","missing","","",false,false,{}, {},{},-1,-2}
    };
    for(size_t form=0;form<roots.size();++form)for(const auto& item:valid)for(bool binding:{false,true}){
        const auto sql=roots[form]+" "+item.tail;auto parsed=parse(sql,binding);const auto* root=query(parsed.stmt.get());
        const auto label=std::to_string(form)+":"+std::to_string(binding)+":"+item.tail;
        check(parsed.isValid() && root,label+":accepted original query "+parsed.error);
        check(originalRootRole(root,form),label+":genuine root role including inlineValues parenthesized set");
        check(root && shape(root->limitExpr.get())==item.limit && shape(root->offsetExpr.get())==item.offset,
            label+":real count trees and StatementInput parameter owner");
        check(root && root->limit==item.cap && root->offset==item.skip &&
            root->signedFetchCount==item.signedFetch && root->signedLimitCount==item.signedLimit &&
            root->signedOffsetCount==item.signedOffset,label+":only original simple literals retain signed/cap fields");
        check(root && root->fetchFirst==item.fetch && root->withTies==item.ties && !CountErrorRole<ParseResult>::get(parsed),
            label+":FETCH role and rawNULL distinct from typed computed count");
        const auto clause=item.fetch?"FETCH FIRST":"LIMIT";
        // FETCH NEXT has the same actual source tree; use its genuine clause.
        const auto actualClause=item.tail.rfind("FETCH NEXT",0)==0?"FETCH NEXT":clause;
        check(root && span(root->limitExpr.get(),sql,actualClause,item.limitBytes,binding) &&
            span(root->offsetExpr.get(),sql,"OFFSET",item.offsetBytes,binding),label+":exact original count byte spans");
    }
    const std::vector<std::string> invalid{
        "LIMIT (1+)","OFFSET 1+1 ROWS","FETCH FIRST 1+1 ROWS ONLY","FETCH FIRST NULL::bigint ROWS ONLY",
        "FETCH FIRST -$1 ROWS ONLY","FETCH FIRST CASE WHEN true THEN 1 ROWS ONLY",
        "FETCH FIRST (2) ONLY","FETCH FIRST 1 ROWS ONLY LIMIT 2"
    };
    for(const auto& base:roots)for(const auto& tail:invalid)for(bool binding:{false,true}){
        auto result=parse(base+" "+tail,binding);const auto label=std::to_string(binding)+":"+tail;
        check(!result.isValid() && !result.error.empty(),label+":raw grammar rejection");
        check(result.sqlState=="42601" && CountErrorRole<ParseResult>::get(result),label+":structured actual count-owner diagnostic");
    }
    for(const std::string sql:{"SELECT 1 LIMIT (1+1)","SELECT 1 OFFSET ($1)",
            "SELECT 1 FETCH FIRST (1+1) ROWS ONLY","(SELECT 1) FETCH FIRST ($1) ROWS ONLY"})
        for(bool binding:{false,true}){
            const auto parsed=parse(sql,binding);
            check(!parsed.isValid() && !parsed.error.empty(),sql+":standalone SELECT unsupported policy retained");
            check(!CountErrorRole<ParseResult>::get(parsed),sql+":does not publish a new set/VALUES diagnostic owner");
        }
    const std::vector<std::string> nested{
        "WITH c AS(VALUES(2)) (VALUES(3) UNION ALL VALUES(1)) ORDER BY 1 FETCH FIRST ($1) ROWS ONLY",
        "(VALUES(3) UNION ALL VALUES(1)) ORDER BY 1 OFFSET ($1) ROWS FETCH FIRST ($2) ROWS ONLY",
        "((SELECT 3 AS n UNION ALL SELECT 1)) ORDER BY 1 LIMIT ($1+1)",
        "(VALUES(3) UNION ALL VALUES(1)) ORDER BY 1 FETCH FIRST (VALUES(1)) ROWS ONLY"
    };
    for(size_t index=0;index<nested.size();++index)for(bool binding:{false,true}){
        const auto& sql=nested[index];
        auto parsed=parse(sql,binding);const auto* root=query(parsed.stmt.get());
        check(parsed.isValid() && root,sql+":nested original query accepted "+parsed.error);
        check(root && root->setOp==SetOp::Union && root->setOpAll && root->setOpRhs &&
            (root->limitExpr || root->offsetExpr),sql+":count belongs to complete actual set root");
        bool cteOwner=root && root->ctes.empty();
        if(index==0 && root && root->ctes.size()==1){
            const auto& cte=root->ctes[0];const auto* child=query(cte.query.get());
            const auto begin=sql.find("VALUES(2)");
            const bool bytes=binding ? cte.queryBegin==begin && cte.queryEnd==begin+9 &&
                sql.substr(cte.queryBegin,cte.queryEnd-cte.queryBegin)=="VALUES(2)" :
                cte.queryBegin==std::string::npos && cte.queryEnd==std::string::npos;
            cteOwner=cte.name=="c" && child && child!=root && child->command==SqlCommand::Values &&
                child->valuesRows.size()==1 && child->valuesRows[0].size()==1 &&
                shape(child->valuesRows[0][0].get())=="L:2" && bytes;
        }
        check(root && root->orderBy.size()==1 && root->hasRowCountClause && cteOwner &&
            !CountErrorRole<ParseResult>::get(parsed),
            sql+":original local ORDER/count roles retained without SQL rewriting");
    }
    std::cout<<"QUERY_ROOT_COUNT_GRAMMAR_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    if(controls!=840)return 2;
    return failures?1:0;
}
