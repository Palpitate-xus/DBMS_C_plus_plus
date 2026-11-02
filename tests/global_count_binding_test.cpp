#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <functional>
#include <iostream>
#include <set>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;

struct Nodes {
    std::vector<const ParameterExpr*> parameters;
    std::vector<const ColumnRefExpr*> columns;
    std::vector<const SelectStmt*> queries;
    void expression(const Expr* value) {
        if(!value)return;
        if(const auto* parameter=dynamic_cast<const ParameterExpr*>(value))parameters.push_back(parameter);
        if(const auto* column=dynamic_cast<const ColumnRefExpr*>(value))columns.push_back(column);
        if(value->preparedSubquery)statement(value->preparedSubquery.get());
        if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(value)) {
            expression(binary->left.get());if(binary->op!="::")expression(binary->right.get());
        } else if(const auto* cast=dynamic_cast<const CastExpr*>(value))expression(cast->operand.get());
        else if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(value))expression(unary->operand.get());
        else if(const auto* call=dynamic_cast<const FunctionCallExpr*>(value)) {
            for(const auto& argument:call->args)expression(argument.get());
            for(const auto& argument:call->namedArgs)expression(argument.value.get());
            for(const auto& key:call->aggregateOrderKeys)expression(key.expr.get());
            expression(call->filter.get());
            for(const auto& field:call->over.partitionBy)expression(field.get());
            for(const auto& key:call->over.orderBy)expression(key.first.get());
            expression(call->over.frameStart.get());expression(call->over.frameEnd.get());
        } else if(const auto* conditional=dynamic_cast<const CaseExpr*>(value)) {
            expression(conditional->switchExpr.get());expression(conditional->elseExpr.get());
            for(const auto& arm:conditional->whenClauses){expression(arm.first.get());expression(arm.second.get());}
        } else if(const auto* array=dynamic_cast<const ArrayExpr*>(value))
            for(const auto& element:array->elements)expression(element.get());
        else if(const auto* row=dynamic_cast<const RowExpr*>(value))
            for(const auto& element:row->elements)expression(element.get());
    }
    void source(const FromItem* item) {
        if(!item)return;
        source(item->left.get());source(item->right.get());expression(item->joinCondition.get());
        statement(item->subquery.get());
    }
    void statement(const Stmt* value) {
        if(const auto* envelope=dynamic_cast<const WithStmt*>(value)) {
            for(const auto& cte:envelope->ctes)statement(cte.query.get());statement(envelope->statement.get());return;
        }
        const auto* query=dynamic_cast<const SelectStmt*>(value);if(!query)return;
        queries.push_back(query);
        for(const auto& cte:query->ctes)statement(cte.query.get());
        statement(query->setOpLhs.get());statement(query->setOpRhs.get());source(query->fromClause.get());
        for(const auto& target:query->selectList)expression(target.expr.get());
        for(const auto& row:query->valuesRows)for(const auto& value:row)expression(value.get());
        expression(query->whereClause.get());expression(query->having.get());
        for(const auto& key:query->orderBy)expression(key.expr.get());
        expression(query->offsetExpr.get());expression(query->limitExpr.get());
    }
};

std::vector<QueryBindingDatum> inputs(size_t count,ParameterOrigin origin=ParameterOrigin::MetadataPlaceholder) {
    std::vector<QueryBindingDatum> result;
    for(size_t i=0;i<count;++i) {
        QueryBindingDatum datum;datum.identity="global-count-input-"+std::to_string(i+1);
        datum.position=i+1;datum.type=origin==ParameterOrigin::MetadataPlaceholder?"unknown":"integer";
        datum.origin=origin;if(origin!=ParameterOrigin::MetadataPlaceholder)datum.value="7";
        result.push_back(std::move(datum));
    }
    return result;
}

bool metadata(const std::string& database,const std::string& sql,const std::vector<std::string>& types) {
    const auto declarations=inputs(types.size());
    auto query=g_engine.prepareBoundQuery(database,sql,declarations);
    Nodes nodes;nodes.statement(query.ast.get());
    if(query.parameters.size()!=types.size() || query.uses.size()!=nodes.parameters.size() || !query.sourceRanges.empty())return false;
    for(const auto& input:declarations)if(input.type!="unknown" || input.value || input.origin!=ParameterOrigin::MetadataPlaceholder)return false;
    std::set<size_t> slots;
    for(const auto& use:query.uses) {
        if(use.slot>=query.parameters.size() || use.begin>=use.end || use.end>sql.size())return false;
        const auto spelling=sql.substr(use.begin,use.end-use.begin);
        if(spelling.empty() || spelling.front()!='$')return false;
        const auto ordinal=std::stoul(spelling.substr(1));
        if(!ordinal || ordinal>types.size() || query.parameters[use.slot].typeName!=types[ordinal-1] ||
           !query.parameters[use.slot].isNull || !query.parameters[use.slot].value.empty())return false;
        slots.insert(use.slot);
    }
    if(slots.size()!=types.size())return false;
    for(const auto* node:nodes.parameters)
        if(node->origin!=ParameterOrigin::MetadataPlaceholder || node->slot>=query.parameters.size() ||
           node->declaredType!=query.parameters[node->slot].typeName)return false;
    return true;
}

bool rejects(const std::string& database,const std::string& sql,const std::string& state,size_t count=0) {
    try{(void)g_engine.prepareBoundQuery(database,sql,inputs(count));}
    catch(const DbError& error){return error.sqlState()==state;}
    return false;
}
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();const auto database=testDbPath("global_count_binding");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& test) {
        ++controls;bool good=false;
        try{good=test();}catch(const DbError& error){std::cout<<"state="<<error.sqlState()<<" message="<<error.what()<<'\n';}
        catch(const std::exception& error){std::cout<<"exception="<<error.what()<<'\n';}
        failures+=!good;std::cout<<"GLOBAL_COUNT_BINDING "<<controls<<' '<<label<<" good="<<good<<'\n';
    };
    const std::string base="VALUES(3),(1),(2) UNION ALL VALUES(4) ORDER BY 1";
    struct Inference {std::string sql;std::vector<std::string> types;};
    const std::vector<Inference> inference={
        {base+" LIMIT $1",{"bigint"}},
        {base+" LIMIT ($1+0)",{"integer"}},
        {base+" LIMIT ($1::text)::bigint",{"text"}},
        {base+" LIMIT COALESCE($1,1)",{"integer"}},
        {base+" LIMIT $1 OFFSET $2",{"bigint","bigint"}},
        {base+" LIMIT $1 OFFSET ($1+0)",{"integer"}},
        {base+" LIMIT ($1+0) OFFSET $1",{"bigint"}},
        {"SELECT $1 AS k UNION ALL SELECT 2 ORDER BY k LIMIT $1",{"integer"}},
        {base+" LIMIT CASE WHEN true THEN $1 ELSE 1 END",{"integer"}},
        {base+" LIMIT ($1+1.0)",{"numeric"}},
        {"SELECT $1 AS x UNION ALL SELECT $1 LIMIT $2",{"text","bigint"}}
    };
    for(const auto& value:inference)check("actual selected input and all original uses: "+value.sql,[&]{return metadata(database,value.sql,value.types);});
    for(const auto& value:std::vector<std::pair<std::string,std::string>>{
        {base+" LIMIT $1 OFFSET ($1::text)::bigint","42804"},
        {base+" LIMIT ($1+$2)","42725"},
        {base+" LIMIT $1 OFFSET (SELECT $1)","42804"}})
        check("unknown constraints reject "+value.first,[&]{return rejects(database,value.first,value.second,value.first.find("$2")!=std::string::npos?2:1);});
    for(const auto& value:std::vector<std::pair<std::string,std::string>>{
        {base+" LIMIT column1","42703"},{base+" LIMIT (SELECT column1)","42703"},
        {"SELECT 3 AS k UNION ALL SELECT 1 ORDER BY k LIMIT k","42703"},
        {"SELECT p.x FROM(VALUES(3))p(x) UNION ALL VALUES(1) LIMIT p.x","42P01"},
        {base+" LIMIT 0 OFFSET column1","42703"},
        {"VALUES(3),(1) LIMIT column1","42P10"},
        {"VALUES(3),(1) LIMIT(SELECT column1)","42P10"},
        {base+" LIMIT (SELECT column1 FROM(VALUES(1))s(x))","42703"}})
        check("actual namespace "+value.first,[&]{return rejects(database,value.first,value.second);});
    for(const auto& value:std::vector<std::pair<std::string,size_t>>{
        {"SELECT p.x,(VALUES(10),(20) UNION ALL VALUES(30) LIMIT p.x%2) FROM(VALUES(2),(3))p(x)",1},
        {"SELECT p.x,(SELECT(VALUES(10),(20) UNION ALL VALUES(30) LIMIT p.x%2)) FROM(VALUES(2),(3))p(x)",2},
        {"SELECT p.column1,(VALUES(10),(20) UNION ALL VALUES(30) LIMIT column1%2) FROM(VALUES(2),(3))p(column1)",1}})
        check("genuine ancestor source depth "+std::to_string(value.second),[&] {
            auto query=g_engine.prepareBoundQuery(database,value.first,{});Nodes nodes;nodes.statement(query.ast.get());
            bool found=false;
            for(const auto* column:nodes.columns)if(column->binding && column->binding->scopeDepth==value.second) {
                const auto source=std::find_if(query.sourceRanges.begin(),query.sourceRanges.end(),[&](const auto& range){return range.ordinal==column->binding->sourceOrdinal;});
                if(source==query.sourceRanges.end() || !source->source)return false;
                found=true;
            }
            for(const auto* node:nodes.queries)if(node->setOp!=SetOp::None)
                for(const auto& range:query.sourceRanges)if(range.owner==node)return false;
            return found;
        });
    for(const auto& value:std::vector<std::pair<std::string,std::string>>{
        {base+" LIMIT sum(1)","42803"},{base+" LIMIT row_number() OVER()","42P20"},
        {base+" LIMIT generate_series(1,2)","0A000"},
        {"VALUES(3),(1) LIMIT(SELECT sum(column1))","42803"}})
        check("real count routine role "+value.first,[&]{return rejects(database,value.first,value.second);});
    check("raw NULL TIES rejected during binding",[&]{return rejects(database,base+" FETCH FIRST NULL ROWS WITH TIES","2201W");});
    for(const auto& expression:std::vector<std::string>{"(NULL::bigint)","(CASE WHEN true THEN NULL::bigint ELSE 1 END)"})
        check("computed NULL retains real expression "+expression,[&] {
            auto query=g_engine.prepareBoundQuery(database,base+" FETCH FIRST "+expression+" ROWS WITH TIES",{});
            const auto* root=dynamic_cast<const SelectStmt*>(query.ast.get());
            return root && root->limitExpr && root->withTies;
        });
    for(auto origin:{ParameterOrigin::StatementInput,ParameterOrigin::RuntimeCell})
        check("existing nonmetadata declaration and source identity preserved",[&] {
            const auto declared=inputs(1,origin);auto query=g_engine.prepareBoundQuery(database,base+" LIMIT $1 OFFSET $1",declared);
            Nodes nodes;nodes.statement(query.ast.get());
            if(query.parameters.size()!=1 || query.parameters[0].typeName!="integer" ||
               query.parameters[0].isNull || query.parameters[0].value!="7" || nodes.parameters.size()!=2)return false;
            for(const auto* node:nodes.parameters)if(node->slot || node->origin!=origin || node->declaredType!="integer")return false;
            return declared.front().type=="integer" && declared.front().value=="7";
        });
    setCurrentSession(previous);
    if(controls!=34){std::cout<<"unexpected controls="<<controls<<'\n';return 2;}
    std::cout<<"GLOBAL_COUNT_BINDING_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}
