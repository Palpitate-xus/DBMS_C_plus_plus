#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

using namespace dbms;
namespace {
struct Trace { size_t creates=0,opens=0,reads=0,closes=0,restarts=0; };
class Rows final : public Operator {
    std::shared_ptr<Trace> trace_;
    PreparedQueryRows rows_;
    size_t position_=0;
    bool lateError_=false,closeError_=false;
public:
    Rows(std::shared_ptr<Trace> trace,PreparedQueryRows rows,bool lateError=false,bool closeError=false)
        :trace_(std::move(trace)),rows_(std::move(rows)),lateError_(lateError),closeError_(closeError) {}
    bool open() override {++trace_->opens;position_=0;return true;}
    bool next(std::string& display) override {
        if(position_==1 && lateError_)throw DbError("P0002","primary SQL failure");
        if(position_==rows_.size())return false;
        ++position_;++trace_->reads;display="not a typed datum";return true;
    }
    bool supportsStructuredRows() const override {return true;}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {row=rows_.at(position_-1);return true;}
    void close() override {++trace_->closes;if(closeError_)throw DbError("58030","secondary close failure");}
};
QueryBindingMetadata metadata() {
    QueryBindingMetadata result;
    result.relation=[](const std::string& name){return QueryRelationMetadata{"public",name,{{"id","integer"}}, {}};};
    result.setReturning=[](const FunctionCallExpr* call)->std::optional<QuerySetReturningBinding> {
        if(call->funcName!="unnest" || (!call->schema.empty() && call->schema!="pg_catalog"))return {};
        if(call->args.size()!=1)throw DbError("42883","unknown set-returning signature");
        const auto input=ExprHelper::inferParsedResultType(call->args.front().get());
        if(input.size()<2 || input.substr(input.size()-2)!="[]")throw DbError("42883","unknown set-returning signature");
        return QuerySetReturningBinding{QuerySetReturningBinding::Kind::Unnest,
            "builtin:pg_catalog.unnest(anyarray)",input.substr(0,input.size()-2)};
    };
    return result;
}
template<class F> void expect(const std::string& state,F action) {
    bool caught=false;try{action();}catch(const DbError& error){caught=error.sqlState()==state;}
    assert(caught);
}
PreparedQueryRows rows() {return {{ExprValue("integer","1")},{ExprValue("integer","2")},{ExprValue("integer","3")}};}
}
int main() {
    TypeRegistry::instance().bootstrap();StorageEngine owner;SQLParser parser;
    for(const auto& sql:{"SELECT 10>ANY(SELECT id FROM r)","SELECT 10 > ANY (SELECT id FROM r)",
            "SELECT 10>SOME(ARRAY[1,2])","SELECT 10>ALL(ARRAY[1,2])"}) {
        const auto parsed=parser.parseForBinding(sql);assert(parsed.isValid());
        const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
        assert(dynamic_cast<const QuantifiedComparisonExpr*>(select->selectList.front().expr.get()));
        auto prepared=prepareQuery(sql,{},metadata());assert(prepared.output.front().type=="boolean");
    }
    expect("42601",[&]{(void)prepareQuery("SELECT 1=ANY(SELECT id,id FROM r)",{},metadata());});
    expect("42883",[&]{(void)prepareQuery("SELECT 1=ANY(ARRAY['bad']) WHERE FALSE",{},metadata());});
    expect("42809",[&]{(void)prepareQuery("SELECT 1=ANY(1)",{},metadata());});
    expect("42883",[&]{(void)ExprEvaluator::resolveComparison("=","xml","xml");});
    assert(ExprEvaluator::resolveComparison("=","integer","bigint").hashable);
    assert(!ExprEvaluator::resolveComparison(">","integer","integer").hashable);
    // The signature is chosen from types before values are seen. Floating
    // cross operators promote a rounded float4, and integer/float8 chooses
    // PostgreSQL's float8 signature rather than comparing decimal spellings.
    ExprEvaluator comparator;
    for(const auto& control:std::vector<std::tuple<ExprValue,ExprValue,std::string,bool>>{
        {ExprValue("bigint","9007199254740993"),ExprValue("double precision","9007199254740992"),"=",true},
        {ExprValue("bigint","9007199254740993"),ExprValue("double precision","9007199254740992"),">",false},
        {ExprValue("real","1e20"),ExprValue("double precision","1e20"),"=",false},
        {ExprValue("real","1e20"),ExprValue("double precision","1e20"),">",true},
        {ExprValue("integer","16777217"),ExprValue("real","16777216"),"=",false},
        {ExprValue("numeric","9007199254740993"),ExprValue("double precision","9007199254740992"),"=",true},
        {ExprValue("interval","1 mon"),ExprValue("interval","30 days"),"=",true},
        {ExprValue("interval","1 mon"),ExprValue("interval","29 days"),">",true},
        {ExprValue("interval","1 day"),ExprValue("interval","24 hours"),"=",true},
        {ExprValue("name","1"),ExprValue("name","01"),"=",false},
        {ExprValue("name","10"),ExprValue("name","2"),">",false},
        {ExprValue("name","1"),ExprValue("text","01"),"=",false},
        {ExprValue("text","1"),ExprValue("name","01"),"=",false},
        {ExprValue("real","NaN"),ExprValue("double precision","NaN"),"=",true},
        {ExprValue("real","NaN"),ExprValue("double precision","Infinity"),">",true}}) {
        const auto& left=std::get<0>(control);const auto& right=std::get<1>(control);
        const auto binding=ExprEvaluator::resolveComparison(std::get<2>(control),left.typeName,right.typeName);
        const auto truth=comparator.comparePrepared(binding,left,right);
        assert(!truth.isNull && truth.asBool()==std::get<3>(control));
        if(binding.hashable && truth.asBool())
            assert(ExprEvaluator::comparisonHashKey(binding,comparator.coerceComparison(binding,left,true),true)==
                ExprEvaluator::comparisonHashKey(binding,comparator.coerceComparison(binding,right,false),false));
    }
    const auto signedZero=ExprEvaluator::resolveComparison("=","real","double precision");
    assert(ExprEvaluator::comparisonHashKey(signedZero,ExprValue("real","-0"),true)==
           ExprEvaluator::comparisonHashKey(signedZero,ExprValue("double precision","0"),false));
    // Shared typed equality (not a Quant-only special case) also serves
    // simple CASE and ordinary binary comparisons.
    auto intervalComparison=parser.parse("SELECT INTERVAL '1 mon'=INTERVAL '30 days'");
    assert(intervalComparison.isValid());
    assert(comparator.eval(static_cast<SelectStmt*>(intervalComparison.stmt.get())->selectList.front().expr.get(),RowContext{}).asBool());
    const auto intervalCase=ExprHelper::evalString("CASE INTERVAL '1 mon' WHEN INTERVAL '30 days' THEN 1 ELSE 2 END",{},{},"","",&owner);
    assert(intervalCase.ok && intervalCase.value=="1");
    expect("42P21",[&]{(void)prepareQuery("SELECT 'x' COLLATE \"C\"=ANY(ARRAY['x' COLLATE \"POSIX\"]) WHERE FALSE",{},metadata());});
    expect("0A000",[&]{(void)prepareQuery("SELECT 1 WHERE unnest(ARRAY[1])=1",{},metadata());});
    // A genuine ProjectSet node has typed NULLs and row demand. Its array is
    // an evaluated datum; display text and SQL token replacement play no role.
    for(const auto& control:std::vector<std::pair<std::string,PreparedQueryRows>>{
        {"SELECT unnest(ARRAY[1,NULL,2])",{{ExprValue("integer","1")},{ExprValue("integer","",true)},{ExprValue("integer","2")}}},
        {"SELECT unnest(NULL::INT[])",{}},
        {"SELECT unnest(ARRAY[1,2]) LIMIT 0",{}},
        {"SELECT unnest(ARRAY[1,2]) WHERE FALSE",{}},
        {"SELECT unnest(ARRAY[1,2,3]) OFFSET 1 LIMIT 1",{{ExprValue("integer","2")}}}}) {
        auto prepared=std::make_shared<PreparedQuery>(prepareQuery(control.first,{},metadata()));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&owner,"unused",prepared,prepared->ast.get());
        auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),prepared->output);
        assert(cursor->descriptor().size()==1 && cursor->descriptor().front().type=="integer");
        for(const auto& expected:control.second) {
            std::vector<ExprValue> actual;assert(cursor->next(actual));
            assert(actual.size()==1 && actual.front().typeName==expected.front().typeName &&
                actual.front().isNull==expected.front().isNull &&
                (actual.front().isNull || actual.front().value==expected.front().value));
        }
        std::vector<ExprValue> noRow;assert(!cursor->next(noRow));cursor->close();
    }
    // Real descriptor cells, not a display or row-count based evaluator.
    for(const auto& control:std::vector<std::tuple<std::string,size_t,std::string>>{
            {"10>ANY",1,"t"},{"0>ALL",1,"f"},{"0>ANY",3,"f"},{"10>ALL",3,"t"},
            {"1=ANY",3,"t"},{"1=SOME",3,"t"}}) {
        auto query=std::make_shared<PreparedQuery>(prepareQuery("SELECT "+std::get<0>(control)+"(SELECT id FROM r)",{},metadata()));
        auto* select=static_cast<SelectStmt*>(query->ast.get());
        PreparedQueryExecution runtime(query,&owner,"unused");runtime.prepareExpression(select->selectList.front().expr.get());
        auto trace=std::make_shared<Trace>();
        runtime.setChildCursorFactory([&](const Stmt* child,const RowContext&){
            ++trace->creates;assert(query->statementOutputs.at(child).size()==1);
            return QueryPlanner::makePreparedCursor(std::make_unique<Rows>(trace,rows()),query->statementOutputs.at(child));
        });
        runtime.prepareChildCursors();assert(trace->creates==1 && trace->opens==0 && trace->reads==0);
        const auto truth=runtime.evaluate(select->selectList.front().expr.get(),runtime.context());
        assert(truth.typeName=="boolean" && !truth.isNull && truth.value==std::get<2>(control));
        assert(trace->reads==std::get<1>(control));runtime.closeChildCursors();assert(trace->closes==1);
    }
    // An uncorrelated scan's retained spool resumes and reuses without
    // reexecuting a row or converting an actual site into a global memo.
    for(const bool resume:{false,true}) {
        auto query=std::make_shared<PreparedQuery>(prepareQuery("SELECT id>ANY(SELECT id FROM r) FROM r",{},metadata()));
        auto* select=static_cast<SelectStmt*>(query->ast.get());auto* expression=select->selectList.front().expr.get();
        PreparedQueryExecution runtime(query,&owner,"unused");runtime.prepareExpression(expression);
        auto trace=std::make_shared<Trace>();
        runtime.setChildCursorFactory([&](const Stmt* child,const RowContext&){++trace->creates;return QueryPlanner::makePreparedCursor(std::make_unique<Rows>(trace,rows()),query->statementOutputs.at(child));});
        runtime.prepareChildCursors();
        auto context=runtime.context();const auto source=std::find_if(query->sourceRanges.begin(),query->sourceRanges.end(),[&](const auto& range){return range.owner==select;});
        assert(source!=query->sourceRanges.end());runtime.setSourceRow(context,source->ordinal,{ExprValue("integer","10")});
        assert(runtime.evaluate(expression,context).value=="t");
        runtime.setSourceRow(context,source->ordinal,{ExprValue("integer",resume?"0":"10")});
        assert(runtime.evaluate(expression,context).value==(resume?"f":"t"));
        assert(trace->creates==1 && trace->opens==1 && trace->reads==(resume?3:1));runtime.closeChildCursors();assert(trace->closes==1);
    }
    for(const bool hash:{false,true}) {
        auto query=std::make_shared<PreparedQuery>(prepareQuery(hash?"SELECT 1=ANY(SELECT id FROM r)":"SELECT 10>ANY(SELECT id FROM r)",{},metadata()));
        auto* select=static_cast<SelectStmt*>(query->ast.get());PreparedQueryExecution runtime(query,&owner,"unused");
        auto trace=std::make_shared<Trace>();runtime.prepareExpression(select->selectList.front().expr.get());
        runtime.setChildCursorFactory([&](const Stmt* child,const RowContext&){++trace->creates;return QueryPlanner::makePreparedCursor(std::make_unique<Rows>(trace,rows(),true,hash),query->statementOutputs.at(child));});
        runtime.prepareChildCursors();
        if(hash)expect("P0002",[&]{runtime.evaluate(select->selectList.front().expr.get(),runtime.context());});
        else {assert(runtime.evaluate(select->selectList.front().expr.get(),runtime.context()).value=="t");runtime.closeChildCursors();}
        assert(trace->reads==1 && trace->closes==1);
    }
    // The public cursor's close preserves a descriptor, primary SQLSTATE and
    // cleanup priority. Scalar helpers also prepare real scalar-array nodes.
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
            {"1=ANY(ARRAY[NULL,1])","t"},{"1=ALL(ARRAY[NULL,2])","f"},
            {"NULL::INT=ALL(ARRAY[]::INT[])","t"}}) {
        const auto result=ExprHelper::evalString(control.first,{}, {},"","",&owner);
        assert(result.ok && result.typeName=="boolean" && !result.isNull && result.value==control.second);
    }
    QueryBindingDatum parameter{"n","needle","integer",{},true,std::nullopt,1};
    auto query=std::make_shared<PreparedQuery>(prepareQuery("SELECT $1=ANY(ARRAY[]::INT[])",{parameter},metadata()));
    auto* select=static_cast<SelectStmt*>(query->ast.get());PreparedQueryExecution runtime(query,&owner,"unused");
    runtime.prepareExpression(select->selectList.front().expr.get());
    const auto empty=runtime.evaluate(select->selectList.front().expr.get(),runtime.context());
    assert(empty.value=="f" && !empty.isNull && query->parameters[0].isNull);
    // The cursor consumes canonical physical identities, not SQL-rendered
    // qualified/quoted names. Storage APIs take the stored identifier.
    const auto database=testDbPath("quantified_physical_child");
    assert(owner.createDatabase(database,"utf8")==DBStatus::OK);
    TableSchema physical;physical.len=1;physical.cols[0].dataName="id";
    physical.cols[0].dataType="int";physical.cols[0].dsize=4;
    Session session;session.pid=991;session.tempTables.insert("trows");
    struct RestoreSession { Session* previous;~RestoreSession(){setCurrentSession(previous);} } restore{currentSession()};
    setCurrentSession(&session);
    for(const auto& table:{"physical_rows","scope__rows","__tmp_991_trows"}) {
        physical.isTemporary=std::string(table)=="__tmp_991_trows";
        assert(owner.createTable(database,table,physical)==DBStatus::OK);
        for(int value:{1,2,3})assert(owner.insertRow(database,table,{{"id",std::to_string(value)}})==DBStatus::OK);
    }
    for(const auto& table:{"physical_rows","scope.rows","pg_temp.trows"}) {
        const auto execute=[&](const std::string& projection) {
            auto prepared=std::make_shared<PreparedQuery>(owner.prepareBoundQuery(database,"SELECT "+projection));
            return QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedQueryPlan(&owner,database,prepared,prepared->ast.get()));
        };
        auto zero=execute("(SELECT id FROM "+std::string(table)+" WHERE id=99)");
        zero.throwIfFailed();assert(zero.structuredNulls==std::vector<std::vector<bool>>({{true}}));
        auto multi=execute("(SELECT id FROM "+std::string(table)+")");
        assert(!multi.ok && multi.errorSqlState=="21000" && multi.rows.empty());
        auto quantified=execute("2=ANY(SELECT id FROM "+std::string(table)+")");
        quantified.throwIfFailed();assert(quantified.structuredRows==std::vector<std::vector<std::string>>({{"t"}}));
    }
    std::cout<<"[QUANTIFIED QUERY EXECUTION] metadata/stream/spool/hash/NULL/errors/typed parameters passed\n";
}
