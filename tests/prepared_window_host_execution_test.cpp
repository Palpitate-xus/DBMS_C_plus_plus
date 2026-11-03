#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "expression/prepared_query_execution.h"
#include "expression/expr_helper.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
using Rows=std::vector<std::vector<ExprValue>>;
bool same(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull || a.collation!=b.collation ||
       a.objectOid!=b.objectOid || a.elementOids!=b.elementOids || bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields){if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)if(!same(a.recordFields->at(i),b.recordFields->at(i)))return false;}
    return true;
}
bool same(const Rows& a,const Rows& b) {
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i){if(a[i].size()!=b[i].size())return false;
        for(size_t j=0;j<a[i].size();++j)if(!same(a[i][j],b[i][j]))return false;}
    return true;
}
bool values(const Rows& actual,const std::vector<std::vector<std::optional<std::string>>>& wanted) {
    if(actual.size()!=wanted.size())return false;
    for(size_t i=0;i<actual.size();++i){if(actual[i].size()!=wanted[i].size())return false;
        for(size_t j=0;j<actual[i].size();++j)if(wanted[i][j] ? actual[i][j].isNull || actual[i][j].value!=*wanted[i][j] : !actual[i][j].isNull)return false;}
    return true;
}
struct Fixture {std::shared_ptr<PreparedQuery> query;std::unique_ptr<PreparedQueryCursor> cursor;};
Fixture fixture(const std::string& db,const std::string& sql,const std::vector<QueryBindingDatum>& arguments={},const RowContext& caller={}) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql,arguments));
    return {query,QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get(),caller),query->output)};
}
Rows consume(PreparedQueryCursor& cursor) {Rows result;std::vector<ExprValue> row;while(cursor.next(row))result.push_back(row);cursor.close();cursor.close();return result;}
Rows execute(const std::string& db,const std::string& sql) {auto f=fixture(db,sql);return consume(*f.cursor);}
size_t inputSlot(const PreparedQuery& query,const std::string& spelling) {
    std::optional<size_t> result;
    for(const auto& use:query.uses)if(query.source.substr(use.begin,use.end-use.begin)==spelling){
        if(result && *result!=use.slot)throw DbError("XX000","input identity owns different slots");
        result=use.slot;}
    if(!result || *result>=query.parameters.size())throw DbError("XX000","missing actual input slot");
    return *result;
}
ExprPtr parameter(size_t index,const char* type) {auto value=std::make_unique<ParameterExpr>();value->slot=index;value->declaredType=type;value->origin=ParameterOrigin::RuntimeCell;return value;}
ExprValue record(size_t id) {
    ExprEvaluator evaluator;RowExpr empty;empty.constructor=true;auto zero=evaluator.eval(&empty,RowContext{});
    if(id==0)return zero;
    if(id==2)return ExprValue("record","",true);
    ExprValue text("text",id==1?"β":"");text.collation="c";
    RowExpr expression;expression.constructor=true;expression.fieldTypes={"text","record"};
    expression.elements.push_back(parameter(0,"text"));expression.elements.push_back(parameter(1,"record"));
    RowContext context;context.setParameters({text,zero});return evaluator.eval(&expression,context);
}
QueryRowDescriptor descriptor(){return {{"id","integer",false,0,23},{"x","bigint",false,0,20},{"payload","text",false,0,25},
    {"rec","record",false,0,2249},{"o","regtype",false,0,2206},{"arr","regtype[]",false,0,2211}};}
Rows inputRows() {
    Rows result;
    for(size_t i=0;i<6;++i){ExprValue payload("text",i==0?"":i==1?"NULL":"row β "+std::to_string(i));payload.collation="c";
        if(i==2){payload.value.clear();payload.isNull=true;}
        ExprValue object("regtype",i%2?"bigint":"boolean");object.objectOid=i%2?20:16;
        if(i==2){object.value.clear();object.isNull=true;object.objectOid.reset();}
        ExprValue array("regtype[]","{boolean,NULL,bigint}");array.elementOids={16,std::nullopt,20};
        if(i==1){array.value="{}";array.elementOids.clear();}else if(i==2){array.value.clear();array.isNull=true;array.elementOids.clear();}
        result.push_back({ExprValue("integer",std::to_string(i+1)),ExprValue("bigint",std::to_string(i+1)),std::move(payload),record(i),std::move(object),std::move(array)});}
    return result;
}
struct Probe {size_t reads=0,eof=0,opens=0,closes=0;};
class Source final:public Operator {
    std::shared_ptr<Probe> probe_;Rows data_;std::vector<ExprValue> row_;size_t index_=0;bool opened_=false;
public:
    Source(std::shared_ptr<Probe> probe,Rows data):probe_(std::move(probe)),data_(std::move(data)){}
    bool open()override{clearError();opened_=true;index_=0;row_.clear();++probe_->opens;return true;}
    bool next(std::string& out)override{if(!opened_)throw DbError("XX000","closed genuine source read");
        if(index_==data_.size()){++probe_->eof;return false;}row_=data_.at(index_++);++probe_->reads;out.clear();return true;}
    void close()override{if(opened_){opened_=false;row_.clear();++probe_->closes;}}
    bool supportsStructuredRows()const override{return true;}
    bool lastStructuredValues(std::vector<ExprValue>& row)const override{row=row_;return !row.empty();}
    bool supportsPreparedOuterRow()const override{return true;}
    bool bindPreparedOuterRow(const RowContext&)override{close();return true;}
};
struct Stream {std::shared_ptr<PreparedQuery> query;std::shared_ptr<Probe> probe;std::unique_ptr<PreparedQueryCursor> cursor;};
Stream stream(const std::string& db,const std::string& targets,const std::string& tail="",const std::vector<QueryBindingDatum>& arguments={}) {
    QueryBindingMetadata metadata;metadata.relation=[](const std::string& name){return QueryRelationMetadata{"public",name,descriptor(),{}};};
    // Reuse the real public type inference on the wholly bound actual call;
    // no hand-written row_number-only declaration or evaluator is installed.
    metadata.functionType=[&](const FunctionCallExpr* call){return ExprHelper::inferParsedResultType(call,{},db,&g_engine);};
    auto query=std::make_shared<PreparedQuery>(prepareQuery("SELECT "+targets+" FROM window_logical "+tail,arguments,metadata));
    auto probe=std::make_shared<Probe>();TableSchema schema;
    for(const auto& item:descriptor()){Column c;c.dataName=item.name;c.dataType=item.type;c.isVariableLength=true;schema.append(c);}
    auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,db,query,static_cast<SelectStmt*>(query->ast.get()),schema,std::make_unique<Source>(probe,inputRows()));
    return {query,probe,QueryPlanner::makePreparedCursor(std::move(plan),query->output)};
}
bool emptyBinding(){return StorageEngine::captureNullRowBinding().engine==nullptr;}
}
int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();const auto db=testDbPath("prepared_window_host");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);DdlExecutor ddl;size_t controls=0,failures=0,sequenceIndex=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action){++controls;bool good=false;
        try{good=action();}catch(const DbError& error){std::cout<<"SQLSTATE="<<error.sqlState()<<" message="<<error.message()<<'\n';}
        catch(const std::exception& error){std::cout<<"exception="<<error.what()<<'\n';}
        good=good && emptyBinding();failures+=!good;std::cout<<"PREPARED_WINDOW_HOST "<<controls<<' '<<label<<" good="<<good<<'\n';};
    const auto sequence=[&]{const auto name="wh_native_seq_"+std::to_string(sequenceIndex++);if(ddl.executeSql("CREATE SEQUENCE "+name,session) || g_engine.nextval(db,name)!=1)throw DbError("XX000","sequence fixture failed");return name;};
    const bool created=!ddl.executeSql("CREATE TABLE wh_native(id INTEGER,p INTEGER,v BIGINT)",session);
    const std::vector<std::string> source{"9007199254740993","9007199254740994","","-9007199254740993","-9007199254740994","-9007199254740994"};
    bool inserted=created;
    for(size_t i=0;i<source.size();++i)inserted=(g_engine.insertRow(db,"wh_native",{{"id",std::to_string(i+1)},{"p",i<3?"0":"1"},{"v",i==2?std::optional<std::string>{}:std::optional<std::string>{source[i]}}})==DBStatus::OK) && inserted;
    if(!inserted)++failures;
    check("actual physical mixed ranking and exact bigint peers",[&]{return values(execute(db,"SELECT id,row_number() OVER(PARTITION BY p ORDER BY id),rank() OVER(PARTITION BY p ORDER BY v NULLS LAST),dense_rank() OVER(PARTITION BY p ORDER BY v NULLS LAST),lag(v) OVER(PARTITION BY p ORDER BY id) FROM wh_native ORDER BY id"),
        {{"1","1","1","1",{}},{"2","2","2","2","9007199254740993"},{"3","3","3","3","9007199254740994"},{"4","1","3","2",{}},{"5","2","1","1","-9007199254740993"},{"6","3","1","1","-9007199254740994"}});});
    check("genuine VALUES-derived window source",[&]{return values(execute(db,"SELECT id,lag(v) OVER(ORDER BY id),last_value(v) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM(VALUES(1,9007199254740993::bigint),(2,9007199254740994::bigint)) AS s(id,v) ORDER BY id"),{{"1",{},"9007199254740994"},{"2","9007199254740993","9007199254740994"}});});
    check("genuine CTE source and named inherited frame",[&]{return values(execute(db,"WITH q AS(SELECT id,v FROM wh_native WHERE p=0) SELECT id,lag(v) OVER w2,last_value(v) OVER w2 FROM q WINDOW w1 AS(ORDER BY id),w2 AS(w1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY id"),{{"1",{},{}},{"2","9007199254740993",{}},{"3","9007199254740994",{}}});});
    check("genuine SourceContexts JOIN retains runtime frames and source occurrences",[&]{
        const auto source=execute(db,"SELECT id,p,v FROM wh_native ORDER BY id");
        QueryBindingDatum parameterValue{"context-window-p","p","regtype",{},false,"integer",1};parameterValue.origin=ParameterOrigin::RuntimeCell;
        const std::string sql="SELECT a.id,b.id,row_number() OVER(ORDER BY a.id,b.id),lag(a.v) OVER(ORDER BY a.id,b.id),lag(b.v,0) OVER(ORDER BY a.id,b.id),lag($1,0) OVER(ORDER BY a.id,b.id),lag(current_user,0) OVER(ORDER BY a.id,b.id) FROM wh_native a JOIN wh_native b ON a.id+3=b.id WHERE a.p=0";
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql,{parameterValue}));
        const auto parameterIndex=inputSlot(*query,"$1");std::optional<size_t> a,b;
        for(const auto& range:query->sourceRanges){if(range.name=="a")a=range.ordinal;if(range.name=="b")b=range.ordinal;}
        if(!a || !b || *a==*b || source.size()!=6)return false;
        auto closed=std::make_shared<size_t>(0);auto reads=std::make_shared<size_t>(0);
        // These are actual matched rows of the two genuine stored sources.
        // The public SourceContexts API represents the logical JOIN result;
        // it carries true bound occurrences and no invented heap/RID origin.
        auto contexts=std::make_unique<PreparedSourceContextsOp>(
            [query,source,a=*a,b=*b,parameterIndex,reads](size_t index,RowContext& row){
                if(index>=3)return false;
                ++*reads;row=RowContext{};
                for(size_t column=0;column<3;++column){row.setBoundColumn(a,column,source[index][column]);row.setBoundColumn(b,column,source[index+3][column]);}
                ExprValue identity("regtype",index%2?"bigint":"boolean");identity.objectOid=index%2?20:16;
                row.set("legacy",identity);row.setSqlValue(FunctionCallExpr::SqlValue::CurrentUser,ExprValue("name","caller_"+std::to_string(index+1)));
                auto frame=query->parameters;frame.at(parameterIndex)=identity;row.setParameters(std::move(frame));return true;
            },[closed]{++*closed;});
        auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,db,query,static_cast<SelectStmt*>(query->ast.get()),TableSchema{},std::move(contexts));
        auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),query->output);const auto actual=consume(*cursor);Rows wanted;
        for(size_t index=0;index<3;++index){ExprValue identity("regtype",index%2?"bigint":"boolean");identity.objectOid=index%2?20:16;
            wanted.push_back({source[index][0],source[index+3][0],ExprValue("bigint",std::to_string(index+1)),index?source[index-1][2]:ExprValue("bigint","",true),source[index+3][2],identity,ExprValue("name","caller_"+std::to_string(index+1))});}
        return same(actual,wanted) && *reads==3 && *closed==1 && query->parameters[parameterIndex].value=="integer";
    });
    check("real ancestor scalar-window child and parent projection",[&]{return values(execute(db,"SELECT a.id,(SELECT lag(a.v,0) OVER()),a.v FROM wh_native a ORDER BY a.id"),{{"1","9007199254740993","9007199254740993"},{"2","9007199254740994","9007199254740994"},{"3",{},{}},{"4","-9007199254740993","-9007199254740993"},{"5","-9007199254740994","-9007199254740994"},{"6","-9007199254740994","-9007199254740994"}});});
    check("window scalar child used as actual count",[&]{return values(execute(db,"VALUES(3),(1),(2) ORDER BY column1 LIMIT(SELECT row_number() OVER(ORDER BY id) FROM wh_native ORDER BY id LIMIT 1)"),{{"1"}});});
    check("scalar cardinality remains21000",[&]{auto f=fixture(db,"SELECT(SELECT row_number() OVER(ORDER BY id) FROM wh_native)");std::vector<ExprValue> row;std::string state;try{f.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}f.cursor->close();return state=="21000" && row.empty();});
    struct Demand {const char* name;const char* expression;size_t reads;};
    const std::vector<Demand> demand={{"row_number","row_number() OVER()",1},{"rank","rank() OVER()",1},{"dense_rank","dense_rank() OVER()",1},
        {"lag","lag(x) OVER()",1},{"lead","lead(x) OVER()",2},{"lead2","lead(x,2) OVER()",3},{"first","first_value(x) OVER()",1},
        {"last","last_value(x) OVER()",6},{"nth2","nth_value(x,2) OVER()",2},{"nthNULL","nth_value(x,NULL::integer) OVER()",1},
        {"ntile","ntile(2) OVER()",6},{"ntileNULL","ntile(NULL::integer) OVER()",6},{"percent","percent_rank() OVER()",6},
        {"cume","cume_dist() OVER()",6},{"sumFull","sum(x) OVER()",6},{"sumPrefix","sum(x) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",2},
        {"sumFollow","sum(x) OVER(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",3}};
    for(const auto& item:demand)check(std::string("PG exact source demand LIMIT1 ")+item.name,[&]{auto f=stream(db,std::string("id,")+item.expression,"LIMIT 1");std::vector<ExprValue> row;
        bool good=f.probe->reads==0 && f.cursor->next(row) && row.size()==2 && row[0].value=="1" && f.probe->reads==item.reads;
        f.cursor->close();f.cursor->close();return good && f.probe->closes==1 && f.probe->reads==item.reads;});
    check("zero prevents source open and invalid ntile evaluation",[&]{auto f=stream(db,"ntile(0) OVER()","LIMIT 0");return consume(*f.cursor).empty() && !f.probe->opens && !f.probe->reads;});
    check("complete owning NULL record OID arrays survive stage snapshots",[&]{auto f=stream(db,"id,lag(x,0) OVER(ORDER BY id),first_value(payload) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW),nth_value(rec,1) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW),lead(o,0) OVER(ORDER BY id),last_value(arr) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW)","ORDER BY id");return same(consume(*f.cursor),inputRows()) && f.probe->reads==6 && f.probe->closes==1;});
    for(const auto origin:{ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder,ParameterOrigin::StatementInput})check("same graph parameter origin "+std::to_string(static_cast<unsigned>(origin)),[&]{
        QueryBindingDatum argument{"offset","offset","integer",{},false,"1",1};argument.origin=origin;
        auto f=fixture(db,"SELECT id,lag(v,$1) OVER(ORDER BY id) FROM wh_native ORDER BY id",{argument});const auto index=inputSlot(*f.query,"$1");const auto* graph=f.cursor->plan();
        bool good=f.cursor->supportsRestart() && values(consume(*f.cursor),{{"1",{}},{"2","9007199254740993"},{"3","9007199254740994"},{"4",{}},{"5","-9007199254740993"},{"6","-9007199254740994"}});
        auto cells=f.query->parameters;cells[index]=ExprValue("integer","0");RowContext caller;caller.setParameters(std::move(cells));f.cursor->restart(caller);const auto actual=consume(*f.cursor);
        good=(origin==ParameterOrigin::StatementInput?values(actual,{{"1",{}},{"2","9007199254740993"},{"3","9007199254740994"},{"4",{}},{"5","-9007199254740993"},{"6","-9007199254740994"}}):values(actual,{{"1","9007199254740993"},{"2","9007199254740994"},{"3",{}},{"4","-9007199254740993"},{"5","-9007199254740994"},{"6","-9007199254740994"}})) && good;
        return good && f.cursor->plan()==graph && f.query->parameters[index].value=="1" && caller.parameter(index).value=="0";});
    check("uncorrelated default child uses one actual invocation memo",[&]{const auto name=sequence();auto f=fixture(db,"SELECT id,lag(v,1,(SELECT nextval('"+name+"'))) OVER(PARTITION BY p ORDER BY id) FROM wh_native ORDER BY id");
        bool good=g_engine.currval(db,name)==1 && values(consume(*f.cursor),{{"1","2"},{"2","9007199254740993"},{"3","9007199254740994"},{"4","2"},{"5","-9007199254740993"},{"6","-9007199254740994"}}) && g_engine.currval(db,name)==2;
        f.cursor->restart(RowContext{});return values(consume(*f.cursor),{{"1","3"},{"2","9007199254740993"},{"3","9007199254740994"},{"4","3"},{"5","-9007199254740993"},{"6","-9007199254740994"}}) && g_engine.currval(db,name)==3 && good;});
    check("same spec volatile sites stay independent",[&]{const auto name=sequence();return values(execute(db,"SELECT id,lag(nextval('"+name+"'),0) OVER(ORDER BY id),lag(nextval('"+name+"'),0) OVER(ORDER BY id) FROM wh_native ORDER BY id"),{{"1","2","3"},{"2","4","5"},{"3","6","7"},{"4","8","9"},{"5","10","11"},{"6","12","13"}}) && g_engine.currval(db,name)==13;});
    check("different specs follow actual DESC then ASC stages",[&]{const auto name=sequence();return values(execute(db,"SELECT id,lag(nextval('"+name+"'),0) OVER(ORDER BY id),lag(nextval('"+name+"'),0) OVER(ORDER BY id DESC) FROM wh_native ORDER BY id"),{{"1","8","7"},{"2","9","6"},{"3","10","5"},{"4","11","4"},{"5","12","3"},{"6","13","2"}}) && g_engine.currval(db,name)==13;});
    check("NULL offset skips both volatile argument sites",[&]{const auto value=sequence(),fallback=sequence();const auto rows=execute(db,"SELECT id,lag(nextval('"+value+"'),NULL::integer,nextval('"+fallback+"')) OVER(ORDER BY id) FROM wh_native ORDER BY id");return values(rows,{{"1",{}},{"2",{}},{"3",{}},{"4",{}},{"5",{}},{"6",{}}}) && g_engine.currval(db,value)==1 && g_engine.currval(db,fallback)==1;});
    check("runtime window error terminally closes actual source",[&]{auto f=stream(db,"id,nth_value(x,0) OVER(ORDER BY id)");auto* plan=f.cursor->plan();std::string out,state;bool good=plan->open();
        try{if(plan->next(out))good=false;}catch(const DbError& error){state=error.sqlState();}
        const auto reads=f.probe->reads;good=good && state=="22016" && f.probe->closes==1 && !plan->next(out) && f.probe->reads==reads;
        plan->close();plan->close();return good && f.probe->closes==1;});
    for(bool timeout:{false,true})check(timeout?"live timeout releases lazy window source":"live cancel releases lazy window source",[&]{
        const auto prior=currentQueryInterruptState();auto token=session.interruptState;token->queryActive.store(true);token->cancelRequested.store(false);token->timeoutRequested.store(false);setCurrentQueryInterruptState(token);
        struct Restore{std::shared_ptr<SessionInterruptState> previous,token;~Restore(){token->cancelRequested.store(false);token->timeoutRequested.store(false);token->queryActive.store(false);setCurrentQueryInterruptState(std::move(previous));}} restore{prior,token};
        auto f=stream(db,"id,row_number() OVER()");auto* plan=f.cursor->plan();std::string out;bool good=plan->open() && plan->next(out) && f.probe->reads==1;
        token->cancelRequested.store(true);token->timeoutRequested.store(timeout);std::string state;try{if(plan->next(out))good=false;}catch(const DbError& error){state=error.sqlState();}
        const bool closed=f.probe->closes==1;token->cancelRequested.store(false);token->timeoutRequested.store(false);
        good=good && state=="57014" && closed && !plan->next(out) && f.probe->reads==1;plan->close();plan->close();return good && f.probe->closes==1;});
    check("ntile NULL first row retries only until first nonNULL bucket",[&]{
        const auto name=sequence();
        auto f=fixture(db,"SELECT id,ntile(CASE WHEN id=1 THEN NULL ELSE nextval('"+name+"')::integer END) OVER(ORDER BY id) FROM wh_native WHERE p=0 ORDER BY id");
        bool good=g_engine.currval(db,name)==1;
        const auto actual=consume(*f.cursor);
        return good && values(actual,{{"1",{}},{"2","1"},{"3","1"}}) && g_engine.currval(db,name)==2;
    });
    const bool dropped=g_engine.dropDatabase(db)==DBStatus::OK;setCurrentSession(previous);
    constexpr size_t expectedControls=37;if(controls!=expectedControls || !dropped)++failures;
    std::cout<<"PREPARED_WINDOW_HOST_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}
