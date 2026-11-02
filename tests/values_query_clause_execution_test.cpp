// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite
#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "expression/prepared_query_execution.h"
#include "Config.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <numeric>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
using Rows = std::vector<std::vector<ExprValue>>;
struct Observation { size_t creates=0, reads=0, writes=0, maxFds=0; } observed;
bool sortFile(int fd) {
    char path[64], name[4096]; std::snprintf(path,sizeof(path),"/proc/self/fd/%d",fd);
    const auto size=::readlink(path,name,sizeof(name)-1);
    if(size<0)return false;
    name[size]='\0';return std::strstr(name,"/.dbms-sort-")!=nullptr;
}
size_t descriptors() {
    size_t result=0;
    for(const auto& item:std::filesystem::directory_iterator("/proc/self/fd"))
        result+=sortFile(std::stoi(item.path().filename().string()));
    return result;
}
bool cleaned() {
    if(descriptors())return false;
    for(const auto& item:std::filesystem::directory_iterator("."))
        if(item.path().filename().string().rfind(".dbms-sort-",0)==0)return false;
    return true;
}
bool equal(const ExprValue& a,const ExprValue& b) {
    if(a.typeName!=b.typeName || a.value!=b.value || a.isNull!=b.isNull ||
       a.collation!=b.collation || a.objectOid!=b.objectOid || a.elementOids!=b.elementOids ||
       bool(a.recordFields)!=bool(b.recordFields))return false;
    if(a.recordFields) {
        if(a.recordFields->size()!=b.recordFields->size())return false;
        for(size_t i=0;i<a.recordFields->size();++i)
            if(!equal(a.recordFields->at(i),b.recordFields->at(i)))return false;
    }
    return true;
}
bool equal(const Rows& a,const Rows& b) {
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i) {
        if(a[i].size()!=b[i].size())return false;
        for(size_t j=0;j<a[i].size();++j)if(!equal(a[i][j],b[i][j]))return false;
    }
    return true;
}
std::string shown(const Rows& rows) {
    std::string result;
    for(const auto& row:rows) {
        if(!result.empty())result+=';';
        for(size_t i=0;i<row.size();++i) {
            if(i)result+=',';
            result+=row[i].isNull?"SQLNULL":row[i].value;
        }
    }
    return result;
}
bool integerRows(const Rows& rows,const std::vector<std::vector<int64_t>>& wanted,
                 const std::vector<std::string>& types={}) {
    if(rows.size()!=wanted.size())return false;
    for(size_t i=0;i<rows.size();++i) {
        if(rows[i].size()!=wanted[i].size())return false;
        for(size_t j=0;j<rows[i].size();++j)
            if(rows[i][j].isNull || rows[i][j].value!=std::to_string(wanted[i][j]) ||
               (!types.empty() && rows[i][j].typeName!=types.at(j)))return false;
    }
    return true;
}
struct Fixture {
    std::shared_ptr<PreparedQuery> query;
    std::unique_ptr<PreparedQueryCursor> cursor;
};
Fixture fixture(const std::string& database,const std::string& sql,
                const std::vector<QueryBindingDatum>& parameters={},const RowContext& caller={}) {
    auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql,parameters));
    auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller);
    auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),query->output);
    return {std::move(query),std::move(cursor)};
}
Rows consume(PreparedQueryCursor& cursor) {
    Rows result;std::vector<ExprValue> row;
    while(cursor.next(row))result.push_back(row);
    cursor.close();cursor.close();return result;
}
Rows execute(const std::string& database,const std::string& sql) {
    auto source=fixture(database,sql);return consume(*source.cursor);
}
const PreparedQuery::SourceRange& range(const PreparedQuery& query,size_t ordinal) {
    const auto found=std::find_if(query.sourceRanges.begin(),query.sourceRanges.end(),
        [&](const auto& item){return item.ordinal==ordinal;});
    if(found==query.sourceRanges.end())throw DbError("XX000","native assertion lost an actual source occurrence");
    return *found;
}
using OwnedColumn = std::pair<const ColumnRefExpr*,const Stmt*>;
void columns(const Stmt*,std::vector<OwnedColumn>&);
void columns(const Expr* expression,const Stmt* owner,std::vector<OwnedColumn>& result) {
    if(!expression)return;
    if(expression->preparedSubquery){columns(expression->preparedSubquery.get(),result);return;}
    if(const auto* value=dynamic_cast<const ColumnRefExpr*>(expression))result.emplace_back(value,owner);
    const auto child=[&](const ExprPtr& value){columns(value.get(),owner,result);};
    if(const auto* value=dynamic_cast<const UnaryOpExpr*>(expression))child(value->operand);
    else if(const auto* value=dynamic_cast<const BinaryOpExpr*>(expression)){child(value->left);if(value->op!="::")child(value->right);}
    else if(const auto* value=dynamic_cast<const CastExpr*>(expression))child(value->operand);
    else if(const auto* value=dynamic_cast<const FunctionCallExpr*>(expression))for(const auto& arg:value->args)child(arg);
    else if(const auto* value=dynamic_cast<const ArrayExpr*>(expression))for(const auto& arg:value->elements)child(arg);
    else if(const auto* value=dynamic_cast<const RowExpr*>(expression))for(const auto& arg:value->elements)child(arg);
}
void columns(const Stmt* statement,std::vector<OwnedColumn>& result) {
    const auto* select=dynamic_cast<const SelectStmt*>(statement);
    if(!select)return;
    for(const auto& target:select->selectList)columns(target.expr.get(),statement,result);
    for(const auto& row:select->valuesRows)for(const auto& cell:row)columns(cell.get(),statement,result);
    for(const auto& key:select->orderBy)columns(key.expr.get(),statement,result);
    columns(select->whereClause.get(),statement,result);
    std::function<void(const FromItem*)> from=[&](const FromItem* item) {
        if(!item)return;
        columns(item->subquery.get(),result);columns(item->joinCondition.get(),statement,result);
        from(item->left.get());from(item->right.get());
    };
    from(select->fromClause.get());
    for(const auto& cte:select->ctes)columns(cte.query.get(),result);
    columns(select->setOpLhs.get(),result);columns(select->setOpRhs.get(),result);
}
bool logicalValues(const PreparedQuery::SourceRange& source,const Stmt* owner) {
    return source.owner==owner && owner && owner->command==SqlCommand::Values && !source.source &&
        source.schema.empty() && source.name.empty() && source.relationSchema.empty() &&
        source.relationName.empty() && !source.mergedUsing && !source.cteStatement && !source.viewQuery;
}
std::string metadataValues() {
    std::string sql="VALUES";
    for(size_t id=0;id<48;++id) {
        if(id)sql+=',';
        const std::string record=id%4==0?"ROW()":id%4==1?
            "ROW("+std::to_string(id)+",'integer'::regtype,ROW('x'::text,ROW()))":id%4==2?
            "NULL::record":"ROW(NULL::integer,'text'::regtype,ROW(''::text,ROW()))";
        const std::string oid=id%11==0?"NULL::regtype":id%2?"'bigint'::regtype":"'boolean'::regtype";
        const std::string array=id%3==0?"ARRAY['integer'::regtype,NULL::regtype,'text'::regtype]":
            id%3==1?"ARRAY[]::regtype[]":"NULL::regtype[]";
        sql+='('+std::to_string(id)+",'row "+std::to_string(id)+" β "+std::string(8192,'a'+id%23)+
            "'::text,"+record+','+oid+','+array+",ARRAY[1::oid,NULL::oid,'4294967295'::oid],"
            "'β'::text COLLATE \"C\")";
    }
    return sql;
}
// These first keys are really compared after spill: the OID array is equal,
// then a uniform nested record compares actual regtype identities, NULL
// fields and the row-dependent integer. Losing a key's recordFields fails
// this control even though the owning output datum remains intact.
const std::string metadataOrder=" ORDER BY column6,ROW(column4,ROW(column1%5,ROW())),column1 DESC,column3,column5,column7";
bool metadataControl(const std::string& database,Session& session,bool spill,size_t offset,size_t count,bool early=false) {
    const auto body=metadataValues();auto original=execute(database,body);
    if(original.size()!=48)return false;
    bool good=true;
    for(size_t id=0;id<original.size();++id) {
        const auto& row=original[id];good=good && row.size()==7;
        if(row.size()!=7)continue;
        good=good && row[0].typeName=="integer" && row[0].value==std::to_string(id) &&
            row[1].value=="row "+std::to_string(id)+" β "+std::string(8192,'a'+id%23) &&
            row[2].typeName=="record" && row[3].typeName=="regtype" &&
            row[4].typeName=="regtype[]" && row[5].typeName=="oid[]" && row[6].collation=="c";
        if(id%4==0)good=good && row[2].recordFields && row[2].recordFields->empty();
        else if(id%4==1) {
            good=good && row[2].recordFields && row[2].recordFields->size()==3;
            if(row[2].recordFields && row[2].recordFields->size()==3) {
                const auto& fields=*row[2].recordFields;
                good=good && fields[1].objectOid==23 && fields[2].recordFields && fields[2].recordFields->size()==2;
                if(fields[2].recordFields && fields[2].recordFields->size()==2)
                    good=good && fields[2].recordFields->at(1).recordFields && fields[2].recordFields->at(1).recordFields->empty();
            }
        } else if(id%4==2)good=good && row[2].isNull && !row[2].recordFields;
        if(id%11==0)good=good && row[3].isNull && !row[3].objectOid;
        else good=good && row[3].objectOid==(id%2?20U:16U);
        if(id%3==0)good=good && row[4].elementOids==std::vector<std::optional<uint32_t>>{23,std::nullopt,25};
        else if(id%3==1)good=good && !row[4].isNull && row[4].elementOids.empty();
        else good=good && row[4].isNull && row[4].elementOids.empty();
    }
    const bool baselineGood=good;
    std::vector<size_t> ids(48);std::iota(ids.begin(),ids.end(),0);
    std::sort(ids.begin(),ids.end(),[](size_t a,size_t b) {
        if((a%11==0)!=(b%11==0))return a%11!=0;
        if(a%11!=0 && a%2!=b%2)return a%2<b%2;
        if(a%5!=b%5)return a%5<b%5;
        return a>b;
    });
    Rows wanted;
    for(size_t i=offset;i<std::min(ids.size(),offset+count);++i)wanted.push_back(original[ids[i]]);
    session.workMem.assign(spill?64:16384,false);
    const auto tail=count==48 && !offset?std::string{}:" LIMIT "+std::to_string(count)+" OFFSET "+std::to_string(offset);
    auto current=fixture(database,body+metadataOrder+tail);
    const std::vector<uint32_t> oids{23,25,2249,2206,2211,1028,25};
    bool descriptorGood=current.cursor->descriptor().size()==oids.size() && observed.creates==0;
    for(size_t i=0;i<current.cursor->descriptor().size();++i)
        descriptorGood=descriptorGood && current.cursor->descriptor()[i].typeOid==oids.at(i);
    good=good && descriptorGood;
    const auto* graph=current.cursor->plan();std::vector<ExprValue> first;
    bool outputGood=false;
    if(early) {
        outputGood=current.cursor->next(first) && equal(Rows{first},Rows{wanted.front()});
        good=outputGood && good;
        const auto reads=observed.reads,writes=observed.writes;
        current.cursor->close();current.cursor->close();
        good=good && observed.reads==reads && observed.writes==writes;
    } else {
        const auto actual=consume(*current.cursor);outputGood=equal(actual,wanted);good=outputGood && good;
        if(!good)std::cout<<"metadata rows="<<actual.size()<<" wanted="<<wanted.size()<<'\n';
    }
    std::cout<<"VALUES_METADATA_DIAGNOSTIC baseline="<<baselineGood<<" descriptor="<<descriptorGood
             <<" full_recursive_output="<<outputGood<<" expected_rows="<<wanted.size()<<" early="<<early<<'\n';
    return good && current.cursor->plan()==graph && observed.maxFds<=2 && cleaned() &&
        (spill?(observed.creates>0 && observed.reads>0 && observed.writes>0):observed.creates==0);
}
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __wrap_mkstemp(char* name) {
    if(std::strncmp(name,".dbms-sort-",11))return __real_mkstemp(name);
    ++observed.creates;const auto fd=__real_mkstemp(name);
    observed.maxFds=std::max(observed.maxFds,descriptors());return fd;
}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset) {
    if(sortFile(fd))++observed.reads;
    return __real_pread(fd,data,size,offset);
}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset) {
    if(sortFile(fd))++observed.writes;
    return __real_pwrite(fd,data,size,offset);
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();const auto database=testDbPath("values_query_clause_execution");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    const auto budget=g_config.workMemKb;g_config.workMemKb=64;session.workMem.assign(64,false);
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& test) {
        observed={};++controls;bool good=false;
        try{good=test();}catch(const DbError& error){std::cout<<"SQLSTATE="<<error.sqlState()<<" message="<<error.what()<<'\n';}
        catch(const std::exception& error){std::cout<<"exception="<<error.what()<<'\n';}
        good=good && cleaned();failures+=!good;
        std::cout<<"VALUES_QUERY_CLAUSE_EXECUTION "<<controls<<' '<<label<<" good="<<good
                 <<" creates="<<observed.creates<<" reads="<<observed.reads<<" writes="<<observed.writes
                 <<" maxfds="<<observed.maxFds<<'\n';
    };
    const std::string base="VALUES(3),(1),(2)";
    check("genuine ordinary VALUES output range and common type",[&] {
        auto current=fixture(database,"VALUES(1),(2147483648),(2) ORDER BY column1+1 DESC");
        const auto* root=dynamic_cast<const SelectStmt*>(current.query->ast.get());
        std::vector<OwnedColumn> found;columns(root,found);
        bool good=root && found.size()==1 && found[0].first->binding;
        if(!good)return false;
        const auto& binding=*found[0].first->binding;const auto& source=range(*current.query,binding.sourceOrdinal);
        good=logicalValues(source,root) && !binding.scopeDepth && binding.columnOrdinal==0 &&
            binding.declaredType=="bigint" && source.columns.size()==1 && source.columns[0].type=="bigint";
        return integerRows(consume(*current.cursor),{{2147483648LL},{2},{1}},{"bigint"}) && good;
    });
    for(const auto& sql:std::vector<std::string>{base+" ORDER BY (SELECT -column1)",base+" ORDER BY (SELECT (SELECT -column1))"})
        check("ORDER scalar correlation retains true VALUES ancestor "+sql,[&] {
            auto current=fixture(database,sql);const auto* root=current.query->ast.get();std::vector<OwnedColumn> found;columns(root,found);
            bool good=found.size()==1 && found[0].second!=root && found[0].first->binding;
            if(good){const auto& binding=*found[0].first->binding;good=binding.scopeDepth>0 && logicalValues(range(*current.query,binding.sourceOrdinal),root);}
            return integerRows(consume(*current.cursor),{{3},{2},{1}},{"integer"}) && good;
        });
    check("ORDER child-local source shadows VALUES output",[&] {
        auto current=fixture(database,base+" ORDER BY (SELECT -column1 FROM(VALUES(0)) AS s(column1)),column1");
        const auto* root=current.query->ast.get();std::vector<OwnedColumn> found;columns(root,found);bool local=false,output=false;
        for(const auto& owned:found)if(owned.first->binding) {
            const auto& binding=*owned.first->binding;const auto& source=range(*current.query,binding.sourceOrdinal);
            if(owned.second==root)output=logicalValues(source,root) && binding.scopeDepth==0;
            else local=source.owner==owned.second && source.source && binding.scopeDepth==0;
        }
        return local && output && integerRows(consume(*current.cursor),{{1},{2},{3}},{"integer"});
    });
    for(const auto& sql:std::vector<std::string>{"VALUES(column1),(1) ORDER BY 1","VALUES((SELECT column1)),(2) ORDER BY column1"})
        check("VALUES input cannot see later local output "+sql,[&] {
            try{(void)g_engine.prepareBoundQuery(database,sql);}catch(const DbError& error){return error.sqlState()=="42703";}return false;
        });

    TableSchema parent;parent.tablename="values_parent";parent.append(makeIntColumn("column1",false,2));
    if(g_engine.createTable(database,parent)!=DBStatus::OK)return 2;
    for(const auto value:{5,2,9})if(g_engine.insertRow(database,parent.tablename,{{"column1",std::to_string(value)}})!=DBStatus::OK)return 2;
    check("VALUES input and ORDER child retain distinct real outer bindings",[&] {
        auto current=fixture(database,"SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) "
            "ORDER BY (SELECT -column1+p.column1) LIMIT 1) FROM values_parent p ORDER BY p.column1");
        std::vector<OwnedColumn> found;columns(current.query->ast.get(),found);size_t inputs=0,localKeys=0,higher=0;
        for(const auto& owned:found)if(owned.first->binding) {
            const auto& binding=*owned.first->binding;const auto& source=range(*current.query,binding.sourceOrdinal);
            if(owned.second->command==SqlCommand::Values && source.owner==current.query->ast.get() && binding.scopeDepth==1)++inputs;
            if(owned.second->command==SqlCommand::Select && source.owner->command==SqlCommand::Values && binding.scopeDepth==1)++localKeys;
            if(binding.scopeDepth==2 && source.owner==current.query->ast.get())++higher;
        }
        return inputs==2 && localKeys==1 && higher==1 && integerRows(consume(*current.cursor),{{2,22},{5,25},{9,29}},{"integer","integer"});
    });
    check("CTE aliases apply outside their genuine VALUES ORDER scope",[&] {
        auto current=fixture(database,"WITH v(a) AS(VALUES(3),(1),(2) ORDER BY column1 DESC LIMIT 2) SELECT a FROM v ORDER BY a");
        return integerRows(consume(*current.cursor),{{2},{3}},{"integer"});
    });
    for(const auto origin:{ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder,ParameterOrigin::StatementInput})
        check("original parameter owner survives output-row key context role="+std::to_string(static_cast<unsigned>(origin)),[&] {
            QueryBindingDatum datum{"values-runtime-owned-input","p","integer",{},false,"7",1};datum.origin=origin;
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,
                "VALUES($1+2),($1) ORDER BY $1-column1 DESC",{datum}));
            PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();
            auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output);
            const auto* graph=cursor->plan();const auto first=consume(*cursor);
            bool good=cursor->supportsRestart() && integerRows(first,{{7},{9}},{"integer"});
            caller.setParameters({ExprValue("integer","20")});cursor->restart(caller);
            const auto expected=origin==ParameterOrigin::StatementInput?7:20;
            const auto second=consume(*cursor);
            good=integerRows(second,{{expected},{expected+2}},{"integer"}) && good;
            if(!good)std::cout<<"parameter first="<<shown(first)<<" second="<<shown(second)<<'\n';
            return good && cursor->plan()==graph && query->parameters.size()==1 && query->parameters[0].value=="7" && caller.parameter(0).value=="20";
        });
    check("high budget full owning VALUES/key metadata control",[&]{return metadataControl(database,session,false,0,48);});
    check("low budget full owning VALUES/key metadata really spills",[&]{return metadataControl(database,session,true,0,48);});
    check("low budget VALUES TopN OFFSET preserves all typed fields",[&]{return metadataControl(database,session,true,3,7);});
    check("early close after first spilled VALUES result avoids successor I/O",[&]{return metadataControl(database,session,true,0,48,true);});

    DdlExecutor ddl;size_t sequenceNumber=0;
    const auto sequence=[&] {
        const auto name="values_native_calls_"+std::to_string(sequenceNumber++);
        if(ddl.executeSql("CREATE SEQUENCE "+name,session) || g_engine.nextval(database,name)!=1)
            throw DbError("XX000","genuine catalog sequence fixture failed");
        return name;
    };
    check("builder and descriptor are pure; duplicate volatile keys share one site",[&] {
        const auto name=sequence(),call="nextval('"+name+"')";
        auto current=fixture(database,"VALUES("+call+','+call+"),("+call+','+call+"),("+call+','+call+") ORDER BY "+call+','+call+" DESC");
        bool good=g_engine.currval(database,name)==1 && current.cursor->descriptor().size()==2 && observed.creates==0;
        return integerRows(consume(*current.cursor),{{2,3},{5,6},{8,9}},{"bigint","bigint"}) && g_engine.currval(database,name)==10 && good;
    });
    check("volatile source output used as key executes once per actual site",[&] {
        const auto name=sequence(),call="nextval('"+name+"')";
        auto current=fixture(database,"VALUES("+call+"),("+call+"),("+call+") ORDER BY column1 DESC LIMIT 1");
        bool good=g_engine.currval(database,name)==1;
        return integerRows(consume(*current.cursor),{{4}},{"bigint"}) && g_engine.currval(database,name)==4 && good;
    });
    check("correlated volatile ORDER child executes for every input row",[&] {
        const auto name=sequence();auto current=fixture(database,base+" ORDER BY (SELECT nextval('"+name+"')+column1)");
        return g_engine.currval(database,name)==1 && integerRows(consume(*current.cursor),{{1},{3},{2}},{"integer"}) && g_engine.currval(database,name)==4;
    });
    check("uncorrelated volatile ORDER child keeps actual per-execution memo",[&] {
        const auto name=sequence();auto current=fixture(database,base+" ORDER BY (SELECT nextval('"+name+"')),column1");
        return g_engine.currval(database,name)==1 && integerRows(consume(*current.cursor),{{1},{2},{3}},{"integer"}) && g_engine.currval(database,name)==2;
    });
    check("unordered LIMIT consumes only its demanded VALUES cell",[&] {
        const auto name=sequence(),call="nextval('"+name+"')";
        auto current=fixture(database,"VALUES("+call+"),("+call+"),("+call+") LIMIT 1");
        return g_engine.currval(database,name)==1 && integerRows(consume(*current.cursor),{{2}},{"bigint"}) && g_engine.currval(database,name)==2;
    });
    for(const auto& tail:std::vector<std::string>{" LIMIT 0"," FETCH FIRST 0 ROWS WITH TIES"})
        check("zero count performs no source/key evaluation "+tail,[&] {
            const auto name=sequence(),call="nextval('"+name+"')";
            auto current=fixture(database,"VALUES("+call+"),("+call+"),("+call+") ORDER BY "+call+tail);
            return g_engine.currval(database,name)==1 && consume(*current.cursor).empty() && g_engine.currval(database,name)==1 && observed.creates==0;
        });
    check("FETCH NULL ONLY is unbounded and still evaluates all source cells",[&] {
        const auto name=sequence(),call="nextval('"+name+"')";
        auto current=fixture(database,"VALUES("+call+"),("+call+"),("+call+") ORDER BY column1 DESC FETCH FIRST NULL ROWS ONLY");
        return g_engine.currval(database,name)==1 && integerRows(consume(*current.cursor),{{4},{3},{2}},{"bigint"}) && g_engine.currval(database,name)==4;
    });
    check("WITH TIES follows computed multikey values and OFFSET peer demand",[&] {
        return integerRows(execute(database,"VALUES(2,0),(1,1),(1,1),(1,2),(3,0) ORDER BY column1+0,column2 OFFSET 1 ROW FETCH NEXT 1 ROW WITH TIES"),{{1,1}},{"integer","integer"});
    });
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {" LIMIT -1","2201W"},{" OFFSET -1 LIMIT 0","2201X"},{" OFFSET -1 LIMIT -1","2201X"},
        {" FETCH FIRST -1 ROWS ONLY","2201W"},{" FETCH FIRST NULL ROWS WITH TIES","2201W"}})
        check("count errors precede real source/key demand "+item.first,[&] {
            const auto name=sequence(),call="nextval('"+name+"')";
            const auto sql="VALUES("+call+"),("+call+") ORDER BY "+call+item.first;
            if(item.first==" FETCH FIRST NULL ROWS WITH TIES") {
                // PostgreSQL transformLimitClause rejects this literal
                // during analysis. Never construct an operator/cursor to
                // obtain its error; source/key sites must remain untouched.
                std::string state;
                try{(void)g_engine.prepareBoundQuery(database,sql);}
                catch(const DbError& error){state=error.sqlState();}
                return state=="2201W" && g_engine.currval(database,name)==1 &&
                    observed.creates==0 && observed.reads==0 && observed.writes==0;
            }
            auto current=fixture(database,sql);
            bool good=g_engine.currval(database,name)==1 && current.cursor->descriptor().size()==1;
            std::string state;std::vector<ExprValue> row;
            try{(void)current.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}
            current.cursor->close();current.cursor->close();
            return good && state==item.second && row.empty() && g_engine.currval(database,name)==1 && observed.creates==0;
        });
    check("discarded TopN ORDER key error is not hidden by LIMIT",[&] {
        auto current=fixture(database,base+" ORDER BY 1/(column1-1) LIMIT 1");std::vector<ExprValue> row;std::string state;
        try{(void)current.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}
        current.cursor->close();return state=="22012" && row.empty();
    });
    // Appended expression-count controls. The original thirty controls and
    // their assertions above remain byte-identical to the accepted C3 test.
    const auto ownedParameterSlot=[](const PreparedQuery& query,const std::string& spelling) {
        std::optional<size_t> slot;
        for(const auto& use:query.uses)if(query.source.substr(use.begin,use.end-use.begin)==spelling) {
            if(slot && *slot!=use.slot)throw DbError("XX000","one SQL parameter has multiple owned frame slots");
            slot=use.slot;
        }
        if(!slot || *slot>=query.parameters.size())throw DbError("XX000","SQL parameter has no actual prepared frame slot");
        return *slot;
    };
    check("expression LIMIT/OFFSET arithmetic consumes the actual VALUES plan",[&] {
        return integerRows(execute(database,base+" ORDER BY column1 LIMIT (1+1) OFFSET (1-1)"),{{1},{2}},{"integer"});
    });
    check("scalar LIMIT and OFFSET counts use their genuine child sites",[&] {
        return integerRows(execute(database,base+" ORDER BY column1 LIMIT (SELECT 2) OFFSET (SELECT 1)"),{{2},{3}},{"integer"});
    });
    for(const auto origin:{ParameterOrigin::RuntimeCell,ParameterOrigin::MetadataPlaceholder,ParameterOrigin::StatementInput})
        check("count parameter owner and same graph restart role="+std::to_string(static_cast<unsigned>(origin)),[&] {
            QueryBindingDatum cap{"values-count-cap","cap","integer",{},false,"2",1};cap.origin=origin;
            QueryBindingDatum skip{"values-count-offset","skip","integer",{},false,"1",2};skip.origin=origin;
            auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,
                base+" ORDER BY column1 LIMIT $1 OFFSET $2",{cap,skip}));
            const auto capSlot=ownedParameterSlot(*query,"$1"),skipSlot=ownedParameterSlot(*query,"$2");
            PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();
            auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output);
            const auto* graph=cursor->plan();bool good=cursor->supportsRestart();
            good=integerRows(consume(*cursor),{{2},{3}},{"integer"}) && good;
            auto frame=query->parameters;frame.at(capSlot)=ExprValue("integer","1");frame.at(skipSlot)=ExprValue("integer","0");
            caller.setParameters(std::move(frame));cursor->restart(caller);
            const auto second=consume(*cursor);
            good=(origin==ParameterOrigin::StatementInput?integerRows(second,{{2},{3}},{"integer"}):
                integerRows(second,{{1}},{"integer"})) && good;
            return good && cursor->plan()==graph && query->parameters.size()==2 &&
                query->parameters[capSlot].value=="2" && query->parameters[skipSlot].value=="1" &&
                caller.parameter(capSlot).value=="1" && caller.parameter(skipSlot).value=="0";
        });
    check("scalar count memo is fresh for each invocation of the same graph",[&] {
        const auto name=sequence();auto current=fixture(database,base+" ORDER BY column1 LIMIT (SELECT nextval('"+name+"'))");
        const auto* graph=current.cursor->plan();bool good=current.cursor->supportsRestart() &&
            current.cursor->descriptor().size()==1 && g_engine.currval(database,name)==1;
        good=integerRows(consume(*current.cursor),{{1},{2}},{"integer"}) && g_engine.currval(database,name)==2 && good;
        current.cursor->restart(RowContext{});
        good=integerRows(consume(*current.cursor),{{1},{2},{3}},{"integer"}) && g_engine.currval(database,name)==3 && good;
        return good && current.cursor->plan()==graph;
    });
    for(bool child:{false,true})check(child?"real parent correlation inside scalar count":"real parent correlation inside direct count",[&] {
        const auto count=child?"(SELECT p.column1%2)":"p.column1%2";
        auto current=fixture(database,"SELECT p.column1,(VALUES(p.column1+10),(p.column1+20) ORDER BY column1 DESC LIMIT "+
            std::string(count)+") FROM values_parent p ORDER BY p.column1");
        const auto rows=consume(*current.cursor);
        return rows.size()==3 && rows[0].size()==2 && rows[0][0].value=="2" && rows[0][1].isNull &&
            rows[0][1].typeName=="integer" && integerRows(Rows{rows[1],rows[2]},{{5,25},{9,29}},{"integer","integer"});
    });
    check("volatile count is evaluated once before source startup",[&] {
        const auto name=sequence();auto current=fixture(database,base+" ORDER BY column1 LIMIT nextval('"+name+"') OFFSET (0+0)");
        return g_engine.currval(database,name)==1 && integerRows(consume(*current.cursor),{{1},{2}},{"integer"}) &&
            g_engine.currval(database,name)==2;
    });
    check("OFFSET expression runs before LIMIT expression",[&] {
        const auto name=sequence(),call="nextval('"+name+"')";
        auto current=fixture(database,base+" ORDER BY column1 LIMIT "+call+" OFFSET "+call);
        return g_engine.currval(database,name)==1 && integerRows(consume(*current.cursor),{{3}},{"integer"}) &&
            g_engine.currval(database,name)==3;
    });
    check("expression LIMIT zero evaluates both counts but no source or key",[&] {
        const auto count=sequence(),source=sequence(),call="nextval('"+source+"')";
        auto current=fixture(database,"VALUES("+call+"),("+call+"),("+call+") ORDER BY "+call+
            " LIMIT (nextval('"+count+"')-3) OFFSET nextval('"+count+"')");
        bool good=g_engine.currval(database,count)==1 && g_engine.currval(database,source)==1;
        return consume(*current.cursor).empty() && g_engine.currval(database,count)==3 &&
            g_engine.currval(database,source)==1 && observed.creates==0 && good;
    });
    check("typed NULL count is unbounded and NULL offset is zero",[&] {
        return integerRows(execute(database,base+" ORDER BY column1 LIMIT (NULL::bigint) OFFSET (NULL::bigint)"),{{1},{2},{3}},{"integer"});
    });
    check("scalar count with no rows is an unbounded typed NULL",[&] {
        return integerRows(execute(database,base+" ORDER BY column1 LIMIT (SELECT 1 WHERE false)"),{{1},{2},{3}},{"integer"});
    });
    check("scalar count cardinality error precedes source evaluation",[&] {
        const auto name=sequence(),call="nextval('"+name+"')";
        auto current=fixture(database,"VALUES("+call+"),("+call+") ORDER BY column1 LIMIT (VALUES(1),(2))");
        bool good=g_engine.currval(database,name)==1 && current.cursor->descriptor().size()==1;
        std::vector<ExprValue> row;std::string state;
        try{(void)current.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}
        current.cursor->close();current.cursor->close();
        return good && state=="21000" && row.empty() && g_engine.currval(database,name)==1 && observed.creates==0;
    });
    check("numeric count receives the real bigint conversion",[&] {
        return integerRows(execute(database,base+" ORDER BY column1 LIMIT (1.5::numeric)"),{{1},{2}},{"integer"});
    });
    for(const auto& tail:std::vector<std::string>{" LIMIT column1+0"," LIMIT (SELECT column1)",
        " LIMIT (SELECT (SELECT column1))"," OFFSET (SELECT column1)",
        " LIMIT (SELECT column1 FROM(VALUES(1)) AS s(x))"})
        check("local output source occurrence is rejected in count analysis "+tail,[&] {
            try{(void)g_engine.prepareBoundQuery(database,base+" ORDER BY column1"+tail);}
            catch(const DbError& error){return error.sqlState()=="42P10" && observed.creates==0;}
            return false;
        });
    for(const auto& expression:std::vector<std::string>{"column1","s.column1"})
        check("count child-local column shadow is legal "+expression,[&] {
            return integerRows(execute(database,base+" ORDER BY column1 LIMIT (SELECT "+expression+
                " FROM(VALUES(1)) AS s(column1))"),{{1}},{"integer"});
        });
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {" LIMIT (0-1)","2201W"},{" OFFSET (0-1) LIMIT (1-1)","2201X"},
        {" OFFSET (0-1) LIMIT (0-1)","2201X"}})
        check("expression count error and OFFSET priority precede source "+item.first,[&] {
            const auto name=sequence(),call="nextval('"+name+"')";
            auto current=fixture(database,"VALUES("+call+"),("+call+") ORDER BY "+call+item.first);
            bool good=g_engine.currval(database,name)==1 && current.cursor->descriptor().size()==1;
            std::vector<ExprValue> row;std::string state;
            try{(void)current.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}
            current.cursor->close();current.cursor->close();
            return good && state==item.second && row.empty() && g_engine.currval(database,name)==1 && observed.creates==0;
        });
    check("runtime count overflow preserves exact error and no source calls",[&] {
        const auto count=sequence(),source=sequence(),call="nextval('"+source+"')";
        auto current=fixture(database,"VALUES("+call+"),("+call+") ORDER BY column1 LIMIT (nextval('"+
            count+"')::numeric+9223372036854775807)");
        bool good=g_engine.currval(database,count)==1 && g_engine.currval(database,source)==1;
        std::vector<ExprValue> row;std::string state;
        try{(void)current.cursor->next(row);}catch(const DbError& error){state=error.sqlState();}
        current.cursor->close();current.cursor->close();
        return good && state=="22003" && row.empty() && g_engine.currval(database,count)==2 &&
            g_engine.currval(database,source)==1 && observed.creates==0;
    });
    check("largest valid count and offset remain bounded while skipping spilled rows",[&] {
        session.workMem.assign(64,false);
        auto current=fixture(database,metadataValues()+metadataOrder+
            " LIMIT ('9223372036854775807'::bigint+0) OFFSET ('9223372036854775807'::bigint+0)");
        return consume(*current.cursor).empty() && observed.creates>0 && observed.reads>0 &&
            observed.writes>0 && observed.maxFds<=2 && cleaned();
    });
    check("dynamic OFFSET WITH TIES uses the genuine computed peer keys",[&] {
        return integerRows(execute(database,"VALUES(2,0),(1,1),(1,1),(1,2),(3,0) ORDER BY column1+0,column2 "
            "OFFSET (SELECT 1) ROW FETCH NEXT 1 ROW WITH TIES"),{{1,1}},{"integer","integer"});
    });
    check("dynamic NULL OFFSET WITH TIES starts at the first peer",[&] {
        return integerRows(execute(database,"VALUES(2),(1),(1),(3) ORDER BY column1 "
            "OFFSET (NULL::bigint) ROW FETCH FIRST 1 ROW WITH TIES"),{{1},{1}},{"integer"});
    });
    check("spilled expression counts rebind same graph and preserve full owning values",[&] {
        const auto body=metadataValues();auto original=execute(database,body);
        std::vector<size_t> ids(48);std::iota(ids.begin(),ids.end(),0);
        std::sort(ids.begin(),ids.end(),[](size_t a,size_t b) {
            if((a%11==0)!=(b%11==0))return a%11!=0;
            if(a%11!=0 && a%2!=b%2)return a%2<b%2;
            if(a%5!=b%5)return a%5<b%5;
            return a>b;
        });
        session.workMem.assign(64,false);
        QueryBindingDatum cap{"values-spilled-cap","cap","integer",{},false,"7",1};cap.origin=ParameterOrigin::RuntimeCell;
        QueryBindingDatum skip{"values-spilled-offset","skip","integer",{},false,"3",2};skip.origin=ParameterOrigin::RuntimeCell;
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,body+metadataOrder+" LIMIT $1 OFFSET $2",{cap,skip}));
        const auto capSlot=ownedParameterSlot(*query,"$1"),skipSlot=ownedParameterSlot(*query,"$2");
        PreparedQueryExecution execution(query,&g_engine,database);auto caller=execution.context();
        auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get(),caller),query->output);
        const auto* graph=cursor->plan();std::vector<ExprValue> first;
        bool good=cursor->supportsRestart() && cursor->next(first) && equal(Rows{first},Rows{original.at(ids.at(3))});
        good=good && observed.creates>0 && descriptors()==1;
        const auto reads=observed.reads,writes=observed.writes;
        cursor->close();cursor->close();good=good && cleaned() && observed.reads==reads && observed.writes==writes;
        auto frame=query->parameters;frame.at(capSlot)=ExprValue("integer","2");frame.at(skipSlot)=ExprValue("integer","0");
        caller.setParameters(std::move(frame));cursor->restart(caller);
        const auto second=consume(*cursor);good=equal(second,Rows{original.at(ids[0]),original.at(ids[1])}) && good;
        return good && cursor->plan()==graph && query->parameters[capSlot].value=="7" && query->parameters[skipSlot].value=="3" &&
            observed.maxFds<=2 && cleaned();
    });
    check("expression count wrapper terminates and releases spool on key error",[&] {
        session.workMem.assign(64,false);
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,metadataValues()+
            " ORDER BY 1/(column1-23),column1 LIMIT (NULL::bigint)"));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get());
        bool good=plan->open();std::string display,state;
        try{if(plan->next(display))good=false;}catch(const DbError& error){state=error.sqlState();}
        good=good && state=="22012" && observed.creates>0 && cleaned();
        const auto reads=observed.reads,writes=observed.writes;
        good=!plan->next(display) && observed.reads==reads && observed.writes==writes && good;
        plan->close();plan->close();return good && cleaned();
    });
    for(bool timeout:{false,true})check(timeout?"dynamic count TIES timeout closes its spilled child immediately":
        "dynamic count TIES cancellation closes its spilled child immediately",[&] {
        const auto previousInterrupt=currentQueryInterruptState();auto token=session.interruptState;
        token->queryActive.store(true);token->cancelRequested.store(false);token->timeoutRequested.store(false);
        setCurrentQueryInterruptState(token);
        struct RestoreCountInterrupt {
            std::shared_ptr<SessionInterruptState> previous,token;
            ~RestoreCountInterrupt(){token->cancelRequested.store(false);token->timeoutRequested.store(false);
                token->queryActive.store(false);setCurrentQueryInterruptState(std::move(previous));}
        } restore{previousInterrupt,token};
        session.workMem.assign(64,false);
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,metadataValues()+metadataOrder+
            " OFFSET (0+0) ROW FETCH FIRST 1 ROW WITH TIES"));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get());
        std::string display;size_t rows=0;bool good=plan->open();
        if(plan->next(display))++rows;
        good=good && rows==1 && observed.creates>0 && descriptors()==1;
        const auto reads=observed.reads,writes=observed.writes;
        token->timeoutRequested.store(timeout);token->cancelRequested.store(true);
        bool exact=false;
        try{if(plan->next(display))++rows;}
        catch(const DbError& error){exact=error.sqlState()=="57014" && error.message()==
            (timeout?"canceling statement due to statement timeout":"canceling statement due to user request");}
        const bool immediate=cleaned() && observed.reads==reads && observed.writes==writes;
        token->cancelRequested.store(false);token->timeoutRequested.store(false);
        good=!plan->next(display) && rows==1 && exact && immediate && good;
        plan->close();plan->close();return good && cleaned() && observed.maxFds<=2;
    });
    check("dynamic K1 still demands discarded source cells and error keys without spool",[&] {
        session.workMem.assign(64,false);
        const auto sourceName=sequence(),keyName=sequence();
        std::string sql="VALUES";
        for(size_t id=0;id<48;++id) {
            if(id)sql+=',';
            sql+='('+std::to_string(id)+",'"+std::string(8192,'a'+id%23)+"'::text,nextval('"+sourceName+"'))";
        }
        sql+=" ORDER BY 1/(column1-23),column1,nextval('"+keyName+"') LIMIT (1+0)";
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get());
        bool good=g_engine.currval(database,sourceName)==1 && g_engine.currval(database,keyName)==1 && plan->open();
        good=good && g_engine.currval(database,sourceName)==1 && g_engine.currval(database,keyName)==1;
        std::string display,state;size_t rows=0;
        try{if(plan->next(display))++rows;}catch(const DbError& error){state=error.sqlState();}
        // Row 23 evaluates its source cell before its first ORDER key fails;
        // the later volatile ORDER key completed for rows 0 through 22 only.
        const auto sourceCalls=g_engine.currval(database,sourceName),keyCalls=g_engine.currval(database,keyName);
        good=good && state=="22012" && rows==0 && sourceCalls==25 && keyCalls==24 &&
            observed.creates==0 && cleaned();
        const auto reads=observed.reads,writes=observed.writes;
        good=!plan->next(display) && !plan->next(display) && observed.reads==reads && observed.writes==writes && good;
        plan->close();plan->close();
        std::cout<<"VALUES_K1_ERROR_DEMAND source_counter="<<sourceCalls<<" key_counter="<<keyCalls<<'\n';
        return good && cleaned() && g_engine.currval(database,sourceName)==25 && g_engine.currval(database,keyName)==24;
    });
    const bool dropped=g_engine.dropDatabase(database)==DBStatus::OK;
    g_config.workMemKb=budget;setCurrentSession(previous);
    constexpr size_t expectedControls=64;
    if(controls!=expectedControls || !dropped)++failures;
    std::cout<<"VALUES_QUERY_CLAUSE_EXECUTION_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
