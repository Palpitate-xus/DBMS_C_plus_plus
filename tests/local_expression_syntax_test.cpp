// Pristine statement-local syntax, original per-row evaluation, real heap mutations.
#include "expression/LocalExpressionSyntax.h"
#include "expression/expr_helper.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
void require(bool value,const std::string& message) {
    if(!value)throw std::runtime_error(message);
}
void text(std::ostream& out,const std::string& value){out<<std::quoted(value)<<';';}
void comparison(std::ostream& out,const std::optional<QueryComparisonBinding>& value) {
    out<<bool(value)<<';';if(!value)return;
    const auto& x=*value;text(out,x.identity);text(out,x.op);text(out,x.leftType);
    text(out,x.rightType);text(out,x.collation);out<<x.strict<<';'<<x.hashable<<';'<<x.enumTypeOid<<';';
    out<<x.enumLabels.size()<<';';for(const auto& item:x.enumLabels)text(out,item);
}
void expression(std::ostream&,const Expr*);
void window(std::ostream& out,const WindowDef& value) {
    text(out,value.name);text(out,value.referenceName);text(out,value.frameMode);text(out,value.frameExclusion);
    out<<int(value.frameStartKind)<<';'<<int(value.frameEndKind)<<';'<<value.frameBetween<<';';
    out<<value.partitionBy.size()<<';';for(const auto& item:value.partitionBy)expression(out,item.get());
    out<<value.orderBy.size()<<';';for(const auto& item:value.orderBy){expression(out,item.first.get());out<<item.second<<';';}
    out<<value.orderNullsFirst.size()<<';';for(const auto item:value.orderNullsFirst)out<<(item?(*item?1:0):-1)<<';';
    out<<value.orderDirectionSpecified.size()<<';';for(const bool item:value.orderDirectionSpecified)out<<item<<';';
    out<<value.partitionComparisons.size()<<';';for(const auto& item:value.partitionComparisons)comparison(out,item);
    out<<value.orderComparisons.size()<<';';for(const auto& item:value.orderComparisons)comparison(out,item);
    expression(out,value.frameStart.get());expression(out,value.frameEnd.get());
}
// This is an observation of the actual public syntax fields, compared with a
// fresh parser oracle. It neither clones nodes nor renders replacement SQL.
void expression(std::ostream& out,const Expr* value) {
    if(!value){out<<"null;";return;}
    out<<"node;"<<int(value->type)<<';'<<value->sourceBegin<<';'<<value->sourceEnd<<';'<<bool(value->preparedSubquery)<<';';
    if(const auto* x=dynamic_cast<const LiteralExpr*>(value)) {
        text(out,x->value);text(out,x->typeName);out<<x->analyzedPrimitiveConstant<<';';
    }else if(const auto* x=dynamic_cast<const ColumnRefExpr*>(value)) {
        text(out,x->schema);text(out,x->table);text(out,x->column);out<<bool(x->binding)<<';';
        if(x->binding){const auto& b=*x->binding;out<<b.scopeDepth<<';'<<b.sourceOrdinal<<';'<<b.columnOrdinal<<';';
            text(out,b.declaredType);out<<b.mergedUsing<<';'<<b.typeOid<<';'<<bool(b.typeMetadata)<<';';}
    }else if(const auto* x=dynamic_cast<const ParameterExpr*>(value)) {
        out<<x->slot<<';'<<int(x->origin)<<';';text(out,x->declaredType);
    }else if(const auto* x=dynamic_cast<const UnaryOpExpr*>(value)) {
        text(out,x->op);expression(out,x->operand.get());
    }else if(const auto* x=dynamic_cast<const BinaryOpExpr*>(value)) {
        text(out,x->op);out<<bool(x->arrayConcat)<<';';if(x->arrayConcat){const auto& a=*x->arrayConcat;
            text(out,a.leftType);text(out,a.rightType);text(out,a.elementType);out<<a.leftArray<<';'<<a.rightArray<<';';}
        comparison(out,x->comparison);out<<x->rowComparison<<';'<<x->rowComparisons.size()<<';';
        for(const auto& row:x->rowComparisons){out<<row.size()<<';';for(const auto& item:row)comparison(out,item);}
        expression(out,x->left.get());expression(out,x->right.get());
    }else if(const auto* x=dynamic_cast<const QuantifiedComparisonExpr*>(value)) {
        text(out,x->op);out<<int(x->quantifier)<<';';comparison(out,x->comparison);
        expression(out,x->left.get());expression(out,x->right.get());
    }else if(const auto* x=dynamic_cast<const CastExpr*>(value)) {
        text(out,x->typeName);out<<x->implicit<<';'<<x->typeMods.size()<<';';
        for(const auto& item:x->typeMods)text(out,item);expression(out,x->operand.get());
    }else if(const auto* x=dynamic_cast<const CaseExpr*>(value)) {
        expression(out,x->switchExpr.get());out<<x->whenClauses.size()<<';';
        for(const auto& item:x->whenClauses){expression(out,item.first.get());expression(out,item.second.get());}
        expression(out,x->elseExpr.get());out<<x->simpleComparisonTypes.size()<<';';
        for(const auto& item:x->simpleComparisonTypes){text(out,item.first);text(out,item.second);}
        out<<x->simpleEnumComparisons.size()<<';';for(const auto& item:x->simpleEnumComparisons)comparison(out,item);
    }else if(const auto* x=dynamic_cast<const FunctionCallExpr*>(value)) {
        text(out,x->schema);text(out,x->funcName);out<<int(x->sqlValue)<<';'<<bool(x->sqlValuePrecision)<<';';
        if(x->sqlValuePrecision)out<<*x->sqlValuePrecision<<';';
        out<<x->distinct<<';'<<x->hasOver<<';';text(out,x->orderBy);text(out,x->resolvedResultType);
        out<<bool(x->setReturning)<<';';if(x->setReturning){out<<int(x->setReturning->kind)<<';';
            text(out,x->setReturning->identity);text(out,x->setReturning->elementType);}
        out<<x->args.size()<<';';for(const auto& item:x->args)expression(out,item.get());
        out<<x->namedArgs.size()<<';';for(const auto& item:x->namedArgs){text(out,item.name);expression(out,item.value.get());}
        out<<x->aggregateOrderKeys.size()<<';';for(const auto& key:x->aggregateOrderKeys){expression(out,key.expr.get());
            out<<key.ascending<<';'<<key.nullsFirst<<';';comparison(out,key.enumComparison);text(out,key.usingOperator);}
        expression(out,x->filter.get());window(out,x->over);
    }else if(const auto* x=dynamic_cast<const ArrayExpr*>(value)) {
        text(out,x->elementType);out<<x->nestedElements<<';'<<bool(x->typeMetadata)<<';'<<x->elements.size()<<';';
        for(const auto& item:x->elements)expression(out,item.get());
    }else if(const auto* x=dynamic_cast<const RowExpr*>(value)) {
        out<<x->constructor<<';'<<bool(x->typeMetadata)<<';'<<bool(x->resultMetadata)<<';'<<x->fieldTypes.size()<<';';
        for(const auto& item:x->fieldTypes)text(out,item);out<<x->elements.size()<<';';
        for(const auto& item:x->elements)expression(out,item.get());
    }else throw std::runtime_error("unobserved actual syntax node");
}
SelectStmt& select(ParseResult& result) {
    auto* statement=result.stmt?dynamic_cast<SelectStmt*>(result.stmt.get()):nullptr;
    require(result.success && statement && statement->selectList.size()==1,"actual scalar SELECT parse");return *statement;
}
std::string snapshot(const ParseResult& result) {
    std::ostringstream out;out<<result.success<<';';text(out,result.error);text(out,result.originalSql);
    text(out,result.sqlState);out<<result.rowCountSyntaxError<<';'<<bool(result.stmt)<<';';
    if(!result.stmt)return out.str();
    const auto* statement=dynamic_cast<const SelectStmt*>(result.stmt.get());require(statement,"SELECT snapshot");
    out<<int(statement->command)<<';'<<statement->selectList.size()<<';';
    for(const auto& item:statement->selectList){text(out,item.alias);out<<item.sourceExpressionEnd<<';';expression(out,item.expr.get());}
    out<<bool(statement->fromClause)<<';';if(statement->fromClause){const auto& from=*statement->fromClause;
        out<<int(from.type)<<';';text(out,from.tableName);text(out,from.alias);}
    expression(out,statement->whereClause.get());return out.str();
}
ParseResult fresh(const std::string& source,bool binding) {
    SQLParser parser;return binding?parser.parseForBinding(source):parser.parse(source);
}
void compareSyntax(LocalExpressionSyntax& cache,const std::string& source,bool binding) {
    auto original=fresh(source,binding),first=cache.parse(source,binding),second=cache.parse(source,binding);
    require(snapshot(first)==snapshot(original) && snapshot(second)==snapshot(original),"full fresh syntax/diagnostic equality: "+source);
    if(original.success){auto& a=select(first);auto& b=select(second);
        const auto* originalRoot=select(original).selectList.front().expr.get();
        if(originalRoot)
            require(a.selectList.front().expr.get()!=b.selectList.front().expr.get(),"mutable clone root ownership");
        else
            require(!a.selectList.front().expr && !b.selectList.front().expr,"original NULL syntax node stays NULL");}
}
bool same(const ExprEvalResult& a,const ExprEvalResult& b) {
    return a.ok==b.ok && a.isNull==b.isNull && a.value==b.value && a.typeName==b.typeName &&
        a.collation==b.collation && a.error==b.error && a.sqlState==b.sqlState;
}
ExprEvalResult evaluate(LocalExpressionSyntax& cache,const std::string& sql,
    const std::map<std::string,std::string>& row,const std::set<std::string>& nulls,
    const std::map<std::string,std::string>& hints,const std::map<std::string,std::string>& collations={}) {
    auto old=ExprHelper::evalStringWithNulls(sql,row,nulls,hints,"","",&g_engine,collations);
    auto actual=ExprHelper::evalStringWithNullsSyntax(cache,sql,row,nulls,hints,"","",&g_engine,collations);
    require(same(old,actual),"cached helper differs from original actual evaluator: "+sql);return actual;
}
std::string trimmed(const std::string& value) {
    const auto last=value.find_last_not_of(" \t\r\n");return last==std::string::npos?"":value.substr(0,last+1);
}
struct Fixture {
    std::string name="local_expression_syntax",db=testDbPath(name);
    Session session;Session* prior=nullptr;
    Fixture() {
        cleanupTestDb(name);require(g_engine.createDatabase(db,"utf8")==DBStatus::OK,"create private actual database");
        session.username="admin";session.permission=1;session.currentDB=db;prior=currentSession();setCurrentSession(&session);
        struct SetupScope {
            Session* previous;bool retained=false;
            ~SetupScope(){if(!retained){if(g_engine.inTransaction())g_engine.rollbackTransaction();setCurrentSession(previous);}}
        } setupScope{prior};
        DdlExecutor ddl;require(!ddl.executeSql("CREATE TABLE syntax_rows(id integer PRIMARY KEY,v integer,t text)",session),"create real heap");
        require(!ddl.executeSql("CREATE TABLE syntax_empty(id integer PRIMARY KEY,v integer,t text)",session),"create real empty heap");
        require(g_engine.beginTransaction(db)==DBStatus::OK,"fixture transaction");
        for(size_t id=1;id<=512;++id)require(g_engine.insertRow(db,"syntax_rows",{
            {"id",std::to_string(id)}, {"v",id%7?StorageEngine::SqlCell{std::to_string(id*2)}:StorageEngine::SqlCell{}},
            {"t",id%11?StorageEngine::SqlCell{"row-"+std::to_string(id)}:StorageEngine::SqlCell{}}})==DBStatus::OK,"insert actual physical datum");
        require(g_engine.commitTransaction()==DBStatus::OK,"commit fixture");
        setupScope.retained=true;
    }
    ~Fixture(){if(g_engine.inTransaction())g_engine.rollbackTransaction();setCurrentSession(prior);}
    std::set<int> ids(const std::vector<std::string>& conditions,const std::string& table="syntax_rows") {
        const auto raw=g_engine.query(db,table,conditions,{"id"});std::set<int> result;
        for(const auto& row:raw)require(result.insert(std::stoi(trimmed(row))).second,"unique real row identity");return result;
    }
    auto rows() {
        std::vector<std::vector<std::string>> values;std::vector<std::vector<bool>> nulls;
        g_engine.query(db,"syntax_rows",{},{"id","t","v"},{},false,false,false,0,{},&values,&nulls);
        require(values.size()==512 && nulls.size()==512,"full actual heap snapshot");
        return std::make_pair(values,nulls);
    }
};
struct Rollback {
    ~Rollback(){if(g_engine.inTransaction())g_engine.rollbackTransaction();}
};
}

int main() {
    TypeRegistry::instance().bootstrap();size_t checked=0,failed=0;
    const auto check=[&](const char* label,const std::function<void()>& run) {
        ++checked;bool good=false;std::string failure;
        try{run();good=true;}catch(const DbError& error){failure=error.sqlState()+":"+error.what();}
        catch(const std::exception& error){failure=error.what();}
        failed+=!good;std::cout<<"LOCAL_EXPRESSION_SYNTAX "<<checked<<' '<<label<<" good="<<good;
        if(!good)std::cout<<" error="<<std::quoted(failure);std::cout<<'\n';
    };
    check("all original syntax families and both parser modes",[] {
        LocalExpressionSyntax cache;
        const std::vector<std::string> expressions={
            "CASE WHEN \"F\" > 1 THEN CAST(\"F\" AS numeric(9,2)) ELSE -2 END",
            "ARRAY[1,NULL,3]", "ROW(1,'x',NULL)", "1 = ANY(ARRAY[1,2])", "1 <> ALL(ARRAY[2,3])",
            "EXTRACT(YEAR FROM timestamp '2020-01-01')", "abs(x => 1)", "$1 + 2",
            "sum(x ORDER BY x DESC NULLS LAST) FILTER (WHERE x > 0)",
            "lag(x,2,0) OVER (PARTITION BY g ORDER BY x DESC NULLS FIRST ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE TIES)",
            "x COLLATE \"C\"", "(SELECT 1) + 2"};
        for(const auto& sql:expressions)for(bool binding:{false,true}){
            auto oracle=fresh("SELECT "+sql,binding);require(oracle.success,"fixture syntax was not accepted by original parser: "+sql);
            compareSyntax(cache,"SELECT "+sql,binding);}
    });
    check("original SQL bytes and source spans remain distinct",[] {
        LocalExpressionSyntax cache;
        for(const auto& sql:{"SELECT \"F\" + 1","SELECT f+1","SELECT 'a''b'","SELECT 'a  b'"})
            for(bool binding:{false,true})compareSyntax(cache,sql,binding);
        auto a=cache.parse("SELECT \"F\" + 1",true),b=cache.parse("SELECT f+1",true);
        require(snapshot(a)!=snapshot(b),"distinct actual quoted/source spelling");
    });
    check("consumer mutations never enter pristine syntax",[] {
        LocalExpressionSyntax cache;auto first=cache.parse("SELECT \"F\" + 1",true);
        const auto before=snapshot(first);auto& root=select(first).selectList.front().expr;
        auto* binary=dynamic_cast<BinaryOpExpr*>(root.get());require(binary,"binary fixture");
        auto* column=dynamic_cast<ColumnRefExpr*>(binary->left.get());require(column,"column fixture");
        column->column="execution-private-mutated-key";binary->op="changed";
        auto second=cache.parse("SELECT \"F\" + 1",true);require(snapshot(second)==before,"row-local name/op mutation leaked");
    });
    check("BETWEEN keeps exactly the original three syntax children",[] {
        LocalExpressionSyntax cache;
        for(const auto& sql:{"SELECT x BETWEEN 2 AND 7","SELECT x NOT BETWEEN 2 AND 7"})for(bool binding:{false,true}){
            auto a=cache.parse(sql,binding);auto* call=dynamic_cast<FunctionCallExpr*>(select(a).selectList.front().expr.get());
            require(call && call->args.size()==3,"no prepared fourth BETWEEN occurrence");compareSyntax(cache,sql,binding);}
    });
    check("original EXTRACT and array/cast semantic lowering stays per row",[] {
        LocalExpressionSyntax cache;
        for(size_t i=0;i<3;++i){auto x=evaluate(cache,"EXTRACT(YEAR FROM timestamp '2020-01-01')",{},{},{});
            require(x.ok && !x.isNull && std::stod(x.value)==2020,"actual EXTRACT result");
            auto a=evaluate(cache,"ARRAY[x,NULL]::integer[]",{{"x",std::to_string(i+1)}},{},{{"x","integer"}});
            require(a.ok && !a.isNull && a.typeName=="integer[]","actual array cast result");}
        compareSyntax(cache,"SELECT EXTRACT(YEAR FROM timestamp '2020-01-01')",false);
    });
    check("same syntax sees each actual type NULL and collation input",[] {
        LocalExpressionSyntax cache;
        auto a=evaluate(cache,"x",{{"x","9"}},{},{{"x","integer"}});require(a.ok && a.typeName=="integer" && a.value=="9","integer row");
        auto b=evaluate(cache,"x",{{"x","hello"}},{},{{"x","text"}},{{"x","C"}});
        require(b.ok && b.typeName=="text" && b.value=="hello" && !b.isNull,"text row");
        auto n=evaluate(cache,"x",{{"x",""}},{"x"},{{"x","text"}});require(n.ok && n.isNull,"actual NULL row");
        auto e=evaluate(cache,"x",{{"x",""}},{},{{"x","text"}});require(e.ok && !e.isNull && e.value.empty(),"actual empty text row");
    });
    check("runtime errors do not retain first-row values or bound state",[] {
        LocalExpressionSyntax cache;
        auto error=evaluate(cache,"10/x",{{"x","0"}},{},{{"x","integer"}});
        require(!error.ok && error.sqlState=="22012","primary division SQLSTATE");
        auto good=evaluate(cache,"10/x",{{"x","2"}},{},{{"x","integer"}});require(good.ok && good.value=="5","next row evaluated afresh");
    });
    check("AND OR demand uses original actual short circuit",[] {
        LocalExpressionSyntax cache;
        for(const auto& sql:{"x < 0 AND 10/0 > 1","x > 0 OR 10/0 > 1"}){
            const auto actual=evaluate(cache,sql,{{"x","1"}},{},{{"x","integer"}});
            require(actual.ok && actual.typeName=="boolean","unreached division stayed unreached");}
    });
    check("unsupported complete SELECT stays on the original parser",[] {
        LocalExpressionSyntax cache;const std::string sql="SELECT id FROM actual_rows WHERE id > 1";
        auto original=fresh(sql,true),first=cache.parse(sql,true),second=cache.parse(sql,true);
        require(original.success && select(original).fromClause,"genuine FROM fallback fixture");
        require(snapshot(first)==snapshot(original) && snapshot(second)==snapshot(original),"unsupported FROM not truncated");
        compareSyntax(cache,"SELECT (",false);compareSyntax(cache,"SELECT (",true);
    });
    check("fresh constructor clock and separate statement cache",[] {
        auto* previous=currentSession();setCurrentSession(nullptr);
        struct Restore{Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
        LocalExpressionSyntax first;
        const auto a=ExprHelper::evalStringWithNullsSyntax(first,"now()",{},{},{});require(a.ok && !a.isNull,"actual first constructor clock");
        const auto start=std::time(nullptr);
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(3);
        while(std::time(nullptr)==start && std::chrono::steady_clock::now()<deadline)std::this_thread::sleep_for(std::chrono::milliseconds(2));
        require(std::time(nullptr)>start,"real wall clock advanced");
        const auto b=ExprHelper::evalStringWithNullsSyntax(first,"now()",{},{},{});require(b.ok && b.typeName==a.typeName && !b.isNull && b.value!=a.value,"cached syntax did not retain evaluator clock");
        LocalExpressionSyntax second;auto x=evaluate(second,"x+1",{{"x","4"}},{},{{"x","integer"}});
        require(x.ok && x.value=="5","independent actual row binding");
    });
    std::unique_ptr<Fixture> fixture;
    const auto physical=[&]() -> Fixture& {if(!fixture)fixture=std::make_unique<Fixture>();return *fixture;};
    check("real 512-row predicate and stored NULL identity",[&] {
        auto& f=physical();const auto actual=f.ids({"typedexpr id BETWEEN 10 AND 29"});std::set<int> wanted;
        for(int id=10;id<=29;++id)wanted.insert(id);require(actual==wanted,"all real BETWEEN row identities");
        wanted.clear();for(int id=11;id<=512;id+=11)wanted.insert(id);
        require(f.ids({"typedexpr t IS NULL"})==wanted,"all genuine heap NULL identities");
    });
    check("empty heap preserves runtime and existing analysis boundaries",[&] {
        auto& f=physical();require(f.ids({"typedexpr 1/0 > 0"},"syntax_empty").empty(),"no runtime evaluation without row");
        bool error=false;try{(void)f.ids({"typedexpr missing_col = NULL"},"syntax_empty");}
        catch(const DbError& failure){error=failure.sqlState()=="42703";}require(error,"original pre-row NULL-column diagnostic");
        require(f.ids({"typedexpr id < 0 AND 1/0 > 0"}).empty(),"real heap short circuit");
    });
    check("actual 50-row UPDATE retains PK and transaction rollback",[&] {
        auto& f=physical();const auto before=f.rows();require(g_engine.beginTransaction(f.db)==DBStatus::OK,"begin actual mutation");Rollback rollback;
        size_t count=0;std::vector<StorageEngine::SqlRow> values;std::vector<StorageEngine::UpdateRowImage> images;
        const auto status=g_engine.updateRows(f.db,"syntax_rows",{{"v",std::string("9000")}},
            {"typedexpr id BETWEEN 200 AND 249"},&values,{}, {},&count,&images);
        require(status==DBStatus::OK && count==50 && values.size()==50 && images.size()==50,"actual affected and row images");
        std::set<int> wanted;for(int id=200;id<=249;++id)wanted.insert(id);
        require(f.ids({"=v 9000"})==wanted,"all actual new values and retained ids");
        for(int id:{199,200,249,250})require(f.ids({"=id "+std::to_string(id)})==std::set<int>{id},"real primary-key new RID mapping");
        require(g_engine.rollbackTransaction()==DBStatus::OK && f.rows()==before,"all original NULL/value rows restored");
    });
    check("late predicate error is primary atomic and later statement works",[&] {
        auto& f=physical();const auto before=f.rows();bool exact=false;size_t count=99;
        require(g_engine.beginTransaction(f.db)==DBStatus::OK,"real caller owns error transaction");Rollback rollback;
        try{(void)g_engine.updateRows(f.db,"syntax_rows",{{"v",std::string("8000")}},
            {"typedexpr 1/(id-257) > 0"},nullptr,{}, {},&count);}
        catch(const DbError& error){exact=error.sqlState()=="22012";}
        require(exact && count==0 && f.rows()==before,"primary division and all rows unchanged");
        // updateRows does not promise catch cleanup for a throwing caller.
        // Exercise the real caller-owned rollback rather than inventing one.
        require(g_engine.rollbackTransaction()==DBStatus::OK && !g_engine.inTransaction() && f.rows()==before,
            "actual caller rollback retained complete physical rows");
        require(f.ids({"typedexpr id BETWEEN 1 AND 2"})==std::set<int>({1,2}),"next actual statement independent");
        require(g_engine.getLockManager().captureCheckpoint().tableCounts.empty(),"actual locks released");
    });
    bool cleanup=true;
    if(fixture){const auto db=fixture->db;fixture.reset();cleanup=g_engine.dropDatabase(db)==DBStatus::OK;
        if(cleanup)cleanupTestDb("local_expression_syntax");}
    if(!cleanup){++failed;std::cerr<<"LOCAL_EXPRESSION_SYNTAX_CLEANUP_FAILED\n";}
    if(checked!=14){++failed;std::cerr<<"LOCAL_EXPRESSION_SYNTAX_BAD_CONTROL_COUNT="<<checked<<'\n';}
    std::cout<<"LOCAL_EXPRESSION_SYNTAX_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return failed?1:0;
}
