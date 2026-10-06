#include "Session.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <stdexcept>

using namespace dbms;
namespace {
struct Counts { size_t opens=0, nexts=0, closes=0, destroys=0; };
class Probe final : public Operator {
public:
    enum Failure { None, Metadata, Open, OpenFalse, Next, Cells, Reported, Cpp, Commit };
    Probe(std::shared_ptr<Counts> calls, PreparedQueryRows rows, Failure failure=None,
          bool closeFailure=false, bool typed=true)
        : calls_(std::move(calls)), rows_(std::move(rows)), failure_(failure),
          closeFailure_(closeFailure), typed_(typed) {}
    ~Probe() override { ++calls_->destroys; }
    bool supportsStructuredRows() const override {
        if(failure_==Metadata) fail();
        return true;
    }
    bool open() override {
        ++calls_->opens;
        if(failure_==Open) fail();
        if(failure_==OpenFalse) { setError("actual open failure");return false; }
        if(failure_==Commit) throw StatementCommitError("23503","actual commit failure");
        return true;
    }
    bool next(std::string& raw) override {
        ++calls_->nexts;
        if(failure_==Next) fail();
        if(failure_==Cpp) throw std::logic_error("actual C++ failure");
        if(failure_==Reported) { setError("actual next failure");return false; }
        if(position_==rows_.size()) return false;
        ++position_;raw="this display is not a cell";return true;
    }
    bool lastStructuredValues(std::vector<ExprValue>& cells) const override {
        if(failure_==Cells) fail();
        if(!typed_)return false;
        cells=rows_.at(position_-1);return true;
    }
    bool lastStructuredRow(std::vector<std::string>& cells,std::vector<bool>& nulls) const override {
        cells.clear();nulls.clear();
        for(const auto& value:rows_.at(position_-1)) {cells.push_back(value.value);nulls.push_back(value.isNull);}
        return true;
    }
    void close() override {
        ++calls_->closes;
        if(closeFailure_)throw DbError("58030","actual close failure");
    }
private:
    [[noreturn]] static void fail(){throw DbError("P0002","primary includes misleading SQLSTATE 22P02");}
    std::shared_ptr<Counts> calls_;PreparedQueryRows rows_;Failure failure_;
    bool closeFailure_,typed_;size_t position_=0;
};
template<class F> void expect(const std::string& state,F action) {
    bool caught=false;
    try {action();}catch(const DbError& error){assert(error.sqlState()==state);caught=true;}
    assert(caught);
}
const QueryRowDescriptor integer={{"value","integer"}},text={{"value","text"}};
std::unique_ptr<PreparedQueryCursor> cursor(std::shared_ptr<Counts> calls,
    PreparedQueryRows rows,Probe::Failure failure=Probe::None,bool closeFailure=false,
    QueryRowDescriptor descriptor=integer,bool typed=true) {
    return QueryPlanner::makePreparedCursor(std::make_unique<Probe>(calls,std::move(rows),
        failure,closeFailure,typed),std::move(descriptor));
}
}
int main() {
    TypeRegistry::instance().bootstrap();
    std::vector<ExprValue> row;
    auto calls=std::make_shared<Counts>();
    {
        ExprValue value("text","  NULL\tO'Brien  ");value.collation="C";
        auto plan=std::make_unique<Probe>(calls,PreparedQueryRows{{value},{ExprValue("text","",true)}});
        auto* actual=plan.get();
        auto stream=QueryPlanner::makePreparedCursor(std::move(plan),text);
        assert(stream->plan()==actual && stream->descriptor().front().type=="text");
        assert(calls->opens==0 && calls->nexts==0 && calls->closes==0);
        assert(stream->next(row) && row[0].value==value.value && row[0].collation=="C" && !row[0].isNull);
        assert(stream->next(row) && row[0].isNull && row[0].typeName=="text");
        assert(!stream->next(row) && row.empty());
        assert(!stream->next(row) && calls->nexts==3);
        stream->close();assert(calls->closes==1);
    }
    assert(calls->destroys==1);
    calls=std::make_shared<Counts>();
    {auto stream=cursor(calls,{{ExprValue("integer","1")},{ExprValue("integer","2")}});
     assert(stream->next(row));stream->close();expect("XX000",[&]{stream->next(row);});}
    assert(calls->nexts==1 && calls->closes==1);
    for(const auto failure:{Probe::Metadata,Probe::Open,Probe::Next,Probe::Cells})
        for(const bool secondary:{false,true}) {
            calls=std::make_shared<Counts>();
            {auto stream=cursor(calls,{{ExprValue("integer","1")}},failure,secondary);
             expect("P0002",[&]{stream->next(row);});assert(row.empty());stream->close();}
            assert(calls->closes==1 && calls->destroys==1);
        }
    for(const auto failure:{Probe::OpenFalse,Probe::Reported}) {
        calls=std::make_shared<Counts>();
        {auto stream=cursor(calls,{},failure,true);expect("XX000",[&]{stream->next(row);});}
        assert(calls->closes==1);
    }
    calls=std::make_shared<Counts>();
    {auto stream=cursor(calls,{},Probe::None,true);expect("58030",[&]{stream->next(row);});assert(row.empty());}
    assert(calls->closes==1);
    calls=std::make_shared<Counts>();
    {auto stream=cursor(calls,{},Probe::Cpp,true);bool exact=false;
     try{stream->next(row);}catch(const std::logic_error& e){exact=std::string(e.what())=="actual C++ failure";}
     assert(exact && row.empty());}
    assert(calls->closes==1);
    calls=std::make_shared<Counts>();
    {auto stream=cursor(calls,{},Probe::Commit,true);bool subtype=false;
     try{stream->next(row);}catch(const StatementCommitError& e){subtype=e.sqlState()=="23503";}
     assert(subtype);}
    assert(calls->closes==1);
    for(const bool terminate:{false,true}) {
        auto interrupt=std::make_shared<SessionInterruptState>();
        interrupt->cancelRequested=!terminate;interrupt->terminateRequested=terminate;
        setCurrentQueryInterruptState(interrupt);calls=std::make_shared<Counts>();
        {auto stream=cursor(calls,{});expect(terminate?"57P01":"57014",[&]{stream->next(row);});}
        setCurrentQueryInterruptState({});assert(calls->opens==0 && calls->closes==1);
    }
    for(const auto& malformed:PreparedQueryRows{{ExprValue("text","wrong")},{},
            {ExprValue("integer","1"),ExprValue("integer","2")}}) {
        calls=std::make_shared<Counts>();auto stream=cursor(calls,{malformed});
        expect("XX000",[&]{stream->next(row);});assert(row.empty() && calls->closes==1);
    }
    calls=std::make_shared<Counts>();
    {auto stream=cursor(calls,{},Probe::None,false,{{"bad","unknown"}});
     expect("XX000",[&]{stream->next(row);});assert(calls->opens==0);}
    // Compatibility structured strings retain whitespace and SQL NULL,
    // while the declared descriptor supplies type, never display parsing.
    calls=std::make_shared<Counts>();
    {auto stream=cursor(calls,{{ExprValue("text","NULL",false)},{ExprValue("text","",true)}},
        Probe::None,false,text,false);
     assert(stream->next(row) && row[0].typeName=="text" && !row[0].isNull && row[0].value=="NULL");
     assert(stream->next(row) && row[0].isNull);}
    // LIMIT/OFFSET preserve the actual typed cell (including collation).
    calls=std::make_shared<Counts>();
    ExprValue collated("text","actual");collated.collation="C";
    {OpPtr plan=std::make_unique<Probe>(calls,PreparedQueryRows{{collated},{collated}});
     plan=std::make_unique<OffsetOp>(std::move(plan),1);
     plan=std::make_unique<LimitOp>(std::move(plan),1);
     auto stream=QueryPlanner::makePreparedCursor(std::move(plan),text);
     assert(stream->next(row) && row[0].collation=="C");assert(!stream->next(row));}
    assert(calls->nexts==2 && calls->closes==1);
    // Buffered DISTINCT must retain the prepared typed cell, not reconstruct
    // its collation from a legacy string after the child has reached EOF.
    {
        StorageEngine localOwner;
        auto distinct=prepareQuery("SELECT DISTINCT 'actual'::TEXT COLLATE \"C\"",{},{});
        const auto descriptor=distinct.output;
        auto plan=QueryPlanner::buildPreparedSelectPlan(&localOwner,"unused","",std::move(distinct));
        auto stream=QueryPlanner::makePreparedCursor(std::move(plan),descriptor);
        assert(stream->next(row));
        std::cout<<"DISTINCT cursor value="<<row[0].value<<" collation="<<row[0].collation<<std::endl;
        assert(row[0].value=="actual" && row[0].collation=="c");
        assert(!stream->next(row));
    }

    // Pure prepared child metadata and real AST identity, no catalog/query
    // execution during preparation. Both callback contracts remain usable.
    StorageEngine owner;
    QueryBindingMetadata metadata;
    metadata.relation=[](const std::string&){return QueryRelationMetadata{"public","r",{{"id","integer"}}, {}};};
    for(const bool correlated:{false,true}) {
        auto query=std::make_shared<PreparedQuery>(prepareQuery(correlated?
            "SELECT(SELECT r.id),(SELECT r.id) FROM r":"SELECT(SELECT 1),(SELECT 1)",{},metadata));
        auto* select=static_cast<SelectStmt*>(query->ast.get());
        PreparedQueryExecution runtime(query,&owner,"unused");
        for(auto& item:select->selectList)runtime.prepareExpression(item.expr.get());
        size_t creates=0,compat=0;std::set<const Stmt*> sites;
        runtime.setQueryExecutor([&](const Stmt*,const RowContext&,size_t){++compat;return PreparedQueryRows{{ExprValue("integer","42")}};});
        auto total=std::make_shared<Counts>();
        runtime.setChildCursorFactory([&](const Stmt* original,const RowContext& cells){
            assert(query->statementOutputs.count(original));sites.insert(original);++creates;
            const auto value=correlated?cells.boundColumn(query->sourceRanges.front().ordinal,0):ExprValue("integer","1");
            return cursor(total,{{value}});
        });
        for(size_t n=0;n<3;++n) {
            auto cells=runtime.context();
            if(correlated)runtime.setSourceRow(cells,query->sourceRanges.front().ordinal,{ExprValue("integer",std::to_string(n+1))});
            for(const auto& target:select->selectList)
                assert(runtime.evaluate(target.expr.get(),cells).value==(correlated?std::to_string(n+1):"1"));
        }
        assert(creates==(correlated?6:2) && sites.size()==2 && compat==0 && total->closes==creates);
        runtime.setChildCursorFactory({});
        assert(runtime.evaluate(select->selectList[0].expr.get(),runtime.context()).value=="42" && compat==1);
        assert(!owner.inTransaction());
    }
    auto query=std::make_shared<PreparedQuery>(prepareQuery("SELECT(SELECT 1)",{},metadata));
    auto* site=static_cast<SelectStmt*>(query->ast.get())->selectList[0].expr.get();
    PreparedQueryExecution runtime(query,&owner,"unused");runtime.prepareExpression(site);
    calls=std::make_shared<Counts>();
    runtime.setChildCursorFactory([&](const Stmt*,const RowContext&){
        return cursor(calls,{{ExprValue("integer","1")},{ExprValue("integer","2")},{ExprValue("integer","3")}},Probe::None,true);});
    expect("21000",[&]{runtime.evaluate(site,runtime.context());});
    assert(calls->nexts==2 && calls->closes==1); // no third projection, primary beats close
    calls=std::make_shared<Counts>();
    runtime.setChildCursorFactory([&](const Stmt*,const RowContext&){return cursor(calls,{});});
    const auto empty=runtime.evaluate(site,runtime.context());assert(empty.isNull && empty.typeName=="integer");
    runtime.setChildCursorFactory([&](const Stmt*,const RowContext&){return cursor(calls,{{ExprValue("integer","9")}});});
    assert(runtime.evaluate(site,runtime.context()).value=="9"); // factory change invalidates initmemo
    calls=std::make_shared<Counts>();
    runtime.setChildCursorFactory([&](const Stmt*,const RowContext&){return cursor(calls,{},Probe::None,false,text);});
    expect("XX000",[&]{runtime.evaluate(site,runtime.context());});assert(calls->opens==0 && calls->closes==1);
    runtime.setChildCursorFactory([&](const Stmt*,const RowContext&)->std::unique_ptr<PreparedQueryCursor>{return {};});
    expect("XX000",[&]{runtime.evaluate(site,runtime.context());});
    // The cursor drives the genuine retained typed projection plan. A later
    // data-dependent CAST does not run after an early close; requesting its
    // row preserves the actual 22P02 rather than interpreting display text.
    for(const bool demandLater:{false,true}) {
        auto actual=std::make_shared<PreparedQuery>(prepareQuery(
            "SELECT CAST(CASE WHEN id=1 THEN '1' ELSE 'bad' END AS INT) FROM r",{},metadata));
        auto* select=static_cast<SelectStmt*>(actual->ast.get());
        const auto& range=*std::find_if(actual->sourceRanges.begin(),actual->sourceRanges.end(),
            [&](const auto& entry){return entry.owner==select;});
        TableSchema schema;Column id;id.dataName="id";
        assert(TypeRegistry::instance().resolveColumnType(id,"integer",{},false).empty());schema.append(id);
        size_t reads=0;
        auto source=std::make_unique<PreparedSourceRowsOp>(range.columns,
            [&](size_t at,std::vector<ExprValue>& cells){++reads;if(at>=3)return false;
                cells={ExprValue("integer",std::to_string(at+1))};return true;});
        auto plan=QueryPlanner::buildPreparedSelectPlan(&owner,"unused",actual,select,schema,std::move(source));
        auto* actualGraph=plan.get();
        auto stream=QueryPlanner::makePreparedCursor(std::move(plan),actual->statementOutputs.at(select));
        assert(reads==0 && stream->plan()==actualGraph && !owner.inTransaction());
        assert(stream->next(row) && row[0].value=="1" && reads==1);
        if(demandLater) {expect("22P02",[&]{stream->next(row);});assert(row.empty() && reads==2);}
        stream->close();assert(reads==(demandLater?2:1) && !owner.inTransaction());
    }
    // A cursor borrows a real caller transaction/snapshot/CID; consuming or
    // closing a child must not call the PL SPI bridge, commit, or refresh it.
    StorageEngine transactionOwner;
    const std::string fixture="prepared_query_cursor";
    cleanupTestDb(fixture);const auto database=testDbPath(fixture);
    assert(transactionOwner.createDatabase(database,"utf8")==DBStatus::OK);
    assert(transactionOwner.beginTransaction(database)==DBStatus::OK);
    assert(transactionOwner.beginSqlCommand());
    const auto xid=transactionOwner.currentTxnId();
    const auto cid=transactionOwner.currentCommandId();
    const auto before=*transactionOwner.getCurrentReadView();size_t spiCalls=0;
    transactionOwner.setPlpgsqlQueryExecutor([&](const std::string&,const std::string&,const PlPgsqlQueryOptions&){
        ++spiCalls;return PlPgsqlQueryResult{};});
    auto borrowed=std::make_shared<PreparedQuery>(prepareQuery("SELECT(SELECT p)",
        {{"cursor:p","p","bigint",{},true,"2147483648",1}},metadata));
    auto* borrowedExpr=static_cast<SelectStmt*>(borrowed->ast.get())->selectList.front().expr.get();
    PreparedQueryExecution borrowing(borrowed,&transactionOwner,database);borrowing.prepareExpression(borrowedExpr);
    calls=std::make_shared<Counts>();
    borrowing.setChildCursorFactory([&](const Stmt* child,const RowContext& context){
        assert(child==borrowedExpr->preparedSubquery.get() && context.parameter(0).typeName=="bigint");
        return cursor(calls,{{context.parameter(0)}},Probe::None,false,{{"p","bigint"}});});
    assert(borrowing.evaluate(borrowedExpr,borrowing.context()).value=="2147483648");
    const auto* after=transactionOwner.getCurrentReadView();
    assert(transactionOwner.currentTxnId()==xid && transactionOwner.currentCommandId()==cid && spiCalls==0);
    assert(after->activeTxnIds==before.activeTxnIds && after->upLimitId==before.upLimitId &&
        after->lowLimitId==before.lowLimitId && calls->closes==1);
    assert(transactionOwner.rollbackTransaction()==DBStatus::OK);cleanupTestDb(fixture);
    std::cout<<"[PREPARED QUERY CURSOR] metadata/types/NULL/demand/AST ownership/error priority/close once passed\n";
}
