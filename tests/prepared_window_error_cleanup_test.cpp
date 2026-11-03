#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "Session.h"
#include "test_utils.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
enum class Mode { OpenFalse, NextFalse, NextThrow };
struct Probe { size_t opens=0,nexts=0,closes=0,heapCloses=0; };
class FaultSource final : public Operator {
    OpPtr heap_;
    std::shared_ptr<Probe> probe_;
    Mode mode_;
    bool opened_=false,heapOpened_=false;
public:
    FaultSource(OpPtr heap,std::shared_ptr<Probe> probe,Mode mode)
        :heap_(std::move(heap)),probe_(std::move(probe)),mode_(mode) {}
    ~FaultSource() override { try { close(); } catch(...) {} }
    bool open() override {
        clearError();opened_=true;++probe_->opens;
        if(mode_==Mode::OpenFalse){setError("primary-open-false");return false;}
        heapOpened_=true;return heap_->open();
    }
    bool next(std::string&) override {
        ++probe_->nexts;
        if(mode_==Mode::NextFalse){setError("primary-next-false");return false;}
        throw DbError("22012","primary-thrown-division");
    }
    void close() override {
        if(!opened_)return;
        opened_=false;++probe_->closes;
        if(heapOpened_){heapOpened_=false;heap_->close();++probe_->heapCloses;}
        throw DbError("22003","secondary-source-close");
    }
    bool supportsStructuredRows() const override {return heap_->supportsStructuredRows();}
    bool lastStructuredValues(std::vector<ExprValue>& row) const override {return heap_->lastStructuredValues(row);}
    ScanOrigin scanOrigin() const override {return heap_->scanOrigin();}
};
}

int main() {
    using namespace dbms;TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("window_fault_cleanup_probe");cleanupTestDb("window_fault_cleanup_probe");
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 2;
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE window_fault_heap(id INTEGER)",session))return 2;
    if(g_engine.insertRow(db,"window_fault_heap",{{"id","1"}})!=DBStatus::OK)return 2;
    const auto schema=g_engine.getTableSchema(db,"window_fault_heap");
    size_t controls=0,failures=0;
    for(Mode mode:{Mode::OpenFalse,Mode::NextFalse,Mode::NextThrow}) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT row_number() OVER() AS rn FROM window_fault_heap"));
        auto probe=std::make_shared<Probe>();
        auto source=std::make_unique<FaultSource>(std::make_unique<TableScanOp>(&g_engine,db,"window_fault_heap"),probe,mode);
        auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,db,query,static_cast<SelectStmt*>(query->ast.get()),schema,std::move(source));
        auto cursor=QueryPlanner::makePreparedCursor(std::move(plan),query->output);
        std::string state,message;bool returned=false;std::vector<ExprValue> row;
        try{returned=cursor->next(row);}catch(const DbError& error){state=error.sqlState();message=error.message();}
        catch(const std::exception& error){state="std-exception";message=error.what();}
        std::string closeError;
        try{cursor->close();cursor->close();}catch(const std::exception& error){closeError=error.what();}
        const auto expectedState=mode==Mode::NextThrow?"22012":"XX000";
        const auto expectedMessage=mode==Mode::OpenFalse?"primary-open-false":mode==Mode::NextFalse?"primary-next-false":"primary-thrown-division";
        const bool good=!returned && row.empty() && state==expectedState && message==expectedMessage &&
            probe->opens==1 && probe->closes==1 && probe->nexts==(mode==Mode::OpenFalse?0u:1u) &&
            probe->heapCloses==(mode==Mode::OpenFalse?0u:1u) && closeError.empty() &&
            StorageEngine::captureNullRowBinding().engine==nullptr;
        ++controls;failures+=!good;
        std::cout<<"WINDOW_FAULT_CONTROL "<<controls<<" mode="<<static_cast<int>(mode)<<" state="<<state<<" message="<<message
            <<" expected_state="<<expectedState<<" expected_message="<<expectedMessage<<" opens="<<probe->opens
            <<" nexts="<<probe->nexts<<" closes="<<probe->closes<<" heap_closes="<<probe->heapCloses<<" pass="<<good<<'\n';
    }
    const auto dropped=g_engine.dropDatabase(db);setCurrentSession(previous);cleanupTestDb("window_fault_cleanup_probe");
    std::cout<<"WINDOW_FAULT_COMPLETE "<<controls<<" FAILURES "<<failures<<" drop_ok="<<(dropped==DBStatus::OK)<<'\n';
    return controls==3 && failures==0 && dropped==DBStatus::OK?0:1;
}
