#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

using namespace dbms;
namespace {
struct Trace { size_t opens=0,nexts=0,closes=0;bool active=false; };
class Rows final : public Operator {
    std::shared_ptr<Trace> trace_;
    size_t count_,position_=0;
    bool closeError_,readError_;
public:
    Rows(std::shared_ptr<Trace> trace,size_t count,bool closeError=false,bool readError=false)
        :trace_(std::move(trace)),count_(count),closeError_(closeError),readError_(readError) {}
    bool open() override {++trace_->opens;trace_->active=true;position_=0;return true;}
    bool next(std::string& row) override {
        ++trace_->nexts;
        if(readError_ && position_==1)throw DbError("22012","primary child read error");
        if(position_==count_)return false;
        ++position_;row="opaque physical row";return true;
    }
    // More than one SQL NULL row is still a cardinality violation. A NULL
    // value also avoids pretending this fixture's opaque display is a datum.
    bool lastColumnIsNull(size_t) const override {return true;}
    void close() override {
        ++trace_->closes;trace_->active=false;
        if(closeError_)throw DbError("58030","secondary child close error");
    }
};
OpPtr plan(std::shared_ptr<Trace> outer,std::shared_ptr<Trace> inner,size_t count,
           bool closeError=false,bool readError=false) {
    TableSchema schema;Column column;column.dataName="id";column.dataType="int";column.dsize=4;
    schema.append(column);
    return std::make_unique<ScalarSubqueryProjectOp>(std::make_unique<Rows>(outer,1),
        std::make_unique<Rows>(inner,count,closeError,readError),schema,schema,
        std::vector<ProjectionTarget>{{true,""}},"id");
}
}
int main() {
    // Exactly the legacy operator reached by the original full-protocol
    // scalar_multi_messages, not only the newer prepared carrier.
    for(const bool closeError:{false,true}) {
        auto outer=std::make_shared<Trace>(),inner=std::make_shared<Trace>();
        const auto failed=QueryPlanner::executePlanChecked(plan(outer,inner,2,closeError));
        assert(!failed.ok && failed.errorSqlState=="21000");
        assert(failed.errorMessage=="more than one row returned by a subquery used as an expression");
        assert(failed.rows.empty() && failed.structuredRows.empty() && failed.structuredNulls.empty());
        assert(inner->opens==1 && inner->nexts==2 && inner->closes==1 && !inner->active);
        assert(outer->opens==0 && outer->nexts==0 && outer->closes==1 && !outer->active);
        bool caught=false;try{failed.throwIfFailed();}catch(const DbError& error){caught=error.sqlState()=="21000";}
        assert(caught);
    }
    {
        auto outer=std::make_shared<Trace>(),inner=std::make_shared<Trace>();
        auto direct=plan(outer,inner,2,true);
        bool caught=false;try{(void)direct->open();}catch(const DbError& error){caught=error.sqlState()=="21000";}
        assert(caught && inner->closes==1 && !inner->active);
        direct->close();assert(inner->closes==1 && outer->closes==1);
    }
    for(const size_t count:{0,1}) {
        auto outer=std::make_shared<Trace>(),inner=std::make_shared<Trace>();
        auto result=QueryPlanner::executePlanChecked(plan(outer,inner,count));result.throwIfFailed();
        assert(result.structuredRows.size()==1 && result.structuredNulls[0][0]);
        assert(inner->closes==1 && outer->closes==1 && !inner->active && !outer->active);
    }
    // Structured child errors and a successful-path close failure keep their
    // own identity. Cleanup never substitutes a later close error for a read
    // failure, and never closes the inner child twice.
    for(const bool readError:{false,true}) {
        auto outer=std::make_shared<Trace>(),inner=std::make_shared<Trace>();
        auto result=QueryPlanner::executePlanChecked(plan(outer,inner,1,true,readError));
        assert(!result.ok && result.errorSqlState==(readError?"22012":"58030"));
        assert(inner->closes==1 && outer->closes==1 && !inner->active && !outer->active);
        assert(result.rows.empty() && result.structuredRows.empty());
    }
    std::cout<<"[SCALAR SUBQUERY OPERATOR SQLSTATE] exact cardinality and primary-error cleanup passed\n";
}
