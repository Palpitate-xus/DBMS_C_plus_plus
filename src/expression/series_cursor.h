#pragma once

#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "types/numeric.h"
#include "utils/interval.h"
#include "utils/Session.h"
#include "common/DbError.h"
#include <algorithm>
#include <limits>
#include <memory>

namespace dbms {
// An execution-owned provider cursor. No provider is opened by metadata
// lookup, and a LIMIT must not allocate or compute an unrequested range.
class QueryHostValueCursor {
public:
    virtual ~QueryHostValueCursor() = default;
    virtual bool next(ExprValue&) = 0;
};

inline std::string seriesResultType(const std::vector<std::string>& input) {
    if(input.size()<2 || input.size()>4)throw DbError("42883","function generate_series does not exist for these arguments");
    std::vector<std::string> types;
    for(const auto& type:input)types.push_back(ExprHelper::canonicalResultTypeName(type));
    const auto unknown=[](const std::string& type){return type.empty() || type=="unknown";};
    const auto integer=[](const std::string& type){return type=="smallint" || type=="integer" || type=="bigint";};
    const auto temporal=[](const std::string& type){return type=="date" || type=="timestamp" || type=="timestamptz";};
    if(input.size()>=3 && (temporal(types[0]) || temporal(types[1]) || types[2]=="interval" || input.size()==4)) {
        if(input.size()<3 || (!unknown(types[2]) && types[2]!="interval") ||
           (!unknown(types[0]) && !temporal(types[0])) || (!unknown(types[1]) && !temporal(types[1])) ||
           (input.size()==4 && !unknown(types[3]) && types[3]!="text" && types[3]!="varchar" && types[3]!="name"))
            throw DbError("42883","function generate_series does not exist for these arguments");
        if(input.size()==4 || types[0]=="timestamptz" || types[1]=="timestamptz" ||
           (types[0]!="timestamp" && types[1]!="timestamp"))return "timestamptz";
        return "timestamp";
    }
    if(input.size()>3)throw DbError("42883","function generate_series does not exist for these arguments");
    bool exactNumeric=false;std::string result="integer";
    for(const auto& type:types) {
        if(unknown(type))continue;
        if(!integer(type) && type!="numeric")throw DbError("42883","function generate_series does not exist for these arguments");
        if(type!="smallint")exactNumeric=true;
        if(type=="numeric")result="numeric";
        else if(type=="bigint" && result!="numeric")result="bigint";
    }
    // int2 has no exact overload: integer, bigint and numeric are equally
    // eligible until another argument supplies one of those exact types.
    if(!exactNumeric)throw DbError("42725","function generate_series is not unique");
    return result;
}

class GenerateSeriesCursor final : public QueryHostValueCursor {
    std::string type_,zone_;
    ExprEvaluator evaluator_;
    ExprValue current_,stop_;
    IntervalInputParts interval_;
    Numeric number_,numericStop_,numericStep_;
    int64_t integer_=0,integerStop_=0,integerStep_=1;
    int direction_=1;
    bool empty_=false,first_=true,integerDone_=false;
    ExprValue cast(const ExprValue& value,const std::string& type) {
        CastExpr expression;expression.typeName=type;
        auto reference=std::make_unique<ColumnRefExpr>();reference->column="series_value";
        expression.operand=std::move(reference);
        RowContext context;context.set("series_value",value);
        return evaluator_.eval(&expression,context);
    }
    ExprValue binary(const ExprValue& left,const ExprValue& right,const std::string& op) {
        BinaryOpExpr expression;expression.op=op;
        auto lhs=std::make_unique<ColumnRefExpr>();lhs->column="series_left";
        auto rhs=std::make_unique<ColumnRefExpr>();rhs->column="series_right";
        expression.left=std::move(lhs);expression.right=std::move(rhs);
        RowContext context;context.set("series_left",left);context.set("series_right",right);
        return evaluator_.eval(&expression,context);
    }
    ExprValue timezone(const ExprValue& value) {
        FunctionCallExpr expression;expression.funcName="timezone";
        for(const auto* name:{"series_zone","series_value"}) {
            auto reference=std::make_unique<ColumnRefExpr>();reference->column=name;
            expression.args.push_back(std::move(reference));
        }
        RowContext context;context.set("series_zone",ExprValue("text",zone_));context.set("series_value",value);
        return evaluator_.eval(&expression,context);
    }
    void temporalStep() {
        // PostgreSQL applies months, then calendar days, then elapsed time.
        for(const auto field:{0,1,2}) {
            const auto amount=field==0?interval_.months:field==1?interval_.days:interval_.micros;
            if(!amount)continue;
            const auto step=ExprValue("interval",formatIntervalInput(field==0?amount:0,field==1?amount:0,field==2?amount:0));
            if(type_=="timestamptz" && field!=2) {
                auto local=timezone(current_);
                local=binary(local,step,"+");
                if(local.isNull)throw DbError("22008","timestamp out of range");
                current_=timezone(local);
            } else current_=binary(current_,step,"+");
            if(current_.isNull)throw DbError("22008","timestamp out of range");
        }
    }
public:
    GenerateSeriesCursor(const std::vector<ExprValue>& input,const std::string& result,const Session* session)
        :type_(result),zone_(session?session->timeZone:"UTC") {
        if(input.size()<2 || input.size()>4 ||
           ((type_=="timestamp" || type_=="timestamptz") && input.size()<3))
            throw DbError("XX000","generate_series cursor argument count changed");
        for(const auto& argument:input)if(argument.isNull){empty_=true;return;}
        if(type_=="integer" || type_=="bigint") {
            integer_=cast(input[0],type_).asInt();integerStop_=cast(input[1],type_).asInt();
            integerStep_=input.size()==3?cast(input[2],type_).asInt():1;
            if(!integerStep_)throw DbError("22023","step size cannot equal zero");
            direction_=integerStep_>0?1:-1;
        } else if(type_=="numeric") {
            number_=Numeric(cast(input[0],type_).value);numericStop_=Numeric(cast(input[1],type_).value);
            numericStep_=input.size()==3?Numeric(cast(input[2],type_).value):Numeric(1);
            if(!number_.isFinite() || !numericStop_.isFinite() || !numericStep_.isFinite())
                throw DbError("22023","start, stop and step must be finite");
            direction_=numericStep_.sign();
            if(!direction_)throw DbError("22023","step size cannot equal zero");
        } else {
            current_=cast(input[0],type_);stop_=cast(input[1],type_);
            interval_=parseIntervalInput(cast(input[2],"interval").value);
            const auto error=intervalInputSqlState(interval_);
            if(!error.empty())throw DbError(error,"invalid generate_series interval");
            const __int128 sign=(static_cast<__int128>(interval_.months)*30+interval_.days)*86400000000LL+interval_.micros;
            direction_=sign>0?1:sign<0?-1:0;
            if(!direction_)throw DbError("22023","step size cannot equal zero");
            if(input.size()==4)zone_=cast(input[3],"text").value;
            // Resolve the real zone even for an empty interval range.
            if(type_=="timestamptz") (void)timezone(current_);
        }
    }
    bool next(ExprValue& value) override {
        if(empty_)return false;
        if(type_=="integer" || type_=="bigint") {
            if(integerDone_ || (direction_>0?integer_>integerStop_:integer_<integerStop_))return false;
            value=ExprValue(type_,std::to_string(integer_));
            const __int128 next=static_cast<__int128>(integer_)+integerStep_;
            const auto low=type_=="integer"?std::numeric_limits<int32_t>::min():std::numeric_limits<int64_t>::min();
            const auto high=type_=="integer"?std::numeric_limits<int32_t>::max():std::numeric_limits<int64_t>::max();
            if(next<low || next>high)integerDone_=true;else integer_=static_cast<int64_t>(next);
            return true;
        }
        if(type_=="numeric") {
            if(direction_>0?number_>numericStop_:number_<numericStop_)return false;
            value=ExprValue(type_,number_.toString());
            try{number_+=numericStep_;}catch(const std::exception&){throw DbError("22003","numeric value out of range");}
            return true;
        }
        if(!first_)temporalStep();
        first_=false;
        const auto inside=binary(current_,stop_,direction_>0?"<=":">=");
        if(inside.isNull || !inside.asBool()){empty_=true;return false;}
        value=current_;
        // Datum output suppresses insignificant fractional-second zeros,
        // including those introduced by timestamp interval arithmetic.
        const auto dot=value.value.find('.');
        if(dot!=std::string::npos) {
            auto end=dot+1;
            while(end<value.value.size() && value.value[end]>='0' && value.value[end]<='9')++end;
            auto trimmed=end;
            while(trimmed>dot+1 && value.value[trimmed-1]=='0')--trimmed;
            if(trimmed==dot+1)--trimmed;
            value.value.erase(trimmed,end-trimmed);
        }
        return true;
    }
};
} // namespace dbms
