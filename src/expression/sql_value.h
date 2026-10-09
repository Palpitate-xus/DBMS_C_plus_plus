#pragma once
#include "parser/ast.h"
#include "common/DbError.h"
#include <string>

namespace dbms::sql_value_detail {
using Kind = FunctionCallExpr::SqlValue;
inline Kind kind(const std::string& name) {
    if(name=="current_user")return Kind::CurrentUser;
    if(name=="session_user")return Kind::SessionUser;
    if(name=="current_role")return Kind::CurrentRole;
    if(name=="current_catalog")return Kind::CurrentCatalog;
    if(name=="current_schema")return Kind::CurrentSchema;
    if(name=="current_date")return Kind::CurrentDate;
    if(name=="current_time")return Kind::CurrentTime;
    if(name=="localtime")return Kind::LocalTime;
    if(name=="current_timestamp")return Kind::CurrentTimestamp;
    if(name=="localtimestamp")return Kind::LocalTimestamp;
    return Kind::None;
}
inline bool temporalPrecision(Kind value) {
    return value==Kind::CurrentTime || value==Kind::LocalTime ||
        value==Kind::CurrentTimestamp || value==Kind::LocalTimestamp;
}
inline std::string type(Kind value) {
    switch(value) {
    case Kind::CurrentDate:return "date";
    case Kind::CurrentTime:return "timetz";
    case Kind::LocalTime:return "time";
    case Kind::CurrentTimestamp:return "timestamptz";
    case Kind::LocalTimestamp:return "timestamp";
    case Kind::None:throw DbError("XX000","SQL value has no grammar owner");
    default:return "name";
    }
}
inline void validate(const FunctionCallExpr& call) {
    if(call.sqlValue==Kind::None || kind(call.funcName)!=call.sqlValue ||
       !call.args.empty() || !call.namedArgs.empty() || call.distinct || call.filter ||
       call.hasOver || !call.orderBy.empty() ||
       (call.sqlValuePrecision && (!temporalPrecision(call.sqlValue) || *call.sqlValuePrecision<0)))
        throw DbError("42601","invalid SQL value keyword grammar");
}
inline bool realNameCallee(const std::string& name) {
    return name=="current_user" || name=="session_user" ||
        name=="current_database" || name=="current_schema";
}
} // namespace dbms::sql_value_detail
