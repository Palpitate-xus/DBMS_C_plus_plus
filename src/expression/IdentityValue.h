#pragma once
#include "common/DbError.h"
#include "parser/ast.h"
#include <memory>
#include <string>

namespace dbms::identity_value_detail {
inline bool bound(const FunctionCallExpr* value) {
    return value && value->identitySequenceOid != 0;
}
inline void validate(const FunctionCallExpr& value) {
    const auto* oid = value.args.size() == 1
        ? dynamic_cast<const LiteralExpr*>(value.args.front().get()) : nullptr;
    if (!value.identitySequenceOid || value.funcName != "__dbms_identity_nextval" ||
        !value.schema.empty() || value.sqlValue != FunctionCallExpr::SqlValue::None ||
        value.sqlValuePrecision || value.distinct || value.filter || value.hasOver ||
        value.setReturning || !value.orderBy.empty() || !value.namedArgs.empty() ||
        !value.aggregateOrderKeys.empty() || !oid || oid->preparedSubquery ||
        oid->typeName != "oid" || oid->value != std::to_string(value.identitySequenceOid))
        throw DbError("XX001", "identity default lost its bound sequence value");
}
inline ExprPtr make(uint32_t sequenceOid) {
    auto result = std::make_unique<FunctionCallExpr>();
    result->funcName = "__dbms_identity_nextval";
    result->identitySequenceOid = sequenceOid;
    auto oid = std::make_unique<LiteralExpr>();
    oid->typeName = "oid"; oid->value = std::to_string(sequenceOid);
    result->args.push_back(std::move(oid));
    validate(*result);
    return result;
}
}
