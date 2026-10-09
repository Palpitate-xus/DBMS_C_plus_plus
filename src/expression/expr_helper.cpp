#include "expr_helper.h"
#include "expression/aggregate_type.h"
#include "between_input.h"
#include "expression/common_type.h"
#include "array_type.h"
#include "arithmetic_type.h"
#include "geometric_input.h"
#include "ExprEvaluator.h"
#include "sql_value.h"
#include "parser/parser.h"
#include "parser/ast.h"
#include "parser/query_binding.h"
#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "catalog/declared_type.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "common/DbError.h"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <cstring>
#include <functional>
#include <memory>
#include <sstream>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace dbms {

namespace {

std::string toLower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

std::string formatUtcClock(std::time_t value, const char* format) {
    std::tm utc{};
    if (::gmtime_r(&value, &utc) == nullptr) return "";
    char buffer[40];
    if (std::strftime(buffer, sizeof(buffer), format, &utc) == 0) return "";
    return buffer;
}

bool looksLikeNumber(const std::string& s) {
    if (s.empty()) return false;
    size_t i = 0;
    if (s[0] == '+' || s[0] == '-') i = 1;
    bool hasDot = false, hasDigit = false;
    for (; i < s.size(); ++i) {
        if (s[i] >= '0' && s[i] <= '9') { hasDigit = true; continue; }
        if (s[i] == '.' && !hasDot) { hasDot = true; continue; }
        return false;
    }
    return hasDigit;
}

// Legacy public AST callers annotate an unquoted NULL token with "null".
// This is a NULL sentinel, not a declared SQL type. Keep real annotations
// and quoted text/type names on their normal declaration path.
bool legacyNullLiteral(const LiteralExpr* literal) {
    return literal && !literal->preparedSubquery &&
        toLower(literal->value)=="null" && toLower(literal->typeName)=="null";
}

std::string inferType(const std::string& value) {
    if (value.empty()) return "text";
    if (looksLikeNumber(value)) {
        return value.find('.') != std::string::npos ? "double precision" : "integer";
    }
    return "text";
}

std::string canonicalTypeName(const std::string& storageType) {
    if(const auto geometry=geometric_input_detail::builtinType(storageType);!geometry.empty())
        return geometry;
    std::string t = toLower(storageType);
    // Storage aliases must retain the datum's actual width.  In particular,
    // labeling BIGINT as integer changes arithmetic overflow and return casts.
    if (t == "int2" || t == "smallint" || t == "smallserial") return "smallint";
    if (t == "int" || t == "int4" || t == "integer" || t == "serial") return "integer";
    if (t == "int8" || t == "bigint" || t == "bigserial") return "bigint";
    if (t == "float" || t == "float4" || t == "real") return "real";
    if (t == "double" || t == "float8" || t == "double precision") return "double precision";
    if (t == "numeric" || t == "decimal") return "numeric";
    if (t == "char" || t == "character" || t == "bpchar") {
        return "bpchar";
    }
    if (t == "varchar" || t == "character varying" || t == "text") {
        return "text";
    }
    if (t == "bool" || t == "boolean") return "boolean";
    if (t == "timestamp" || t == "datetime") return "timestamp";
    if (t == "timestamptz" || t == "timestamp with time zone")
        return "timestamptz";
    if (t == "date") return "date";
    if (t == "time") return "time";
    if (t == "timetz" || t == "time with time zone") return "timetz";
    if (t == "interval") return "interval";
    if (t == "uuid") return "uuid";
    if (t == "blob") return "bytea";
    return t;
}

std::string protocolTypeName(std::string type) {
    while(!type.empty() && std::isspace(static_cast<unsigned char>(type.back())))type.pop_back();
    bool array=false;
    while(type.size()>=2 && type.compare(type.size()-2,2,"[]")==0) {
        array=true;type.resize(type.size()-2);
        while(!type.empty() && std::isspace(static_cast<unsigned char>(type.back())))type.pop_back();
    }
    if(array)return protocolTypeName(type)+"[]";
    if(const auto geometry=geometric_input_detail::builtinType(type);!geometry.empty())
        return geometry;
    type = toLower(type);
    if (type.rfind("interval ", 0) == 0) {
        const auto declaration = SQLParser::parseTypeSpecification(type);
        if (SQLParser::toLower(declaration.typeName) == "interval") return "interval";
    }
    const size_t modifier = type.find('(');
    if (modifier != std::string::npos) type.resize(modifier);
    while (!type.empty() && std::isspace(static_cast<unsigned char>(type.back())))
        type.pop_back();
    if (type == "int" || type == "int4" || type == "serial") return "integer";
    if (type == "int2" || type == "smallserial") return "smallint";
    if (type == "int8" || type == "bigserial") return "bigint";
    if (type == "decimal") return "numeric";
    if (type == "float8" || type == "double")
        return "double precision";
    if (type == "float" || type == "float4") return "real";
    if (type == "bool") return "boolean";
    if (type == "character varying") return "varchar";
    if (type == "character") return "bpchar";
    if (type == "timestamp with time zone") return "timestamptz";
    if (type == "timestamp without time zone") return "timestamp";
    if (type == "time with time zone") return "timetz";
    if (type == "time without time zone") return "time";
    if (type == "blob") return "bytea";
    if (type == "varbit") return "bit varying";
    return type;
}

bool numericProtocolType(const std::string& type) {
    const std::string t = protocolTypeName(type);
    return t == "smallint" || t == "integer" || t == "bigint" ||
           t == "numeric" || t == "real" || t == "double precision";
}

int numericTypeRank(const std::string& type) {
    const std::string t = protocolTypeName(type);
    if (t == "double precision") return 6;
    if (t == "real") return 5;
    if (t == "numeric") return 4;
    if (t == "bigint") return 3;
    if (t == "integer") return 2;
    if (t == "smallint") return 1;
    return 0;
}

std::string mergeProtocolTypes(const std::string& leftRaw,
                               const std::string& rightRaw) {
    const std::string left = protocolTypeName(leftRaw);
    const std::string right = protocolTypeName(rightRaw);
    if (left.empty() || left == "unknown") return right;
    if (right.empty() || right == "unknown") return left;
    if (left == right) return left;
    if (numericProtocolType(left) && numericProtocolType(right)) {
        const int rank = std::max(numericTypeRank(left), numericTypeRank(right));
        if (rank >= 6) return "double precision";
        if (rank == 5) return "real";
        if (rank == 4) return "numeric";
        if (rank == 3) return "bigint";
        if (rank == 2) return "integer";
        return "smallint";
    }
    if ((left == "text" || left == "varchar" || left == "bpchar") &&
        (right == "text" || right == "varchar" || right == "bpchar")) {
        if (left == "text" || right == "text") return "text";
        if (left == "varchar" || right == "varchar") return "varchar";
        return "bpchar";
    }
    return left;
}

std::string builtinReductionName(const FunctionCallExpr* call) {
    if (!call) return {};
    CatalogManager::QualifiedName function, schema;
    if (!CatalogManager::parseQualifiedName(call->funcName, function, true) ||
        !function.schema.empty()) return {};
    if (!call->schema.empty() &&
        (!CatalogManager::parseQualifiedName(call->schema, schema, true) ||
         !schema.schema.empty() || schema.name != "pg_catalog")) return {};
    static const std::set<std::string> reductions = {
        "sum", "avg", "count", "min", "max", "bool_and", "bool_or", "every"};
    return reductions.count(function.name) ? function.name : std::string{};
}

std::string inferAstResultType(
    const Expr* expression,
    const std::map<std::string, std::string>& typeHints,
    const std::map<const Expr*, std::string>* routineTypes = nullptr) {
    if (!expression) return "text";
    if(routineTypes) {
        const auto declared=routineTypes->find(expression);
        if(declared!=routineTypes->end())return protocolTypeName(declared->second);
    }
    if (const auto* parameter = dynamic_cast<const ParameterExpr*>(expression))
        return protocolTypeName(parameter->declaredType);
    if (const auto* literal = dynamic_cast<const LiteralExpr*>(expression)) {
        if (legacyNullLiteral(literal)) return "unknown";
        if (!literal->typeName.empty())
            return protocolTypeName(ExprHelper::declaredTypeInput(literal->typeName));
        const std::string value = toLower(literal->value);
        if (literal->value.size() >= 3 && literal->value[1] == '\'' &&
            (literal->value[0] == 'b' || literal->value[0] == 'B' ||
             literal->value[0] == 'x' || literal->value[0] == 'X')) {
            return "bit";
        }
        if (value == "null" ||
            (literal->value.size() >= 2 && literal->value.front() == '\'' &&
             literal->value.back() == '\'')) return "unknown";
        if (value == "true" || value == "false") return "boolean";
        if (looksLikeNumber(literal->value))
            return literal->value.find_first_of(".eE") == std::string::npos
                ? ExprHelper::inferValuesResultType(literal->value)
                : "numeric";
        return "unknown";
    }
    if (const auto* column = dynamic_cast<const ColumnRefExpr*>(expression)) {
        if (column->binding) return protocolTypeName(column->binding->declaredType);
        for (const std::string& key : {
                 column->toString(), column->column}) {
            auto found = typeHints.find(key);
            if (found != typeHints.end())
                return protocolTypeName(found->second);
        }
        if (column->schema.empty() && column->table.empty()) {
            if (column->column == "current_user" || column->column == "session_user" ||
                column->column == "user") return "name";
            if (column->column == "current_date") return "date";
            if (column->column == "current_timestamp") return "timestamptz";
            if (column->column == "localtimestamp") return "timestamp";
        }
        return "text";
    }
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression))
        return protocolTypeName(ExprHelper::declaredTypeInput(cast->typeName));
    if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expression)) {
        const std::string op = toLower(unary->op);
        if (op == "not" || op.find("is ") == 0) return "boolean";
        if (op.rfind("at time zone", 0) == 0) {
            const std::string input = inferAstResultType(
                unary->operand.get(), typeHints, routineTypes);
            return input == "timestamptz" ? "timestamp" : "timestamptz";
        }
        if (op == "-") {
            if (const auto* literal = dynamic_cast<const LiteralExpr*>(
                    unary->operand.get());
                literal && literal->typeName.empty() &&
                looksLikeNumber(literal->value) &&
                literal->value.find_first_of(".eE") == std::string::npos) {
                return ExprHelper::inferValuesResultType(
                    "-" + literal->value);
            }
        }
        return inferAstResultType(unary->operand.get(), typeHints, routineTypes);
    }
    if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expression)) {
        const std::string op = toLower(binary->op);
        if (op == "::") {
            if (const auto* target =
                    dynamic_cast<const LiteralExpr*>(binary->right.get())) {
                return protocolTypeName(ExprHelper::declaredTypeInput(target->value));
            }
            return protocolTypeName(binary->right
                                        ? binary->right->toString()
                                        : std::string("unknown"));
        }
        static const std::set<std::string> booleanOperators = {
            "and", "or", "=", "<>", "!=", "<", ">", "<=", ">=",
            "like", "not like", "ilike", "not ilike", "in", "not in",
            "between", "not between", "is distinct from",
            "is not distinct from", "similar to", "not similar to",
            "<<=", ">>=", "&&", "@>", "<@", "~", "~*", "!~", "!~*"
        };
        if (booleanOperators.count(op)) return "boolean";
        const std::string left = inferAstResultType(binary->left.get(), typeHints, routineTypes);
        const std::string right = inferAstResultType(binary->right.get(), typeHints, routineTypes);
        if(op=="[]" || op=="[:]") {
            const Expr* receiver=binary->left.get();
            if(op=="[]")while(const auto* item=dynamic_cast<const BinaryOpExpr*>(receiver)) {
                if(item->op!="[]")break;
                receiver=item->left.get();
            }
            const auto type=ExprHelper::canonicalResultTypeName(inferAstResultType(receiver,typeHints,routineTypes));
            return op=="[]" && type.size()>=2 && type.compare(type.size()-2,2,"[]")==0
                ?type.substr(0,type.size()-2):type;
        }
        if (op == "->>" || op == "#>>") return "text";
        if (op == "->" || op == "#>") return left;
        if (op == "||") {
            if (const auto binding = ExprHelper::resolveArrayConcatTypes(left, right, true))
                return binding->elementType + "[]";
            if ((left == "bit" || left == "bit varying") &&
                (right == "bit" || right == "bit varying")) {
                return "bit";
            }
            return left == "bytea" && right == "bytea" ? "bytea" : "text";
        }
        if (op == "&" || op == "|" || op == "#" ||
            op == "<<" || op == ">>") {
            if ((op == "<<" || op == ">>") &&
                (left == "inet" || left == "cidr")) {
                return "boolean";
            }
            return left;
        }
        if (op == "+" || op == "-") {
            if (left == "money" && right == "money") return "money";
            if (left == "date" && right == "interval") return "timestamp";
            if ((left == "timestamp" || left == "timestamptz") &&
                right == "interval") return left;
            if (op == "-" && left == "date" && right == "date") return "integer";
            if (op == "-" && left == "timestamp" && right == "timestamp")
                return "interval";
            if (left == "date" && numericProtocolType(right)) return "date";
        }
        if (op == "/" && left == "money" && right == "money")
            return "double precision";
        if (op == "*" &&
            ((left == "money" && numericProtocolType(right)) ||
             (right == "money" && numericProtocolType(left)))) {
            return "money";
        }
        if (op == "/" && left == "money" && numericProtocolType(right))
            return "money";
        if (const auto type = arithmetic_detail::resultType(op, left, right))
            return *type;
        return mergeProtocolTypes(left, right);
    }
    if (dynamic_cast<const QuantifiedComparisonExpr*>(expression)) return "boolean";
    if (const auto* array = dynamic_cast<const ArrayExpr*>(expression)) {
        if (!array->elementType.empty()) return array->elementType + "[]";
        std::vector<std::string> types;
        for (const auto& value : array->elements) types.push_back(inferAstResultType(value.get(),typeHints,routineTypes));
        return array_detail::commonElement(types) + "[]";
    }
    if (dynamic_cast<const RowExpr*>(expression)) return "record";
    if (const auto* caseExpression = dynamic_cast<const CaseExpr*>(expression)) {
        std::vector<std::string> types{
            caseExpression->elseExpr ? inferAstResultType(caseExpression->elseExpr.get(),typeHints,routineTypes)
                                     : "unknown"};
        for (const auto& clause : caseExpression->whenClauses)
            types.push_back(inferAstResultType(clause.second.get(),typeHints,routineTypes));
        return selectCommonType(types,"CASE");
    }
    if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expression)) {
        if(call->sqlValue!=FunctionCallExpr::SqlValue::None)
            return sql_value_detail::type(call->sqlValue);
        if (!call->resolvedResultType.empty()) return protocolTypeName(call->resolvedResultType);
        if(call->setReturning)return call->setReturning->elementType;
        if (routineTypes) {
            const auto routine = routineTypes->find(call);
            if (routine != routineTypes->end()) return protocolTypeName(routine->second);
        }
        const std::string reduction = builtinReductionName(call);
        const std::string name = reduction.empty() ? toLower(call->funcName) : reduction;
        // These names are parser-owned three-operand grammar nodes, not
        // ordinary scalar calls whose type can follow their first argument.
        if (call->schema.empty() &&
            (name == "between" || name == "not between" ||
             name == "like escape" || name == "not like escape" ||
             name == "ilike escape" || name == "not ilike escape" ||
             name == "similar to escape" || name == "not similar to escape"))
            return "boolean";
        auto argType = [&](size_t index) {
            return index < call->args.size()
                ? inferAstResultType(call->args[index].get(), typeHints, routineTypes)
                : std::string("unknown");
        };
        if (name == "cast" && call->args.size() >= 2) {
            if (const auto* target =
                    dynamic_cast<const ColumnRefExpr*>(call->args[1].get())) {
                return protocolTypeName(target->column);
            }
            if (const auto* target =
                    dynamic_cast<const LiteralExpr*>(call->args[1].get())) {
                return protocolTypeName(target->value);
            }
            return protocolTypeName(call->args[1]->toString());
        }
        if (name == "case_when") {
            std::string result = "unknown";
            for (size_t i = 1; i < call->args.size(); i += 2)
                result = mergeProtocolTypes(result, argType(i));
            if (call->args.size() % 2 == 1)
                result = mergeProtocolTypes(result, argType(call->args.size() - 1));
            return result;
        }
        if (name == "exists" || name == "is_null" || name == "is_not_null" ||
            name == "isdistinct" || name == "isnotdistinct" ||
            name == "xml_is_well_formed" ||
            name == "xml_is_well_formed_content" ||
            name == "xml_is_well_formed_document" ||
            name == "xml_is_document") return "boolean";
        if (name == "xmlconcat" || name == "xmlcomment") return "xml";
        if (name == "pg_notify") return "void";
        if (name == "gen_random_uuid" || name == "uuidv4" ||
            name == "uuidv7") return "uuid";
        if (name == "uuid_extract_version") return "smallint";
        if (name == "uuid_extract_timestamp") return "timestamptz";
        if (name == "decode" || name == "convert" || name == "convert_to" ||
            name == "sha224" || name == "sha256" || name == "sha384" ||
            name == "sha512" ||
            name == "reverse" || name == "set_byte" ||
            name == "set_bit" || name == "substring" || name == "substr" ||
            name == "overlay" || name == "btrim" || name == "ltrim" ||
            name == "rtrim") {
            if (name == "decode" || name == "convert" ||
                name == "convert_to" || name == "sha224" ||
                name == "sha256" || name == "sha384" ||
                name == "sha512") return "bytea";
            const std::string input = argType(0);
            if (input == "bytea") return "bytea";
            if ((name == "substring" || name == "substr") &&
                (input == "bit" || input == "bit varying")) return "bit";
        }
        if (name == "convert_from") return "text";
        if (name == "pg_notification_queue_usage") return "double precision";
        if (name == "count" || name == "row_number" || name == "rank" ||
            name == "dense_rank") return "bigint";
        if (name == "ntile" || name == "width_bucket" || name == "length" ||
            name == "char_length" || name == "character_length" ||
            name == "bit_length" || name == "octet_length" || name == "strpos" ||
            name == "position" || name == "get_byte" || name == "get_bit" ||
            name == "ascii" || name == "gcd" ||
            name == "lcm") return "integer";
        if (name == "bit_count" || name == "crc32" || name == "crc32c" ||
            name == "nextval" || name == "currval" || name == "lastval")
            return "bigint";
        if (name == "percent_rank" || name == "cume_dist" || name == "date_part")
            return "double precision";
        if (name == "extract") return "numeric";
        if (name == "age") return "interval";
        if (name == "to_date") return "date";
        if (name == "to_timestamp") return "timestamptz";
        if (name == "timezone")
            return argType(1) == "timestamp" ? "timestamptz" : "timestamp";
        if (name == "date_trunc") {
            const std::string input = argType(1);
            // PostgreSQL resolves date input through the timestamptz
            // overload; the evaluator also returns a zoned timestamp.
            return input == "date" ? "timestamptz" : input;
        }
        if (name == "current_date") return "date";
        if (name == "now" || name == "current_timestamp") return "timestamptz";
        if (name == "string_to_array" || name == "regexp_split_to_array" ||
            name == "regexp_matches") return "text[]";
        if (name == "array_append" || name == "array_prepend" ||
            name == "array_remove" || name == "array_replace")
            return name == "array_prepend" ? argType(1) : argType(0);
        if(name=="array_cat"){
            const auto binding=ExprHelper::resolveArrayConcatTypes(argType(0),argType(1));
            return binding ? binding->elementType+"[]" : "text[]";
        }
        if(name=="array_position" || name=="array_length" || name=="array_upper" ||
            name=="array_lower" || name=="array_ndims" || name=="cardinality")return "integer";
        if (name == "array_agg") {
            const std::string element = argType(0);
            return (element.empty() || element == "unknown" ? "text" : element) + "[]";
        }
        if (name == "string_agg") return "text";
        if (name == "json_agg") return "json";
        if (name == "jsonb_agg") return "jsonb";
        if (name == "bool_and" || name == "bool_or" || name == "every")
            return "boolean";
        if (name == "sum") {
            const std::string input = argType(0);
            if (input == "smallint" || input == "integer") return "bigint";
            if (input == "bigint" || input == "numeric") return "numeric";
            if (input == "real" || input == "double precision" ||
                input == "money" || input == "interval") return input;
        }
        if (name == "avg" || name == "stddev" || name == "stddev_samp" ||
            name == "stddev_pop" || name == "variance" || name == "var_samp" ||
            name == "var_pop") {
            const std::string input = argType(0);
            if (name == "avg" && input == "interval") return "interval";
            return input == "real" || input == "double precision"
                ? "double precision" : "numeric";
        }
        if (name == "sign") {
            const std::string input = argType(0);
            return input == "numeric" ? "numeric" : "double precision";
        }
        if (name == "coalesce" || name == "greatest" || name == "least") {
            std::string result = "unknown";
            for (size_t i = 0; i < call->args.size(); ++i)
                result = mergeProtocolTypes(result, argType(i));
            return result == "unknown" ? "text" : result;
        }
        if (name == "min" || name == "max" || name == "lag" ||
            name == "lead" || name == "first_value" || name == "last_value" ||
            name == "nth_value" || name == "nullif" || name == "abs" ||
            name == "round" || name == "trunc" || name == "ceil" ||
            name == "ceiling" || name == "floor" || name == "mod")
            return argType(0);
        if (name == "div") return "numeric";
        if (name == "power") {
            const std::string left = argType(0);
            const std::string right = argType(1);
            return left == "numeric" || right == "numeric"
                ? "numeric" : "double precision";
        }
        if (name == "log" && call->args.size() == 2) return "numeric";
        if (name == "exp" || name == "ln" || name == "log" || name == "sqrt" ||
            name == "sin" || name == "cos" || name == "tan")
            return argType(0) == "numeric" ? "numeric" : "double precision";
        static const std::set<std::string> textFunctions = {
            "lower", "upper", "initcap", "concat", "concat_ws", "substring",
            "substr", "left", "right", "trim", "btrim", "ltrim", "rtrim",
            "reverse", "replace", "translate", "format", "quote_literal",
            "quote_nullable", "overlay", "to_char"
        };
        if (textFunctions.count(name)) return "text";
        return "text";
    }
    return "text";
}

// Keep routine result metadata associated with the exact live AST node, not
// with strings which could collide with user column names. This walk resolves
// declarations only: it does not bind callbacks or execute any expression.
std::map<const Expr*, std::string> collectRoutineResultTypes(
    const Expr* expression, const std::string& currentDB,
    StorageEngine* functionEngine) {
    std::map<const Expr*, std::string> routineTypes;
    if (currentDB.empty()) return routineTypes;
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    std::function<void(const Expr*)> inspect = [&](const Expr* node) {
        if (!node) return;
        if(const auto* literal=dynamic_cast<const LiteralExpr*>(node)) {
            if(!literal->typeName.empty() && !legacyNullLiteral(literal))
                routineTypes[node]=ExprHelper::declaredTypeInput(literal->typeName,currentDB,functionEngine);
        } else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(node)) {
            if (evaluator.hasScalarFunction(call, functionEngine)) {
                const std::string type = evaluator.scalarFunctionResultType(call, functionEngine);
                if (!type.empty()) routineTypes[call] = type;
            }
            for (const auto& arg : call->args) inspect(arg.get());
            for (const auto& arg : call->namedArgs) inspect(arg.value.get());
        } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(node)) {
            inspect(unary->operand.get());
        } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node)) {
            inspect(binary->left.get());
            if (binary->op != "::") inspect(binary->right.get());
            else if(const auto* target=dynamic_cast<const LiteralExpr*>(binary->right.get()))
                routineTypes[node]=ExprHelper::declaredTypeInput(target->value,currentDB,functionEngine);
        } else if (const auto* quantified = dynamic_cast<const QuantifiedComparisonExpr*>(node)) {
            inspect(quantified->left.get()); inspect(quantified->right.get());
        } else if (const auto* cast = dynamic_cast<const CastExpr*>(node)) {
            inspect(cast->operand.get());
            routineTypes[node]=ExprHelper::declaredTypeInput(cast->typeName,currentDB,functionEngine);
        } else if (const auto* conditional = dynamic_cast<const CaseExpr*>(node)) {
            inspect(conditional->switchExpr.get());
            for (const auto& arm : conditional->whenClauses) {
                inspect(arm.first.get()); inspect(arm.second.get());
            }
            inspect(conditional->elseExpr.get());
        } else if (const auto* array = dynamic_cast<const ArrayExpr*>(node)) {
            for (const auto& item : array->elements) inspect(item.get());
        } else if (const auto* row = dynamic_cast<const RowExpr*>(node)) {
            for (const auto& item : row->elements) inspect(item.get());
        }
    };
    inspect(expression);
    return routineTypes;
}

bool countParsedColumnReferences(
    const Expr* expression, const std::string& columnName, size_t& count) {
    if (!expression) return true;
    switch (expression->type) {
        case ExprType::Literal:
        case ExprType::Parameter:
        case ExprType::A_Star:
            return true;
        case ExprType::ColumnRef: {
            const auto* column =
                dynamic_cast<const ColumnRefExpr*>(expression);
            if (!column) return false;
            if (column->column == columnName) ++count;
            return true;
        }
        case ExprType::UnaryOp: {
            const auto* unary =
                dynamic_cast<const UnaryOpExpr*>(expression);
            return unary && countParsedColumnReferences(
                unary->operand.get(), columnName, count);
        }
        case ExprType::BinaryOp: {
            const auto* binary =
                dynamic_cast<const BinaryOpExpr*>(expression);
            return binary &&
                countParsedColumnReferences(
                    binary->left.get(), columnName, count) &&
                countParsedColumnReferences(
                    binary->right.get(), columnName, count);
        }
        case ExprType::QuantifiedComparison: {
            const auto* quantified = static_cast<const QuantifiedComparisonExpr*>(expression);
            return countParsedColumnReferences(quantified->left.get(),columnName,count) &&
                countParsedColumnReferences(quantified->right.get(),columnName,count);
        }
        case ExprType::FunctionCall: {
            const auto* function =
                dynamic_cast<const FunctionCallExpr*>(expression);
            if (!function) return false;
            for (const auto& argument : function->args) {
                if (!countParsedColumnReferences(
                        argument.get(), columnName, count)) return false;
            }
            for (const auto& argument : function->namedArgs) {
                if (!countParsedColumnReferences(
                        argument.value.get(), columnName, count)) return false;
            }
            if (!countParsedColumnReferences(
                    function->filter.get(), columnName, count)) return false;
            for (const auto& partition : function->over.partitionBy) {
                if (!countParsedColumnReferences(
                        partition.get(), columnName, count)) return false;
            }
            for (const auto& order : function->over.orderBy) {
                if (!countParsedColumnReferences(
                        order.first.get(), columnName, count)) return false;
            }
            return countParsedColumnReferences(
                       function->over.frameStart.get(), columnName, count) &&
                   countParsedColumnReferences(
                       function->over.frameEnd.get(), columnName, count);
        }
        case ExprType::CastExpr: {
            const auto* cast = dynamic_cast<const CastExpr*>(expression);
            return cast && countParsedColumnReferences(
                cast->operand.get(), columnName, count);
        }
        case ExprType::CaseExpr: {
            const auto* caseExpression =
                dynamic_cast<const CaseExpr*>(expression);
            if (!caseExpression ||
                !countParsedColumnReferences(
                    caseExpression->switchExpr.get(), columnName, count)) {
                return false;
            }
            for (const auto& clause : caseExpression->whenClauses) {
                if (!countParsedColumnReferences(
                        clause.first.get(), columnName, count) ||
                    !countParsedColumnReferences(
                        clause.second.get(), columnName, count)) {
                    return false;
                }
            }
            return countParsedColumnReferences(
                caseExpression->elseExpr.get(), columnName, count);
        }
        case ExprType::ArrayExpr: {
            const auto* array =
                dynamic_cast<const ArrayExpr*>(expression);
            if (!array) return false;
            for (const auto& element : array->elements) {
                if (!countParsedColumnReferences(
                        element.get(), columnName, count)) return false;
            }
            return true;
        }
        case ExprType::RowExpr: {
            const auto* row = dynamic_cast<const RowExpr*>(expression);
            if (!row) return false;
            for (const auto& element : row->elements) {
                if (!countParsedColumnReferences(
                        element.get(), columnName, count)) return false;
            }
            return true;
        }
        case ExprType::Subquery:
            // Stored row expressions must not hide bindings in a subquery.
            return false;
    }
    return false;
}

const Expr* parseStoredExpression(
    const std::string& expression, ParseResult& parsed) {
    SQLParser parser;
    parsed = parser.parse("SELECT " + expression);
    if (!parsed.success || !parsed.stmt) return nullptr;
    const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
    if (!select || select->selectList.size() != 1 ||
        !select->selectList.front().expr) {
        return nullptr;
    }
    return select->selectList.front().expr.get();
}

enum class SourceTokenKind { Identifier, String, Symbol };

struct SourceToken {
    size_t begin = 0;
    size_t end = 0;
    SourceTokenKind kind = SourceTokenKind::Symbol;
    std::string text;
};

bool isExpressionSymbol(char c) {
    switch (c) {
        case '(':
        case ')':
        case ',':
        case ';':
        case '*':
        case '=':
        case '<':
        case '>':
        case '+':
        case '-':
        case '/':
        case '%':
        case '^':
        case '~':
        case '!':
        case '|':
        case '&':
        case '#':
        case '@':
        case '?':
        case ':':
        case '[':
        case ']':
        case '.':
            return true;
        default:
            return false;
    }
}

std::optional<std::vector<SourceToken>> lexExpressionSource(
    const std::string& expression) {
    std::vector<SourceToken> tokens;
    const auto append = [&](size_t begin, size_t end, SourceTokenKind kind) {
        tokens.push_back(
            {begin, end, kind, expression.substr(begin, end - begin)});
    };

    for (size_t index = 0; index < expression.size();) {
        const unsigned char current =
            static_cast<unsigned char>(expression[index]);
        if (std::isspace(current)) {
            ++index;
            continue;
        }
        if (expression.compare(index, 2, "--") == 0) {
            const size_t newline = expression.find('\n', index + 2);
            index = newline == std::string::npos
                ? expression.size() : newline + 1;
            continue;
        }
        if (expression.compare(index, 2, "/*") == 0) {
            const size_t close = expression.find("*/", index + 2);
            if (close == std::string::npos) return std::nullopt;
            index = close + 2;
            continue;
        }

        if (expression[index] == '$') {
            size_t delimiterEnd = index + 1;
            bool validTag = true;
            if (delimiterEnd < expression.size() &&
                std::isdigit(static_cast<unsigned char>(
                    expression[delimiterEnd]))) {
                validTag = false;
            }
            while (validTag && delimiterEnd < expression.size() &&
                   (std::isalnum(static_cast<unsigned char>(
                        expression[delimiterEnd])) ||
                    expression[delimiterEnd] == '_')) {
                ++delimiterEnd;
            }
            if (validTag && delimiterEnd < expression.size() &&
                expression[delimiterEnd] == '$') {
                const std::string delimiter = expression.substr(
                    index, delimiterEnd - index + 1);
                const size_t close = expression.find(
                    delimiter, delimiterEnd + 1);
                if (close == std::string::npos) return std::nullopt;
                const size_t end = close + delimiter.size();
                append(index, end, SourceTokenKind::String);
                index = end;
                continue;
            }
        }

        if (expression[index] == '\'') {
            const size_t begin = index++;
            bool closed = false;
            while (index < expression.size()) {
                if (expression[index] != '\'') {
                    ++index;
                    continue;
                }
                if (index + 1 < expression.size() &&
                    expression[index + 1] == '\'') {
                    index += 2;
                    continue;
                }
                size_t backslashCount = 0;
                for (size_t cursor = index;
                     cursor > begin + 1 && expression[cursor - 1] == '\\';
                     --cursor) {
                    ++backslashCount;
                }
                if (backslashCount % 2 != 0) {
                    ++index;
                    continue;
                }
                ++index;
                closed = true;
                break;
            }
            if (!closed) return std::nullopt;
            append(begin, index, SourceTokenKind::String);
            continue;
        }

        if (expression[index] == '"') {
            const size_t begin = index++;
            bool closed = false;
            while (index < expression.size()) {
                if (expression[index] != '"') {
                    ++index;
                    continue;
                }
                if (index + 1 < expression.size() &&
                    expression[index + 1] == '"') {
                    index += 2;
                    continue;
                }
                ++index;
                closed = true;
                break;
            }
            if (!closed) return std::nullopt;
            append(begin, index, SourceTokenKind::Identifier);
            continue;
        }

        if (isExpressionSymbol(expression[index])) {
            const size_t begin = index;
            size_t length = 1;
            if (index + 1 < expression.size()) {
                const std::string two = expression.substr(index, 2);
                if (two == "<=" || two == ">=" || two == "<>" ||
                    two == "!=" || two == "::" || two == "||" ||
                    two == "->" || two == "~*" || two == "!~" ||
                    two == "@@" || two == "&&" || two == "<<" ||
                    two == ">>" || two == "=>" || two == "#>" ||
                    two == "@>" || two == "<@") {
                    length = 2;
                }
            }
            if (index + 2 < expression.size()) {
                const std::string three = expression.substr(index, 3);
                if (three == "->>" || three == "#>>" || three == "!~*") {
                    length = 3;
                }
            }
            index += length;
            append(begin, index, SourceTokenKind::Symbol);
            continue;
        }

        const size_t begin = index;
        while (index < expression.size() &&
               !std::isspace(static_cast<unsigned char>(expression[index])) &&
               expression[index] != '\'' && expression[index] != '"' &&
               !isExpressionSymbol(expression[index])) {
            ++index;
        }
        if (begin == index) return std::nullopt;
        append(begin, index, SourceTokenKind::Identifier);
    }
    return tokens;
}

bool tokenIs(const SourceToken& token, const std::string& text) {
    return token.text == text;
}

bool tokenIsKeyword(const SourceToken& token, const std::string& keyword) {
    return token.kind == SourceTokenKind::Identifier &&
           toLower(token.text) == keyword;
}

std::vector<size_t> columnReferenceSourceTokens(
    const std::vector<SourceToken>& tokens, const std::string& columnName) {
    std::vector<size_t> references;
    for (size_t index = 0; index < tokens.size(); ++index) {
        if (tokens[index].kind != SourceTokenKind::Identifier ||
            tokens[index].text != columnName) {
            continue;
        }
        const SourceToken* previous =
            index > 0 ? &tokens[index - 1] : nullptr;
        const SourceToken* next =
            index + 1 < tokens.size() ? &tokens[index + 1] : nullptr;

        // A name before '.' is a schema/table qualifier. A name before '('
        // is a function, and a name before '=>' is a named argument.
        if (next && (tokenIs(*next, ".") || tokenIs(*next, "(") ||
                     tokenIs(*next, "=>"))) {
            continue;
        }
        if (next && tokenIs(*next, "=") && index + 2 < tokens.size() &&
            tokenIs(tokens[index + 2], ">")) {
            continue;
        }

        // These grammar positions name types/collations rather than row
        // values. If a more exotic construct remains ambiguous, the AST
        // reference-count check below rejects the whole rewrite safely.
        if (previous &&
            (tokenIs(*previous, "::") ||
             tokenIsKeyword(*previous, "as") ||
             tokenIsKeyword(*previous, "collate"))) {
            continue;
        }
        references.push_back(index);
    }
    return references;
}

} // namespace

std::string ExprHelper::canonicalResultTypeName(std::string typeName) {
    const size_t first = typeName.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    const size_t last = typeName.find_last_not_of(" \t\r\n");
    typeName = typeName.substr(first, last - first + 1);
    // SQL array dimensions do not define distinct element/array types. Fold
    // the base alias before attaching the array marker: normalizing int4[]
    // as an opaque scalar spelling would disagree with integer[] metadata.
    bool array = false;
    while (typeName.size() >= 2 && typeName.compare(typeName.size() - 2, 2, "[]") == 0) {
        array = true; typeName.resize(typeName.size() - 2);
        const auto baseEnd = typeName.find_last_not_of(" \t\r\n");
        if (baseEnd == std::string::npos) return {};
        typeName.resize(baseEnd + 1);
    }
    if(const auto geometry=geometric_input_detail::builtinType(typeName);!geometry.empty())
        return geometry+(array?"[]":"");
    if (toLower(typeName).rfind("interval ", 0) == 0) {
        const auto declaration = SQLParser::parseTypeSpecification(typeName);
        if (SQLParser::toLower(declaration.typeName) == "interval") return "interval" + std::string(array ? "[]" : "");
    }
    const size_t modifier = typeName.find('(');
    if (modifier != std::string::npos) {
        typeName.resize(modifier);
        while (!typeName.empty() &&
               std::isspace(static_cast<unsigned char>(typeName.back()))) {
            typeName.pop_back();
        }
    }
    std::string canonical =
        TypeRegistry::instance().normalizeTypeName(typeName);
    return (canonical.empty() ? toLower(typeName) : canonical) + (array ? "[]" : "");
}

std::string ExprHelper::inferValuesResultType(const std::string& exprSql) {
    const auto trimCopy = [](const std::string& input) {
        const size_t first = input.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) return std::string{};
        const size_t last = input.find_last_not_of(" \t\r\n");
        return input.substr(first, last - first + 1);
    };
    const std::string value = trimCopy(exprSql);
    std::string lower = toLower(value);
    if (lower == "null") return "unknown";
    size_t quote = 0;
    bool escapeString = false;
    if (value.size() >= 2 && (value[0] == 'e' || value[0] == 'E') &&
        value[1] == '\'') {
        escapeString = true;
        quote = 1;
    }
    bool stringLiteral = quote < value.size() && value[quote] == '\'';
    if (stringLiteral) {
        ++quote;
        bool closed = false;
        while (quote < value.size()) {
            if (escapeString && value[quote] == '\\') {
                quote += std::min<size_t>(2, value.size() - quote);
                continue;
            }
            if (value[quote] != '\'') { ++quote; continue; }
            if (quote + 1 < value.size() && value[quote + 1] == '\'') {
                quote += 2;
                continue;
            }
            closed = ++quote == value.size();
            break;
        }
        if (closed) return "unknown";
    }
    if (lower == "current_user" || lower == "session_user" ||
        lower == "user") return "name";
    if (lower == "current_date") return "date";
    if (lower == "current_timestamp") return "timestamptz";
    if (lower == "localtimestamp") return "timestamp";

    size_t index = 0;
    bool negative = false;
    if (index < value.size() &&
        (value[index] == '+' || value[index] == '-')) {
        negative = value[index] == '-';
        ++index;
    }
    const size_t digitStart = index;
    while (index < value.size() &&
           std::isdigit(static_cast<unsigned char>(value[index]))) ++index;
    if (digitStart != index && index == value.size()) {
        size_t significantStart = digitStart;
        while (significantStart < value.size() &&
               value[significantStart] == '0') ++significantStart;
        const std::string magnitude = significantStart == value.size()
            ? "0" : value.substr(significantStart);
        const auto within = [&](const char* bound) {
            const size_t size = std::strlen(bound);
            return magnitude.size() < size ||
                   (magnitude.size() == size && magnitude <= bound);
        };
        if (within(negative ? "2147483648" : "2147483647"))
            return "integer";
        if (within(negative ? "9223372036854775808" :
                              "9223372036854775807")) return "bigint";
        return "numeric";
    }
    return canonicalResultTypeName(inferResultType(exprSql));
}

bool ExprHelper::resolveValuesResultType(
    const std::vector<std::string>& inputTypes,
    std::string& resultType, std::string& error) {
    std::vector<std::string> known;
    for (const auto& input : inputTypes) {
        if (!input.empty() && input != "unknown")
            known.push_back(canonicalResultTypeName(input));
    }
    if (known.empty()) {
        resultType = "text";
        return true;
    }
    resultType = known.front();
    const auto numericRank = [](const std::string& type) {
        if (type == "smallint") return 0;
        if (type == "integer") return 1;
        if (type == "bigint") return 2;
        if (type == "numeric") return 3;
        if (type == "real") return 4;
        if (type == "double precision") return 5;
        return -1;
    };
    for (size_t i = 1; i < known.size(); ++i) {
        const std::string& next = known[i];
        if (next == resultType) continue;
        const auto* currentEntry =
            TypeRegistry::instance().findType(resultType);
        const auto* nextEntry = TypeRegistry::instance().findType(next);
        if (!currentEntry || !nextEntry ||
            currentEntry->category != nextEntry->category) {
            error = "VALUES types " + resultType + " and " + next +
                    " cannot be matched";
            return false;
        }
        if (currentEntry->category == TypeCategory::Numeric) {
            const int currentRank = numericRank(resultType);
            const int nextRank = numericRank(next);
            if (currentRank < 0 || nextRank < 0) {
                error = "VALUES types " + resultType + " and " + next +
                        " cannot be matched";
                return false;
            }
            if (nextRank > currentRank) resultType = next;
            continue;
        }
        if (currentEntry->category == TypeCategory::String) continue;
        if (currentEntry->category == TypeCategory::DateTime) {
            const bool currentTimestamp = resultType == "date" ||
                resultType == "timestamp" || resultType == "timestamptz";
            const bool nextTimestamp = next == "date" ||
                next == "timestamp" || next == "timestamptz";
            if (currentTimestamp && nextTimestamp) {
                if (resultType == "timestamptz" || next == "timestamptz")
                    resultType = "timestamptz";
                else if (resultType == "timestamp" || next == "timestamp")
                    resultType = "timestamp";
                continue;
            }
            const bool currentTime =
                resultType == "time" || resultType == "timetz";
            const bool nextTime = next == "time" || next == "timetz";
            if (currentTime && nextTime) {
                if (next == "timetz") resultType = "timetz";
                continue;
            }
        }
        error = "VALUES types " + resultType + " and " + next +
                " cannot be matched";
        return false;
    }
    return true;
}

std::string ExprHelper::analyzeExplicitResultCollation(
    const std::string& exprSql) {
    SQLParser parser;
    ParseResult parsed = parser.parse("SELECT " + exprSql);
    if (!parsed.success || !parsed.stmt)
        throw DbError("42601", parsed.error.empty()
            ? "invalid ORDER BY expression" : parsed.error);
    const auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get());
    if (!select || select->selectList.empty() ||
        !select->selectList[0].expr)
        throw DbError("42601", "invalid ORDER BY expression");
    return ExprEvaluator::analyzeExplicitResultCollation(
        select->selectList[0].expr.get());
}

std::string ExprHelper::scalarExpressionIdentity(
    const std::string& exprSql,
    const std::function<std::string(const ColumnRefExpr&)>& columnIdentity,
    const std::string& currentDB,
    StorageEngine* functionEngine) {
    SQLParser parser;
    auto parsed = parser.parse("SELECT " + exprSql);
    const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
    if (!select || select->selectList.size() != 1)
        throw DbError("42601", "invalid scalar expression identity");
    return scalarExpressionIdentity(select->selectList.front().expr.get(),
        columnIdentity, currentDB, functionEngine);
}

std::string ExprHelper::scalarExpressionIdentity(
    const Expr* expression,
    const std::function<std::string(const ColumnRefExpr&)>& columnIdentity,
    const std::string& currentDB,
    StorageEngine* functionEngine,
    const std::function<std::optional<std::string>(const Expr*)>& preparedIdentity) {
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    const auto field = [](const std::string& value) {
        return std::to_string(value.size()) + ":" + value;
    };
    std::function<std::string(const Expr*)> key = [&](const Expr* node) -> std::string {
        if (!node) return "absent";
        if (preparedIdentity) {
            if (const auto prepared = preparedIdentity(node)) return *prepared;
        }
        if (node->preparedSubquery)
            return "prepared-child-site" + field(std::to_string(reinterpret_cast<uintptr_t>(node)));
        if (const auto* parameter = dynamic_cast<const ParameterExpr*>(node)) {
            if (parameter->declaredType.empty()) throw DbError("0A000", "parameter identity requires a prepared typed slot");
            return "parameter" + field(std::to_string(parameter->slot)) + field(parameter->declaredType) +
                field(std::to_string(static_cast<unsigned>(parameter->origin)));
        }
        if (const auto* column = dynamic_cast<const ColumnRefExpr*>(node))
            return "column" + field(columnIdentity(*column));
        if (const auto* literal = dynamic_cast<const LiteralExpr*>(node)) {
            std::string value = literal->value;
            const std::string type = inferAstResultType(literal, {});
            const bool sqlNull = toLower(value) == "null";
            // Parsing an integer token is not expression evaluation. The
            // canonical AST datum must not depend on spelling 1 versus 01.
            if (type == "integer" || type == "bigint" || type == "smallint") {
                try {
                    size_t consumed = 0;
                    const auto number = std::stoll(value, &consumed);
                    if (consumed == value.size()) value = std::to_string(number);
                } catch (...) {}
            } else if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
                std::string decoded;
                for (size_t i = 1; i + 1 < value.size(); ++i) {
                    decoded += value[i];
                    if (value[i] == '\'' && i + 2 < value.size() && value[i + 1] == '\'') ++i;
                }
                value = std::move(decoded);
            }
            return "literal" + field(sqlNull ? "sql-null" : "non-null") + field(type) + field(value);
        }
        if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(node)) {
            const std::string lower = toLower(unary->op);
            // Grammar labels are not values or case-folded identifiers.
            // Quoted collation names and time-zone literals keep their bytes.
            const auto labelStart = lower.rfind("collate ", 0) == 0 ? 8u :
                lower.rfind("at time zone ", 0) == 0 ? 13u : 0u;
            const std::string op = labelStart
                ? lower.substr(0, labelStart) + unary->op.substr(labelStart) : lower;
            return "unary" + field(op) + field(key(unary->operand.get()));
        }
        if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node)) {
            if (binary->op == "::") {
                const std::string rawType = binary->right->toString();
                std::string type = canonicalResultTypeName(rawType);
                const auto modifierStart = rawType.find('(');
                if (modifierStart != std::string::npos) {
                    const auto modifierEnd = rawType.find(')', modifierStart);
                    std::string modifier;
                    for (const auto& token : SQLParser::tokenize(rawType.substr(
                            modifierStart + 1, modifierEnd - modifierStart - 1))) {
                        if (token == ",") { type += field(modifier); modifier.clear(); }
                        else modifier += token;
                    }
                    type += field(modifier);
                }
                type += field("explicit");
                return "cast" + field(type) + field(key(binary->left.get()));
            }
            if (toLower(binary->op) == "collate")
                return "collate" + field(binary->right->toString()) + field(key(binary->left.get()));
            return "binary" + field(toLower(binary->op)) + field(binary->comparison?binary->comparison->identity:"") + field(key(binary->left.get())) +
                field(key(binary->right.get()));
        }
        if (const auto* quantified = dynamic_cast<const QuantifiedComparisonExpr*>(node)) {
            return "quantified" + field(quantified->op) +
                field(quantified->quantifier==QuantifiedComparisonExpr::Quantifier::All?"all":"any") +
                field(quantified->comparison?quantified->comparison->identity:std::string("unprepared")) +
                field(key(quantified->left.get())) + field(key(quantified->right.get()));
        }
        if (const auto* cast = dynamic_cast<const CastExpr*>(node)) {
            std::string type = canonicalResultTypeName(cast->typeName);
            for (const auto& modifier : cast->typeMods) type += field(modifier);
            type += field(cast->implicit ? "implicit" : "explicit");
            return "cast" + field(type) + field(key(cast->operand.get()));
        }
        if (const auto* conditional = dynamic_cast<const CaseExpr*>(node)) {
            std::string result = "case" + field(key(conditional->switchExpr.get()));
            for (const auto& types : conditional->simpleComparisonTypes)
                result += field(types.first) + field(types.second);
            for(const auto& binding:conditional->simpleEnumComparisons)
                result+=field(binding?binding->identity:"");
            for (const auto& arm : conditional->whenClauses)
                result += field(key(arm.first.get())) + field(key(arm.second.get()));
            return result + field(key(conditional->elseExpr.get()));
        }
        if (const auto* function = dynamic_cast<const FunctionCallExpr*>(node)) {
            if (function->hasOver || function->filter || function->distinct || !function->orderBy.empty())
                throw DbError("0A000", "aggregate/window identity requires a prepared query");
            const std::string routine = function->setReturning?function->setReturning->identity:
                evaluator.scalarFunctionIdentity(function, functionEngine);
            std::string result = "function" + field(routine);
            for (size_t i = 0; i < function->args.size(); ++i) {
                const auto* fieldName = dynamic_cast<const ColumnRefExpr*>(function->args[i].get());
                if (i == 0 && function->schema.empty() && routine.rfind("builtin", 0) == 0 &&
                    toLower(function->funcName) == "extract" && fieldName &&
                    fieldName->schema.empty() && fieldName->table.empty())
                    result += field("extract-field" + field(toLower(fieldName->column)));
                else result += field(key(function->args[i].get()));
            }
            for (const auto& argument : function->namedArgs)
                result += field(argument.name) + field(key(argument.value.get()));
            return result;
        }
        if (const auto* array = dynamic_cast<const ArrayExpr*>(node)) {
            std::string result = "array" + field(canonicalResultTypeName(array->elementType)) +
                field(array->nestedElements ? "nested" : "scalar");
            for (const auto& value : array->elements) result += field(key(value.get()));
            return result;
        }
        if (const auto* row = dynamic_cast<const RowExpr*>(node)) {
            std::string result = row->constructor ? "row-constructor" : "in-list";
            for (const auto& type : row->fieldTypes) result += field(type);
            for (const auto& value : row->elements) result += field(key(value.get()));
            return result;
        }
        throw DbError("0A000", "expression identity requires a prepared query scope");
    };
    return key(expression);
}

std::string ExprHelper::preparedSortExpressionIdentity(
    const Expr* expression, const PreparedQuery& query,
    const std::function<std::string(const ColumnRefExpr&)>& columnIdentity,
    const std::string& currentDB, StorageEngine* functionEngine) {
    const auto field = [](const std::string& value) {
        return std::to_string(value.size()) + ":" + value;
    };
    const auto site = [&](const Expr* node) {
        return "prepared-child-site" + field(std::to_string(reinterpret_cast<uintptr_t>(node)));
    };
    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    std::function<std::string(const Expr*)> childKey;
    childKey = [&](const Expr* child) -> std::string {
        if (!child || !child->preparedSubquery) throw DbError("XX000", "missing prepared SQL child");
        // Normalize only ranges *inside* this retained child. True caller
        // ranges and parameter slots stay in the parent's prepared namespace.
        std::map<const Stmt*, size_t> statements;
        std::map<const ColumnRefExpr*, std::string> outputAliases;
        std::function<void(const Stmt*)> gatherStatement;
        std::function<void(const Expr*)> gatherExpression;
        std::function<void(const FromItem*)> gatherFrom;
        gatherExpression = [&](const Expr* node) {
            if (!node) return;
            if (node->preparedSubquery) { gatherStatement(node->preparedSubquery.get()); return; }
            if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(node)) gatherExpression(unary->operand.get());
            else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(node)) {
                gatherExpression(binary->left.get()); gatherExpression(binary->right.get());
            } else if (const auto* quantified = dynamic_cast<const QuantifiedComparisonExpr*>(node)) {
                gatherExpression(quantified->left.get()); gatherExpression(quantified->right.get());
            } else if (const auto* cast = dynamic_cast<const CastExpr*>(node)) gatherExpression(cast->operand.get());
            else if (const auto* conditional = dynamic_cast<const CaseExpr*>(node)) {
                gatherExpression(conditional->switchExpr.get()); gatherExpression(conditional->elseExpr.get());
                for (const auto& arm : conditional->whenClauses) { gatherExpression(arm.first.get()); gatherExpression(arm.second.get()); }
            } else if (const auto* call = dynamic_cast<const FunctionCallExpr*>(node)) {
                for (const auto& argument : call->args) gatherExpression(argument.get());
                for (const auto& argument : call->namedArgs) gatherExpression(argument.value.get());
                gatherExpression(call->filter.get());
                for (const auto& value : call->over.partitionBy) gatherExpression(value.get());
                for (const auto& value : call->over.orderBy) gatherExpression(value.first.get());
                gatherExpression(call->over.frameStart.get()); gatherExpression(call->over.frameEnd.get());
            } else if (const auto* array = dynamic_cast<const ArrayExpr*>(node)) {
                for (const auto& value : array->elements) gatherExpression(value.get());
            } else if (const auto* row = dynamic_cast<const RowExpr*>(node)) {
                for (const auto& value : row->elements) gatherExpression(value.get());
            }
        };
        gatherFrom = [&](const FromItem* from) {
            if (!from) return;
            gatherStatement(from->subquery.get()); gatherFrom(from->left.get()); gatherFrom(from->right.get());
            gatherExpression(from->joinCondition.get());
        };
        const auto gatherItems = [&](const std::vector<SelectItem>& items) {
            for (const auto& item : items) gatherExpression(item.expr.get());
        };
        gatherStatement = [&](const Stmt* statement) {
            if (!statement || !statements.emplace(statement, statements.size()).second) return;
            if (const auto* select = dynamic_cast<const SelectStmt*>(statement)) {
                for (const auto& cte : select->ctes) gatherStatement(cte.query.get());
                gatherStatement(select->setOpLhs.get()); gatherStatement(select->setOpRhs.get());
                gatherFrom(select->fromClause.get()); gatherItems(select->selectList);
                gatherExpression(select->whereClause.get()); gatherExpression(select->having.get());
                for (const auto& value : select->groupBy) gatherExpression(value.get());
                for (const auto& group : select->groupByElems) for (const auto& value : group.exprs) gatherExpression(value.get());
                for (const auto& order : select->orderBy) {
                    gatherExpression(order.expr.get());
                    const auto* alias = dynamic_cast<const ColumnRefExpr*>(order.expr.get());
                    const auto output = query.statementOutputs.find(statement);
                    if (!alias || alias->binding || !alias->schema.empty() || !alias->table.empty() || output == query.statementOutputs.end()) continue;
                    size_t matches = 0, ordinal = 0;
                    for (size_t i = 0; i < output->second.size(); ++i)
                        if (output->second[i].name == alias->column) { ++matches; ordinal = i; }
                    if (matches == 1) outputAliases.emplace(alias, field(alias->column) + field(std::to_string(ordinal)) + field(output->second[ordinal].type));
                }
                for (const auto& value : select->distinctOn) gatherExpression(value.get());
                for (const auto& row : select->valuesRows) for (const auto& value : row) gatherExpression(value.get());
                for (const auto& window : select->windowDefs) {
                    for (const auto& value : window.partitionBy) gatherExpression(value.get());
                    for (const auto& order : window.orderBy) gatherExpression(order.first.get());
                    gatherExpression(window.frameStart.get()); gatherExpression(window.frameEnd.get());
                }
            } else if (const auto* with = dynamic_cast<const WithStmt*>(statement)) {
                for (const auto& cte : with->ctes) gatherStatement(cte.query.get());
                gatherStatement(with->statement.get());
            } else if (const auto* insert = dynamic_cast<const InsertStmt*>(statement)) {
                gatherStatement(insert->selectSource.get()); gatherItems(insert->returning);
                for (const auto& row : insert->values) for (const auto& value : row) gatherExpression(value.get());
                for (const auto& set : insert->conflictUpdateSet) gatherExpression(set.second.get());
                gatherExpression(insert->conflictWhere.get());
            } else if (const auto* update = dynamic_cast<const UpdateStmt*>(statement)) {
                gatherFrom(update->fromClause.get()); gatherItems(update->returning); gatherExpression(update->whereClause.get());
                for (const auto& set : update->setClauses) gatherExpression(set.second.get());
            } else if (const auto* remove = dynamic_cast<const DeleteStmt*>(statement)) {
                gatherFrom(remove->usingClause.get()); gatherItems(remove->returning); gatherExpression(remove->whereClause.get());
            } else throw DbError("0A000", "SQL child identity requires a structured query statement");
        };
        gatherStatement(child->preparedSubquery.get());
        std::map<size_t, size_t> ranges;
        for (const auto& range : query.sourceRanges)
            if (statements.count(range.owner)) ranges.emplace(range.ordinal, ranges.size());
        const auto descriptorKey = [&](const QueryRowDescriptor& descriptor) {
            std::string key;
            for (const auto& column : descriptor)
                key += field(column.name) + field(canonicalResultTypeName(column.type)) +
                    field(column.generated ? "generated" : "ordinary") + field(std::to_string(column.identity));
            return key;
        };
        std::function<std::string(const Stmt*)> statementKey;
        std::function<std::string(const Expr*)> expressionKey;
        std::function<std::string(const FromItem*)> fromKey;
        const auto boundColumn = [&](const ColumnRefExpr& column) {
            if (!column.binding) {
                const auto alias = outputAliases.find(&column);
                if (alias != outputAliases.end()) return "output-alias" + field(alias->second);
                throw DbError("0A000", "SQL child identity requires bound column provenance");
            }
            const auto& binding = *column.binding;
            const auto local = ranges.find(binding.sourceOrdinal);
            return field(local == ranges.end() ? "caller-range" : "child-range") +
                field(std::to_string(local == ranges.end() ? binding.sourceOrdinal : local->second)) +
                field(std::to_string(binding.scopeDepth)) + field(std::to_string(binding.columnOrdinal)) +
                field(canonicalResultTypeName(binding.declaredType)) + field(binding.mergedUsing ? "merged" : "ordinary");
        };
        const auto windowKey = [&](const WindowDef& window) {
            std::string key = field(window.name) + field(toLower(window.frameMode)) + field(toLower(window.frameExclusion));
            for (const auto& value : window.partitionBy) key += field(expressionKey(value.get()));
            key += field("order");
            for (const auto& order : window.orderBy) key += field(expressionKey(order.first.get())) + field(order.second ? "asc" : "desc");
            return key + field(expressionKey(window.frameStart.get())) + field(expressionKey(window.frameEnd.get()));
        };
        const auto itemsKey = [&](const std::vector<SelectItem>& items) {
            std::string key;
            for (const auto& item : items) key += field(item.alias) + field(expressionKey(item.expr.get()));
            return key;
        };
        expressionKey = [&](const Expr* node) {
            return scalarExpressionIdentity(node, boundColumn, currentDB, functionEngine,
                [&](const Expr* nested) -> std::optional<std::string> {
                    if (nested->preparedSubquery) return statementKey(nested->preparedSubquery.get());
                    const auto* call = dynamic_cast<const FunctionCallExpr*>(nested);
                    if (!call) return {};
                    if (!call->orderBy.empty()) throw DbError("0A000", "aggregate ORDER identity requires structured order nodes");
                    CatalogManager::QualifiedName name;
                    const auto spelling = call->schema.empty() ? call->funcName : call->schema + "." + call->funcName;
                    if (!CatalogManager::parseQualifiedName(spelling, name, true)) throw DbError("0A000", "function identity requires a canonical name");
                    std::string routine;
                    if (evaluator.hasScalarFunction(call, functionEngine)) routine = evaluator.scalarFunctionIdentity(call, functionEngine);
                    else if (!aggregate_type_detail::builtinName(call).empty())
                        routine = "builtin-aggregate" + field(name.name);
                    else throw DbError("0A000", "SQL child identity requires resolved function metadata");
                    std::string key = "function" + field(routine) + field(call->distinct ? "distinct" : "all") +
                        field(expressionKey(call->filter.get())) + field(call->hasOver ? windowKey(call->over) : "no-window");
                    for (size_t i = 0; i < call->args.size(); ++i) {
                        const auto* grammarField = dynamic_cast<const ColumnRefExpr*>(call->args[i].get());
                        if (i == 0 && name.schema.empty() && name.name == "extract" && routine.rfind("builtin", 0) == 0 && grammarField &&
                            grammarField->schema.empty() && grammarField->table.empty()) key += field("extract-field" + field(toLower(grammarField->column)));
                        else if (call->args[i]->type == ExprType::A_Star) key += field("star");
                        else key += field(expressionKey(call->args[i].get()));
                    }
                    for (const auto& argument : call->namedArgs) key += field(argument.name) + field(expressionKey(argument.value.get()));
                    return key;
                });
        };
        fromKey = [&](const FromItem* from) -> std::string {
            if (!from) return "no-source";
            if (from->type == FromItem::Type::Function) throw DbError("0A000", "SQL child function source identity requires a structured value AST");
            std::string key = field(std::to_string(static_cast<int>(from->type))) + field(from->alias) +
                field(toLower(from->joinType)) + field(fromKey(from->left.get())) + field(fromKey(from->right.get())) +
                field(expressionKey(from->joinCondition.get())) + field(statementKey(from->subquery.get()));
            for (const auto& column : from->usingCols) key += field(column);
            for (const auto& column : from->columnAliases) key += field(column);
            return key;
        };
        const auto ctesKey = [&](const std::vector<SelectStmt::CTE>& ctes) {
            std::string key;
            for (const auto& cte : ctes) {
                key += field(cte.name) + field(cte.recursive ? "recursive" : "ordinary") + field(cte.materialized ? "materialized" : "inline");
                key += field(cte.materializationSpecified ? "explicit" : "default");
                for (const auto& column : cte.columnNames) key += field(column);
                key += field(statementKey(cte.query.get()));
            }
            return key;
        };
        const auto returningKey = [&](const ReturningOptions& returning) {
            return field(returning.oldAliased ? returning.oldAlias : "no-old-alias") + field(returning.newAliased ? returning.newAlias : "no-new-alias");
        };
        statementKey = [&](const Stmt* statement) -> std::string {
            if (!statement) return "no-statement";
            std::string key = field(std::to_string(static_cast<int>(statement->command)));
            const auto output = query.statementOutputs.find(statement);
            if (output != query.statementOutputs.end()) key += field(descriptorKey(output->second));
            for (const auto& range : query.sourceRanges) {
                if (range.owner != statement) continue;
                key += field(std::to_string(ranges.at(range.ordinal))) + field(range.schema) + field(range.name) +
                    field(range.relationSchema) + field(range.relationName) + field(descriptorKey(range.columns)) +
                    field(range.mergedUsing ? "merged" : "ordinary");
                if (range.cteStatement) {
                    const auto local = statements.find(range.cteStatement);
                    key += field(local == statements.end() ? "caller-cte" : "child-cte") +
                        field(std::to_string(local == statements.end() ? reinterpret_cast<uintptr_t>(range.cteStatement) : local->second));
                } else key += field("no-cte");
            }
            if (const auto* select = dynamic_cast<const SelectStmt*>(statement)) {
                key += field(ctesKey(select->ctes)) + field(statementKey(select->setOpLhs.get())) + field(statementKey(select->setOpRhs.get())) +
                    field(std::to_string(static_cast<int>(select->setOp))) + field(select->setOpAll ? "all" : "distinct") +
                    field(fromKey(select->fromClause.get())) + field(itemsKey(select->selectList)) + field(expressionKey(select->whereClause.get())) +
                    field(expressionKey(select->having.get())) + field(select->distinct ? "distinct" : "all");
                for (const auto& value : select->distinctOn) key += field(expressionKey(value.get()));
                key += field("group");
                for (const auto& value : select->groupBy) key += field(expressionKey(value.get()));
                for (const auto& group : select->groupByElems) {
                    key += field(std::to_string(static_cast<int>(group.kind)));
                    for (const auto& value : group.exprs) key += field(expressionKey(value.get()));
                }
                key += field("order");
                for (const auto& order : select->orderBy) key += field(expressionKey(order.expr.get())) + field(order.asc ? "asc" : "desc") +
                    field(order.nullsFirst ? "nulls-first" : "nulls-last") + field(order.usingOp);
                key += field(select->limit ? std::to_string(*select->limit) : "no-limit") + field(select->offset ? std::to_string(*select->offset) : "no-offset") +
                    field(select->withTies ? "ties" : "no-ties") + field(select->fetchFirst ? "fetch" : "no-fetch") +
                    field(select->signedFetchCount ? std::to_string(*select->signedFetchCount) : "no-signed-fetch");
                for (const auto& row : select->valuesRows) { std::string values; for (const auto& value : row) values += field(expressionKey(value.get())); key += field(values); }
                for (const auto& window : select->windowDefs) key += field(windowKey(window));
                for (const auto& lock : select->locking) {
                    key += field(lock.strength) + field(lock.noWait ? "nowait" : "wait") + field(lock.skipLocked ? "skip" : "no-skip");
                    for (const auto& table : lock.tables) key += field(table);
                }
            } else if (const auto* with = dynamic_cast<const WithStmt*>(statement)) {
                key += field(ctesKey(with->ctes)) + field(statementKey(with->statement.get()));
            } else if (const auto* insert = dynamic_cast<const InsertStmt*>(statement)) {
                for (const auto& column : insert->columns) key += field(column);
                for (const auto& row : insert->values) { std::string values; for (const auto& value : row) values += field(expressionKey(value.get())); key += field(values); }
                key += field(statementKey(insert->selectSource.get())) + field(insert->conflictAction) + field(insert->conflictConstraint) +
                    field(expressionKey(insert->conflictWhere.get())) + field(insert->defaultValues ? "defaults" : "values") + field(insert->override_) +
                    field(itemsKey(insert->returning)) + field(returningKey(insert->returningOptions));
                for (const auto& column : insert->conflictTarget) key += field(column);
                for (const auto& set : insert->conflictUpdateSet) key += field(set.first) + field(expressionKey(set.second.get()));
            } else if (const auto* update = dynamic_cast<const UpdateStmt*>(statement)) {
                if (!update->whereCurrentOf.empty()) throw DbError("0A000", "cursor identity requires prepared cursor provenance");
                key += field(update->alias) + field(update->only ? "only" : "inherited") + field(fromKey(update->fromClause.get())) +
                    field(expressionKey(update->whereClause.get())) + field(itemsKey(update->returning)) + field(returningKey(update->returningOptions));
                for (const auto& set : update->setClauses) key += field(set.first) + field(expressionKey(set.second.get()));
            } else if (const auto* remove = dynamic_cast<const DeleteStmt*>(statement)) {
                if (!remove->whereCurrentOf.empty()) throw DbError("0A000", "cursor identity requires prepared cursor provenance");
                key += field(remove->alias) + field(remove->only ? "only" : "inherited") + field(fromKey(remove->usingClause.get())) +
                    field(expressionKey(remove->whereClause.get())) + field(itemsKey(remove->returning)) + field(returningKey(remove->returningOptions));
            } else throw DbError("0A000", "SQL child identity requires a structured query statement");
            return key;
        };
        return "prepared-child" + field(child->type == ExprType::Subquery ? "subquery" : "retained-child") +
            field(statementKey(child->preparedSubquery.get()));
    };
    return scalarExpressionIdentity(expression, columnIdentity, currentDB, functionEngine,
        [&](const Expr* node) -> std::optional<std::string> {
            if (!node->preparedSubquery) {
                const auto* call = dynamic_cast<const FunctionCallExpr*>(node);
                const auto name = aggregate_type_detail::builtinName(call);
                ExprEvaluator evaluator; evaluator.setCurrentDB(currentDB);
                if (name.empty() || evaluator.hasScalarFunction(call, functionEngine)) return {};
                if (!call->orderBy.empty())
                    throw DbError("0A000", "aggregate ORDER identity requires structured order nodes");
                const auto identity = [&](const Expr* value) {
                    return preparedSortExpressionIdentity(value, query, columnIdentity, currentDB, functionEngine);
                };
                std::string result = "builtin-aggregate" + field(name) +
                    field(call->resolvedResultType) + field(call->distinct ? "distinct" : "all");
                for (const auto& argument : call->args) result += field(identity(argument.get()));
                result += field(identity(call->filter.get()));
                return result;
            }
            try { return childKey(node); }
            catch (const DbError& error) {
                // Opaque unsupported grammar stays a distinct slot. This
                // conservative no-sharing path does not reject execution or
                // falsely equate text; genuine preparation errors still pass.
                if (error.sqlState() != "0A000") throw;
                return site(node);
            }
        });
}

std::string ExprHelper::inferParsedResultType(
    const Expr* expression,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    StorageEngine* functionEngine) {
    const std::string type = inferParsedInputType(expression, typeHints, currentDB, functionEngine);
    return type.empty() || type == "unknown" ? "text" : type;
}

std::string ExprHelper::declaredTypeInput(const std::string& spelling,
    const std::string& currentDB,StorageEngine* owner) {
    const auto* session=currentSession();
    const auto database=currentDB.empty() && session?session->currentDB:currentDB;
    if(database.empty())return resolveDeclaredTypeName(spelling,nullptr,session).inputType;
    auto* engine=owner?owner:&g_engine;
    const auto catalog=engine->catalogService().metadataSnapshot(database);
    // Standalone consumers may carry only a nominal database label. A cold,
    // absent catalog cannot supply user types, but the actual registered
    // builtin input codecs remain available without bootstrapping or writes.
    if(catalog.namespaces.empty() && catalog.types.empty())
        return resolveDeclaredTypeName(spelling,nullptr,session).inputType;
    return resolveDeclaredTypeName(spelling,&catalog,session).inputType;
}

std::pair<std::string,int> ExprHelper::projectionLabel(const Expr* expression,
    const std::function<std::string(const std::string&)>& declaredType) {
    const auto typeLabel=[&](const std::string& spelling) {
        // Naming uses the untransformed declaration, not the eventual input
        // codec (a named array/domain can have a different visible name).
        if(declaredType)(void)declaredType(spelling);
        auto type=SQLParser::parseTypeSpecification(spelling).typeName;
        // SQL grammar aliases name their actual catalog types. Quoted or
        // qualified names keep their own final identifier instead.
        static const std::map<std::string,std::string> catalogNames={
            {"boolean","bool"},{"smallint","int2"},{"integer","int4"},{"int","int4"},{"bigint","int8"},
            {"real","float4"},{"double precision","float8"},{"character","bpchar"},{"char","bpchar"},
            {"character varying","varchar"},{"bit varying","varbit"},{"decimal","numeric"},{"dec","numeric"}};
        if(!type.empty() && type.front()!='\"' && type.find('.')==std::string::npos)
            if(const auto found=catalogNames.find(SQLParser::toLower(type));found!=catalogNames.end())return found->second;
        CatalogManager::QualifiedName name;
        return CatalogManager::parseQualifiedName(type,name,true)?name.name:type;
    };
    if(!expression)return {"",0};
    if(const auto* column=dynamic_cast<const ColumnRefExpr*>(expression))return {column->column,2};
    if(const auto* call=dynamic_cast<const FunctionCallExpr*>(expression)) {
        if(call->schema.empty() && SQLParser::toLower(call->funcName)=="case_when") {
            const auto label=call->args.size()%2?projectionLabel(call->args.back().get(),declaredType):std::make_pair(std::string(),0);
            return label.second==2?label:std::make_pair(std::string("case"),1);
        }
        CatalogManager::QualifiedName name;
        return CatalogManager::parseQualifiedName(call->funcName,name,true)
            ?std::make_pair(name.name,2):std::make_pair(std::string(),0);
    }
    const Expr* operand=nullptr;std::string type;
    if(const auto* cast=dynamic_cast<const CastExpr*>(expression)){operand=cast->operand.get();type=cast->typeName;}
    else if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(expression);binary && binary->op=="::") {
        operand=binary->left.get();
        if(const auto* declaration=dynamic_cast<const LiteralExpr*>(binary->right.get()))type=declaration->value;
    }
    if(!type.empty()) {
        const auto inherited=projectionLabel(operand,declaredType);
        return inherited.second==2?inherited:std::make_pair(typeLabel(type),1);
    }
    if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(expression);
        unary && SQLParser::toLower(unary->op).rfind("collate ",0)==0)
        return projectionLabel(unary->operand.get(),declaredType);
    if(const auto* conditional=dynamic_cast<const CaseExpr*>(expression)) {
        const auto label=projectionLabel(conditional->elseExpr.get(),declaredType);
        return label.second==2?label:std::make_pair(std::string("case"),1);
    }
    if(dynamic_cast<const ArrayExpr*>(expression))return {"array",2};
    if(dynamic_cast<const RowExpr*>(expression))return {"row",2};
    if(const auto* literal=dynamic_cast<const LiteralExpr*>(expression);literal && !literal->typeName.empty())
        return legacyNullLiteral(literal)?std::make_pair(std::string(),0):std::make_pair(typeLabel(literal->typeName),1);
    return {"",0};
}

std::string ExprHelper::inferParsedInputType(
    const Expr* expression,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    StorageEngine* functionEngine) {
    const auto routineTypes = collectRoutineResultTypes(expression, currentDB, functionEngine);
    const std::string type = protocolTypeName(inferAstResultType(expression, typeHints, &routineTypes));
    return type.empty() ? "unknown" : type;
}

std::optional<ArrayConcatBinding> ExprHelper::resolveArrayConcatTypes(
    const std::string& leftRaw, const std::string& rightRaw, bool binaryOperator) {
    auto canonical = [](std::string type) {
        const bool array = type.size() >= 2 && type.compare(type.size()-2,2,"[]") == 0;
        if (array) type.resize(type.size()-2);
        return canonicalResultTypeName(type) + (array ? "[]" : "");
    };
    ArrayConcatBinding result;
    result.leftType = canonical(leftRaw); result.rightType = canonical(rightRaw);
    const auto array = [](const std::string& type) {
        return type.size() >= 2 && type.compare(type.size()-2,2,"[]") == 0;
    };
    result.leftArray = array(result.leftType); result.rightArray = array(result.rightType);
    if (!result.leftArray && !result.rightArray) {
        // The scalar || operator and array_append/prepend share array type
        // promotion, not overload sets. Internal char can use implicit text
        // conversion, but with another text-compatible operand its scalar
        // text/polymorphic || candidates are ambiguous, even for NULLs.
        if (binaryOperator) {
            const auto left=common_type_detail::canonical(result.leftType);
            const auto right=common_type_detail::canonical(result.rightType);
            const auto text=[](const std::string& type){return common_type_detail::implicit(type,"text");};
            if ((left=="\"char\"" && text(right)) || (right=="\"char\"" && text(left)))
                throw DbError("42725","operator is not unique: "+leftRaw+" || "+rightRaw);
        }
        return {};
    }
    // An unknown operand chooses the array-cat overload, not append/prepend.
    if (result.leftType == "unknown") { result.leftType = result.rightType; result.leftArray = true; }
    if (result.rightType == "unknown") { result.rightType = result.leftType; result.rightArray = true; }
    const auto element = [&](const std::string& type) { return array(type) ? type.substr(0,type.size()-2) : type; };
    std::string error;
    if (!resolveValuesResultType({element(result.leftType),element(result.rightType)}, result.elementType, error))
        throw DbError("42883", "operator does not exist: " + leftRaw + " || " + rightRaw);
    result.leftType = result.elementType + (result.leftArray ? "[]" : "");
    result.rightType = result.elementType + (result.rightArray ? "[]" : "");
    return result;
}

void ExprHelper::prepareArrayTypes(Expr* expression,
    const std::map<std::string, std::string>& hints, const std::string& database,
    StorageEngine* owner) {
    const auto routines = collectRoutineResultTypes(expression, database, owner);
    const auto type = [&](const Expr* node) { return inferAstResultType(node,hints,&routines); };
    const auto patternType = [&](std::string operation,const std::vector<const Expr*>& operands) {
        const auto op=toLower(operation);
        const bool like=op=="like" || op=="not like" || op=="like escape" || op=="not like escape";
        bool bytes=false;
        std::vector<std::string> types;
        for (const auto* operand:operands) {
            const auto resolved=canonicalResultTypeName(type(operand));
            bytes=bytes || resolved=="bytea";
            types.push_back(resolved);
        }
        for (const auto& resolved:types) {
            const bool unknown=resolved.empty() || resolved=="unknown";
            const bool text=resolved=="text" || resolved=="character" || resolved=="character varying" ||
                resolved=="name" || resolved=="citext";
            if ((!like && bytes) || (!unknown && (bytes ? resolved!="bytea" : !text)))
                throw DbError("42883","operator does not exist for SQL pattern operand types");
        }
    };
    const auto validateConst = [&](const Expr* node, const std::string& target) {
        const auto* literal = dynamic_cast<const LiteralExpr*>(node);
        if (!literal || literal->preparedSubquery || !literal->typeName.empty()) return;
        const auto tokens = SQLParser::tokenize(literal->value);
        if (tokens.size()!=1 || tokens.front().empty() ||
            (tokens.front().front()!='\'' && toLower(tokens.front())!="null")) return;
        CastExpr conversion; conversion.typeName = target;
        conversion.operand = std::make_unique<LiteralExpr>(*literal);
        ExprEvaluator pure; (void)pure.eval(&conversion,RowContext{});
    };
    std::function<void(Expr*,std::string)> visit = [&](Expr* node,std::string context) {
        if (!node || node->preparedSubquery) return;
        if (auto* quantified = dynamic_cast<QuantifiedComparisonExpr*>(node)) {
            visit(quantified->left.get(),{}); visit(quantified->right.get(),{});
            if (!quantified->right || !quantified->left)
                throw DbError("42601","quantified comparison requires two operands");
            if (!quantified->right->preparedSubquery) {
                const auto arrayType=type(quantified->right.get());
                if (!array_detail::isArray(arrayType))
                    throw DbError("42809","op ANY/ALL (array) requires array on right side");
                quantified->comparison=ExprEvaluator::resolveComparison(
                    quantified->op,type(quantified->left.get()),array_detail::elementType(arrayType));
                validateConst(quantified->left.get(),quantified->comparison->leftType);
            }
            return;
        }
        if (auto* cast = dynamic_cast<CastExpr*>(node)) { visit(cast->operand.get(),cast->typeName); return; }
        if (auto* binary = dynamic_cast<BinaryOpExpr*>(node)) {
            if (binary->op=="::") {
                const auto* target = dynamic_cast<const LiteralExpr*>(binary->right.get());
                visit(binary->left.get(),target ? target->value : std::string()); return;
            }
            visit(binary->left.get(),{}); visit(binary->right.get(),{});
            const auto operation=toLower(binary->op);
            const auto leftType=canonicalResultTypeName(type(binary->left.get()));
            const auto rightType=canonicalResultTypeName(type(binary->right.get()));
            const bool bitOperand=leftType=="bit" || leftType=="bit varying" ||
                                  rightType=="bit" || rightType=="bit varying";
            static const std::set<std::string> comparisons={"=","<>","!=","<",">","<=",">="};
            if (binary->rowComparison) return; // actual binder-owned signatures win
            auto* lhsRow = dynamic_cast<RowExpr*>(binary->left.get());
            auto* rhsRow = dynamic_cast<RowExpr*>(binary->right.get());
            if (lhsRow && lhsRow->constructor && rhsRow) {
                const auto pair = [&](RowExpr& peer,const std::string& op) {
                    if (lhsRow->elements.size() != peer.elements.size())
                        throw DbError("42601","unequal number of entries in row expressions");
                    if (lhsRow->elements.empty())
                        throw DbError("0A000","cannot compare rows of zero length");
                    std::vector<QueryComparisonBinding> bindings;
                    for (size_t i = 0; i < lhsRow->elements.size(); ++i) {
                        const auto left = canonicalResultTypeName(type(lhsRow->elements[i].get()));
                        const auto right = canonicalResultTypeName(type(peer.elements[i].get()));
                        const auto binding = ExprEvaluator::resolveComparison(op,left,right);
                        if (left == "unknown") validateConst(lhsRow->elements[i].get(),binding.leftType);
                        if (right == "unknown") validateConst(peer.elements[i].get(),binding.rightType);
                        bindings.push_back(binding);
                    }
                    return bindings;
                };
                if (rhsRow->constructor && comparisons.count(operation)) {
                    binary->rowComparison = true;
                    binary->rowComparisons = {pair(*rhsRow,operation)};
                    return;
                }
                if (!rhsRow->constructor && (operation == "in" || operation == "not in") &&
                    std::all_of(rhsRow->elements.begin(),rhsRow->elements.end(),[](const auto& member) {
                        const auto* row = dynamic_cast<const RowExpr*>(member.get());
                        return row && row->constructor;
                    })) {
                    binary->rowComparison = true;
                    binary->rowComparisons.clear();
                    for (auto& member : rhsRow->elements)
                        binary->rowComparisons.push_back(pair(*static_cast<RowExpr*>(member.get()),"="));
                    return;
                }
            }
            const auto unknownInput=[&](ExprPtr& operand,const std::string& source,const std::string& target) {
                if(source!="unknown")return;
                auto conversion=std::make_unique<CastExpr>();
                conversion->typeName=target;conversion->implicit=true;
                conversion->sourceBegin=operand->sourceBegin;conversion->sourceEnd=operand->sourceEnd;
                conversion->operand=std::move(operand);
                if(const auto* literal=dynamic_cast<const LiteralExpr*>(conversion->operand.get());
                   literal && literal->typeName.empty() && !literal->preparedSubquery) {
                    ExprEvaluator pure;(void)pure.eval(conversion.get(),RowContext{});
                }
                operand=std::move(conversion);
            };
            if(bitOperand && comparisons.count(operation)) {
                const auto comparison=ExprEvaluator::resolveComparison(operation,leftType,rightType);
                unknownInput(binary->left,leftType,comparison.leftType);
                unknownInput(binary->right,rightType,comparison.rightType);
            }
            if((operation=="in" || operation=="not in") && dynamic_cast<RowExpr*>(binary->right.get())) {
                auto* list=static_cast<RowExpr*>(binary->right.get());
                std::string listLeftType=leftType;
                std::string bitMemberType;
                bool mixedKnownTypes=false;
                for(const auto& member:list->elements) {
                    const auto memberType=canonicalResultTypeName(type(member.get()));
                    if(memberType=="bit" || memberType=="bit varying")bitMemberType=memberType;
                    else if(memberType!="unknown")mixedKnownTypes=true;
                }
                if(listLeftType=="unknown" && !bitMemberType.empty() && mixedKnownTypes &&
                   dynamic_cast<ParameterExpr*>(binary->left.get())) {
                    for(const auto& member:list->elements) {
                        const auto memberType=canonicalResultTypeName(type(member.get()));
                        if(memberType=="unknown")continue;
                        const auto target=ExprEvaluator::resolveComparison("=","unknown",memberType).leftType;
                        if(target!="bit" && target!="bit varying")
                            throw DbError("42P08","inconsistent types deduced for parameter");
                    }
                }
                if(listLeftType=="unknown" && !bitMemberType.empty() && !mixedKnownTypes) {
                    const auto comparison=ExprEvaluator::resolveComparison("=",listLeftType,bitMemberType);
                    unknownInput(binary->left,listLeftType,comparison.leftType);
                    listLeftType=comparison.leftType;
                }
                for(auto& member:list->elements) {
                    const auto memberType=canonicalResultTypeName(type(member.get()));
                    if(!(listLeftType=="unknown" && !bitMemberType.empty()) &&
                       listLeftType!="bit" && listLeftType!="bit varying" &&
                       memberType!="bit" && memberType!="bit varying")continue;
                    const auto comparison=ExprEvaluator::resolveComparison("=",listLeftType,memberType);
                    if(listLeftType=="unknown") {
                        // Mixed known member categories use independent
                        // comparisons. Validate a pure UNKNOWN literal, but
                        // do not pin the shared left operand to the first type.
                        if(const auto* literal=dynamic_cast<const LiteralExpr*>(binary->left.get());
                           literal && literal->typeName.empty() && !literal->preparedSubquery) {
                            CastExpr conversion;conversion.typeName=comparison.leftType;conversion.implicit=true;
                            conversion.operand=std::make_unique<LiteralExpr>(*literal);
                            ExprEvaluator pure;(void)pure.eval(&conversion,RowContext{});
                        }
                    }
                    unknownInput(member,memberType,comparison.rightType);
                }
            }
            if (operation=="like" || operation=="not like" || operation=="ilike" ||
                operation=="not ilike" || operation=="similar to" || operation=="not similar to")
                patternType(operation,{binary->left.get(),binary->right.get()});
            if (binary->op=="||") {
                binary->arrayConcat = resolveArrayConcatTypes(type(binary->left.get()),type(binary->right.get()),true);
                if (binary->arrayConcat) {
                    validateConst(binary->left.get(),binary->arrayConcat->leftType);
                    validateConst(binary->right.get(),binary->arrayConcat->rightType);
                }
            }
            return;
        }
        if (auto* array = dynamic_cast<ArrayExpr*>(node)) {
            const bool contextual = context.size()>=2 && context.compare(context.size()-2,2,"[]")==0;
            if (contextual) array->elementType = canonicalResultTypeName(context.substr(0,context.size()-2));
            for (auto& item : array->elements) visit(item.get(),contextual ? context : std::string());
            std::vector<std::string> types;
            for(const auto& item:array->elements)types.push_back(type(item.get()));
            array->nestedElements=std::any_of(types.begin(),types.end(),array_detail::isArray);
            if(!types.empty()) (void)array_detail::commonElement(types);
            if(contextual)for(const auto& source:types)array_detail::checkExplicitElementCast(source,array->elementType);
            if (array->elementType.empty()) {
                auto declared = type(array); array->elementType = declared.substr(0,declared.size()-2);
            }
            for (const auto& item : array->elements) validateConst(item.get(),array->elementType);
            return;
        }
        if (auto* unary = dynamic_cast<UnaryOpExpr*>(node)) visit(unary->operand.get(),{});
        else if (auto* conditional = dynamic_cast<CaseExpr*>(node)) {
            visit(conditional->switchExpr.get(),{}); visit(conditional->elseExpr.get(),{});
            for (auto& arm : conditional->whenClauses) { visit(arm.first.get(),{}); visit(arm.second.get(),{}); }
        } else if (auto* call = dynamic_cast<FunctionCallExpr*>(node)) {
            const auto operation=toLower(call->funcName);
            if (call->schema.empty() && (call->funcName == "BETWEEN" || call->funcName == "NOT BETWEEN"))
                for (const auto& argument : call->args)
                    between_input_detail::validateLiteralCasts(argument.get());
            if (call->schema.empty() && call->args.size()==3 &&
                (operation=="like escape" || operation=="not like escape" || operation=="ilike escape" ||
                 operation=="not ilike escape" || operation=="similar to escape" || operation=="not similar to escape"))
                patternType(operation,{call->args[0].get(),call->args[1].get(),call->args[2].get()});
            for (auto& arg : call->args) visit(arg.get(),{});
            if (call->schema.empty() && call->args.size() == 3 &&
                (operation == "between" || operation == "not between")) {
                const auto unknownInput = [&](ExprPtr& operand, const std::string& source,
                                              const std::string& target) {
                    if (source != "unknown") return;
                    auto conversion = std::make_unique<CastExpr>();
                    conversion->typeName = target; conversion->implicit = true;
                    conversion->sourceBegin = operand->sourceBegin;
                    conversion->sourceEnd = operand->sourceEnd;
                    conversion->operand = std::move(operand);
                    if (const auto* literal = dynamic_cast<const LiteralExpr*>(conversion->operand.get());
                        literal && literal->typeName.empty() && !literal->preparedSubquery) {
                        ExprEvaluator pure; (void)pure.eval(conversion.get(), RowContext{});
                    }
                    operand = std::move(conversion);
                };
                bool bitRange = false, integerRange = false;
                for (const auto& argument : call->args) {
                    const auto input = canonicalResultTypeName(type(argument.get()));
                    bitRange = bitRange || input == "bit" || input == "bit varying";
                    integerRange = integerRange || input == "smallint" || input == "integer" || input == "bigint";
                }
                for (size_t i = 1; i < 3; ++i) {
                    if (!bitRange && !integerRange) continue;
                    const auto lhs = canonicalResultTypeName(type(call->args[0].get()));
                    const auto rhs = canonicalResultTypeName(type(call->args[i].get()));
                    const auto comparison = ExprEvaluator::resolveComparison(i == 1 ? ">=" : "<=", lhs, rhs);
                    if (lhs == "unknown") {
                        if (dynamic_cast<ParameterExpr*>(call->args[0].get()))
                            unknownInput(call->args[0], lhs, comparison.leftType);
                        else if (const auto* literal = dynamic_cast<const LiteralExpr*>(call->args[0].get());
                                 literal && literal->typeName.empty() && !literal->preparedSubquery) {
                            ExprPtr input = std::make_unique<LiteralExpr>(*literal);
                            unknownInput(input, "unknown", comparison.leftType);
                        }
                    }
                    unknownInput(call->args[i], rhs, comparison.rightType);
                }
            }
            for (auto& arg : call->namedArgs) visit(arg.value.get(),{});
            visit(call->filter.get(),{});
            for (auto& arg : call->over.partitionBy) visit(arg.get(),{});
            for (auto& arg : call->over.orderBy) visit(arg.first.get(),{});
            visit(call->over.frameStart.get(),{}); visit(call->over.frameEnd.get(),{});
        } else if (auto* row = dynamic_cast<RowExpr*>(node)) {
            row->fieldTypes.clear();
            for (auto& arg : row->elements) {
                visit(arg.get(),{});
                row->fieldTypes.push_back(type(arg.get()));
            }
        }
    };
    visit(expression,{});
}

std::string ExprHelper::inferResultType(
    const std::string& exprSql,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    StorageEngine* functionEngine) {
    const std::string trimmed = [&] {
        size_t first = exprSql.find_first_not_of(" \t\r\n");
        if (first == std::string::npos) return std::string{};
        size_t last = exprSql.find_last_not_of(" \t\r\n");
        return exprSql.substr(first, last - first + 1);
    }();
    const std::string lower = toLower(trimmed);
    if (!currentDB.empty()) {
        SQLParser parser;
        auto parsed = parser.parse("SELECT " + trimmed);
        const auto* select = parsed.success ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
        if (select && select->selectList.size() == 1) {
            const auto routineTypes = collectRoutineResultTypes(
                select->selectList.front().expr.get(), currentDB, functionEngine);
            if (!routineTypes.empty()) {
                const std::string type = inferAstResultType(
                    select->selectList.front().expr.get(), typeHints, &routineTypes);
                return type.empty() || type == "unknown" ? "text" : type;
            }
        }
    }

    // CASE result types come from value arms, not predicates or an untyped
    // NULL's standalone text fallback. Use the parsed CASE before textual
    // heuristics inspect operators/functions inside individual arms.
    size_t caseBegin = 0;
    while (caseBegin < lower.size() && (lower[caseBegin] == '(' ||
           std::isspace(static_cast<unsigned char>(lower[caseBegin])))) ++caseBegin;
    if (lower.compare(caseBegin, 4, "case") == 0 &&
        (caseBegin + 4 == lower.size() ||
         !std::isalnum(static_cast<unsigned char>(lower[caseBegin + 4])))) {
        SQLParser caseParser;
        const auto parsedCase = caseParser.parse("SELECT " + trimmed);
        const auto* caseSelect = parsedCase.success
            ? dynamic_cast<const SelectStmt*>(parsedCase.stmt.get()) : nullptr;
        if (caseSelect && caseSelect->selectList.size() == 1 &&
            dynamic_cast<const CaseExpr*>(caseSelect->selectList.front().expr.get())) {
            const std::string type = inferAstResultType(
                caseSelect->selectList.front().expr.get(), typeHints);
            return type.empty() || type == "unknown" ? "text" : type;
        }
    }

    if (lower == "current_user" || lower == "session_user" ||
        lower == "user") return "name";
    if (lower == "current_date") return "date";
    if (lower == "current_timestamp") return "timestamptz";
    if (lower == "localtimestamp") return "timestamp";

    // Compound and reduction roots must be typed as a whole. Textual cast/function
    // shortcuts below can otherwise mistake an operand's type for a boolean
    // comparison or for a mixed-width arithmetic operator's result.
    ParseResult structuralParse;
    if (const Expr* structural = parseStoredExpression(trimmed, structuralParse)) {
        const auto* binary = dynamic_cast<const BinaryOpExpr*>(structural);
        const auto* outerCast = dynamic_cast<const CastExpr*>(structural);
        const auto* grammar = dynamic_cast<const FunctionCallExpr*>(outerCast ? outerCast->operand.get() : structural);
        const bool rangePredicate = grammar && grammar->schema.empty() && grammar->args.size() == 3 &&
            (grammar->funcName == "BETWEEN" || grammar->funcName == "NOT BETWEEN");
        const std::string type = inferAstResultType(structural, typeHints);
        const bool numericOperator = binary && arithmetic_detail::resultType(
            toLower(binary->op), inferAstResultType(binary->left.get(), typeHints),
            inferAstResultType(binary->right.get(), typeHints)).has_value();
        // A predicate inside an aggregate argument/FILTER (or a string value)
        // cannot declare the aggregate's result boolean. Infer the actual
        // outer reducer and its value-argument overload before textual scans.
        if (rangePredicate || !builtinReductionName(dynamic_cast<const FunctionCallExpr*>(structural)).empty() ||
            dynamic_cast<const UnaryOpExpr*>(structural) || dynamic_cast<const ArrayExpr*>(structural) ||
            (binary && (type == "boolean" || binary->op == "::" ||
                        binary->op == "||" || numericOperator))) {
            return type.empty() || type == "unknown" ? "text" : type;
        }
    }

    // JSON extraction operators preserve the JSON container type; their
    // text variants deliberately return text.  Handle these before looking
    // for a postfix cast because the cast belongs to the left operand.
    if (lower.find("->>") != std::string::npos ||
        lower.find("#>>") != std::string::npos) return "text";
    if (lower.find("->") != std::string::npos ||
        lower.find("#>") != std::string::npos) {
        if (lower.find("::jsonb") != std::string::npos) return "jsonb";
        if (lower.find("::json") != std::string::npos) return "json";
    }
    if (lower.rfind("date ", 0) == 0 &&
        (lower.find(" + interval ") != std::string::npos ||
         lower.find(" - interval ") != std::string::npos)) {
        return "timestamp";
    }
    // Some projection paths preserve a typed literal as DATE '...' instead
    // of lowering it to a CastExpr.  Recognize column - DATE '...' so the
    // protocol advertises PostgreSQL's integer result type.
    const size_t typedDateSubtract = lower.find(" - date ");
    if (typedDateSubtract != std::string::npos) {
        const auto trimCopy = [](std::string value) {
            const size_t first = value.find_first_not_of(" \t\r\n");
            if (first == std::string::npos) return std::string{};
            const size_t last = value.find_last_not_of(" \t\r\n");
            return value.substr(first, last - first + 1);
        };
        std::string left = trimCopy(lower.substr(0, typedDateSubtract));
        const std::string right =
            trimCopy(lower.substr(typedDateSubtract + 3));
        while (left.size() >= 2 && left.front() == '(' && left.back() == ')')
            left = trimCopy(left.substr(1, left.size() - 2));
        const size_t qualifier = left.rfind('.');
        const std::string bareLeft = qualifier == std::string::npos
            ? left : left.substr(qualifier + 1);
        std::string leftType;
        for (const auto& hint : typeHints) {
            const std::string key = toLower(hint.first);
            if (key == left || key == bareLeft) {
                leftType = canonicalTypeName(hint.second);
                break;
            }
        }
        if (leftType == "date" && right.size() >= 7 &&
            right.rfind("date '", 0) == 0 &&
            right.back() == static_cast<char>(39)) {
            return "integer";
        }
    }
    const size_t dateCast = lower.find("::date");
    if (dateCast != std::string::npos) {
        const size_t secondDateCast = lower.find("::date", dateCast + 6);
        if (secondDateCast != std::string::npos &&
            lower.find(" - ", dateCast + 6) != std::string::npos)
            return "integer";
        const size_t op = lower.find_first_of("+-", dateCast + 6);
        if (op != std::string::npos) {
            const std::string right = lower.substr(op + 1);
            if (right.find_first_not_of(" \t\r\n0123456789") ==
                std::string::npos)
                return "date";
        }
    }
    if (const size_t atZone = lower.rfind(" at time zone ");
        atZone != std::string::npos) {
        const std::string input = inferResultType(
            trimmed.substr(0, atZone), typeHints);
        return input == "timestamptz" ? "timestamp" : "timestamptz";
    }
    if (lower.rfind("round(", 0) == 0 || lower.rfind("trunc(", 0) == 0) {
        if (lower.find("::float8") != std::string::npos ||
            lower.find("::double precision") != std::string::npos ||
            lower.find("::real") != std::string::npos)
            return "double precision";
    }
    if (lower.find(" @> ") != std::string::npos ||
        lower.find(" <@ ") != std::string::npos ||
        lower.find(" && ") != std::string::npos)
        return "boolean";

    // The expression parser accepts PostgreSQL postfix casts while evaluating,
    // but older AST paths can leave the cast suffix outside the returned root.
    // Read a top-level suffix directly so protocol metadata follows the cast,
    // including typemods such as numeric(12,2).
    size_t postfixCast = std::string::npos;
    int castDepth = 0;
    bool castString = false;
    bool castIdentifier = false;
    for (size_t i = 0; i + 1 < trimmed.size(); ++i) {
        const char ch = trimmed[i];
        if (castString) {
            if (ch == '\'' && i + 1 < trimmed.size() && trimmed[i + 1] == '\'') {
                ++i;
            } else if (ch == '\'') {
                castString = false;
            }
            continue;
        }
        if (castIdentifier) {
            if (ch == '"' && i + 1 < trimmed.size() && trimmed[i + 1] == '"') {
                ++i;
            } else if (ch == '"') {
                castIdentifier = false;
            }
            continue;
        }
        if (ch == '\'') { castString = true; continue; }
        if (ch == '"') { castIdentifier = true; continue; }
        if (ch == '(' || ch == '[') { ++castDepth; continue; }
        if (ch == ')' || ch == ']') { if (castDepth > 0) --castDepth; continue; }
        if (castDepth == 0 && ch == ':' && trimmed[i + 1] == ':') {
            postfixCast = i;
            ++i;
        }
    }
    if (postfixCast != std::string::npos) {
        std::string rawTarget = toLower(trimmed.substr(postfixCast + 2));
        const size_t first = rawTarget.find_first_not_of(" \t\r\n");
        const size_t last = rawTarget.find_last_not_of(" \t\r\n");
        rawTarget = first == std::string::npos
            ? std::string{} : rawTarget.substr(first, last - first + 1);
        bool completeType = !rawTarget.empty();
        const size_t modifier = rawTarget.find('(');
        if (modifier != std::string::npos) {
            completeType = rawTarget.back() == ')' &&
                rawTarget.find(')', modifier) == rawTarget.size() - 1;
            for (size_t i = modifier + 1;
                 completeType && i + 1 < rawTarget.size(); ++i) {
                const unsigned char ch =
                    static_cast<unsigned char>(rawTarget[i]);
                completeType = std::isdigit(ch) || std::isspace(ch) ||
                    rawTarget[i] == ',';
            }
        }
        const std::string target = completeType
            ? protocolTypeName(rawTarget) : std::string{};
        static const std::set<std::string> postfixTypes = {
            "smallint", "integer", "bigint", "numeric", "real",
            "double precision", "boolean", "text", "varchar", "bpchar",
            "date", "time", "timetz", "timestamp", "timestamptz",
            "interval", "json", "jsonb", "xml", "uuid", "name", "regtype",
            "smallint[]", "integer[]", "bigint[]", "numeric[]", "real[]",
            "double precision[]", "boolean[]", "text[]", "varchar[]",
            "date[]", "time[]", "timestamp[]", "timestamptz[]", "uuid[]"
        };
        if (completeType && postfixTypes.count(target)) return target;
    }

    // SQL preprocessing lowers CASE into an evaluator-only case_when wrapper.
    // Conditions in that wrapper are not general SQL, so infer its value slots
    // directly instead of asking the expression parser to read them.
    if (lower.rfind("case_when(", 0) == 0 && trimmed.back() == ')') {
        std::vector<std::string> args;
        std::string current;
        int depth = 0;
        bool quoted = false;
        for (size_t i = 10; i + 1 < trimmed.size(); ++i) {
            const char ch = trimmed[i];
            if (ch == '\'') quoted = !quoted;
            if (!quoted && (ch == '(' || ch == '[')) ++depth;
            if (!quoted && (ch == ')' || ch == ']') && depth > 0) --depth;
            if (!quoted && depth == 0 && ch == ',') {
                args.push_back(current);
                current.clear();
            } else {
                current += ch;
            }
        }
        args.push_back(current);
        std::string result = "unknown";
        for (size_t i = 1; i < args.size(); i += 2)
            result = mergeProtocolTypes(
                result, inferResultType(args[i], typeHints));
        if (args.size() % 2 == 1)
            result = mergeProtocolTypes(
                result, inferResultType(args.back(), typeHints));
        if (!result.empty() && result != "unknown") return result;
    }

    static const std::vector<std::string> typedLiteralTypes = {
        "timestamptz", "timestamp", "interval", "date", "time",
        "numeric", "boolean", "text", "xml"
    };
    for (const std::string& type : typedLiteralTypes) {
        const std::string prefix = type + " ";
        if (lower.rfind(prefix, 0) == 0 &&
            trimmed.size() > prefix.size() && trimmed[prefix.size()] == '\'') {
            size_t close = prefix.size() + 1;
            while (close < trimmed.size()) {
                if (trimmed[close] != '\'') { ++close; continue; }
                if (close + 1 < trimmed.size() && trimmed[close + 1] == '\'') {
                    close += 2;
                    continue;
                }
                break;
            }
            if (close + 1 == trimmed.size()) return type;
        }
    }
    if (lower.rfind("case", 0) != 0 &&
        (lower.find(" between ") != std::string::npos ||
         lower.find(" is distinct from ") != std::string::npos ||
         lower.find(" is not distinct from ") != std::string::npos ||
         lower.find(" like ") != std::string::npos ||
         lower.find(" ilike ") != std::string::npos)) {
        return "boolean";
    }
    if (lower.rfind("array[", 0) == 0 || lower.rfind("array [", 0) == 0) {
        if (trimmed.find('\'') != std::string::npos) return "text[]";
        if (trimmed.find('.') != std::string::npos) return "numeric[]";
        return "integer[]";
    }
    for (const char* arrayFunction : {
             "array_append(", "array_prepend(", "array_remove(",
             "array_replace("}) {
        if (lower.rfind(arrayFunction, 0) != 0 ||
            lower.find("array[") == std::string::npos) continue;
        if (trimmed.find('\'') != std::string::npos) return "text[]";
        if (trimmed.find('.') != std::string::npos) return "numeric[]";
        return "integer[]";
    }

    // Simple CASE has a selector between CASE and the first WHEN.  Some legacy
    // parser paths treat that form as an opaque expression, so merge only its
    // result arms here.  Nested CASE expressions are kept at their own depth.
    if (lower.rfind("case", 0) == 0) {
        struct CaseWord { size_t begin; size_t end; std::string word; };
        std::vector<CaseWord> words;
        int parenDepth = 0;
        int caseDepth = 0;
        bool string = false;
        bool identifier = false;
        for (size_t i = 0; i < trimmed.size();) {
            const char ch = trimmed[i];
            if (string) {
                if (ch == '\'' && i + 1 < trimmed.size() &&
                    trimmed[i + 1] == '\'') i += 2;
                else { if (ch == '\'') string = false; ++i; }
                continue;
            }
            if (identifier) {
                if (ch == '"' && i + 1 < trimmed.size() &&
                    trimmed[i + 1] == '"') i += 2;
                else { if (ch == '"') identifier = false; ++i; }
                continue;
            }
            if (ch == '\'') { string = true; ++i; continue; }
            if (ch == '"') { identifier = true; ++i; continue; }
            if (ch == '(' || ch == '[') { ++parenDepth; ++i; continue; }
            if (ch == ')' || ch == ']') {
                if (parenDepth > 0) --parenDepth;
                ++i;
                continue;
            }
            if (parenDepth == 0 &&
                (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_')) {
                const size_t begin = i++;
                while (i < trimmed.size() &&
                       (std::isalnum(static_cast<unsigned char>(trimmed[i])) ||
                        trimmed[i] == '_' || trimmed[i] == '$')) ++i;
                const std::string word = lower.substr(begin, i - begin);
                if (word == "case") {
                    ++caseDepth;
                } else if (word == "end") {
                    if (caseDepth == 1) words.push_back({begin, i, word});
                    if (caseDepth > 0) --caseDepth;
                } else if (caseDepth == 1 &&
                           (word == "then" || word == "when" ||
                            word == "else")) {
                    words.push_back({begin, i, word});
                }
                continue;
            }
            ++i;
        }
        std::string result = "unknown";
        for (size_t i = 0; i < words.size(); ++i) {
            if (words[i].word != "then" && words[i].word != "else") continue;
            const size_t valueBegin = words[i].end;
            const size_t valueEnd = i + 1 < words.size()
                ? words[i + 1].begin : trimmed.size();
            result = mergeProtocolTypes(
                result, inferResultType(
                            trimmed.substr(valueBegin, valueEnd - valueBegin),
                            typeHints));
        }
        if (!result.empty() && result != "unknown") return result;
    }

    ParseResult parsed;
    const Expr* expression = parseStoredExpression(trimmed, parsed);
    if (!expression) {
        if (lower.find(" is ") != std::string::npos ||
            lower.find(" between ") != std::string::npos ||
            lower.find(" like ") != std::string::npos) return "boolean";
        return "text";
    }
    std::string type = protocolTypeName(
        inferAstResultType(expression, typeHints));
    if (type.empty() || type == "unknown") type = "text";
    return type;
}

std::optional<bool> ExprHelper::referencesColumn(
    const std::string& exprSql, const std::string& columnName) {
    if (exprSql.empty()) return false;
    ParseResult parsed;
    const Expr* expression = parseStoredExpression(exprSql, parsed);
    if (!expression) return std::nullopt;
    size_t count = 0;
    if (!countParsedColumnReferences(expression, columnName, count)) {
        // Unsupported AST shapes remain conservative for DDL dependency
        // checks: callers must not remove a possibly referenced column.
        return true;
    }
    return count != 0;
}

std::optional<std::string> ExprHelper::renameColumnReferences(
    const std::string& exprSql, const std::string& oldName,
    const std::string& newName) {
    if (exprSql.empty() || oldName == newName) return exprSql;

    const auto sourceTokens = lexExpressionSource(exprSql);
    if (!sourceTokens) return std::nullopt;
    const bool containsOldIdentifier = std::any_of(
        sourceTokens->begin(), sourceTokens->end(),
        [&](const SourceToken& token) {
            return token.kind == SourceTokenKind::Identifier &&
                   token.text == oldName;
        });
    if (!containsOldIdentifier) return exprSql;

    ParseResult parsed;
    const Expr* expression = parseStoredExpression(exprSql, parsed);
    if (!expression) return std::nullopt;
    size_t oldReferenceCount = 0;
    size_t existingNewReferenceCount = 0;
    if (!countParsedColumnReferences(
            expression, oldName, oldReferenceCount) ||
        !countParsedColumnReferences(
            expression, newName, existingNewReferenceCount)) {
        return std::nullopt;
    }
    if (oldReferenceCount == 0) return exprSql;

    const std::vector<size_t> referenceTokens =
        columnReferenceSourceTokens(*sourceTokens, oldName);
    if (referenceTokens.size() != oldReferenceCount) return std::nullopt;

    std::string rewritten;
    rewritten.reserve(exprSql.size() +
        oldReferenceCount *
            (newName.size() > oldName.size()
                 ? newName.size() - oldName.size() : 0));
    size_t cursor = 0;
    for (const size_t tokenIndex : referenceTokens) {
        const SourceToken& token = sourceTokens->at(tokenIndex);
        rewritten.append(exprSql, cursor, token.begin - cursor);
        rewritten += newName;
        cursor = token.end;
    }
    rewritten.append(exprSql, cursor, std::string::npos);

    ParseResult rewrittenParse;
    const Expr* rewrittenExpression =
        parseStoredExpression(rewritten, rewrittenParse);
    if (!rewrittenExpression) return std::nullopt;
    size_t remainingOldReferences = 0;
    size_t rewrittenNewReferences = 0;
    if (!countParsedColumnReferences(
            rewrittenExpression, oldName, remainingOldReferences) ||
        !countParsedColumnReferences(
            rewrittenExpression, newName, rewrittenNewReferences) ||
        remainingOldReferences != 0 ||
        rewrittenNewReferences !=
            existingNewReferenceCount + oldReferenceCount) {
        return std::nullopt;
    }
    return rewritten;
}

static ExprEvalResult evalStringImpl(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    const std::set<std::string>* nullColumns,
    const std::string& currentDB,
    const std::string& currentUser,
    StorageEngine* functionEngine = nullptr,
    const std::map<std::string, std::string>* collationHints = nullptr) {

    // The parser retains the actual declared-type grammar. Rewriting a
    // keyword/string pair into :: changes bare BIT/CHAR length semantics and
    // corrupts qualified names or escaped quotes.
    const std::string& sql = exprSql;

    ExprEvalResult res;
    if (exprSql.empty()) {
        res.error = "empty expression";
        return res;
    }

    // Parse the expression by wrapping it in a SELECT.
    SQLParser parser;
    ParseResult pr = parser.parse("SELECT " + sql);
    if (!pr.success || !pr.stmt) {
        res.error = pr.error.empty() ? "failed to parse expression" : pr.error;
        res.sqlState = pr.sqlState;
        return res;
    }

    auto* select = dynamic_cast<SelectStmt*>(pr.stmt.get());
    if (!select || select->selectList.empty() || !select->selectList[0].expr) {
        res.error = "expression did not parse to a SELECT item";
        return res;
    }

    // RowContext retains its legacy case-insensitive keys.  Parser column
    // components, however, already contain canonical SQL identities: quoted
    // "F" is not f. Bind this local AST to private datum positions instead of
    // asking RowContext to resolve user identifiers a second time.
    std::vector<ColumnRefExpr*> references;
    std::function<void(Expr*)> visitValueReferences = [&](Expr* expression) {
        if (!expression) return;
        if (auto* column = dynamic_cast<ColumnRefExpr*>(expression)) {
            references.push_back(column);
        } else if (auto* unary = dynamic_cast<UnaryOpExpr*>(expression)) {
            visitValueReferences(unary->operand.get());
        } else if (auto* binary = dynamic_cast<BinaryOpExpr*>(expression)) {
            visitValueReferences(binary->left.get());
            // These right-hand nodes are type/collation labels, not values.
            if (binary->op != "::" && binary->op != "COLLATE")
                visitValueReferences(binary->right.get());
        } else if (auto* quantified = dynamic_cast<QuantifiedComparisonExpr*>(expression)) {
            visitValueReferences(quantified->left.get()); visitValueReferences(quantified->right.get());
        } else if (auto* cast = dynamic_cast<CastExpr*>(expression)) {
            visitValueReferences(cast->operand.get());
        } else if (auto* conditional = dynamic_cast<CaseExpr*>(expression)) {
            visitValueReferences(conditional->switchExpr.get());
            for (auto& arm : conditional->whenClauses) {
                visitValueReferences(arm.first.get());
                visitValueReferences(arm.second.get());
            }
            visitValueReferences(conditional->elseExpr.get());
        } else if (auto* call = dynamic_cast<FunctionCallExpr*>(expression)) {
            for (size_t i = 0; i < call->args.size(); ++i) {
                // EXTRACT's bare field is grammar, unlike date_part's value
                // or a schema-qualified extract() argument. Preserve that
                // parser role for every whitespace/quoted spelling.
                if (i == 0 && call->schema.empty() &&
                    toLower(call->funcName) == "extract") {
                    const auto* field = dynamic_cast<ColumnRefExpr*>(call->args[i].get());
                    if (field && field->schema.empty() && field->table.empty()) {
                        auto literal = std::make_unique<LiteralExpr>();
                        literal->typeName = "text";
                        literal->value = "'";
                        for (const char character : field->column) {
                            literal->value += character;
                            if (character == '\'') literal->value += character;
                        }
                        literal->value += "'";
                        call->args[i] = std::move(literal);
                        continue;
                    }
                }
                visitValueReferences(call->args[i].get());
            }
            for (auto& argument : call->namedArgs)
                visitValueReferences(argument.value.get());
            visitValueReferences(call->filter.get());
            for (auto& item : call->over.partitionBy) visitValueReferences(item.get());
            for (auto& item : call->over.orderBy) visitValueReferences(item.first.get());
            visitValueReferences(call->over.frameStart.get());
            visitValueReferences(call->over.frameEnd.get());
        } else if (auto* array = dynamic_cast<ArrayExpr*>(expression)) {
            for (auto& item : array->elements) visitValueReferences(item.get());
        } else if (auto* tuple = dynamic_cast<RowExpr*>(expression)) {
            for (auto& item : tuple->elements) visitValueReferences(item.get());
        }
    };
    visitValueReferences(select->selectList[0].expr.get());
    std::set<std::string> occupiedKeys;
    for (const auto* reference : references) {
        occupiedKeys.insert(toLower(reference->column));
        occupiedKeys.insert(toLower(reference->toString()));
    }
    for (const auto& entry : row) occupiedKeys.insert(toLower(entry.first));

    RowContext ctx;
    std::map<std::string, std::string> bindings;
    size_t nextPosition = 0;
    for (const auto& [name, value] : row) {
        std::string typeName = inferType(value);
        auto it = typeHints.find(name);
        if (it != typeHints.end() && !it->second.empty()) {
            typeName = canonicalTypeName(it->second);
        }
        const bool isNull = nullColumns
            ? nullColumns->count(name) != 0
            : value.empty();
        std::string key;
        do {
            key = "\x01helper_value_" + std::to_string(nextPosition++);
        } while (!occupiedKeys.insert(key).second);
        ExprValue datum(typeName,value,isNull);
        if (collationHints) {
            const auto collation=collationHints->find(name);
            if (collation!=collationHints->end()) datum.collation=collation->second;
        }
        ctx.set(key,std::move(datum));
        bindings.emplace(name, key);
    }
    // PostgreSQL exposes these as special session-value expressions. The
    // parser accepts both the function-like AST form and the bare identifier
    // form, so seed the context as well as registering evaluator functions.
    if (!currentUser.empty()) {
        const ExprValue userValue("name", currentUser, false);
        ctx.set("current_user", userValue);
        ctx.set("session_user", userValue);
        ctx.setSqlValue(FunctionCallExpr::SqlValue::CurrentUser,userValue);
        ctx.setSqlValue(FunctionCallExpr::SqlValue::CurrentRole,userValue);
        const auto* session=currentSession();
        ctx.setSqlValue(FunctionCallExpr::SqlValue::SessionUser,
            ExprValue("name",session && !session->originalRole.empty()?session->originalRole:currentUser,false));
    }
    // SQL-standard date/time special values, resolvable as bare identifiers
    // (PG exposes current_date/current_timestamp/localtimestamp both ways).
    // This layer currently uses UTC as its session timezone.
    {
        const std::time_t clock = std::time(nullptr);
        const std::string date = formatUtcClock(clock, "%Y-%m-%d");
        const std::string timestamp =
            formatUtcClock(clock, "%Y-%m-%d %H:%M:%S");
        ctx.set("current_date", ExprValue("date", date, date.empty()));
        ctx.set("current_timestamp",
                ExprValue("timestamptz", timestamp + "+00",
                          timestamp.empty()));
        ctx.set("localtimestamp",
                ExprValue("timestamp", timestamp, timestamp.empty()));
    }

    ExprEvaluator evaluator;
    evaluator.setCurrentDB(currentDB);
    evaluator.registerFunction("current_user", [currentUser](const std::vector<ExprValue>&) {
        return ExprValue("name", currentUser, currentUser.empty());
    }, 's');
    evaluator.registerFunction("session_user", [currentUser](const std::vector<ExprValue>&) {
        return ExprValue("name", currentUser, currentUser.empty());
    }, 's');
    ExprValue v;
    try {
        for (auto* reference : references) {
            auto binding = bindings.find(reference->toString());
            // A scalar caller can provide a single prevalidated relation as
            // bare column keys. Preserve that existing qualifier fallback,
            // but never fold the column identity or an explicit qualified key.
            if (binding == bindings.end() && !reference->schema.empty())
                binding = bindings.find(reference->table + "." + reference->column);
            if (binding == bindings.end() && !reference->table.empty())
                binding = bindings.find(reference->column);
            if (binding == bindings.end()) {
                static const std::set<std::string> sessionValues = {
                    "current_user", "session_user", "current_date",
                    "current_timestamp", "localtimestamp"
                };
                if (reference->schema.empty() && reference->table.empty() &&
                    sessionValues.count(reference->column)) continue;
                throw DbError("42703", "column does not exist: " + reference->toString());
            }
            reference->column = binding->second;
            reference->table.clear();
            reference->schema.clear();
        }
        std::map<std::string,std::string> preparedHints;
        for (const auto& entry : bindings) preparedHints[entry.second] = ctx.get(entry.second)->typeName;
        ExprHelper::prepareArrayTypes(select->selectList[0].expr.get(),preparedHints,currentDB,functionEngine);
        evaluator.bindScalarFunctions(select->selectList[0].expr.get(), functionEngine);
        v = evaluator.eval(select->selectList[0].expr.get(), ctx);
    } catch (const DbError& e) {
        res.error = e.what();
        res.sqlState = e.sqlState();
        return res;
    } catch (const std::exception& e) {
        // Runtime expression errors (division by zero, invalid cast input)
        // surface as evaluation failures carrying the engine message.
        res.error = e.what();
        return res;
    }

    if (v.isUnknown()) {
        res.error = "expression evaluated to an unsupported value";
        return res;
    }

    res.ok = true;
    res.isNull = v.isNull;
    res.value = v.value;
    res.typeName = v.typeName;
    res.collation = v.collation;
    return res;
}

ExprEvalResult ExprHelper::evalString(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    const std::string& currentUser,
    StorageEngine* functionEngine) {
    return evalStringImpl(
        exprSql, row, typeHints, nullptr, currentDB, currentUser, functionEngine);
}

ExprEvalResult ExprHelper::evalStringWithNulls(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::set<std::string>& nullColumns,
    const std::map<std::string, std::string>& typeHints,
    const std::string& currentDB,
    const std::string& currentUser,
    StorageEngine* functionEngine,
    const std::map<std::string, std::string>& collationHints) {
    return evalStringImpl(
        exprSql, row, typeHints, &nullColumns, currentDB, currentUser, functionEngine,&collationHints);
}

bool ExprHelper::evalBool(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    std::string* error,
    const std::string& currentDB,
    const std::string& currentUser,
    StorageEngine* functionEngine) {

    ExprEvalResult r = evalString(exprSql, row, typeHints, currentDB, currentUser, functionEngine);
    if (!r.ok) {
        if (error) *error = r.error;
        return false;
    }
    if (r.isNull) return false;

    // ExprValue::asBool treats "t"/"true"/"1" as true; otherwise truthy.
    ExprValue tmp("boolean", r.value, false);
    return tmp.asBool();
}

bool ExprHelper::evalCheck(
    const std::string& exprSql,
    const std::map<std::string, std::string>& row,
    const std::map<std::string, std::string>& typeHints,
    std::string* error,
    const std::string& currentDB,
    const std::string& currentUser,
    StorageEngine* functionEngine) {

    ExprEvalResult r = evalString(
        exprSql, row, typeHints, currentDB, currentUser, functionEngine);
    if (!r.ok) {
        if (error) *error = r.error;
        return false;
    }
    if (r.isNull) return true;

    ExprValue tmp("boolean", r.value, false);
    return tmp.asBool();
}

} // namespace dbms
