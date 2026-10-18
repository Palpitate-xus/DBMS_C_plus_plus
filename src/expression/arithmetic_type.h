#pragma once

#include "common/DbError.h"
#include <algorithm>
#include <cctype>
#include <optional>
#include <string>

// Internal operator typing shared by execution and metadata analysis. This
// is deliberately separate from CASE/VALUES common-type selection.
namespace dbms::arithmetic_detail {

inline std::string numericTypeName(std::string type) {
    const size_t first = type.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    type = type.substr(first, type.find_last_not_of(" \t\r\n") - first + 1);
    std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (type == "int2" || type == "smallserial") return "smallint";
    if (type == "int" || type == "int4" || type == "serial") return "integer";
    if (type == "int8" || type == "bigserial") return "bigint";
    if (type == "float" || type == "float4") return "real";
    if (type == "double" || type == "float8") return "double precision";
    if (type == "decimal" || type.rfind("decimal(", 0) == 0 ||
        type.rfind("numeric(", 0) == 0) return "numeric";
    return type;
}

inline int integerWidth(const std::string& type) {
    const std::string name = numericTypeName(type);
    if (name == "smallint") return 1;
    if (name == "integer") return 2;
    if (name == "bigint") return 3;
    return 0;
}

inline int floatingWidth(const std::string& type) {
    const std::string name = numericTypeName(type);
    if (name == "real") return 1;
    if (name == "double precision") return 2;
    return 0;
}

inline const char* integerTypeName(int width) {
    return width == 1 ? "smallint" : width == 3 ? "bigint" : "integer";
}

inline std::optional<std::string> resultType(
    const std::string& operation, std::string left, std::string right,
    bool evaluatorUnknownStringMarker = false) {
    if (operation != "+" && operation != "-" && operation != "*" &&
        operation != "/" && operation != "%" && operation != "^")
        return std::nullopt;
    left = numericTypeName(std::move(left));
    right = numericTypeName(std::move(right));
    const auto temporalName = [](std::string type) {
        if (type == "timestamp without time zone" || type == "datetime")
            return std::string("timestamp");
        if (type == "timestamp with time zone") return std::string("timestamptz");
        return type;
    };
    left = temporalName(std::move(left));
    right = temporalName(std::move(right));
    const auto timestamp = [](const std::string& type) {
        return type == "timestamp" || type == "timestamptz";
    };
    // Resolve supported declared temporal signatures before UNKNOWN numeric
    // inference. Neither operand values nor a peer's sampled value own this
    // result type; DATE +/- INTERVAL promotes even for NULL and zero rows.
    if (operation == "+" || operation == "-") {
        if (right == "interval" && (left == "date" || timestamp(left)))
            return left == "date" ? "timestamp" : left;
        if (operation == "+" && left == "interval" &&
            (right == "date" || timestamp(right)))
            return right == "date" ? "timestamp" : right;
        if (operation == "-" && left == "date" && right == "date")
            return "integer";
        if (operation == "-" && timestamp(left) && timestamp(right))
            return "interval";
        if ((left == "date" && right == "integer") ||
            (operation == "+" && left == "integer" && right == "date"))
            return "date";
        if (left == "interval" && right == "interval") return "interval";
    }
    const auto unknown = [&](const std::string& type) {
        return type.empty() || type == "unknown" ||
            (evaluatorUnknownStringMarker && type == "character varying");
    };
    if (unknown(left) && unknown(right)) return std::nullopt;
    if (unknown(left)) left = right;
    if (unknown(right)) right = left;
    const auto numeric = [](const std::string& type) {
        return integerWidth(type) || floatingWidth(type) || type == "numeric";
    };
    const auto string = [](const std::string& type) {
        return type == "text" || type == "varchar" || type == "character varying" ||
            type == "char" || type == "character";
    };
    // A declared string is not an UNKNOWN input that may inherit the peer's
    // numeric type. This shared boundary is used by metadata and execution;
    // reject before a NULL/empty source can hide the absent operator.
    if ((numeric(left) && string(right)) || (string(left) && numeric(right)))
        throw DbError("42883", "operator does not exist: " + left + " " + operation + " " + right);
    if (!numeric(left) || !numeric(right)) return std::nullopt;
    const int leftFloat = floatingWidth(left), rightFloat = floatingWidth(right);
    if (operation == "^")
        return leftFloat || rightFloat || (left != "numeric" && right != "numeric")
            ? "double precision" : "numeric";
    if (leftFloat || rightFloat) {
        if (operation == "%") return std::nullopt;
        return leftFloat == 1 && rightFloat == 1 ? "real" : "double precision";
    }
    if (left == "numeric" || right == "numeric") return "numeric";
    return integerTypeName(std::max(integerWidth(left), integerWidth(right)));
}

} // namespace dbms::arithmetic_detail
