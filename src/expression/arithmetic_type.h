#pragma once

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
