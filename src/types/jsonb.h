#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace dbms {

// A JSONB value, not a SQL NULL or a serialized SQL expression. Parsing and
// rendering use explicit heap stacks. Numbers retain decimal display scale
// and never pass through floating point or the narrower SQL Numeric carrier.
class JsonbValue {
public:
    JsonbValue(const JsonbValue&) = default;
    JsonbValue(JsonbValue&&) noexcept = default;
    JsonbValue& operator=(const JsonbValue&) = default;
    JsonbValue& operator=(JsonbValue&&) noexcept = default;
    static JsonbValue parse(std::string_view input);
    static JsonbValue concatenate(JsonbValue left, JsonbValue right);
    std::string toString() const;

private:
    JsonbValue() = default;
    struct KeyLess {
        bool operator()(const std::string& left, const std::string& right) const;
    };
    enum class Kind { Object, Array, String, Scalar };
    using Members = std::map<std::string, size_t, KeyLess>;
    struct Node {
        Kind kind = Kind::Scalar;
        std::string scalar;
        std::vector<size_t> elements;
        Members members;
    };

    std::vector<Node> nodes_;
    size_t root_ = 0;
};

} // namespace dbms
