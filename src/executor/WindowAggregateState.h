#pragma once

#include "common/BooleanCodec.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "expression/float8_output.inc"
#include "types/bytea.h"
#include "types/numeric.h"

#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace dbms::prepared_window_detail {

// One actual window aggregate transition, independent of source/filter/frame
// traversal. It receives already evaluated owning datums, never original SQL
// expressions or a statement's parameter/context/provider/cursor.
class AggregateState {
public:
    using Compare = std::function<int(const ExprValue&, const ExprValue&)>;

private:
    std::string name_, inputType_, resultType_;
    ExprEvaluator* evaluator_;
    Compare compare_;
    int64_t count_ = 0, integerSum_ = 0;
    Numeric numericSum_{0};
    double floatingSum_ = 0;
    uint64_t integerBits_ = 0;
    bool boolean_ = false;
    std::optional<ExprValue> selected_, cached_;
    std::vector<ExprValue> array_;
    std::string concatenated_;

    static int64_t integer(const ExprValue& value) {
        const char* first = value.value.data();
        const char* last = first + value.value.size();
        if (first != last && *first == '+') ++first;
        int64_t result = 0;
        const auto parsed = std::from_chars(first, last, result);
        if (parsed.ec == std::errc::result_out_of_range)
            throw DbError("22003", "window aggregate integer input is out of range");
        if (parsed.ec != std::errc{} || parsed.ptr != last)
            throw DbError("22P02", "invalid input syntax for window aggregate integer");
        return result;
    }
    void advance() {
        if (count_ == std::numeric_limits<int64_t>::max())
            throw DbError("22003", "window aggregate count is out of range");
        ++count_;
    }
    static ExprValue parameterValue(const ExprValue& value, const std::string& type) {
        auto result = value;
        result.typeName = type;
        return result;
    }
    ExprValue binary(const std::string& operation, const ExprValue& left,
                     const ExprValue& right) const {
        BinaryOpExpr expression;
        expression.op = operation;
        auto first = std::make_unique<ParameterExpr>();
        first->slot = 0; first->declaredType = left.typeName;
        first->origin = ParameterOrigin::RuntimeCell;
        auto second = std::make_unique<ParameterExpr>();
        second->slot = 1; second->declaredType = right.typeName;
        second->origin = ParameterOrigin::RuntimeCell;
        expression.left = std::move(first); expression.right = std::move(second);
        std::vector<ExprValue> arguments;
        arguments.reserve(2); arguments.emplace_back(left); arguments.emplace_back(right);
        RowContext row;
        row.setParameters(std::move(arguments));
        return evaluator_->eval(&expression, row);
    }
    static std::string float4Text(float value) {
        if (std::isnan(value)) return "NaN";
        if (std::isinf(value)) return std::signbit(value) ? "-Infinity" : "Infinity";
        char buffer[64];
        const auto result = std::to_chars(buffer, buffer + sizeof(buffer), value,
                                          std::chars_format::general);
        if (result.ec != std::errc{})
            throw DbError("XX000", "could not render window aggregate REAL result");
        return std::string(buffer, result.ptr);
    }
    static std::string bytes(const ExprValue& value) {
        ByteaValue parsed;
        if (!ByteaValue::parse(value.value, parsed))
            throw DbError("22P02", "invalid input syntax for type bytea");
        return parsed.bytes();
    }
    ExprValue arrayResult() const {
        if (array_.empty()) return ExprValue(resultType_, "", true);
        if (resultType_.size() < 2 ||
            resultType_.compare(resultType_.size() - 2, 2, "[]") != 0)
            throw DbError("XX000", "window ARRAY_AGG lost its array result declaration");
        ArrayExpr expression;
        expression.elementType = resultType_.substr(0, resultType_.size() - 2);
        expression.elements.reserve(array_.size());
        for (size_t i = 0; i < array_.size(); ++i) {
            auto element = std::make_unique<ParameterExpr>();
            element->slot = i;
            element->origin = ParameterOrigin::RuntimeCell;
            element->declaredType = array_[i].typeName;
            expression.elements.push_back(std::move(element));
        }
        RowContext row;
        row.setParameters(array_);
        auto result = evaluator_->eval(&expression, row);
        // RuntimeCell syntax has no COLLATE node. Preserve the already
        // analyzed argument datum's actual result collation as well.
        for (const auto& value : array_)
            if (!value.collation.empty()) { result.collation = value.collation; break; }
        return result;
    }
    void addNumber(const ExprValue& value) {
        if (resultType_ == "numeric") {
            numericSum_ += Numeric(value.value);
        } else if (resultType_ == "bigint") {
            const auto input = integer(value);
            if ((input > 0 && integerSum_ > std::numeric_limits<int64_t>::max() - input) ||
                (input < 0 && integerSum_ < std::numeric_limits<int64_t>::min() - input))
                throw DbError("22003", "window aggregate bigint sum is out of range");
            integerSum_ += input;
        } else if (resultType_ == "real" || resultType_ == "double precision") {
            const double raw = value.asDouble();
            const double input = inputType_ == "real"
                ? static_cast<double>(static_cast<float>(raw)) : raw;
            if (std::isinf(input) && std::isfinite(raw))
                throw DbError("22003", "window aggregate REAL input is out of range");
            double next = count_ == 0 && name_ == "sum" ? input : floatingSum_ + input;
            if (resultType_ == "real") next = static_cast<float>(next);
            if (std::isinf(next) && std::isfinite(floatingSum_) && std::isfinite(input))
                throw DbError("22003", "window aggregate floating-point sum is out of range");
            floatingSum_ = next;
        } else if (resultType_ == "interval" || (resultType_ == "money" && name_ == "sum")) {
            // Reuse the real interval/calendar and cash arithmetic owners.
            // These two RuntimeCells contain datums, not a replacement
            // statement frame or a user's/source's expression tree.
            selected_ = selected_ ? binary("+", *selected_, value)
                                 : parameterValue(value, resultType_);
        } else throw DbError("0A000", "unsupported window aggregate sum result type: " + resultType_);
    }
    ExprValue numberResult() const {
        if (!count_) return ExprValue(resultType_, "", true);
        if (resultType_ == "numeric")
            return ExprValue(resultType_, (name_ == "avg"
                ? numericSum_ / Numeric(count_) : numericSum_).toString());
        if (resultType_ == "bigint") return ExprValue(resultType_, std::to_string(integerSum_));
        if (resultType_ == "real" || resultType_ == "double precision") {
            const double value = name_ == "avg" ? floatingSum_ / static_cast<double>(count_) : floatingSum_;
            return ExprValue(resultType_, resultType_ == "real" ? float4Text(static_cast<float>(value))
                : float8_output_detail::format(value));
        }
        if (!selected_) throw DbError("XX000", "window sum lost its typed transition value");
        if (name_ == "avg")
            return binary("/", *selected_, ExprValue("double precision", std::to_string(count_)));
        return *selected_;
    }

public:
    AggregateState(std::string name, std::string inputType, std::string resultType,
                   ExprEvaluator& evaluator, Compare compare)
        : name_(std::move(name)),
          inputType_(ExprHelper::canonicalResultTypeName(inputType)),
          resultType_(ExprHelper::canonicalResultTypeName(resultType)),
          evaluator_(&evaluator), compare_(std::move(compare)) {}

    void reset() {
        count_ = 0; integerSum_ = 0; numericSum_ = Numeric(0); floatingSum_ = 0;
        integerBits_ = 0; boolean_ = false;
        selected_.reset(); cached_.reset(); array_.clear(); concatenated_.clear();
    }

    void add(const std::vector<ExprValue>& args) {
        const size_t width = name_ == "string_agg" ? 2 : 1;
        if (args.size() != width)
            throw DbError("XX000", "window aggregate received an invalid argument count");
        cached_.reset();
        const auto& value = args.front();
        if (name_ == "array_agg") {
            array_.push_back(value); // ARRAY_AGG includes genuine SQL NULL.
            return;
        }
        if (value.isNull) return;
        if (name_ == "count") { advance(); return; }
        if (name_ == "sum" || name_ == "avg") addNumber(value);
        else if (name_ == "min" || name_ == "max") {
            if (!compare_) throw DbError("XX000", "window MIN/MAX lost its typed comparator");
            if (!selected_ || (name_ == "min" ? compare_(value, *selected_) < 0
                                             : compare_(value, *selected_) > 0))
                selected_ = value; // Preserve the complete owning datum.
        } else if (name_ == "bool_and" || name_ == "bool_or" || name_ == "every") {
            const auto input = parsePostgresBoolean(value.value);
            if (!input) throw DbError("22P02", "invalid input syntax for type boolean");
            if (!count_) boolean_ = *input;
            else boolean_ = name_ == "bool_or" ? boolean_ || *input : boolean_ && *input;
        } else if (name_ == "string_agg") {
            const bool bytea = resultType_ == "bytea";
            if (count_ && !args[1].isNull)
                concatenated_ += bytea ? bytes(args[1]) : args[1].value;
            concatenated_ += bytea ? bytes(value) : value.value;
        } else if (name_ == "bit_and" || name_ == "bit_or" || name_ == "bit_xor") {
            if (resultType_ == "bit") {
                selected_ = selected_ ? binary(name_ == "bit_and" ? "&" : name_ == "bit_or" ? "|" : "#", *selected_, value) : value;
            } else {
                const auto input = static_cast<uint64_t>(integer(value));
                if (!count_) integerBits_ = input;
                else if (name_ == "bit_and") integerBits_ &= input;
                else if (name_ == "bit_or") integerBits_ |= input;
                else integerBits_ ^= input;
            }
        } else throw DbError("0A000", "unsupported window aggregate transition: " + name_);
        advance();
    }

    ExprValue finish() {
        if (cached_) return *cached_;
        ExprValue result(resultType_, "", true);
        if (name_ == "count") result = ExprValue(resultType_, std::to_string(count_));
        else if (name_ == "array_agg") result = arrayResult();
        else if (name_ == "sum" || name_ == "avg") result = numberResult();
        else if (name_ == "min" || name_ == "max") {
            if (selected_) result = *selected_;
        } else if (count_) {
            if (name_ == "bool_and" || name_ == "bool_or" || name_ == "every")
                result = ExprValue(resultType_, boolean_ ? "t" : "f");
            else if (name_ == "string_agg")
                result = ExprValue(resultType_, resultType_ == "bytea"
                    ? ByteaValue::fromBytes(concatenated_).toString() : concatenated_);
            else if (name_ == "bit_and" || name_ == "bit_or" || name_ == "bit_xor") {
                if (resultType_ == "bit") {
                    if (!selected_) throw DbError("XX000", "window bit aggregate lost its typed transition");
                    result = *selected_;
                } else {
                    int64_t signedBits;
                    static_assert(sizeof(signedBits) == sizeof(integerBits_));
                    std::memcpy(&signedBits, &integerBits_, sizeof(signedBits));
                    result = ExprValue(resultType_, std::to_string(signedBits));
                }
            }
        }
        result.typeName = resultType_;
        cached_ = result;
        return result;
    }
};

} // namespace dbms::prepared_window_detail
