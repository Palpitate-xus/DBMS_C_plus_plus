#include "jsonb.h"
#include "common/DbError.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

namespace dbms {
namespace {
constexpr size_t kMaxJsonbString = 0x0fffffff;
constexpr size_t kMaxJsonbText = (size_t{1} << 30) - 1;
constexpr int64_t kMaxDecimalIntegerDigits = 131072;
constexpr int64_t kMaxDecimalScale = 16383;
constexpr int64_t kMaxDecimalExponent = std::numeric_limits<int32_t>::max() / 2;

[[noreturn]] void badJson() {
    throw DbError("22P02", "invalid input syntax for type jsonb");
}
[[noreturn]] void numericRange() {
    throw DbError("22003", "value overflows numeric format in jsonb input");
}
bool digit(char ch) { return ch >= '0' && ch <= '9'; }
bool space(char ch) { return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'; }

void utf8(std::string& output, uint32_t point) {
    if (point == 0)
        throw DbError("22P05", "unsupported Unicode escape sequence: \\u0000 cannot be converted to text");
    if (point <= 0x7f) output.push_back(static_cast<char>(point));
    else if (point <= 0x7ff) {
        output.push_back(static_cast<char>(0xc0 | (point >> 6)));
        output.push_back(static_cast<char>(0x80 | (point & 0x3f)));
    } else if (point <= 0xffff) {
        output.push_back(static_cast<char>(0xe0 | (point >> 12)));
        output.push_back(static_cast<char>(0x80 | ((point >> 6) & 0x3f)));
        output.push_back(static_cast<char>(0x80 | (point & 0x3f)));
    } else {
        output.push_back(static_cast<char>(0xf0 | (point >> 18)));
        output.push_back(static_cast<char>(0x80 | ((point >> 12) & 0x3f)));
        output.push_back(static_cast<char>(0x80 | ((point >> 6) & 0x3f)));
        output.push_back(static_cast<char>(0x80 | (point & 0x3f)));
    }
}

std::string stringValue(std::string_view input, size_t& cursor) {
    if (cursor == input.size() || input[cursor++] != '"') badJson();
    std::string result;
    const auto unit = [&]() {
        if (input.size() - cursor < 4) badJson();
        uint32_t point = 0;
        for (size_t i = 0; i < 4; ++i) {
            const char ch = input[cursor++];
            const auto value = digit(ch) ? ch - '0' : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10 :
                ch >= 'A' && ch <= 'F' ? ch - 'A' + 10 : -1;
            if (value < 0) badJson();
            point = (point << 4) | static_cast<uint32_t>(value);
        }
        return point;
    };
    while (cursor < input.size()) {
        const auto byte = static_cast<unsigned char>(input[cursor++]);
        if (byte == '"') {
            if (result.size() > kMaxJsonbString)
                throw DbError("54000", "string is too long to represent as jsonb");
            return result;
        }
        if (byte < 0x20) badJson();
        if (byte == '\\') {
            if (cursor == input.size()) badJson();
            switch (input[cursor++]) {
            case '"': result.push_back('"'); break;
            case '\\': result.push_back('\\'); break;
            case '/': result.push_back('/'); break;
            case 'b': result.push_back('\b'); break;
            case 'f': result.push_back('\f'); break;
            case 'n': result.push_back('\n'); break;
            case 'r': result.push_back('\r'); break;
            case 't': result.push_back('\t'); break;
            case 'u': {
                uint32_t point = unit();
                if (point >= 0xd800 && point <= 0xdbff) {
                    if (input.size() - cursor < 6 || input[cursor] != '\\' || input[cursor + 1] != 'u')
                        badJson();
                    cursor += 2;
                    const uint32_t low = unit();
                    if (low < 0xdc00 || low > 0xdfff) badJson();
                    point = 0x10000 + ((point - 0xd800) << 10) + low - 0xdc00;
                } else if (point >= 0xdc00 && point <= 0xdfff) badJson();
                utf8(result, point);
                break;
            }
            default: badJson();
            }
        } else if (byte < 0x80) result.push_back(static_cast<char>(byte));
        else {
            // JSON strings are UTF-8 datums. Reject overlong encodings,
            // isolated continuations, surrogate encodings and > U+10FFFF.
            const size_t begin = cursor - 1;
            const size_t width = byte >= 0xc2 && byte <= 0xdf ? 2 : byte >= 0xe0 && byte <= 0xef ? 3 :
                byte >= 0xf0 && byte <= 0xf4 ? 4 : 0;
            if (!width || input.size() - begin < width)
                throw DbError("22021", "invalid byte sequence for encoding UTF8");
            uint32_t point = byte & (0x7f >> width);
            for (size_t i = 1; i < width; ++i) {
                const auto next = static_cast<unsigned char>(input[cursor++]);
                if ((next & 0xc0) != 0x80)
                    throw DbError("22021", "invalid byte sequence for encoding UTF8");
                point = (point << 6) | (next & 0x3f);
            }
            if ((width == 2 && point < 0x80) || (width == 3 && point < 0x800) ||
                (width == 4 && point < 0x10000) || point > 0x10ffff ||
                (point >= 0xd800 && point <= 0xdfff))
                throw DbError("22021", "invalid byte sequence for encoding UTF8");
            result.append(input.data() + begin, width);
        }
    }
    badJson();
}

std::string numberValue(std::string_view input, size_t& cursor) {
    const bool negative = input[cursor] == '-';
    if (negative && ++cursor == input.size()) badJson();
    std::string digits;
    if (input[cursor] == '0') {
        digits.push_back(input[cursor++]);
        if (cursor < input.size() && digit(input[cursor])) badJson();
    } else {
        if (input[cursor] < '1' || input[cursor] > '9') badJson();
        while (cursor < input.size() && digit(input[cursor])) digits.push_back(input[cursor++]);
    }
    const auto integral = static_cast<int64_t>(digits.size());
    int64_t fraction = 0;
    if (cursor < input.size() && input[cursor] == '.') {
        if (++cursor == input.size() || !digit(input[cursor])) badJson();
        while (cursor < input.size() && digit(input[cursor])) { digits.push_back(input[cursor++]); ++fraction; }
    }
    int64_t exponent = 0;
    bool exponentOverflow = false;
    if (cursor < input.size() && (input[cursor] == 'e' || input[cursor] == 'E')) {
        ++cursor;
        const bool minus = cursor < input.size() && input[cursor] == '-';
        if (cursor < input.size() && (input[cursor] == '+' || input[cursor] == '-')) ++cursor;
        if (cursor == input.size() || !digit(input[cursor])) badJson();
        while (cursor < input.size() && digit(input[cursor])) {
            const auto next = input[cursor++] - '0';
            if (!exponentOverflow) {
                exponent = exponent * 10 + next;
                exponentOverflow = exponent > kMaxDecimalExponent;
            }
        }
        if (minus) exponent = -exponent;
    }
    if (cursor < input.size() && !space(input[cursor]) && input[cursor] != ',' &&
        input[cursor] != ']' && input[cursor] != '}') badJson();
    if (exponentOverflow) numericRange();
    const int64_t scale = std::max<int64_t>(0, fraction - exponent);
    if (scale > kMaxDecimalScale) numericRange();
    const auto first = digits.find_first_not_of('0');
    if (first == std::string::npos)
        return scale ? "0." + std::string(static_cast<size_t>(scale), '0') : "0";
    const int64_t point = integral + exponent - static_cast<int64_t>(first);
    if (point > kMaxDecimalIntegerDigits) numericRange();
    digits.erase(0, first);
    std::string result = negative ? "-" : "";
    if (point <= 0) {
        result += "0.";
        result.append(static_cast<size_t>(-point), '0');
        result += digits;
    } else if (static_cast<size_t>(point) >= digits.size()) {
        result += digits;
        result.append(static_cast<size_t>(point) - digits.size(), '0');
    } else {
        result.append(digits, 0, static_cast<size_t>(point));
        result.push_back('.');
        result.append(digits, static_cast<size_t>(point), std::string::npos);
    }
    return result;
}

void quote(std::string& result, const std::string& value) {
    static const char hex[] = "0123456789abcdef";
    result.push_back('"');
    for (const auto ch : value) {
        switch (ch) {
        case '"': result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\b': result += "\\b"; break;
        case '\f': result += "\\f"; break;
        case '\n': result += "\\n"; break;
        case '\r': result += "\\r"; break;
        case '\t': result += "\\t"; break;
        default:
            if (static_cast<unsigned char>(ch) < 0x20) {
                result += "\\u00";
                result.push_back(hex[(static_cast<unsigned char>(ch) >> 4) & 15]);
                result.push_back(hex[static_cast<unsigned char>(ch) & 15]);
            } else result.push_back(ch);
        }
    }
    result.push_back('"');
}
} // namespace

bool JsonbValue::KeyLess::operator()(const std::string& left, const std::string& right) const {
    if (left.size() != right.size()) return left.size() < right.size();
    return std::lexicographical_compare(left.begin(), left.end(), right.begin(), right.end(),
        [](unsigned char a, unsigned char b) { return a < b; });
}

JsonbValue JsonbValue::parse(std::string_view input) {
    if (input.size() > kMaxJsonbText) throw DbError("54000", "jsonb input is too long");
    JsonbValue result;
    size_t cursor = 0;
    struct Frame { size_t node; bool afterValue = false, afterComma = false; };
    std::vector<Frame> frames;
    const auto whitespace = [&] { while (cursor < input.size() && space(input[cursor])) ++cursor; };
    const auto value = [&] {
        whitespace();
        if (cursor == input.size()) badJson();
        Node node;
        const char first = input[cursor];
        if (first == '{' || first == '[') {
            node.kind = first == '{' ? Kind::Object : Kind::Array;
            ++cursor;
        } else if (first == '"') {
            node.kind = Kind::String; node.scalar = stringValue(input, cursor);
        } else if (first == '-' || digit(first)) node.scalar = numberValue(input, cursor);
        else {
            const std::string_view token = first == 't' ? "true" : first == 'f' ? "false" : first == 'n' ? "null" : "";
            if (token.empty() || input.substr(cursor, token.size()) != token) badJson();
            node.scalar = std::string(token); cursor += token.size();
        }
        const auto index = result.nodes_.size();
        result.nodes_.push_back(std::move(node));
        if (first == '{' || first == '[') frames.push_back({index});
        return index;
    };
    result.root_ = value();
    while (!frames.empty()) {
        whitespace();
        if (cursor == input.size()) badJson();
        const auto parent = frames.back().node;
        const bool object = result.nodes_[parent].kind == Kind::Object;
        if (input[cursor] == (object ? '}' : ']')) {
            if (frames.back().afterComma) badJson();
            ++cursor; frames.pop_back(); continue;
        }
        if (frames.back().afterValue) {
            if (input[cursor++] != ',') badJson();
            frames.back().afterValue = false; frames.back().afterComma = true;
            continue;
        }
        std::string key;
        if (object) {
            key = stringValue(input, cursor); whitespace();
            if (cursor == input.size() || input[cursor++] != ':') badJson();
        }
        frames.back().afterValue = true; frames.back().afterComma = false;
        const auto child = value();
        if (object) result.nodes_[parent].members[std::move(key)] = child;
        else result.nodes_[parent].elements.push_back(child);
    }
    whitespace();
    if (cursor != input.size()) badJson();
    return result;
}

JsonbValue JsonbValue::concatenate(JsonbValue left, JsonbValue right) {
    if (left.root_ >= left.nodes_.size() || right.root_ >= right.nodes_.size())
        throw DbError("XX000", "jsonb value has no owned root");
    const auto offset = left.nodes_.size();
    if (offset == left.nodes_.max_size() || right.nodes_.size() > left.nodes_.max_size() - offset - 1)
        throw DbError("54000", "jsonb concatenation is too large");
    left.nodes_.reserve(offset + right.nodes_.size() + 1);
    for (auto& node : right.nodes_) {
        for (auto& element : node.elements) element += offset;
        for (auto& member : node.members) member.second += offset;
        left.nodes_.push_back(std::move(node));
    }
    auto& lhs = left.nodes_[left.root_];
    const auto rhsIndex = right.root_ + offset;
    const auto& rhs = left.nodes_[rhsIndex];
    if (lhs.kind == Kind::Object && rhs.kind == Kind::Object) {
        for (const auto& member : rhs.members) lhs.members[member.first] = member.second;
    } else {
        Node array; array.kind = Kind::Array;
        if (lhs.kind == Kind::Array) array.elements = lhs.elements;
        else array.elements.push_back(left.root_);
        if (rhs.kind == Kind::Array) array.elements.insert(array.elements.end(), rhs.elements.begin(), rhs.elements.end());
        else array.elements.push_back(rhsIndex);
        left.root_ = left.nodes_.size(); left.nodes_.push_back(std::move(array));
    }
    return left;
}

std::string JsonbValue::toString() const {
    if (root_ >= nodes_.size()) throw DbError("XX000", "jsonb value has no owned root");
    struct Frame { size_t node, next = 0; bool entered = false; Members::const_iterator member; };
    std::vector<Frame> frames;
    frames.push_back({root_, 0, false, {}});
    std::string result;
    while (!frames.empty()) {
        auto& frame = frames.back();
        const auto& node = nodes_[frame.node];
        if (!frame.entered) {
            if (node.kind == Kind::String) { quote(result, node.scalar); frames.pop_back(); continue; }
            if (node.kind == Kind::Scalar) { result += node.scalar; frames.pop_back(); continue; }
            result.push_back(node.kind == Kind::Object ? '{' : '[');
            frame.member = node.members.begin(); frame.entered = true;
        }
        const bool object = node.kind == Kind::Object;
        if (object ? frame.member == node.members.end() : frame.next == node.elements.size()) {
            result.push_back(object ? '}' : ']'); frames.pop_back(); continue;
        }
        if (frame.next) result += ", ";
        size_t child;
        if (object) {
            quote(result, frame.member->first); result += ": ";
            child = frame.member++->second;
        } else child = node.elements[frame.next];
        ++frame.next;
        frames.push_back({child, 0, false, {}});
        if (result.size() > kMaxJsonbText) throw DbError("54000", "jsonb output is too long");
    }
    if (result.size() > kMaxJsonbText) throw DbError("54000", "jsonb output is too long");
    return result;
}

} // namespace dbms
