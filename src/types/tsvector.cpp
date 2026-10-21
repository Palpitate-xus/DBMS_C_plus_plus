#include "tsvector.h"
#include "common/DbError.h"
#include "encoding_conversion.h"

#include <algorithm>
#include <cctype>
#include <map>

namespace dbms {
namespace {
constexpr uint16_t kMaxPosition = 16383;
constexpr size_t kMaxPositions = 256;
constexpr size_t kMaxLexeme = 2047;
constexpr size_t kMaxData = (size_t{1} << 20) - 1;
uint16_t position(uint16_t value) { return value & kMaxPosition; }
bool space(char ch) { return std::isspace(static_cast<unsigned char>(ch)); }
bool digit(char ch) { return ch >= '0' && ch <= '9'; }
bool less(const std::string& a, const std::string& b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(),
        [](unsigned char x, unsigned char y) { return x < y; });
}
void validText(const std::string& text) {
    std::vector<uint32_t> points;
    if (!decodeUtf8(text, points))
        throw DbError("22021", "invalid byte sequence for encoding UTF8");
}
[[noreturn]] void syntax() {
    throw DbError("42601", "syntax error in tsvector");
}
[[noreturn]] void tooLarge() {
    throw DbError("54000", "string is too long for tsvector");
}
void uniquePositions(std::vector<uint16_t>& values) {
    std::stable_sort(values.begin(), values.end(), [](uint16_t a, uint16_t b) {
        return position(a) < position(b);
    });
    size_t count = 0;
    for (uint16_t value : values) {
        if (count && position(values[count - 1]) == position(value)) {
            values[count - 1] = std::max(values[count - 1], value);
        } else {
            values[count++] = value;
            if (count == kMaxPositions || (count > 1 && position(value) == kMaxPosition)) break;
        }
    }
    values.resize(count);
}
} // namespace

void TsVectorValue::validateSize() const {
    size_t bytes = 0;
    for (const auto& entry : entries_) {
        if (entry.lexeme.size() > kMaxLexeme) tooLarge();
        bytes += entry.lexeme.size();
        if (!entry.positions.empty()) bytes = ((bytes + 1) & ~size_t{1}) + 2 + entry.positions.size() * 2;
        if (bytes > kMaxData) tooLarge();
    }
}

TsVectorValue TsVectorValue::parse(std::string_view input) {
    validText(std::string(input));
    std::map<std::string, std::vector<uint16_t>, decltype(&less)> parsed(&less);
    size_t cursor = 0, rawBytes = 0;
    while (cursor < input.size()) {
        while (cursor < input.size() && space(input[cursor])) ++cursor;
        if (cursor == input.size()) break;
        if (rawBytes > kMaxData) tooLarge();
        std::string lexeme;
        const bool quoted = input[cursor] == '\'';
        if (quoted) ++cursor;
        bool closed = !quoted;
        while (cursor < input.size()) {
            const char ch = input[cursor];
            if (ch == '\\') {
                if (++cursor == input.size()) syntax();
                lexeme += input[cursor++];
            } else if (quoted && ch == '\'') {
                ++cursor;
                if (cursor < input.size() && input[cursor] == '\'') { lexeme += '\''; ++cursor; }
                else { closed = true; break; }
            } else if (!quoted && (space(ch) || ch == ':')) break;
            else { lexeme += ch; ++cursor; }
            if (lexeme.size() >= kMaxLexeme) tooLarge();
        }
        if (!closed || lexeme.empty()) syntax();
        if (lexeme.size() >= kMaxLexeme) tooLarge();
        rawBytes += lexeme.size();
        auto& positions = parsed[lexeme];
        if (cursor < input.size() && input[cursor] == ':') {
            ++cursor;
            for (;;) {
                if (cursor == input.size() || !digit(input[cursor])) syntax();
                unsigned value = 0;
                while (cursor < input.size() && digit(input[cursor])) {
                    value = std::min<unsigned>(kMaxPosition, value * 10 + unsigned(input[cursor++] - '0'));
                }
                if (!value) syntax();
                unsigned weight = 0;
                while (cursor < input.size() && !space(input[cursor]) && input[cursor] != ',') {
                    const char ch = static_cast<char>(std::toupper(static_cast<unsigned char>(input[cursor++])));
                    if (digit(ch)) continue;
                    const unsigned next = ch == 'A' || ch == '*' ? 3 : ch == 'B' ? 2 : ch == 'C' ? 1 : 0;
                    if ((ch != 'A' && ch != 'B' && ch != 'C' && ch != 'D' && ch != '*') || weight) syntax();
                    weight = next;
                }
                positions.push_back(static_cast<uint16_t>(value | (weight << 14)));
                if (cursor == input.size() || input[cursor] != ',') break;
                ++cursor;
            }
        }
    }
    TsVectorValue result;
    for (auto& pair : parsed) {
        uniquePositions(pair.second);
        result.entries_.push_back({pair.first, std::move(pair.second)});
    }
    result.validateSize();
    return result;
}

TsVectorValue TsVectorValue::concatenate(TsVectorValue left, const TsVectorValue& right) {
    unsigned offset = 0;
    for (const auto& entry : left.entries_)
        for (uint16_t value : entry.positions) offset = std::max<unsigned>(offset, position(value));
    TsVectorValue result;
    size_t index = 0;
    for (const auto& source : right.entries_) {
        while (index < left.entries_.size() && less(left.entries_[index].lexeme, source.lexeme))
            result.entries_.push_back(std::move(left.entries_[index++]));
        Entry target{source.lexeme, {}};
        if (index < left.entries_.size() && left.entries_[index].lexeme == source.lexeme)
            target = std::move(left.entries_[index++]);
        for (uint16_t value : source.positions) {
            if (target.positions.size() == kMaxPositions ||
                (!target.positions.empty() && position(target.positions.back()) == kMaxPosition)) break;
            target.positions.push_back(static_cast<uint16_t>((value & ~kMaxPosition) |
                std::min<unsigned>(position(value) + offset, kMaxPosition)));
        }
        result.entries_.push_back(std::move(target));
    }
    while (index < left.entries_.size()) result.entries_.push_back(std::move(left.entries_[index++]));
    result.validateSize();
    return result;
}

std::string TsVectorValue::toString() const {
    std::string result;
    for (const auto& entry : entries_) {
        if (!result.empty()) result += ' ';
        result += '\'';
        for (char ch : entry.lexeme) {
            if (ch == '\'' || ch == '\\') result += ch;
            result += ch;
        }
        result += '\'';
        for (size_t i = 0; i < entry.positions.size(); ++i) {
            result += i ? ',' : ':';
            const auto value = entry.positions[i];
            result += std::to_string(position(value));
            if (value >> 14) result += "DCBA"[value >> 14];
        }
    }
    return result;
}

std::vector<uint8_t> TsVectorValue::toBinary() const {
    std::vector<uint8_t> result;
    const auto number = [&](uint32_t value, unsigned width) {
        while (width) result.push_back(static_cast<uint8_t>(value >> (--width * 8)));
    };
    number(static_cast<uint32_t>(entries_.size()), 4);
    for (const auto& entry : entries_) {
        result.insert(result.end(), entry.lexeme.begin(), entry.lexeme.end());
        result.push_back(0);
        number(static_cast<uint32_t>(entry.positions.size()), 2);
        for (uint16_t value : entry.positions) number(value, 2);
    }
    return result;
}

TsVectorValue TsVectorValue::fromBinary(const std::vector<uint8_t>& input) {
    size_t cursor = 0;
    const auto number = [&](unsigned width) {
        if (width > input.size() - cursor) throw DbError("08P01", "insufficient data left in message");
        uint32_t value = 0;
        while (width--) value = (value << 8) | input[cursor++];
        return value;
    };
    const auto count = number(4);
    if (count > kMaxData) throw DbError("XX000", "invalid size of tsvector");
    TsVectorValue result;
    for (uint32_t i = 0; i < count; ++i) {
        const auto end = std::find(input.begin() + cursor, input.end(), 0);
        if (end == input.end()) throw DbError("08P01", "invalid string in message");
        Entry entry{std::string(input.begin() + cursor, end), {}};
        cursor = static_cast<size_t>(end - input.begin()) + 1;
        validText(entry.lexeme);
        const auto size = number(2);
        if (entry.lexeme.empty() || entry.lexeme.size() > kMaxLexeme || size > kMaxPositions)
            throw DbError("XX000", "invalid lexeme or positions in tsvector");
        for (uint32_t j = 0; j < size; ++j) {
            const auto value = static_cast<uint16_t>(number(2));
            if (!entry.positions.empty() && position(value) <= position(entry.positions.back()))
                throw DbError("XX000", "position information is misordered");
            entry.positions.push_back(value);
        }
        result.entries_.push_back(std::move(entry));
    }
    if (cursor != input.size()) throw DbError("22P03", "incorrect binary data format in tsvector");
    std::stable_sort(result.entries_.begin(), result.entries_.end(), [](const Entry& a, const Entry& b) { return less(a.lexeme, b.lexeme); });
    result.validateSize();
    return result;
}

} // namespace dbms
