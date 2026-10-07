#pragma once

#include "common/DbError.h"
#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <locale.h>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include <wctype.h>
#include <unicode/utf8.h>

namespace dbms::sql_pattern {
using Character = uint32_t;
using Characters = std::vector<Character>;

inline Characters characters(const std::string& value, bool bytes = false) {
    Characters result;
    for (int32_t offset = 0; offset < static_cast<int32_t>(value.size());) {
        UChar32 character;
        if (bytes) character = static_cast<unsigned char>(value[offset++]);
        else U8_NEXT(value.data(), offset, static_cast<int32_t>(value.size()), character);
        if (character < 0) throw DbError("22021", "invalid UTF8 pattern input");
        result.push_back(static_cast<Character>(character));
    }
    return result;
}

inline locale_t unicodeLocale(bool asciiOnly = false) {
    struct Locale {
        locale_t value;
        explicit Locale(const char* name) : value(newlocale(LC_CTYPE_MASK,name,nullptr)) {
            if (!value) throw DbError("42704", "pattern locale is unavailable");
        }
        ~Locale() { freelocale(value); }
    };
    static const Locale utf8("C.UTF-8"), ascii("C");
    return asciiOnly ? ascii.value : utf8.value;
}

inline Character lower(Character character, bool asciiOnly) {
    if (asciiOnly) return character >= 'A' && character <= 'Z' ? character + 32 : character;
    return static_cast<Character>(towlower_l(character, unicodeLocale()));
}

// LIKE has three results internally. ABORT means that no later suffix of the
// remaining text can match; this preserves PostgreSQL's demand for escapes.
inline bool like(const std::string& text, const std::string& pattern,
                 const std::string& escape = "\\", bool insensitive = false,
                 bool bytes = false, bool asciiOnly = false) {
    auto input = characters(text, bytes);
    const auto source = characters(pattern, bytes), escapes = characters(escape, bytes);
    if (escapes.size() > 1) throw DbError("22025", "invalid escape string");
    enum Kind { Literal, Any, Many, TrailingEscape };
    struct Token { Kind kind; Character character = 0; };
    std::vector<Token> tokens;
    for (size_t i = 0; i < source.size(); ++i) {
        if (!escapes.empty() && source[i] == escapes[0]) {
            if (++i == source.size()) { tokens.push_back({TrailingEscape}); break; }
            tokens.push_back({Literal,source[i]});
        } else if (source[i] == '%') tokens.push_back({Many});
        else if (source[i] == '_') tokens.push_back({Any});
        else tokens.push_back({Literal,source[i]});
    }
    if (insensitive) {
        for (auto& character : input) character = lower(character, asciiOnly);
        for (auto& token : tokens) if (token.kind == Literal)
            token.character = lower(token.character, asciiOnly);
    }
    enum Result { False, True, Abort };
    const auto invalidEscape = []() -> void {
        throw DbError("22025", "LIKE pattern must not end with escape character");
    };
    std::function<Result(size_t,size_t)> match = [&](size_t t, size_t p) -> Result {
        while (t < input.size() && p < tokens.size()) {
            if (tokens[p].kind == Many) {
                ++p;
                while (p < tokens.size()) {
                    if (tokens[p].kind == Many) ++p;
                    else if (tokens[p].kind == Any) {
                        if (t == input.size()) return Abort;
                        ++t; ++p;
                    } else break;
                }
                if (p == tokens.size()) return True;
                if (tokens[p].kind == TrailingEscape) invalidEscape();
                while (t < input.size()) {
                    if (input[t] == tokens[p].character) {
                        const Result result = match(t,p);
                        if (result != False) return result;
                    }
                    ++t;
                }
                return Abort;
            }
            if (tokens[p].kind == TrailingEscape) invalidEscape();
            if (tokens[p].kind != Any && input[t] != tokens[p].character) return False;
            ++t; ++p;
        }
        if (t < input.size()) return False;
        while (p < tokens.size() && tokens[p].kind == Many) ++p;
        return p == tokens.size() ? True : Abort;
    };
    return match(0,0) == True;
}

[[noreturn]] inline void invalidRegex() {
    throw DbError("2201B", "invalid regular expression");
}

inline bool classMember(Character character, const std::string& name, bool asciiOnly = false) {
    const auto locale = unicodeLocale(asciiOnly);
    if (name == "ascii") return character < 128;
    if (name == "word") return character == '_' || iswalnum_l(character,locale);
    const auto category = wctype_l(name.c_str(),locale);
    if (!category) invalidRegex();
    return iswctype_l(character,category,locale);
}

struct ClassPart {
    Character first = 0, last = 0;
    std::string category;
    bool complement = false;
    bool matches(Character character, bool asciiOnly) const {
        const bool member = category.empty() ? character >= first && character <= last
                                             : classMember(character,category,asciiOnly);
        return complement ? !member : member;
    }
};

struct Node {
    enum Kind { Empty, Literal, Any, Class, Sequence, Alternative, Repeat, Assertion,
                Capture, Backreference } kind = Empty;
    Character character = 0;
    bool complement = false;
    size_t minimum = 0, maximum = 0;
    std::vector<ClassPart> members;
    std::vector<std::shared_ptr<Node>> children;
};
using Pattern = std::shared_ptr<Node>;

// Parse the SQL pattern itself: '.', '^' and '$' are literal, '%' and '_'
// are SQL wildcards, and bracket classes/groups/alternation/repetition own
// their grammar. No ECMAScript or byte-regex interpretation is involved.
class SimilarParser {
    struct Symbol { Character character; bool escaped; };
    std::vector<Symbol> symbols_;
    size_t position_ = 0;
    bool captureClosed_ = false;
    static constexpr size_t unbounded = std::numeric_limits<size_t>::max();
    bool at(Character character) const {
        return position_ < symbols_.size() && !symbols_[position_].escaped &&
               symbols_[position_].character == character;
    }
    bool separator() const {
        return position_ < symbols_.size() && symbols_[position_].escaped &&
               symbols_[position_].character == '"';
    }
    Pattern node(Node::Kind kind) { auto result = std::make_shared<Node>(); result->kind = kind; return result; }
    ClassPart escapePart(Character character) {
        if (character == 'd' || character == 'D') return {0,0,"digit",character == 'D'};
        if (character == 's' || character == 'S') return {0,0,"space",character == 'S'};
        if (character == 'w' || character == 'W') return {0,0,"word",character == 'W'};
        switch (character) {
        case 'a': character = 7; break; case 'b': character = 8; break;
        case 'B': character = '\\'; break; case 'e': character = 27; break;
        case 'f': character = 12; break; case 'n': character = 10; break;
        case 'r': character = 13; break; case 't': character = 9; break;
        case 'v': character = 11; break;
        case 'c':
            if (position_ == symbols_.size()) invalidRegex();
            character = symbols_[position_++].character & 31; break;
        case 'u': case 'U': case 'x': {
            const size_t minimum = character == 'x' ? 1 : character == 'u' ? 4 : 8;
            const size_t maximum = character == 'x' ? 255 : minimum;
            Character value = 0; size_t count = 0;
            while (position_ < symbols_.size() && count < maximum) {
                const Character next = symbols_[position_].character;
                const int digit = next >= '0' && next <= '9' ? next-'0'
                    : next >= 'a' && next <= 'f' ? next-'a'+10
                    : next >= 'A' && next <= 'F' ? next-'A'+10 : -1;
                if (digit < 0 || symbols_[position_].escaped) break;
                if (value > (0x10ffffU - digit) / 16) invalidRegex();
                value = value * 16 + digit; ++count; ++position_;
            }
            if (count < minimum || (value >= 0xd800 && value <= 0xdfff)) invalidRegex();
            character = value; break;
        }
        default:
            if (character >= '0' && character <= '7') {
                if (character != '0' && (position_ == symbols_.size() ||
                    symbols_[position_].character < '0' || symbols_[position_].character > '7'))
                    invalidRegex(); // no capturing parentheses in SQL groups
                Character value = character - '0'; size_t count = 1;
                while (position_ < symbols_.size() && count < 3 &&
                       !symbols_[position_].escaped && symbols_[position_].character >= '0' &&
                       symbols_[position_].character <= '7') {
                    value = value * 8 + symbols_[position_++].character - '0'; ++count;
                }
                if (value > 255) { --position_; value >>= 3; }
                character = value;
            } else if ((character >= 'a' && character <= 'z') ||
                       (character >= 'A' && character <= 'Z') ||
                       (character >= '0' && character <= '9')) invalidRegex();
        }
        return {character,character,{},false};
    }
    ClassPart classPart() {
        if (position_ == symbols_.size()) invalidRegex();
        const auto symbol = symbols_[position_++];
        if (symbol.escaped) return escapePart(symbol.character);
        if (symbol.character == '[' && position_ < symbols_.size() &&
            (at(':') || at('.') || at('='))) {
            const Character delimiter = symbols_[position_++].character;
            Characters contents;
            while (position_ < symbols_.size() && !at(delimiter))
                contents.push_back(symbols_[position_++].character);
            if (position_ == symbols_.size()) invalidRegex();
            ++position_;
            if (!at(']')) invalidRegex();
            ++position_;
            if (delimiter != ':') {
                if (contents.size() != 1) invalidRegex();
                return {contents[0],contents[0],{},false};
            }
            std::string category;
            for (const auto character : contents) {
                if (character > 127) invalidRegex();
                category += static_cast<char>(character);
            }
            if (category != "ascii" && category != "word" &&
                !wctype_l(category.c_str(),unicodeLocale())) invalidRegex();
            return {0,0,std::move(category),false};
        }
        return {symbol.character,symbol.character,{},false};
    }
    Pattern characterClass() {
        auto result = node(Node::Class);
        if (at('^')) { result->complement = true; ++position_; }
        bool first = true;
        while (position_ < symbols_.size() && (first || !at(']'))) {
            auto member = classPart(); first = false;
            if (at('-') && position_+1 < symbols_.size() &&
                (symbols_[position_+1].escaped || symbols_[position_+1].character != ']')) {
                ++position_; const auto last = classPart();
                if (!member.category.empty() || !last.category.empty() || member.first > last.first)
                    invalidRegex();
                member.last = last.first;
            }
            result->members.push_back(std::move(member));
        }
        if (!at(']') || result->members.empty()) invalidRegex();
        ++position_;
        return result;
    }
    size_t bound() {
        size_t result = 0; bool present = false;
        while (position_ < symbols_.size() && !symbols_[position_].escaped &&
               symbols_[position_].character >= '0' && symbols_[position_].character <= '9') {
            present = true; result = result * 10 + symbols_[position_++].character - '0';
            if (result > 255) invalidRegex();
        }
        if (!present) invalidRegex();
        return result;
    }
    bool interval() const {
        return at('{') && position_+1 < symbols_.size() && !symbols_[position_+1].escaped &&
            symbols_[position_+1].character >= '0' && symbols_[position_+1].character <= '9';
    }
    Pattern piece() {
        if (at('*') || at('+') || at('?') || interval()) invalidRegex();
        const auto symbol = symbols_[position_++];
        Pattern result;
        if (symbol.escaped) {
            size_t decimalEnd = position_;
            size_t decimalValue = symbol.character >= '1' && symbol.character <= '9'
                ? symbol.character - '0' : 0;
            if (decimalValue) {
                while (decimalEnd < symbols_.size() && !symbols_[decimalEnd].escaped &&
                       symbols_[decimalEnd].character >= '0' && symbols_[decimalEnd].character <= '9') {
                    decimalValue = std::min<size_t>(256,decimalValue * 10 + symbols_[decimalEnd++].character - '0');
                }
            }
            if (decimalValue && (decimalEnd == position_ || decimalValue == 1)) {
                if (!captureClosed_ || decimalValue != 1) invalidRegex();
                position_ = decimalEnd; result = node(Node::Backreference);
            } else if (symbol.character == 'm' || symbol.character == 'M' ||
                symbol.character == 'y' || symbol.character == 'Y' ||
                symbol.character == 'A' || symbol.character == 'Z') {
                result = node(Node::Assertion); result->character = symbol.character;
            } else {
                const auto member = escapePart(symbol.character);
                result = node(member.category.empty() ? Node::Literal : Node::Class);
                result->character = member.first; result->members.push_back(member);
            }
        } else if (symbol.character == '(') {
            result = alternative();
            if (!at(')')) invalidRegex();
            ++position_;
        } else if (symbol.character == '[') result = characterClass();
        else if (symbol.character == '_' || symbol.character == '%') result = node(Node::Any);
        else { result = node(Node::Literal); result->character = symbol.character; }
        bool quantified = !symbol.escaped && symbol.character == '%';
        size_t minimum = 0, maximum = unbounded;
        if (!quantified && (at('*') || at('+') || at('?') || interval())) {
            if (result->kind == Node::Assertion) invalidRegex();
            quantified = true;
            if (at('*')) ++position_;
            else if (at('+')) { minimum = 1; ++position_; }
            else if (at('?')) { maximum = 1; ++position_; }
            else {
                ++position_; minimum = bound(); maximum = minimum;
                if (at(',')) {
                    ++position_; maximum = at('}') ? unbounded : bound();
                }
                if (!at('}') || maximum < minimum) invalidRegex();
                ++position_;
            }
        }
        if (quantified) {
            auto repeated = node(Node::Repeat); repeated->children.push_back(std::move(result));
            repeated->minimum = minimum; repeated->maximum = maximum; result = std::move(repeated);
            if (at('?')) ++position_; // greediness does not change the match language
            if (at('*') || at('+') || at('?') || interval()) invalidRegex();
        }
        return result;
    }
    Pattern sequence() {
        auto result = node(Node::Sequence);
        while (position_ < symbols_.size() && !at('|') && !at(')') && !separator())
            result->children.push_back(piece());
        return result;
    }
    Pattern alternative() {
        auto result = node(Node::Alternative); result->children.push_back(sequence());
        while (at('|')) { ++position_; result->children.push_back(sequence()); }
        return result;
    }
public:
    SimilarParser(const std::string& pattern, const std::string& escape) {
        const auto source = characters(pattern), escapes = characters(escape);
        if (escapes.size() > 1) throw DbError("22025", "invalid escape string");
        for (size_t i = 0; i < source.size(); ++i) {
            if (!escapes.empty() && source[i] == escapes[0]) {
                if (++i == source.size()) break; // SIMILAR's trailing SQL escape is omitted
                symbols_.push_back({source[i],true});
            } else symbols_.push_back({source[i],false});
        }
    }
    Pattern parse() {
        auto result = node(Node::Sequence); result->children.push_back(alternative());
        size_t separators = 0;
        while (separator()) {
            if (++separators > 2) throw DbError("2200B", "too many SQL pattern quote separators");
            ++position_;
            if (separators == 1) {
                auto captured = node(Node::Capture); captured->children.push_back(alternative());
                result->children.push_back(std::move(captured));
            } else {
                captureClosed_ = true; result->children.push_back(alternative());
            }
        }
        if (position_ != symbols_.size()) invalidRegex();
        return result;
    }
};

inline bool similar(const std::string& text, const std::string& pattern,
                    const std::string& escape = "\\", bool asciiOnly = false) {
    const auto input = characters(text);
    const auto root = SimilarParser(pattern,escape).parse();
    struct Position {
        size_t offset, captureBegin = std::numeric_limits<size_t>::max(), captureEnd = 0;
        bool operator<(const Position& other) const {
            return std::tie(offset,captureBegin,captureEnd) <
                   std::tie(other.offset,other.captureBegin,other.captureEnd);
        }
        bool operator==(const Position& other) const {
            return offset == other.offset && captureBegin == other.captureBegin && captureEnd == other.captureEnd;
        }
    };
    using Positions = std::vector<Position>;
    const auto deduplicate = [](Positions& positions) {
        std::sort(positions.begin(),positions.end());
        positions.erase(std::unique(positions.begin(),positions.end()),positions.end());
    };
    std::map<std::pair<const Node*,Position>,Positions> memo;
    std::function<Positions(const Pattern&,Position)> evaluate;
    evaluate = [&](const Pattern& node, Position state) -> Positions {
        const size_t begin = state.offset;
        const auto key = std::make_pair(node.get(),state);
        if (const auto found = memo.find(key); found != memo.end()) return found->second;
        Positions result;
        switch (node->kind) {
        case Node::Empty: result.push_back(state); break;
        case Node::Literal: case Node::Any: case Node::Class:
            if (begin < input.size()) {
                bool matches = node->kind == Node::Any ||
                    (node->kind == Node::Literal && node->character == input[begin]);
                if (node->kind == Node::Class) {
                    for (const auto& member : node->members) matches = matches || member.matches(input[begin],asciiOnly);
                    if (node->complement) matches = !matches;
                }
                if (matches) { ++state.offset; result.push_back(state); }
            }
            break;
        case Node::Assertion: {
            const bool left = begin > 0 && classMember(input[begin-1],"word",asciiOnly);
            const bool right = begin < input.size() && classMember(input[begin],"word",asciiOnly);
            const bool matches = node->character == 'm' ? !left && right
                : node->character == 'M' ? left && !right
                : node->character == 'y' ? left != right
                : node->character == 'Y' ? left == right
                : node->character == 'A' ? begin == 0 : begin == input.size();
            if (matches) result.push_back(state);
            break;
        }
        case Node::Capture:
            result = evaluate(node->children[0],state);
            for (auto& ending : result) { ending.captureBegin = begin; ending.captureEnd = ending.offset; }
            break;
        case Node::Backreference:
            if (state.captureBegin <= state.captureEnd && state.captureEnd <= input.size()) {
                const size_t length = state.captureEnd - state.captureBegin;
                if (length <= input.size() - begin &&
                    std::equal(input.begin()+state.captureBegin,input.begin()+state.captureEnd,input.begin()+begin)) {
                    state.offset += length; result.push_back(state);
                }
            }
            break;
        case Node::Alternative:
            for (const auto& child : node->children) {
                const auto positions = evaluate(child,state);
                result.insert(result.end(),positions.begin(),positions.end());
            }
            break;
        case Node::Sequence:
            result = {state};
            for (const auto& child : node->children) {
                Positions next;
                for (const auto position : result) {
                    const auto positions = evaluate(child,position);
                    next.insert(next.end(),positions.begin(),positions.end());
                }
                deduplicate(next); result = std::move(next);
                if (result.empty()) break;
            }
            break;
        case Node::Repeat: {
            Positions current{state};
            if (node->minimum == 0) result = current;
            const size_t maximum = node->maximum == std::numeric_limits<size_t>::max()
                ? input.size() + node->minimum + 1 : node->maximum;
            for (size_t repetition = 1; repetition <= maximum; ++repetition) {
                Positions next;
                for (const auto position : current) {
                    const auto positions = evaluate(node->children[0],position);
                    next.insert(next.end(),positions.begin(),positions.end());
                }
                deduplicate(next);
                if (repetition >= node->minimum) result.insert(result.end(),next.begin(),next.end());
                if (next.empty() || (next == current && repetition >= node->minimum)) break;
                current = std::move(next);
            }
            break;
        }
        }
        deduplicate(result); memo.emplace(key,result); return result;
    };
    const auto endings = evaluate(root,{0});
    return std::any_of(endings.begin(),endings.end(),[&](const Position& ending) {
        return ending.offset == input.size();
    });
}
} // namespace dbms::sql_pattern
