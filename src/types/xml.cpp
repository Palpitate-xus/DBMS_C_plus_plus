#include "types/xml.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <map>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

namespace dbms {
namespace {

constexpr std::string_view kXmlNamespace =
    "http://www.w3.org/XML/1998/namespace";
constexpr std::string_view kXmlnsNamespace =
    "http://www.w3.org/2000/xmlns/";

bool asciiIEquals(std::string_view left, std::string_view right) {
    if (left.size() != right.size()) return false;
    for (size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) !=
            std::tolower(static_cast<unsigned char>(right[i]))) return false;
    }
    return true;
}

bool isXmlChar(uint32_t codePoint) {
    return codePoint == 0x9 || codePoint == 0xa || codePoint == 0xd ||
           (codePoint >= 0x20 && codePoint <= 0xd7ff) ||
           (codePoint >= 0xe000 && codePoint <= 0xfffd) ||
           (codePoint >= 0x10000 && codePoint <= 0x10ffff);
}

bool isXmlSpace(char value) {
    return value == ' ' || value == '\t' || value == '\r' || value == '\n';
}

bool decodeUtf8(std::string_view text, size_t& position, uint32_t& codePoint) {
    if (position >= text.size()) return false;
    const auto first = static_cast<unsigned char>(text[position]);
    if (first < 0x80) {
        codePoint = first;
        ++position;
        return true;
    }
    size_t continuationCount = 0;
    uint32_t minimum = 0;
    if (first >= 0xc2 && first <= 0xdf) {
        continuationCount = 1;
        codePoint = first & 0x1f;
        minimum = 0x80;
    } else if (first >= 0xe0 && first <= 0xef) {
        continuationCount = 2;
        codePoint = first & 0x0f;
        minimum = 0x800;
    } else if (first >= 0xf0 && first <= 0xf4) {
        continuationCount = 3;
        codePoint = first & 0x07;
        minimum = 0x10000;
    } else {
        return false;
    }
    if (position + continuationCount >= text.size()) return false;
    for (size_t i = 1; i <= continuationCount; ++i) {
        const auto next = static_cast<unsigned char>(text[position + i]);
        if ((next & 0xc0) != 0x80) return false;
        codePoint = (codePoint << 6) | (next & 0x3f);
    }
    position += continuationCount + 1;
    return codePoint >= minimum && codePoint <= 0x10ffff &&
           !(codePoint >= 0xd800 && codePoint <= 0xdfff);
}

bool isNameStart(uint32_t codePoint) {
    // XML 1.0 permits a broad Unicode name set. Accept valid non-ASCII XML
    // characters and enforce the precise ASCII exclusions that matter for SQL
    // input, rather than treating signed UTF-8 bytes as letters.
    return codePoint == ':' || codePoint == '_' ||
           (codePoint >= 'A' && codePoint <= 'Z') ||
           (codePoint >= 'a' && codePoint <= 'z') || codePoint >= 0x80;
}

bool isNameChar(uint32_t codePoint) {
    return isNameStart(codePoint) || codePoint == '-' || codePoint == '.' ||
           (codePoint >= '0' && codePoint <= '9') || codePoint == 0xb7;
}

class XmlParser {
public:
    XmlParser(const std::string& input, XmlParseMode mode)
        : input_(input), mode_(mode) {}

    XmlValidationResult parse() {
        if (input_.size() >= 3 &&
            static_cast<unsigned char>(input_[0]) == 0xef &&
            static_cast<unsigned char>(input_[1]) == 0xbb &&
            static_cast<unsigned char>(input_[2]) == 0xbf) {
            position_ = 3;
        }
        if (!validateCharacters(std::string_view(input_).substr(position_)))
            return failure();
        if (startsWith("<?xml") &&
            (position_ + 5 == input_.size() ||
             isXmlSpace(input_[position_ + 5]))) {
            if (!parseDeclaration()) return failure();
        }

        while (position_ < input_.size()) {
            if (input_[position_] != '<') {
                if (!parseText()) return failure();
                continue;
            }
            if (startsWith("<!--")) {
                if (!parseComment()) return failure();
            } else if (startsWith("<![CDATA[")) {
                if (!parseCdata()) return failure();
            } else if (startsWith("<?")) {
                if (!parseProcessingInstruction()) return failure();
            } else if (startsWith("</")) {
                if (!parseEndTag()) return failure();
            } else if (startsWith("<!")) {
                return fail("DTD and markup declarations are not supported");
            } else {
                if (!parseStartTag()) return failure();
            }
        }
        if (!elements_.empty()) return fail("unclosed element");
        if (mode_ == XmlParseMode::Document && rootCount_ != 1)
            return fail("XML document must contain exactly one root element");
        return {true, {}};
    }

private:
    struct Element {
        std::string name;
        std::map<std::string, std::string> namespaces;
    };

    XmlValidationResult failure() const { return {false, error_}; }
    XmlValidationResult fail(std::string message) {
        error_ = std::move(message);
        return failure();
    }

    bool startsWith(std::string_view token) const {
        return position_ + token.size() <= input_.size() &&
               std::equal(token.begin(), token.end(), input_.begin() + position_);
    }

    void skipSpace() {
        while (position_ < input_.size() && isXmlSpace(input_[position_])) {
            ++position_;
        }
    }

    bool parseName(std::string& name) {
        const size_t begin = position_;
        uint32_t codePoint = 0;
        size_t next = position_;
        if (!decodeUtf8(input_, next, codePoint) || !isNameStart(codePoint))
            return false;
        position_ = next;
        while (position_ < input_.size()) {
            next = position_;
            if (!decodeUtf8(input_, next, codePoint) || !isNameChar(codePoint))
                break;
            position_ = next;
        }
        name = input_.substr(begin, position_ - begin);
        const size_t colon = name.find(':');
        return colon == std::string::npos ||
               (colon != 0 && colon + 1 < name.size() &&
                name.find(':', colon + 1) == std::string::npos);
    }

    bool validateCharacters(std::string_view text) {
        size_t position = 0;
        while (position < text.size()) {
            uint32_t codePoint = 0;
            if (!decodeUtf8(text, position, codePoint) || !isXmlChar(codePoint)) {
                error_ = "invalid XML character or UTF-8 sequence";
                return false;
            }
        }
        return true;
    }

    bool validateReference(std::string_view reference) {
        if (reference == "lt" || reference == "gt" || reference == "amp" ||
            reference == "apos" || reference == "quot") return true;
        if (reference.size() < 2 || reference.front() != '#') return false;
        bool hex = reference.size() >= 3 &&
                   (reference[1] == 'x' || reference[1] == 'X');
        size_t offset = hex ? 2 : 1;
        if (offset == reference.size()) return false;
        uint32_t value = 0;
        for (; offset < reference.size(); ++offset) {
            const char digit = reference[offset];
            unsigned part = 0;
            if (digit >= '0' && digit <= '9') part = digit - '0';
            else if (hex && digit >= 'a' && digit <= 'f') part = digit - 'a' + 10;
            else if (hex && digit >= 'A' && digit <= 'F') part = digit - 'A' + 10;
            else return false;
            const unsigned base = hex ? 16U : 10U;
            if (value > (0x10ffffU - part) / base) return false;
            value = value * base + part;
        }
        return isXmlChar(value);
    }

    bool validateTextRange(size_t begin, size_t end, bool attribute) {
        if (!validateCharacters(std::string_view(input_).substr(begin, end - begin)))
            return false;
        for (size_t i = begin; i < end; ++i) {
            if (input_[i] == '&') {
                const size_t semicolon = input_.find(';', i + 1);
                if (semicolon == std::string::npos || semicolon >= end ||
                    !validateReference(std::string_view(input_).substr(
                        i + 1, semicolon - i - 1))) {
                    error_ = "invalid or unrecognized entity reference";
                    return false;
                }
                i = semicolon;
            } else if (attribute && input_[i] == '<') {
                error_ = "unescaped '<' in attribute value";
                return false;
            }
        }
        return true;
    }

    bool parseDeclaration() {
        const size_t declarationStart = position_;
        position_ += 5;
        if (position_ >= input_.size() ||
            !isXmlSpace(input_[position_])) {
            error_ = "invalid XML declaration";
            return false;
        }
        std::vector<std::pair<std::string, std::string>> attributes;
        while (true) {
            skipSpace();
            if (startsWith("?>")) {
                position_ += 2;
                break;
            }
            std::string name;
            if (!parseName(name)) {
                error_ = "invalid XML declaration";
                return false;
            }
            skipSpace();
            if (position_ >= input_.size() || input_[position_++] != '=') {
                error_ = "invalid XML declaration";
                return false;
            }
            skipSpace();
            if (position_ >= input_.size() ||
                (input_[position_] != '\'' && input_[position_] != '"')) {
                error_ = "invalid XML declaration";
                return false;
            }
            const char quote = input_[position_++];
            const size_t valueBegin = position_;
            const size_t end = input_.find(quote, position_);
            if (end == std::string::npos || !validateCharacters(
                    std::string_view(input_).substr(valueBegin, end - valueBegin))) {
                error_ = "invalid XML declaration";
                return false;
            }
            attributes.emplace_back(std::move(name),
                                    input_.substr(valueBegin, end - valueBegin));
            position_ = end + 1;
        }
        if (attributes.empty() || attributes[0].first != "version" ||
            (attributes[0].second != "1.0" && attributes[0].second != "1.1")) {
            error_ = "XML declaration requires version 1.0 or 1.1 first";
            return false;
        }
        bool sawEncoding = false;
        bool sawStandalone = false;
        for (size_t i = 1; i < attributes.size(); ++i) {
            const auto& [name, value] = attributes[i];
            if (name == "encoding" && !sawEncoding && !sawStandalone) {
                sawEncoding = true;
                if (!asciiIEquals(value, "utf-8") && !asciiIEquals(value, "utf8")) {
                    error_ = "only UTF-8 XML declarations are supported";
                    return false;
                }
            } else if (name == "standalone" && !sawStandalone &&
                       (value == "yes" || value == "no")) {
                sawStandalone = true;
            } else {
                error_ = "invalid XML declaration attribute order or value";
                return false;
            }
        }
        (void)declarationStart;
        return true;
    }

    bool parseText() {
        const size_t begin = position_;
        while (position_ < input_.size() && input_[position_] != '<')
            ++position_;
        if (input_.find("]]>", begin) < position_) {
            error_ = "']]>' is not allowed in character data";
            return false;
        }
        if (!validateTextRange(begin, position_, false)) return false;
        if (mode_ == XmlParseMode::Document && elements_.empty()) {
            const bool whitespaceOnly = std::all_of(
                input_.begin() + begin, input_.begin() + position_,
                [](char c) { return isXmlSpace(c); });
            if (!whitespaceOnly) {
                error_ = "character data is not allowed outside the document element";
                return false;
            }
        }
        return true;
    }

    bool parseComment() {
        const size_t begin = position_ + 4;
        const size_t end = input_.find("-->", begin);
        if (end == std::string::npos || input_.find("--", begin) < end ||
            !validateCharacters(std::string_view(input_).substr(begin, end - begin))) {
            error_ = "invalid XML comment";
            return false;
        }
        position_ = end + 3;
        return true;
    }

    bool parseCdata() {
        const size_t begin = position_ + 9;
        const size_t end = input_.find("]]>", begin);
        if (end == std::string::npos ||
            !validateCharacters(std::string_view(input_).substr(begin, end - begin))) {
            error_ = "invalid CDATA section";
            return false;
        }
        if (mode_ == XmlParseMode::Document && elements_.empty()) {
            error_ = "CDATA is not allowed outside the document element";
            return false;
        }
        position_ = end + 3;
        return true;
    }

    bool parseProcessingInstruction() {
        position_ += 2;
        std::string target;
        if (!parseName(target) || asciiIEquals(target, "xml")) {
            error_ = "invalid processing instruction target";
            return false;
        }
        const size_t end = input_.find("?>", position_);
        if (end == std::string::npos ||
            (position_ < end && !isXmlSpace(input_[position_])) ||
            !validateCharacters(std::string_view(input_).substr(
                position_, end - position_))) {
            error_ = "invalid processing instruction";
            return false;
        }
        position_ = end + 2;
        return true;
    }

    static std::string prefixOf(const std::string& name) {
        const size_t colon = name.find(':');
        return colon == std::string::npos ? std::string{} : name.substr(0, colon);
    }

    bool validateNamespaceName(const std::string& name,
                               const std::map<std::string, std::string>& namespaces,
                               bool attribute) {
        const std::string prefix = prefixOf(name);
        if (prefix.empty()) return true;
        if (prefix == "xmlns") return attribute;
        if (namespaces.find(prefix) == namespaces.end()) {
            error_ = "unbound namespace prefix '" + prefix + "'";
            return false;
        }
        return true;
    }

    bool parseStartTag() {
        ++position_;
        std::string elementName;
        if (!parseName(elementName)) {
            error_ = "invalid element name";
            return false;
        }
        std::map<std::string, std::string> namespaces = elements_.empty()
            ? std::map<std::string, std::string>{{"xml", std::string(kXmlNamespace)}}
            : elements_.back().namespaces;
        std::vector<std::pair<std::string, std::string>> attributes;
        std::set<std::string> attributeNames;
        bool selfClosing = false;
        while (true) {
            const size_t beforeSpace = position_;
            skipSpace();
            if (position_ >= input_.size()) {
                error_ = "unterminated start tag";
                return false;
            }
            if (input_[position_] == '>') {
                ++position_;
                break;
            }
            if (input_[position_] == '/' && position_ + 1 < input_.size() &&
                input_[position_ + 1] == '>') {
                position_ += 2;
                selfClosing = true;
                break;
            }
            if (position_ == beforeSpace) {
                error_ = "attributes must be separated by whitespace";
                return false;
            }
            std::string name;
            if (!parseName(name) || !attributeNames.insert(name).second) {
                error_ = "invalid or duplicate attribute name";
                return false;
            }
            skipSpace();
            if (position_ >= input_.size() || input_[position_++] != '=') {
                error_ = "attribute value must follow '='";
                return false;
            }
            skipSpace();
            if (position_ >= input_.size() ||
                (input_[position_] != '\'' && input_[position_] != '"')) {
                error_ = "attribute value must be quoted";
                return false;
            }
            const char quote = input_[position_++];
            const size_t begin = position_;
            const size_t end = input_.find(quote, position_);
            if (end == std::string::npos || !validateTextRange(begin, end, true))
                return false;
            attributes.emplace_back(std::move(name),
                                    input_.substr(begin, end - begin));
            position_ = end + 1;
        }

        for (const auto& [name, value] : attributes) {
            if (name == "xmlns") {
                if (value == kXmlnsNamespace) {
                    error_ = "the XMLNS namespace cannot be the default namespace";
                    return false;
                }
                namespaces[""] = value;
            } else if (name.rfind("xmlns:", 0) == 0) {
                const std::string prefix = name.substr(6);
                if (prefix.empty() || prefix == "xmlns" || value.empty() ||
                    value == kXmlnsNamespace ||
                    (prefix == "xml" && value != kXmlNamespace) ||
                    (prefix != "xml" && value == kXmlNamespace)) {
                    error_ = "invalid namespace declaration";
                    return false;
                }
                namespaces[prefix] = value;
            }
        }
        if (!validateNamespaceName(elementName, namespaces, false)) return false;
        for (const auto& [name, value] : attributes) {
            (void)value;
            if (name == "xmlns" || name.rfind("xmlns:", 0) == 0) continue;
            if (!validateNamespaceName(name, namespaces, true)) return false;
        }

        if (elements_.empty()) {
            ++rootCount_;
            if (mode_ == XmlParseMode::Document && rootCount_ > 1) {
                error_ = "XML document contains multiple root elements";
                return false;
            }
        }
        if (!selfClosing)
            elements_.push_back({std::move(elementName), std::move(namespaces)});
        return true;
    }

    bool parseEndTag() {
        position_ += 2;
        std::string name;
        if (!parseName(name)) {
            error_ = "invalid end tag";
            return false;
        }
        skipSpace();
        if (position_ >= input_.size() || input_[position_++] != '>') {
            error_ = "invalid end tag";
            return false;
        }
        if (elements_.empty() || elements_.back().name != name) {
            error_ = "mismatched end tag";
            return false;
        }
        elements_.pop_back();
        return true;
    }

    const std::string& input_;
    XmlParseMode mode_;
    size_t position_ = 0;
    size_t rootCount_ = 0;
    std::vector<Element> elements_;
    std::string error_;
};

}  // namespace

XmlValidationResult validateXml(const std::string& input, XmlParseMode mode) {
    return XmlParser(input, mode).parse();
}

std::string stripXmlDeclaration(const std::string& input) {
    size_t position = 0;
    if (input.size() >= 3 && static_cast<unsigned char>(input[0]) == 0xef &&
        static_cast<unsigned char>(input[1]) == 0xbb &&
        static_cast<unsigned char>(input[2]) == 0xbf) position = 3;
    if (input.compare(position, 5, "<?xml") != 0 ||
        (position + 5 < input.size() &&
         !isXmlSpace(input[position + 5]))) {
        return input;
    }
    const size_t end = input.find("?>", position + 5);
    return end == std::string::npos ? input : input.substr(end + 2);
}

}  // namespace dbms
