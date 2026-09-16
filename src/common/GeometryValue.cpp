#include "GeometryValue.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>

namespace dbms {
namespace {

constexpr uint32_t kPointOid = 600;
constexpr uint32_t kLsegOid = 601;
constexpr uint32_t kPathOid = 602;
constexpr uint32_t kBoxOid = 603;
constexpr uint32_t kPolygonOid = 604;
constexpr uint32_t kLineOid = 628;
constexpr uint32_t kCircleOid = 718;
constexpr size_t kMaximumGeometryPoints = 1U << 20;

std::string trimGeometry(std::string_view input) {
    size_t first = 0;
    while (first < input.size() &&
           std::isspace(static_cast<unsigned char>(input[first]))) {
        ++first;
    }
    size_t last = input.size();
    while (last > first &&
           std::isspace(static_cast<unsigned char>(input[last - 1]))) {
        --last;
    }
    return std::string(input.substr(first, last - first));
}

std::string lowerGeometry(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    });
    return value;
}

bool parseGeometryNumber(const std::string& input, double& output) {
    const std::string token = trimGeometry(input);
    if (token.empty()) return false;
    const std::string lower = lowerGeometry(token);
    if (lower == "nan" || lower == "+nan" || lower == "-nan") {
        output = std::numeric_limits<double>::quiet_NaN();
        if (!token.empty() && token.front() == '-') output = -output;
        return true;
    }
    if (lower == "infinity" || lower == "+infinity") {
        output = std::numeric_limits<double>::infinity();
        return true;
    }
    if (lower == "-infinity") {
        output = -std::numeric_limits<double>::infinity();
        return true;
    }

    char* end = nullptr;
    errno = 0;
    output = std::strtod(token.c_str(), &end);
    return errno != ERANGE && end == token.c_str() + token.size();
}

std::string formatGeometryNumber(double value) {
    if (std::isnan(value)) return "NaN";
    if (std::isinf(value)) return std::signbit(value) ? "-Infinity" : "Infinity";

    char buffer[64];
    const auto converted = std::to_chars(
        buffer, buffer + sizeof(buffer), value, std::chars_format::general);
    if (converted.ec == std::errc()) return std::string(buffer, converted.ptr);

    std::ostringstream output;
    output << std::setprecision(std::numeric_limits<double>::max_digits10)
           << value;
    return output.str();
}

bool unwrapQuotedGeometry(std::string& text) {
    text = trimGeometry(text);
    if (text.empty()) return false;
    if (text.front() == '\'' || text.back() == '\'') {
        if (text.size() < 2 || text.front() != '\'' || text.back() != '\'') {
            return false;
        }
        text = trimGeometry(std::string_view(text).substr(1, text.size() - 2));
    }
    return !text.empty();
}

bool splitGeometry(const std::string& input, std::vector<double>& numbers,
                   std::string& skeleton) {
    std::string flattened;
    flattened.reserve(input.size());
    std::vector<char> closers;
    for (char c : input) {
        switch (c) {
            case '(':
                closers.push_back(')');
                skeleton.push_back(c);
                flattened.push_back(' ');
                break;
            case '[':
                closers.push_back(']');
                skeleton.push_back(c);
                flattened.push_back(' ');
                break;
            case '{':
                closers.push_back('}');
                skeleton.push_back(c);
                flattened.push_back(' ');
                break;
            case '<':
                closers.push_back('>');
                skeleton.push_back(c);
                flattened.push_back(' ');
                break;
            case ')': case ']': case '}': case '>':
                if (closers.empty() || closers.back() != c) return false;
                closers.pop_back();
                skeleton.push_back(c);
                flattened.push_back(' ');
                break;
            default:
                flattened.push_back(c);
                break;
        }
    }
    if (!closers.empty()) return false;

    size_t start = 0;
    while (start <= flattened.size()) {
        const size_t comma = flattened.find(',', start);
        const size_t end = comma == std::string::npos ? flattened.size() : comma;
        double number = 0.0;
        if (!parseGeometryNumber(flattened.substr(start, end - start), number)) {
            return false;
        }
        numbers.push_back(number);
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return !numbers.empty();
}

std::string pointSkeleton(size_t count) {
    std::string result;
    result.reserve(count * 2);
    for (size_t i = 0; i < count; ++i) result += "()";
    return result;
}

bool isPointListSkeleton(const std::string& skeleton, size_t count,
                         bool allowOpenOuter) {
    const std::string points = pointSkeleton(count);
    if (skeleton.empty() || skeleton == points || skeleton == "()" ||
        skeleton == "(" + points + ")") {
        return true;
    }
    return allowOpenOuter &&
           (skeleton == "[]" || skeleton == "[" + points + "]");
}

bool parsePointForm(std::string text, GeometryValue& value) {
    if (text.size() >= 5 && lowerGeometry(text.substr(0, 5)) == "point" &&
        (text.size() == 5 || text[5] == '(' ||
         std::isspace(static_cast<unsigned char>(text[5])))) {
        text = trimGeometry(std::string_view(text).substr(5));
    }
    std::vector<double> numbers;
    std::string skeleton;
    if (!splitGeometry(text, numbers, skeleton) || numbers.size() != 2 ||
        (skeleton != "" && skeleton != "()")) {
        return false;
    }
    // The current packed point datum is also the key source for the local
    // SP-GiST implementation, whose bounding-box invariants require finite
    // coordinates.  Fail closed until that opclass has explicit NaN/infinity
    // buckets rather than silently omitting such rows from the index.
    if (!std::isfinite(numbers[0]) || !std::isfinite(numbers[1])) return false;
    value.coordinates = std::move(numbers);
    return true;
}

bool parseLineForm(const std::string& text, GeometryValue& value) {
    std::vector<double> numbers;
    std::string skeleton;
    if (!splitGeometry(text, numbers, skeleton)) return false;
    if (numbers.size() == 3 && skeleton == "{}") {
        if (numbers[0] == 0.0 && numbers[1] == 0.0) return false;
        value.coordinates = std::move(numbers);
        return true;
    }
    if (numbers.size() != 4 || !isPointListSkeleton(skeleton, 2, true)) {
        return false;
    }
    const double x1 = numbers[0];
    const double y1 = numbers[1];
    const double x2 = numbers[2];
    const double y2 = numbers[3];
    if (x1 == x2 && y1 == y2) return false;
    if (x1 == x2) {
        value.coordinates = {-1.0, 0.0, x1};
    } else if (y1 == y2) {
        value.coordinates = {0.0, -1.0, y1};
    } else {
        const double slope = (y2 - y1) / (x2 - x1);
        value.coordinates = {slope, -1.0, y1 - slope * x1};
    }
    return true;
}

bool parsePointListForm(const std::string& text, const std::string& type,
                        GeometryValue& value) {
    std::vector<double> numbers;
    std::string skeleton;
    if (!splitGeometry(text, numbers, skeleton) || numbers.size() % 2 != 0) {
        return false;
    }
    const size_t count = numbers.size() / 2;
    if (count == 0 || count > kMaximumGeometryPoints) return false;
    const bool allowOpen = type == "lseg" || type == "path";
    if (!isPointListSkeleton(skeleton, count, allowOpen)) return false;
    if ((type == "lseg" || type == "box") && count != 2) return false;
    value.openPath = type == "path" && !skeleton.empty() && skeleton.front() == '[';
    value.coordinates = std::move(numbers);
    return true;
}

bool parseCircleForm(const std::string& text, GeometryValue& value) {
    std::vector<double> numbers;
    std::string skeleton;
    if (!splitGeometry(text, numbers, skeleton) || numbers.size() != 3) {
        return false;
    }
    if (skeleton != "" && skeleton != "()" && skeleton != "(())" &&
        skeleton != "<>" && skeleton != "<()>") {
        return false;
    }
    // PostgreSQL deliberately accepts NaN radii, but rejects negative finite
    // values and negative infinity.
    if (numbers[2] < 0.0) return false;
    value.coordinates = std::move(numbers);
    return true;
}

void appendUint32(std::vector<uint8_t>& output, uint32_t value) {
    output.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
    output.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
    output.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    output.push_back(static_cast<uint8_t>(value & 0xff));
}

void appendDouble(std::vector<uint8_t>& output, double value) {
    uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    for (int shift = 56; shift >= 0; shift -= 8) {
        output.push_back(static_cast<uint8_t>((bits >> shift) & 0xff));
    }
}

bool readUint32(const std::vector<uint8_t>& input, size_t& offset,
                uint32_t& output) {
    if (input.size() - offset < 4) return false;
    output = (static_cast<uint32_t>(input[offset]) << 24) |
             (static_cast<uint32_t>(input[offset + 1]) << 16) |
             (static_cast<uint32_t>(input[offset + 2]) << 8) |
             static_cast<uint32_t>(input[offset + 3]);
    offset += 4;
    return true;
}

bool readDouble(const std::vector<uint8_t>& input, size_t& offset,
                double& output) {
    if (input.size() - offset < 8) return false;
    uint64_t bits = 0;
    for (size_t i = 0; i < 8; ++i) bits = (bits << 8) | input[offset + i];
    offset += 8;
    std::memcpy(&output, &bits, sizeof(output));
    return true;
}

bool decodeFixedGeometry(const std::vector<uint8_t>& input, size_t count,
                         GeometryValue& value) {
    if (input.size() != count * sizeof(double)) return false;
    value.coordinates.resize(count);
    size_t offset = 0;
    for (double& coordinate : value.coordinates) {
        if (!readDouble(input, offset, coordinate)) return false;
    }
    return offset == input.size();
}

}  // namespace

bool isGeometryTypeName(const std::string& typeName) {
    const std::string type = lowerGeometry(trimGeometry(typeName));
    return type == "point" || type == "line" || type == "lseg" ||
           type == "box" || type == "path" || type == "polygon" ||
           type == "circle";
}

const char* geometryTypeNameForOid(uint32_t oid) {
    switch (oid) {
        case kPointOid: return "point";
        case kLsegOid: return "lseg";
        case kPathOid: return "path";
        case kBoxOid: return "box";
        case kPolygonOid: return "polygon";
        case kLineOid: return "line";
        case kCircleOid: return "circle";
        default: return nullptr;
    }
}

int16_t geometryTypeLengthForOid(uint32_t oid) {
    switch (oid) {
        case kPointOid: return 16;
        case kLsegOid: case kBoxOid: return 32;
        case kLineOid: case kCircleOid: return 24;
        case kPathOid: case kPolygonOid: return -1;
        default: return 0;
    }
}

bool parseGeometryValue(const std::string& input, const std::string& typeName,
                        GeometryValue& value) {
    value = GeometryValue{};
    value.type = lowerGeometry(trimGeometry(typeName));
    if (!isGeometryTypeName(value.type)) return false;
    std::string text = input;
    if (!unwrapQuotedGeometry(text)) return false;

    if (value.type == "point") return parsePointForm(text, value);
    if (value.type == "line") return parseLineForm(text, value);
    if (value.type == "circle") return parseCircleForm(text, value);
    return parsePointListForm(text, value.type, value);
}

std::string formatGeometryValue(const GeometryValue& value, bool wrapPoint) {
    const auto pointAt = [&](size_t offset) {
        return "(" + formatGeometryNumber(value.coordinates[offset]) + "," +
               formatGeometryNumber(value.coordinates[offset + 1]) + ")";
    };
    if (value.type == "point" && value.coordinates.size() == 2) {
        const std::string body = formatGeometryNumber(value.coordinates[0]) + "," +
                                 formatGeometryNumber(value.coordinates[1]);
        return wrapPoint ? "(" + body + ")" : body;
    }
    if (value.type == "line" && value.coordinates.size() == 3) {
        return "{" + formatGeometryNumber(value.coordinates[0]) + "," +
               formatGeometryNumber(value.coordinates[1]) + "," +
               formatGeometryNumber(value.coordinates[2]) + "}";
    }
    if (value.type == "lseg" && value.coordinates.size() == 4) {
        return "[" + pointAt(0) + "," + pointAt(2) + "]";
    }
    if (value.type == "box" && value.coordinates.size() == 4) {
        const double highX = std::max(value.coordinates[0], value.coordinates[2]);
        const double highY = std::max(value.coordinates[1], value.coordinates[3]);
        const double lowX = std::min(value.coordinates[0], value.coordinates[2]);
        const double lowY = std::min(value.coordinates[1], value.coordinates[3]);
        GeometryValue ordered{"box", {highX, highY, lowX, lowY}, false};
        return "(" + formatGeometryNumber(ordered.coordinates[0]) + "," +
               formatGeometryNumber(ordered.coordinates[1]) + "),(" +
               formatGeometryNumber(ordered.coordinates[2]) + "," +
               formatGeometryNumber(ordered.coordinates[3]) + ")";
    }
    if ((value.type == "path" || value.type == "polygon") &&
        !value.coordinates.empty() && value.coordinates.size() % 2 == 0) {
        std::string body;
        for (size_t i = 0; i < value.coordinates.size(); i += 2) {
            if (!body.empty()) body.push_back(',');
            body += pointAt(i);
        }
        if (value.type == "path" && value.openPath) return "[" + body + "]";
        return "(" + body + ")";
    }
    if (value.type == "circle" && value.coordinates.size() == 3) {
        return "<" + pointAt(0) + "," +
               formatGeometryNumber(value.coordinates[2]) + ">";
    }
    return {};
}

bool normalizeGeometryText(const std::string& input,
                           const std::string& typeName,
                           std::string& output, bool wrapPoint) {
    GeometryValue value;
    if (!parseGeometryValue(input, typeName, value)) return false;
    output = formatGeometryValue(value, wrapPoint);
    return !output.empty();
}

bool encodeGeometryBinary(const std::string& text, uint32_t oid,
                          std::vector<uint8_t>& output) {
    const char* type = geometryTypeNameForOid(oid);
    if (!type) return false;
    GeometryValue value;
    if (!parseGeometryValue(text, type, value)) return false;

    output.clear();
    if (value.type == "path") {
        output.push_back(value.openPath ? 0 : 1);
        appendUint32(output,
                     static_cast<uint32_t>(value.coordinates.size() / 2));
    } else if (value.type == "polygon") {
        appendUint32(output,
                     static_cast<uint32_t>(value.coordinates.size() / 2));
    }
    for (double coordinate : value.coordinates) appendDouble(output, coordinate);
    return true;
}

bool decodeGeometryBinary(uint32_t oid, const std::vector<uint8_t>& input,
                          std::string& output) {
    const char* type = geometryTypeNameForOid(oid);
    if (!type) return false;
    GeometryValue value;
    value.type = type;

    size_t fixedCount = 0;
    if (oid == kPointOid) fixedCount = 2;
    else if (oid == kLineOid || oid == kCircleOid) fixedCount = 3;
    else if (oid == kLsegOid || oid == kBoxOid) fixedCount = 4;
    if (fixedCount != 0) {
        if (!decodeFixedGeometry(input, fixedCount, value)) return false;
    } else {
        size_t offset = 0;
        if (oid == kPathOid) {
            if (input.empty() || input[0] > 1) return false;
            value.openPath = input[0] == 0;
            offset = 1;
        }
        uint32_t count = 0;
        if (!readUint32(input, offset, count) || count == 0 ||
            count > kMaximumGeometryPoints ||
            input.size() - offset != static_cast<size_t>(count) * 16) {
            return false;
        }
        value.coordinates.resize(static_cast<size_t>(count) * 2);
        for (double& coordinate : value.coordinates) {
            if (!readDouble(input, offset, coordinate)) return false;
        }
        if (offset != input.size()) return false;
    }

    if (value.type == "line" && value.coordinates[0] == 0.0 &&
        value.coordinates[1] == 0.0) {
        return false;
    }
    if (value.type == "point" &&
        (!std::isfinite(value.coordinates[0]) ||
         !std::isfinite(value.coordinates[1]))) {
        return false;
    }
    if (value.type == "circle" && value.coordinates[2] < 0.0) return false;
    output = formatGeometryValue(value, true);
    return !output.empty();
}

}  // namespace dbms
