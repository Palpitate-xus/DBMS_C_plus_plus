#pragma once

#include "catalog/systables.h"
#include "common/DbError.h"

#include <charconv>
#include <cstring>
#include <limits>
#include <optional>
#include <string_view>

namespace dbms::pg_const_node {

struct Constant {
    Oid type;
    int32_t typmod;
    Oid collation;
    int16_t length;
    bool byValue;
    std::optional<std::string> bytes;
};

inline std::string encode(const Constant& constant) {
    std::string result = "{CONST :consttype " + std::to_string(constant.type) +
        " :consttypmod " + std::to_string(constant.typmod) +
        " :constcollid " + std::to_string(constant.collation) +
        " :constlen " + std::to_string(constant.length) +
        " :constbyval " + (constant.byValue ? "true" : "false") +
        " :constisnull " + (constant.bytes ? "false" : "true") +
        " :location -1 :constvalue ";
    if (!constant.bytes) return result + "<>}";
    result += std::to_string(constant.byValue ? static_cast<size_t>(constant.length) : constant.bytes->size());
    result += " [ ";
    for (char byte : *constant.bytes) result += std::to_string(static_cast<int>(byte)) + ' ';
    return result + "]}";
}

inline std::string datum(uintptr_t value) {
    return {reinterpret_cast<const char*>(&value), sizeof(value)};
}

inline bool littleEndian() {
    const uint16_t marker = 1;
    return *reinterpret_cast<const unsigned char*>(&marker) == 1;
}

inline std::string varlena(std::string_view value) {
    if (value.size() > 0x3fffffffU - sizeof(uint32_t))
        throw DbError("54000", "constant is too large");
    const uint32_t size = static_cast<uint32_t>(value.size() + sizeof(uint32_t)) << (littleEndian() ? 2 : 0);
    std::string bytes(reinterpret_cast<const char*>(&size), sizeof(size));
    bytes.append(value);
    return bytes;
}

template<class T>
inline T read(std::string_view bytes, size_t offset = 0) {
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset)
        throw DbError("XX001", "invalid catalog constant datum width");
    T value;
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
}

inline std::string_view varlenaValue(std::string_view bytes) {
    const auto length = read<uint32_t>(bytes);
    const bool plain = littleEndian() ? !(length & 3U) : !(length & 0xc0000000U);
    if (!plain || (littleEndian() ? length >> 2 : length) != bytes.size())
        throw DbError("XX001", "invalid catalog constant variable-length datum");
    return bytes.substr(sizeof(length));
}

// Catalog node text is a native Datum, not an input or executable SQL form.
// Validate every field and the full datum width before interpreting its bytes.
class Reader {
protected:
    std::string_view remaining_;

    [[noreturn]] static void invalid() {
        throw DbError("XX001", "invalid catalog constant expression");
    }

    void expect(std::string_view token) {
        if (remaining_.substr(0, token.size()) != token) invalid();
        remaining_.remove_prefix(token.size());
    }

    template<class T> T number() {
        if (remaining_.empty()) invalid();
        T value{};
        const auto parsed = std::from_chars(remaining_.data(), remaining_.data() + remaining_.size(), value);
        if (parsed.ec != std::errc{}) invalid();
        remaining_.remove_prefix(static_cast<size_t>(parsed.ptr - remaining_.data()));
        return value;
    }

    bool flag() {
        if (remaining_.substr(0, 4) == "true") { expect("true"); return true; }
        expect("false");
        return false;
    }

public:
    explicit Reader(std::string_view expression) : remaining_(expression) {}

    Constant parse() {
        auto constant = readConstant();
        if (!remaining_.empty()) invalid();
        return constant;
    }

protected:
    Constant readConstant() {
        constexpr std::string_view prefix = "{CONST :consttype ";
        if (remaining_.substr(0, prefix.size()) != prefix)
            throw DbError("0A000", "catalog expression node is not supported");
        expect(prefix);
        const auto type = number<Oid>();
        expect(" :consttypmod ");
        const auto typmod = number<int32_t>();
        expect(" :constcollid ");
        const auto collation = number<Oid>();
        expect(" :constlen ");
        const auto length = number<int16_t>();
        expect(" :constbyval ");
        const auto byValue = flag();
        expect(" :constisnull ");
        const auto isNull = flag();
        expect(" :location ");
        (void)number<int32_t>();
        expect(" :constvalue ");
        std::optional<std::string> bytes;
        if (isNull) expect("<>");
        else {
            const auto size = number<size_t>();
            if (byValue && (length <= 0 || size != static_cast<size_t>(length) || size > sizeof(uintptr_t))) invalid();
            const auto width = byValue ? sizeof(uintptr_t) : size;
            expect(" [ ");
            if (width > remaining_.size() / 2) invalid();
            bytes.emplace();
            bytes->reserve(width);
            for (size_t i = 0; i < width; ++i) {
                const auto byte = number<int>();
                if (byte < std::numeric_limits<char>::min() || byte > std::numeric_limits<char>::max()) invalid();
                bytes->push_back(static_cast<char>(byte));
                expect(" ");
            }
            expect("]");
        }
        expect("}");
        return {type, typmod, collation, length, byValue, std::move(bytes)};
    }
};

} // namespace dbms::pg_const_node
