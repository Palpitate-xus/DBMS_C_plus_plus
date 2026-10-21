#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dbms {

// An owned TSVECTOR datum. SQL NULL is carried by the caller, not by an
// empty vector. The public binary representation is PostgreSQL's send format.
class TsVectorValue {
public:
    static TsVectorValue parse(std::string_view input);
    static TsVectorValue fromBinary(const std::vector<uint8_t>& input);
    static TsVectorValue concatenate(TsVectorValue left, const TsVectorValue& right);
    std::string toString() const;
    std::vector<uint8_t> toBinary() const;

private:
    struct Entry {
        std::string lexeme;
        // Low 14 bits: position; high 2 bits: D/C/B/A weight.
        std::vector<uint16_t> positions;
    };
    std::vector<Entry> entries_;
    void validateSize() const;
};

} // namespace dbms
