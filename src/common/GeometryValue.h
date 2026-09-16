#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

// Parsed PostgreSQL geometric value.  Fixed-width shapes store their
// coordinates in order; path/polygon store x,y pairs.  `openPath` is only
// meaningful for path.
struct GeometryValue {
    std::string type;
    std::vector<double> coordinates;
    bool openPath = false;
};

bool isGeometryTypeName(const std::string& typeName);
const char* geometryTypeNameForOid(uint32_t oid);
int16_t geometryTypeLengthForOid(uint32_t oid);

// Parse all documented PostgreSQL input spellings and reject malformed or
// mismatched delimiter structures.  Output always uses PostgreSQL's canonical
// spelling.  The storage layer asks for an unwrapped point because its packed
// point datum historically uses "x,y" internally; protocol output uses the
// default wrapped spelling "(x,y)".
bool parseGeometryValue(const std::string& input, const std::string& typeName,
                        GeometryValue& value);
std::string formatGeometryValue(const GeometryValue& value,
                                bool wrapPoint = true);
bool normalizeGeometryText(const std::string& input,
                           const std::string& typeName,
                           std::string& output,
                           bool wrapPoint = true);

// PostgreSQL v3 binary representation (network byte order), matching the
// corresponding *_send / *_recv functions in geo_ops.c.
bool encodeGeometryBinary(const std::string& text, uint32_t oid,
                          std::vector<uint8_t>& output);
bool decodeGeometryBinary(uint32_t oid, const std::vector<uint8_t>& input,
                          std::string& output);

}  // namespace dbms
