#include "PostgresProtocol.h"
#include "common/DateType.h"
#include "PostgresNumeric.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <limits>
#include <string>

namespace dbms {

namespace {

constexpr uint32_t kProtocol30 = 0x00030000;
constexpr size_t kMaxStartupPacket = 1024 * 1024;
constexpr size_t kMaxFrontendPacket = 16 * 1024 * 1024;

uint32_t decodeUInt32(const uint8_t* bytes) {
    return (static_cast<uint32_t>(bytes[0]) << 24) |
           (static_cast<uint32_t>(bytes[1]) << 16) |
           (static_cast<uint32_t>(bytes[2]) << 8) |
           static_cast<uint32_t>(bytes[3]);
}

uint16_t decodeUInt16(const uint8_t* bytes) {
    return static_cast<uint16_t>((static_cast<uint16_t>(bytes[0]) << 8) |
                                  static_cast<uint16_t>(bytes[1]));
}

void appendRawUInt16(std::vector<uint8_t>& output, uint16_t value) {
    output.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    output.push_back(static_cast<uint8_t>(value & 0xff));
}

void appendRawUInt32(std::vector<uint8_t>& output, uint32_t value) {
    output.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
    output.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
    output.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    output.push_back(static_cast<uint8_t>(value & 0xff));
}

void appendRawUInt64(std::vector<uint8_t>& output, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        output.push_back(static_cast<uint8_t>((value >> shift) & 0xff));
    }
}

bool parseDateDays(const std::string& value, int32_t& days) {
    Date date(value.c_str());
    if (date.year == 0) return false;
    const int64_t offset = date.convert() - Date(2000, 1, 1).convert();
    if (offset < std::numeric_limits<int32_t>::min() ||
        offset > std::numeric_limits<int32_t>::max()) return false;
    days = static_cast<int32_t>(offset);
    return true;
}

bool parseTimestampMicros(const std::string& value, int64_t& micros) {
    const int64_t seconds = parseTimestampToSeconds(value);
    if (seconds == INT64_MAX || seconds == INT64_MIN) return false;
    const int64_t epoch = parseTimestampToSeconds("2000-01-01 00:00:00");
    const __int128 result = (static_cast<__int128>(seconds) - epoch) * 1000000;
    if (result < std::numeric_limits<int64_t>::min() ||
        result > std::numeric_limits<int64_t>::max()) return false;
    micros = static_cast<int64_t>(result);
    return true;
}

bool parseUuidBytes(const std::string& value, std::vector<uint8_t>& bytes) {
    std::string hex;
    hex.reserve(32);
    for (char c : value) {
        if (c == '-') continue;
        if (!std::isxdigit(static_cast<unsigned char>(c))) return false;
        hex.push_back(c);
    }
    if (hex.size() != 32) return false;
    auto hexValue = [](char c) -> uint8_t {
        if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return static_cast<uint8_t>(c - 'a' + 10);
    };
    bytes.clear();
    bytes.reserve(16);
    for (size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(static_cast<uint8_t>((hexValue(hex[i]) << 4) |
                                              hexValue(hex[i + 1])));
    }
    return true;
}

bool encodeBinaryValue(const std::string& value, const PgColumnDescription& column,
                       std::vector<uint8_t>& encoded) {
    try {
        switch (column.typeOid) {
            case 16: {
                if (value == "true" || value == "t" || value == "1") encoded.push_back(1);
                else if (value == "false" || value == "f" || value == "0") encoded.push_back(0);
                else return false;
                return true;
            }
            case 21: {
                const auto number = std::stoll(value);
                if (number < std::numeric_limits<int16_t>::min() ||
                    number > std::numeric_limits<int16_t>::max()) return false;
                appendRawUInt16(encoded, static_cast<uint16_t>(static_cast<int16_t>(number)));
                return true;
            }
            case 23: {
                const auto number = std::stoll(value);
                if (number < std::numeric_limits<int32_t>::min() ||
                    number > std::numeric_limits<int32_t>::max()) return false;
                appendRawUInt32(encoded, static_cast<uint32_t>(static_cast<int32_t>(number)));
                return true;
            }
            case 20: {
                const auto number = std::stoll(value);
                appendRawUInt64(encoded, static_cast<uint64_t>(number));
                return true;
            }
            case 700: {
                size_t consumed = 0;
                const float number = std::stof(value, &consumed);
                if (consumed != value.size()) return false;
                uint32_t bits = 0;
                std::memcpy(&bits, &number, sizeof(bits));
                appendRawUInt32(encoded, bits);
                return true;
            }
            case 701: {
                size_t consumed = 0;
                const double number = std::stod(value, &consumed);
                if (consumed != value.size()) return false;
                uint64_t bits = 0;
                std::memcpy(&bits, &number, sizeof(bits));
                appendRawUInt64(encoded, bits);
                return true;
            }
            case 1082: {
                int32_t days = 0;
                if (!parseDateDays(value, days)) return false;
                appendRawUInt32(encoded, static_cast<uint32_t>(days));
                return true;
            }
            case 1083: {
                const int32_t seconds = parseTimeToSeconds(value);
                if (seconds < 0) return false;
                appendRawUInt64(encoded, static_cast<uint64_t>(seconds) * 1000000);
                return true;
            }
            case 1114: case 1184: {
                int64_t micros = 0;
                if (!parseTimestampMicros(value, micros)) return false;
                appendRawUInt64(encoded, static_cast<uint64_t>(micros));
                return true;
            }
            case 1700:
                return encodePostgresNumeric(value, encoded);
            case 2950:
                return parseUuidBytes(value, encoded);
            case 25: case 1042: case 1043:
                encoded.insert(encoded.end(), value.begin(), value.end());
                return true;
            default:
                return false;
        }
    } catch (...) {
        return false;
    }
}

} // namespace

bool PostgresProtocol::readExact(void* destination, size_t length) {
    auto* bytes = static_cast<uint8_t*>(destination);
    size_t received = 0;
    while (received < length) {
        ssize_t n = socket_.recv(bytes + received, length - received);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    return true;
}

bool PostgresProtocol::writeAll(const void* data, size_t length) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t written = 0;
    while (written < length) {
        ssize_t n = socket_.send(bytes + written, length - written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

bool PostgresProtocol::readStartup(PgStartupMessage& startup, std::string& error) {
    uint8_t lengthBytes[4]{};
    if (!readExact(lengthBytes, sizeof(lengthBytes))) {
        error = "connection closed while reading startup packet";
        return false;
    }
    uint32_t length = decodeUInt32(lengthBytes);
    if (length < 8 || length > kMaxStartupPacket) {
        error = "invalid startup packet length";
        return false;
    }

    std::vector<uint8_t> body(length - 4);
    if (!readExact(body.data(), body.size())) {
        error = "connection closed inside startup packet";
        return false;
    }
    startup.protocolVersion = readUInt32(body, 0);
    if (startup.protocolVersion != kProtocol30) {
        error = "unsupported PostgreSQL protocol version";
        return false;
    }

    size_t offset = 4;
    while (offset < body.size()) {
        std::string key;
        if (!readCString(body, offset, key)) {
            error = "malformed startup parameter";
            return false;
        }
        if (key.empty()) break;
        std::string value;
        if (!readCString(body, offset, value)) {
            error = "malformed startup parameter value";
            return false;
        }
        startup.parameters[std::move(key)] = std::move(value);
    }
    return true;
}

bool PostgresProtocol::readMessage(PgFrontendMessage& message, std::string& error) {
    uint8_t type = 0;
    uint8_t lengthBytes[4]{};
    if (!readExact(&type, sizeof(type)) || !readExact(lengthBytes, sizeof(lengthBytes))) {
        error = "connection closed while reading frontend message";
        return false;
    }
    uint32_t length = decodeUInt32(lengthBytes);
    if (length < 4 || length > kMaxFrontendPacket) {
        error = "invalid frontend message length";
        return false;
    }
    message.type = static_cast<char>(type);
    message.payload.resize(length - 4);
    if (!message.payload.empty() && !readExact(message.payload.data(), message.payload.size())) {
        error = "connection closed inside frontend message";
        return false;
    }
    return true;
}

bool PostgresProtocol::sendMessage(char type, const std::vector<uint8_t>& body) {
    if (body.size() > std::numeric_limits<uint32_t>::max() - 4) return false;
    std::vector<uint8_t> packet;
    packet.reserve(1 + 4 + body.size());
    packet.push_back(static_cast<uint8_t>(type));
    appendUInt32(packet, static_cast<uint32_t>(body.size() + 4));
    packet.insert(packet.end(), body.begin(), body.end());
    return writeAll(packet.data(), packet.size());
}

void PostgresProtocol::appendUInt16(std::vector<uint8_t>& body, uint16_t value) {
    body.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    body.push_back(static_cast<uint8_t>(value & 0xff));
}

void PostgresProtocol::appendUInt32(std::vector<uint8_t>& body, uint32_t value) {
    body.push_back(static_cast<uint8_t>((value >> 24) & 0xff));
    body.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
    body.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    body.push_back(static_cast<uint8_t>(value & 0xff));
}

void PostgresProtocol::appendInt32(std::vector<uint8_t>& body, int32_t value) {
    appendUInt32(body, static_cast<uint32_t>(value));
}

void PostgresProtocol::appendCString(std::vector<uint8_t>& body, const std::string& value) {
    body.insert(body.end(), value.begin(), value.end());
    body.push_back(0);
}

uint32_t PostgresProtocol::readUInt32(const std::vector<uint8_t>& data, size_t offset) {
    if (offset > data.size() || data.size() - offset < 4) return 0;
    return decodeUInt32(data.data() + offset);
}

uint16_t PostgresProtocol::readUInt16(const std::vector<uint8_t>& data, size_t offset) {
    if (offset > data.size() || data.size() - offset < 2) return 0;
    return decodeUInt16(data.data() + offset);
}

int32_t PostgresProtocol::readInt32(const std::vector<uint8_t>& data, size_t offset) {
    return static_cast<int32_t>(readUInt32(data, offset));
}

bool PostgresProtocol::readCString(const std::vector<uint8_t>& data, size_t& offset,
                                   std::string& value) {
    if (offset > data.size()) return false;
    auto end = std::find(data.begin() + static_cast<std::ptrdiff_t>(offset), data.end(), 0);
    if (end == data.end()) return false;
    value.assign(data.begin() + static_cast<std::ptrdiff_t>(offset), end);
    offset = static_cast<size_t>(std::distance(data.begin(), end)) + 1;
    return true;
}

bool PostgresProtocol::sendAuthenticationOk() {
    std::vector<uint8_t> body;
    appendUInt32(body, 0);
    return sendMessage('R', body);
}

bool PostgresProtocol::sendAuthenticationCleartextPassword() {
    std::vector<uint8_t> body;
    appendUInt32(body, 3);
    return sendMessage('R', body);
}

bool PostgresProtocol::sendAuthenticationSasl(const std::vector<std::string>& mechanisms) {
    std::vector<uint8_t> body;
    appendUInt32(body, 10);
    for (const auto& mechanism : mechanisms) appendCString(body, mechanism);
    body.push_back(0);
    return sendMessage('R', body);
}

bool PostgresProtocol::sendAuthenticationSaslContinue(const std::string& data) {
    std::vector<uint8_t> body;
    appendUInt32(body, 11);
    appendCString(body, data);
    return sendMessage('R', body);
}

bool PostgresProtocol::sendAuthenticationSaslFinal(const std::string& data) {
    std::vector<uint8_t> body;
    appendUInt32(body, 12);
    appendCString(body, data);
    return sendMessage('R', body);
}

bool PostgresProtocol::sendParameterStatus(const std::string& name, const std::string& value) {
    std::vector<uint8_t> body;
    appendCString(body, name);
    appendCString(body, value);
    return sendMessage('S', body);
}

bool PostgresProtocol::sendBackendKeyData(uint32_t processId, uint32_t secretKey) {
    std::vector<uint8_t> body;
    appendUInt32(body, processId);
    appendUInt32(body, secretKey);
    return sendMessage('K', body);
}

bool PostgresProtocol::sendReadyForQuery(char transactionStatus) {
    std::vector<uint8_t> body{static_cast<uint8_t>(transactionStatus)};
    return sendMessage('Z', body);
}

bool PostgresProtocol::sendErrorResponse(const std::string& severity,
                                         const std::string& sqlState,
                                         const std::string& message,
                                         const std::string& detail) {
    std::vector<uint8_t> body;
    body.push_back('S'); appendCString(body, severity);
    body.push_back('V'); appendCString(body, severity);
    body.push_back('C'); appendCString(body, sqlState.empty() ? "XX000" : sqlState);
    body.push_back('M'); appendCString(body, message);
    if (!detail.empty()) {
        body.push_back('D'); appendCString(body, detail);
    }
    body.push_back(0);
    return sendMessage('E', body);
}

bool PostgresProtocol::sendNoticeResponse(const std::string& message) {
    std::vector<uint8_t> body;
    body.push_back('S'); appendCString(body, "NOTICE");
    body.push_back('V'); appendCString(body, "NOTICE");
    body.push_back('M'); appendCString(body, message);
    body.push_back(0);
    return sendMessage('N', body);
}

bool PostgresProtocol::sendEmptyQueryResponse() {
    return sendMessage('I', {});
}

bool PostgresProtocol::sendParameterDescription(const std::vector<uint32_t>& parameterTypes) {
    if (parameterTypes.size() > std::numeric_limits<uint16_t>::max()) return false;
    std::vector<uint8_t> body;
    appendUInt16(body, static_cast<uint16_t>(parameterTypes.size()));
    for (uint32_t type : parameterTypes) appendUInt32(body, type);
    return sendMessage('t', body);
}

bool PostgresProtocol::sendParseComplete() { return sendMessage('1', {}); }
bool PostgresProtocol::sendBindComplete() { return sendMessage('2', {}); }
bool PostgresProtocol::sendCloseComplete() { return sendMessage('3', {}); }
bool PostgresProtocol::sendNoData() { return sendMessage('n', {}); }
bool PostgresProtocol::sendPortalSuspended() { return sendMessage('s', {}); }

bool PostgresProtocol::sendCommandComplete(const std::string& tag) {
    std::vector<uint8_t> body;
    appendCString(body, tag);
    return sendMessage('C', body);
}

bool PostgresProtocol::sendRowDescription(const std::vector<PgColumnDescription>& columns) {
    if (columns.size() > std::numeric_limits<uint16_t>::max()) return false;
    std::vector<uint8_t> body;
    appendUInt16(body, static_cast<uint16_t>(columns.size()));
    for (const auto& column : columns) {
        appendCString(body, column.name);
        appendUInt32(body, column.tableOid);
        appendUInt16(body, column.attributeNumber);
        appendUInt32(body, column.typeOid);
        appendUInt16(body, static_cast<uint16_t>(column.typeSize));
        appendInt32(body, column.typeModifier);
        appendUInt16(body, static_cast<uint16_t>(column.formatCode));
    }
    return sendMessage('T', body);
}

bool PostgresProtocol::sendDataRow(const std::vector<std::string>& values) {
    std::vector<PgColumnDescription> columns(values.size());
    return sendDataRow(values, columns);
}

bool PostgresProtocol::sendDataRow(const std::vector<std::string>& values,
                                   const std::vector<PgColumnDescription>& columns) {
    if (values.size() > std::numeric_limits<uint16_t>::max()) return false;
    if (columns.size() != values.size()) return false;
    std::vector<uint8_t> body;
    appendUInt16(body, static_cast<uint16_t>(values.size()));
    for (size_t i = 0; i < values.size(); ++i) {
        const auto& value = values[i];
        if (value == "NULL") {
            appendInt32(body, -1);
        } else {
            std::vector<uint8_t> encoded;
            if (columns[i].formatCode == 0) {
                if (value.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) return false;
                encoded.assign(value.begin(), value.end());
            } else if (columns[i].formatCode == 1) {
                if (!encodeBinaryValue(value, columns[i], encoded)) return false;
            } else {
                return false;
            }
            if (encoded.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) return false;
            appendInt32(body, static_cast<int32_t>(encoded.size()));
            body.insert(body.end(), encoded.begin(), encoded.end());
        }
    }
    return sendMessage('D', body);
}

} // namespace dbms
