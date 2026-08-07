#pragma once

#include "TLSWrapper.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace dbms {

// PostgreSQL protocol 3.0 startup packet. The protocol is deliberately kept
// independent from Session so the transport/parser can be tested without the
// full executor.
struct PgStartupMessage {
    uint32_t protocolVersion = 0;
    std::map<std::string, std::string> parameters;
};

struct PgFrontendMessage {
    char type = '\0';
    std::vector<uint8_t> payload;
};

struct PgColumnDescription {
    std::string name;
    uint32_t tableOid = 0;
    uint16_t attributeNumber = 0;
    uint32_t typeOid = 25;       // text
    int16_t typeSize = -1;       // varlena/text
    int32_t typeModifier = -1;
    int16_t formatCode = 0;      // text format
};

class PostgresProtocol {
public:
    explicit PostgresProtocol(SecureSocket& socket) : socket_(socket) {}

    // Read and validate a startup packet. This does not consume SSLRequest;
    // SSL negotiation is performed on the raw accepted socket first.
    bool readStartup(PgStartupMessage& startup, std::string& error);

    // Read one typed Frontend/Backend protocol message after startup.
    bool readMessage(PgFrontendMessage& message, std::string& error);

    bool sendAuthenticationOk();
    bool sendAuthenticationCleartextPassword();
    bool sendAuthenticationSasl(const std::vector<std::string>& mechanisms);
    bool sendAuthenticationSaslContinue(const std::string& data);
    bool sendAuthenticationSaslFinal(const std::string& data);
    bool sendParameterStatus(const std::string& name, const std::string& value);
    bool sendBackendKeyData(uint32_t processId, uint32_t secretKey);
    bool sendReadyForQuery(char transactionStatus = 'I');

    bool sendErrorResponse(const std::string& severity,
                           const std::string& sqlState,
                           const std::string& message,
                           const std::string& detail = {});
    bool sendNoticeResponse(const std::string& message);
    bool sendEmptyQueryResponse();
    bool sendParseComplete();
    bool sendBindComplete();
    bool sendCloseComplete();
    bool sendNoData();
    bool sendCommandComplete(const std::string& tag);
    bool sendRowDescription(const std::vector<PgColumnDescription>& columns);
    bool sendDataRow(const std::vector<std::string>& values);

    static uint32_t readUInt32(const std::vector<uint8_t>& data, size_t offset);
    static uint16_t readUInt16(const std::vector<uint8_t>& data, size_t offset);
    static int32_t readInt32(const std::vector<uint8_t>& data, size_t offset);
    static bool readCString(const std::vector<uint8_t>& data, size_t& offset,
                            std::string& value);

private:
    SecureSocket& socket_;

    bool readExact(void* destination, size_t length);
    bool writeAll(const void* data, size_t length);
    bool sendMessage(char type, const std::vector<uint8_t>& body);

    static void appendUInt16(std::vector<uint8_t>& body, uint16_t value);
    static void appendUInt32(std::vector<uint8_t>& body, uint32_t value);
    static void appendInt32(std::vector<uint8_t>& body, int32_t value);
    static void appendCString(std::vector<uint8_t>& body, const std::string& value);
};

} // namespace dbms
