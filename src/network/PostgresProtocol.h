#pragma once

#include "TLSWrapper.h"

#include <chrono>
#include <cstdint>
#include <functional>
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
    std::vector<std::string> unsupportedProtocolOptions;
};

struct PgFrontendMessage {
    char type = '\0';
    std::vector<uint8_t> payload;
};

enum class ProtocolInputWaitResult { Ready, TimedOut, Error };
enum class ProtocolMessageReadResult { Complete, TimedOut, Interrupted, Error };
using ProtocolMessageWriteResult = SocketWriteResult;

struct PgColumnDescription {
    std::string name;
    uint32_t tableOid = 0;
    uint16_t attributeNumber = 0;
    uint32_t typeOid = 25;       // text
    int16_t typeSize = -1;       // varlena/text
    int32_t typeModifier = -1;
    int16_t formatCode = 0;      // text format
    std::string moneyLocale = "C"; // session lc_monetary for binary cash I/O
};

class PostgresProtocol {
public:
    explicit PostgresProtocol(SecureSocket& socket) : socket_(socket) {}

    // Read and validate a startup packet. This does not consume SSLRequest;
    // SSL negotiation is performed on the raw accepted socket first.
    bool readStartup(PgStartupMessage& startup, std::string& error);

    // Read one typed Frontend/Backend protocol message after startup.
    bool readMessage(PgFrontendMessage& message, std::string& error);
    ProtocolMessageReadResult readMessageUntil(
        PgFrontendMessage& message, std::string& error,
        std::chrono::steady_clock::time_point deadline,
        const std::function<bool()>& interrupted,
        bool& partialMessage);

    // Wait until at least one frontend byte is available or the absolute
    // deadline expires. SSL-decrypted bytes already buffered are ready too.
    ProtocolInputWaitResult waitForInputUntil(
        std::chrono::steady_clock::time_point deadline);

    bool sendAuthenticationOk();
    bool sendAuthenticationCleartextPassword();
    bool sendAuthenticationSasl(const std::vector<std::string>& mechanisms);
    bool sendAuthenticationSaslContinue(const std::string& data);
    bool sendAuthenticationSaslFinal(const std::string& data);
    bool sendNegotiateProtocolVersion(
        uint32_t supportedVersion,
        const std::vector<std::string>& unsupportedOptions);
    bool sendParameterStatus(const std::string& name, const std::string& value);
    bool sendBackendKeyData(uint32_t processId, uint32_t secretKey);
    bool sendReadyForQuery(char transactionStatus = 'I');

    bool sendErrorResponse(const std::string& severity,
                           const std::string& sqlState,
                           const std::string& message,
                           const std::string& detail = {});
    bool sendErrorResponseUntil(
        const std::string& severity, const std::string& sqlState,
        const std::string& message,
        std::chrono::steady_clock::time_point deadline);
    bool sendNoticeResponse(const std::string& message,
                            const std::string& severity = "NOTICE",
                            const std::string& sqlState = "00000",
                            const std::string& detail = {});
    bool sendNotificationResponse(uint32_t senderPid,
                                  const std::string& channel,
                                  const std::string& payload);
    bool sendEmptyQueryResponse();
    bool sendParameterDescription(const std::vector<uint32_t>& parameterTypes);
    bool sendParseComplete();
    bool sendBindComplete();
    bool sendCloseComplete();
    bool sendNoData();
    bool sendPortalSuspended();
    bool sendCommandComplete(const std::string& tag);
    // COPY sub-protocol messages.  The overall and per-column format codes
    // are PostgreSQL's 0=text / 1=binary values.  Callers deliberately send
    // one CopyData message at a time so socket backpressure bounds memory.
    bool sendCopyInResponse(uint8_t overallFormat,
                            const std::vector<uint16_t>& columnFormats);
    bool sendCopyOutResponse(uint8_t overallFormat,
                             const std::vector<uint16_t>& columnFormats);
    bool sendCopyData(const std::string& data);
    ProtocolMessageWriteResult sendCopyDataUntil(
        const std::string& data,
        std::chrono::steady_clock::time_point deadline,
        const std::function<bool()>& interrupted);
    bool sendCopyDone();
    ProtocolMessageWriteResult sendCopyDoneUntil(
        std::chrono::steady_clock::time_point deadline,
        const std::function<bool()>& interrupted);
    bool sendRowDescription(const std::vector<PgColumnDescription>& columns);
    bool sendDataRow(const std::vector<std::string>& values);
    bool sendDataRow(const std::vector<std::string>& values,
                     const std::vector<PgColumnDescription>& columns);
    bool sendDataRow(const std::vector<std::string>& values,
                     const std::vector<PgColumnDescription>& columns,
                     const std::vector<bool>& nulls);

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
