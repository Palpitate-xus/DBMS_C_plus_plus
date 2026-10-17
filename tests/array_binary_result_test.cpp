#include "network/PostgresProtocol.h"
#include "catalog/catalog.h"

#include <cerrno>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace {
using Bytes = std::vector<uint8_t>;
size_t controls = 0;

void require(bool value, const std::string& message) {
    if (!value) throw std::runtime_error(message);
}
void append32(Bytes& bytes, uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8)
        bytes.push_back(static_cast<uint8_t>(value >> shift));
}
Bytes hex(const std::string& value) {
    Bytes result;
    for (size_t i = 0; i < value.size(); i += 2)
        result.push_back(static_cast<uint8_t>(std::stoul(value.substr(i, 2), nullptr, 16)));
    return result;
}
Bytes array(uint32_t oid, const std::vector<Bytes>& elements,
            const std::vector<bool>& nulls, const std::vector<std::pair<int32_t,int32_t>>& dimensions) {
    Bytes result;
    append32(result, static_cast<uint32_t>(dimensions.size()));
    bool hasNull = false;
    for (bool value : nulls) hasNull = hasNull || value;
    append32(result, hasNull ? 1 : 0);
    append32(result, oid);
    for (const auto& dimension : dimensions) {
        append32(result, static_cast<uint32_t>(dimension.first));
        append32(result, static_cast<uint32_t>(dimension.second));
    }
    for (size_t i = 0; i < elements.size(); ++i) {
        append32(result, nulls[i] ? UINT32_MAX : static_cast<uint32_t>(elements[i].size()));
        if (!nulls[i]) result.insert(result.end(), elements[i].begin(), elements[i].end());
    }
    return result;
}

void check(uint32_t oid, const std::string& literal, const Bytes& expected,
           bool sqlNull = false, bool accepted = true, int16_t format = 1) {
    ++controls;
    int fds[2];
    require(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0, "socketpair");
    dbms::SecureSocket sender(fds[0]);
    dbms::SecureSocket receiver(fds[1]);
    timeval timeout{2,0};
    require(::setsockopt(receiver.fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0, "timeout");
    dbms::PostgresProtocol protocol(sender);
    dbms::PgColumnDescription column;
    column.typeOid = oid;
    column.formatCode = format;
    const std::string context = "control " + std::to_string(controls) + " oid " + std::to_string(oid) + " " + literal;
    const bool sent = protocol.sendDataRow({literal}, {column}, {sqlNull});
    require(sent == accepted, context + " acceptance");
    if (!accepted) {
        uint8_t byte = 0;
        const auto received = ::recv(receiver.fd, &byte, 1, MSG_DONTWAIT);
        require(received == -1 && (errno == EAGAIN || errno == EWOULDBLOCK), context + " partial row");
        return;
    }
    Bytes expectedFrame{'D'};
    append32(expectedFrame, static_cast<uint32_t>(10 + (sqlNull ? 0 : expected.size())));
    expectedFrame.insert(expectedFrame.end(), {0,1});
    append32(expectedFrame, sqlNull ? UINT32_MAX : static_cast<uint32_t>(expected.size()));
    if (!sqlNull) expectedFrame.insert(expectedFrame.end(), expected.begin(), expected.end());
    Bytes actual(expectedFrame.size());
    require(::recv(receiver.fd, actual.data(), actual.size(), MSG_WAITALL) == static_cast<ssize_t>(actual.size()), context + " frame length");
    require(actual == expectedFrame, context + " exact bytes");
}
} // namespace

int main() {
    const auto one = hex("00000001"), two = hex("00000002");
    check(1007, "{1,2}", array(23, {one,two}, {false,false}, {{2,1}}));
    check(1007, "[-3:-2]={1,NULL}", array(23, {one,{}}, {false,true}, {{2,-3}}));
    check(1007, "[0:1][-2:-1]={{1,NULL},{2,1}}", array(23, {one,{},two,one}, {false,true,false,false}, {{2,0},{2,-2}}));
    check(1007, "{{{{{{1}}}}}}", array(23, {one}, {false}, {{1,1},{1,1},{1,1},{1,1},{1,1},{1,1}}));
    check(1009, R"array({NULL,"NULL","","a,b","a\\b","a\"b","{nested}"})array",
          array(25, {{},{'N','U','L','L'},{},{'a',',','b'},{'a','\\','b'},{'a','"','b'},{'{','n','e','s','t','e','d','}'}},
                {true,false,false,false,false,false,false}, {{7,1}}));
    check(1231, "{1.00,NULL}", array(1700, {hex("00010000000000020001"),{}}, {false,true}, {{2,1}}));
    check(2951, "{00000000-0000-0000-0000-000000000001}", array(2950, {hex("00000000000000000000000000000001")}, {false}, {{1,1}}));
    check(1001, R"array({"\\x0001ff","\\x"})array", array(17, {hex("0001ff"),{}}, {false,false}, {{2,1}}));
    check(1017, R"array({"(1,2)"})array", array(600, {hex("3ff00000000000004000000000000000")}, {false}, {{1,1}}));
    check(1020, R"array({"(3,4),(1,2)"})array", array(603, {hex("400800000000000040100000000000003ff00000000000004000000000000000")}, {false}, {{1,1}}));
    // SQL input normalizes box corners before the row reaches this sender.
    // Raw native carriers follow the existing scalar codec's own contract.
    check(603, "(1,2),(3,4)", hex("3ff0000000000000400000000000000040080000000000004010000000000000"));
    check(603, "(3,4),(1,2)", hex("400800000000000040100000000000003ff00000000000004000000000000000"));
    check(1020, R"array({"(1,2),(3,4)"})array", array(603, {hex("3ff0000000000000400000000000000040080000000000004010000000000000")}, {false}, {{1,1}}));
    check(1021, "{-0,1.40129846e-45}", array(700, {hex("80000000"),hex("00000001")}, {false,false}, {{2,1}}));
    check(1022, "{-0}", array(701, {hex("8000000000000000")}, {false}, {{1,1}}));
    check(1563, "{10101}", array(1562, {hex("00000005a8")}, {false}, {{1,1}}));
    check(1000, "{t,NULL,f}", array(16, {{1},{},{0}}, {false,true,false}, {{3,1}}));
    check(791, "{1.23}", array(790, {hex("000000000000007b")}, {false}, {{1,1}}));
    size_t arrayTypes = 0;
    for (const auto& type : dbms::CatalogManager::builtinTypeRows()) {
        if (type.typnamespace != 11 || type.typcategory != 'A' || !type.typelem) continue;
        ++arrayTypes;
        check(type.oid, "{}", array(type.typelem, {}, {}, {}));
        check(type.oid, "{NULL,NULL}", array(type.typelem, {{},{}}, {true,true}, {{2,1}}));
        check(type.oid, "not an array; explicit SQL NULL", {}, true);
    }
    require(arrayTypes == 49, "builtin array identity coverage");
    check(23, "-7", hex("fffffff9"));
    check(25, "{1,NULL}", {'{','1',',','N','U','L','L','}'});
    check(25, "NULL", {'N','U','L','L'});
    check(17, "\\x0001ff", hex("0001ff"));
    check(16, "t", {1});
    for (const auto& literal : {"{1,}", "{x}", "{{1},{2,3}}", "[0:2]={1,2}", "{{{{{{{1}}}}}}}", "bare"})
        check(1007, literal, {}, false, false);
    check(1005, "{32768}", {}, false, false);
    check(999999, "{1}", {}, false, false);
    // Scalar JSON binary send is still unsupported; do not smuggle its text
    // bytes into an array element and call the result binary.
    check(199, "{1}", {}, false, false);
    check(1007, "{1}", {}, false, false, 2);
    check(1007, "{1,2}", {'{','1',',','2','}'}, false, true, 0);
    check(1009, "NULL", {}, true, true, 0);
    require(controls == 182, "control coverage");
    std::cout << "[ARRAY BINARY RESULT] all " << controls << " native controls passed\n";
}
