#include "network/PostgresProtocol.h"
#include <cerrno>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <vector>

namespace {
using Bytes = std::vector<uint8_t>;
size_t controls = 0, failures = 0;
void append32(Bytes& bytes, uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) bytes.push_back(static_cast<uint8_t>(value >> shift));
}
Bytes bytes(const std::string& value) { return Bytes(value.begin(),value.end()); }
std::string quote(const std::string& value) {
    std::string result = "\"";
    for (char c : value) { if (c == '\\' || c == '"') result += '\\'; result += c; }
    return result+'"';
}
Bytes array(const std::vector<std::optional<std::string>>& values,
            const std::vector<std::pair<int32_t,int32_t>>& dimensions) {
    Bytes result;
    append32(result,static_cast<uint32_t>(dimensions.size()));
    bool nulls = false;
    for (const auto& value : values) nulls = nulls || !value;
    append32(result,nulls); append32(result,19);
    for (const auto& dimension : dimensions) { append32(result,dimension.first); append32(result,dimension.second); }
    for (const auto& value : values) {
        append32(result,value ? static_cast<uint32_t>(value->size()) : UINT32_MAX);
        if (value) result.insert(result.end(),value->begin(),value->end());
    }
    return result;
}
void frame(const std::vector<std::string>& values, const std::vector<uint32_t>& oids,
           const std::vector<int16_t>& formats, const std::vector<bool>& nulls,
           const std::vector<Bytes>& expected, bool accepted = true) {
    ++controls;
    int descriptors[2];
    if (::socketpair(AF_UNIX,SOCK_STREAM,0,descriptors)) throw std::runtime_error("socketpair");
    dbms::SecureSocket sender(descriptors[0]),receiver(descriptors[1]);
    timeval timeout{2,0};
    if (::setsockopt(receiver.fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout))) throw std::runtime_error("timeout");
    dbms::PostgresProtocol protocol(sender);
    std::vector<dbms::PgColumnDescription> columns;
    for (size_t i = 0; i < oids.size(); ++i) {
        dbms::PgColumnDescription field;
        field.name = oids[i] == 19 ? "name" : "_name";
        field.typeOid = oids[i]; field.typeSize = oids[i] == 19 ? 64 : -1;
        field.formatCode = formats[i]; columns.push_back(field);
    }
    const bool sent = protocol.sendDataRow(values,columns,nulls);
    bool valid = sent == accepted;
    if (!accepted) {
        uint8_t byte = 0;
        const auto count = ::recv(receiver.fd,&byte,1,MSG_DONTWAIT);
        valid = valid && count == -1 && (errno == EAGAIN || errno == EWOULDBLOCK);
    } else if (sent) {
        Bytes body{0,static_cast<uint8_t>(values.size())};
        for (size_t i = 0; i < values.size(); ++i) {
            append32(body,nulls[i] ? UINT32_MAX : static_cast<uint32_t>(expected[i].size()));
            if (!nulls[i]) body.insert(body.end(),expected[i].begin(),expected[i].end());
        }
        Bytes wanted{'D'}; append32(wanted,static_cast<uint32_t>(body.size()+4));
        wanted.insert(wanted.end(),body.begin(),body.end());
        Bytes actual(wanted.size());
        const auto count = ::recv(receiver.fd,actual.data(),actual.size(),MSG_WAITALL);
        valid = count == static_cast<ssize_t>(wanted.size()) && actual == wanted;
    }
    if (!valid) ++failures;
    std::cout << "NAME_BINARY_NATIVE " << controls << " oid=" << oids.front() << " accepted=" << sent << " match=" << valid << '\n';
}
void check(uint32_t oid, const std::string& value, const Bytes& expected,
           bool null = false, int16_t format = 1, bool accepted = true) {
    frame({value},{oid},{format},{null},{expected},accepted);
}
}

int main() {
    std::string cjk,emoji;
    for (int i = 0; i < 21; ++i) cjk += "界";
    for (int i = 0; i < 15; ++i) emoji += "😀";
    emoji += "abc";
    std::vector<std::string> values = {"", "plain", "two words", "NULL", "a,b", "quote\"", "slash\\", "é", "中文", "😀",
        std::string(63,'n'),std::string(62,'n')+"x",cjk,emoji,"\tline\nbreak\r"};
    for (unsigned value = 1; value < 128; ++value) values.emplace_back(1,static_cast<char>(value));
    for (const auto& value : values) {
        check(19,value,bytes(value),false,0);
        check(19,value,bytes(value));
        const auto literal = "{"+quote(value)+",NULL,\"NULL\"}";
        check(1003,literal,array({value,std::nullopt,std::string("NULL")},{{3,1}}));
        check(1003,literal,bytes(literal),false,0);
    }
    for (uint32_t oid : {19U,1003U}) for (int16_t format : {0,1}) check(oid,"not a datum",{},true,format);
    check(1003,"{}",array({},{}));
    check(1003,"{NULL,NULL}",array({std::nullopt,std::nullopt},{{2,1}}));
    check(1003,"[-2:-1]={\"left\",NULL}",array({std::string("left"),std::nullopt},{{2,-2}}));
    const auto matrix = array({std::string("left"),std::nullopt,std::string("NULL"),std::string()},{{2,0},{2,-2}});
    check(1003,"[0:1][-2:-1]={{\"left\",NULL},{\"NULL\",\"\"}}",matrix);
    check(1003,"{{{{{{left}}}}}}",array({std::string("left")},{{1,1},{1,1},{1,1},{1,1},{1,1},{1,1}}));
    for (bool null : {false,true}) frame({"NULL","two words","{left,NULL}","tail"},{19,19,1003,25},{0,1,1,0},
        {null,false,false,false},{bytes("NULL"),bytes("two words"),array({std::string("left"),std::nullopt},{{2,1}}),bytes("tail")});
    for (uint32_t oid : {901919U,901003U,999999U,1000000U,4000000000U}) check(oid,"spoofed name",{},false,1,false);
    if (values.size() != 142 || controls != 584) { ++failures; std::cerr << "NAME_BINARY_NATIVE_INVENTORY " << controls << '\n'; }
    std::cout << "NAME_BINARY_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
