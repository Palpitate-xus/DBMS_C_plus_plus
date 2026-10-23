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
using Bytes=std::vector<uint8_t>;
size_t controls=0,failures=0;
void append32(Bytes& output,uint32_t value) {
    for(int shift=24;shift>=0;shift-=8)output.push_back(static_cast<uint8_t>(value>>shift));
}
Bytes integer(uint32_t value) {Bytes result;append32(result,value);return result;}
Bytes text(const std::string& value) {return Bytes(value.begin(),value.end());}
Bytes array(const std::vector<std::optional<uint32_t>>& values,
            const std::vector<std::pair<int32_t,int32_t>>& dimensions) {
    Bytes result;append32(result,dimensions.size());
    bool nulls=false;for(const auto& value:values)nulls=nulls || !value;
    append32(result,nulls);append32(result,26);
    for(const auto& dimension:dimensions) {append32(result,dimension.first);append32(result,dimension.second);}
    for(const auto& value:values) {
        append32(result,value?4:UINT32_MAX);
        if(value)append32(result,*value);
    }
    return result;
}
void frame(const std::vector<std::string>& values,const std::vector<uint32_t>& oids,
           const std::vector<int16_t>& formats,const std::vector<bool>& nulls,
           const std::vector<Bytes>& expected,bool accepted=true) {
    ++controls;
    int descriptors[2];
    if(::socketpair(AF_UNIX,SOCK_STREAM,0,descriptors))throw std::runtime_error("socketpair");
    dbms::SecureSocket sender(descriptors[0]),receiver(descriptors[1]);
    timeval timeout{2,0};
    if(::setsockopt(receiver.fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout)))throw std::runtime_error("timeout");
    dbms::PostgresProtocol protocol(sender);
    std::vector<dbms::PgColumnDescription> columns;
    for(size_t i=0;i<oids.size();++i) {
        dbms::PgColumnDescription column;column.name="oid";column.typeOid=oids[i];
        column.typeSize=oids[i]==26?4:-1;column.formatCode=formats[i];columns.push_back(column);
    }
    const bool sent=protocol.sendDataRow(values,columns,nulls);
    bool valid=sent==accepted;
    if(!accepted) {
        uint8_t byte=0;
        const auto count=::recv(receiver.fd,&byte,1,MSG_DONTWAIT);
        valid=valid && count==-1 && (errno==EAGAIN || errno==EWOULDBLOCK);
    } else if(sent) {
        Bytes body{0,static_cast<uint8_t>(values.size())};
        for(size_t i=0;i<values.size();++i) {
            append32(body,nulls[i]?UINT32_MAX:static_cast<uint32_t>(expected[i].size()));
            if(!nulls[i])body.insert(body.end(),expected[i].begin(),expected[i].end());
        }
        Bytes wanted{'D'};append32(wanted,body.size()+4);wanted.insert(wanted.end(),body.begin(),body.end());
        Bytes actual(wanted.size());
        const auto count=::recv(receiver.fd,actual.data(),actual.size(),MSG_WAITALL);
        valid=valid && count==static_cast<ssize_t>(wanted.size()) && actual==wanted;
    }
    failures+=!valid;
    std::cout<<"OID_BINARY_NATIVE "<<controls<<" type="<<oids.front()<<" sent="<<sent<<" MATCH="<<valid<<'\n';
}
}

int main() {
    for(uint32_t value:{0U,1U,2U,127U,128U,255U,256U,32767U,32768U,65535U,65536U,
                       2147483647U,2147483648U,4294967294U,4294967295U}) {
        const auto input=std::to_string(value);
        frame({input},{26},{1},{false},{integer(value)});
        frame({input},{26},{0},{false},{text(input)});
        frame({"{"+input+",NULL,"+input+"}"},{1028},{1},{false},{array({value,std::nullopt,value},{{3,1}})});
        frame({input,"payload"},{26,25},{1,0},{false,false},{integer(value),text("payload")});
    }
    frame({""},{26},{1},{true},{{}});
    frame({"{}"},{1028},{1},{false},{array({},{})});
    frame({"{NULL,NULL}"},{1028},{1},{false},{array({std::nullopt,std::nullopt},{{2,1}})});
    frame({"[-2:0]={0,NULL,4294967295}"},{1028},{1},{false},{array({0U,std::nullopt,UINT32_MAX},{{3,-2}})});
    frame({"{{0,1},{2147483648,4294967295}}"},{1028},{1},{false},{array({0U,1U,2147483648U,UINT32_MAX},{{2,1},{2,1}})});
    // These are malformed canonical output cells, not SQL OID input grammar.
    const std::vector<std::string> invalid={"","-1","-2147483648","4294967296","18446744073709551616",
        "1x"," 1","1 ","+1","NaN","1.0",std::string("1\0x",3)};
    for(const auto& value:invalid) {
        frame({value},{26},{1},{false},{{}},false);
        frame({"prefix",value},{25,26},{0,1},{false,false},{{},{}},false);
    }
    frame({"1"},{999002},{1},{false},{{}},false);
    frame({"{}"},{999003},{1},{false},{{}},false);
    frame({"{0,4294967296}"},{1028},{1},{false},{{}},false);
    frame({"[1:2]={0}"},{1028},{1},{false},{{}},false);
    frame({"not a number"},{26},{1},{true},{{}});
    std::cout<<"OID_BINARY_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    return controls==94 && !failures?0:1;
}
