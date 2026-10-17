#include "network/PostgresProtocol.h"

#include <cerrno>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace {
using Bytes=std::vector<uint8_t>;
size_t controls=0, failures=0;
void append32(Bytes& bytes,uint32_t value) {
    for(int shift=24;shift>=0;shift-=8)bytes.push_back(static_cast<uint8_t>(value>>shift));
}
void append64(Bytes& bytes,uint64_t value) {
    for(int shift=56;shift>=0;shift-=8)bytes.push_back(static_cast<uint8_t>(value>>shift));
}
Bytes interval(int64_t micros,int32_t days=0,int32_t months=0) {
    Bytes result;append64(result,static_cast<uint64_t>(micros));
    append32(result,static_cast<uint32_t>(days));append32(result,static_cast<uint32_t>(months));return result;
}
Bytes array(const std::vector<std::optional<Bytes>>& values,
    const std::vector<std::pair<int32_t,int32_t>>& dimensions,uint32_t oid=1114) {
    Bytes result;append32(result,static_cast<uint32_t>(dimensions.size()));
    bool hasNull=false;for(const auto& value:values)hasNull=hasNull || !value;
    append32(result,hasNull?1:0);append32(result,oid);
    for(const auto& dimension:dimensions){append32(result,static_cast<uint32_t>(dimension.first));append32(result,static_cast<uint32_t>(dimension.second));}
    for(const auto& value:values){append32(result,value?static_cast<uint32_t>(value->size()):UINT32_MAX);if(value)result.insert(result.end(),value->begin(),value->end());}
    return result;
}
Bytes timestampBytes(int64_t micros) { Bytes result;append64(result,static_cast<uint64_t>(micros));return result; }
void frame(const std::vector<std::string>& values,const std::vector<uint32_t>& oids,
    const std::vector<int16_t>& formats,const std::vector<bool>& nulls,
    const std::vector<Bytes>& expected,bool accepted=true) {
    ++controls;
    int fds[2];if(::socketpair(AF_UNIX,SOCK_STREAM,0,fds))throw std::runtime_error("socketpair");
    dbms::SecureSocket sender(fds[0]),receiver(fds[1]);
    timeval timeout{2,0};if(::setsockopt(receiver.fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout)))throw std::runtime_error("timeout");
    dbms::PostgresProtocol protocol(sender);std::vector<dbms::PgColumnDescription> columns;
    for(size_t i=0;i<oids.size();++i){dbms::PgColumnDescription field;field.typeOid=oids[i];field.formatCode=formats[i];columns.push_back(field);}
    const bool sent=protocol.sendDataRow(values,columns,nulls);
    bool good=sent==accepted;
    if(!accepted){uint8_t byte=0;const auto read=::recv(receiver.fd,&byte,1,MSG_DONTWAIT);good=good && read==-1 && (errno==EAGAIN || errno==EWOULDBLOCK);}
    else if(sent){
        Bytes body;body.push_back(0);body.push_back(static_cast<uint8_t>(values.size()));
        for(size_t i=0;i<values.size();++i){append32(body,nulls[i]?UINT32_MAX:static_cast<uint32_t>(expected[i].size()));if(!nulls[i])body.insert(body.end(),expected[i].begin(),expected[i].end());}
        Bytes wanted{'D'};append32(wanted,static_cast<uint32_t>(body.size()+4));wanted.insert(wanted.end(),body.begin(),body.end());
        Bytes actual(wanted.size());const auto read=::recv(receiver.fd,actual.data(),actual.size(),MSG_WAITALL);good=read==static_cast<ssize_t>(actual.size()) && actual==wanted;
    }
    failures+=!good;
    std::cout<<"TIMESTAMP_BINARY_NATIVE "<<controls<<" oid="<<oids.front()<<" accepted="<<sent<<" pass="<<good<<'\n';
}
void check(uint32_t oid,const std::string& value,const Bytes& expected,bool null=false,bool accepted=true,int16_t format=1) {
    frame({value},{oid},{format},{null},{expected},accepted);
}
} // namespace

int main() {
    const std::vector<std::pair<std::string,int64_t>> cases={
        {"1970-01-01 00:00:00",-946684800000000LL},
        {"1969-12-31 23:59:59.999999",-946684800000001LL},
        {"2000-01-01 00:00:00",0},{"2000-01-01 00:00:00.000001",1},
        {"1999-12-31 23:59:59.999999",-1},
        {"2000-02-29 03:04:05.123456",5108645123456LL},
        {"0001-01-01 00:00:00.000001",-63082281599999999LL},
        {"9999-12-31 23:59:59.999999",252455615999999999LL},
        {"1900-03-01 12:34:56.654321",-3150530703345679LL},
        {"2024-02-29 12:34:56.000006",762525296000006LL},
        {"2000-01-01 00:00:00.0000004",0},{"2000-01-01 00:00:00.0000005",0},
        {"2000-01-01 00:00:00.0000015",2},{"2000-01-01 00:00:00.0000025",2},
        {"2000-01-01 00:00:00.0000035",4},{"2000-01-01 00:00:00.1234565",123456},
        {"2000-01-01 00:00:00.1234575",123458},{"2000-01-01 00:00:00.9999995",1000000},
        {"1999-12-31 23:59:59.9999995",0},{"2000-12-31 23:59:59.9999995",31622400000000LL},
        {"infinity",INT64_MAX},{"-infinity",INT64_MIN}};
    const std::vector<std::tuple<std::string,int64_t,int64_t>> zones={
        {"2000-01-01 00:00:00.000001+08:00",1,-28799999999LL},
        {"2000-01-01 00:00:00.000001-05:30",1,19800000001LL},
        {"1999-12-31 23:59:59.999999+15:59",-1,-57540000001LL},
        {"2000-01-01 00:00:00.000001-15:59",1,57540000001LL},
        {"2000-01-01 00:00:00.000001Z",1,1},
        {"2000-01-01 00:00:00.000001+05",1,-17999999999LL}};
    for(const uint32_t oid:{1114U,1184U}) {
        const uint32_t arrayOid=oid==1114?1115:1185;
        for(const auto& item:cases) {
            std::string value=item.first;
            if(oid==1184 && value.find("infinity")==std::string::npos)value+="+00";
            const auto bytes=timestampBytes(item.second);
            check(oid,value,bytes);
            check(arrayOid,"{\""+value+"\",NULL}",array({bytes,std::nullopt},{{2,1}},oid));
        }
        for(const auto& [value,local,utc]:zones) {
            const auto bytes=timestampBytes(oid==1114?local:utc);
            check(oid,value,bytes);
            check(arrayOid,"{\""+value+"\",NULL}",array({bytes,std::nullopt},{{2,1}},oid));
        }
        for(const auto& [value,micros]:std::vector<std::pair<std::string,int64_t>>{
                {"2000-01-01",0},{"2000-01-01T00:00:00.000001",1},
                {" 2000-01-01 00:00:00.000001 ",1}}) {
            const auto bytes=timestampBytes(micros);
            check(oid,value,bytes);
            check(arrayOid,"{\""+value+"\",NULL}",array({bytes,std::nullopt},{{2,1}},oid));
        }
        check(oid,"invalid carrier",{},true);check(arrayOid,"not an array",{},true);
        check(arrayOid,"{}",array({}, {},oid));
        check(arrayOid,"{NULL,NULL}",array({std::nullopt,std::nullopt},{{2,1}},oid));
        check(arrayOid,"[-3:-1]={\"2000-01-01 00:00:00.000001+00\",NULL,\"1999-12-31 23:59:59.999999+00\"}",
            array({timestampBytes(1),std::nullopt,timestampBytes(-1)},{{3,-3}},oid));
        check(arrayOid,"{{\"2000-01-01 00:00:00.000001\",NULL},{infinity,-infinity}}",
            array({timestampBytes(1),std::nullopt,timestampBytes(INT64_MAX),timestampBytes(INT64_MIN)},{{2,1},{2,1}},oid));
        check(arrayOid,"{{{{{{\"2000-01-01 00:00:00.000001\"}}}}}}",
            array({timestampBytes(1)},{{1,1},{1,1},{1,1},{1,1},{1,1},{1,1}},oid));
        const std::string text="2000-01-01 00:00:00.000001";
        check(oid,text,Bytes(text.begin(),text.end()),false,true,0);
        check(arrayOid,"{}",{'{','}'},false,true,0);
    }
    check(1083,"00:00:00.000001",timestampBytes(1));
    check(1186,"1 day",interval(0,1));check(23,"-7",{255,255,255,249});
    check(1700,"1.00",{0,1,0,0,0,0,0,2,0,1});
    const std::vector<std::string> invalid={
        "","bad","2000-02-30 00:00:00","1900-02-29 00:00:00",
        "2000-00-01 00:00:00","2000-01-00 00:00:00","0000-01-01 00:00:00",
        "2000-01-01 25:00:00","2000-01-01 12:60:00","2000-01-01 12:00:61",
        "2000-01-01 12:00:-1","2000-01-01 12:00:00.","2000-01-01 12:00:00.1x",
        "2000-01-01 12:00:00:00","2000-01-01 garbage","2000-01-01 00:00:00+16",
        "2000-01-01 00:00:00+08:60","2000-01-01 00:00:00+ab",
        "2000-01-01 00:00:00+08:00Z","2000-01-01 00:00:00garbage"};
    for(const auto& value:invalid) {
        for(const uint32_t oid:{1114U,1184U}) {
            check(oid,value,{},false,false);
            check(oid==1114?1115:1185,"{\""+value+"\"}",{},false,false);
        }
    }
    for(const uint32_t oid:{1115U,1185U})
        for(const std::string value:{"{{\"2000-01-01\"},{\"2000-01-01\",\"2000-01-02\"}}",
                "[0:2]={\"2000-01-01\",\"2000-01-02\"}","bare"})check(oid,value,{},false,false);
    frame({"2000-01-01 00:00:00.000001","{\"1999-12-31 23:59:59.999999+00\",NULL}"},
        {1114,1185},{1,1},{false,false},{timestampBytes(1),array({timestampBytes(-1),std::nullopt},{{2,1}},1184)});
    frame({"2000-01-01 00:00:00.000001","bad"},{1114,1184},{1,1},{false,false},{},false);
    check(1114,"2000-01-01 00:00:00",{},false,false,2);
    std::cout<<"[TIMESTAMP BINARY RESULT] native controls="<<controls<<" failures="<<failures<<'\n';
    return cases.size()==22 && zones.size()==6 && invalid.size()==20 && controls==235 && !failures?0:1;
}
