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
    const std::vector<std::pair<int32_t,int32_t>>& dimensions, uint32_t elementOid=1083) {
    Bytes result;append32(result,static_cast<uint32_t>(dimensions.size()));
    bool hasNull=false;for(const auto& value:values)hasNull=hasNull || !value;
    append32(result,hasNull?1:0);append32(result,elementOid);
    for(const auto& dimension:dimensions){append32(result,static_cast<uint32_t>(dimension.first));append32(result,static_cast<uint32_t>(dimension.second));}
    for(const auto& value:values){append32(result,value?static_cast<uint32_t>(value->size()):UINT32_MAX);if(value)result.insert(result.end(),value->begin(),value->end());}
    return result;
}
Bytes clockBytes(int64_t micros) { Bytes result;append64(result,static_cast<uint64_t>(micros));return result; }
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
    std::cout<<"TIME_BINARY_NATIVE "<<controls<<" oid="<<oids.front()<<" accepted="<<sent<<" pass="<<good<<'\n';
}
void check(uint32_t oid,const std::string& value,const Bytes& expected,bool null=false,bool accepted=true,int16_t format=1) {
    frame({value},{oid},{format},{null},{expected},accepted);
}
} // namespace

int main() {
    const std::vector<std::pair<std::string,int64_t>> cases={
        {"00:00:00",0},{"00:00:00.000001",1},{"00:00:00.0000015",2},
        {"00:00:00.9999995",1000000},{"00:00:59.9999995",60000000},
        {"00:59:59.9999995",3600000000LL},{"23:59:59.9999995",86400000000LL},
        {"12:34:56",45296000000LL},{"12:34:56.123456",45296123456LL},
        {"12:34:56.0000015",45296000002LL},{"12:34:56.9999995",45297000000LL},
        {"01:02:03.1",3723100000LL},{"01:02:03.000001",3723000001LL},
        {"23:59:59.999999",86399999999LL},{"24:00:00",86400000000LL},
        {"24:00:00.0000001",86400000000LL},{"00:00:00.0000004",0},
        {"00:00:00.0000005",0},{"00:00:00.0000025",2},{"00:00:00.0000035",4},
        {"00:00:00.1234564",123456},{"00:00:00.1234565",123456},
        {"00:00:00.1234575",123458},{"03:04:05.000006",11045000006LL},
        {"0:0:0",0},{"12:34",45240000000LL},{"24:00",86400000000LL},
        {"23:59:60",86400000000LL},{"12:34:60",45300000000LL},
        {" 12:34:56.123456 ",45296123456LL}};
    for(const auto& item:cases) {
        const auto bytes=clockBytes(item.second);
        check(1083,item.first,bytes);
        check(1183,"{\""+item.first+"\",NULL}",array({bytes,std::nullopt},{{2,1}}));
    }
    check(1083,"invalid carrier",{},true);check(1183,"not an array",{},true);
    check(1183,"{}",array({},{}));check(1183,"{NULL,NULL}",array({std::nullopt,std::nullopt},{{2,1}}));
    check(1183,"[-3:-1]={00:00:00.000001,NULL,24:00:00}",array({clockBytes(1),std::nullopt,clockBytes(86400000000LL)},{{3,-3}}));
    check(1183,"[0:1][-2:-1]={{00:00:00.000001,NULL},{24:00:00,12:34:56.123456}}",array({clockBytes(1),std::nullopt,clockBytes(86400000000LL),clockBytes(45296123456LL)},{{2,0},{2,-2}}));
    check(1183,"{{{{{{00:00:00.000001}}}}}}",array({clockBytes(1)},{{1,1},{1,1},{1,1},{1,1},{1,1},{1,1}}));
    const std::string text="12:34:56.123456";
    check(1083,text,Bytes(text.begin(),text.end()),false,true,0);check(1183,"{}",{'{','}'},false,true,0);
    check(23,"-7",{255,255,255,249});check(1186,"1 day",interval(0,1));check(16,"t",{1});check(1700,"1.00",{0,1,0,0,0,0,0,2,0,1});
    for(const std::string value:{"","bad","-00:00:01","ab:00:00","25:00:00","24:01:00","24:00:01",
            "24:00:00.000001","12:60:00","12:00:61","12:00:-01","12:00:00.","12:00:00.1x",
            "12:00:00:00","100:00:00","00:-1:00","NaN","infinity","1 day","0 days 12:00:00"}) {
        check(1083,value,{},false,false);check(1183,"{\""+value+"\"}",{},false,false);
    }
    for(const std::string value:{"{{00:00:00},{00:00:00,01:00:00}}","[0:2]={00:00:00,01:00:00}","bare"})check(1183,value,{},false,false);
    // Actual PG18.6 wire observations: the clock is local, the offset is
    // seconds WEST, and each non-null timetz datum occupies exactly 12 bytes.
    auto zoned=[](int64_t micros,int32_t west) {
        Bytes bytes=clockBytes(micros);append32(bytes,static_cast<uint32_t>(west));return bytes;
    };
    check(1266,"12:34:56+00",zoned(45296000000LL,0));check(199,"{1}",{},false,false);
    const std::vector<std::tuple<std::string,int64_t,int32_t>> timetzCases={
        {"00:00:00+00",0,0},{"24:00:00+00",86400000000LL,0},
        {"00:00:00.000001+00",1,0},{"23:59:59.999999+00",86399999999LL,0},
        {"12:34:56.123456+08:30",45296123456LL,-30600},
        {"12:34:56.123456-08:30",45296123456LL,30600},
        {"12:34:56+15:59:59",45296000000LL,-57599},
        {"12:34:56-15:59:59",45296000000LL,57599},
        {"12:34:56+00:00:01",45296000000LL,-1},
        {"12:34:56-00:00:01",45296000000LL,1},
        {"12:34:56+00:30",45296000000LL,-1800},
        {"12:34:56-00:30",45296000000LL,1800},
        {"12:34:56+08",45296000000LL,-28800},
        {"12:34:56-08",45296000000LL,28800},
        {"12:34:56+0830",45296000000LL,-30600},
        {"0:0:0+00",0,0},{"12:34+00",45240000000LL,0},
        {"12:34:56.9999999+00",45297000000LL,0},
        {"23:59:60+00",86400000000LL,0}};
    for(const auto& [value,micros,west]:timetzCases) {
        const Bytes bytes=zoned(micros,west);
        check(1266,value,bytes);
        check(1270,"{\""+value+"\",NULL}",array({bytes,std::nullopt},{{2,1}},1266));
    }
    check(1266,"invalid carrier",{},true);check(1270,"not an array",{},true);
    check(1270,"{}",array({},{},1266));
    check(1270,"{NULL,NULL}",array({std::nullopt,std::nullopt},{{2,1}},1266));
    check(1270,"[0:1]={24:00:00+00,12:34:56-15:59:59}",array({zoned(86400000000LL,0),zoned(45296000000LL,57599)},{{2,0}},1266));
    check(1270,"{{12:34:56+08,NULL},{24:00:00+00,00:00:00-08}}",array({zoned(45296000000LL,-28800),std::nullopt,zoned(86400000000LL,0),zoned(0,28800)},{{2,1},{2,1}},1266));
    const std::vector<std::string> invalidTimetz={"","bad","12:34:56","25:00:00+00","24:00:01+00","24:00:00.000001+00",
        "12:60:00+00","12:00:61+00","12:34:56+16","12:34:56-16","12:34:56+00:60","12:34:56+00:00:60",
        "12:34:56+","12:34:56+08:","12:34:56+08:00:","12:34:56+08:00:00:00","12:34:56+08trailing","12:34:56-083045"};
    for(const auto& value:invalidTimetz) {
        check(1266,value,{},false,false);check(1270,"{\""+value+"\"}",{},false,false);
    }
    frame({"12:34:56+08","bad"},{1266,1266},{1,1},{false,false},{},false);
    frame({"12:34:56+08","12:34:56-08"},{1266,1266},{1,1},{false,false},{zoned(45296000000LL,-28800),zoned(45296000000LL,28800)});
    frame({"00:00:00.000001","{24:00:00,NULL}"},{1083,1183},{1,1},{false,false},{clockBytes(1),array({clockBytes(86400000000LL),std::nullopt},{{2,1}})});
    frame({"00:00:00.000001","bad"},{1083,1083},{1,1},{false,false},{},false);
    check(1083,"00:00:00",{},false,false,2);
    std::cout<<"[TIME BINARY RESULT] native controls="<<controls<<" failures="<<failures<<'\n';
    return cases.size()==30 && timetzCases.size()==19 && invalidTimetz.size()==18 && controls==203 && !failures?0:1;
}
