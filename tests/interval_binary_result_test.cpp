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
    const std::vector<std::pair<int32_t,int32_t>>& dimensions) {
    Bytes result;append32(result,static_cast<uint32_t>(dimensions.size()));
    bool hasNull=false;for(const auto& value:values)hasNull=hasNull || !value;
    append32(result,hasNull?1:0);append32(result,1186);
    for(const auto& dimension:dimensions){append32(result,static_cast<uint32_t>(dimension.first));append32(result,static_cast<uint32_t>(dimension.second));}
    for(const auto& value:values){append32(result,value?static_cast<uint32_t>(value->size()):UINT32_MAX);if(value)result.insert(result.end(),value->begin(),value->end());}
    return result;
}
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
    std::cout<<"INTERVAL_BINARY_NATIVE "<<controls<<" oid="<<oids.front()<<" accepted="<<sent<<" pass="<<good<<'\n';
}
void check(uint32_t oid,const std::string& value,const Bytes& expected,bool null=false,bool accepted=true,int16_t format=1) {
    frame({value},{oid},{format},{null},{expected},accepted);
}
} // namespace

int main() {
    const auto maximum=std::numeric_limits<int64_t>::max(),minimum=std::numeric_limits<int64_t>::min();
    const auto max32=std::numeric_limits<int32_t>::max(),min32=std::numeric_limits<int32_t>::min();
    const std::vector<std::tuple<std::string,int64_t,int32_t,int32_t>> cases={
        {"0",0,0,0},{"1 microsecond",1,0,0},{"-1 microsecond",-1,0,0},
        {"1 second",1000000,0,0},{"-1 second",-1000000,0,0},
        {"123.456789 seconds",123456789,0,0},{"-123.456789 seconds",-123456789,0,0},
        {"1 mon 2 days 03:04:05.000006",11045000006LL,2,1},
        {"-1 mon -2 days -03:04:05.000006",-11045000006LL,-2,-1},
        {"1 mon -2 days +03:04:05.000006",11045000006LL,-2,1},
        {"-1 mon 2 days -03:04:05.000006",-11045000006LL,2,-1},
        {"24 hours",86400000000LL,0,0},{"1 day",0,1,0},{"1 year",0,0,12},
        {"0.9 years",0,0,11},{"1.5 months",0,15,1},{"-1.5 months",0,-15,-1},
        {"1.5 weeks",43200000000LL,10,0},{"1.5 days",43200000000LL,1,0},
        {"1.5 microseconds",1,0,0},{"00:00:00.0000015",2,0,0},
        {"P1Y2M3DT4H5M6.000007S",14706000007LL,3,14},{"@ 1 day 2 hours ago",-7200000000LL,-1,0},
        {"1-2",0,0,14},{"-0-2",0,0,-2},{"2147483647 months",0,0,max32},
        {"-2147483648 months",0,0,min32},{"2147483647 days",0,max32,0},{"-2147483648 days",0,min32,0},
        {"9223372036854775807 microseconds",maximum,0,0},{"-9223372036854775808 microseconds",minimum,0,0},
        {"178956971 years -5 months",0,0,max32},{"infinity",maximum,max32,max32},
        {"-infinity",minimum,min32,min32},{"+infinity",maximum,max32,max32}};
    for(const auto& item:cases){
        const auto& value=std::get<0>(item);const auto encoded=interval(std::get<1>(item),std::get<2>(item),std::get<3>(item));
        check(1186,value,encoded);check(1187,"{\""+value+"\",NULL}",array({encoded,std::nullopt},{{2,1}}));
    }
    check(1186,"not a datum",{},true);check(1187,"not an array",{},true);
    check(1187,"{}",array({},{}));check(1187,"{NULL,NULL}",array({std::nullopt,std::nullopt},{{2,1}}));
    check(1187,"[-3:-2]={\"1 day\",NULL}",array({interval(0,1),std::nullopt},{{2,-3}}));
    check(1187,"[0:1][-2:-1]={{\"1 mon\",NULL},{\"24 hours\",\"-1 microsecond\"}}",array({interval(0,0,1),std::nullopt,interval(86400000000LL),interval(-1)},{{2,0},{2,-2}}));
    check(1187,"{{{{{{\"1 day\"}}}}}}",array({interval(0,1)},{{1,1},{1,1},{1,1},{1,1},{1,1},{1,1}}));
    check(1186,"1 day",{'1',' ','d','a','y'},false,true,0);check(1186,"infinity",{'i','n','f','i','n','i','t','y'},false,true,0);
    check(1187,"{}",{'{','}'},false,true,0);
    check(23,"-7",{255,255,255,249});check(1700,"1.00",{0,1,0,0,0,0,0,2,0,1});check(16,"t",{1});
    for(const std::string value:{"","abc","1 day garbage","2147483648 months","-2147483649 months", "2147483648 days","-2147483649 days","9223372036854775808 microseconds","-9223372036854775809 microseconds","1e1000 days","NaN"}){
        check(1186,value,{},false,false);check(1187,"{\""+value+"\"}",{},false,false);
    }
    for(const std::string value:{"{{1},{2,3}}","[0:2]={1,2}","bare"})check(1187,value,{},false,false);
    check(1266,"12:34:56+00",{},false,false);check(114,"1",{},false,false);check(199,"{1}",{},false,false);
    check(1186," Infinity ",interval(maximum,max32,max32));check(1186,"\t+INFINITY\n",interval(maximum,max32,max32));
    check(1186," -Infinity ",interval(minimum,min32,min32));check(1186," \t-infinity",interval(minimum,min32,min32));
    frame({"1 day","{\"-1 microsecond\",NULL}"},{1186,1187},{1,1},{false,false},{interval(0,1),array({interval(-1),std::nullopt},{{2,1}})});
    frame({"1 day","bad"},{1186,1186},{1,1},{false,false},{},false);
    std::cout<<"[INTERVAL BINARY RESULT] native controls="<<controls<<" failures="<<failures<<'\n';
    return cases.size()==35 && controls==117 && !failures?0:1;
}
