#include "network/NetworkServer.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "process/StatementNotice.h"
#include "utils/permissions.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

extern dbms::StorageEngine g_engine;
// Root has an existing, unaccepted typed legacy diagnostic draft; use its
// actual producer when present, and stdout only in the accepted legacy path.
// This is a test-only weak reference, not adoption of that production API.
namespace dbms {
void emitNotice(std::string) __attribute__((weak));
void emitWarning(std::string) __attribute__((weak));
}
namespace {
struct Notice { std::string severity, code, message; };
struct Event { bool typed; Notice notice; };
struct Case { std::vector<Event> events; bool error = false; };
const std::vector<Case> cases = {
    {{{false,{"NOTICE","00000","legacy only"}}}},
    {{{true,{"NOTICE","42P07","typed only"}}}},
    {{{false,{"NOTICE","00000","first"}},{true,{"NOTICE","42P07","second"}}}},
    {{{true,{"NOTICE","42P07","first"}},{false,{"NOTICE","00000","second"}}}},
    {{{false,{"NOTICE","00000","first"}},{true,{"NOTICE","42P07","middle"}},{false,{"WARNING","01000","last"}}}},
    {{{true,{"NOTICE","42P07","first"}},{false,{"NOTICE","00000","middle"}},{true,{"WARNING","22023","last"}}}},
    {{{false,{"NOTICE","00000","first"}},{true,{"NOTICE","42P07","second"}},{true,{"WARNING","22023","third"}},{false,{"NOTICE","00000","last"}}}},
    {{{false,{"NOTICE","00000","identical payload"}},{true,{"NOTICE","42P07","identical payload"}},{false,{"WARNING","01000","identical payload"}}}},
    {{{false,{"NOTICE","00000","user (SQLSTATE 42P07)"}},{true,{"WARNING","22023","typed warning"}},{false,{"NOTICE","00000","last"}}}},
    {{{false,{"NOTICE","00000","first"}},{true,{"NOTICE","42P07","multiline\nNOTICE: payload \xE4\xB8\xAD"}},{false,{"NOTICE","00000","last"}}}},
    {{{false,{"NOTICE","00000","before error"}},{true,{"NOTICE","42P07","typed before error"}}},true},
    {{{true,{"NOTICE","42P07","typed before error"}},{false,{"NOTICE","00000","last before error"}}},true},
};
void require(bool good, const std::string& message) {
    if (!good) throw std::runtime_error(message);
}
void append32(std::string& value, uint32_t number) {
    const auto net = htonl(number); value.append(reinterpret_cast<const char*>(&net),4);
}
uint32_t read32(const char* data) {
    uint32_t number; std::memcpy(&number,data,4); return ntohl(number);
}
void cstring(std::string& output, const std::string& value) { output += value; output.push_back('\0'); }
void sendAll(int fd, const std::string& bytes) {
    size_t sent=0;
    while (sent<bytes.size()) {
        const auto count=::send(fd,bytes.data()+sent,bytes.size()-sent,MSG_NOSIGNAL);
        if (count<0 && errno==EINTR) continue;
        require(count>0,"send failed"); sent+=static_cast<size_t>(count);
    }
}
void receiveAll(int fd, char* bytes, size_t size) {
    size_t received=0;
    while (received<size) {
        const auto count=::recv(fd,bytes+received,size-received,0);
        if (count<0 && errno==EINTR) continue;
        require(count>0,"receive failed or premature EOF"); received+=static_cast<size_t>(count);
    }
}
std::string typed(char type,const std::string& body={}) {
    std::string output(1,type); append32(output,static_cast<uint32_t>(body.size()+4)); return output+body;
}
using Frames=std::vector<std::pair<char,std::string>>;
Frames untilReady(int fd) {
    Frames result;
    for (size_t count=0;count<256;++count) {
        char header[5]; receiveAll(fd,header,5);
        const auto length=read32(header+1); require(length>=4 && length<1024*1024,"frame size");
        std::string body(length-4,'\0'); if (!body.empty()) receiveAll(fd,body.data(),body.size());
        result.emplace_back(header[0],std::move(body));
        if (header[0]=='Z') return result;
    }
    throw std::runtime_error("no ReadyForQuery");
}
std::map<char,std::string> diagnostic(const std::string& body) {
    std::map<char,std::string> fields; size_t offset=0;
    while (offset<body.size() && body[offset]) {
        const char key=body[offset++]; const auto end=body.find('\0',offset);
        require(end!=std::string::npos && !fields.count(key),"diagnostic framing");
        fields.emplace(key,body.substr(offset,end-offset)); offset=end+1;
    }
    require(offset+1==body.size() && body[offset]=='\0',"diagnostic terminator");
    return fields;
}
struct Client {
    int fd=-1;
    explicit Client(int port) {
        for (int attempt=0;attempt<200;++attempt) {
            fd=::socket(AF_INET,SOCK_STREAM,0); require(fd>=0,"socket");
            timeval timeout{10,0};
            ::setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
            ::setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));
            sockaddr_in address{}; address.sin_family=AF_INET; address.sin_port=htons(static_cast<uint16_t>(port));
            address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
            if (::connect(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address))==0) break;
            ::close(fd);fd=-1;std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        require(fd>=0,"server did not start");
        std::string startup;append32(startup,196608);cstring(startup,"user");cstring(startup,"r311_owner");
        cstring(startup,"database");cstring(startup,"r311_notice_order");startup.push_back('\0');
        std::string packet;append32(packet,static_cast<uint32_t>(startup.size()+4));sendAll(fd,packet+startup);
        const auto frames=untilReady(fd);
        require(frames.back()==std::make_pair('Z',std::string("I")),"startup ReadyForQuery");
        bool authenticated=false;
        for (const auto& frame:frames) {
            require(frame.first!='E',"startup diagnostic "+frame.second);
            if (frame.first=='R') authenticated=frame.second==std::string(4,'\0');
        }
        require(authenticated,"AuthenticationOk");
    }
    ~Client() { if (fd>=0) {::shutdown(fd,SHUT_RDWR);::close(fd);} }
    Frames query(const std::string& sql,bool extended) {
        if (!extended) sendAll(fd,typed('Q',sql+std::string(1,'\0')));
        else {
            std::string parse(1,'\0');cstring(parse,sql);parse.append(2,'\0');
            sendAll(fd,typed('P',parse)+typed('B',std::string(8,'\0'))+
                typed('E',std::string(5,'\0'))+typed('S'));
        }
        return untilReady(fd);
    }
};
std::atomic<size_t> controls{0},failures{0};
void check(const Frames& frames,const std::vector<Notice>& expected,bool extended,bool error,const std::string& label) {
    ++controls;bool good=true;std::vector<Notice> received;std::string kinds;
    try {
        for (const auto& frame:frames) {
            kinds+=frame.first;
            if (frame.first=='N') {
                const auto fields=diagnostic(frame.second);
                received.push_back({fields.at('S'),fields.at('C'),fields.at('M')});
                if (fields.count('V')) good=good && fields.at('V')==fields.at('S');
            }
            if (frame.first=='E') {const auto fields=diagnostic(frame.second);good=good && fields.at('C')=="22023" && fields.at('M')=="component execution failure";}
            if (frame.first=='C') good=good && frame.second==std::string("CREATE SEQUENCE\0",16);
            if (frame.first=='1' || frame.first=='2') good=good && frame.second.empty();
        }
        good=good && kinds==(extended?"12":"")+std::string(expected.size(),'N')+(error?"EZ":"CZ") &&
            frames.back()==std::make_pair('Z',std::string("I")) && received.size()==expected.size();
        for (size_t index=0;good && index<expected.size();++index)
            good=received[index].severity==expected[index].severity && received[index].code==expected[index].code && received[index].message==expected[index].message;
    } catch (const std::exception&) {good=false;}
    if (!good) ++failures;
    std::ostringstream log;log<<"STATEMENT_NOTICE_ORDER "<<label<<" pass="<<good<<" kinds="<<kinds;
    if (!good) for (const auto& notice:received) log<<" ["<<notice.severity<<','<<notice.code<<','<<notice.message<<']';
    std::cout<<log.str()<<std::endl;
}
void runClient(int port,size_t peer) {
    Client client(port);
    for (bool extended:{false,true}) for (size_t index=0;index<cases.size();++index) {
        const std::string suffix=std::to_string(index)+"_peer_"+std::to_string(peer);
        const std::string prefix="peer"+std::to_string(peer)+" ";std::vector<Notice> expected;
        for (const auto& event:cases[index].events) expected.push_back({event.notice.severity,event.notice.code,prefix+event.notice.message});
        check(client.query("CREATE SEQUENCE r311_case_"+suffix,extended),expected,extended,cases[index].error,"case"+suffix+(extended?" extended":" simple"));
        check(client.query("CREATE SEQUENCE r311_quiet_"+suffix,extended),{},extended,false,"no-stale-"+suffix+(extended?" extended":" simple"));
    }
}
} // namespace

// Only tests/test_stubs.cpp calls this optional hook; real Main is untouched.
bool dbmsTestExecute(const std::string& sql,Session&) {
    const std::string marker="r311_case_";const auto begin=sql.find(marker);
    if (begin==std::string::npos) return false;
    const size_t index=static_cast<size_t>(std::stoul(sql.substr(begin+marker.size())));
    const auto peerMarker=sql.find("_peer_",begin);require(peerMarker!=std::string::npos,"callback request identity");
    const auto peer=std::stoul(sql.substr(peerMarker+6));const std::string prefix="peer"+std::to_string(peer)+" ";
    const auto& scenario=cases.at(index);
    for (const auto& event:scenario.events) {
        if (event.typed) dbms::emitStatementNotice({event.notice.severity,event.notice.code,prefix+event.notice.message});
        else if (event.notice.severity=="NOTICE" && dbms::emitNotice) dbms::emitNotice(prefix+event.notice.message);
        else if (event.notice.severity=="WARNING" && dbms::emitWarning) dbms::emitWarning(prefix+event.notice.message);
        else std::cout<<event.notice.severity<<": "<<prefix<<event.notice.message<<std::endl;
    }
    if (scenario.error) throw dbms::DbError("22023","component execution failure");
    return false;
}

int main() {
    try {
        require(g_engine.createDatabase("r311_notice_order")==dbms::DBStatus::OK,"private database setup");
        require(createUser({"r311_owner","unused-R311-fixture-secret!","1"})==0,"private role setup");
        {std::ofstream hba("pg_hba.conf");hba<<"host all r311_owner 127.0.0.1/32 trust\n";require(static_cast<bool>(hba),"fixture hba");}
        int listener=::socket(AF_INET,SOCK_STREAM,0);require(listener>=0,"reserve socket");
        sockaddr_in address{};address.sin_family=AF_INET;address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
        require(::bind(listener,reinterpret_cast<sockaddr*>(&address),sizeof(address))==0,"reserve bind");
        socklen_t length=sizeof(address);require(::getsockname(listener,reinterpret_cast<sockaddr*>(&address),&length)==0,"reserve port");
        const int port=ntohs(address.sin_port);::close(listener);
        bool serverResult=false;std::thread server([&]{serverResult=dbms::startServer(port,true);});
        struct Stop {std::thread& thread;~Stop(){dbms::requestServerShutdown();if(thread.joinable())thread.join();}} stop{server};
        runClient(port,0);
        std::vector<std::thread> clients;
        for (size_t peer=1;peer<=4;++peer) clients.emplace_back([&,peer]{
            try {runClient(port,peer);} catch (const std::exception& error) {++failures;std::cerr<<"peer failure "<<error.what()<<std::endl;}
        });
        for (auto& client:clients) client.join();
        dbms::requestServerShutdown();server.join();require(serverResult,"server normal shutdown");
        std::cout<<"STATEMENT_NOTICE_ORDER_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
        require(controls==cases.size()*2*5*2,"all 240 original controls reached");
        return failures?1:0;
    } catch (const std::exception& error) {std::cerr<<"fixture failure: "<<error.what()<<std::endl;return 2;}
}
