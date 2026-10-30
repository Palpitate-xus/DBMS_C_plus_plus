#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/sha256.h"
#include "test_utils.h"

#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

namespace {
thread_local const char* failingDatabasePrefix=nullptr;
thread_local size_t injectedWrites=0;
}
extern "C" ssize_t write(int fd,const void* data,size_t size){
    if(failingDatabasePrefix && size){
        struct stat status{};
        if(::fstat(fd,&status)==0 && S_ISREG(status.st_mode)){
            char link[64],path[4096];
            const int length=std::snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
            const ssize_t got=length>0?::readlink(link,path,sizeof(path)-1):-1;
            if(got>=0){
                path[got]='\0';
                if(std::strncmp(path,failingDatabasePrefix,std::strlen(failingDatabasePrefix))==0){
                    ++injectedWrites;errno=EIO;return -1;
                }
            }
        }
    }
    return static_cast<ssize_t>(::syscall(SYS_write,fd,data,size));
}
extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace fs=std::filesystem;

struct Profile { std::string body,language,value; bool isNull=false; };
static const std::vector<Profile> profiles={
    {"SELECT 'plain'","sql","plain"},{"SELECT ''","sql",""},
    {"SELECT 'null'","sql","null"},{"SELECT NULL","sql","",true},
    {"SELECT 'first\nsecond'","sql","first\nsecond"},
    {"SELECT 'first\r\nsecond'","sql","first\r\nsecond"},
    {"SELECT 'first\rsecond'","sql","first\rsecond"},
    {"SELECT\n'界😀'","sql","界😀"},
    {"SELECT /* first\nsecond */ 'comment'","sql","comment"},
    {"SELECT -- first\n'line'","sql","line"},
    {"SELECT 'quote''s\\tail'\n","sql","quote's\\tail"},
    {"\nSELECT 'trailing'\n\n","sql","trailing"},
    {"BEGIN RETURN 'plain'; END;","plpgsql","plain"},
    {"BEGIN RETURN ''; END;","plpgsql",""},
    {"BEGIN RETURN 'null'; END;","plpgsql","null"},
    {"BEGIN RETURN NULL; END;","plpgsql","",true},
    {"BEGIN RETURN 'first\nsecond'; END;","plpgsql","first\nsecond"},
    {"BEGIN RETURN 'first\r\nsecond'; END;","plpgsql","first\r\nsecond"},
    {"BEGIN RETURN 'first\rsecond'; END;","plpgsql","first\rsecond"},
    {"BEGIN\nRETURN '界😀';\nEND;","plpgsql","界😀"},
    {"BEGIN /* first\nsecond */ RETURN 'comment'; END;","plpgsql","comment"},
    {"BEGIN -- first\nRETURN 'line'; END;","plpgsql","line"},
    {"BEGIN RETURN 'quote''s\\tail'; END;\n","plpgsql","quote's\\tail"},
    {"\nBEGIN RETURN 'trailing'; END;\n\n","plpgsql","trailing"},
};

static std::string field(const std::string& value){return std::to_string(value.size())+':'+value;}
static std::string seal(std::string bytes){
    const auto checksum=SHA256::digestBytes(bytes);
    bytes.append(reinterpret_cast<const char*>(checksum.data()),checksum.size());return bytes;
}
static std::string record(const std::vector<std::string>& names,const std::vector<std::string>& types,
                          const std::string& body,const std::string& language,char volatility,bool strict){
    std::string bytes=std::string("\x89" "DBMS_UDF3\r\n")+field(std::to_string(names.size()));
    for(size_t i=0;i<names.size();++i)bytes+=field(names[i])+field(types[i]);
    return seal(bytes+field(body)+field(std::string(1,volatility))+field(language)+field("text")+field(strict?"1":"0"));
}
static void writeFile(const fs::path& path,const std::string& bytes){
    std::ofstream out(path,std::ios::binary|std::ios::trunc);
    out.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));out.close();
    if(!out)throw std::runtime_error("fixture write failed");
}
static bool same(const StorageEngine::UDFInfo& info,const std::string& name,
                 const std::vector<std::string>& names,const std::vector<std::string>& types,
                 const Profile& profile,char vol,bool strict){
    return info.name==name && info.expression==profile.body && info.language==profile.language &&
           info.provolatile==vol && info.strict==strict && info.returnType=="text" &&
           info.paramNames==names && info.paramTypes==types &&
           info.paramName==(names.empty()?"":names.front());
}
struct Saved { std::string name; std::vector<std::string> names,types; Profile profile; char vol; bool strict; };

int main(){
    TypeRegistry::instance().bootstrap();
    const std::string test="routine_body_metadata",db=testDbPath(test);
    cleanupTestDb(test);
    if(g_engine.createDatabase(db,"utf8")!=DBStatus::OK)return 120;
    size_t controls=0,failures=0;
    const auto check=[&](bool valid,const std::string& label){
        ++controls;if(!valid){++failures;std::cerr<<"ROUTINE_METADATA_FAIL "<<label<<'\n';}
    };
    std::vector<Saved> saved;
    size_t ordinal=0;
    for(const auto& profile:profiles)for(int descriptor=0;descriptor<4;++descriptor)
    for(char volatility:{'i','s','v'})for(bool strict:{false,true}){
        std::vector<std::string> names,types;
        if(descriptor==0){names={"arg"};types={"text"};}
        if(descriptor==2){names={"a","line\narg","c"};types={"integer","text","numeric(10,2)"};}
        if(descriptor==3){names={"line\narg"};types={"text"};}
        const std::string name="body_"+std::to_string(ordinal++);
        const auto create=[&](const Profile& p,char vol,bool st,bool replace){
            if(descriptor==0 || descriptor==3)
                return g_engine.createUDF(db,name,names.front(),p.body,vol,p.language,"text",types.front(),st,replace);
            return g_engine.createUDF(db,name,names,types,p.body,vol,p.language,"text",st,replace);
        };
        try{
            const auto status=create(profile,volatility,strict,false);
            check(status==DBStatus::OK && same(g_engine.getUDF(db,name),name,names,types,profile,volatility,strict),name+" create exact bytes/attributes");
            check(create(Profile{"SELECT 'replacement'","sql","replacement"},volatility,strict,false)==DBStatus::TABLE_ALREADY_EXISTS &&
                  same(g_engine.getUDF(db,name),name,names,types,profile,volatility,strict),name+" duplicate preserves prior definition");
            if(volatility=='v' && !strict){
                std::string value;bool isNull=false;bool ok=false;
                const std::vector<std::string> values=descriptor==2?std::vector<std::string>{"7","value","1.25"}:std::vector<std::string>(names.size(),"value");
                try {ok=g_engine.callUDF(db,name,values,value,&isNull);}catch(const std::exception& error){std::cerr<<"CALL_OBSERVATION "<<name<<' '<<error.what()<<'\n';}
                check(ok && isNull==profile.isNull && (isNull || value==profile.value),name+" actual complete value");
            }
            Profile replacement=profile;replacement.body+=" \n";
            const char nextVol=volatility=='i'?'v':'i';
            check(create(replacement,nextVol,!strict,true)==DBStatus::OK &&
                  same(g_engine.getUDF(db,name),name,names,types,replacement,nextVol,!strict),name+" replace complete body and flags");
            saved.push_back({name,names,types,replacement,nextVol,!strict});
        }catch(const std::exception& error){
            std::cerr<<"ROUTINE_METADATA_SETUP_EXCEPTION "<<name<<' '<<error.what()<<'\n';return 120;
        }
    }
    {
        StorageEngine fresh;
        for(const auto& row:saved)
            check(same(fresh.getUDF(db,row.name),row.name,row.names,row.types,row.profile,row.vol,row.strict),row.name+" independent engine reread");
    }
    fs::create_directories(fs::path(db)/".funcs");
    size_t legacyCases=0;
    for(const auto& profile:profiles)for(int version=0;version<4;++version){
        const std::string name="legacy_"+std::to_string(legacyCases++);
        std::vector<std::string> names={"arg"},types;
        const std::string header=version==0?"arg":version==1?"PARAMS:arg:text":"PARAMS2:"+field(version==3?"line\narg":"arg")+field("text");
        if(version)types={"text"};
        if(version==3)names={"line\narg"};
        const auto path=fs::path(db)/".funcs"/(name+".func");
        writeFile(path,header+'\n'+profile.body+"\ns\n"+profile.language+"\nRETURNS:text\nSTRICT:1\n");
        auto expected=types;if(expected.empty())expected.resize(names.size());
        bool valid=false;
        try {
            auto info=g_engine.getUDF(db,name);if(info.paramTypes.empty())info.paramTypes.resize(info.paramNames.size());
            valid=same(info,name,names,expected,profile,'s',true);
        } catch(const std::exception& error){std::cerr<<"LEGACY_OBSERVATION "<<name<<' '<<error.what()<<'\n';}
        check(valid,name+" valid historical format exact body");
    }
    // Original short records predate the complete RETURNS/STRICT footer.
    // Only genuine single-line bodies have an unambiguous short layout.
    for(size_t profileIndex:{0U,1U,2U,3U,12U,13U,14U,15U})
    for(int version=0;version<3;++version)for(int tail=1;tail<=4;++tail){
        auto profile=profiles[profileIndex];if(tail<3)profile.language="sql";
        const std::string name="legacy_"+std::to_string(legacyCases++);
        const std::string header=version==0?"arg":version==1?"PARAMS:arg:text":"PARAMS2:"+field("arg")+field("text");
        std::string bytes=header+'\n'+profile.body+'\n';
        if(tail>=2)bytes+="v\n";
        if(tail>=3)bytes+=profile.language+'\n';
        if(tail>=4)bytes+="RETURNS:text\n";
        writeFile(fs::path(db)/".funcs"/(name+".func"),bytes);
        auto info=g_engine.getUDF(db,name);if(info.paramTypes.empty())info.paramTypes.resize(info.paramNames.size());
        check(same(info,name,{"arg"},{version==0?"":"text"},profile,'v',false),name+" historical short-record defaults");
    }
    size_t validCodec=0;
    for(const auto& profile:profiles){
        const std::string name="framed_"+std::to_string(validCodec++);
        writeFile(fs::path(db)/".funcs"/(name+".func"),record({"line\narg"},{"text"},profile.body,profile.language,'v',false));
        bool valid=false;
        try {valid=same(g_engine.getUDF(db,name),name,{"line\narg"},{"text"},profile,'v',false);}
        catch(const std::exception& error){std::cerr<<"FRAMING_OBSERVATION "<<name<<' '<<error.what()<<'\n';}
        check(valid,name+" independently encoded new framing");
    }
    const std::string healthy=record({"arg"},{"text"},"BEGIN RETURN 'line\nvalue'; END;","plpgsql",'v',false);
    const auto damagedPath=fs::path(db)/".funcs"/"damaged.func";
    size_t damagedCases=0;
    const auto reject=[&](const std::string& bytes,const std::string& label){
        writeFile(damagedPath,bytes);bool rejected=false;
        try {const auto info=g_engine.getUDF(db,"damaged");rejected=info.name.empty() && info.expression.empty();}
        catch(const DbError& error){rejected=error.sqlState()=="58030";}
        ++damagedCases;check(rejected,"reject "+label);
    };
    for(size_t length=0;length<healthy.size();++length)reject(healthy.substr(0,length),"truncation "+std::to_string(length));
    for(size_t offset=0;offset<healthy.size();++offset){auto bytes=healthy;bytes[offset]^=1;reject(bytes,"one-byte mutation "+std::to_string(offset));}
    for(const auto& suffix:std::vector<std::string>{"x","\n",std::string(1,'\0'),"\nv\nsql\nRETURNS:text\nSTRICT:0\n"})reject(healthy+suffix,"extra tail");
    const std::string magic="\x89" "DBMS_UDF3\r\n";
    for(const auto& count:std::vector<std::string>{"","-1","01","+1","18446744073709551616000","999999999"})
        reject(seal(magic+field(count)+field("body")+field("v")+field("sql")+field("text")+field("0")),"bad count with valid checksum");
    for(const auto& bodyLength:std::vector<std::string>{"", "-1:", "999999999:", "3xx:abc"})
        reject(seal(magic+field("0")+bodyLength+field("v")+field("sql")+field("text")+field("0")),"bad body length with valid checksum");
    for(const auto& fields:std::vector<std::vector<std::string>>{
        {"body","vv","sql","text","0"},{"body","x","sql","text","0"},
        {"body","v","","text","0"},{"body","v","sql","","0"},
        {"body","v","sql","text","2"},{"body","v","sql","text",""}}){
        std::string payload=magic+field("0");for(const auto& part:fields)payload+=field(part);
        reject(seal(payload),"bad property with valid checksum");
    }
    // Only real writes to this private database fail, on this caller thread.
    // Standard output and other descriptors are not synthetic fault sites.
    const std::string prefix=fs::absolute(db).string()+"/";
    const Profile prior{"SELECT 'before\nvalue'","sql","before\nvalue"};
    const Profile next{"SELECT 'after\nvalue'","sql","after\nvalue"};
    const std::vector<std::string> noParameters;
    const std::string failureName="publish_failure";
    check(g_engine.createUDF(db,failureName,noParameters,noParameters,prior.body,'v',"sql","text")==DBStatus::OK &&
          same(g_engine.getUDF(db,failureName),failureName,{},{},prior,'v',false),"publication prior definition");
    struct WriteFault {
        explicit WriteFault(const char* prefix){injectedWrites=0;failingDatabasePrefix=prefix;}
        ~WriteFault(){failingDatabasePrefix=nullptr;}
    };
    DBStatus failedReplace=DBStatus::OK,failedCreate=DBStatus::OK;
    size_t replaceWrites=0,createWrites=0;
    {WriteFault fault(prefix.c_str());failedReplace=g_engine.createUDF(db,failureName,noParameters,noParameters,next.body,'i',"sql","text",true,true);replaceWrites=injectedWrites;}
    check(failedReplace==DBStatus::IO_ERROR && replaceWrites>0 &&
          same(g_engine.getUDF(db,failureName),failureName,{},{},prior,'v',false),"real prepublication write failure preserves old definition");
    {WriteFault fault(prefix.c_str());failedCreate=g_engine.createUDF(db,"failed_new",noParameters,noParameters,next.body,'v',"sql","text");createWrites=injectedWrites;}
    check(failedCreate==DBStatus::IO_ERROR && createWrites>0 && !g_engine.udfExists(db,"failed_new"),"real prepublication failure publishes no new definition");
    check(g_engine.createUDF(db,failureName,noParameters,noParameters,next.body,'i',"sql","text",true,true)==DBStatus::OK &&
          same(g_engine.getUDF(db,failureName),failureName,{},{},next,'i',true),"publication retry stores whole new definition");
    std::cout<<"ROUTINE_METADATA_REAL_FAILED_WRITES "<<replaceWrites<<' '<<createWrites<<'\n';
    const size_t expected=2400+192+24+2*healthy.size()+4+6+4+6+4;
    if(controls!=expected || saved.size()!=576 || legacyCases!=192 || validCodec!=24){++failures;std::cerr<<"ROUTINE_METADATA_BAD_INVENTORY "<<controls<<' '<<expected<<'\n';}
    // All files above belong to this private test database. No global cleanup.
    if(g_engine.dropDatabase(db)!=DBStatus::OK){++failures;std::cerr<<"ROUTINE_METADATA_CLEANUP_FAIL\n";}
    cleanupTestDb(test);
    std::cout<<"ROUTINE_BODY_METADATA_COMPLETE "<<controls<<" FAILURES "<<failures<<" ROUNDTRIPS "<<saved.size()<<" LEGACY "<<legacyCases<<" NEW_RECORDS "<<validCodec<<" DAMAGED "<<damagedCases<<'\n';
    return failures?1:0;
}
