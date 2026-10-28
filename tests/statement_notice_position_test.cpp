#include "process/OutputCapture.h"
#include "process/StatementNotice.h"
#include <array>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>

int main() {
    using namespace dbms;
    int controls=0,failures=0;
    auto check=[&](bool good,const char* label){++controls;if(!good)++failures;std::cout<<"NOTICE_POSITION "<<label<<" pass="<<good<<'\n';};
    std::vector<StatementNotice> outer,inner;std::ostringstream anchor,decoy;bool caught=false;
    {
        ScopedOutputCapture stdoutCapture(decoy);
        ScopedStatementNoticeCapture bound(outer,anchor);
        emitStatementNotice({"NOTICE","42P07","first"});
        anchor<<"\xE4\xB8\xAD\n";
        emitStatementNotice({"NOTICE","42P07","UTF8 offset"});
        {ScopedStatementNoticeCapture independent(inner);anchor<<'x';emitStatementNotice({"NOTICE","00000","independent"});}
        emitStatementNotice({"NOTICE","42P07","outer restored"});
        {std::ostringstream nested;nested<<"ab";ScopedStatementNoticeCapture rebound(inner,nested);emitStatementNotice({"WARNING","22023","nested bound"});}
        emitStatementNotice({"NOTICE","42P07","outer bound restored"});
        std::cout<<"NOTICE: ordinary captured row\n";
        emitStatementNotice({"NOTICE","42P07","not a stdout offset"});
        try {ScopedStatementNoticeCapture independent(inner);emitStatementNotice({"NOTICE","00000","before throw"});throw std::runtime_error("unwind");}
        catch(const std::runtime_error&){caught=true;}
        emitStatementNotice({"NOTICE","42P07","after throw"});
    }
    check(outer.size()==6,"outer-count");
    check(inner.size()==3,"inner-count");
    check(outer.size()==6 && outer[0].outputOffset==0,"initial-byte-position");
    check(outer.size()==6 && outer[1].outputOffset==4,"UTF8-uses-bytes-not-characters");
    check(anchor.str()=="\xE4\xB8\xAD\nx","anchor-bytes-unchanged");
    check(outer.size()==6 && outer[2].outputOffset==5,"restore-after-independent-receiver");
    check(inner.size()==3 && inner[0].outputOffset==-1,"independent-receiver-has-no-outer-anchor");
    check(inner.size()==3 && inner[1].outputOffset==2,"nested-bound-receiver-position");
    check(outer.size()==6 && outer[3].outputOffset==5,"restore-after-bound-receiver");
    check(decoy.str()=="NOTICE: ordinary captured row\n","stdout-bytes-unchanged");
    check(outer.size()==6 && outer[4].outputOffset==5,"anchor-not-incidental-stdout-capture");
    check(caught && outer.size()==6 && outer[5].outputOffset==5 && inner.size()==3 && inner[2].outputOffset==-1,"exception-restores-anchor");
    std::vector<StatementNotice> independent;
    {ScopedStatementNoticeCapture capture(independent);StatementNotice value{"NOTICE","42P07","caller"};value.outputOffset=999;emitStatementNotice(std::move(value));}
    check(independent.size()==1 && independent[0].outputOffset==-1 && independent[0].message=="caller","caller-cannot-supply-capture-position");
    struct Nonseekable:std::streambuf{} buffer;std::ostream unsupported(&buffer);std::vector<StatementNotice> unknown;
    {ScopedStatementNoticeCapture capture(unknown,unsupported);emitStatementNotice({"WARNING","22023","nonseekable"});}
    check(unknown.size()==1 && unknown[0].outputOffset==-1,"nonseekable-position-remains-unknown");
    check(unknown.size()==1 && unknown[0].severity=="WARNING" && unknown[0].sqlState=="22023" && unknown[0].message=="nonseekable","unknown-position-keeps-diagnostic");
    std::ostringstream fallback;
    {ScopedOutputCapture capture(fallback);emitStatementNotice({"NOTICE","42P07","fallback"});}
    check(fallback.str()=="NOTICE: fallback\n","all-receivers-restored");
    std::array<std::vector<StatementNotice>,8> results;std::array<std::thread,8> workers;
    for(size_t id=0;id<workers.size();++id) workers[id]=std::thread([&,id]{
        std::ostringstream bytes;ScopedStatementNoticeCapture capture(results[id],bytes);
        for(size_t n=0;n<64;++n){bytes<<std::string(n%3+1,'x');emitStatementNotice({"NOTICE","42P07",std::to_string(id)+":"+std::to_string(n)});}
    });
    for(auto& worker:workers)worker.join();
    for(size_t id=0;id<workers.size();++id){
        bool good=results[id].size()==64;std::streamoff offset=0;
        for(size_t n=0;n<results[id].size();++n){offset+=static_cast<std::streamoff>(n%3+1);good=good && results[id][n].outputOffset==offset && results[id][n].message==std::to_string(id)+":"+std::to_string(n);}
        check(good,"thread-local-anchor-and-message-isolation");
    }
    std::cout<<"NOTICE_POSITION_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    if(controls!=24)return 2;
    return failures?1:0;
}
