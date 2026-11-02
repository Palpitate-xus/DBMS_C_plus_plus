// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite,--wrap=ftruncate
#include "executor/ExecutionPlan.h"
#include "Config.h"
#include "Session.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::Config g_config;

namespace {
int mode = 0;
size_t reads = 0,writes = 0,creates = 0,truncates = 0,maxDescriptors = 0;
bool triggered = false,outputPhase = false;
SessionInterruptState* interrupt = nullptr;
int crashPipe = -1;
bool sortFile(int fd) {
    char link[64],name[4096]; std::snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
    const auto count = ::readlink(link,name,sizeof(name)-1);
    if (count < 0) return false;
    name[count] = '\0'; return std::strstr(name,"/.dbms-sort-") != nullptr;
}
size_t descriptors() {
    size_t count=0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd"))
        count += sortFile(std::stoi(entry.path().filename().string()));
    return count;
}
bool noPaths() {
    for (const auto& entry : std::filesystem::directory_iterator("."))
        if (entry.path().filename().string().rfind(".dbms-sort-",0)==0) return false;
    return true;
}
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __real_ftruncate(int,off_t);

extern "C" int __wrap_mkstemp(char* name) {
    if (std::strncmp(name,".dbms-sort-",11)!=0) return __real_mkstemp(name);
    ++creates;
    if (mode==5 && !triggered) { triggered=true; errno=EMFILE; return -1; }
    const int fd=__real_mkstemp(name);
    maxDescriptors=std::max(maxDescriptors,descriptors()); return fd;
}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset) {
    if (!sortFile(fd)) return __real_pwrite(fd,data,size,offset);
    ++writes;
    if (mode==1 && writes==1) return __real_pwrite(fd,data,std::min<size_t>(7,size),offset);
    if (mode==1 && writes==2) { errno=EINTR; return -1; }
    if (mode==2 && !triggered) { triggered=true; errno=ENOSPC; return -1; }
    if (mode==7 && !triggered) { triggered=true; interrupt->cancelRequested.store(true); }
    if (mode==15 && !triggered) {
        triggered=true; const char ready='R';
        if (::write(crashPipe,&ready,1)!=1) ::_exit(3);
        for (;;) ::pause();
    }
    return __real_pwrite(fd,data,size,offset);
}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset) {
    if (!sortFile(fd)) return __real_pread(fd,data,size,offset);
    ++reads;
    if (mode==1 && reads==1) return __real_pread(fd,data,std::min<size_t>(3,size),offset);
    if (mode==1 && reads==2) { errno=EINTR; return -1; }
    if ((mode==3 || (mode==8 && outputPhase)) && !triggered) { triggered=true; errno=EIO; return -1; }
    if (mode==4 && !triggered) { triggered=true; return 0; }
    return __real_pread(fd,data,size,offset);
}
extern "C" int __wrap_ftruncate(int fd,off_t length) {
    if (!sortFile(fd)) return __real_ftruncate(fd,length);
    ++truncates;
    if (mode==6 && !triggered) { triggered=true; errno=EIO; return -1; }
    return __real_ftruncate(fd,length);
}

class FaultSource final : public dbms::Operator {
    size_t position_=0; size_t& closed_; std::exception_ptr primary_;
public:
    FaultSource(size_t& closed,std::exception_ptr primary) : closed_(closed),primary_(primary) {}
    bool open() override {
        position_=0; clearError();
        if (mode==13) throw dbms::DbError("22003","open failure");
        return true;
    }
    bool next(std::string& raw) override {
        if (position_==8 && (mode==9 || mode==12)) std::rethrow_exception(primary_);
        if (position_==8 && mode==10) { setError("source failure"); return false; }
        if (position_==32) return false;
        const int32_t value=31-position_++;
        raw.assign(4100,'x'); std::memcpy(raw.data(),&value,4); return true;
    }
    void close() override {
        ++closed_; if (mode==11 || mode==12) throw dbms::DbError("22023","close failure");
    }
};

int main() {
    using namespace dbms;
    const auto originalBudget=g_config.workMemKb; g_config.workMemKb=1;
    const auto previous=currentQueryInterruptState();
    TableSchema schema; schema.tablename="sort_fault"; schema.append(makeIntColumn("id",false,2));
    size_t checked=0,failures=0;
    for (mode=0;mode<=14;++mode) {
        auto state=std::make_shared<SessionInterruptState>(); state->queryActive.store(true);
        setCurrentQueryInterruptState(state); interrupt=state.get();
        reads=writes=creates=truncates=maxDescriptors=0; triggered=outputPhase=false;
        size_t closed=0,count=0; bool good=true,opened=false,identity=true;
        const auto primary=std::make_exception_ptr(DbError("22012","source division failure"));
        std::string sqlState,message,raw;
        SortOp sort(std::make_unique<FaultSource>(closed,primary),schema,"id",true);
        try {
            opened=sort.open();
            if (mode==8) outputPhase=true;
            if (mode==14 && opened) {
                good=sort.next(raw);sort.close();
                good=good && descriptors()==0 && noPaths(); opened=sort.open();
            }
            while (opened && sort.next(raw)) {
                int32_t value=0;std::memcpy(&value,raw.data(),4);
                good=good && raw.size()==4100 && value==int32_t(count++);
            }
            if (mode==10) good=good && !opened && sort.hasError() && sort.errorMessage()=="source failure";
        } catch (const DbError& error) {
            sqlState=error.sqlState();message=error.message();
            if (mode==9 || mode==12) identity=std::current_exception()==primary;
        } catch (const std::exception& error) { sqlState="UNEXPECTED";message=error.what(); }
        state->cancelRequested.store(false);
        try { sort.close();sort.close(); } catch (...) { good=false; }
        std::string expected;
        if (mode==2) expected="53100";
        if (mode==3 || mode==4 || mode==6 || mode==8) expected="58030";
        if (mode==5) expected="53000";
        if (mode==7) expected="57014";
        if (mode==9 || mode==12) expected="22012";
        if (mode==11) expected="22023";
        if (mode==13) expected="22003";
        const bool success=mode==0 || mode==1 || mode==14;
        good=good && sqlState==expected && identity && closed==(mode==14 ? 2U : 1U) &&
            descriptors()==0 && noPaths() && maxDescriptors<=2;
        if (success) good=good && opened && count==32 && creates>0 && reads>2 && writes>2;
        if (mode>=2 && mode<=8) good=good && triggered;
        if (mode==1) good=good && reads>2 && writes>2;
        // Final-run heads are preloaded during open. Returning the winning
        // row does no successor read; output EIO is raised on the next demand.
        if (!expected.empty()) good=good && count==(mode==8 ? 1U : 0U);
        ++checked;failures+=!good;
        std::cout << "SORT_SPILL_IO mode=" << mode << " opened=" << opened << " rows=" << count
                  << " state=" << sqlState << " message=" << message << " closed=" << closed
                  << " reads=" << reads << " writes=" << writes << " maxfds=" << maxDescriptors
                  << " triggered=" << triggered << " passed=" << good << '\n';
    }
    setCurrentQueryInterruptState(previous);
    int pipe[2];assert(::pipe(pipe)==0);const auto child=::fork();assert(child>=0);
    if (!child) {
        ::close(pipe[0]);crashPipe=pipe[1];mode=15;triggered=false;
        size_t closed=0;
        SortOp sort(std::make_unique<FaultSource>(closed,std::exception_ptr{}),schema,"id",true);
        try { sort.open(); } catch (...) {}
        const char failed='F';(void)::write(crashPipe,&failed,1);::_exit(2);
    }
    ::close(pipe[1]);pollfd ready{pipe[0],POLLIN,0};
    char status='F';const auto polled=::poll(&ready,1,5000);
    if (polled>0) (void)::read(pipe[0],&status,1);
    ::close(pipe[0]);size_t childDescriptors=0;bool deleted=true;
    std::error_code error;
    for (std::filesystem::directory_iterator entry("/proc/"+std::to_string(child)+"/fd",error),end;
         !error && entry!=end;entry.increment(error)) {
        const auto link=std::filesystem::read_symlink(entry->path(),error).string();
        if (!error && link.find("/.dbms-sort-")!=std::string::npos) {
            ++childDescriptors;deleted=deleted && link.find("(deleted)")!=std::string::npos;
        }
    }
    (void)::kill(child,SIGKILL);int childStatus=0;assert(::waitpid(child,&childStatus,0)==child);
    const bool crashed=status=='R' && childDescriptors==1 && deleted && WIFSIGNALED(childStatus) &&
        WTERMSIG(childStatus)==SIGKILL && noPaths() && descriptors()==0;
    ++checked;failures+=!crashed;
    std::cout<<"SORT_SPILL_IO process death ready="<<status<<" fds="<<childDescriptors<<" passed="<<crashed<<'\n';
    g_config.workMemKb=originalBudget;
    assert(checked==16);
    std::cout << "SORT_SPILL_IO_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
