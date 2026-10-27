#include "process/StatsPersistence.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
namespace fs = std::filesystem;
using namespace std::chrono_literals;
int checks=0, failures=0;
void check(bool value,const std::string& label) {
    ++checks;
    if(!value){++failures;std::cerr<<"FAIL "<<label<<'\n';}
}
int waitChild(pid_t pid) {
    int status=0;
    const auto deadline=std::chrono::steady_clock::now()+10s;
    for(;;){
        const auto result=::waitpid(pid,&status,WNOHANG);
        if(result==pid)return WIFEXITED(status)?WEXITSTATUS(status):128+WTERMSIG(status);
        if(result<0&&errno!=EINTR)return 253;
        if(std::chrono::steady_clock::now()>=deadline){
            ::kill(pid,SIGKILL);::waitpid(pid,&status,0);return 254;
        }
        std::this_thread::sleep_for(1ms);
    }
}
// Use the old POSIX protocol from another process, not an expected new
// implementation detail. Both old and new cooperating binaries must exclude it.
int legacyProbe(const fs::path& snapshot) {
    const std::string lockPath=snapshot.string()+".lock";
    const pid_t pid=::fork();
    if(pid<0)return 252;
    if(pid==0){
        const int fd=::open(lockPath.c_str(),O_CREAT|O_RDWR,0600);
        if(fd<0)::_exit(2);
        struct flock lock{};lock.l_type=F_WRLCK;lock.l_whence=SEEK_SET;
        const int result=::fcntl(fd,F_SETLK,&lock);const int error=errno;
        ::close(fd);
        ::_exit(result==0?0:((error==EAGAIN||error==EACCES)?1:2));
    }
    return waitChild(pid);
}
void closeIsolation(const fs::path& snapshot,const std::string& label) {
    {
        dbms::StatsFileLock holder(snapshot);check(holder.ok(),label+" holder");
        check(legacyProbe(snapshot)==1,label+" excludes legacy process");
        const int other=::open((snapshot.string()+".lock").c_str(),O_RDWR);
        check(other>=0,label+" unrelated open");
        if(other>=0)::close(other);
        check(legacyProbe(snapshot)==1,label+" unrelated close retains lock");
    }
    check(legacyProbe(snapshot)==0,label+" destructor releases lock");
}
void threadIsolation(const fs::path& first,const fs::path& alias,const std::string& label) {
    auto holder=std::make_unique<dbms::StatsFileLock>(first);
    check(holder->ok(),label+" first holder");
    std::promise<void> started,acquired;auto begin=started.get_future();auto done=acquired.get_future();
    std::atomic<bool> secondOk{false};
    std::promise<void> releaseSecond;auto release=releaseSecond.get_future();
    std::thread worker([&]{
        started.set_value();
        dbms::StatsFileLock lock(alias);secondOk=lock.ok();acquired.set_value();release.wait();
    });
    begin.wait();
    check(done.wait_for(120ms)==std::future_status::timeout,label+" second thread waits");
    holder.reset();
    check(done.wait_for(5s)==std::future_status::ready,label+" second thread wakes");
    check(secondOk.load(),label+" second holder succeeds");
    // Closing the first instance must not drop the second thread's live lock.
    check(legacyProbe(first)==1,label+" second holder excludes legacy process");
    releaseSecond.set_value();worker.join();
    check(legacyProbe(first)==0,label+" second destructor releases");
}
volatile std::sig_atomic_t interruptions=0;
void interrupted(int){++interruptions;}
void legacyHolder(const fs::path& snapshot) {
    const std::string path=snapshot.string()+".lock";
    int ready[2],release[2];
    if(::pipe(ready)!=0||::pipe(release)!=0){check(false,"legacy peer pipes");return;}
    const pid_t pid=::fork();
    if(pid<0){check(false,"legacy peer fork");return;}
    if(pid==0){
        ::close(ready[0]);::close(release[1]);
        const int fd=::open(path.c_str(),O_CREAT|O_RDWR,0600);
        struct flock lock{};lock.l_type=F_WRLCK;lock.l_whence=SEEK_SET;
        const char result=fd>=0&&::fcntl(fd,F_SETLK,&lock)==0?'1':'0';
        (void)::write(ready[1],&result,1);
        char go;while(::read(release[0],&go,1)<0&&errno==EINTR){}
        if(fd>=0)::close(fd);::_exit(result=='1'?0:1);
    }
    ::close(ready[1]);::close(release[0]);char result='0';
    const bool peerReady=::read(ready[0],&result,1)==1&&result=='1';
    ::close(ready[0]);check(peerReady,"legacy peer holds POSIX lock");
    struct sigaction handler{},previous{};handler.sa_handler=interrupted;::sigemptyset(&handler.sa_mask);
    const bool installed=::sigaction(SIGUSR1,&handler,&previous)==0;check(installed,"signal handler setup");
    std::promise<void> started,acquired,finish;auto begin=started.get_future();auto done=acquired.get_future();auto end=finish.get_future();
    std::atomic<bool> ok{false};
    std::thread worker([&]{started.set_value();dbms::StatsFileLock lock(snapshot);ok=lock.ok();acquired.set_value();end.wait();});
    begin.wait();check(done.wait_for(120ms)==std::future_status::timeout,"waits for legacy peer");
    if(installed){
        for(int i=0;i<3;++i){::pthread_kill(worker.native_handle(),SIGUSR1);std::this_thread::sleep_for(20ms);}
    }
    check(done.wait_for(120ms)==std::future_status::timeout,"signals do not abandon held-lock wait");
    const char go='1';(void)::write(release[1],&go,1);::close(release[1]);
    check(waitChild(pid)==0,"legacy peer exits");
    check(done.wait_for(5s)==std::future_status::ready,"wakes after legacy peer exits");
    check(ok.load(),"lock succeeds after interrupted wait");
    finish.set_value();worker.join();
    check(interruptions==3,"three real signals delivered to waiter");
    if(installed)::sigaction(SIGUSR1,&previous,nullptr);
    check(legacyProbe(snapshot)==0,"legacy interoperability cleanup");
}
int incrementSnapshot(const fs::path& snapshot,int threads,int repetitions) {
    std::atomic<int> errors{0};std::vector<std::thread> workers;
    for(int t=0;t<threads;++t)workers.emplace_back([&]{
        for(int i=0;i<repetitions;++i){
            dbms::StatsFileLock lock(snapshot);
            if(!lock.ok()){++errors;continue;}
            int value=-1;{std::ifstream in(snapshot);if(!(in>>value)){++errors;continue;}}
            if(!dbms::publishStatsSnapshot(snapshot,[&](const fs::path& temporary){
                std::ofstream out(temporary);out<<value+1<<'\n';out.flush();return out.good();
            }))++errors;
        }
    });
    for(auto& worker:workers)worker.join();return errors.load();
}
void snapshotConcurrency(const fs::path& snapshot,bool processes) {
    {std::ofstream out(snapshot);out<<0<<'\n';}
    const pid_t child=processes?::fork():-1;
    if(processes&&child==0)::_exit(incrementSnapshot(snapshot,2,20)==0?0:1);
    check(!processes||child>0,"snapshot child setup");
    check(incrementSnapshot(snapshot,3,20)==0,"all threaded snapshot publications succeed");
    if(processes&&child>0)check(waitChild(child)==0,"peer process threaded publications succeed");
    int value=-1;std::string trailing;std::ifstream in(snapshot);in>>value;
    check(value==(processes?100:60),"every locked read/modify/publish counted");
    check(!(in>>trailing),"published snapshot is complete");
    bool temporary=false;
    for(const auto& entry:fs::directory_iterator(snapshot.parent_path()))
        if(entry.path().filename().string().find(snapshot.filename().string()+".tmp.")==0)temporary=true;
    check(!temporary,"no publication temporary remains");
}
}
int main(){
    char pattern[]="/tmp/dbms-stats-lock-test.XXXXXX";const char* made=::mkdtemp(pattern);
    if(!made){std::cerr<<"SETUP_ERROR mkdtemp\n";return 2;}
    const fs::path root(made);
    try{
        closeIsolation(root/"sql.bin","SQL snapshot");
        closeIsolation(root/"runtime.bin","runtime snapshot");
        const auto first=root/"same.bin";
        {dbms::StatsFileLock setup(first);check(setup.ok(),"alias setup");}
        threadIsolation(first,first,"same path");
        fs::create_directory(root/"sub");
        threadIsolation(first,root/"sub"/".."/"same.bin","dot-dot alias");
        const auto hard=root/"hard.bin";fs::create_hard_link(first.string()+".lock",hard.string()+".lock");
        threadIsolation(first,hard,"hard-link alias");
        const auto symbolic=root/"symbolic.bin";fs::create_symlink(first.string()+".lock",symbolic.string()+".lock");
        threadIsolation(first,symbolic,"symbolic-link alias");
        {
            dbms::StatsFileLock holder(first);check(holder.ok(),"independent first holder");
            auto result=std::async(std::launch::async,[&]{dbms::StatsFileLock other(root/"other.bin");return other.ok();});
            check(result.wait_for(5s)==std::future_status::ready,"different snapshots do not block");
            check(result.get(),"independent snapshot succeeds");
        }
        {
            dbms::StatsFileLock missing(root/"missing"/"snapshot.bin");check(!missing.ok(),"missing parent fails");
            fs::create_directory(root/"directory.bin.lock");
            dbms::StatsFileLock directory(root/"directory.bin");check(!directory.ok(),"directory lock fails");
        }
        legacyHolder(root/"legacy.bin");
        snapshotConcurrency(root/"threaded.bin",false);
        snapshotConcurrency(root/"multiprocess.bin",true);
    }catch(const std::exception& e){++failures;std::cerr<<"SETUP_ERROR "<<e.what()<<'\n';}
    std::error_code cleanupError;fs::remove_all(root,cleanupError);
    check(!cleanupError,"private fixture cleanup");
    std::cout<<"STATS_FILE_LOCK_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
