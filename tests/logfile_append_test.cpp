// Byte-exact legacy log append, rotation and finite writev progress regression.
// test_link_flags: -Wl,--wrap=writev
// Before: -DLOGFILE_HAS_WRITEV=0 with frozen before src include path.
// Candidate: -DLOGFILE_HAS_WRITEV=1 with candidate src include path.
// No engine, SQL, WAL, allocator or generic fault-injection dependency.
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#ifndef LOGFILE_HAS_WRITEV
#define LOGFILE_HAS_WRITEV 1
#endif

// Include the actual unmodified production definition, including when this
// source links the normal core. No differing inline definition or header macro.
#include "common/logs.h"
extern "C" ssize_t __real_writev(int,const struct iovec*,int);

namespace {
enum class Kind { Cap, Interrupted, Zero, Error };
struct Step {Kind kind;size_t bytes=0;};
struct Script {
 std::string expected;
 std::vector<Step> steps;
 std::vector<std::string> errors;
 size_t calls=0,consumed=0,interruptions=0,zeroReturns=0,errorReturns=0;
 int fd=-1;dev_t device=0;ino_t inode=0;
};
thread_local Script* activeScript=nullptr;
thread_local bool observePublicLog=false;
std::atomic<size_t> actualLogWritevCalls{0};
size_t checks=0,failures=0,groups=0,notrun=0;
void need(bool pass,const std::string& why) {
 ++checks;if(!pass){++failures;std::cerr<<"FAIL "<<why<<'\n';}
}
void group(const std::string& name,const std::function<void()>& body) {
 ++groups;std::cout<<"GROUP "<<name<<'\n';
 try {body();}catch(const std::exception& e){need(false,name+": "+e.what());}
}
std::string bytes(const std::filesystem::path& file) {
 std::ifstream in(file,std::ios::binary);if(!in)throw std::runtime_error("read "+file.string());
 std::string result{std::istreambuf_iterator<char>(in),{}};
 if(in.bad()) {
  throw std::runtime_error("short/error file read");
 }
 return result;
}
void put(const std::filesystem::path& file,const std::string& content) {
 std::ofstream out(file,std::ios::binary|std::ios::trunc);
 out.write(content.data(),std::streamsize(content.size()));out.close();
 if(!out)throw std::runtime_error("fixture write");
}
std::string oracle(const std::string& user,const std::string& operation,const std::string& time) {
 // The byte contract, not a call to the production formatter or stream.
 return time+std::string(4,' ')+user+std::string(4,' ')+operation+'\n';
}
struct CaptureStdout {
 std::ostringstream out;std::streambuf* previous=std::cout.rdbuf(out.rdbuf());
 ~CaptureStdout(){std::cout.rdbuf(previous);}
};
struct InDirectory {
 int previous;
 explicit InDirectory(const std::filesystem::path& dir):previous(::open(".",O_RDONLY|O_DIRECTORY|O_CLOEXEC)) {
  if(previous<0)throw std::runtime_error("open previous cwd");
  if(::chdir(dir.c_str())!=0){::close(previous);throw std::runtime_error("enter fixture cwd");}
 }
 ~InDirectory(){if(previous>=0){const int restored=::fchdir(previous);(void)restored;::close(previous);}}
};
struct Umask {mode_t previous;explicit Umask(mode_t mask):previous(::umask(mask)){}~Umask(){::umask(previous);}};
struct ObservePublicLog {
 bool previous=observePublicLog;
 ObservePublicLog(){observePublicLog=true;}
 ~ObservePublicLog(){observePublicLog=previous;}
};
struct Active {
 Script* previous;
 explicit Active(Script& s):previous(activeScript){activeScript=&s;}
 ~Active(){activeScript=previous;}
};
struct Fixture {
 std::filesystem::path root;size_t ordinal=0;
 Fixture(){char name[]="/tmp/dbms-logfile-append-XXXXXX";const auto p=::mkdtemp(name);if(!p)throw std::runtime_error("mkdtemp");root=p;}
 std::filesystem::path next(){const auto p=root/("case-"+std::to_string(++ordinal));std::filesystem::create_directory(p);return p;}
 // Deliberately retained for the owning harness to archive, not self-deleted.
};
struct stat status(const char* file) {
 struct stat out{};if(::stat(file,&out)!=0)throw std::runtime_error("stat fixture");return out;
}
void invoke(const std::string& user,const std::string& operation,const std::string& time,
            const std::string& stdoutExpected="") {
 CaptureStdout capture;ObservePublicLog observed;const int result=::log(user,operation,time);
 need(result==0,"public return remains0");need(capture.out.str()==stdoutExpected,"exact stdout bytes");
}
void scripted(Fixture& fixture,const std::string& role,const std::string& user,
              const std::string& operation,const std::string& time,
              const std::vector<Step>& steps,size_t expectedConsumed,
              size_t expectedCalls,size_t expectedInterrupts,size_t expectedZero,size_t expectedErrors) {
 group(role,[&] {
#if LOGFILE_HAS_WRITEV
  InDirectory cwd(fixture.next());const std::string prefix("existing\0prefix\n",16);put("dbms.log",prefix);
  Script script;script.expected=oracle(user,operation,time);script.steps=steps;
  {Active active(script);invoke(user,operation,time);}
  // Observe immediately, before any test read opens and reuses that descriptor.
  errno=0;const int fdResult=::fcntl(script.fd,F_GETFD);const int fdError=errno;
  need(script.fd>=0&&fdResult==-1&&fdError==EBADF,"log's actual descriptor closed on every exit role");
  need(script.calls>0,"candidate writev canary is actually intercepted");
  need(script.calls==expectedCalls,"finite expected writev calls, including stop on0/nonEINTR error");
  need(script.consumed==expectedConsumed,"actual successfully written byte count");
  need(script.interruptions==expectedInterrupts&&script.zeroReturns==expectedZero&&script.errorReturns==expectedErrors,"script roles really consumed");
  for(const auto& error:script.errors)need(false,"syscall boundary: "+error);
  need(bytes("dbms.log")==prefix+script.expected.substr(0,expectedConsumed),"exact real file after syscall role");
#else
  (void)fixture;(void)user;(void)operation;(void)time;(void)steps;(void)expectedConsumed;
  (void)expectedCalls;(void)expectedInterrupts;(void)expectedZero;(void)expectedErrors;
  ++notrun;std::cout<<"NOTRUN no candidate writev capability: "<<role<<'\n';
#endif
 });
}
}

extern "C" ssize_t __wrap_writev(int fd,const struct iovec* vectors,int count) {
 if(!observePublicLog)return __real_writev(fd,vectors,count);
 ++actualLogWritevCalls;
 Script* script=activeScript;
 if(!script)return __real_writev(fd,vectors,count);
 ++script->calls;
 if(script->calls>1024)throw std::runtime_error("test guard: writev loop did not terminate within finite script");
 script->fd=fd;
 const int flags=::fcntl(fd,F_GETFL);struct stat st{};
 if(flags<0||!(flags&O_APPEND))script->errors.push_back("real fd is not open O_APPEND");
 if(::fstat(fd,&st)!=0||!S_ISREG(st.st_mode))script->errors.push_back("real fd is not a regular file");
 else if(!script->inode){script->device=st.st_dev;script->inode=st.st_ino;}
 else if(script->device!=st.st_dev||script->inode!=st.st_ino)script->errors.push_back("retry changed file identity");
 if(count<0||count>1024){script->errors.push_back("invalid iovec count");errno=EINVAL;return -1;}
 std::string supplied;
 for(int i=0;i<count;++i)if(vectors[i].iov_len)
  supplied.append(static_cast<const char*>(vectors[i].iov_base),vectors[i].iov_len);
 if(supplied!=script->expected.substr(script->consumed))script->errors.push_back("retry bytes are not the exact unwritten suffix");
 Step step{Kind::Cap,supplied.size()};
 if(script->calls<=script->steps.size())step=script->steps[script->calls-1];
 if(step.kind==Kind::Interrupted){++script->interruptions;errno=EINTR;return -1;}
 if(step.kind==Kind::Zero){++script->zeroReturns;return 0;}
 if(step.kind==Kind::Error){++script->errorReturns;errno=EIO;return -1;}
 size_t remaining=std::min(step.bytes,supplied.size());std::vector<struct iovec> shortVectors;
 for(int i=0;i<count&&remaining;++i) {
  const auto size=std::min(remaining,vectors[i].iov_len);
  if(size) {
   shortVectors.push_back({vectors[i].iov_base,size});
  }
  remaining-=size;
 }
 const ssize_t n=__real_writev(fd,shortVectors.data(),int(shortVectors.size()));
 if(n>0)script->consumed+=size_t(n);
 return n;
}

int main() {
 try {
  Fixture fixture;
  std::cout<<"LOGFILE_FIXTURE "<<fixture.root<<" HAS_WRITEV="<<LOGFILE_HAS_WRITEV<<'\n';
  group("bytes/NULL/nonASCII/newline/empty/long/append",[&] {
   InDirectory cwd(fixture.next());std::string expected("OLD\0bytes\n",10);put("dbms.log",expected);
   const std::vector<std::tuple<std::string,std::string,std::string>> inputs{
    {"user","operation","time\n"},{"","",""},
    {std::string("u\0ser",5),std::string("op\0\xff\n",5),std::string("t\0ime",5)},
    {u8"使用者",u8"é / 漢字 / 🙂\n追加",u8"時刻\n"},
    {"a\nb","line1\nline2\r\n","\n"}};
   for(const auto& [u,o,t]:inputs){invoke(u,o,t);expected+=oracle(u,o,t);need(bytes("dbms.log")==expected,"all bytes present immediately after each return");}
   std::string large(1024*1024+17,'x');for(size_t i=0;i<large.size();++i)large[i]=char(i%256);
   invoke("long",large,"long-time");expected+=oracle("long",large,"long-time");
   need(bytes("dbms.log")==expected,"long field exact all256 byte values; no C-string truncation");
  });
  group("rename/unlink rotation and no cached target",[&] {
   InDirectory cwd(fixture.next());invoke("u","first","t");const auto first=oracle("u","first","t");
   const auto old=status("dbms.log");std::filesystem::rename("dbms.log","rotated.log");
   invoke("u","second","t");const auto second=oracle("u","second","t");
   const auto fresh=status("dbms.log");need(old.st_dev!=fresh.st_dev||old.st_ino!=fresh.st_ino,"new pathname inode differs from still-retained rotated file");
   need(bytes("rotated.log")==first&&bytes("dbms.log")==second,"rename rotation byte identity");
   std::filesystem::remove("dbms.log");invoke("u","third","t");
   need(bytes("rotated.log")==first&&bytes("dbms.log")==oracle("u","third","t"),"unlinked target re-created, not written through retired fd");
  });
  group("symlink follows the current append target",[&] {
   InDirectory cwd(fixture.next());put("target.log","target-prefix\n");
   std::filesystem::create_symlink("target.log","dbms.log");
   const auto old=status("target.log");invoke("linked","row","t");
   need(std::filesystem::is_symlink("dbms.log"),"append preserves the symlink itself");
   const auto resolved=status("dbms.log");
   need(old.st_dev==resolved.st_dev&&old.st_ino==resolved.st_ino,"append resolves the original target inode");
   need(bytes("target.log")==std::string("target-prefix\n")+oracle("linked","row","t"),"exact bytes appended through symlink");
  });
  group("real umask and existing mode",[&] {
   InDirectory cwd(fixture.next());
   {Umask mask(0027);invoke("u","mode027","t");need((status("dbms.log").st_mode&0777)==0640,"new file mode0666 masked by0027");}
   need(::chmod("dbms.log",0644)==0,"set existing fixture mode");const auto first=bytes("dbms.log");
   {Umask mask(0077);invoke("u","existing","t");need((status("dbms.log").st_mode&0777)==0644,"append preserves existing permission bits");}
   need(bytes("dbms.log")==first+oracle("u","existing","t"),"existing mode append bytes");
   std::filesystem::remove("dbms.log");{Umask mask(0077);invoke("u","mode077","t");need((status("dbms.log").st_mode&0777)==0600,"new file mode0666 masked by0077");}
  });
  group("actual open error stdout and return",[&] {
   InDirectory cwd(fixture.next());std::filesystem::create_directory("dbms.log");
   invoke("ignored","ignored","ignored","error 1\n");
   need(std::filesystem::is_directory("dbms.log")&&std::filesystem::is_empty("dbms.log"),"failed open has no fabricated file/payload");
  });
  group("bounded local short-record concurrency",[&] {
   InDirectory cwd(fixture.next());put("dbms.log","prefix\n");std::multiset<std::string> expected;
   for(int t=0;t<4;++t)for(int n=0;n<32;++n)expected.insert(oracle("writer"+std::to_string(t),"row"+std::to_string(n),"t"));
   std::vector<std::thread> writers;
   for(int t=0;t<4;++t)writers.emplace_back([t]{for(int n=0;n<32;++n)::log("writer"+std::to_string(t),"row"+std::to_string(n),"t");});
   for(auto& w:writers) {
    w.join();
   }
   const auto actual=bytes("dbms.log");need(actual.rfind("prefix\n",0)==0,"concurrent calls preserve prefix");
   std::multiset<std::string> lines;size_t at=7;
   while(at<actual.size()){const auto end=actual.find('\n',at);if(end==std::string::npos){need(false,"unterminated short record");break;}lines.insert(actual.substr(at,end-at+1));at=end+1;}
   need(lines==expected,"all128 whole short records exactly once, order intentionally unspecified");
   // This observed local regular-file role is not a large-record/short-write
   // transaction-atomicity promise or a cross-process rotation contract.
  });
  const std::string u("U\0u",3),o("op\0\xff-tail",9),t("T\n",2);const auto total=oracle(u,o,t).size();
  scripted(fixture,"writev full-success canary/close",u,o,t,{},total,1,0,0,0);
  scripted(fixture,"EINTR and partial crossing field boundaries",u,o,t,
   {{Kind::Interrupted},{Kind::Cap,2},{Kind::Cap,1},{Kind::Interrupted},{Kind::Cap,5},{Kind::Cap,4}},total,7,2,0,0);
  const auto emptyTotal=oracle("",std::string("\0x",2),"").size();
  std::vector<Step> ones(emptyTotal,Step{Kind::Cap,1});
  scripted(fixture,"zero-length fields and one-byte progress through all boundaries","",std::string("\0x",2),"",ones,emptyTotal,emptyTotal,0,0,0);
  scripted(fixture,"zero return stops without retry",u,o,t,{{Kind::Zero}},0,1,0,1,0);
  scripted(fixture,"nonEINTR error stops/close",u,o,t,{{Kind::Error}},0,1,0,0,1);
  scripted(fixture,"partial then EIO preserves only real prefix",u,o,t,{{Kind::Cap,3},{Kind::Error}},3,2,0,0,1);
  scripted(fixture,"partial then zero preserves only real prefix",u,o,t,{{Kind::Cap,5},{Kind::Zero}},5,2,0,1,0);
#if LOGFILE_HAS_WRITEV
  need(actualLogWritevCalls.load()>0,"new log has a genuinely observable writev path");
#endif
  std::cout<<"LOGFILE_APPEND_SUMMARY groups="<<groups<<" checks="<<checks<<" failures="<<failures<<" capability_NOTRUN="<<notrun<<" writev_calls="<<actualLogWritevCalls.load()<<'\n';
  return failures?1:0;
 }catch(const std::exception& e){std::cerr<<"LOGFILE_APPEND_FATAL "<<e.what()<<'\n';return 1;}
}
