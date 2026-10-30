// test_sources: src/storage/CommitLog.cpp
#include "storage/CommitLog.h"
#include <cassert>
#include <cerrno>
#include <cstdarg>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sys/stat.h>
#include <unistd.h>

namespace {
using Log = dbms::CommitLog;
using Status = Log::Status;
enum class Fault { None, Write, ZeroWrite, ShortWrite, WriteEintr,
                   Seek, SeekEintr, Size, SizeEintr, FileSync, DirectorySync, Rename };
Fault fault = Fault::None;
bool active = false, injected = false;
int imageFd = -1, directoryFd = -1;
size_t writtenBytes = 0;
std::string directory;

bool inject(Fault requested) {
    if (!active || injected || fault != requested) return false;
    injected = true;
    errno = requested == Fault::WriteEintr || requested == Fault::SeekEintr ||
        requested == Fault::SizeEintr ? EINTR : EIO;
    return true;
}
template<class Function> Function original(const char* name) {
    auto function = reinterpret_cast<Function>(::dlsym(RTLD_NEXT, name));
    if (!function) ::_exit(91);
    return function;
}
struct Temporary {
    std::string path;
    Temporary() {
        char buffer[] = "/tmp/dbms-clog-sparse-XXXXXX";
        assert(::mkdtemp(buffer)); path = buffer;
    }
    ~Temporary() { active = false; std::filesystem::remove_all(path); }
};
std::vector<uint8_t> bytes(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    assert(file.good());
    return {std::istreambuf_iterator<char>(file), {}};
}
void expect(const std::string& path, const std::vector<std::pair<dbms::TxnId, Status>>& statuses) {
    std::vector<uint8_t> wanted(Log::kSegmentFileSize, 0);
    for (const auto& [xid, value] : statuses) {
        const auto offset = static_cast<size_t>(xid / 4);
        const auto shift = static_cast<unsigned>((xid % 4) * 2);
        wanted[offset] = static_cast<uint8_t>((wanted[offset] & ~(3u << shift)) |
            (static_cast<unsigned>(value) << shift));
    }
    assert(bytes(path) == wanted);
}
void arm(Fault selected, const std::string& root) {
    fault = selected; injected = false; writtenBytes = 0;
    imageFd = directoryFd = -1; directory = root + "/pg_xact"; active = true;
}
}

extern "C" int open(const char* path, int flags, ...) {
    static const auto call = original<int(*)(const char*, int, ...)>("open");
    int descriptor;
    if ((flags & O_CREAT) || (flags & O_TMPFILE) == O_TMPFILE) {
        va_list arguments; va_start(arguments, flags);
        auto mode = va_arg(arguments, unsigned int); va_end(arguments);
        descriptor = call(path, flags, mode);
    } else descriptor = call(path, flags);
    if (active && descriptor >= 0 && std::strncmp(path, directory.c_str(), directory.size()) == 0) {
        if (std::strstr(path, ".tmp.")) imageFd = descriptor;
        else if ((flags & O_DIRECTORY) && std::strcmp(path, directory.c_str()) == 0) directoryFd = descriptor;
    }
    return descriptor;
}
extern "C" int close(int descriptor) {
    static const auto call = original<int(*)(int)>("close");
    if (descriptor == imageFd) imageFd = -1;
    if (descriptor == directoryFd) directoryFd = -1;
    return call(descriptor);
}
extern "C" ssize_t write(int descriptor, const void* data, size_t length) {
    static const auto call = original<ssize_t(*)(int,const void*,size_t)>("write");
    if (active && descriptor == imageFd) {
        if (inject(Fault::Write) || inject(Fault::WriteEintr)) return -1;
        if (inject(Fault::ZeroWrite)) return 0;
        if (inject(Fault::ShortWrite)) length = std::max<size_t>(1, length / 2);
        const auto count = call(descriptor, data, length);
        if (count > 0) writtenBytes += static_cast<size_t>(count);
        return count;
    }
    return call(descriptor, data, length);
}
extern "C" off_t lseek(int descriptor, off_t offset, int whence) {
    static const auto call = original<off_t(*)(int,off_t,int)>("lseek");
    if (active && descriptor == imageFd && (inject(Fault::Seek) || inject(Fault::SeekEintr))) return -1;
    return call(descriptor, offset, whence);
}
extern "C" int ftruncate(int descriptor, off_t length) {
    static const auto call = original<int(*)(int,off_t)>("ftruncate");
    if (active && descriptor == imageFd && (inject(Fault::Size) || inject(Fault::SizeEintr))) return -1;
    return call(descriptor, length);
}
extern "C" int fsync(int descriptor) {
    static const auto call = original<int(*)(int)>("fsync");
    if (active && ((descriptor == imageFd && inject(Fault::FileSync)) ||
                   (descriptor == directoryFd && inject(Fault::DirectorySync)))) return -1;
    return call(descriptor);
}
extern "C" int rename(const char* from, const char* to) {
    static const auto call = original<int(*)(const char*,const char*)>("rename");
    if (active && std::strncmp(from, directory.c_str(), directory.size()) == 0 &&
        std::strstr(from, ".tmp.") && inject(Fault::Rename)) return -1;
    return call(from, to);
}

int main() {
    constexpr dbms::TxnId first = 12, middle = 40000, far = Log::kXidsPerSegment - 1;
    const std::vector<std::pair<dbms::TxnId, Status>> seed{
        {first,Status::Committed},{far,Status::Aborted}};
    size_t controls = 0;
    for (const auto selected : {Fault::None, Fault::Write, Fault::ZeroWrite, Fault::ShortWrite,
             Fault::WriteEintr, Fault::Seek, Fault::SeekEintr, Fault::Size, Fault::SizeEintr,
             Fault::FileSync, Fault::DirectorySync, Fault::Rename}) {
        Temporary temporary;
        Log writer(temporary.path), observer(temporary.path);
        assert(writer.setStatuses(seed));
        assert(observer.getStatus(first) == Status::Committed);
        const auto path = temporary.path + "/pg_xact/0";
        const auto prior = bytes(path);
        writer.setStatus(middle, Status::Committed);
        arm(selected, temporary.path);
        const bool saved = writer.flush();
        active = false;
        const bool recoverable = selected == Fault::None || selected == Fault::ShortWrite ||
            selected == Fault::WriteEintr || selected == Fault::SeekEintr || selected == Fault::SizeEintr;
        assert(saved == recoverable);
        assert(selected == Fault::None || injected);
        if (saved) assert(writtenBytes < Log::kSegmentFileSize);
        else if (selected != Fault::DirectorySync) assert(bytes(path) == prior);
        assert(writer.getStatus(middle) == Status::Committed);
        {
            Log peer(temporary.path);
            peer.setStatus(middle + 1, Status::Aborted);
            assert(peer.flush());
        }
        assert(writer.flush());
        expect(path, {{first,Status::Committed},{far,Status::Aborted},
                      {middle,Status::Committed},{middle+1,Status::Aborted}});
        assert(observer.getStatus(middle) == Status::Committed);
        assert(observer.getStatus(middle+1) == Status::Aborted);
        assert(observer.getStatus(far) == Status::Aborted);
        for (const auto& file : std::filesystem::directory_iterator(temporary.path + "/pg_xact"))
            assert(file.path().filename().string().find(".tmp.") == std::string::npos);
        ++controls;
    }
    {
        Temporary temporary;
        Log writer(temporary.path), observer(temporary.path);
        assert(writer.setStatuses(seed));
        assert(observer.getStatus(first) == Status::Committed);
        assert(writer.setStatuses({{first,Status::InProgress},{far,Status::InProgress}}));
        expect(temporary.path + "/pg_xact/0", {});
        assert(observer.getStatus(first) == Status::InProgress);
        assert(observer.getStatus(far) == Status::InProgress);
        writer.setStatus(Log::kXidsPerSegment+first, Status::SubCommitted);
        assert(writer.flush());
        Log reopened(temporary.path);
        assert(reopened.getStatus(Log::kXidsPerSegment+first) == Status::SubCommitted);
        ++controls;
    }
    {
        Temporary temporary;
        Log writer(temporary.path), observer(temporary.path);
        assert(writer.setStatuses({{first,Status::Committed}}));
        assert(observer.getStatus(first) == Status::Committed);
        const auto path = temporary.path + "/pg_xact/0";
        struct stat before{}; assert(::stat(path.c_str(), &before) == 0);
        const int descriptor = ::open(path.c_str(), O_WRONLY);
        assert(descriptor >= 0);
        const uint8_t byte = 2; // xid 12 has shift zero: Aborted.
        assert(::pwrite(descriptor, &byte, 1, first/4) == 1);
        const timespec times[] = {before.st_atim, before.st_mtim};
        assert(::futimens(descriptor, times) == 0);
        assert(::close(descriptor) == 0);
        assert(observer.getStatus(first) == Status::Aborted);
        ++controls;
    }
    std::cout << "[CLOG SPARSE PUBLICATION] " << controls << " controls passed\n";
}
