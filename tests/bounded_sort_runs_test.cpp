// test_link_flags: -Wl,--wrap=mkstemp,--wrap=pread,--wrap=pwrite,--wrap=ftruncate
#include "executor/BoundedSortRuns.h"

#include <cassert>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <iostream>
#include <malloc.h>
#include <unistd.h>

namespace {
enum class Fault { None, Read, Write, Create, Partial, Cancel, BadHeader };
size_t liveBytes() { const auto memory = mallinfo2(); return memory.uordblks + memory.hblkhd; }
struct Observation {
    Fault fault = Fault::None;
    size_t reads = 0, writes = 0, creates = 0, truncates = 0, maxFds = 0;
    uint64_t initialExtent = 0, initialWritten = 0, mergeWritten = 0, readBytes = 0;
    bool inputDone = false, triggered = false;
    size_t baseline = 0, peak = 0, bytesCalls = 0, decoded = 0;
    bool comparisonFailure = false, decodeFailure = false;
    std::exception_ptr primary;
    SessionInterruptState* interrupt = nullptr;
    void sample() {
        const auto current = liveBytes();
        peak = std::max(peak,current > baseline ? current - baseline : 0);
    }
} observed;
bool sortFile(int fd) {
    char path[64], name[4096]; std::snprintf(path,sizeof(path),"/proc/self/fd/%d",fd);
    const auto count = ::readlink(path,name,sizeof(name)-1);
    if (count < 0) return false;
    name[count] = '\0'; return std::strstr(name,"/.dbms-sort-") != nullptr;
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd"))
        count += sortFile(std::stoi(entry.path().filename().string()));
    return count;
}
bool cleaned() {
    if (descriptors()) return false;
    for (const auto& entry : std::filesystem::directory_iterator("."))
        if (entry.path().filename().string().rfind(".dbms-sort-",0) == 0) return false;
    return true;
}
void reset(Fault fault = Fault::None) {
    observed = {}; observed.fault = fault; observed.baseline = liveBytes();
}
// Opaque owning test rows have no heap engine/RID or claimed database origin.
// Their binary payload, independent NULL bit and ordinal must survive the codec.
struct Row {
    int64_t key = 0;
    uint64_t ordinal = 0;
    bool isNull = false;
    std::string payload;
};
Row row(int64_t key,uint64_t ordinal,size_t width=4096,bool isNull=false) {
    Row value{key,ordinal,isNull,std::string(width,'a'+ordinal%23)};
    if (width >= 8) value.payload.replace(0,8,std::string("\0\nNULL\xce\xb2",8));
    return value;
}
bool same(const Row& value,const Row& expected) {
    return value.key == expected.key && value.ordinal == expected.ordinal &&
        value.isNull == expected.isNull && value.payload == expected.payload;
}
struct Less {
    bool operator()(const Row& a,const Row& b) const {
        if (observed.comparisonFailure) std::rethrow_exception(observed.primary);
        if (a.isNull != b.isNull) return !a.isNull;
        if (!a.isNull && a.key != b.key) return a.key < b.key;
        return a.ordinal < b.ordinal;
    }
};
struct Codec {
    size_t bytes(const Row& value) const { ++observed.bytesCalls; return sizeof(Row)+value.payload.capacity(); }
    void write(dbms::sort_detail::Writer& writer,const Row& value) const {
        writer.scalar(value.key); writer.scalar(value.ordinal);
        writer.scalar<uint8_t>(value.isNull); writer.scalar<uint64_t>(value.payload.size());
        writer.append(value.payload.data(),value.payload.size()); observed.sample();
    }
    Row read(dbms::sort_detail::Reader& reader) const {
        Row value;
        value.key = reader.scalar<int64_t>(); value.ordinal = reader.scalar<uint64_t>();
        const auto null = reader.scalar<uint8_t>();
        const auto length = reader.scalar<uint64_t>();
        if (null > 1 || length > reader.remaining() || length > std::numeric_limits<size_t>::max())
            throw dbms::DbError("58030","invalid opaque sort record");
        value.isNull = null; value.payload = reader.string(static_cast<size_t>(length));
        ++observed.decoded; observed.sample();
        if (observed.decodeFailure) std::rethrow_exception(observed.primary);
        return value;
    }
};
using Runs = dbms::sort_detail::BoundedRuns<Row,Codec,Less>;
void batchSort(std::vector<Row>& rows,const Less& less) {
    dbms::checkForQueryInterrupt(); size_t comparisons = 0;
    std::sort(rows.begin(),rows.end(),[&](const Row& a,const Row& b) {
        if ((comparisons++ & 127U) == 0) dbms::checkForQueryInterrupt();
        return less(a,b);
    });
    observed.sample(); dbms::checkForQueryInterrupt();
}
void eight(Runs& runs) {
    for (size_t i=0;i<8;++i) runs.add(row(7-i,i));
    observed.inputDone = true;
}
bool terminal(Runs& runs) {
    bool good = cleaned();
    const auto reads = observed.reads, writes = observed.writes;
    Row value;
    try { good = !runs.next(value) && !runs.next(value) && good; }
    catch (...) { good = false; }
    runs.close(); runs.close();
    return good && cleaned() && observed.reads == reads && observed.writes == writes;
}
}

extern "C" int __real_mkstemp(char*);
extern "C" ssize_t __real_pread(int,void*,size_t,off_t);
extern "C" ssize_t __real_pwrite(int,const void*,size_t,off_t);
extern "C" int __real_ftruncate(int,off_t);
extern "C" int __wrap_mkstemp(char* name) {
    if (std::strncmp(name,".dbms-sort-",11)) return __real_mkstemp(name);
    ++observed.creates;
    if (observed.fault == Fault::Create) { observed.triggered = true; errno = EMFILE; return -1; }
    const auto fd = __real_mkstemp(name);
    observed.maxFds = std::max(observed.maxFds,descriptors()); return fd;
}
extern "C" ssize_t __wrap_pwrite(int fd,const void* data,size_t size,off_t offset) {
    if (!sortFile(fd)) return __real_pwrite(fd,data,size,offset);
    ++observed.writes; observed.sample();
    if (observed.fault == Fault::Write) { observed.triggered = true; errno = ENOSPC; return -1; }
    if (observed.fault == Fault::Partial && observed.writes == 2) { errno = EINTR; return -1; }
    const auto amount = observed.fault == Fault::Partial && observed.writes == 1 ? std::min<size_t>(7,size) : size;
    const auto result = __real_pwrite(fd,data,amount,offset);
    if (result > 0) {
        if (observed.inputDone) observed.mergeWritten += result;
        else {
            observed.initialWritten += result;
            observed.initialExtent = std::max(observed.initialExtent,uint64_t(offset)+uint64_t(result));
        }
    }
    if (observed.fault == Fault::Cancel && !observed.triggered) {
        observed.triggered = true; observed.interrupt->cancelRequested.store(true);
    }
    return result;
}
extern "C" ssize_t __wrap_pread(int fd,void* data,size_t size,off_t offset) {
    if (!sortFile(fd)) return __real_pread(fd,data,size,offset);
    ++observed.reads; observed.sample();
    if (observed.fault == Fault::Read && !observed.triggered) {
        observed.triggered = true; errno = EIO; return -1;
    }
    if (observed.fault == Fault::Partial && observed.reads == 2) { errno = EINTR; return -1; }
    const auto amount = observed.fault == Fault::Partial && observed.reads == 1 ? std::min<size_t>(3,size) : size;
    const auto result = __real_pread(fd,data,amount,offset);
    if (result > 0) observed.readBytes += result;
    if (observed.fault == Fault::BadHeader && !observed.triggered &&
        result == ssize_t(sizeof(dbms::sort_detail::RunHeader))) {
        auto header = dbms::sort_detail::RunHeader{};
        std::memcpy(&header,data,sizeof(header)); header.rows = std::numeric_limits<uint64_t>::max();
        std::memcpy(data,&header,sizeof(header)); observed.triggered = true;
    }
    return result;
}
extern "C" int __wrap_ftruncate(int fd,off_t size) {
    if (sortFile(fd)) ++observed.truncates;
    return __real_ftruncate(fd,size);
}

int main() {
    size_t checked = 0, failures = 0;
    const auto check = [&](const char* label,bool good) {
        ++checked; failures += !good;
        std::cout << "BOUNDED_SORT_RUNS " << label << " passed=" << good << " peak=" << observed.peak
                  << " creates=" << observed.creates << " maxfds=" << observed.maxFds
                  << " reads=" << observed.readBytes << " merge_written=" << observed.mergeWritten << '\n';
    };
    {
        reset(); Runs runs(65536,{},Less{},std::nullopt,batchSort);
        for (size_t i=0;i<12;++i) runs.add(row(i%3,i,64,i%5==0));
        runs.finish(); Row value; size_t count = 0; bool good = !runs.spilled();
        for (int64_t key=0;key<3;++key) for (size_t i=0;i<12;++i)
            if (i%5 && int64_t(i%3)==key) { good = runs.next(value) && same(value,row(key,i,64)) && good; ++count; }
        for (const auto i : {0,5,10}) { good = runs.next(value) && same(value,row(i%3,i,64,true)) && good; ++count; }
        good = good && count == 12 && !runs.next(value) && observed.creates == 0;
        runs.close(); check("in-memory exact NULLs/binary payload/stable peers",good && cleaned());
    }
    {
        reset(); Runs runs(1024,{},Less{},std::nullopt,batchSort); eight(runs); runs.finish();
        Row value; size_t count = 0; bool good = runs.spilled() && descriptors()==1;
        while (runs.next(value)) { good = count<8 && same(value,row(count,7-count)) && good; ++count; }
        const auto payload = observed.initialExtent - 8*sizeof(dbms::sort_detail::RunHeader);
        good = good && count==8 && observed.creates==2 && observed.truncates==1 && observed.maxFds<=2 &&
            observed.mergeWritten >= 2*payload && observed.mergeWritten < 2*payload+1024 &&
            observed.readBytes >= 3*payload && observed.readBytes < 3*payload+1024;
        runs.close(); check("multi-pass exact output without final write/read cycle",good && cleaned());
    }
    {
        reset(); Runs runs(65536,{},Less{},size_t(3),batchSort);
        for (size_t i=0;i<512;++i) runs.add(row(i,i,64));
        const auto counted = observed.bytesCalls; runs.finish(); Row value; bool good = !runs.spilled() && counted==3;
        for (size_t i=0;i<3;++i) good = runs.next(value) && same(value,row(i,i,64)) && good;
        good = good && !runs.next(value); runs.close();
        check("TopN rejects losers before deep byte traversal",good && cleaned());
    }
    {
        reset(); Runs runs(65536,{},Less{},size_t(20000),batchSort);
        for (size_t i=0;i<512;++i) { runs.add(row(511-i,i)); observed.sample(); }
        observed.inputDone=true; runs.finish(); Row value; size_t count=0; bool good=runs.spilled();
        while (runs.next(value)) { good = count<512 && same(value,row(count,511-count)) && good; ++count; }
        good = good && count==512 && observed.peak<512*1024 && observed.maxFds<=2;
        runs.close(); check("large LIMIT retains a byte budget instead of all rows",good && cleaned());
    }
    {
        reset(); Runs runs(2048,{},Less{},size_t(3),batchSort);
        for (size_t i=0;i<3;++i) runs.add(row(i,i,16));
        const auto before=observed.bytesCalls; runs.add(row(999,999,16384));
        bool good = observed.bytesCalls==before && !runs.spilled();
        for (size_t i=1;i<=3;++i) runs.add(row(-int64_t(i),i+2,8192));
        runs.finish(); Row value;
        for (size_t i=3;i>0;--i) good = runs.next(value) && same(value,row(-int64_t(i),i+2,8192)) && good;
        good = good && runs.spilled() && !runs.next(value); runs.close();
        check("TopN replacement spills witnesses with distinct spooled count",good && cleaned());
    }
    {
        reset(); Runs runs(2048,{},Less{},size_t(3),batchSort);
        for (size_t i=0;i<3;++i) runs.add(row(i,i,400));
        runs.add(row(-1,3,8));
        runs.add(row(-2,4,700));
        runs.finish(); Row value;
        bool good = runs.spilled() && runs.next(value) && same(value,row(-2,4,700));
        good = runs.next(value) && same(value,row(-1,3,8)) && good;
        good = runs.next(value) && same(value,row(0,0,400)) && !runs.next(value) && good;
        runs.close(); check("replacement counts capacity retained by short-string move",good && cleaned());
    }
    {
        reset(); Runs runs(1024,{},Less{},size_t(1),batchSort); eight(runs); runs.finish();
        const auto reads=observed.reads; observed.fault=Fault::Read; Row value;
        bool good = runs.next(value) && same(value,row(0,7)) && !runs.next(value) && !runs.next(value) &&
            observed.reads==reads && !observed.triggered;
        runs.close(); check("output bound stops before pending winner refill",good && cleaned());
    }
    {
        reset(); Runs runs(1024,{},Less{},std::nullopt,batchSort); eight(runs); runs.finish();
        const auto reads=observed.reads,writes=observed.writes; observed.fault=Fault::Read; Row value;
        bool good = runs.next(value) && same(value,row(0,7)); runs.close(); runs.close();
        check("early close reads no successor",good && observed.reads==reads && observed.writes==writes && !observed.triggered && cleaned());
    }
    {
        reset(); Runs runs(1024,{},Less{},std::nullopt,batchSort); eight(runs); runs.finish();
        const auto reads=observed.reads; observed.fault=Fault::Read; Row value; size_t count=0; bool exact=false,good=true;
        try {
            if (runs.next(value)) { ++count; good=same(value,row(0,7)); }
            if (runs.next(value)) ++count;
        } catch (const dbms::DbError& error) {
            exact = error.sqlState()=="58030" && error.message()==std::string("could not read sort temporary file: ")+std::strerror(EIO);
        }
        good = good && exact && count==1 && observed.reads==reads+1 && observed.triggered;
        check("next-refill EIO preserves exact one-row prefix and terminal cleanup",terminal(runs) && good);
    }
    {
        reset(Fault::Read); Runs runs(1024,{},Less{},std::nullopt,batchSort); bool exact=false;
        try { eight(runs); runs.finish(); }
        catch (const dbms::DbError& error) { exact = error.sqlState()=="58030"; }
        check("merge setup EIO closes all descriptors",terminal(runs) && exact && observed.triggered);
    }
    {
        reset(); Runs runs(1024,{},Less{},std::nullopt,batchSort); eight(runs);
        observed.primary=std::make_exception_ptr(dbms::DbError("22012","opaque comparison failure"));
        observed.comparisonFailure=true; bool identity=false;
        try { runs.finish(); } catch (...) { identity=std::current_exception()==observed.primary; }
        check("comparison exception identity survives descriptor cleanup",terminal(runs) && identity);
    }
    {
        reset(); Runs runs(1024,{},Less{},std::nullopt,batchSort); eight(runs); runs.finish();
        observed.primary=std::make_exception_ptr(dbms::DbError("22003","opaque decode failure"));
        observed.decodeFailure=true; Row value; size_t count=0; bool identity=false;
        try { while(runs.next(value)) ++count; } catch (...) { identity=std::current_exception()==observed.primary; }
        check("codec exception is terminal and retains original identity",terminal(runs) && identity && count==1);
    }
    {
        reset(Fault::Cancel); auto prior=dbms::currentQueryInterruptState();
        auto interrupt=std::make_shared<SessionInterruptState>(); interrupt->queryActive.store(true);
        dbms::setCurrentQueryInterruptState(interrupt); observed.interrupt=interrupt.get();
        Runs runs(1024,{},Less{},std::nullopt,batchSort); bool canceled=false;
        try { eight(runs); runs.finish(); } catch (const dbms::DbError& error) { canceled=error.sqlState()=="57014"; }
        interrupt->cancelRequested.store(false);
        const bool good=terminal(runs) && canceled && observed.triggered;
        dbms::setCurrentQueryInterruptState(prior); check("real interrupt during temporary I/O",good);
    }
    {
        reset(Fault::Partial); Runs runs(1024,{},Less{},std::nullopt,batchSort); eight(runs); runs.finish();
        Row value; size_t count=0; bool good=true;
        while(runs.next(value)) { good=count<8 && same(value,row(count,7-count)) && good; ++count; }
        runs.close(); check("actual partial reads/writes and EINTR retry",good && count==8 && observed.reads>2 && observed.writes>2 && cleaned());
    }
    for (const auto fault : {Fault::Write,Fault::Create}) {
        reset(fault); Runs runs(1024,{},Less{},std::nullopt,batchSort); bool exact=false;
        try { eight(runs); runs.finish(); }
        catch (const dbms::DbError& error) { exact=error.sqlState()==(fault==Fault::Write?"53100":"53000"); }
        check(fault==Fault::Write?"ENOSPC cleanup":"EMFILE cleanup",terminal(runs) && exact && observed.triggered);
    }
    {
        reset(Fault::BadHeader); Runs runs(1024,{},Less{},std::nullopt,batchSort); bool exact=false;
        try { eight(runs); runs.finish(); } catch (const dbms::DbError& error) { exact=error.sqlState()=="58030"; }
        check("impossible on-disk run count rejected before allocation",terminal(runs) && exact && observed.triggered);
    }
    assert(checked==17);
    std::cout << "BOUNDED_SORT_RUNS_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures?1:0;
}
