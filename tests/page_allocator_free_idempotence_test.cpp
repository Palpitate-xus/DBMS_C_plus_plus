// test_sources: src/storage/PageAllocator.cpp src/storage/BufferPool.cpp src/storage/PageCrypto.cpp src/storage/PgPage.cpp src/storage/PageWrapper.cpp
#include "storage/PageAllocator.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace dbms;
using Image = std::vector<std::array<char, PgPage::PAGE_SIZE>>;
static size_t controls = 0, failures = 0, setups = 0, cases = 0;

static void check(bool good, const char* label, size_t id) {
    ++controls;
    if (!good) {
        ++failures;
        std::cout << "FREE_PAGE_FAILURE case=" << id << " control=" << label << '\n';
    }
}
static void require(bool good, const char* label) {
    if (!good) {
        ++setups;
        throw std::runtime_error(std::string("setup: ") + label);
    }
}
static Image image(PageAllocator& allocator) {
    Image result(allocator.numPages());
    for (uint32_t id = 0; id < result.size(); ++id) {
        char* data = allocator.fetchPage(id);
        require(data != nullptr,"snapshot fetch");
        std::memcpy(result[id].data(),data,PgPage::PAGE_SIZE);
        allocator.unpinPage(id);
    }
    return result;
}
static bool writeValue(PageAllocator& allocator, uint32_t id, char value) {
    char* data = allocator.fetchPage(id);
    if (!data) return false;
    PgPage page(data);
    std::array<char,257> payload{};
    payload.fill(value);
    OffsetNumber slot = 0;
    const bool good = page.insert(payload.data(),payload.size(),slot) && slot==1;
    allocator.markDirty(id);
    allocator.unpinPage(id);
    return good;
}
static bool valueIs(PageAllocator& allocator, uint32_t id, char expected) {
    char* data = allocator.fetchPage(id);
    if (!data) return false;
    PgPage page(data);
    const char* tuple = nullptr;
    size_t length = 0;
    bool good = page.isValid(id) && page.get(1,tuple,length) && length==257 && page.liveCount()==1;
    if (good) good = std::all_of(tuple,tuple+length,[&](char byte){return byte==expected;});
    allocator.unpinPage(id);
    return good;
}

static void exercise(unsigned count,unsigned order,int duplicate,bool reopen,unsigned threads) {
    const size_t caseId = cases++;
    const std::string path = "free_case_"+std::to_string(caseId)+".dt";
    PageAllocator allocator(path,32);
    require(allocator.open(),"open");
    for (unsigned i=1;i<=count+1;++i) require(allocator.allocPage()==i,"initial allocation");
    const uint32_t anchor = count+1;
    require(writeValue(allocator,anchor,'A'),"anchor");
    std::vector<uint32_t> freed;
    for (unsigned i=1;i<=count;++i) freed.push_back(i);
    if (order==1) std::reverse(freed.begin(),freed.end());
    if (order==2) {
        std::vector<uint32_t> rotated;
        for (uint32_t i:freed) if(i%2) rotated.push_back(i);
        for (uint32_t i:freed) if(i%2==0) rotated.push_back(i);
        freed=std::move(rotated);
    }
    for (uint32_t id:freed) allocator.freePage(id);
    if (reopen) {
        require(allocator.flush(),"initial flush");
        allocator.close();
        require(allocator.open(),"valid free-list reopen");
    }
    const Image before=image(allocator);
    if (threads) {
        std::atomic<bool> go{false};
        std::vector<std::thread> workers;
        for(unsigned t=0;t<threads;++t) workers.emplace_back([&] {
            while(!go.load(std::memory_order_acquire)) std::this_thread::yield();
            for(unsigned repeat=0;repeat<3;++repeat)
                for(uint32_t id:freed) allocator.freePage(id);
        });
        go.store(true,std::memory_order_release);
        for(auto& worker:workers) worker.join();
    } else if(duplicate>=0) {
        for(unsigned repeat=0;repeat<3;++repeat) allocator.freePage(freed[static_cast<size_t>(duplicate)]);
    }
    check(image(allocator)==before,"repeated-free-byte-preservation",caseId);
    std::vector<uint32_t> allocated;
    bool expectedOrder=true, written=true;
    for(unsigned i=0;i<count;++i) {
        const uint32_t id=allocator.allocPage();
        allocated.push_back(id);
        expectedOrder = expectedOrder && id==freed[count-1-i];
        if(id) written = writeValue(allocator,id,static_cast<char>('a'+i)) && written;
        else written=false;
    }
    const uint32_t extension=allocator.allocPage();
    expectedOrder = expectedOrder && extension==count+2 &&
        std::set<uint32_t>(allocated.begin(),allocated.end()).size()==count;
    check(expectedOrder && written,"unique-lifo-reuse-then-extend",caseId);
    bool intact=valueIs(allocator,anchor,'A');
    for(unsigned i=0;i<count;++i)
        intact=valueIs(allocator,allocated[i],static_cast<char>('a'+i)) && intact;
    check(intact,"no-returned-page-alias-or-anchor-loss",caseId);
    bool durable=allocator.flush();
    allocator.close();
    const bool opened=allocator.open();
    durable=durable && opened;
    if(opened) {
        durable=valueIs(allocator,anchor,'A') && durable;
        for(unsigned i=0;i<count;++i)
            durable=valueIs(allocator,allocated[i],static_cast<char>('a'+i)) && durable;
        const uint32_t next=allocator.allocPage();
        durable=durable && next==count+3;
    }
    check(durable,"flush-reopen-values-and-next-allocation",caseId);
}

static void invalidCalls() {
    const size_t caseId=cases++;
    PageAllocator allocator("invalid_free.dt",32);
    allocator.freePage(1);
    check(!allocator.isOpen() && !std::filesystem::exists("invalid_free.dt"),"closed-free-no-create",caseId);
    require(allocator.open(),"invalid open");
    require(allocator.allocPage()==1,"invalid initial");
    require(writeValue(allocator,1,'A'),"invalid anchor");
    for(uint32_t id:{0U,2U,17U,std::numeric_limits<uint32_t>::max()}) {
        const Image before=image(allocator);
        allocator.freePage(id);
        check(image(allocator)==before,"invalid-free-byte-preservation",caseId);
    }
    check(allocator.allocPage()==2 && valueIs(allocator,1,'A'),"invalid-free-followed-by-allocation",caseId);
}

int main() {
    try {
        for(unsigned count:{1U,2U,7U,17U})
            for(unsigned order=0;order<3;++order)
                for(bool reopen:{false,true})
                    for(int duplicate=-1;duplicate<static_cast<int>(count);++duplicate)
                        exercise(count,order,duplicate,reopen,0);
        for(unsigned count:{1U,7U,17U})
            for(unsigned threads:{2U,8U})
                for(bool reopen:{false,true}) exercise(count,0,0,reopen,threads);
        invalidCalls();
    } catch(const std::exception& error) {
        std::cout << "FREE_PAGE_CONTROL_ERROR " << error.what() << '\n';
    }
    const bool complete=cases==199 && controls==798 && setups==0;
    std::cout << "PAGE_ALLOCATOR_FREE_IDEMPOTENCE_COMPLETE " << controls << " FAILURES " << failures
              << " CASES " << cases << " SETUP_ERRORS " << setups << std::endl;
    return complete && failures==0 ? 0 : 1;
}
