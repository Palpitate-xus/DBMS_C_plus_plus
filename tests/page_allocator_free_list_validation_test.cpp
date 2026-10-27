// test_sources: src/storage/PageAllocator.cpp src/storage/BufferPool.cpp src/storage/PageCrypto.cpp src/storage/PgPage.cpp src/storage/PageWrapper.cpp
#include "storage/PageAllocator.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dbms;
static size_t cases=0,controls=0,failures=0,setups=0;
static void check(bool good,const char* label,size_t id) {
    ++controls;
    if(!good) {++failures;std::cout<<"FREE_LIST_FAILURE case="<<id<<" control="<<label<<'\n';}
}
static void require(bool good,const char* label) {
    if(!good) {++setups;throw std::runtime_error(label);}
}
static std::vector<char> readFile(const std::string& path) {
    std::ifstream input(path,std::ios::binary|std::ios::ate);
    require(static_cast<bool>(input),"read open");
    const auto size=input.tellg();require(size>=0,"read size");
    std::vector<char> result(static_cast<size_t>(size));
    input.seekg(0);input.read(result.data(),static_cast<std::streamsize>(result.size()));
    require(static_cast<bool>(input),"read bytes");return result;
}
static void exercise(unsigned n,unsigned layout,unsigned shape) {
    const size_t id=cases++;
    const std::string path="list_case_"+std::to_string(id)+".dt";
    {
        PageAllocator allocator(path,32);require(allocator.open(),"create");
        for(unsigned page=1;page<=n;++page)require(allocator.allocPage()==page,"allocate");
        require(allocator.flush(),"flush initial");
    }
    auto bytes=readFile(path);
    auto* header=reinterpret_cast<DataFileHeader*>(bytes.data());
    header->freeListHead=shape==0?0:1;
    std::vector<uint32_t> expected;
    for(unsigned page=1;page<=n;++page) {
        PgPage image(bytes.data()+page*PgPage::PAGE_SIZE);
        image.init(page);
        uint32_t next=page<n?page+1:0;
        if(shape==2) next=(page%2 && page+2<=n)?page+2:0;
        if(shape==3 && page==1) next=1;
        if(shape==4 && page==n) next=n;
        if(shape==5 && page==n) next=1;
        if(shape==6 && page==1) next=n+1;
        if(shape==7 && page==n) next=n+1;
        image.setNextPage(next);
        if((shape==8 && page==1)||(shape==9 && page==n)) {
            OffsetNumber slot=0;const char value='x';require(image.insert(&value,1,slot),"live free-page setup");
        }
        if(layout==PgPage::LEGACY_PAGE_LAYOUT_VERSION) {
            image.header()->pd_flags &= ~PgPage::PD_PAGE_ID_BOUND;
            image.header()->pd_pagesize_version=static_cast<uint16_t>((PgPage::PAGE_SIZE/512<<8)|layout);
            image.writeChecksum();
        }
        require(image.isValid(),"fixture page validity");
        if(shape==1 || (shape==2 && page%2)) expected.push_back(page);
    }
    header->headerChecksum=computeDataFileHeaderChecksum(*header);
    {
        std::ofstream output(path,std::ios::binary|std::ios::trunc);
        output.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
        output.close();require(static_cast<bool>(output),"write fixture");
    }
    const bool valid=shape<3;
    PageAllocator allocator(path,32);
    const bool opened=allocator.open();
    check(opened==valid,"open-admission",id);
    bool result=true;
    if(!valid) result=!allocator.isOpen() && allocator.numPages()==0 && allocator.fetchPage(1)==nullptr;
    allocator.close();
    check(readFile(path)==bytes,"read-only-open-close-bytes",id);
    if(valid) {
        result=allocator.open();
        std::set<uint32_t> allocated;
        for(uint32_t page:expected) {
            const uint32_t actual=allocator.allocPage();
            result=result && actual==page && allocated.insert(actual).second;
        }
        result=result && allocator.allocPage()==n+1;
    }
    check(result,valid?"valid-chain-reuse-then-extend":"failed-open-is-closed",id);
}
int main() {
    try {
        for(unsigned n:{1U,3U,17U})
            for(unsigned layout:{static_cast<unsigned>(PgPage::LEGACY_PAGE_LAYOUT_VERSION),static_cast<unsigned>(PgPage::PAGE_LAYOUT_VERSION)})
                for(unsigned shape=0;shape<10;++shape)exercise(n,layout,shape);
    } catch(const std::exception& error) {std::cout<<"FREE_LIST_CONTROL_ERROR "<<error.what()<<'\n';}
    std::cout<<"PAGE_ALLOCATOR_FREE_LIST_VALIDATION_COMPLETE "<<controls<<" FAILURES "<<failures
             <<" CASES "<<cases<<" SETUP_ERRORS "<<setups<<std::endl;
    return controls==180 && cases==60 && failures==0 && setups==0?0:1;
}
