#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <set>
#include <string>

extern dbms::StorageEngine g_engine;

static std::string readCounter(const std::filesystem::path& path, uint64_t& floor) {
    std::ifstream input(path, std::ios::binary);
    assert(input);
    const std::string bytes{std::istreambuf_iterator<char>(input), {}};
    assert(bytes.size()==24);
    uint32_t magic=0,version=0;
    uint64_t storedChecksum=0,checksum=1469598103934665603ULL;
    std::memcpy(&magic,bytes.data(),4);
    std::memcpy(&version,bytes.data()+4,4);
    std::memcpy(&floor,bytes.data()+8,8);
    std::memcpy(&storedChecksum,bytes.data()+16,8);
    for(size_t i=0;i<16;++i) {
        checksum^=static_cast<unsigned char>(bytes[i]);
        checksum*=1099511628211ULL;
    }
    assert(magic==0x31444954u && version==1 && checksum==storedChecksum);
    return bytes;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("toast_id_publication_cost");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    TableSchema table;table.tablename="values";
    table.append(makeTextColumn("payload",true));
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    const auto path=std::filesystem::path(database)/"values.toastmeta";
    uint64_t floor=0;
    auto previous=readCounter(path,floor);
    std::set<uint64_t> ids;
    size_t publications=0;
    // Count actual durable-counter changes, not elapsed time or a mock path.
    for(size_t i=0;i<64;++i) {
        const auto allocated=g_engine.allocToastId(database,table.tablename);
        assert(allocated && ids.insert(allocated).second);
        auto current=readCounter(path,floor);
        assert(floor>allocated); // Every returned ID is already covered on disk.
        publications+=current!=previous;
        previous=std::move(current);
    }
    std::cout<<"TOAST_ID_PUBLICATION_COST allocations="<<ids.size()
             <<" publications="<<publications<<" durable_floor="<<floor<<'\n';
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    return publications<=2?0:1;
}
