#include "catalog/AnalyzedStatisticsReadCache.h"
#include <iostream>
#include <stdexcept>

namespace {
using namespace dbms;
namespace stats = analyzed_statistics;
namespace fs = std::filesystem;
size_t checks = 0;
void need(bool pass, const char* label) { ++checks; if (!pass) throw std::runtime_error(label); }
void write(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path,std::ios::binary|std::ios::trunc);out.write(bytes.data(),bytes.size());
    out.close();need(!out.fail(),"write complete real image");
}
void publish(const fs::path& path, const std::string& bytes) {
    write(path.string()+".new",bytes);fs::rename(path.string()+".new",path);
}
stats::Snapshot fixture(uint64_t generation) {
    stats::Snapshot result;result.generation=generation;
    auto type=std::make_shared<DatumType>();type->actualOid=type->codecOid=25;
    stats::Column column;column.attributeType=type;column.physicalRelation=7;
    column.schemaGeneration=9;column.sampleRows=column.totalRows=10;column.distinctEstimate=2;
    result.columns.emplace(stats::Key{11,1,false},std::move(column));return result;
}
} // namespace

int main() try {
    const auto root=fs::path("owned_statistics_read_cache");fs::create_directory(root);
    struct Cleanup {fs::path path;~Cleanup(){std::error_code error;fs::remove_all(path,error);}} cleanup{root};
    const auto a=root/"a",b=root/"b",schema=root/"schema";
    stats::ReadCache cache;
    need(!cache.snapshot(a),"missing current name declines");
    publish(a,stats::encode(fixture(1)));auto first=cache.snapshot(a);
    need(first&&first->generation==1,"first complete decode");
    const auto lease=std::shared_ptr<const stats::Column>(first,&first->columns.begin()->second);
    for(size_t i=0;i<1024;++i)need(cache.snapshot(a).get()==first.get(),"unchanged image reuses immutable owned decode");
    const auto time=fs::last_write_time(a);
    publish(a,stats::encode(fixture(2)));fs::last_write_time(a,time);
    auto second=cache.snapshot(a);
    need(second&&second->generation==2&&second.get()!=first.get(),"rename with restored mtime still invalidates inode/ctime");
    need(first->generation==1&&lease->physicalRelation==7,"prior generation and alias leases remain owned");
    publish(b,stats::encode(fixture(3)));auto third=cache.snapshot(b);
    need(third&&third->generation==3,"different database path evicts one current image");
    auto again=cache.snapshot(a);need(again&&again->generation==2&&again.get()!=second.get(),"evicted database reloads without stale pointer cache");
    fs::remove(a);need(!cache.snapshot(a),"unlink invalidates current name");
    publish(a,stats::encode(fixture(4)));need(cache.snapshot(a)->generation==4,"same name recreated has new generation");
    publish(a,"corrupt snapshot");bool rejected=false;
    try{(void)cache.snapshot(a);}catch(const DbError& e){rejected=e.sqlState()=="XX001";}
    need(rejected,"corrupt current bytes never reuse prior decoded image");
    publish(a,stats::encode(fixture(5)));need(cache.snapshot(a)->generation==5,"valid replacement after corruption is decoded");
    publish(a,stats::encode(fixture(6)));need(cache.snapshot(a)->generation==6,"same-size changed complete frame invalidates");
    const auto bytes=stats::encode(fixture(7));write(a,bytes);
    need(cache.snapshot(a)->generation==7,"in-place same-size replacement invalidates ctime");
    auto sameGeneration=fixture(7);sameGeneration.columns.begin()->second.totalRows=20;
    write(a,stats::encode(sameGeneration));
    need(cache.snapshot(a)->columns.begin()->second.totalRows==20,"same generation with changed complete payload invalidates checksum stamp");
    need(!cache.schemaDigest(schema),"missing schema is unavailable");
    write(schema,"schema 1");const auto oldDigest=cache.schemaDigest(schema);
    need(oldDigest&&*oldDigest==stats::codec::checksum("schema 1"),"real schema bytes are hashed");
    for(size_t i=0;i<1024;++i)need(cache.schemaDigest(schema)==oldDigest,"unchanged schema digest remains exact");
    publish(schema,"schema 2");need(cache.schemaDigest(schema)==stats::codec::checksum("schema 2"),"renamed schema digest refreshed");
    write(schema,"schema 3");need(cache.schemaDigest(schema)==stats::codec::checksum("schema 3"),"same-size in-place schema bytes are always rehashed");
    fs::remove(schema);need(!cache.schemaDigest(schema),"deleted schema is unavailable");
    bool directoryRejected=false;
    try{(void)cache.snapshot(root);}catch(const DbError& e){directoryRejected=e.sqlState()=="58030";}
    need(directoryRejected,"directory cannot act as optional file image");
    need(first->generation==1&&second->generation==2&&third->generation==3&&lease->schemaGeneration==9,
        "all explicit leases survive every replacement and rejection");
    std::cout<<"ANALYZED_READ_CACHE checks="<<checks<<" failures=0\n";return 0;
} catch(const std::exception& e) {
    std::cerr<<"ANALYZED_READ_CACHE_FAIL checks="<<checks<<' '<<e.what()<<'\n';return 1;
}
