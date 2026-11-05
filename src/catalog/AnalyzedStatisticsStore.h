#pragma once

#include "catalog/AnalyzedStatisticsCodec.h"
#include "process/StatsPersistence.h"
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <limits>

namespace dbms::analyzed_statistics {

inline std::optional<Snapshot> read(const std::filesystem::path& path) {
    std::ifstream input(path,std::ios::binary);
    if(!input) {
        const int error=errno;
        if(error==ENOENT)return std::nullopt;
        throw DbError("58030","could not open analyzed statistics snapshot");
    }
    input.seekg(0,std::ios::end);const auto size=input.tellg();
    if(size<0)throw DbError("58030","could not size analyzed statistics snapshot");
    if(static_cast<uint64_t>(size)>codec::maxBytes)codec::invalid("file size");
    input.seekg(0,std::ios::beg);std::string bytes(static_cast<size_t>(size),'\0');
    if(!bytes.empty())input.read(bytes.data(),static_cast<std::streamsize>(bytes.size()));
    if(!input)
        throw DbError("58030","could not read complete analyzed statistics snapshot");
    const auto end=input.peek();
    if(end!=std::char_traits<char>::eof() || input.bad())
        throw DbError("58030","could not read complete analyzed statistics snapshot");
    return decode(bytes);
}

namespace store_detail {
// The caller owns StatsFileLock and has finished all serialization/validation.
// Preserve PublishedNotDurable instead of pretending a false durability result
// means that the previous name/image is still installed.
inline StatsSnapshotPublication publish(const std::filesystem::path& path,
                                       const std::string& bytes) {
    return publishStatsSnapshotWithStatus(path,[&](const auto& temporary) {
        std::ofstream out(temporary,std::ios::binary|std::ios::trunc);
        if(!out)return false;
        out.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
        out.close();return !out.fail();
    });
}
} // namespace store_detail

inline StatsSnapshotPublication replace(const std::filesystem::path& path,
                                        const Snapshot& snapshot) {
    const auto bytes=encode(snapshot);
    StatsFileLock lock(path);
    if(!lock.ok())throw DbError("58030","could not lock analyzed statistics snapshot");
    return store_detail::publish(path,bytes);
}
struct UpdateResult {
    StatsSnapshotPublication publication = StatsSnapshotPublication::NotPublished;
    uint64_t generation = 0;
};
// One relation's real analysis is one replacement set: removed/dropped/base or
// inherited attributes do not linger. Other relations survive concurrent
// thread/process producers because reload and merge use the same OFD lock.
inline UpdateResult updateRelation(const std::filesystem::path& path,uint32_t relation,
                                   const std::map<Key,Column>& columns) {
    if(!relation)codec::invalid("relation identity");
    for(const auto& x:columns)if(x.first.relation!=relation)codec::invalid("replacement relation mismatch");
    StatsFileLock lock(path);
    if(!lock.ok())throw DbError("58030","could not lock analyzed statistics snapshot");
    auto previous=read(path);Snapshot next=previous?std::move(*previous):Snapshot{};
    if(next.generation==std::numeric_limits<uint64_t>::max())
        throw DbError("54000","analyzed statistics generation exhausted");
    for(auto it=next.columns.begin();it!=next.columns.end();) {
        if(it->first.relation==relation)it=next.columns.erase(it);else ++it;
    }
    next.columns.insert(columns.begin(),columns.end());++next.generation;
    const auto bytes=encode(next);
    return {store_detail::publish(path,bytes),next.generation};
}
} // namespace dbms::analyzed_statistics
