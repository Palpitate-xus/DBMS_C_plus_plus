// test_sources: src/storage/WAL.cpp
#include "storage/WAL.h"
#include <array>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>

using namespace dbms;
namespace {
void checkpoint(const std::filesystem::path& root, Lsn lsn) {
    const std::array<uint64_t, 3> words{1, 10, lsn};
    std::ofstream file(root/"checkpoint",std::ios::binary|std::ios::trunc);
    file.write(reinterpret_cast<const char*>(words.data()),sizeof(words));
    assert(file.good());
}
void flip(const std::filesystem::path& file, size_t offset) {
    std::fstream stream(file,std::ios::binary|std::ios::in|std::ios::out);
    stream.seekg(offset);char byte=0;stream.read(&byte,1);assert(stream.good());
    stream.seekp(offset);byte^=1;stream.write(&byte,1);assert(stream.good());
}
}
int main() {
    char temporary[]="/tmp/dbms-wal-prefix-XXXXXX";
    assert(mkdtemp(temporary));const std::filesystem::path base=temporary;
    size_t controls=0;
    for (unsigned crossed:{1u,2u}) {
        const auto root=base/('r'+std::to_string(crossed));
        const auto dir=root/"pg_wal";
        Lsn first=0,cp=0,last=0;
        {
            WALManager wal(dir);assert(wal.ensureOpen());
            const size_t initial=WALManager::kSegmentSize-256;
            assert(wal.XLogInsert(RM_HEAP_ID,0,1,
                std::vector<char>(initial-sizeof(XLogRecHeader),'a'))==0);
            assert(wal.XLogInsert(RM_HEAP_ID,0,2,
                std::vector<char>((crossed-1)*WALManager::kSegmentSize+512,'b'))==initial);
            first=wal.XLogInsert(RM_HEAP_ID,0,3,{'c'});assert(first!=INVALID_LSN);
            cp=wal.XLogInsert(RM_CHECKPOINT_ID,XLOG_CHECKPOINT_ONLINE,0,{'k'});
            last=wal.XLogInsert(RM_HEAP_ID,0,4,{'d'});assert(last!=INVALID_LSN);
            assert(wal.XLogFlush(wal.currentWriteLsn()));
            checkpoint(root,cp);
            assert(wal.markSegmentsReadyBefore(cp));
            assert(wal.archivePendingSegments(root/"archive"));
            assert(wal.truncateBefore(cp));
            assert(!std::filesystem::exists(wal.segmentPath(0)));
        }
        {
            WALManager reopened(dir);
            assert(reopened.ensureOpen());++controls;
            assert(reopened.earliestAvailableLsn()==first);++controls;
            assert(reopened.ReadRecord(first)->header.xl_xid==3);
            assert(reopened.ReadRecord(cp)->rmid()==RM_CHECKPOINT_ID);
            assert(reopened.ReadRecord(last)->header.xl_xid==4);++controls;
            const auto appended=reopened.XLogInsert(RM_HEAP_ID,0,5,{'e'});
            assert(appended!=INVALID_LSN && reopened.ReadRecord(appended)->header.xl_prev==last);
            assert(reopened.XLogFlush(reopened.currentWriteLsn()));++controls;
        }
        // Corrupt prefix data, anchor metadata and post-checkpoint data must
        // still fail startup. The anchor is not permission to skip bad WAL.
        for (unsigned fault=0;fault<4;++fault) {
            const auto copy=base/("bad"+std::to_string(crossed)+std::to_string(fault));
            std::filesystem::copy(root,copy,std::filesystem::copy_options::recursive);
            WALManager bad(copy/"pg_wal");
            if(fault==0)flip(bad.segmentPath(first/WALManager::kSegmentSize),
                             first%WALManager::kSegmentSize+sizeof(XLogRecHeader));
            if(fault==1)checkpoint(copy,first); // a valid non-checkpoint record
            if(fault==2)checkpoint(copy,INVALID_LSN);
            if(fault==3)flip(bad.segmentPath(last/WALManager::kSegmentSize),
                             last%WALManager::kSegmentSize+sizeof(XLogRecHeader));
            assert(!bad.ensureOpen());++controls;
        }
    }
    {
        const auto root=base/"no_anchor";WALManager wal(root/"pg_wal");assert(wal.ensureOpen());
        assert(wal.XLogInsert(RM_HEAP_ID,0,1,
            std::vector<char>(WALManager::kSegmentSize-256-sizeof(XLogRecHeader),'a'))==0);
        assert(wal.XLogInsert(RM_HEAP_ID,0,2,std::vector<char>(512,'b'))!=INVALID_LSN);
        const auto tail=wal.XLogInsert(RM_HEAP_ID,0,3,{'c'});assert(tail!=INVALID_LSN);
        assert(wal.XLogFlush(wal.currentWriteLsn()));
        assert(wal.markSegmentsReadyBefore(tail));assert(wal.archivePendingSegments(root/"archive"));
        assert(!wal.truncateBefore(tail));assert(std::filesystem::exists(wal.segmentPath(0)));++controls;
    }
    std::filesystem::remove_all(base);
    std::cout<<"[WAL CHECKPOINT PREFIX] "<<controls<<" controls passed\n";
}
