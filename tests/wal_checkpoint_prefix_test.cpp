// test_sources: src/storage/WAL.cpp
#include "storage/WAL.h"
#include <array>
#include <cassert>
#include <cstring>
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

void switchBoundaries(const std::filesystem::path& base, size_t& controls) {
    const auto root=base/"switch_boundaries";
    const auto dir=root/"pg_wal";
    Lsn first=INVALID_LSN, middle=INVALID_LSN, cp=INVALID_LSN, last=INVALID_LSN;
    const uint64_t nextXid=101;
    std::vector<char> checkpointData(sizeof(nextXid));
    std::memcpy(checkpointData.data(),&nextXid,sizeof(nextXid));
    {
        WALManager wal(dir);assert(wal.ensureOpen());
        assert(wal.switchWal()==0);++controls;
        first=wal.XLogInsert(RM_HEAP_ID,0,11,{'a'});assert(first==0);
        assert(wal.switchWal()==WALManager::kSegmentSize);
        assert(wal.switchWal()==WALManager::kSegmentSize);++controls;
        middle=wal.XLogInsert(RM_HEAP_ID,0,12,{'b'});
        assert(middle==WALManager::kSegmentSize);
        assert(wal.switchWal()==2*WALManager::kSegmentSize);
        assert(wal.switchWal()==2*WALManager::kSegmentSize);++controls;
        cp=wal.XLogInsert(RM_CHECKPOINT_ID,XLOG_CHECKPOINT_ONLINE,0,checkpointData);
        assert(cp==2*WALManager::kSegmentSize);
        last=wal.XLogInsert(RM_HEAP_ID,0,13,{'c'});assert(last!=INVALID_LSN);
        assert(wal.XLogFlush(wal.currentWriteLsn()));
    }
    {
        WALManager wal(dir);assert(wal.ensureOpen());
        const auto firstRec=wal.ReadRecord(first), middleRec=wal.ReadRecord(middle);
        const auto checkpointRec=wal.ReadRecord(cp), lastRec=wal.ReadRecord(last);
        assert(firstRec && firstRec->header.xl_xid==11);
        assert(middleRec && middleRec->header.xl_xid==12 && middleRec->header.xl_prev==first);
        assert(checkpointRec && checkpointRec->rmid()==RM_CHECKPOINT_ID && checkpointRec->header.xl_prev==middle);
        assert(lastRec && lastRec->header.xl_xid==13 && lastRec->header.xl_prev==cp);
        assert(wal.findLastCheckpointLsn()==cp);
        const auto next=wal.ReadNextRecord(middle);
        assert(next && next->rmid()==RM_CHECKPOINT_ID);++controls;
        const auto appended=wal.XLogInsert(RM_HEAP_ID,0,14,{'d'});
        const auto appendedRec=wal.ReadRecord(appended);
        assert(appended!=INVALID_LSN && appendedRec && appendedRec->header.xl_prev==last);
        assert(wal.XLogFlush(wal.currentWriteLsn()));++controls;
    }
    uint64_t highWater=0;
    assert(WALManager::readSnapshotTransactionHighWater(dir,highWater) && highWater==100);++controls;
    // Check both startup and immutable-snapshot validation: padding and
    // every later record remain part of the chain's integrity boundary.
    for (unsigned fault=0;fault<3;++fault) {
        const auto copy=base/("bad_switch_"+std::to_string(fault));
        std::filesystem::copy(root,copy,std::filesystem::copy_options::recursive);
        WALManager wal(copy/"pg_wal");
        if(fault==0)flip(wal.segmentPath(0),WALManager::kSegmentSize-1);
        if(fault==1)flip(wal.segmentPath(2),last%WALManager::kSegmentSize+sizeof(XLogRecHeader));
        if(fault==2)assert(std::filesystem::remove(wal.segmentPath(1)));
        assert(!wal.ensureOpen());
        highWater=777;
        assert(!WALManager::readSnapshotTransactionHighWater(copy/"pg_wal",highWater));
        assert(highWater==777);++controls;
        std::filesystem::remove_all(copy);
    }
}

void shortSwitchPadding(const std::filesystem::path& base, size_t& controls) {
    // SWITCH has always padded up to the segment edge, including an eight
    // byte tail too short for a WAL header. Keep that old on-disk framing
    // readable both when the stream ends at the padding and after appending.
    const auto dir=base/"short_switch"/"pg_wal";
    {
        WALManager wal(dir);assert(wal.ensureOpen());
        const size_t length=WALManager::kSegmentSize-8-sizeof(XLogRecHeader);
        assert(wal.XLogInsert(RM_HEAP_ID,0,31,std::vector<char>(length,'x'))==0);
        assert(wal.currentWriteLsn()==WALManager::kSegmentSize-8);
        assert(wal.switchWal()==WALManager::kSegmentSize);
    }
    {
        WALManager wal(dir);assert(wal.ensureOpen());
        assert(wal.currentWriteLsn()==WALManager::kSegmentSize);
        const auto appended=wal.XLogInsert(RM_HEAP_ID,0,32,{'y'});
        const auto record=wal.ReadRecord(appended);
        assert(appended==WALManager::kSegmentSize && record && record->header.xl_prev==0);
        assert(wal.XLogFlush(wal.currentWriteLsn()));++controls;
    }
    {
        WALManager wal(dir);assert(wal.ensureOpen());
        assert(wal.ReadRecord(WALManager::kSegmentSize)->header.xl_xid==32);
        uint64_t highWater=0;
        assert(WALManager::readSnapshotTransactionHighWater(dir,highWater) && highWater==32);++controls;
    }
}

void switchAfterPeerAppend(const std::filesystem::path& base, size_t& controls) {
    const auto dir = base / "switch_after_peer_append" / "pg_wal";
    WALManager writer(dir);
    assert(writer.ensureOpen());
    const auto first = writer.XLogInsert(RM_HEAP_ID, 0, 41, {'a'});
    assert(first == 0);

    WALManager switcher(dir);
    assert(switcher.ensureOpen());
    const auto peerRecord = writer.XLogInsert(RM_HEAP_ID, 0, 42, {'b'});
    assert(peerRecord != INVALID_LSN && peerRecord > first);
    assert(writer.XLogFlush(writer.currentWriteLsn()));
    assert(switcher.switchWal() == WALManager::kSegmentSize);
    const auto tail = switcher.XLogInsert(RM_HEAP_ID, 0, 43, {'c'});
    assert(tail == WALManager::kSegmentSize);
    assert(switcher.XLogFlush(switcher.currentWriteLsn()));

    WALManager reopened(dir);
    assert(reopened.ensureOpen());
    assert(reopened.ReadRecord(peerRecord)->header.xl_xid == 42);
    assert(reopened.ReadRecord(tail)->header.xl_prev == peerRecord);
    ++controls;
}
}
int main() {
    char temporary[]="/tmp/dbms-wal-prefix-XXXXXX";
    assert(mkdtemp(temporary));const std::filesystem::path base=temporary;
    size_t controls=0;
    switchBoundaries(base,controls);
    shortSwitchPadding(base,controls);
    switchAfterPeerAppend(base,controls);
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
