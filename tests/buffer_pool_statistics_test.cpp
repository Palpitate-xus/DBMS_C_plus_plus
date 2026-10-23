// test_sources: src/storage/BufferPool.cpp src/access/BPTree.cpp
// Controlled lock ownership, not a probabilistic timing/data-race oracle.
#include "access/BPTree.h"
#include "commands/TableManage.h"
#include "storage/BufferPool.h"
#include "storage/PageAllocator.h"
#include <atomic>
#include <cerrno>
#include <cmath>
#include <dlfcn.h>
#include <future>
#include <iostream>
#include <limits>
#include <semaphore.h>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <unistd.h>

namespace {
thread_local int role=0;
thread_local bool reported=false;
std::atomic<pthread_mutex_t*> ownerMutex{nullptr},observerMutex{nullptr};
sem_t holderEntered,releaseHolder,observerEvent;
void require(bool good,const char* message) {if(!good)throw std::runtime_error(message);}
void post(sem_t& sem) {if(::sem_post(&sem)!=0)::_exit(87);}
void wait(sem_t& sem) {int r;do{r=::sem_wait(&sem);}while(r && errno==EINTR);if(r)::_exit(88);}
bool timedWait(sem_t& sem) {
    timespec limit{};if(::clock_gettime(CLOCK_REALTIME,&limit))return false;limit.tv_sec+=5;
    int r;do{r=::sem_timedwait(&sem,&limit);}while(r && errno==EINTR);return r==0;
}
template<class T,class=void> struct HasStatistics:std::false_type{};
template<class T> struct HasStatistics<T,std::void_t<decltype(std::declval<const T&>().statistics())>>:std::true_type{};
template<class T,class=void> struct HasCacheStatistics:std::false_type{};
template<class T> struct HasCacheStatistics<T,std::void_t<decltype(std::declval<const T&>().cacheStatistics())>>:std::true_type{};
template<class T> double operation(T& pool,int op) {
    if(op==0)return static_cast<double>(pool.hits());
    if(op==1)return static_cast<double>(pool.misses());
    if(op==2)return pool.hitRate();
    if(op==3){pool.resetStats();return 0;}
    if constexpr(HasStatistics<T>::value)return pool.statistics().hitRate();
    return -1;
}
void seed(dbms::BufferPool& pool) {
    require(pool.open(),"open pool");
    auto* page=pool.fetchPage(1);require(page,"seed page");page[0]='S';pool.markDirty(1);pool.unpinPage(1);
    for(int i=0;i<4;++i){require(pool.fetchPage(1),"warm page");pool.unpinPage(1);}
}
}

extern "C" int pthread_mutex_lock(pthread_mutex_t* mutex) noexcept {
    using Original=int(*)(pthread_mutex_t*);
    static const auto original=reinterpret_cast<Original>(dlsym(RTLD_NEXT,"pthread_mutex_lock"));
    if(!original)::_exit(86);
    if(role==1 && !reported){reported=true;ownerMutex=mutex;}
    if(role==2 && !reported){reported=true;observerMutex=mutex;post(observerEvent);}
    return original(mutex);
}

static bool lockControl(int op,bool zero) {
    dbms::BufferPool pool("statistics-lock-"+std::to_string(op)+"-"+std::to_string(zero)+".data",2,128);
    seed(pool);if(zero)pool.resetStats();
    ownerMutex=nullptr;observerMutex=nullptr;
    pool.setWritebackBarrier([]{post(holderEntered);wait(releaseHolder);return true;});
    bool flushed=false;std::thread holder([&]{role=1;reported=false;flushed=pool.flush();role=0;});
    const bool entered=timedWait(holderEntered);
    std::atomic<bool> completed{false};double value=-2;
    std::thread observer([&]{role=2;reported=false;value=operation(pool,op);completed=true;
        if(!reported)post(observerEvent);
        role=0;});
    const bool reached=timedWait(observerEvent);
    const bool synchronized=reached && observerMutex.load()==ownerMutex.load() && ownerMutex.load()!=nullptr && !completed.load();
    post(releaseHolder);holder.join();observer.join();pool.setWritebackBarrier({});
    const double expected=zero || op==3?0:op==0?4:op==1?1:80;
    const bool counters=pool.hits()==(op==3 || zero?0U:4U) && pool.misses()==(op==3 || zero?0U:1U);
    std::cout<<"BUFFER_STATS_LOCK "<<op<<' '<<zero<<" held="<<entered<<" synchronized="<<synchronized<<" value="<<value<<'\n';
    return entered && synchronized && flushed && value==expected && counters;
}

template<class T,class Check> static void snapshotControls(T& pool,Check check) {
    if constexpr(HasStatistics<T>::value) {
        const auto before=pool.statistics();
        check(before.hits==4 && before.misses==1 && before.hitRate()==80,"same-point pair");
        pool.resetStats();const auto after=pool.statistics();
        check(before.hits==4 && before.misses==1 && after.hits==0 && after.misses==0 && after.hitRate()==0,"immutable reset snapshot");
        auto edge=before;edge.hits=std::numeric_limits<size_t>::max();edge.misses=edge.hits;
        bool good=edge.hitRate()==50;edge.misses=0;good=good && edge.hitRate()==100;
        edge.misses=edge.hits;edge.hits=0;good=good && edge.hitRate()==0;
        check(good,"snapshot denominator does not wrap");
    } else {
        check(false,"same-point pair absent");check(false,"immutable reset snapshot absent");
        check(false,"snapshot denominator boundary absent");
    }
}

template<class T> static bool independentPools() {
    if constexpr(!HasStatistics<T>::value)return false;
    else {
        T held("statistics-independent-a.data",2,128),other("statistics-independent-b.data",2,128);
        seed(held);seed(other);
        held.setWritebackBarrier([]{post(holderEntered);wait(releaseHolder);return true;});
        auto writer=std::async(std::launch::async,[&]{return held.flush();});
        const bool entered=timedWait(holderEntered);
        auto reader=std::async(std::launch::async,[&]{return other.statistics();});
        const bool independent=reader.wait_for(std::chrono::seconds(5))==std::future_status::ready;
        post(releaseHolder);const bool flushed=writer.get();const auto result=reader.get();
        held.setWritebackBarrier({});return entered && independent && flushed && result.hits==4 && result.misses==1;
    }
}

template<class T> static bool concurrentHits() {
    if constexpr(!HasStatistics<T>::value)return false;
    else {
        T pool("statistics-concurrent.data",2,128);require(pool.open(),"open concurrent pool");
        require(pool.fetchPage(1),"warm concurrent page");pool.unpinPage(1);
        std::atomic<bool> go{false},bad{false};std::atomic<int> finished{0};
        std::vector<std::thread> workers;
        for(int t=0;t<4;++t)workers.emplace_back([&]{while(!go.load())std::this_thread::yield();
            for(int i=0;i<1000;++i){if(!pool.fetchPage(1))bad=true;else pool.unpinPage(1);}++finished;});
        go=true;size_t previous=0;
        while(finished.load()!=4){const auto snap=pool.statistics();
            if(snap.misses!=1 || snap.hits<previous || snap.hits>4000 || !std::isfinite(snap.hitRate()) || snap.hitRate()<0 || snap.hitRate()>100)bad=true;
            previous=snap.hits;}
        for(auto& worker:workers)worker.join();
        const auto final=pool.statistics();
        return !bad && final.hits==4000 && final.misses==1 && final.hitRate()>99 && final.hitRate()<100;
    }
}

template<class T,class Check> static void btreeControls(T& tree,Check check) {
    if constexpr(HasCacheStatistics<T>::value) {
        const auto closed=tree.cacheStatistics();check(closed.hits==0 && closed.misses==0,"closed BTree snapshot");
        require(tree.open() && tree.insert("key",7),"seed BTree stats");int64_t value=0;
        require(tree.search("key",value) && value==7,"BTree lookup");
        const auto snap=tree.cacheStatistics();
        check(snap.hits==tree.cacheHits() && snap.misses==tree.cacheMisses() && snap.hits>0 && snap.misses>0,"BTree forwards same-point snapshot");
    } else {check(false,"closed BTree snapshot absent");check(false,"BTree paired snapshot absent");}
}

int main() {
    require(::sem_init(&holderEntered,0,0)==0 && ::sem_init(&releaseHolder,0,0)==0 && ::sem_init(&observerEvent,0,0)==0,"semaphore init");
    size_t controls=0,failures=0;
    const auto check=[&](bool good,const std::string& name){++controls;if(!good){++failures;std::cerr<<"BUFFER_STATS_FAILURE "<<name<<'\n';}};
    {
        dbms::BufferPool pool("statistics-basic.data",2,128);
        check(pool.hits()==0 && pool.misses()==0 && pool.hitRate()==0,"initial statistics");
        require(pool.open() && pool.fetchPage(1),"basic first miss");pool.unpinPage(1);
        check(pool.hits()==0 && pool.misses()==1 && pool.hitRate()==0,"one miss");
        for(int i=0;i<4;++i){require(pool.fetchPage(1),"basic warm");pool.unpinPage(1);}
        check(pool.hits()==4 && pool.misses()==1 && pool.hitRate()==80,"warm statistics");
        pool.resetStats();check(pool.hits()==0 && pool.misses()==0 && pool.hitRate()==0,"reset");
        require(pool.fetchPage(1),"cached after reset");pool.unpinPage(1);
        check(pool.hits()==1 && pool.misses()==0 && pool.hitRate()==100,"cache retained by reset");
        pool.close();const bool retained=pool.hits()==1 && pool.misses()==0;pool.resetStats();
        check(retained && pool.hits()==0 && pool.misses()==0,"closed statistics/reset");
    }
    for(bool zero:{false,true})for(int op=0;op<5;++op)check(lockControl(op,zero),"statistics lock ownership");
    {dbms::BufferPool pool("statistics-snapshot.data",2,128);seed(pool);snapshotControls(pool,check);}
    check(independentPools<dbms::BufferPool>(),"independent pools do not share statistics lock");
    check(concurrentHits<dbms::BufferPool>(),"concurrent exact hits and snapshots");
    {dbms::BPTree tree("statistics-tree.idx");btreeControls(tree,check);}
    {
        dbms::StorageEngine engine;const std::string database="statistics-engine";
        require(engine.createDatabase(database)==dbms::DBStatus::OK,"engine database");
        dbms::TableSchema schema;schema.tablename="rows";schema.append(dbms::makeIntColumn("id",false,2,false));
        require(engine.createTable(database,schema)==dbms::DBStatus::OK && engine.insert(database,"rows",{{"id","1"}})==dbms::DBStatus::OK,"engine row");
        auto* allocator=engine.getPageAllocator(database,"rows");require(allocator,"engine allocator");
        auto* pool=allocator->bufferPool();pool->resetStats();require(allocator->fetchPage(1),"engine cached row");allocator->unpinPage(1);
        const auto stats=engine.getBufferPoolStats();const double total=static_cast<double>(stats.totalHits)+static_cast<double>(stats.totalMisses);
        check(stats.totalHits>=pool->hits() && stats.totalMisses>=pool->misses() && total>0 && stats.hitRate==100*static_cast<double>(stats.totalHits)/total,"engine statistics consumer");
    }
    ::sem_destroy(&holderEntered);::sem_destroy(&releaseHolder);::sem_destroy(&observerEvent);
    if(controls!=24){++failures;std::cerr<<"BUFFER_STATS_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"BUFFER_STATS_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';return failures?1:0;
}
