// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Reuse exactly the accepted R251 private interposition helpers, not its tests.
#define main r251_reference_main
#include "buffer_pool_load_waiter_test.cpp"
#undef main

static int firstProgress() {
    timespec start{};
    require(!::clock_gettime(CLOCK_MONOTONIC, &start), "orphan progress clock");
    for (;;) {
        if (!::sem_trywait(&cancellationObserved)) return 1;
        require(errno == EAGAIN || errno == EINTR, "orphan returned event");
        if (!::sem_trywait(&waitEntered)) return 2;
        require(errno == EAGAIN || errno == EINTR, "orphan registered event");
        timespec now{};
        require(!::clock_gettime(CLOCK_MONOTONIC, &now), "orphan progress deadline");
        if (now.tv_sec - start.tv_sec >= 5) return 0;
        std::this_thread::yield();
    }
}

static int orphanScenario(bool inject) {
    setup();
    dbms::BufferPool pool("load-waiter.data", 1, 128);
    require(pool.open(), "orphan pool");
    char* original = pool.fetchPage(1);
    require(original && original[0] == 'L', "orphan original pin");
    pool.invalidatePage(1);  // Its original pin now belongs to orphanedPins_.
    bool caught = false, destroyed = false, spuriousConsumed = false;
    std::thread observer([&] {
        role = 1; cancelWait = true; spuriousRemaining = inject ? 1 : 0;
        try { pool.invalidateAll(); }
        catch (const std::bad_alloc&) { caught = true; }
        failAllocation = false;
        destroyed = lastDestroyed != nullptr;
        spuriousConsumed = spuriousRemaining == 0;
        expiredCondition = lastDestroyed;
        role = 0;
        post(cancellationObserved);
        wait(releaseObserver);
    });
    // A repaired implementation may reuse vector capacity and avoid the next
    // allocation altogether. Accept either an exception exit or an actually
    // registered wait; do not require an exception when no allocation occurs.
    const int progress = firstProgress();
    const bool heldBytes = original[0] == 'L';
    pool.unpinPage(1);
    const bool returned = progress == 1 || timed(cancellationObserved);
    const int stale = expiredNotifications.load();
    expiredCondition = nullptr;
    post(releaseObserver);
    observer.join();
    char* retry = pool.fetchPage(1);
    const bool recovered = retry && retry[0] == 'L';
    if (retry) pool.unpinPage(1);
    const bool good = progress && returned && heldBytes && destroyed && spuriousConsumed &&
        (inject || !caught) && stale == 0 && recovered && pool.quiescentForSnapshot();
    std::cout << "ORPHAN_WAITER_NATIVE inject=" << inject << " progress=" << progress << " returned=" << returned
              << " caught=" << caught << " expired_notifications=" << stale << " recovered=" << recovered
              << " good=" << good << std::endl;
    return good ? 0 : 1;
}

template<class Wait>
static int timedOrphanWait(Wait actual) {
    if (role == 1) {
        if (spuriousRemaining) { --spuriousRemaining; return 0; }
        ++waitCalls;
        post(waitEntered);
    }
    return actual();
}
extern "C" int pthread_cond_timedwait(pthread_cond_t* condition, pthread_mutex_t* mutex, const timespec* limit) {
    using Original = int(*)(pthread_cond_t*, pthread_mutex_t*, const timespec*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_cond_timedwait"));
    if (!original) ::_exit(86);
    return timedOrphanWait([&] { return original(condition, mutex, limit); });
}
extern "C" int pthread_cond_clockwait(pthread_cond_t* condition, pthread_mutex_t* mutex, clockid_t clock, const timespec* limit) {
    using Original = int(*)(pthread_cond_t*, pthread_mutex_t*, clockid_t, const timespec*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_cond_clockwait"));
    if (!original) ::_exit(86);
    return timedOrphanWait([&] { return original(condition, mutex, clock, limit); });
}
static int timedFetch(bool spurious) {
    setup();
    dbms::BufferPool pool("load-waiter.data", 1, 128);
    require(pool.open(), "timed orphan pool");
    char* original = pool.fetchPage(1);
    require(original && original[0] == 'L', "timed orphan original");
    pool.invalidatePage(1);
    pool.resetStats();
    char* result = nullptr;
    bool consumed = false;
    std::thread observer([&] {
        role = 1; spuriousRemaining = spurious ? 1 : 0;
        result = pool.fetchPage(1);
        consumed = spuriousRemaining == 0;
        role = 0;
        post(cancellationObserved);
    });
    const bool registered = timed(waitEntered);
    const bool held = original[0] == 'L';
    pool.unpinPage(1);
    const bool returned = timed(cancellationObserved);
    observer.join();
    const auto counters = pool.statistics();
    const bool good = registered && held && returned && consumed && result && result[0] == 'L' &&
        counters.hits == 0 && counters.misses == 1;
    if (result) pool.unpinPage(1);
    std::cout << "ORPHAN_TIMED_FETCH spurious=" << spurious << " registered=" << registered << " returned=" << returned
              << " good=" << good << std::endl;
    return good && pool.quiescentForSnapshot() ? 0 : 1;
}
int main(int argc, char** argv) {
    if (argc == 4) {
        if (std::string(argv[1]) == "fetch") return timedFetch(std::stoi(argv[2]));
        return orphanScenario(std::stoi(argv[2]));
    }
    size_t controls = 0, failures = 0;
    for (const char* kind : {"orphan", "fetch"}) for (int inject : {0, 1}) {
        ++controls;
        const int actual = child(argv[0], kind, inject, 0);
        if (actual) ++failures;
        std::cout << "ORPHAN_WAITER_NATIVE_CASE " << kind << ' ' << inject << " actual=" << actual << std::endl;
    }
    require(controls == 4, "complete orphan waiter inventory");
    std::cout << "ORPHAN_WAITER_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
