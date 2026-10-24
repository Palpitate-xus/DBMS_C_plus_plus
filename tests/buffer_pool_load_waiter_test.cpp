// test_sources: src/storage/BufferPool.cpp src/storage/PageCrypto.cpp
// Controlled load-generation handoff and expired registration cancellation.
#include "storage/BufferPool.h"

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <dlfcn.h>
#include <iostream>
#include <new>
#include <semaphore.h>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace {
thread_local int role = 0, lockCalls = 0, waitCalls = 0, spuriousRemaining = 0;
thread_local pthread_mutex_t* poolMutex = nullptr;
thread_local pthread_cond_t* lastDestroyed = nullptr;
thread_local bool gateLock = false, cancelWait = false, failAllocation = false;
std::atomic<pthread_cond_t*> expiredCondition{nullptr};
std::atomic<int> expiredNotifications{0};
sem_t gapEntered, gapRelease, firstRead, releaseFirst, wakeEntered, wakeRelease;
sem_t nextRead, releaseNext, waitEntered, cancellationObserved, releaseObserver;

void require(bool good, const char* message) { if (!good) throw std::runtime_error(message); }
void post(sem_t& sem) { if (::sem_post(&sem)) ::_exit(87); }
void wait(sem_t& sem) {
    int result;
    do { result = ::sem_wait(&sem); } while (result && errno == EINTR);
    if (result) ::_exit(88);
}
bool timed(sem_t& sem) {
    timespec limit{};
    require(!::clock_gettime(CLOCK_REALTIME, &limit), "waiter clock");
    limit.tv_sec += 5;
    int result;
    do { result = ::sem_timedwait(&sem, &limit); } while (result && errno == EINTR);
    return result == 0;
}
void deadline(int) {
    const char text[] = "BUFFER_LOAD_WAITER_DEADLINE\n";
    const auto written = ::write(2, text, sizeof(text) - 1);
    (void)written;
    ::_exit(91);
}
void setup() {
    ::signal(SIGALRM, deadline);
    ::alarm(12);
    for (auto* sem : {&gapEntered, &gapRelease, &firstRead, &releaseFirst, &wakeEntered,
                      &wakeRelease, &nextRead, &releaseNext, &waitEntered,
                      &cancellationObserved, &releaseObserver}) {
        require(!::sem_init(sem, 0, 0), "waiter semaphore");
    }
    // Resolve destroy/broadcast before the observer arms allocation failure.
    pthread_cond_t warm;
    require(!::pthread_cond_init(&warm, nullptr), "warm cond init");
    require(!::pthread_cond_broadcast(&warm) && !::pthread_cond_destroy(&warm), "warm cond calls");
    const int fd = ::open("load-waiter.data", O_RDWR | O_CREAT | O_TRUNC, 0600);
    require(fd >= 0, "waiter file");
    char bytes[256]{};
    bytes[128] = 'L';
    require(::pwrite(fd, bytes, sizeof(bytes), 0) == static_cast<ssize_t>(sizeof(bytes)), "waiter seed");
    require(!::close(fd), "waiter file close");
}
}

void* operator new(std::size_t size) {
    if (failAllocation) { failAllocation = false; throw std::bad_alloc(); }
    if (void* value = std::malloc(size ? size : 1)) return value;
    throw std::bad_alloc();
}
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete[](void* value) noexcept { std::free(value); }
void operator delete[](void* value, std::size_t) noexcept { std::free(value); }

extern "C" int pthread_mutex_lock(pthread_mutex_t* mutex) noexcept {
    using Original = int(*)(pthread_mutex_t*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_mutex_lock"));
    if (!original) ::_exit(86);
    if (role == 1 && gateLock) {
        if (!poolMutex) poolMutex = mutex;
        if (poolMutex == mutex && ++lockCalls == 2) {
            gateLock = false;
            post(gapEntered);
            wait(gapRelease);
        }
    }
    return original(mutex);
}
extern "C" int pthread_cond_wait(pthread_cond_t* condition, pthread_mutex_t* mutex) {
    using Original = int(*)(pthread_cond_t*, pthread_mutex_t*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_cond_wait"));
    if (!original) ::_exit(86);
    if (role != 1) return original(condition, mutex);
    if (spuriousRemaining) {
        --spuriousRemaining;
        if (cancelWait) failAllocation = true;
        // A permitted spurious return, with the mutex still held.
        return 0;
    }
    ++waitCalls;
    post(waitEntered);
    const int result = original(condition, mutex);
    if (!cancelWait && waitCalls == 1) {
        // Model the legal order where a new loader acquires the pool lock
        // before the notified waiter observes state. Preserve the lock-held
        // postcondition of pthread_cond_wait when returning to production.
        require(!::pthread_mutex_unlock(mutex), "waiter schedule unlock");
        post(wakeEntered);
        wait(wakeRelease);
        require(!::pthread_mutex_lock(mutex), "waiter schedule relock");
    }
    return result;
}
extern "C" int pthread_cond_destroy(pthread_cond_t* condition) noexcept {
    using Original = int(*)(pthread_cond_t*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_cond_destroy"));
    if (!original) ::_exit(86);
    const int result = original(condition);
    if (role == 1 && result == 0) lastDestroyed = condition;
    return result;
}
extern "C" int pthread_cond_broadcast(pthread_cond_t* condition) noexcept {
    using Original = int(*)(pthread_cond_t*);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pthread_cond_broadcast"));
    if (!original) ::_exit(86);
    const auto expired = expiredCondition.load();
    if (expired && condition == expired) {
        ++expiredNotifications;
        // Record the real notify attempt, but do not dereference a condition
        // variable known to have completed destruction after cancellation.
        return 0;
    }
    return original(condition);
}
extern "C" ssize_t pread(int fd, void* bytes, size_t size, off_t offset) {
    using Original = ssize_t(*)(int, void*, size_t, off_t);
    static const auto original = reinterpret_cast<Original>(dlsym(RTLD_NEXT, "pread"));
    if (!original) ::_exit(86);
    if (role == 2) { post(firstRead); wait(releaseFirst); errno = EIO; return -1; }
    if (role == 3) { post(nextRead); wait(releaseNext); }
    return original(fd, bytes, size, offset);
}

static int generation(bool slow, bool spurious) {
    setup();
    dbms::BufferPool pool("load-waiter.data", 1, 128);
    require(pool.open(), "generation pool");
    char *failed = nullptr, *next = nullptr, *observed = nullptr;
    const auto observer = [&] {
        role = 1; gateLock = slow; spuriousRemaining = spurious ? 1 : 0;
        observed = pool.fetchPage(1);
        role = 0;
        std::cout << "LOAD_WAITER_OBSERVER_RETURNED " << (observed != nullptr) << std::endl;
    };
    std::thread watched;
    bool gap = true;
    if (slow) { watched = std::thread(observer); gap = timed(gapEntered); }
    std::thread first([&] { role = 2; failed = pool.fetchPage(1); role = 0; });
    const bool firstIO = timed(firstRead);
    if (slow) post(gapRelease);
    else watched = std::thread(observer);
    const bool registered = timed(waitEntered);
    post(releaseFirst);
    first.join();
    const bool notified = timed(wakeEntered);
    std::thread subsequent([&] { role = 3; next = pool.fetchPage(1); role = 0; });
    const bool nextIO = timed(nextRead);
    post(wakeRelease);
    const bool waitedAgain = timed(waitEntered);
    std::cout << "LOAD_WAITER_GENERATION slow=" << slow << " spurious=" << spurious << " gap=" << gap
              << " first=" << firstIO << " registered=" << registered << " notified=" << notified
              << " next=" << nextIO << " again=" << waitedAgain << std::endl;
    post(releaseNext);
    subsequent.join();
    std::cout << "LOAD_WAITER_NEXT_LOADER_RETURNED " << (next != nullptr) << std::endl;
    watched.join();
    const auto counters = pool.statistics();
    const bool good = gap && firstIO && registered && notified && nextIO && waitedAgain && !failed &&
        next && next == observed && next[0] == 'L' && counters.hits == 1 && counters.misses == 2;
    if (next) pool.unpinPage(1);
    if (observed) pool.unpinPage(1);
    return good ? 0 : 1;
}

static int cancellation(int method) {
    setup();
    dbms::BufferPool pool("load-waiter.data", 1, 128);
    require(pool.open(), "cancel pool");
    char* failed = nullptr;
    std::thread first([&] { role = 2; failed = pool.fetchPage(1); role = 0; });
    const bool firstIO = timed(firstRead);
    bool caught = false, destroyed = false;
    std::thread observer([&] {
        role = 1; cancelWait = true; spuriousRemaining = 1;
        try {
            if (method == 0) (void)pool.fetchPage(1);
            if (method == 1) pool.close();
            if (method == 2) pool.invalidatePage(1);
            if (method == 3) pool.invalidateAll();
        } catch (const std::bad_alloc&) { caught = true; }
        failAllocation = false;
        destroyed = lastDestroyed != nullptr;
        // The function has unwound; every CV owned by that call has finished
        // destruction before its address is armed for notification watching.
        expiredCondition = lastDestroyed;
        role = 0;
        post(cancellationObserved);
        wait(releaseObserver);
    });
    const bool cancelled = timed(cancellationObserved);
    post(releaseFirst);
    first.join();
    const int stale = expiredNotifications.load();
    expiredCondition = nullptr;
    post(releaseObserver);
    observer.join();
    const bool open = pool.isOpen();
    char* retry = open ? pool.fetchPage(1) : nullptr;
    const bool recovered = retry && retry[0] == 'L';
    if (retry) pool.unpinPage(1);
    std::cout << "LOAD_WAITER_CANCELLATION method=" << method << " first=" << firstIO << " cancelled=" << cancelled
              << " caught=" << caught << " destroyed=" << destroyed << " expired_notifications=" << stale
              << " recovered=" << recovered << std::endl;
    return firstIO && cancelled && caught && destroyed && !failed && stale == 0 && recovered &&
        pool.quiescentForSnapshot() ? 0 : 1;
}

static int child(const char* executable, const char* kind, int first, int second) {
    const pid_t pid = ::fork();
    require(pid >= 0, "waiter fork");
    if (!pid) {
        const auto a = std::to_string(first), b = std::to_string(second);
        ::execl(executable, executable, kind, a.c_str(), b.c_str(), nullptr);
        ::_exit(127);
    }
    int status = 0;
    require(::waitpid(pid, &status, 0) == pid, "waiter child completion");
    return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

int main(int argc, char** argv) {
    if (argc == 4) {
        if (std::string(argv[1]) == "generation") return generation(std::stoi(argv[2]), std::stoi(argv[3]));
        return cancellation(std::stoi(argv[2]));
    }
    size_t controls = 0, failures = 0;
    const auto check = [&](const char* kind, int first, int second) {
        ++controls;
        const int actual = child(argv[0], kind, first, second);
        if (actual) ++failures;
        std::cout << "BUFFER_LOAD_WAITER_CASE " << kind << ' ' << first << ' ' << second << " actual=" << actual << std::endl;
    };
    for (bool slow : {false, true}) for (bool spurious : {false, true}) check("generation", slow, spurious);
    for (int method = 0; method < 4; ++method) check("cancellation", method, 0);
    require(controls == 8, "complete waiter inventory");
    std::cout << "BUFFER_LOAD_WAITER_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
