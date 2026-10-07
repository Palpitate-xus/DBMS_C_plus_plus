#include "storage/PageAllocator.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>

static void testFailedOwner() {
    using namespace dbms;
    const std::filesystem::path path = "allocator_failed_live_extent.dt";
    {
        PageAllocator initial(path.string(), 32);
        assert(initial.open());
        assert(initial.allocPage() == 1);
        assert(initial.flush());
    }
    PageAllocator publisher(path.string(), 32);
    PageAllocator observer("./" + path.string(), 32);
    PageAllocator opener(std::filesystem::absolute(path).string(), 32);
    assert(publisher.open());
    assert(observer.open());
    assert(publisher.allocPage() == 2);
    publisher.bufferPool()->setWritebackBarrier([]() { return false; });
    assert(!publisher.flush());
    assert(std::filesystem::exists(path.string() + ".extent_pending"));
    assert(!observer.flushPage(1));
    const bool wronglyRecovered = opener.open();
    const bool markerRetained = std::filesystem::exists(path.string() + ".extent_pending");
    std::cerr << "[FAILED LIVE EXTENT] other_open=" << wronglyRecovered
              << " marker_retained=" << markerRetained << std::endl;
    publisher.bufferPool()->setWritebackBarrier([]() { return true; });
    assert(publisher.flush());
    assert(!wronglyRecovered && markerRetained);
    assert(observer.flushPage(1));
    assert(opener.open());
    assert(opener.numPages() == 3);
    assert(!std::filesystem::exists(path.string() + ".extent_pending"));
}

int main() {
    using namespace dbms;
    using namespace std::chrono_literals;
    const std::filesystem::path path = "allocator_shared_extent.dt";
    {
        PageAllocator initial(path.string(), 32);
        assert(initial.open());
        assert(initial.allocPage() == 1);
        assert(initial.flush());
    }
    {
        PageAllocator publisher(path.string(), 32);
        PageAllocator observer("./" + path.string(), 32);
        PageAllocator opener(std::filesystem::absolute(path).string(), 32);
        assert(publisher.open());
        assert(observer.open());
        assert(publisher.allocPage() == 2);

        std::promise<void> barrierEntered, releasePublisher, observerStarted;
        auto entered = barrierEntered.get_future();
        auto release = releasePublisher.get_future().share();
        auto started = observerStarted.get_future();
        std::atomic<bool> firstBarrier{true};
        publisher.bufferPool()->setWritebackBarrier([&]() {
            if (firstBarrier.exchange(false)) barrierEntered.set_value();
            release.wait();
            return true;
        });
        auto first = std::async(std::launch::async,
                                [&]() { return publisher.flush(); });
        const bool publisherReachedBarrier = entered.wait_for(5s) == std::future_status::ready;
        const bool markerExists = std::filesystem::exists(path.string() + ".extent_pending");
        auto second = std::async(std::launch::async, [&]() {
            observerStarted.set_value();
            return observer.flushPage(1);
        });
        const bool observerReachedCall = started.wait_for(5s) == std::future_status::ready;
        const bool observerWaited = second.wait_for(200ms) == std::future_status::timeout;
        auto third = std::async(std::launch::async,
                                [&]() { return opener.open(); });
        const bool openerWaited = third.wait_for(200ms) == std::future_status::timeout;
        releasePublisher.set_value();
        const bool published = first.get();
        const bool observed = second.get();
        const bool opened = third.get();
        std::cerr << "[SHARED EXTENT] entered=" << publisherReachedBarrier
                  << " marker=" << markerExists << " observer_started=" << observerReachedCall
                  << " waited=" << observerWaited << " publish=" << published
                  << " observer=" << observed << " opener_waited=" << openerWaited
                  << " opened=" << opened << std::endl;
        assert(publisherReachedBarrier && markerExists && observerReachedCall);
        assert(observerWaited);
        assert(published && observed);
        assert(openerWaited && opened && opener.numPages() == 3);
        assert(!std::filesystem::exists(path.string() + ".extent_pending"));
    }
    {
        PageAllocator reopened(path.string(), 32);
        assert(reopened.open());
        assert(reopened.numPages() == 3);
        assert(std::filesystem::file_size(path) == 3 * PgPage::PAGE_SIZE);
    }
    testFailedOwner();
    std::cout << "[SHARED EXTENT] live publication, failed owner and retry guards passed\n";
}
