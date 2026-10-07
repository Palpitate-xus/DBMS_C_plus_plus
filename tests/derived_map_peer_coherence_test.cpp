#include "storage/FreeSpaceMap.h"
#include "storage/VisibilityMap.h"
#include <cassert>
#include <iostream>
#include <filesystem>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

static void fsmPeers() {
    dbms::FreeSpaceMap first("peers.fsm"), second("peers.fsm");
    assert(first.open() && second.open());
    first.setFreePercent(1, 21);
    assert(second.getFreePercent(1) == 21);
    second.setFreePercent(2, 42);
    assert(first.getFreePercent(2) == 42);
    assert(second.flushChecked());
    assert(first.flushChecked());
    first.close(); second.close();
    dbms::FreeSpaceMap cold("peers.fsm");
    assert(cold.open());
    assert(cold.getFreePercent(1) == 21 && cold.getFreePercent(2) == 42);
}

static void vmPeers() {
    dbms::VisibilityMap first("peers.vm"), second("peers.vm");
    assert(first.open() && second.open());
    first.setAllVisible(1, true);
    assert(second.isAllVisible(1));
    second.setAllVisible(2, true); // Different bits in the same physical byte.
    assert(first.isAllVisible(2));
    first.setAllVisible(1, false);
    assert(!second.isAllVisible(1) && second.isAllVisible(2));
    assert(first.flushChecked() && second.flushChecked());
    first.close(); second.close();
    dbms::VisibilityMap cold("peers.vm");
    assert(cold.open());
    assert(!cold.isAllVisible(1) && cold.isAllVisible(2));
}

static void durablePeers() {
    dbms::FreeSpaceMap first("durable.fsm"), second("durable.fsm");
    dbms::VisibilityMap left("durable.vm"), right("durable.vm");
    assert(first.open() && second.open() && left.open() && right.open());
    first.setFreePercent(1, 21); second.setFreePercent(2, 42);
    left.setAllVisible(1, true); right.setAllVisible(2, true);
    assert(first.flushChecked() && second.flushChecked());
    assert(left.flushChecked() && right.flushChecked());
    first.close(); second.close(); left.close(); right.close();
    dbms::FreeSpaceMap cold("durable.fsm");
    dbms::VisibilityMap coldVM("durable.vm");
    assert(cold.open() && coldVM.open());
    assert(cold.getFreePercent(1) == 21 && cold.getFreePercent(2) == 42);
    assert(coldVM.isAllVisible(1) && coldVM.isAllVisible(2));
}

static void durableVMPeers() {
    dbms::VisibilityMap first("vm-durable.vm"), second("vm-durable.vm");
    assert(first.open() && second.open());
    first.setAllVisible(1, true); second.setAllVisible(2, true);
    assert(first.flushChecked() && second.flushChecked());
    first.close(); second.close();
    dbms::VisibilityMap cold("vm-durable.vm");
    assert(cold.open());
    assert(cold.isAllVisible(1) && cold.isAllVisible(2));
}

static void runChild(char* program, const char* mode) {
    const auto child = ::fork();
    assert(child >= 0);
    if (child == 0) { ::execl(program, program, mode, nullptr); _exit(127); }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void independentProcess(char* program) {
    dbms::FreeSpaceMap fsm("process.fsm");
    dbms::VisibilityMap vm("process.vm");
    assert(fsm.open() && vm.open());
    fsm.setFreePercent(1, 31); vm.setAllVisible(1, true);
    // Parent has pending cells while a fresh process publishes disjoint cells.
    runChild(program, "writer");
    assert(fsm.getFreePercent(2) == 62 && vm.isAllVisible(2));
    assert(fsm.flushChecked() && vm.flushChecked());
    runChild(program, "process-verify");
    vm.setAllVisible(1, false);
    assert(vm.flushChecked());
    runChild(program, "clear");
    assert(fsm.getFreePercent(2) == 63 && !vm.isAllVisible(2));
}

static void concurrentOwners() {
    dbms::FreeSpaceMap first("threads.fsm"), second("threads.fsm");
    dbms::VisibilityMap left("threads.vm"), right("threads.vm");
    assert(first.open() && second.open() && left.open() && right.open());
    std::thread one([&] {
        for (uint32_t i = 1; i < 128; i += 2) {
            first.setFreePercent(i, 41); left.setAllVisible(i, true);
            assert(first.flushChecked() && left.flushChecked());
        }
    });
    std::thread two([&] {
        for (uint32_t i = 2; i < 128; i += 2) {
            second.setFreePercent(i, 42); right.setAllVisible(i, true);
            assert(second.flushChecked() && right.flushChecked());
        }
    });
    one.join(); two.join();
    for (uint32_t i = 1; i < 128; ++i) {
        assert(first.getFreePercent(i) == (i % 2 ? 41 : 42));
        assert(right.isAllVisible(i));
    }
    std::filesystem::create_hard_link("threads.fsm", "threads-alias.fsm");
    dbms::FreeSpaceMap alias("threads-alias.fsm");
    assert(alias.open());
    first.setFreePercent(5, 55);
    assert(alias.getFreePercent(5) == 55 && alias.flushChecked());
}

// The verifier is a fresh exec, not an object that can inherit a live cache.
int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "vm") { vmPeers(); return 0; }
    if (argc == 2 && std::string(argv[1]) == "vm-durable") { durableVMPeers(); return 0; }
    if (argc == 2 && std::string(argv[1]) == "durable") {
        durablePeers(); return 0;
    }
    if (argc == 2 && (std::string(argv[1]) == "writer" ||
                      std::string(argv[1]) == "process-verify" ||
                      std::string(argv[1]) == "clear")) {
        dbms::FreeSpaceMap fsm("process.fsm");
        dbms::VisibilityMap vm("process.vm");
        assert(fsm.open() && vm.open());
        const std::string mode = argv[1];
        if (mode == "process-verify") {
            assert(fsm.getFreePercent(1) == 31 && fsm.getFreePercent(2) == 62);
            assert(vm.isAllVisible(1) && vm.isAllVisible(2));
        } else {
            fsm.setFreePercent(2, mode == "writer" ? 62 : 63);
            vm.setAllVisible(2, mode == "writer");
            assert(fsm.flushChecked() && vm.flushChecked());
        }
        return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "verify") {
        dbms::FreeSpaceMap fsm("peers.fsm");
        dbms::VisibilityMap vm("peers.vm");
        assert(fsm.open() && vm.open());
        assert(fsm.getFreePercent(1) == 21 && fsm.getFreePercent(2) == 42);
        assert(!vm.isAllVisible(1) && vm.isAllVisible(2));
        return 0;
    }
    fsmPeers(); vmPeers(); durablePeers(); durableVMPeers(); concurrentOwners(); independentProcess(argv[0]);
    runChild(argv[0], "verify");
    std::cout << "derived map peers and fresh-process durable bytes OK\n";
}
