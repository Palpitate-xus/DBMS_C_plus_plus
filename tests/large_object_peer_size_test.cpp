// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

struct PrivateDirectory {
    fs::path path;
    PrivateDirectory() {
        char pattern[] = "/tmp/dbms-lo-peer-size.XXXXXX";
        const char* made = ::mkdtemp(pattern);
        require(made != nullptr, "mkdtemp failed");
        path = made;
    }
    ~PrivateDirectory() {
        std::error_code error;
        fs::remove_all(path, error);
    }
};

void put(const fs::path& path, const std::string& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.close();
    require(out.good(), "fixture source write failed");
}

void peerMutation(const fs::path& db, const fs::path& input, int id, int scenario) {
    dbms::LargeObjectManager peer(db.string());
    switch (scenario) {
        case 0: require(peer.write(id, 0, std::string(131, 'G')), "peer grow"); break;
        case 1: require(peer.truncate(id, 3), "peer shrink"); break;
        case 2: require(peer.truncate(id, 139), "peer extend"); break;
        case 3: require(peer.truncate(id, 0), "peer zero"); break;
        case 4: case 5: case 6: case 17:
            require(peer.importFile(id, input.string()), "peer import"); break;
        case 7: require(peer.drop(id), "peer drop"); break;
        case 8: case 9: case 10: {
            const int created = peer.create();
            require(created == id, "peer late create");
            if (scenario == 9) require(peer.write(id, 0, std::string(47, 'L')), "late write");
            if (scenario == 10) require(peer.write(id, 4096, "tail"), "late sparse");
            break;
        }
        case 11: case 13: case 14:
            require(peer.write(id, 0, std::string(131, 'G')), "peer grow observer"); break;
        case 12: require(peer.truncate(id, 3), "peer shrink observer"); break;
        case 15: require(peer.write(id, 0, "same"), "peer overwrite"); break;
        case 16: require(peer.write(id, 4096, "tail"), "peer sparse"); break;
        default: throw std::runtime_error("unknown scenario");
    }
}

void mutate(const fs::path& db, const fs::path& input, int id, int scenario, bool forkPeer) {
    if (!forkPeer) {
        peerMutation(db, input, id, scenario);
        return;
    }
    const pid_t pid = ::fork();
    require(pid >= 0, "fork failed");
    if (pid == 0) {
        try {
            peerMutation(db, input, id, scenario);
            ::_exit(0);
        } catch (const std::exception& error) {
            std::cerr << "peer setup failure: " << error.what() << '\n';
            ::_exit(2);
        }
    }
    int status = 0;
    pid_t waited;
    do { waited = ::waitpid(pid, &status, 0); } while (waited < 0 && errno == EINTR);
    require(waited == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0, "peer process failed");
}

bool peerCase(int scenario, bool forkPeer) {
    PrivateDirectory dir;
    const auto db = dir.path / "db";
    const auto input = dir.path / "input";
    const bool late = scenario >= 8 && scenario <= 10;
    dbms::LargeObjectManager seed(db.string());
    const int id = late ? 1 : seed.create();
    require(id == 1, "seed id");
    if (!late) require(seed.write(id, 0, "seed payload!"), "seed write");
    dbms::LargeObjectManager observer(db.string());
    require(observer.size(id) == (late ? 0 : 13), "initial size");
    if (scenario == 4) put(input, std::string(211, 'I'));
    if (scenario == 5) put(input, "short");
    if (scenario == 6) put(input, "");
    if (scenario == 17) put(input, std::string(13, 'S'));
    mutate(db, input, id, scenario, forkPeer);
    if (scenario == 11) require(observer.write(id, 0, "own"), "observer overwrite grown");
    if (scenario == 12) require(observer.write(id, 0, "own!"), "observer overwrite shrunken");
    if (scenario == 13) require(observer.write(id, 1024, ""), "observer empty write");
    const fs::path object = db / ".lobjects/lo_1.dat";
    const size_t expected = scenario == 7 ? 0 : static_cast<size_t>(fs::file_size(object));
    const std::string bytes = observer.read(id);
    require(bytes.size() == expected, "read did not observe peer size");
    const size_t actual = observer.size(id);
    dbms::LargeObjectManager reopened(db.string());
    require(reopened.size(id) == expected, "fresh manager disagrees");
    if (actual != expected) {
        std::cerr << "SIZE_MISMATCH scenario=" << scenario << " fork=" << forkPeer
                  << " actual=" << actual << " expected=" << expected << '\n';
    }
    // Repeated size reads must not alter the object or hide a stale result.
    return actual == expected && observer.size(id) == expected && observer.read(id) == bytes;
}

bool guardCase(int scenario) {
    PrivateDirectory dir;
    const auto db = dir.path / "db";
    dbms::LargeObjectManager objects(db.string());
    const int id = objects.create();
    require(id == 1 && objects.write(id, 0, "seed payload!"), "guard seed");
    switch (scenario) {
        case 0: return objects.size(0) == 0;
        case 1: return objects.size(-1) == 0;
        case 2: return objects.size(std::numeric_limits<int>::max()) == 0;
        case 3: return objects.size(42) == 0;
        case 4:
            require(objects.drop(id), "guard drop");
            return objects.size(id) == 0;
        case 5: {
            const int empty = objects.create();
            require(empty > 0, "guard empty create");
            return objects.size(empty) == 0;
        }
        case 6:
            require(!objects.write(id, std::numeric_limits<size_t>::max(), "x"), "overflow write accepted");
            break;
        case 7:
            require(!objects.truncate(id, std::numeric_limits<size_t>::max()), "overflow truncate accepted");
            break;
        case 8:
            require(!objects.importFile(id, (dir.path / "absent").string()), "missing import accepted");
            break;
        case 9: {
            dbms::LargeObjectManager peer(db.string());
            const int other = peer.create();
            require(other == 2 && peer.write(other, 0, std::string(77, 'O')), "unrelated object");
            break;
        }
        default: throw std::runtime_error("unknown guard");
    }
    return objects.size(id) == 13 && objects.read(id) == "seed payload!";
}
} // namespace

int main() {
    int controls = 0;
    int failed = 0;
    for (bool forkPeer : {false, true}) {
        for (int scenario = 0; scenario < 18; ++scenario) {
            ++controls;
            try { if (!peerCase(scenario, forkPeer)) ++failed; }
            catch (const std::exception& error) {
                ++failed;
                std::cerr << "CONTROL_ERROR peer=" << scenario << " fork=" << forkPeer << ' ' << error.what() << '\n';
            }
        }
    }
    for (int scenario = 0; scenario < 10; ++scenario) {
        ++controls;
        try { if (!guardCase(scenario)) ++failed; }
        catch (const std::exception& error) {
            ++failed;
            std::cerr << "CONTROL_ERROR guard=" << scenario << ' ' << error.what() << '\n';
        }
    }
    std::cout << "LARGE_OBJECT_PEER_SIZE_COMPLETE " << controls << " FAILURES " << failed << '\n';
    return failed == 0 ? 0 : 1;
}
