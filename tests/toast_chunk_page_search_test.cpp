#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/PageWrapper.h"
#include <array>
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>

extern dbms::StorageEngine g_engine;
extern char** environ;
namespace {
using namespace dbms;
constexpr size_t pageBytes = 8192, chunkBytes = 2048;
const std::string database = "__t_toast_page_search";
using Key = std::pair<uint64_t, uint32_t>;
using Rid = std::pair<uint32_t, uint16_t>;
void need(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
std::string payload(size_t size, uint32_t seed) {
    std::string result(size, '\0');
    for (auto& value : result) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        value = static_cast<char>(seed & 255);
    }
    return result;
}
std::vector<std::string> values() {
    return {payload(1000, 71), payload(12 * chunkBytes, 72), payload(800, 73),
            payload(9 * chunkBytes + 512, 74), payload(7 * chunkBytes + 77, 75),
            payload(1048576 + 777, 76)};
}
std::vector<uint64_t> ids() {
    std::ifstream in("ids.bin", std::ios::binary); need(bool(in), "actual ID observation exists");
    std::vector<uint64_t> result(6);
    in.read(reinterpret_cast<char*>(result.data()), result.size() * sizeof(uint64_t));
    need(bool(in) && in.peek() == std::char_traits<char>::eof(), "all actual IDs exactly decoded");
    return result;
}
void writer() {
    need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual own parent database");
    TableSchema schema; schema.tablename = "items";
    schema.append(makeIntColumn("id", false, 4, true)); schema.append(makeBlobColumn("body", true));
    need(g_engine.createTable(database, schema) == DBStatus::OK, "actual TOAST parent relation");
    std::vector<uint64_t> allocated;
    const auto data = values();
    for (size_t i = 0; i < data.size(); ++i) {
        const auto id = g_engine.allocToastId(database, "items");
        need(id != 0 && (allocated.empty() || id > allocated.back()), "actual nonreused reserved ID");
        allocated.push_back(id);
        need(g_engine.writeToast(database, "items", id, data[i]), "actual full raw TOAST write");
        std::string observed;
        need(g_engine.readToast(database, "items", id, data[i].size(), observed) && observed == data[i],
             "all binary bytes including NUL round-trip through the raw TOAST API");
        if (i == 3) g_engine.deleteToast(database, "items", allocated.front());
    }
    std::ofstream out("ids.bin", std::ios::binary);
    out.write(reinterpret_cast<const char*>(allocated.data()), allocated.size() * sizeof(uint64_t));
    out.flush(); need(bool(out), "actual ID observation complete");
    std::ofstream path("toast-path.txt");
    path << std::filesystem::absolute(g_engine.toastDataPath(database, "items")).string() << '\n';
    path.flush(); need(bool(path), "actual heap pathname recorded");
    std::cout << "TOAST_SEARCH_WRITER values=6 full_bytes=exact raw_binary_API=true\n";
}
void reader() {
    const auto allocated = ids(); const auto data = values();
    for (size_t i = 0; i < data.size(); ++i) {
        std::string observed;
        const bool found = g_engine.readToast(database, "items", allocated[i], data[i].size(), observed);
        need(i == 0 ? !found : found && observed == data[i],
             "fresh process reads every full raw value and retains the actual deletion");
    }
    std::cout << "TOAST_SEARCH_READER full_bytes=exact deleted_ID_absent=true\n";
}

// A deliberately independent serial first-fit model uses the page codec for
// capacity, not the writer's search hint. It also visits earlier pages for tails.
struct Model {
    std::vector<std::vector<char>> pages{std::vector<char>{}};
    std::map<Key, Rid> locations;
    std::map<Key, std::string> rows;
    void add(uint64_t id, const std::string& input) {
        uLongf size = compressBound(static_cast<uLong>(input.size()));
        std::string compressed(size, '\0');
        need(compress2(reinterpret_cast<Bytef*>(compressed.data()), &size,
             reinterpret_cast<const Bytef*>(input.data()), input.size(), Z_DEFAULT_COMPRESSION) == Z_OK,
             "actual codec prerequisite");
        compressed.resize(size); const bool packed = compressed.size() < input.size();
        const auto& stored = packed ? compressed : input;
        for (size_t offset = 0, seq = 0; offset < stored.size(); offset += chunkBytes, ++seq) {
            const uint32_t sequence = static_cast<uint32_t>(seq);
            const uint64_t originalSize = input.size(); const uint8_t flags = packed ? 1 : 0;
            std::string row;
            row.append(reinterpret_cast<const char*>(&id), sizeof(id));
            row.append(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
            row.append(reinterpret_cast<const char*>(&flags), sizeof(flags));
            row.append(reinterpret_cast<const char*>(&originalSize), sizeof(originalSize));
            row.append(stored, offset, std::min(chunkBytes, stored.size() - offset));
            bool inserted = false; uint16_t slot = 0; uint32_t target = 0;
            for (uint32_t pageId = 1; pageId < pages.size(); ++pageId) {
                PageWrapper page(pages[pageId].data(), pageBytes, 2);
                if (page.canFit(row.size())) {
                    need(page.insert(row.data(), row.size(), slot), "model fitting tuple inserts");
                    target = pageId; inserted = true; break;
                }
            }
            if (!inserted) {
                target = static_cast<uint32_t>(pages.size()); pages.emplace_back(pageBytes, 0);
                PageWrapper page(pages.back().data(), pageBytes, 2); page.init(target);
                need(page.insert(row.data(), row.size(), slot), "model fresh page inserts");
            }
            need(locations.emplace(Key{id, sequence}, Rid{target, slot}).second, "unique model chunk");
            rows.emplace(Key{id, sequence}, std::move(row));
        }
    }
    void remove(uint64_t id) {
        for (auto it = locations.begin(); it != locations.end();) {
            if (it->first.first != id) { ++it; continue; }
            const auto [pageId, slot] = it->second;
            PageWrapper page(pages[pageId].data(), pageBytes, 2);
            need(page.remove(slot), "model actual tuple removal");
            rows.erase(it->first); it = locations.erase(it);
        }
    }
};
void verifyClosed(const std::filesystem::path& file) {
    const auto allocated = ids(); const auto input = values(); Model expected;
    for (size_t i = 0; i < input.size(); ++i) {
        expected.add(allocated[i], input[i]);
        if (i == 3) expected.remove(allocated.front());
    }
    need(std::filesystem::file_size(file) == expected.pages.size() * pageBytes,
         "closed TOAST heap has the serial model's exact page count");
    std::ifstream in(file, std::ios::binary); need(bool(in), "closed actual TOAST heap opens");
    std::map<Key, Rid> observedLocations; std::map<Key, std::string> observedRows;
    in.seekg(pageBytes);
    for (uint32_t pageId = 1; pageId < expected.pages.size(); ++pageId) {
        std::array<char, pageBytes> bytes{};
        in.read(bytes.data(), bytes.size()); need(bool(in), "whole closed page read");
        PageWrapper page(bytes.data(), bytes.size(), 2);
        need(page.isValid(pageId) && page.verifyChecksum(), "actual page identity and checksum valid");
        page.forEachLive([&](uint16_t slot, const char* row, size_t size) {
            need(size >= 21, "actual chunk header complete"); uint64_t id = 0; uint32_t seq = 0;
            std::memcpy(&id, row, sizeof(id)); std::memcpy(&seq, row + sizeof(id), sizeof(seq));
            const Key key{id, seq};
            need(observedLocations.emplace(key, Rid{pageId, slot}).second, "no duplicate live chunks");
            observedRows.emplace(key, std::string(row, size));
        });
    }
    need(observedLocations == expected.locations, "all actual page and slot placements equal serial first fit");
    need(observedRows == expected.rows, "all actual chunk headers and complete payload bytes equal model");
    uint32_t fullPage = 0;
    for (uint32_t seq = 0; seq < 9; ++seq)
        fullPage = std::max(fullPage, observedLocations.at(Key{allocated[3], seq}).first);
    need(observedLocations.at(Key{allocated[3], 9}).first < fullPage,
         "short final chunk reuses an earlier hole after full chunks pass it");
    std::cout << "TOAST_SEARCH_COMPLETE pages=" << expected.pages.size() - 1
              << " chunks=" << observedRows.size()
              << " all_bytes_Fletcher16_RIDs=exact tail_hole_reused=true fresh_children=2\n";
}
void runChild(const std::filesystem::path& self, const char* mode) {
    const pid_t child = fork(); need(child >= 0, "actual fork succeeds");
    if (child == 0) {
        const auto program = self.string();
        execl(program.c_str(), program.c_str(), mode, static_cast<char*>(nullptr));
        _exit(127);
    }
    int status = 0; pid_t waited;
    do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    need(waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0,
         "exact owned child reaped with actual normal exit zero");
}
}

int main(int argc, char** argv) {
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 2 && std::string(argv[1]) == "--write") { writer(); return 0; }
        if (argc == 2 && std::string(argv[1]) == "--read") { reader(); return 0; }
        need(argc == 1, "known invocation");
        const auto self = std::filesystem::canonical("/proc/self/exe");
        need(std::filesystem::create_directory("case"), "fresh owned fixture directory");
        std::filesystem::current_path("case");
        need(setenv("DBMS_DATA_DIR", std::filesystem::current_path().c_str(), 1) == 0,
             "child uses own absolute data root");
        runChild(self, "--write"); runChild(self, "--read");
        std::ifstream path("toast-path.txt"); std::string heap; std::getline(path, heap);
        need(bool(path) && !heap.empty(), "writer's actual heap pathname read");
        verifyClosed(heap);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "TOAST_SEARCH_FAILURE " << error.what() << '\n'; return 1;
    }
}
