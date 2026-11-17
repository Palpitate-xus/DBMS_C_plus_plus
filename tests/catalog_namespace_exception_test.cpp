// Catalog allocation failures must release locks and preserve complete file images.
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <new>
#include <stdexcept>
#include <tuple>
#include <vector>

namespace {
thread_local bool armed = false;
thread_local size_t allocations = 0, selected = std::numeric_limits<size_t>::max();
thread_local bool fired = false;
void* allocate(size_t bytes, size_t alignment = 0) {
    if (armed && allocations++ == selected) {
        fired = true;
        throw std::bad_alloc();
    }
    void* result = nullptr;
    if (alignment) {
        if (::posix_memalign(&result, alignment, std::max<size_t>(bytes, 1))) result = nullptr;
    } else result = std::malloc(std::max<size_t>(bytes, 1));
    if (!result) throw std::bad_alloc();
    return result;
}
unsigned checks = 0;
void need(bool value, const char* reason) {
    ++checks;
    if (!value) throw std::runtime_error(reason);
}
using Image = std::map<std::string, std::string>;
Image images(const std::filesystem::path& directory) {
    Image result;
    for (const auto& file : std::filesystem::recursive_directory_iterator(directory)) {
        if (!file.is_regular_file()) continue;
        std::ifstream input(file.path(), std::ios::binary);
        result.emplace(file.path().lexically_relative(directory).string(),
                       std::string(std::istreambuf_iterator<char>(input), {}));
    }
    return result;
}
using Rows = std::vector<std::tuple<dbms::Oid, std::string, dbms::Oid>>;
Rows values(const std::vector<dbms::PgNamespaceRow>& input) {
    Rows result;
    for (const auto& row : input) result.emplace_back(row.oid, row.nspname, row.nspowner);
    return result;
}
} // namespace

void* operator new(size_t n) { return allocate(n); }
void* operator new[](size_t n) { return allocate(n); }
void* operator new(size_t n, const std::nothrow_t&) noexcept { try { return allocate(n); } catch (...) { return nullptr; } }
void* operator new[](size_t n, const std::nothrow_t&) noexcept { try { return allocate(n); } catch (...) { return nullptr; } }
void* operator new(size_t n, std::align_val_t a) { return allocate(n, static_cast<size_t>(a)); }
void* operator new[](size_t n, std::align_val_t a) { return allocate(n, static_cast<size_t>(a)); }
void* operator new(size_t n, std::align_val_t a, const std::nothrow_t&) noexcept { try { return allocate(n, static_cast<size_t>(a)); } catch (...) { return nullptr; } }
void* operator new[](size_t n, std::align_val_t a, const std::nothrow_t&) noexcept { try { return allocate(n, static_cast<size_t>(a)); } catch (...) { return nullptr; } }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }

int main() {
    try {
        using namespace dbms;
        TypeRegistry::instance().bootstrap();
        StorageEngine hot, cold;
        hot.setBackgroundIntervals(60000, 60000);
        cold.setBackgroundIntervals(60000, 60000);
        const std::string database = "__t_namespace_read_exception";
        need(!std::filesystem::exists(database), "fresh private working directory");
        need(hot.createDatabase(database) == DBStatus::OK, "create actual database");
        need(hot.createSchema(database, std::string(50, 'q')) == DBStatus::OK, "nontrivial namespace row");
        const auto expected = values(hot.catalogService().metadataSnapshot(database).namespaces);
        const auto before = images(hot.dbPath(database) / "pg_catalog");
        size_t injected = 0;
        for (StorageEngine* engine : {&hot, &cold}) {
            for (bool complete : {false, true}) {
                const auto read = [&] {
                    return complete ? engine->catalogService().metadataSnapshot(database).namespaces
                                    : engine->catalogService().namespaceSnapshot(database);
                };
                need(values(read()) == expected, "warm read-only control");
                selected = std::numeric_limits<size_t>::max();
                allocations = 0;
                armed = true;
                { const auto rows = read(); (void)rows; }
                armed = false;
                const size_t count = allocations;
                need(count > 0, "actual allocation sites exercised");
                // Exhaust short paths; for large complete loads, select up to
                // 64 evenly spaced sites including both ends. No OOM equality
                // across different APIs or allocation counts is assumed.
                std::vector<size_t> points;
                for (size_t i = 0; i < std::min<size_t>(count, 64); ++i)
                    points.push_back(count <= 64 ? i : i * (count - 1) / 63);
                for (size_t point : points) {
                    allocations = 0;
                    selected = point;
                    fired = false;
                    bool failed = false;
                    std::vector<PgNamespaceRow> result;
                    armed = true;
                    try { result = read(); }
                    catch (const std::bad_alloc&) { failed = true; }
                    catch (const DbError& error) {
                        // Existing loaders may translate a stream's allocation
                        // failure into a catalog read/validation error. Both
                        // must fail closed, rather than return partial rows.
                        if (error.sqlState() != "58030" && error.sqlState() != "XX001" &&
                            error.sqlState() != "53200") { armed = false; throw; }
                        failed = true;
                    }
                    catch (...) { armed = false; throw; }
                    armed = false;
                    if (!(fired && failed && result.empty()))
                        std::cerr << "fault context complete=" << complete << " cold=" << (engine == &cold)
                                  << " point=" << point << " sites=" << count << " fired=" << fired
                                  << " failed=" << failed << " rows=" << result.size() << '\n';
                    need(fired && failed && result.empty(), "allocation failure is not an empty successful catalog");
                    need(values(read()) == expected, "locks released and complete read retry succeeds");
                    ++injected;
                }
                std::cout << "namespace-read complete=" << complete << " cold=" << (engine == &cold)
                          << " sites=" << count << " injected=" << points.size() << '\n';
            }
        }
        need(!cold.catalogService().has(database), "faulted cold reads never populate cache");
        need(images(hot.dbPath(database) / "pg_catalog") == before, "faults and retries never publish catalog bytes");
        const auto directory = hot.dbPath(database) / "serialization_fixture";
        {
            CatalogManager metadata(directory.string(), false);
            metadata.createNamespace(std::string(50, 'r'), 0);
            need(metadata.persistAll(), "initial complete metadata publication");
            const auto original = images(directory);
            metadata.createNamespace(std::string(50, 's'), 0);
            need(metadata.hasUnpublishedChanges(), "actual pending catalog edit");
            selected = std::numeric_limits<size_t>::max();
            allocations = 0;
            armed = true;
            (void)metadata.hasUnpublishedChanges();
            armed = false;
            const size_t count = allocations;
            need(count > 0 && count < 256, "bounded actual serialization allocation sites");
            // persistAll serializes before it opens or changes any image.
            // Exercise every allocation in that shared serialization phase,
            // as both a read-side comparison and an actual publication call.
            for (bool publish : {false, true}) {
                for (size_t point = 0; point < count; ++point) {
                    allocations = 0;
                    selected = point;
                    fired = false;
                    bool failed = false;
                    armed = true;
                    try {
                        if (publish) (void)metadata.persistAll();
                        else (void)metadata.hasUnpublishedChanges();
                    } catch (const std::bad_alloc&) { failed = true; }
                    catch (...) { armed = false; throw; }
                    armed = false;
                    const bool unchanged = images(directory) == original;
                    if (!(fired && failed && unchanged))
                        std::cerr << "serialization fault publish=" << publish << " point=" << point
                                  << " fired=" << fired << " failed=" << failed << " bytes_equal=" << unchanged << '\n';
                    need(fired && failed && unchanged, "incomplete serialization never succeeds or changes catalog files");
                    need(metadata.hasUnpublishedChanges(), "failed serialization retains pending edit and releases locks");
                    ++injected;
                }
            }
            need(metadata.persistAll(), "retry publishes complete images");
            need(!metadata.hasUnpublishedChanges(), "complete publication clears pending edit");
            const auto restored = CatalogManager::readMetadataSnapshot(directory.string());
            need(restored.namespaces.size() == 2 && restored.namespaces[0].nspname == std::string(50, 'r') &&
                 restored.namespaces[1].nspname == std::string(50, 's'), "published images reopen with both full namespace values");
            std::cout << "serialization sites=" << count << " read-and-publish faults=" << count * 2 << '\n';
        }
        need(hot.dropDatabase(database) == DBStatus::OK, "cleanup owned database");
        std::cout << "CATALOG_NAMESPACE_EXCEPTION_PASS checks=" << checks << " faults=" << injected << '\n';
        return 0;
    } catch (const std::exception& error) {
        armed = false;
        std::cerr << "CATALOG_NAMESPACE_EXCEPTION_FAIL checks=" << checks << " " << error.what() << '\n';
        return 1;
    }
}
