// test_link_flags: -Wl,--wrap=mkstemp
#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "Config.h"
#include "Session.h"

#include <algorithm>
#include <cassert>
#include <climits>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <unistd.h>

extern dbms::Config g_config;

namespace {
constexpr size_t rowCount = 512, payloadBytes = 2048;
constexpr int64_t keyBase = 9007199254740993LL;

bool sortFile(const std::filesystem::path& path) {
    std::error_code error;
    const auto name = std::filesystem::read_symlink(path, error).string();
    return !error && name.find("/.dbms-sort-") != std::string::npos;
}
size_t descriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd"))
        count += sortFile(entry.path());
    return count;
}
bool noPaths() {
    for (const auto& entry : std::filesystem::directory_iterator("."))
        if (entry.path().filename().string().rfind(".dbms-sort-", 0) == 0) return false;
    return true;
}
bool anonymousDescriptors() {
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        if (!sortFile(entry.path())) continue;
        std::error_code error;
        const auto name = std::filesystem::read_symlink(entry.path(), error).string();
        if (error || name.find("(deleted)") == std::string::npos) return false;
    }
    return noPaths();
}
bool sameBinding(const dbms::StorageEngine::NullRowBindingState& left,
                 const dbms::StorageEngine::NullRowBindingState& right) {
    return left.engine == right.engine && left.database == right.database &&
        left.table == right.table && left.rid == right.rid && left.natts == right.natts;
}

struct Observation {
    size_t base = 0, peak = 0, creates = 0, emitted = 0, closes = 0;
    bool anonymous = true;
    std::function<void()> change;
    std::exception_ptr failure;
};
Observation* probe = nullptr;

std::string rawRow(size_t id) {
    const int64_t key = keyBase + static_cast<int64_t>(id);
    const int32_t identity = static_cast<int32_t>(id);
    std::string raw(sizeof(key) + sizeof(identity) + payloadBytes, 'a' + id % 23);
    std::memcpy(raw.data(), &key, sizeof(key));
    std::memcpy(raw.data() + sizeof(key), &identity, sizeof(identity));
    raw.replace(sizeof(key) + sizeof(identity), 8, std::string("\0\nNULL\xce\xb2", 8));
    return raw;
}
std::vector<bool> bitmap(size_t id) { return {id % 17 == 0, false, id % 13 == 0}; }

class Source final : public dbms::Operator {
    Observation& events_;
    size_t position_ = 0;
public:
    explicit Source(Observation& events) : events_(events) {}
    bool open() override { position_ = 0; events_.emitted = 0; clearError(); return true; }
    bool next(std::string& raw) override {
        events_.peak = std::max(events_.peak, descriptors());
        events_.anonymous = events_.anonymous && anonymousDescriptors();
        if (position_ == 1 && events_.change) events_.change();
        if (position_ == 128 && events_.failure) std::rethrow_exception(events_.failure);
        if (position_ == rowCount) return false;
        raw = rawRow(rowCount - 1 - position_++);
        ++events_.emitted;
        return true;
    }
    void close() override { ++events_.closes; }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override {
        if (!position_) return false;
        nulls = bitmap(rowCount - position_); return true;
    }
    bool lastColumnIsNull(size_t column) const override {
        return position_ && column < 3 && bitmap(rowCount - position_)[column];
    }
    // This source has no heap owner/RID. Leave its provenance genuinely empty.
};

dbms::TableSchema schema() {
    dbms::TableSchema table; table.tablename = "work_mem_sort_runtime";
    table.append(dbms::makeIntColumn("k", true, 4));
    table.append(dbms::makeIntColumn("id", false, 2));
    table.append(dbms::makeStringColumn("payload", true, payloadBytes));
    return table;
}

struct Sort {
    Observation events;
    dbms::SortOp op;
    Sort() : op(std::make_unique<Source>(events), schema(), "k", true) {}
    bool open(bool spill) {
        events.base = descriptors(); events.peak = events.base; events.creates = 0;
        probe = &events;
        bool opened = false;
        try { opened = op.open(); }
        catch (...) { probe = nullptr; throw; }
        probe = nullptr;
        return opened && events.emitted == rowCount && events.anonymous &&
            events.peak <= events.base + 2 && descriptors() == events.base + (spill ? 1U : 0U) &&
            (spill ? events.creates > 0 : events.creates == 0) && anonymousDescriptors();
    }
    bool consume() {
        std::vector<bool> seen(rowCount, false);
        bool good = true, havePrevious = false, previousNull = false;
        int64_t previous = 0; size_t count = 0; std::string raw;
        while (op.next(raw)) {
            int64_t key = 0; int32_t identity = -1;
            if (raw.size() != sizeof(key) + sizeof(identity) + payloadBytes) { good = false; break; }
            std::memcpy(&key, raw.data(), sizeof(key));
            std::memcpy(&identity, raw.data() + sizeof(key), sizeof(identity));
            if (identity < 0 || size_t(identity) >= rowCount) { good = false; break; }
            const size_t id = static_cast<size_t>(identity);
            const auto nulls = bitmap(id);
            std::vector<bool> actualNulls;
            const auto origin = op.scanOrigin();
            good = good && !seen[id] && raw == rawRow(id) && key == keyBase + int64_t(id) &&
                op.lastStoredNullBitmap(actualNulls) && actualNulls == nulls &&
                origin.engine == nullptr && origin.dbname.empty() && origin.tablename.empty() && origin.rid == 0;
            for (size_t column = 0; column < 3; ++column)
                good = good && op.lastColumnIsNull(column) == nulls[column];
            if (havePrevious) {
                if (previousNull != nulls[0]) good = good && !previousNull;
                else if (!nulls[0]) good = good && previous < key;
            }
            previous = key; previousNull = nulls[0]; havePrevious = true;
            seen[id] = true; ++count;
        }
        return good && count == rowCount && !op.hasError() &&
            std::all_of(seen.begin(), seen.end(), [](bool value) { return value; });
    }
    bool close(size_t remaining = 0) {
        op.close(); op.close();
        return descriptors() == remaining && anonymousDescriptors();
    }
};

bool run(Session* session, size_t instanceDefault, bool spill) {
    dbms::setCurrentSession(session); g_config.workMemKb = instanceDefault;
    const auto caller = dbms::StorageEngine::captureNullRowBinding();
    Sort sort;
    bool good = sort.open(spill);
    good = sort.consume() && good;
    good = sort.close() && good;
    return good && sort.events.closes == 1 &&
        sameBinding(dbms::StorageEngine::captureNullRowBinding(), caller);
}

void configFile(const std::string& name, const std::string& contents) {
    std::ofstream output(name); output << contents; output.close(); assert(output);
}
} // namespace

extern "C" int __real_mkstemp(char*);
extern "C" int __wrap_mkstemp(char* name) {
    const int descriptor = __real_mkstemp(name);
    if (probe && std::strncmp(name, ".dbms-sort-", 11) == 0) {
        ++probe->creates;
        probe->peak = std::max(probe->peak, descriptors());
    }
    return descriptor;
}

int main() {
    using namespace dbms;
    auto* previousSession = currentSession();
    const auto previousBudget = g_config.workMemKb;
    auto previousBinding = StorageEngine::captureNullRowBinding();
    StorageEngine::unbindNullRow();
    size_t checked = 0, failures = 0;
    const auto check = [&](const char* label, bool good) {
        ++checked; failures += !good;
        std::cout << "WORK_MEM_SORT_RUNTIME " << label << " passed=" << good << '\n';
    };

    {
        Config config;
        bool good = true;
        for (const auto& value : std::vector<std::pair<std::string, size_t>>{
                 {"64kB", 64}, {"1MB", 1024}, {"1.5MB", 1536}, {"0x100", 256},
                 {"2147483647kB", size_t(INT_MAX)}})
            good = config.setParameter("work_mem", value.first) && config.workMemKb == value.second && good;
        check("Config work_mem units and upper boundary", good);
        config.workMemKb = 1024; config.maxConnections = 37;
        good = true;
        for (const std::string value : {"", "63kB", "2147483648kB", "1mb", "-64kB", "NaN", "1MBjunk"})
            good = !config.setParameter("work_mem", value) && config.workMemKb == 1024 &&
                config.maxConnections == 37 && good;
        check("invalid Config assignments preserve previous settings", good);
        configFile("work_mem_runtime_valid.conf", "max_connections=41\nwork_mem='1.5MB'\n");
        check("Config file accepts memory units", config.load("work_mem_runtime_valid.conf") &&
            config.workMemKb == 1536 && config.maxConnections == 41);
        configFile("work_mem_runtime_invalid.conf", "max_connections=43\nwork_mem='63kB'\n");
        check("invalid Config file rolls back preceding assignments", !config.load("work_mem_runtime_invalid.conf") &&
            config.workMemKb == 1536 && config.maxConnections == 41);
        check("legacy instance raw KiB permits existing low-budget controls",
            config.setParameter("work_mem_kb", "1") && config.workMemKb == 1 && config.validate());
        std::filesystem::remove("work_mem_runtime_valid.conf");
        std::filesystem::remove("work_mem_runtime_invalid.conf");
    }

    check("no Session uses large instance budget", run(nullptr, 4096, false));
    check("no Session uses small instance budget", run(nullptr, 64, true));
    {
        Session session; session.workMem.assign(64, false);
        check("small session override spills under large instance default", run(&session, 4096, true));
        session.workMem.assign(4096, false);
        check("large session override stays in memory under small default", run(&session, 64, false));
    }
    {
        Session session; session.workMem.setStartup(64);
        check("startup work_mem drives actual spill", run(&session, 4096, true));
        session.workMem.assign(4096, false);
        bool good = run(&session, 64, false);
        session.workMem.assign(std::nullopt, false);
        good = run(&session, 4096, true) && good;
        check("RESET restores startup budget after explicit override", good);
    }
    {
        Session session; session.workMem.assign(4096, false);
        session.workMem.assign(std::nullopt, false);
        bool good = run(&session, 64, true);
        good = run(&session, 4096, false) && good;
        check("RESET with no startup option follows reloaded default", good);
    }
    {
        Session session; session.workMem.assign(4096, false);
        session.workMem.begin(); session.workMem.assign(64, true);
        check("SET LOCAL low budget affects current transaction sort", run(&session, 4096, true));
        session.workMem.finish(true);
        check("LOCAL commit restores persistent session budget", run(&session, 64, false));
        session.workMem.begin(); session.workMem.assign(64, true); session.workMem.finish(false);
        check("LOCAL rollback restores persistent session budget", run(&session, 64, false));
        session.workMem.begin(); session.workMem.assign(64, false); session.workMem.finish(false);
        check("transactional SET rollback restores prior budget", run(&session, 64, false));
        session.workMem.begin(); session.workMem.assign(64, false); session.workMem.finish(true);
        check("transactional SET commit retains new budget", run(&session, 4096, true));
    }
    {
        Session session; session.workMem.assign(4096, false); session.workMem.begin();
        session.workMem.save("sp"); session.workMem.assign(64, false); session.workMem.rollback("sp");
        check("savepoint rollback restores actual sort budget", run(&session, 64, false));
        session.workMem.assign(64, false); session.workMem.release("sp"); session.workMem.finish(true);
        check("savepoint release and commit retain actual sort budget", run(&session, 4096, true));
    }
    {
        Session low, high; low.workMem.assign(64, false); high.workMem.assign(4096, false);
        g_config.workMemKb = 4096; setCurrentSession(&low); Sort first;
        bool good = first.open(true);
        setCurrentSession(&high); Sort second; good = second.open(false) && good;
        // Consume under the other current Session to prove that open snapshots
        // belong to each sort, rather than re-reading mutable connection state.
        good = first.consume() && good; good = first.close() && good;
        setCurrentSession(&low); good = second.consume() && good; good = second.close() && good;
        check("interleaved small and large session sorts remain independent", good &&
            first.events.closes == 1 && second.events.closes == 1);
    }
    {
        Session firstSession, secondSession;
        firstSession.workMem.assign(64, false); secondSession.workMem.assign(64, false);
        g_config.workMemKb = 4096; setCurrentSession(&firstSession); Sort first;
        bool good = first.open(true); setCurrentSession(&secondSession); Sort second;
        good = second.open(true) && descriptors() == 2 && good;
        good = first.consume() && good; good = first.close(1) && good;
        good = second.consume() && good; good = second.close() && good;
        check("two spilled sessions own separate bounded anonymous files", good &&
            first.events.closes == 1 && second.events.closes == 1);
    }
    for (bool startsSmall : {true, false}) {
        Session session; session.workMem.assign(startsSmall ? 64 : 4096, false);
        setCurrentSession(&session); g_config.workMemKb = 4096; Sort sort;
        sort.events.change = [&] { session.workMem.assign(startsSmall ? 4096 : 64, false); };
        bool good = sort.open(startsSmall); good = sort.consume() && good; good = sort.close() && good;
        good = sort.open(!startsSmall) && good; good = sort.consume() && good; good = sort.close() && good;
        check(startsSmall ? "input change low to high affects only next open" :
              "input change high to low affects only next open", good && sort.events.closes == 2);
    }
    {
        Session session; session.workMem.assign(64, false); setCurrentSession(&session);
        g_config.workMemKb = 4096; Sort sort; std::string raw;
        std::vector<bool> firstNulls;
        bool good = sort.open(true) && sort.op.next(raw) && raw == rawRow(1) &&
            sort.op.lastStoredNullBitmap(firstNulls) && firstNulls == bitmap(1);
        good = sort.close() && good; session.workMem.assign(4096, false);
        good = sort.open(false) && good; good = sort.consume() && good; good = sort.close() && good;
        check("early close and reopen adopt changed session budget", good && sort.events.closes == 2);
    }
    {
        Session session; session.workMem.assign(64, false); setCurrentSession(&session);
        g_config.workMemKb = 4096; Sort sort;
        sort.events.failure = std::make_exception_ptr(DbError("22012", "work_mem input failure"));
        const auto caller = StorageEngine::captureNullRowBinding();
        bool exact = false, identity = false;
        try { (void)sort.open(true); }
        catch (const DbError& error) {
            exact = error.sqlState() == "22012" && error.message() == "work_mem input failure";
            identity = std::current_exception() == sort.events.failure;
        }
        std::string unused;
        bool good = exact && identity && descriptors() == 0 && noPaths() && !sort.op.next(unused) &&
            sameBinding(StorageEngine::captureNullRowBinding(), caller) && sort.events.creates > 0 &&
            sort.events.closes == 1 && sort.events.peak <= 2;
        good = sort.close() && good;
        sort.events.failure = {};
        good = sort.open(true) && good; good = sort.consume() && good; good = sort.close() && good;
        check("input exception preserves primary, releases files and permits reopen",
              good && sort.events.closes == 2 &&
              sameBinding(StorageEngine::captureNullRowBinding(), caller));
    }
    {
        setCurrentSession(nullptr); g_config.workMemKb = 64; Sort sort;
        sort.events.change = [&] { g_config.workMemKb = 4096; };
        bool good = sort.open(true); good = sort.consume() && good; good = sort.close() && good;
        good = sort.open(false) && good; good = sort.consume() && good; good = sort.close() && good;
        check("instance reload during input affects only next no-Session open", good && sort.events.closes == 2);
    }
    {
        Session low, high; low.workMem.assign(64, false); high.workMem.assign(4096, false);
        setCurrentSession(&low); g_config.workMemKb = 4096; Sort sort;
        sort.events.change = [&] { setCurrentSession(&high); };
        bool good = sort.open(true); good = sort.consume() && good; good = sort.close() && good;
        good = sort.open(false) && good; good = sort.consume() && good; good = sort.close() && good;
        check("changing current Session during input affects only next open", good && sort.events.closes == 2);
    }

    setCurrentSession(previousSession); g_config.workMemKb = previousBudget;
    StorageEngine::restoreNullRowBinding(std::move(previousBinding));
    assert(checked == 27);
    std::cout << "WORK_MEM_SORT_RUNTIME_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
