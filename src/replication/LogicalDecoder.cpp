#include "replication/LogicalDecoder.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace dbms {

namespace fs = std::filesystem;

// ----------------------------------------------------------------------------
// LogicalDecoder — output plugins
// ----------------------------------------------------------------------------

std::vector<std::string> LogicalDecoder::availablePlugins() {
    return {"pgoutput", "test_decoding"};
}

bool LogicalDecoder::format(const std::string& plugin, const LogicalChangeBatch& batch,
                            std::string& out) {
    std::ostringstream os;
    if (plugin == "test_decoding") {
        // PG's test_decoding: one human-readable line per change.
        for (const auto& ch : batch.changes) {
            os << "table " << ch.table << ":";
            switch (ch.op) {
                case LogicalChange::Op::Insert:
                    os << " INSERT: " << ch.newRow;
                    break;
                case LogicalChange::Op::Update:
                    os << " UPDATE: old-key " << ch.oldRow << " new-tuple "
                       << ch.newRow;
                    break;
                case LogicalChange::Op::Delete:
                    os << " DELETE: old-key " << ch.oldRow;
                    break;
            }
            os << " (xid " << batch.xid << " lsn " << batch.commitLsn << ")\n";
        }
        out = os.str();
        return true;
    }
    if (plugin == "pgoutput") {
        // Compact binary-ish stream: message framing with a type byte.
        // 'B' begin(xid), 'R' relation(table), 'I'/'U'/'D' change, 'C'
        // commit(lsn).  Values are length-prefixed; rows keep the storage
        // layer's bar-separated text so consumers need no schema decode.
        auto putU64 = [&](uint64_t v) {
            for (int i = 0; i < 8; ++i) os.put(static_cast<char>((v >> (8 * i)) & 0xFF));
        };
        auto putU32 = [&](uint32_t v) {
            for (int i = 0; i < 4; ++i) os.put(static_cast<char>((v >> (8 * i)) & 0xFF));
        };
        auto putStr = [&](const std::string& s) {
            putU32(static_cast<uint32_t>(s.size()));
            os.write(s.data(), static_cast<std::streamsize>(s.size()));
        };
        os.put('B'); putU64(batch.xid);
        std::string lastTable;
        for (const auto& ch : batch.changes) {
            if (ch.table != lastTable) {
                os.put('R');
                putStr(ch.table);
                lastTable = ch.table;
            }
            switch (ch.op) {
                case LogicalChange::Op::Insert:
                    os.put('I'); putStr(ch.newRow);
                    break;
                case LogicalChange::Op::Update:
                    os.put('U'); putStr(ch.oldRow); putStr(ch.newRow);
                    break;
                case LogicalChange::Op::Delete:
                    os.put('D'); putStr(ch.oldRow);
                    break;
            }
        }
        os.put('C'); putU64(batch.commitLsn);
        out = os.str();
        return true;
    }
    return false;
}

// ----------------------------------------------------------------------------
// PublicationCatalog
// ----------------------------------------------------------------------------

PublicationCatalog& PublicationCatalog::instance() {
    static PublicationCatalog catalog;
    return catalog;
}

namespace {
fs::path publicationPath(const std::string& dbname, const std::string& name) {
    return fs::path(dbname) / (name + ".publication");
}

Publication parsePublicationFile(const std::string& name, const std::string& content) {
    Publication pub;
    pub.name = name;
    std::istringstream in(content);
    std::string line;
    bool first = true;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        if (first) {
            // header: owner pub-insert pub-update pub-delete all-tables
            std::istringstream hdr(line);
            std::string ins, upd, del, all;
            hdr >> pub.owner >> ins >> upd >> del >> all;
            pub.publishInsert = (ins == "1");
            pub.publishUpdate = (upd == "1");
            pub.publishDelete = (del == "1");
            pub.publishAllTables = (all == "1");
            first = false;
            continue;
        }
        pub.tables.push_back(line);
    }
    return pub;
}

std::string serializePublication(const Publication& pub) {
    std::ostringstream out;
    out << pub.owner << ' ' << (pub.publishInsert ? 1 : 0) << ' '
        << (pub.publishUpdate ? 1 : 0) << ' ' << (pub.publishDelete ? 1 : 0)
        << ' ' << (pub.publishAllTables ? 1 : 0) << '\n';
    for (const auto& t : pub.tables) out << t << '\n';
    return out.str();
}
}  // namespace

bool PublicationCatalog::create(const std::string& dbname, const Publication& pub,
                                std::string& error) {
    if (pub.name.empty()) {
        error = "publication name is required";
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (exists(dbname, pub.name)) {
        error = "publication \"" + pub.name + "\" already exists";
        return false;
    }
    const auto path = publicationPath(dbname, pub.name);
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        error = "cannot write publication file";
        return false;
    }
    out << serializePublication(pub);
    if (!out) {
        error = "cannot write publication file";
        return false;
    }
    return true;
}

bool PublicationCatalog::drop(const std::string& dbname, const std::string& name,
                              std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto path = publicationPath(dbname, name);
    if (!fs::exists(path)) {
        error = "publication \"" + name + "\" does not exist";
        return false;
    }
    std::error_code ec;
    if (!fs::remove(path, ec)) {
        error = "cannot remove publication file";
        return false;
    }
    return true;
}

bool PublicationCatalog::exists(const std::string& dbname, const std::string& name) const {
    return fs::exists(publicationPath(dbname, name));
}

std::vector<Publication> PublicationCatalog::list(const std::string& dbname) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Publication> pubs;
    std::error_code ec;
    if (!fs::is_directory(dbname, ec)) return pubs;
    for (const auto& entry : fs::directory_iterator(dbname, ec)) {
        const std::string fn = entry.path().filename().string();
        if (fn.size() > 12 && fn.substr(fn.size() - 12) == ".publication") {
            std::ifstream in(entry.path());
            std::string content((std::istreambuf_iterator<char>(in)),
                                std::istreambuf_iterator<char>());
            pubs.push_back(
                parsePublicationFile(fn.substr(0, fn.size() - 12), content));
        }
    }
    std::sort(pubs.begin(), pubs.end(),
              [](const Publication& a, const Publication& b) { return a.name < b.name; });
    return pubs;
}

bool PublicationCatalog::publishes(const std::string& dbname,
                                   const std::string& table) const {
    for (const auto& pub : list(dbname)) {
        if (pub.publishAllTables) return true;
        if (std::find(pub.tables.begin(), pub.tables.end(), table) != pub.tables.end())
            return true;
    }
    return false;
}

// ----------------------------------------------------------------------------
// LogicalChangeStore
// ----------------------------------------------------------------------------

LogicalChangeStore& LogicalChangeStore::instance() {
    static LogicalChangeStore store;
    return store;
}

void LogicalChangeStore::append(const std::string& slotName,
                                const LogicalChangeBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& stream = streams_[slotName];
    Entry e;
    e.startLsn = batch.commitLsn;
    e.endLsn = batch.commitLsn;
    e.batch = batch;
    stream.push_back(std::move(e));
    while (stream.size() > kMaxRetained) stream.pop_front();
}

LogicalChangeStore::PeekResult LogicalChangeStore::peek(const std::string& slotName,
                                                        uint64_t fromLsn,
                                                        size_t maxChanges) const {
    std::lock_guard<std::mutex> lock(mutex_);
    PeekResult result;
    auto it = streams_.find(slotName);
    if (it == streams_.end() || it->second.empty()) {
        result.nextLsn = fromLsn;
        return result;
    }
    size_t seen = 0;
    for (const auto& e : it->second) {
        if (e.startLsn <= fromLsn) continue;  // already consumed
        if (seen >= maxChanges) {
            result.hitEnd = false;
            break;
        }
        result.batches.push_back(e.batch);
        result.nextLsn = e.endLsn;
        seen += e.batch.changes.size();
    }
    if (result.batches.empty()) result.nextLsn = fromLsn;
    return result;
}

void LogicalChangeStore::acknowledge(const std::string& slotName, uint64_t confirmedLsn) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = streams_.find(slotName);
    if (it == streams_.end()) return;
    auto& stream = it->second;
    while (!stream.empty() && stream.front().startLsn <= confirmedLsn) {
        stream.pop_front();
    }
    if (stream.empty()) streams_.erase(it);
}

size_t LogicalChangeStore::depth(const std::string& slotName) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = streams_.find(slotName);
    return it == streams_.end() ? 0 : it->second.size();
}

}  // namespace dbms
