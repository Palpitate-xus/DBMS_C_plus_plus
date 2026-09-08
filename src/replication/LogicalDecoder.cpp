#include "replication/LogicalDecoder.h"
#include "access/IndexFileUtil.h"

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
                case LogicalChange::Op::Truncate:
                    os << " TRUNCATE";
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
                case LogicalChange::Op::Truncate:
                    os.put('T');
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
            // Current header: owner insert update delete truncate all-tables.
            // The five-field legacy header did not have truncate; load it as
            // disabled so an upgrade never starts publishing new events.
            std::istringstream hdr(line);
            std::string ins, upd, del, fourth, fifth;
            hdr >> pub.owner >> ins >> upd >> del >> fourth;
            pub.publishInsert = (ins == "1");
            pub.publishUpdate = (upd == "1");
            pub.publishDelete = (del == "1");
            if (hdr >> fifth) {
                pub.publishTruncate = (fourth == "1");
                pub.publishAllTables = (fifth == "1");
            } else {
                pub.publishTruncate = false;
                pub.publishAllTables = (fourth == "1");
            }
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
        << ' ' << (pub.publishTruncate ? 1 : 0)
        << ' ' << (pub.publishAllTables ? 1 : 0) << '\n';
    for (const auto& t : pub.tables) out << t << '\n';
    return out.str();
}

template <typename Transform>
bool rewritePublicationFiles(const std::string& dbname,
                             Transform&& transform,
                             std::string& error) {
    struct PublicationRewrite {
        fs::path path;
        std::string original;
        std::string updated;
    };
    std::vector<PublicationRewrite> rewrites;
    std::error_code filesystemError;
    if (!fs::is_directory(dbname, filesystemError)) {
        if (!filesystemError) return true;
        error = "cannot inspect publication directory";
        return false;
    }
    for (fs::directory_iterator it(dbname, filesystemError), end;
         !filesystemError && it != end; it.increment(filesystemError)) {
        const std::string filename = it->path().filename().string();
        if (filename.size() <= 12 ||
            filename.substr(filename.size() - 12) != ".publication") {
            continue;
        }
        std::ifstream input(it->path(), std::ios::binary);
        if (!input) {
            error = "cannot read publication file";
            return false;
        }
        std::string original((std::istreambuf_iterator<char>(input)),
                             std::istreambuf_iterator<char>());
        if (input.bad()) {
            error = "cannot read publication file";
            return false;
        }
        Publication publication = parsePublicationFile(
            filename.substr(0, filename.size() - 12), original);
        if (!transform(publication)) continue;
        rewrites.push_back(
            {it->path(), std::move(original), serializePublication(publication)});
    }
    if (filesystemError) {
        error = "cannot inspect publication directory";
        return false;
    }

    for (size_t rewriteIndex = 0;
         rewriteIndex < rewrites.size(); ++rewriteIndex) {
        const auto& rewrite = rewrites[rewriteIndex];
        if (index_file::writeAtomically(rewrite.path, rewrite.updated)) continue;
        // A directory fsync can fail after the target was replaced; include
        // the current file in the restoration set.
        for (size_t rollback = 0; rollback <= rewriteIndex; ++rollback) {
            index_file::writeAtomically(
                rewrites[rollback].path, rewrites[rollback].original);
        }
        error = "cannot persist publication membership";
        return false;
    }
    return true;
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

bool PublicationCatalog::dropMany(const std::string& dbname,
                                  const std::vector<std::string>& names,
                                  bool ifExists,
                                  std::vector<std::string>& missing,
                                  std::string& error) {
    missing.clear();
    error.clear();
    struct SavedPublication {
        fs::path path;
        std::string bytes;
    };
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<SavedPublication> existing;
    existing.reserve(names.size());
    std::vector<std::string> inspected;
    for (const auto& name : names) {
        if (std::find(inspected.begin(), inspected.end(), name) !=
            inspected.end()) {
            continue;
        }
        inspected.push_back(name);
        const auto path = publicationPath(dbname, name);
        if (!fs::exists(path)) {
            missing.push_back(name);
            continue;
        }
        std::ifstream input(path, std::ios::binary);
        if (!input) {
            error = "cannot read publication \"" + name + "\"";
            return false;
        }
        std::string bytes((std::istreambuf_iterator<char>(input)),
                          std::istreambuf_iterator<char>());
        if (input.bad()) {
            error = "cannot read publication \"" + name + "\"";
            return false;
        }
        existing.push_back({path, std::move(bytes)});
    }
    if (!ifExists && !missing.empty()) {
        error = "publication \"" + missing.front() + "\" does not exist";
        return false;
    }
    for (size_t index = 0; index < existing.size(); ++index) {
        std::error_code filesystemError;
        if (fs::remove(existing[index].path, filesystemError) &&
            !filesystemError) {
            continue;
        }
        bool restored = true;
        for (size_t rollback = 0; rollback <= index; ++rollback) {
            if (fs::exists(existing[rollback].path)) continue;
            restored = index_file::writeAtomically(
                existing[rollback].path, existing[rollback].bytes) && restored;
        }
        error = restored ? "cannot remove publication file"
                         : "cannot remove publication file; rollback failed";
        return false;
    }
    return true;
}

bool PublicationCatalog::update(const std::string& dbname,
                                const Publication& pub,
                                std::string& error) {
    error.clear();
    if (pub.name.empty()) {
        error = "publication name is required";
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto path = publicationPath(dbname, pub.name);
    if (!fs::exists(path)) {
        error = "publication \"" + pub.name + "\" does not exist";
        return false;
    }
    if (!index_file::writeAtomically(path, serializePublication(pub))) {
        error = "cannot persist publication";
        return false;
    }
    return true;
}

bool PublicationCatalog::rename(const std::string& dbname,
                                const std::string& oldName,
                                const std::string& newName,
                                std::string& error) {
    error.clear();
    if (oldName.empty() || newName.empty()) {
        error = "publication name is required";
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto oldPath = publicationPath(dbname, oldName);
    const auto newPath = publicationPath(dbname, newName);
    if (!fs::exists(oldPath)) {
        error = "publication \"" + oldName + "\" does not exist";
        return false;
    }
    if (fs::exists(newPath)) {
        error = "publication \"" + newName + "\" already exists";
        return false;
    }
    std::error_code filesystemError;
    fs::rename(oldPath, newPath, filesystemError);
    if (filesystemError) {
        error = "cannot rename publication";
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

bool PublicationCatalog::renameTable(const std::string& dbname,
                                     const std::string& oldName,
                                     const std::string& newName,
                                     std::string& error) {
    error.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    return rewritePublicationFiles(
        dbname,
        [&](Publication& publication) {
            bool changed = false;
            for (auto& table : publication.tables) {
                if (table != oldName) continue;
                table = newName;
                changed = true;
            }
            if (!changed) return false;
            std::vector<std::string> uniqueTables;
            for (const auto& table : publication.tables) {
                if (std::find(uniqueTables.begin(), uniqueTables.end(), table) ==
                    uniqueTables.end()) {
                    uniqueTables.push_back(table);
                }
            }
            publication.tables = std::move(uniqueTables);
            return true;
        },
        error);
}

bool PublicationCatalog::removeTable(const std::string& dbname,
                                     const std::string& tableName,
                                     std::string& error) {
    error.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    return rewritePublicationFiles(
        dbname,
        [&](Publication& publication) {
            const auto newEnd = std::remove(
                publication.tables.begin(), publication.tables.end(), tableName);
            if (newEnd == publication.tables.end()) return false;
            publication.tables.erase(newEnd, publication.tables.end());
            return true;
        },
        error);
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

bool PublicationCatalog::publishes(const std::string& dbname,
                                   const std::string& table,
                                   LogicalChange::Op operation) const {
    for (const auto& pub : list(dbname)) {
        const bool member = pub.publishAllTables ||
            std::find(pub.tables.begin(), pub.tables.end(), table) !=
                pub.tables.end();
        if (!member) continue;
        if (operation == LogicalChange::Op::Insert && pub.publishInsert)
            return true;
        if (operation == LogicalChange::Op::Update && pub.publishUpdate)
            return true;
        if (operation == LogicalChange::Op::Delete && pub.publishDelete)
            return true;
        if (operation == LogicalChange::Op::Truncate && pub.publishTruncate)
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
