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
    return {"dbms_pgoutput_preview", "dbms_test_decoding"};
}

bool LogicalDecoder::format(const std::string& plugin, const LogicalChangeBatch& batch,
                            std::string& out) {
    std::ostringstream os;
    if (plugin == "dbms_test_decoding") {
        // Project-readable output; this does not claim contrib/test_decoding
        // wire or text compatibility.
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
    if (plugin == "dbms_pgoutput_preview") {
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
bool validPublicationName(const std::string& name) {
    if (name.empty() || name == "." || name == "..") return false;
    for (const unsigned char ch : name) {
        if (ch == '\0' || ch < 0x20 || ch == 0x7f || ch == '/' ||
            ch == '\\') {
            return false;
        }
    }
    const fs::path candidate(name);
    return !candidate.is_absolute() && !candidate.has_parent_path() &&
           candidate.filename().string() == name;
}

bool validatePublicationName(const std::string& name, std::string& error) {
    if (validPublicationName(name)) return true;
    error = "invalid publication name syntax (SQLSTATE 42601)";
    return false;
}

fs::path publicationPath(const std::string& dbname, const std::string& name) {
    return fs::path(dbname) / (name + ".publication");
}

bool parsePublicationFile(const std::string& name, const std::string& content,
                          Publication& pub, std::string& error) {
    error.clear();
    if (!validPublicationName(name)) {
        error = "invalid publication filename";
        return false;
    }
    Publication parsed;
    parsed.name = name;
    std::istringstream in(content);
    std::string line;
    if (!std::getline(in, line) || line.empty()) {
        error = "invalid publication header";
        return false;
    }
    std::vector<std::string> fields;
    std::istringstream header(line);
    std::string field;
    while (header >> field) fields.push_back(field);
    if (fields.size() != 5 && fields.size() != 6) {
        error = "invalid publication header";
        return false;
    }
    for (size_t index = 1; index < fields.size(); ++index) {
        if (fields[index] != "0" && fields[index] != "1") {
            error = "invalid publication flag";
            return false;
        }
    }
    parsed.owner = fields[0];
    parsed.publishInsert = fields[1] == "1";
    parsed.publishUpdate = fields[2] == "1";
    parsed.publishDelete = fields[3] == "1";
    if (fields.size() == 6) {
        parsed.publishTruncate = fields[4] == "1";
        parsed.publishAllTables = fields[5] == "1";
    } else {
        // The five-field legacy header did not have truncate.  Loading it as
        // disabled prevents an upgrade from broadening publication output.
        parsed.publishTruncate = false;
        parsed.publishAllTables = fields[4] == "1";
    }
    while (std::getline(in, line)) {
        if (line.empty() || line.find('\0') != std::string::npos ||
            line.find('\r') != std::string::npos) {
            error = "invalid publication table entry";
            return false;
        }
        parsed.tables.push_back(line);
    }
    if (in.bad()) {
        error = "cannot read publication data";
        return false;
    }
    pub = std::move(parsed);
    return true;
}

bool validatePublicationDefinition(const Publication& pub, std::string& error) {
    if (pub.owner.empty() ||
        std::any_of(pub.owner.begin(), pub.owner.end(), [](unsigned char ch) {
            return ch == '\0' || ch == ' ' || ch == '\t' || ch == '\n' ||
                   ch == '\r' || ch == '\f' || ch == '\v';
        })) {
        error = "invalid publication owner";
        return false;
    }
    for (const auto& table : pub.tables) {
        if (table.empty() || table.find('\0') != std::string::npos ||
            table.find('\n') != std::string::npos ||
            table.find('\r') != std::string::npos) {
            error = "invalid publication table entry";
            return false;
        }
    }
    return true;
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
        Publication publication;
        std::string parseError;
        if (!parsePublicationFile(filename.substr(0, filename.size() - 12),
                                  original, publication, parseError)) {
            error = "invalid publication file: " + filename;
            return false;
        }
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
    error.clear();
    if (!validatePublicationName(pub.name, error)) return false;
    if (!validatePublicationDefinition(pub, error)) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (exists(dbname, pub.name)) {
        error = "publication \"" + pub.name + "\" already exists";
        return false;
    }
    const auto path = publicationPath(dbname, pub.name);
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    if (ec) {
        error = "cannot create publication directory";
        return false;
    }
    if (!index_file::writeAtomically(path, serializePublication(pub))) {
        // writeAtomically can report a directory-fsync error after rename.
        // The statement still failed, so remove any visible target rather
        // than leave a catalog object that makes a retry look like a duplicate.
        std::error_code cleanupError;
        fs::remove(path, cleanupError);
        error = "cannot write publication file";
        return false;
    }
    return true;
}

bool PublicationCatalog::drop(const std::string& dbname, const std::string& name,
                              std::string& error) {
    error.clear();
    if (!validatePublicationName(name, error)) return false;
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
    for (const auto& name : names) {
        if (!validatePublicationName(name, error)) return false;
    }
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
    if (!validatePublicationName(pub.name, error)) return false;
    if (!validatePublicationDefinition(pub, error)) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    const auto path = publicationPath(dbname, pub.name);
    if (!fs::exists(path)) {
        error = "publication \"" + pub.name + "\" does not exist";
        return false;
    }
    std::ifstream input(path, std::ios::binary);
    std::string existing((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
    Publication parsed;
    std::string parseError;
    if (!input || input.bad() ||
        !parsePublicationFile(pub.name, existing, parsed, parseError)) {
        error = "invalid publication file: " + path.filename().string();
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
    if (!validatePublicationName(oldName, error) ||
        !validatePublicationName(newName, error)) return false;
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
    std::ifstream input(oldPath, std::ios::binary);
    std::string existing((std::istreambuf_iterator<char>(input)),
                         std::istreambuf_iterator<char>());
    Publication parsed;
    std::string parseError;
    if (!input || input.bad() ||
        !parsePublicationFile(oldName, existing, parsed, parseError)) {
        error = "invalid publication file: " + oldPath.filename().string();
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
    if (!validPublicationName(name)) return false;
    return fs::exists(publicationPath(dbname, name));
}

std::vector<Publication> PublicationCatalog::list(const std::string& dbname) const {
    std::vector<Publication> publications;
    std::string error;
    if (!list(dbname, publications, error)) publications.clear();
    return publications;
}

bool PublicationCatalog::list(const std::string& dbname,
                              std::vector<Publication>& publications,
                              std::string& error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    publications.clear();
    std::vector<Publication> pubs;
    error.clear();
    std::error_code ec;
    if (!fs::is_directory(dbname, ec)) {
        if (ec) {
            error = "cannot inspect publication directory";
            return false;
        }
        return true;
    }
    for (fs::directory_iterator it(dbname, ec), end;
         !ec && it != end; it.increment(ec)) {
        const auto& entry = *it;
        const std::string fn = entry.path().filename().string();
        if (fn.size() > 12 && fn.substr(fn.size() - 12) == ".publication") {
            std::ifstream in(entry.path(), std::ios::binary);
            std::string content((std::istreambuf_iterator<char>(in)),
                                std::istreambuf_iterator<char>());
            Publication publication;
            std::string parseError;
            if (!in || in.bad() ||
                !parsePublicationFile(fn.substr(0, fn.size() - 12), content,
                                      publication, parseError)) {
                error = "invalid publication file: " + fn;
                return false;
            }
            pubs.push_back(std::move(publication));
        }
    }
    if (ec) {
        error = "cannot inspect publication directory";
        return false;
    }
    std::sort(pubs.begin(), pubs.end(),
              [](const Publication& a, const Publication& b) { return a.name < b.name; });
    publications = std::move(pubs);
    return true;
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
    std::vector<Publication> publications;
    std::string error;
    if (!list(dbname, publications, error)) return false;
    for (const auto& pub : publications) {
        if (pub.publishAllTables) return true;
        if (std::find(pub.tables.begin(), pub.tables.end(), table) != pub.tables.end())
            return true;
    }
    return false;
}

bool PublicationCatalog::publishes(const std::string& dbname,
                                   const std::string& table,
                                   LogicalChange::Op operation) const {
    std::vector<Publication> publications;
    std::string error;
    if (!list(dbname, publications, error)) return false;
    for (const auto& pub : publications) {
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

bool LogicalChangeStore::append(const std::string& slotName,
                                const LogicalChangeBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& stream = streams_[slotName];
    if (stream.size() >= kMaxRetained) return false;
    Entry e;
    e.startLsn = batch.commitLsn;
    e.endLsn = batch.commitLsn;
    e.batch = batch;
    stream.push_back(std::move(e));
    return true;
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

void LogicalChangeStore::discard(const std::string& slotName) {
    std::lock_guard<std::mutex> lock(mutex_);
    streams_.erase(slotName);
}

size_t LogicalChangeStore::depth(const std::string& slotName) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = streams_.find(slotName);
    return it == streams_.end() ? 0 : it->second.size();
}

}  // namespace dbms
