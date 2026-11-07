#pragma once

#include "access/IndexFileUtil.h"
#include "common/sha256.h"
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <string>
#include <vector>

namespace dbms::vacuum_full_backup {

// The pending record is durable before the first live file is replaced. It
// names only flat, owned backup files; live paths are resolved by StorageEngine.
constexpr const char* pendingSuffix = ".vacuum_full_pending";
constexpr size_t maxManifestBytes = 4 * 1024 * 1024;
enum class Role : uint32_t { Physical = 0, Schema = 1, SpecializedDirty = 2 };
struct File {
    Role role = Role::Physical;
    std::string name;
    bool present = false;
    uint32_t mode = 0;
    uint64_t size = 0;
    std::string digest;
};
struct State {
    std::string database, table, backupLeaf;
    uint64_t xid = 0, oldIdentity = 0, newIdentity = 0;
    bool undone = false;
    std::vector<File> files;
};
inline bool leaf(const std::string& value) {
    return !value.empty() && value != "." && value != ".." &&
        value.size() <= 255 && value.find('/') == std::string::npos &&
        value.find('\0') == std::string::npos;
}
inline bool validState(const State& s) {
    const std::string prefix = "." + s.table + ".vacuum_full_backup.";
    if (!leaf(s.database) || !leaf(s.table) || !leaf(s.backupLeaf) ||
        s.backupLeaf.rfind(prefix, 0) != 0 || s.backupLeaf.size() == prefix.size() ||
        s.backupLeaf.find_first_not_of("0123456789", prefix.size()) != std::string::npos ||
        s.xid == 0 || s.newIdentity == 0 || s.newIdentity == s.oldIdentity ||
        s.newIdentity == std::numeric_limits<uint64_t>::max() ||
        s.files.empty() || s.files.size() > 16384) return false;
    size_t schemas = 0, dirty = 0;
    std::set<std::pair<uint32_t, std::string>> names;
    for (const auto& f : s.files) {
        if (static_cast<uint32_t>(f.role) > 2 || f.mode > 07777 ||
            !names.emplace(static_cast<uint32_t>(f.role), f.name).second) return false;
        if (f.role == Role::Physical) { if (!leaf(f.name)) return false; }
        else if (!f.name.empty()) return false;
        if (f.role == Role::Schema) { ++schemas; if (!f.present) return false; }
        if (f.role == Role::SpecializedDirty) ++dirty;
        if (f.present) {
            if (f.digest.size() != 64 ||
                f.digest.find_first_not_of("0123456789abcdef") != std::string::npos) return false;
        } else if (f.size != 0 || f.mode != 0 || !f.digest.empty()) return false;
    }
    return schemas == 1 && dirty <= 1;
}
template<class T> inline void append(std::string& bytes, T value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}
inline void appendString(std::string& bytes, const std::string& value) {
    append(bytes, static_cast<uint32_t>(value.size())); bytes += value;
}
inline std::string encode(const State& s) {
    if (!validState(s)) return {};
    std::string b;
    append(b, uint32_t{0x31424656}); // VFB1
    append(b, uint32_t{1});
    appendString(b, s.database); appendString(b, s.table); appendString(b, s.backupLeaf);
    append(b, s.xid); append(b, s.oldIdentity); append(b, s.newIdentity);
    append(b, uint32_t(s.undone));
    append(b, static_cast<uint32_t>(s.files.size()));
    for (const auto& f : s.files) {
        append(b, static_cast<uint32_t>(f.role)); appendString(b, f.name);
        append(b, uint32_t(f.present)); append(b, f.mode); append(b, f.size);
        appendString(b, f.digest);
    }
    if (b.size() > maxManifestBytes - 64) return {};
    return b + SHA256::hash(b);
}
inline bool decode(const std::string& bytes, State& result) {
    result = {};
    if (bytes.size() < 64 || bytes.size() > maxManifestBytes) return false;
    const size_t end = bytes.size() - 64;
    if (SHA256::hash(bytes.substr(0, end)) != bytes.substr(end)) return false;
    size_t offset = 0;
    const auto read = [&](auto& value) {
        if (offset > end || sizeof(value) > end - offset) return false;
        std::memcpy(&value, bytes.data() + offset, sizeof(value));
        offset += sizeof(value); return true;
    };
    const auto text = [&](std::string& value) {
        uint32_t n = 0;
        if (!read(n) || n > 4096 || offset > end || n > end - offset) return false;
        value.assign(bytes.data() + offset, n); offset += n; return true;
    };
    uint32_t magic = 0, version = 0, count = 0, undone = 0;
    if (!read(magic) || !read(version) || magic != 0x31424656 || version != 1 ||
        !text(result.database) || !text(result.table) || !text(result.backupLeaf) ||
        !read(result.xid) || !read(result.oldIdentity) || !read(result.newIdentity) ||
        !read(undone) || undone > 1 ||
        !read(count) || count == 0 || count > 16384) return false;
    result.undone = undone != 0;
    result.files.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        File f; uint32_t role = 0, present = 0;
        if (!read(role) || !text(f.name) || !read(present) || present > 1 ||
            !read(f.mode) || !read(f.size) || !text(f.digest)) return false;
        f.role = static_cast<Role>(role); f.present = present != 0;
        result.files.push_back(std::move(f));
    }
    return offset == end && validState(result);
}
inline bool regularExists(const std::filesystem::path& p, bool& exists) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(p, error);
    if (error && error != std::errc::no_such_file_or_directory) return false;
    exists = std::filesystem::exists(status);
    return !exists || std::filesystem::is_regular_file(status);
}
inline bool read(const std::filesystem::path& p, std::string& bytes,
                 size_t maximum = maxManifestBytes) {
    bool exists = false;
    if (!regularExists(p, exists) || !exists) return false;
    std::error_code error;
    if (std::filesystem::file_size(p, error) > maximum || error) return false;
    std::ifstream input(p, std::ios::binary);
    if (!input) return false;
    bytes.assign(std::istreambuf_iterator<char>(input), {});
    return !input.bad() && bytes.size() <= maximum;
}
inline bool syncFile(const std::filesystem::path& p) {
    const int fd = ::open(p.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return false;
    struct stat status {};
    bool ok = ::fstat(fd, &status) == 0 && S_ISREG(status.st_mode);
    if (ok) {
        int rc;
        do { rc = ::fsync(fd); } while (rc != 0 && errno == EINTR);
        ok = rc == 0;
    }
    const bool closed = ::close(fd) == 0;
    return ok && closed;
}
inline bool digestFile(const std::filesystem::path& p, File& file) {
    bool exists = false;
    if (!regularExists(p, exists)) return false;
    file.present = exists; file.mode = 0; file.size = 0; file.digest.clear();
    if (!exists) return true;
    std::error_code error;
    file.size = std::filesystem::file_size(p, error);
    if (error) return false;
    file.mode = static_cast<uint32_t>(std::filesystem::status(p, error).permissions()) & 07777;
    if (error) return false;
    std::ifstream input(p, std::ios::binary);
    if (!input) return false;
    SHA256 digest; std::array<char, 64 * 1024> buffer{};
    while (input) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        if (input.gcount() > 0) digest.append(buffer.data(), static_cast<size_t>(input.gcount()));
    }
    if (!input.eof()) return false;
    file.digest = digest.finishHex(); return true;
}
inline bool publish(const std::filesystem::path& p, const std::string& bytes,
                    bool noReplace = false) {
    const bool ok = noReplace ?
        index_file::writeAtomicallyNoReplace(p, bytes) == index_file::WriteResult::OK :
        index_file::writeAtomically(p, bytes);
    if (ok) return true;
    // A directory sync may fail after the rename/link. Confirm only this
    // exact generation and retry its durability barrier, never another file.
    std::string current;
    return read(p, current) && current == bytes &&
        index_file::syncDirectory(p.parent_path());
}
inline std::filesystem::path target(const File& f,
        const std::filesystem::path& schema, const std::filesystem::path& root,
        const std::filesystem::path& dirty) {
    if (f.role == Role::Schema) return schema;
    if (f.role == Role::SpecializedDirty) return dirty;
    return root / f.name;
}
inline std::filesystem::path backupFile(const std::filesystem::path& backup, size_t i) {
    return backup / (std::to_string(i) + ".bak");
}
inline std::string undoneToken(const std::string& bytes) {
    return "DBMS_VACUUM_FULL_UNDONE_V1\n" + SHA256::hash(bytes) + "\n";
}
inline bool isUndone(const std::filesystem::path& backup, const std::string& bytes,
                     bool& undone) {
    bool exists = false; undone = false;
    const auto p = backup / "undo_complete";
    if (!regularExists(p, exists)) return false;
    if (!exists) return true;
    std::string token;
    if (!read(p, token, 256) || token != undoneToken(bytes)) return false;
    undone = true; return true;
}
inline bool restore(const State& state, const std::string& bytes,
        const std::filesystem::path& backup, const std::filesystem::path& schema,
        const std::filesystem::path& root, const std::filesystem::path& dirty) {
    bool undone = false;
    if (!isUndone(backup, bytes, undone)) return false;
    if (undone) return index_file::syncDirectory(backup);
    std::string manifest;
    if (!read(backup / "manifest", manifest) || manifest != bytes) return false;
    // Verify every original image before touching any live path.
    for (size_t i = 0; i < state.files.size(); ++i) {
        const auto& expected = state.files[i];
        if (!expected.present) continue;
        File actual;
        if (!digestFile(backupFile(backup, i), actual) || !actual.present ||
            actual.size != expected.size || actual.digest != expected.digest ||
            actual.mode != expected.mode) return false;
    }
    std::set<std::filesystem::path> parents;
    const auto restoreFile = [&](size_t i) {
        const auto& f = state.files[i]; const auto p = target(f, schema, root, dirty);
        bool exists = false;
        if (!regularExists(p, exists)) return false;
        std::error_code error;
        parents.insert(p.parent_path());
        if (!f.present) {
            return !exists || (std::filesystem::remove(p, error) && !error);
        }
        // Keep a complete schema path even if a second crash interrupts undo.
        if (f.role == Role::Schema) {
            std::string original;
            if (!read(backupFile(backup, i), original, std::numeric_limits<size_t>::max()) ||
                !publish(p, original)) return false;
        } else {
            const auto temporary = std::filesystem::path(
                p.string() + ".vacuum_full_restore." + std::to_string(state.xid));
            bool temporaryExists = false;
            if (!regularExists(temporary, temporaryExists)) return false;
            if (temporaryExists && (!std::filesystem::remove(temporary, error) || error)) return false;
            if (!std::filesystem::copy_file(backupFile(backup, i), temporary,
                    std::filesystem::copy_options::none, error) || error || !syncFile(temporary))
                return false;
            std::filesystem::rename(temporary, p, error);
            if (error) return false;
        }
        std::filesystem::permissions(p, static_cast<std::filesystem::perms>(f.mode), error);
        return !error && syncFile(p);
    };
    for (size_t i = 0; i < state.files.size(); ++i)
        if (state.files[i].role != Role::Schema && !restoreFile(i)) return false;
    for (size_t i = 0; i < state.files.size(); ++i)
        if (state.files[i].role == Role::Schema && !restoreFile(i)) return false;
    for (const auto& parent : parents) if (!index_file::syncDirectory(parent)) return false;
    // This receipt prevents a stale aborted attempt from erasing later DML
    // if cleanup itself is interrupted or has to be retried.
    return publish(backup / "undo_complete", undoneToken(bytes));
}
inline bool discard(const std::filesystem::path& pending,
        const std::filesystem::path& backup, const std::string& bytes) {
    State state;
    if (!decode(bytes, state)) return false;
    state.undone = false;
    std::string manifest;
    const bool ownBackup = read(backup / "manifest", manifest) && manifest == encode(state);
    bool exists = false;
    if (!regularExists(pending, exists)) return false;
    if (exists) {
        std::string current;
        if (!read(pending, current) || current != bytes ||
            !index_file::removeDurably(pending)) return false;
    } else if (!index_file::syncDirectory(pending.parent_path())) return false;
    // The pending name is durably absent before any backup bytes disappear.
    // A later table move/name reuse may resolve another root. Leave any
    // unmatched directory intact rather than deleting another attempt.
    if (!ownBackup) return true;
    std::error_code error;
    std::filesystem::remove_all(backup, error);
    return !error && index_file::syncDirectory(backup.parent_path());
}
} // namespace dbms::vacuum_full_backup
