#pragma once

#include "catalog/AnalyzedStatisticsStore.h"
#include "catalog/catalog.h"
#include <array>
#include <memory>
#include <sys/stat.h>

namespace dbms::analyzed_statistics {

// Owned by one StorageEngine, under its cache mutex. Keep only one database
// image; aliasing column leases survive replacement. Writers publish complete
// frames by rename. Name identity, change times, generation/header and stored
// checksum are checked on every lookup. Timestamp resolution alone is not a
// valid cache key, even for rapid complete same-size in-place restoration.
class ReadCache {
    struct Identity {
        dev_t device{}; ino_t inode{}; off_t size{};
        timespec modified{}, changed{};
        bool operator==(const Identity& b) const {
            return device == b.device && inode == b.inode && size == b.size &&
                modified.tv_sec == b.modified.tv_sec && modified.tv_nsec == b.modified.tv_nsec &&
                changed.tv_sec == b.changed.tv_sec && changed.tv_nsec == b.changed.tv_nsec;
        }
    };
    struct Stamp {
        Identity identity;
        std::array<char, sizeof(codec::magic) + 4 + 8 + 8 + 4> header{};
        std::array<char, 8> checksum{};
        bool operator==(const Stamp& b) const {
            return identity == b.identity && header == b.header && checksum == b.checksum;
        }
    };
    std::string snapshotPath_;
    Stamp snapshotStamp_;
    std::shared_ptr<const Snapshot> snapshot_;
    std::string catalogPath_;
    dev_t catalogDevice_{};
    ino_t catalogInode_{};
    std::shared_ptr<const CatalogManager> catalog_;
    static std::optional<Identity> identity(const std::filesystem::path& path) {
        struct stat value{};
        if (::stat(path.c_str(), &value)) {
            if (errno == ENOENT) return {};
            throw DbError("58030", "could not inspect analyzed statistics input");
        }
        if (!S_ISREG(value.st_mode))
            throw DbError("58030", "analyzed statistics input is not a regular file");
        return Identity{value.st_dev, value.st_ino, value.st_size, value.st_mtim, value.st_ctim};
    }
    static std::string name(const std::filesystem::path& path) {
        return std::filesystem::absolute(path).lexically_normal().string();
    }
    static std::optional<Stamp> stamp(const std::filesystem::path& path) {
        const auto before = identity(path);
        if (!before) return {};
        Stamp result; result.identity = *before;
        if (before->size < static_cast<off_t>(result.header.size() + result.checksum.size()))
            codec::invalid("frame size");
        if (static_cast<uint64_t>(before->size) > codec::maxBytes) codec::limit("byte limit");
        std::ifstream input(path, std::ios::binary);
        if (!input) throw DbError("58030", "could not open analyzed statistics cache identity");
        input.read(result.header.data(), result.header.size());
        input.seekg(-static_cast<std::streamoff>(result.checksum.size()), std::ios::end);
        input.read(result.checksum.data(), result.checksum.size());
        if (!input) throw DbError("58030", "could not read complete analyzed statistics cache identity");
        const auto after = identity(path);
        if (!after || !(*before == *after)) return {};
        return result;
    }
public:
    // Reuse only a cold, privately owned read generation. Never populate
    // CatalogService's mutable cache as a side effect of cost estimation.
    // An existing service model or own DDL writer must use acquire()'s
    // current checkout instead; file identity alone cannot see dirty rows.
    template<class Acquire>
    std::shared_ptr<const CatalogManager> catalog(const std::filesystem::path& database,
                                                  bool reuseCold, Acquire acquire) {
        if (!reuseCold) { catalog_.reset(); catalogPath_.clear(); return acquire(); }
        struct stat directory{};
        if (::stat(database.c_str(), &directory) || !S_ISDIR(directory.st_mode))
            throw DbError("58030", "could not inspect analyzed catalog database");
        const auto key = name(database);
        if (catalog_ && catalogPath_ == key && catalogDevice_ == directory.st_dev &&
            catalogInode_ == directory.st_ino &&
            catalog_->publicationState() == CatalogManager::PublicationState::Current)
            return catalog_;
        catalog_.reset(); catalogPath_.clear();
        auto owned = acquire();
        catalogPath_ = key; catalogDevice_ = directory.st_dev; catalogInode_ = directory.st_ino;
        catalog_ = std::move(owned); return catalog_;
    }
    std::shared_ptr<const Snapshot> snapshot(const std::filesystem::path& path) {
        const auto key = name(path);
        const auto before = stamp(path);
        if (before && snapshot_ && snapshotPath_ == key && snapshotStamp_ == *before)
            return snapshot_;
        snapshot_.reset(); snapshotPath_.clear();
        if (!before) return {};
        auto value = read(path);
        const auto after = stamp(path);
        if (!value || !after || !(*before == *after)) return {};
        auto owned = std::make_shared<const Snapshot>(std::move(*value));
        snapshotPath_ = key; snapshotStamp_ = *after; snapshot_ = std::move(owned);
        return snapshot_;
    }
    std::optional<uint64_t> schemaDigest(const std::filesystem::path& path) {
        const auto before = identity(path);
        if (!before) return {};
        std::ifstream input(path, std::ios::binary);
        if (!input) throw DbError("58030", "could not open analyzed relation schema");
        const std::string bytes{std::istreambuf_iterator<char>(input), {}};
        if (input.bad()) throw DbError("58030", "could not read complete analyzed relation schema");
        const auto after = identity(path);
        if (!after || !(*before == *after)) return {};
        // Schemas are small. Hash the current bytes rather than trusting a
        // cached timestamp across same-tick DDL or rollback restoration.
        return codec::checksum(bytes);
    }
};
} // namespace dbms::analyzed_statistics
