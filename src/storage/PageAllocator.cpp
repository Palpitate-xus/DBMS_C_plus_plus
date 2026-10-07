#include "PageAllocator.h"

#include "access/IndexFileUtil.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <unordered_map>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace dbms {

namespace {

constexpr uint32_t ALLOCATION_FLUSH_MARKER_MAGIC = 0x31465845u;  // "EXF1"
constexpr uint32_t ALLOCATION_FLUSH_MARKER_VERSION = 1;
constexpr const char* ALLOCATION_FLUSH_MARKER_SUFFIX = ".extent_pending";

#pragma pack(push, 1)
struct AllocationFlushMarkerHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t pageSize;
    uint32_t headerBytes;
    uint64_t dataFileBytes;
    uint64_t tdeFileBytes;
    uint64_t checksum;
};
#pragma pack(pop)

struct DurableAllocationState {
    bool hasHeader = false;
    uint64_t dataFileBytes = 0;
    uint64_t tdeFileBytes = 0;
    DataFileHeader header{};
    std::vector<char> headerPage;
};

std::filesystem::path allocationFlushMarkerPath(
    const std::string& filename) {
    return std::filesystem::path(filename + ALLOCATION_FLUSH_MARKER_SUFFIX);
}

// An extent marker belongs to the physical file, not to one allocator's
// buffer pool. Other live allocators must not mistake an in-flight marker
// for failed publication (or roll it back while opening another pool).
struct AllocationPublicationState {
    std::mutex mutex;
    std::atomic<const PageAllocator*> markerOwner{nullptr};
};

std::shared_ptr<AllocationPublicationState> allocationPublicationMutex(const std::string& filename) {
    struct Registry {
        std::mutex mutex;
        std::unordered_map<std::string, std::shared_ptr<AllocationPublicationState>> files;
        size_t lookups = 0;
    };
    // Allocator destructors can run during global-engine teardown.
    static Registry* registry = new Registry;
    std::error_code error;
    auto path = std::filesystem::weakly_canonical(filename, error);
    if (error) {
        error.clear();
        path = std::filesystem::absolute(filename, error).lexically_normal();
        if (error) path = std::filesystem::path(filename).lexically_normal();
    }
    std::lock_guard<std::mutex> lock(registry->mutex);
    auto& entry = registry->files[path.string()];
    auto result = entry;
    if (!result) {
        result = std::make_shared<AllocationPublicationState>();
        entry = result;
    }
    if (++registry->lookups % 64 == 0) {
        for (auto it = registry->files.begin(); it != registry->files.end();) {
            if (it->second.use_count() == 1 && !it->second->markerOwner.load())
                it = registry->files.erase(it);
            else ++it;
        }
    }
    return result;
}

bool readExactlyAt(int fd, void* data, size_t length, off_t offset) {
    auto* bytes = static_cast<char*>(data);
    size_t readBytes = 0;
    while (readBytes < length) {
        const ssize_t count = ::pread(
            fd, bytes + readBytes, length - readBytes,
            offset + static_cast<off_t>(readBytes));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        readBytes += static_cast<size_t>(count);
    }
    return true;
}

bool writeExactlyAt(int fd, const void* data, size_t length, off_t offset) {
    const auto* bytes = static_cast<const char*>(data);
    size_t written = 0;
    while (written < length) {
        const ssize_t count = ::pwrite(
            fd, bytes + written, length - written,
            offset + static_cast<off_t>(written));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        written += static_cast<size_t>(count);
    }
    return true;
}

bool syncParentDirectory(const std::filesystem::path& path) {
    const std::filesystem::path parent = path.parent_path().empty()
        ? std::filesystem::path(".") : path.parent_path();
    const int fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return false;
    const bool ok = (::fsync(fd) == 0);
    const bool closeOk = (::close(fd) == 0);
    return ok && closeOk;
}

bool removeFileDurably(const std::filesystem::path& path) {
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) return false;
    if (!exists) return true;
    if (!std::filesystem::remove(path, ec) || ec) return false;
    return syncParentDirectory(path);
}

uint64_t allocationMarkerChecksum(const char* bytes, size_t length) {
    constexpr size_t checksumOffset =
        offsetof(AllocationFlushMarkerHeader, checksum);
    constexpr size_t checksumEnd = checksumOffset + sizeof(uint64_t);
    uint64_t hash = 14695981039346656037ULL;  // FNV-1a/64 offset basis
    for (size_t i = 0; i < length; ++i) {
        const uint8_t value = (i >= checksumOffset && i < checksumEnd)
            ? 0 : static_cast<uint8_t>(bytes[i]);
        hash ^= value;
        hash *= 1099511628211ULL;
    }
    return hash;
}

bool validHeaderMetadata(const DataFileHeader& header, size_t rowSize,
                         size_t pageSize, uint32_t formatVersion) {
    if (header.magic != DATA_FILE_MAGIC ||
        header.formatVersion != DATA_FILE_FORMAT_VERSION ||
        header.formatVersion != formatVersion || header.numPages == 0 ||
        header.freeListHead >= header.numPages ||
        header.rowSize != rowSize ||
        header.headerChecksum != computeDataFileHeaderChecksum(header)) {
        return false;
    }
    return static_cast<uint64_t>(header.numPages) <=
        std::numeric_limits<uint64_t>::max() / pageSize;
}

bool captureDurableAllocationState(
    const std::string& filename, size_t rowSize, size_t pageSize,
    uint32_t formatVersion, bool allowEmptyBaseline,
    DurableAllocationState& state) {
    state = DurableAllocationState{};

    const int dataFd = ::open(filename.c_str(), O_RDONLY);
    if (dataFd < 0) {
        return allowEmptyBaseline && errno == ENOENT;
    }
    struct stat dataStatus {};
    bool ok = (::fstat(dataFd, &dataStatus) == 0) &&
              S_ISREG(dataStatus.st_mode) && dataStatus.st_size >= 0;
    const uint64_t physicalBytes = ok
        ? static_cast<uint64_t>(dataStatus.st_size) : 0;

    if (ok && physicalBytes >= pageSize) {
        state.headerPage.resize(pageSize);
        ok = readExactlyAt(dataFd, state.headerPage.data(), pageSize, 0);
        if (ok) {
            std::memcpy(&state.header, state.headerPage.data(),
                        sizeof(state.header));
            ok = validHeaderMetadata(
                state.header, rowSize, pageSize, formatVersion);
        }
        if (ok) {
            const uint64_t declaredBytes =
                static_cast<uint64_t>(state.header.numPages) * pageSize;
            ok = declaredBytes <= physicalBytes;
            if (ok) {
                state.hasHeader = true;
                // Ignore complete pages written past the last durable header.
                // A pending marker must restore the header's logical extent,
                // not preserve an unpublished eviction write.
                state.dataFileBytes = declaredBytes;
            }
        }
    } else if (ok && physicalBytes != 0) {
        ok = false;
    }
    if (::close(dataFd) != 0) ok = false;

    if (!ok) {
        if (!allowEmptyBaseline) return false;
        // A file opened from zero bytes can already have sparse/new pages
        // written by eviction while its protected page-zero header remains
        // dirty in memory. Its durable baseline is still the empty file.
        state = DurableAllocationState{};
    } else if (!state.hasHeader) {
        if (!allowEmptyBaseline || physicalBytes != 0) return false;
    }

    struct stat tdeStatus {};
    uint64_t physicalTdeBytes = 0;
    const std::string tdeFilename = filename + ".tde";
    if (::stat(tdeFilename.c_str(), &tdeStatus) == 0) {
        if (!S_ISREG(tdeStatus.st_mode) || tdeStatus.st_size < 0) return false;
        physicalTdeBytes = static_cast<uint64_t>(tdeStatus.st_size);
    } else if (errno != ENOENT) {
        return false;
    }
    const uint64_t maximumTdeBytes = state.hasHeader
        ? static_cast<uint64_t>(state.header.numPages) *
              PageCrypto::kRecordSize
        : 0;
    state.tdeFileBytes = std::min(physicalTdeBytes, maximumTdeBytes);
    return state.tdeFileBytes % PageCrypto::kRecordSize == 0;
}

std::string encodeAllocationFlushMarker(
    const DurableAllocationState& state, size_t pageSize) {
    AllocationFlushMarkerHeader header{};
    header.magic = ALLOCATION_FLUSH_MARKER_MAGIC;
    header.version = ALLOCATION_FLUSH_MARKER_VERSION;
    header.pageSize = static_cast<uint32_t>(pageSize);
    header.headerBytes = state.hasHeader
        ? static_cast<uint32_t>(pageSize) : 0;
    header.dataFileBytes = state.dataFileBytes;
    header.tdeFileBytes = state.tdeFileBytes;

    std::string bytes(sizeof(header) + header.headerBytes, '\0');
    std::memcpy(bytes.data(), &header, sizeof(header));
    if (header.headerBytes != 0) {
        std::memcpy(bytes.data() + sizeof(header), state.headerPage.data(),
                    pageSize);
    }
    header.checksum = allocationMarkerChecksum(bytes.data(), bytes.size());
    std::memcpy(bytes.data(), &header, sizeof(header));
    return bytes;
}

bool readAllocationFlushMarker(
    const std::filesystem::path& path, size_t rowSize, size_t pageSize,
    uint32_t formatVersion, DurableAllocationState& state) {
    std::error_code ec;
    const uintmax_t fileBytes = std::filesystem::file_size(path, ec);
    if (ec || fileBytes < sizeof(AllocationFlushMarkerHeader) ||
        fileBytes > sizeof(AllocationFlushMarkerHeader) + pageSize) {
        return false;
    }
    std::string bytes(static_cast<size_t>(fileBytes), '\0');
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool readOk = readExactlyAt(fd, bytes.data(), bytes.size(), 0);
    const bool closeOk = (::close(fd) == 0);
    if (!readOk || !closeOk) return false;

    AllocationFlushMarkerHeader marker{};
    std::memcpy(&marker, bytes.data(), sizeof(marker));
    if (marker.magic != ALLOCATION_FLUSH_MARKER_MAGIC ||
        marker.version != ALLOCATION_FLUSH_MARKER_VERSION ||
        marker.pageSize != pageSize ||
        (marker.headerBytes != 0 && marker.headerBytes != pageSize) ||
        bytes.size() != sizeof(marker) + marker.headerBytes ||
        marker.checksum != allocationMarkerChecksum(bytes.data(), bytes.size()) ||
        marker.dataFileBytes % pageSize != 0 ||
        marker.tdeFileBytes % PageCrypto::kRecordSize != 0) {
        return false;
    }

    state = DurableAllocationState{};
    state.dataFileBytes = marker.dataFileBytes;
    state.tdeFileBytes = marker.tdeFileBytes;
    state.hasHeader = marker.headerBytes != 0;
    if (!state.hasHeader) {
        return marker.dataFileBytes == 0 && marker.tdeFileBytes == 0;
    }
    if (marker.dataFileBytes < pageSize) return false;
    state.headerPage.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(sizeof(marker)),
        bytes.end());
    std::memcpy(&state.header, state.headerPage.data(), sizeof(state.header));
    if (!validHeaderMetadata(
            state.header, rowSize, pageSize, formatVersion) ||
        marker.dataFileBytes !=
            static_cast<uint64_t>(state.header.numPages) * pageSize ||
        marker.tdeFileBytes >
            static_cast<uint64_t>(state.header.numPages) *
                PageCrypto::kRecordSize) {
        return false;
    }
    return true;
}

bool restoreAllocationState(const std::string& filename,
                            const DurableAllocationState& state) {
    const int dataFd = ::open(filename.c_str(), O_RDWR | O_CREAT, 0644);
    if (dataFd < 0) return false;
    const std::string tdeFilename = filename + ".tde";
    const int tdeFd = ::open(tdeFilename.c_str(), O_RDWR | O_CREAT, 0600);
    if (tdeFd < 0) {
        ::close(dataFd);
        return false;
    }

    bool ok = true;
    if (state.hasHeader) {
        ok = writeExactlyAt(
            dataFd, state.headerPage.data(), state.headerPage.size(), 0);
    }
    if (ok && ::ftruncate(dataFd, static_cast<off_t>(state.dataFileBytes)) != 0)
        ok = false;
    if (ok && ::ftruncate(tdeFd, static_cast<off_t>(state.tdeFileBytes)) != 0)
        ok = false;
    if (::fsync(dataFd) != 0) ok = false;
    if (::fsync(tdeFd) != 0) ok = false;
    if (::close(dataFd) != 0) ok = false;
    if (::close(tdeFd) != 0) ok = false;
    return ok;
}

}  // namespace

size_t heapBufferFrameCount() {
    static const size_t frames = [] {
        const char* env = std::getenv("DBMS_BUFFER_FRAMES");
        if (env && *env) {
            char* end = nullptr;
            const long value = std::strtol(env, &end, 10);
            if (end && *end == '\0' && value >= 16 && value <= 4096) {
                return static_cast<size_t>(value);
            }
        }
        return static_cast<size_t>(256);
    }();
    return frames;
}

PageAllocator::PageAllocator(const std::string& filename, size_t rowSize, size_t pageSize, uint32_t formatVersion)
    : filename_(filename), rowSize_(rowSize), pageSize_(pageSize), formatVersion_(formatVersion), bp_(std::make_unique<BufferPool>(filename, heapBufferFrameCount(), pageSize)) {
    bp_->setWriteLastPage(0);
    // Verify heap pages when they are first loaded from disk. Page 0 is the
    // file header (own checksum, validated by validateFileHeader) and is
    // explicitly excluded from the heap-page check.
    const uint32_t fv = formatVersion_;
    bp_->setPageValidator([fv](uint32_t pageId, const char* data) -> bool {
        if (pageId == 0) return true;
        PageWrapper page(const_cast<char*>(data), PgPage::PAGE_SIZE, fv);
        if (!page.isValid()) {
            std::cerr << "[storage] invalid heap page " << pageId
                      << "; refusing to load corrupted data" << std::endl;
            return false;
        }
        if (page.hasBoundPageId() && !page.isValid(pageId)) {
            std::cerr << "[storage] heap page identity mismatch at block "
                      << pageId << "; refusing to load misplaced data"
                      << std::endl;
            return false;
        }
        return true;
    });
}

PageAllocator::~PageAllocator() {
    close();
}

bool PageAllocator::open() {
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    if (pageSize_ != PgPage::PAGE_SIZE || formatVersion_ != DATA_FILE_FORMAT_VERSION ||
        rowSize_ > std::numeric_limits<uint32_t>::max()) {
        std::cerr << "[storage] unsupported heap format: pageSize=" << pageSize_
                  << ", formatVersion=" << formatVersion_ << ", rowSize=" << rowSize_ << std::endl;
        return false;
    }
    if (bp_->isOpen()) return true;
    if (!recoverPendingAllocationFlush()) {
        std::cerr << "[storage] cannot recover pending heap extent publication: "
                  << filename_ << std::endl;
        return false;
    }
    std::error_code fileEc;
    const bool fileExists = std::filesystem::exists(filename_, fileEc);
    if (fileEc) return false;
    const uintmax_t fileBytes = fileExists ? std::filesystem::file_size(filename_, fileEc) : 0;
    if (fileEc) return false;
    const bool existingFile = fileBytes != 0;
    if (existingFile && (fileBytes < pageSize_ || fileBytes % pageSize_ != 0)) {
        std::cerr << "[storage] truncated or misaligned heap file: " << filename_ << std::endl;
        return false;
    }
    if (!bp_->open()) return false;

    // Check if page 0 exists and has valid magic
    char* buf = bp_->fetchPage(0);
    if (!buf) {
        bp_->close();
        return false;
    }
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(buf);
    if (fh->magic != DATA_FILE_MAGIC) {
        if (existingFile) {
            std::cerr << "[storage] unsupported heap file header: " << filename_ << std::endl;
            bp_->unpinPage(0);
            bp_->close();
            return false;
        }
        // New file: initialize file header
        std::memset(buf, 0, pageSize_);
        fh->magic = DATA_FILE_MAGIC;
        fh->numPages = 1;  // only page 0 (header)
        fh->freeListHead = 0;
        fh->rowSize = static_cast<uint32_t>(rowSize_);
        fh->formatVersion = DATA_FILE_FORMAT_VERSION;
        fh->headerChecksum = computeDataFileHeaderChecksum(*fh);
        bp_->markDirty(0);
    } else if (!validateFileHeader(*fh)) {
        std::cerr << "[storage] invalid heap file header: " << filename_ << std::endl;
        bp_->unpinPage(0);
        bp_->close();
        return false;
    }
    numPages_ = fh->numPages;
    bp_->unpinPage(0);
    durableHeaderPresent_ = existingFile;
    return true;
}

void PageAllocator::close() {
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::lock_guard<std::mutex> allocLock(allocMutex_);
    if (bp_) {
        if (bp_->isOpen()) {
            if (bp_->refersToCurrentFiles() && !flushWithAllocationMarker(false)) {
                std::cerr
                    << "[storage] failed to flush heap allocator during close; "
                       "discarding cached writeback for WAL/marker recovery: "
                    << filename_ << std::endl;
            }
            // Never let BufferPool retry outside the allocator's marker.
            bp_->close(/*flushPages=*/false);
        }
    }
    numPages_ = 0;
    durableHeaderPresent_ = false;
    // The cache is now closed; a subsequent opener may recover an actual
    // failed publication instead of rolling back underneath a live owner.
    if (publicationMutex->markerOwner.load() == this)
        publicationMutex->markerOwner.store(nullptr);
    pendingAllocationFlush_ = false;
}

bool PageAllocator::isOpen() const {
    return bp_ && bp_->isOpen();
}

uint32_t PageAllocator::allocPage() {
    if (!isOpen()) return 0;
    // The header page (free-list head, numPages) is read-modify-write state
    // shared by every allocator user; concurrent inserts may run under mere
    // intent locks, so serialize the whole allocation decision.
    std::lock_guard<std::mutex> lock(allocMutex_);

    char* fhBuf = bp_->fetchPage(0);
    if (!fhBuf) return 0;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);
    uint32_t pageId = 0;

    if (fh->freeListHead != 0) {
        // Reuse a page from the free list
        pageId = fh->freeListHead;
        if (pageId >= fh->numPages || pageId >= numPages_) {
            bp_->unpinPage(0);
            return 0;
        }
        char* pageBuf = fetchPage(pageId);
        if (!pageBuf) {
            bp_->unpinPage(0);
            return 0;
        }
        PageWrapper page(pageBuf, pageSize_, formatVersion_);
        uint32_t nextFree = page.nextPage();
        bp_->unpinPage(pageId);

        if (nextFree >= fh->numPages) {
            bp_->unpinPage(0);
            return 0;
        }
        fh->freeListHead = nextFree;
    } else {
        // Extend file with a new page
        if (fh->numPages == std::numeric_limits<uint32_t>::max()) {
            bp_->unpinPage(0);
            return 0;
        }
        pageId = fh->numPages;
        fh->numPages++;

        // Initialize the new page
        char* newBuf = bp_->fetchPage(pageId);
        if (!newBuf) {
            fh->numPages--;
            bp_->unpinPage(0);
            return 0;
        }
        PageWrapper newPage(newBuf, pageSize_, formatVersion_);
        newPage.init(pageId);
        bp_->markDirty(pageId);
        bp_->unpinPage(pageId);
        numPages_ = fh->numPages;
    }

    fh->headerChecksum = computeDataFileHeaderChecksum(*fh);
    bp_->markDirty(0);
    bp_->unpinPage(0);
    return pageId;
}

void PageAllocator::freePage(uint32_t pageId) {
    if (!isOpen() || pageId == 0) return;

    std::lock_guard<std::mutex> lock(allocMutex_);
    // Read file header
    char* fhBuf = bp_->fetchPage(0);
    if (!fhBuf) return;
    DataFileHeader* fh = reinterpret_cast<DataFileHeader*>(fhBuf);

    if (pageId >= numPages_) {
        bp_->unpinPage(0);
        return;
    }

    // Initialize the freed page and link it to free list
    char* pageBuf = fetchPage(pageId);
    if (!pageBuf) {
        bp_->unpinPage(0);
        return;
    }
    PageWrapper page(pageBuf, pageSize_, formatVersion_);
    page.init(pageId);
    page.setNextPage(fh->freeListHead);
    bp_->markDirty(pageId);
    bp_->unpinPage(pageId);

    // Update free list head
    fh->freeListHead = pageId;
    fh->headerChecksum = computeDataFileHeaderChecksum(*fh);
    bp_->markDirty(0);
    bp_->unpinPage(0);
}

uint32_t PageAllocator::numPages() const {
    if (!isOpen()) return 0;
    std::lock_guard<std::mutex> lock(allocMutex_);
    return numPages_;
}

std::optional<bool> PageAllocator::pageExistsOnDisk(uint32_t pageId) const {
    if (!isOpen() || pageSize_ == 0) return std::nullopt;
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    // A marker owned by this live allocator means physical publication is
    // incomplete but recoverable. Report the page as not-yet-published
    // rather than trying to interpret a deliberately transitional file.
    if (pendingAllocationFlush_) return false;
    DurableAllocationState state;
    if (!captureDurableAllocationState(
            filename_, rowSize_, pageSize_, formatVersion_,
            !durableHeaderPresent_, state)) {
        return std::nullopt;
    }
    return state.hasHeader && pageId < state.header.numPages;
}

bool PageAllocator::hasDirtyAllocationState() const {
    if (!isOpen() || !bp_) return false;
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    return bp_->isPageDirty(0);
}


char* PageAllocator::fetchPage(uint32_t pageId) {
    if (!isOpen()) return nullptr;
    if (pageId >= numPages_) return nullptr;
    // Page integrity (Fletcher-16 checksum + structural checks) is verified
    // once by the buffer pool when a page is loaded from disk; a cached page
    // cannot change underneath a pin, so re-validating on every access only
    // repeated an 8KB checksum per row operation.
    return bp_->fetchPage(pageId);
}

void PageAllocator::unpinPage(uint32_t pageId) {
    if (isOpen()) bp_->unpinPage(pageId);
}

void PageAllocator::markDirty(uint32_t pageId) {
    if (!isOpen()) return;
    if (pageId == 0) {
        bp_->markDirty(pageId);
        return;
    }

    // Upgrade legacy v4 identity only when a caller already marks a page
    // dirty. A read-only page load must not create a new dirty write.
    char* data = bp_->fetchPage(pageId);
    if (!data) return;
    PageWrapper page(data, pageSize_, formatVersion_);
    if (!page.hasBoundPageId() && !page.bindPageId(pageId)) {
        std::cerr << "[storage] cannot bind legacy heap page identity at block "
                  << pageId << std::endl;
    }
    if (page.hasBoundPageId() && !page.isValid(pageId)) {
        std::cerr << "[storage] heap page identity mismatch at block "
                  << pageId << "; refusing dirty mark" << std::endl;
        bp_->unpinPage(pageId);
        return;
    }
    bp_->markDirty(pageId);
    bp_->unpinPage(pageId);
}

bool PageAllocator::flush() {
    if (!isOpen()) return false;
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::lock_guard<std::mutex> allocLock(allocMutex_);
    if (!bp_->refersToCurrentFiles()) return false;
    return flushWithAllocationMarker(false);
}

bool PageAllocator::flushAllocationStateForCommit() {
    if (!isOpen()) return false;
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::lock_guard<std::mutex> allocLock(allocMutex_);
    if (!bp_->refersToCurrentFiles()) return false;
    return flushWithAllocationMarker(true);
}

bool PageAllocator::prepareAllocationFlushMarker(
    std::string& markerPath, std::string& markerBytes) {
    char* headerBuffer = bp_->fetchPage(0);
    if (!headerBuffer) return false;
    DataFileHeader currentHeader{};
    std::memcpy(&currentHeader, headerBuffer, sizeof(currentHeader));
    bp_->unpinPage(0);
    if (!validHeaderMetadata(
            currentHeader, rowSize_, pageSize_, formatVersion_) ||
        currentHeader.numPages != numPages_) {
        return false;
    }

    DurableAllocationState previous;
    if (!captureDurableAllocationState(
            filename_, rowSize_, pageSize_, formatVersion_,
            !durableHeaderPresent_, previous)) {
        return false;
    }
    if (previous.hasHeader &&
        currentHeader.numPages < previous.header.numPages) {
        // A stale allocator must never shrink a relation another backend has
        // already published.
        return false;
    }

    markerPath = allocationFlushMarkerPath(filename_).string();
    markerBytes = encodeAllocationFlushMarker(previous, pageSize_);
    return true;
}

bool PageAllocator::finishAllocationFlushMarker(
    const std::string& markerPath) {
    const auto publication = allocationPublicationMutex(filename_);
    if (removeFileDurably(markerPath)) {
        pendingAllocationFlush_ = false;
        durableHeaderPresent_ = true;
        if (publication->markerOwner.load() == this)
            publication->markerOwner.store(nullptr);
        return true;
    }
    // unlink may have succeeded while only the directory fsync failed. The
    // live process should retain the successfully flushed cache/disk state;
    // after a crash the old directory entry may reappear and startup recovery
    // can still consume it before opening the cache.
    std::error_code ec;
    const bool exists = std::filesystem::exists(markerPath, ec);
    if (!ec && !exists) {
        pendingAllocationFlush_ = false;
        durableHeaderPresent_ = true;
        if (publication->markerOwner.load() == this)
            publication->markerOwner.store(nullptr);
    }
    return false;
}

bool PageAllocator::flushWithAllocationMarker(bool allocationOnly) {
    if (!recoverPendingAllocationFlush()) return false;
    if (!bp_->isPageDirty(0)) {
        if (pendingAllocationFlush_) {
            if (!finishAllocationFlushMarker(
                    allocationFlushMarkerPath(filename_).string())) {
                return false;
            }
        }
        return allocationOnly ? true : bp_->flush();
    }

    std::string markerPath =
        allocationFlushMarkerPath(filename_).string();
    std::string markerBytes;
    if (!pendingAllocationFlush_) {
        if (!prepareAllocationFlushMarker(markerPath, markerBytes)) return false;
        const bool written = index_file::writeAtomically(markerPath, markerBytes);
        std::error_code error;
        if (written || std::filesystem::exists(markerPath, error) || error) {
            pendingAllocationFlush_ = true;
            allocationPublicationMutex(filename_)->markerOwner.store(this);
        }
        if (!written) return false;
    }

    // BufferPool writes every dirty data page, syncs main/TDE files, and only
    // then writes and syncs page zero.  The durable marker repairs a torn
    // final header or truncates an only-partly-published new extent on open.
    if (!bp_->flush()) return false;
    return finishAllocationFlushMarker(markerPath);
}

bool PageAllocator::flushDirtyUnpinned(
    const std::function<bool()>& walBarrier) {
    if (!isOpen()) return false;
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::lock_guard<std::mutex> allocLock(allocMutex_);
    if (!bp_->refersToCurrentFiles()) return false;
    if (!recoverPendingAllocationFlush()) return false;

    bool headerDirty = bp_->isPageDirty(0);
    std::string markerPath =
        allocationFlushMarkerPath(filename_).string();
    if (pendingAllocationFlush_ && !headerDirty) {
        if (!finishAllocationFlushMarker(markerPath)) return false;
        headerDirty = bp_->isPageDirty(0);
    }
    std::string markerBytes;
    if (headerDirty && !pendingAllocationFlush_ &&
        !prepareAllocationFlushMarker(markerPath, markerBytes)) {
        return false;
    }

    bool writebackStarted = false;
    const bool ok = bp_->flushDirtyUnpinned([&]() {
        // BufferPool holds its mutex here. No eligible frame can be pinned or
        // changed between this durability boundary and physical writeback.
        if (!walBarrier || !walBarrier()) return false;
        if (headerDirty && !pendingAllocationFlush_) {
            const bool written = index_file::writeAtomically(markerPath, markerBytes);
            std::error_code error;
            if (written || std::filesystem::exists(markerPath, error) || error) {
                pendingAllocationFlush_ = true;
                publicationMutex->markerOwner.store(this);
            }
            if (!written) return false;
        }
        writebackStarted = true;
        return true;
    });
    if (!ok) return false;
    if (writebackStarted && pendingAllocationFlush_) {
        return finishAllocationFlushMarker(markerPath);
    }
    return true;
}

bool PageAllocator::flushPage(uint32_t pageId) {
    if (!isOpen() || pageId == 0) return false;
    const auto publicationMutex = allocationPublicationMutex(filename_);
    std::lock_guard<std::mutex> publicationLock(publicationMutex->mutex);
    std::lock_guard<std::mutex> flushLock(flushMutex_);
    std::lock_guard<std::mutex> allocLock(allocMutex_);
    if (!bp_->refersToCurrentFiles()) return false;
    if (!recoverPendingAllocationFlush()) return false;
    if (pendingAllocationFlush_ &&
        !flushWithAllocationMarker(/*allocationOnly=*/true)) {
        return false;
    }
    return bp_->flushPage(pageId);
}

bool PageAllocator::recoverPendingAllocationFlush() {
    const auto publication = allocationPublicationMutex(filename_);
    const std::filesystem::path marker =
        allocationFlushMarkerPath(filename_);
    std::error_code ec;
    const bool exists = std::filesystem::exists(marker, ec);
    if (ec) return false;
    if (!exists) {
        pendingAllocationFlush_ = false;
        if (publication->markerOwner.load() == this)
            publication->markerOwner.store(nullptr);
        return true;
    }
    // A failed flush remains retryable by its live cache. Other allocators
    // must fail closed, not consume its marker as though the owner crashed.
    const auto owner = publication->markerOwner.load();
    if (owner && owner != this) return false;
    // A marker created by this live allocator protects an in-progress or
    // retryable flush. Restoring its old disk image underneath the live cache
    // would lose clean frames after a marker-cleanup failure. Only startup
    // (buffer pool still closed) rolls a foreign/crash marker back.
    if (bp_ && bp_->isOpen()) return pendingAllocationFlush_;

    DurableAllocationState previous;
    if (!readAllocationFlushMarker(
            marker, rowSize_, pageSize_, formatVersion_, previous)) {
        return false;
    }
    if (!restoreAllocationState(filename_, previous)) return false;
    if (!removeFileDurably(marker)) return false;
    durableHeaderPresent_ = previous.hasHeader;
    pendingAllocationFlush_ = false;
    return true;
}

bool PageAllocator::validateFileHeader(const DataFileHeader& fh) const {
    if (fh.magic != DATA_FILE_MAGIC || fh.formatVersion != DATA_FILE_FORMAT_VERSION ||
        fh.numPages == 0 || fh.freeListHead >= fh.numPages ||
        fh.headerChecksum != computeDataFileHeaderChecksum(fh)) {
        return false;
    }
    std::error_code ec;
    const uintmax_t bytes = std::filesystem::file_size(filename_, ec);
    if (ec || bytes < pageSize_ || bytes % pageSize_ != 0) return false;
    const uintmax_t diskPages = bytes / pageSize_;
    return diskPages == fh.numPages &&
           fh.numPages <= std::numeric_limits<uint32_t>::max();
}

} // namespace dbms
