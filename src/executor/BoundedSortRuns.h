#pragma once

#include "commands/TableManage.h"
#include "executor/SortSpill.h"

#include <functional>
#include <memory>
#include <optional>
#include <utility>

namespace dbms::sort_detail {

// Forward-only external sorting for owning row types. Less must include the
// original input ordinal as its final tie-breaker. The caller evaluates every
// original SQL key/target before add(); this class never evaluates SQL.
// Codec::bytes includes sizeof(Row) and all transitively owned capacities.
// Codec::read validates each extent/count before allocating its storage.
template <class Row, class Codec, class Less>
class BoundedRuns {
public:
    using BatchSort = std::function<void(std::vector<Row>&, const Less&)>;
private:
    enum class Phase { Loading, Output, Closed };
    struct Input {
        std::unique_ptr<Reader> reader;
        Row row;
        uint64_t left = 0;
    };
    static constexpr size_t absent = std::numeric_limits<size_t>::max();
    size_t budget_, bufferSize_, allowance_;
    Codec codec_;
    Less less_;
    std::optional<size_t> bound_;
    BatchSort batchSort_;
    Phase phase_ = Phase::Loading;
    std::vector<Row> rows_;
    size_t dynamicBytes_ = 0, maximumRowBytes_ = 1, position_ = 0, comparisons_ = 0;
    bool heapReady_ = false, spilled_ = false;
    uint64_t inputTotal_ = 0, spooledTotal_ = 0, runCount_ = 0, extent_ = 0;
    uint64_t remainingRows_ = 0, emitted_ = 0;
    // Readers/writers are destroyed before the descriptors they borrow.
    std::unique_ptr<File> input_, scratch_;
    std::unique_ptr<Writer> writer_;
    std::vector<Input> finalHeads_;
    std::vector<size_t> finalHeap_;
    size_t pendingHead_ = absent;

    static size_t sum(size_t a, size_t b) {
        return b > std::numeric_limits<size_t>::max() - a
            ? std::numeric_limits<size_t>::max() : a + b;
    }
    static size_t product(size_t a, size_t b) {
        return a && b > std::numeric_limits<size_t>::max() / a
            ? std::numeric_limits<size_t>::max() : a * b;
    }
    static uint64_t countSum(uint64_t a, uint64_t b) {
        if (b > std::numeric_limits<uint64_t>::max() - a)
            throw DbError("58030", "invalid sort temporary row count");
        return a + b;
    }
    size_t extraBytes(const Row& row) const {
        const auto bytes = codec_.bytes(row);
        if (bytes < sizeof(Row)) throw DbError("XX000", "invalid sort row memory estimate");
        return bytes - sizeof(Row);
    }
    bool checkedLess(const Row& a, const Row& b) {
        if ((comparisons_++ & 127U) == 0) checkForQueryInterrupt();
        return less_(a, b);
    }
    size_t nextCapacity() const {
        if (rows_.size() < rows_.capacity()) return rows_.capacity();
        if (rows_.size() == std::numeric_limits<size_t>::max())
            throw DbError("54000", "too many sort input rows");
        size_t next = std::max<size_t>(1, product(rows_.capacity(), 2));
        if (!input_ && bound_) next = std::min(next, *bound_);
        return std::max(next, rows_.size() + 1);
    }
    bool exceeds(size_t capacity, size_t dynamic) const {
        return sum(product(capacity, sizeof(Row)), dynamic) > allowance_;
    }
    void sortBatch() {
        checkForQueryInterrupt();
        batchSort_(rows_, less_);
        checkForQueryInterrupt();
        heapReady_ = false;
    }
    void flushRun() {
        if (rows_.empty()) return;
        sortBatch();
        if (!input_) {
            input_ = std::make_unique<File>();
            spilled_ = true;
            writer_ = std::make_unique<Writer>(input_->fd(), bufferSize_);
        }
        const auto start = writer_->position();
        writer_->scalar(RunHeader{});
        for (const auto& row : rows_) {
            checkForQueryInterrupt();
            codec_.write(*writer_, row);
        }
        const RunHeader header{writer_->position() - start, rows_.size()};
        writer_->flush();
        writeAt(input_->fd(), start, reinterpret_cast<const char*>(&header), sizeof(header));
        spooledTotal_ = countSum(spooledTotal_, header.rows);
        ++runCount_; extent_ = writer_->position();
        std::vector<Row>().swap(rows_);
        dynamicBytes_ = 0;
    }
    void addImpl(Row row) {
        if (phase_ != Phase::Loading) throw DbError("XX000", "sort input is already closed");
        checkForQueryInterrupt();
        if (inputTotal_ == std::numeric_limits<uint64_t>::max())
            throw DbError("54000", "too many sort input rows");
        ++inputTotal_;
        if (bound_ && !*bound_) return;
        // Rejected TopN rows need no deep capacity traversal. Their original
        // SQL keys and early targets have already been evaluated by the owner.
        if (!input_ && bound_ && heapReady_) {
            if (!checkedLess(row, rows_.front())) return;
            const auto extra = extraBytes(row);
            const auto priorExtra = extraBytes(rows_.front());
            const auto replacement = sum(dynamicBytes_ - priorExtra, extra);
            maximumRowBytes_ = std::max(maximumRowBytes_, sum(sizeof(Row), extra));
            if (!exceeds(rows_.capacity(), replacement)) {
                const auto compare = [&](const Row& a, const Row& b) { return checkedLess(a,b); };
                std::pop_heap(rows_.begin(), rows_.end(), compare);
                rows_.back() = std::move(row);
                // Moving a short string into an existing long string can
                // retain the old allocation. Account the stored object, not
                // just the incoming row's smaller capacity estimate.
                const auto storedExtra = extraBytes(rows_.back());
                dynamicBytes_ = sum(dynamicBytes_ - priorExtra, storedExtra);
                maximumRowBytes_ = std::max(maximumRowBytes_, sum(sizeof(Row),storedExtra));
                if (exceeds(rows_.capacity(), dynamicBytes_)) flushRun();
                else std::push_heap(rows_.begin(), rows_.end(), compare);
                return;
            }
            // Previously rejected rows remain dominated by the retained
            // bound-sized witness set. Spool that set and all later rows;
            // sorting it externally still yields the exact first bound rows.
            flushRun();
        }
        const auto extra = extraBytes(row);
        maximumRowBytes_ = std::max(maximumRowBytes_, sum(sizeof(Row), extra));
        if (!rows_.empty() && exceeds(nextCapacity(), sum(dynamicBytes_, extra))) flushRun();
        if (rows_.size() == rows_.capacity()) rows_.reserve(nextCapacity());
        dynamicBytes_ = sum(dynamicBytes_, extra);
        rows_.push_back(std::move(row));
        if (exceeds(rows_.capacity(), dynamicBytes_)) {
            // One oversized incoming row is allowed, then immediately becomes
            // its own run. Do not retain an input-sized list of large rows.
            flushRun();
        } else if (!input_ && bound_ && rows_.size() == *bound_) {
            const auto compare = [&](const Row& a, const Row& b) { return checkedLess(a,b); };
            std::make_heap(rows_.begin(), rows_.end(), compare);
            heapReady_ = true;
        }
    }
    RunHeader runHeader(uint64_t offset) const {
        if (offset > extent_ || extent_ - offset < sizeof(RunHeader))
            throw DbError("58030", "invalid sort temporary run header");
        Reader reader(input_->fd(), offset, offset + sizeof(RunHeader), sizeof(RunHeader));
        const auto header = reader.scalar<RunHeader>();
        if (header.bytes < sizeof(header) || header.bytes > extent_ - offset ||
            !header.rows || header.rows > spooledTotal_)
            throw DbError("58030", "invalid sort temporary run extent");
        return header;
    }
    uint64_t loadHeads(uint64_t& offset, size_t fanIn, std::vector<Input>& heads,
                       std::vector<size_t>& heap) {
        uint64_t count = 0;
        heads.reserve(fanIn); heap.reserve(fanIn);
        for (size_t i = 0; i < fanIn && offset < extent_; ++i) {
            checkForQueryInterrupt();
            const auto header = runHeader(offset);
            auto reader = std::make_unique<Reader>(input_->fd(), offset + sizeof(header),
                                                   offset + header.bytes, bufferSize_);
            auto row = codec_.read(*reader);
            heap.push_back(heads.size());
            heads.push_back({std::move(reader),std::move(row),header.rows - 1});
            offset += header.bytes;
            count = countSum(count, header.rows);
        }
        const auto later = [&](size_t a, size_t b) { return checkedLess(heads[b].row, heads[a].row); };
        std::make_heap(heap.begin(), heap.end(), later);
        return count;
    }
    void mergePass(size_t fanIn) {
        if (!scratch_) scratch_ = std::make_unique<File>();
        else scratch_->truncate();
        Writer target(scratch_->fd(), bufferSize_);
        uint64_t offset = 0, newRuns = 0, seenRuns = 0, count = 0;
        while (offset < extent_) {
            std::vector<Input> heads;
            std::vector<size_t> heap;
            const auto groupRows = loadHeads(offset, fanIn, heads, heap);
            seenRuns = countSum(seenRuns, heads.size());
            count = countSum(count, groupRows);
            const auto start = target.position();
            target.scalar(RunHeader{});
            const auto later = [&](size_t a, size_t b) { return checkedLess(heads[b].row, heads[a].row); };
            while (!heap.empty()) {
                checkForQueryInterrupt();
                std::pop_heap(heap.begin(), heap.end(), later);
                const auto index = heap.back(); heap.pop_back();
                auto& head = heads[index];
                codec_.write(target, head.row);
                if (head.left) {
                    head.row = codec_.read(*head.reader); --head.left;
                    heap.push_back(index); std::push_heap(heap.begin(), heap.end(), later);
                } else if (head.reader->remaining()) {
                    throw DbError("58030", "invalid sort temporary run row count");
                }
            }
            const RunHeader header{target.position() - start, groupRows};
            target.flush();
            writeAt(scratch_->fd(), start, reinterpret_cast<const char*>(&header), sizeof(header));
            ++newRuns;
        }
        if (offset != extent_ || seenRuns != runCount_ || count != spooledTotal_)
            throw DbError("58030", "sort temporary output count mismatch");
        extent_ = target.position(); runCount_ = newRuns;
        input_.swap(scratch_);
    }
    void finishImpl() {
        if (phase_ == Phase::Output) return;
        if (phase_ != Phase::Loading) throw DbError("XX000", "sort is already closed");
        if (!input_) {
            sortBatch();
        } else {
            flushRun(); writer_.reset();
            // Count buffers, head/index storage and owning row payloads. The
            // minimum two-way merge may exceed work_mem for oversized rows;
            // this is bounded by a fixed number of rows, never input length.
            const auto perHead = sum(maximumRowBytes_, sum(bufferSize_, sizeof(Input) + sizeof(size_t)));
            const size_t fanIn = std::max<size_t>(2, std::min<size_t>(16, budget_ / perHead));
            while (runCount_ > fanIn) { checkForQueryInterrupt(); mergePass(fanIn); }
            scratch_.reset();
            uint64_t offset = 0;
            const auto count = loadHeads(offset, fanIn, finalHeads_, finalHeap_);
            if (offset != extent_ || finalHeads_.size() != runCount_ || count != spooledTotal_)
                throw DbError("58030", "sort temporary output count mismatch");
            remainingRows_ = spooledTotal_;
        }
        phase_ = Phase::Output;
    }
    bool nextImpl(Row& row) {
        if (phase_ == Phase::Closed) return false;
        if (phase_ != Phase::Output) throw DbError("XX000", "sort output is not ready");
        // The bound applies before a pending refill: no additional tape work
        // belongs to the first row that the consumer did not request.
        if (bound_ && emitted_ >= *bound_) return false;
        checkForQueryInterrupt();
        if (!input_) {
            if (position_ == rows_.size()) return false;
            row = std::move(rows_[position_++]); ++emitted_; return true;
        }
        const auto later = [&](size_t a, size_t b) { return checkedLess(finalHeads_[b].row, finalHeads_[a].row); };
        if (pendingHead_ != absent) {
            auto& head = finalHeads_[pendingHead_];
            if (head.left) {
                head.row = codec_.read(*head.reader); --head.left;
                finalHeap_.push_back(pendingHead_);
                std::push_heap(finalHeap_.begin(), finalHeap_.end(), later);
            } else if (head.reader->remaining()) {
                throw DbError("58030", "invalid sort temporary run row count");
            }
            pendingHead_ = absent;
        }
        if (finalHeap_.empty()) {
            if (remainingRows_) throw DbError("58030", "sort temporary output count mismatch");
            return false;
        }
        if (!remainingRows_) throw DbError("58030", "sort temporary output count mismatch");
        std::pop_heap(finalHeap_.begin(), finalHeap_.end(), later);
        pendingHead_ = finalHeap_.back(); finalHeap_.pop_back();
        row = std::move(finalHeads_[pendingHead_].row);
        --remainingRows_; ++emitted_; return true;
    }
public:
    BoundedRuns(size_t budgetBytes, Codec codec, Less less, std::optional<size_t> bound,
                BatchSort batchSort)
        : budget_(std::max<size_t>(1,budgetBytes)),
          bufferSize_(std::max<size_t>(64,std::min<size_t>(65536,budget_ / 16))),
          allowance_(budget_ - budget_ / 4), codec_(std::move(codec)), less_(std::move(less)),
          bound_(bound), batchSort_(std::move(batchSort)) {
        if (!batchSort_) throw DbError("XX000", "sort has no batch sorting callback");
    }
    ~BoundedRuns() { close(); }
    BoundedRuns(const BoundedRuns&) = delete;
    BoundedRuns& operator=(const BoundedRuns&) = delete;
    void add(Row row) {
        try { addImpl(std::move(row)); }
        catch (...) { close(); throw; }
    }
    void finish() {
        try { finishImpl(); }
        catch (...) { close(); throw; }
    }
    bool next(Row& row) {
        try { return nextImpl(row); }
        catch (...) { close(); throw; }
    }
    void close() noexcept {
        phase_ = Phase::Closed;
        finalHeads_.clear(); finalHeap_.clear(); writer_.reset();
        scratch_.reset(); input_.reset(); std::vector<Row>().swap(rows_);
        dynamicBytes_ = 0; pendingHead_ = absent;
    }
    bool spilled() const { return spilled_; }
};

} // namespace dbms::sort_detail
