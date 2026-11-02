#include "executor/ExecutionPlan.h"
#include "Config.h"

#include <cassert>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <malloc.h>
#include <optional>
#include <unistd.h>

extern dbms::Config g_config;

static size_t liveBytes() {
    const auto info = mallinfo2(); return info.uordblks + info.hblkhd;
}
static size_t sortDescriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code error;
        const auto name = std::filesystem::read_symlink(entry.path(),error).string();
        count += !error && name.find("/.dbms-sort-") != std::string::npos;
    }
    return count;
}
static std::string payload(size_t id,size_t width) {
    std::string text(width,'a' + id % 23);
    if (width >= 8) text.replace(0,8,std::string("\0\nNULL\xce\xb2",8));
    return text;
}

class WideSource final : public dbms::Operator {
    size_t count_, width_, position_ = 0;
    size_t& before_; size_t& retained_; size_t& peak_; size_t& descriptors_; size_t& closed_;
public:
    WideSource(size_t count,size_t width,size_t& before,size_t& retained,size_t& peak,size_t& descriptors,size_t& closed)
        : count_(count),width_(width),before_(before),retained_(retained),peak_(peak),descriptors_(descriptors),closed_(closed) {}
    bool open() override { position_ = 0; before_ = liveBytes(); return true; }
    bool next(std::string& raw) override {
        descriptors_ = std::max(descriptors_,sortDescriptors());
        const auto current = liveBytes();
        peak_ = std::max(peak_,current > before_ ? current - before_ : 0);
        if (position_ == count_) {
            retained_ = current > before_ ? current - before_ : 0;
            return false;
        }
        const int32_t id = static_cast<int32_t>(count_ - 1 - position_++),key = id % 31;
        raw.resize(8); std::memcpy(raw.data(),&key,4); std::memcpy(raw.data()+4,&id,4);
        raw += payload(id,width_); return true;
    }
    void close() override { ++closed_; }
    bool lastColumnIsNull(size_t ordinal) const override { return ordinal == 0 && (count_ - position_) % 13 == 0; }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override {
        if (!position_) return false;
        nulls = {lastColumnIsNull(0),false}; return true;
    }
};

int main() {
    using namespace dbms;
    const auto original = g_config.workMemKb;
    TableSchema schema; schema.tablename = "wide_sort";
    schema.append(makeIntColumn("k",true,2)); schema.append(makeIntColumn("id",false,2));
    size_t checked = 0,failures = 0;
    for (const auto count : {size_t(0),size_t(1),size_t(257),size_t(8192)})
        for (bool ascending : {false,true}) for (bool first : {false,true}) {
            constexpr size_t width = 2048;
            g_config.workMemKb = 64;
            size_t before=0,retained=0,peak=0,descriptors=0,closed=0;
            SortOp sort(std::make_unique<WideSource>(count,width,before,retained,peak,descriptors,closed),schema,"k",ascending,first,true);
            bool good = sort.open();
            const auto openedDescriptors = sortDescriptors();
            std::vector<bool> seen(count,false);
            std::optional<int32_t> previous; bool previousNull = false,havePrevious = false;
            std::string raw; size_t rows = 0;
            while (sort.next(raw)) {
                int32_t key=0,id=0;
                if (raw.size() != width+8) { good=false; break; }
                std::memcpy(&key,raw.data(),4); std::memcpy(&id,raw.data()+4,4);
                if (id<0 || size_t(id)>=count) { good=false; break; }
                const bool isNull = id % 13 == 0;
                good = good && !seen[id] && key == id%31 &&
                    raw.substr(8) == payload(id,width) && sort.lastColumnIsNull(0)==isNull &&
                    !sort.lastColumnIsNull(1);
                std::vector<bool> bitmap;
                good = sort.lastStoredNullBitmap(bitmap) && bitmap==std::vector<bool>({isNull,false}) && good;
                if (havePrevious) {
                    if (previousNull != isNull) good = good && previousNull == first;
                    else if (!isNull) good = good && (ascending ? *previous<=key : *previous>=key);
                }
                previous=key; previousNull=isNull; havePrevious=true; seen[id]=true; ++rows;
            }
            good = good && !sort.hasError() && rows==count && closed==1 && descriptors<=2 &&
                (count<257 ? openedDescriptors==0 : openedDescriptors==1) && retained < 1024*1024 && peak < 1024*1024;
            sort.close(); sort.close();
            good = good && sortDescriptors()==0 && closed==1;
            ++checked; failures += !good;
            std::cout << "SORT_SPILL_MEMORY count=" << count << " asc=" << ascending << " first=" << first
                      << " rows=" << rows << " retained_at_eof=" << retained << " input_boundary_peak=" << peak << " fds=" << openedDescriptors
                      << " passed=" << good << '\n';
        }
    // Oversized records may exceed work_mem individually, but must not cause
    // the sorter to retain the entire oversized input or an fd per record.
    g_config.workMemKb = 1;
    size_t before=0,retained=0,peak=0,descriptors=0,closed=0;
    SortOp sort(std::make_unique<WideSource>(32,262144,before,retained,peak,descriptors,closed),schema,"id",true);
    bool good = sort.open(); std::string raw; size_t count=0;
    while (sort.next(raw)) {
        int32_t id=0; std::memcpy(&id,raw.data()+4,4);
        good = good && size_t(id)==count && raw.substr(8)==payload(id,262144); ++count;
    }
    good = good && count==32 && retained<1024*1024 && peak<1024*1024 && sortDescriptors()==1;
    sort.close(); good = good && sortDescriptors()==0 && closed==1;
    ++checked; failures += !good;
    std::cout << "SORT_SPILL_OVERSIZED rows=" << count << " retained=" << retained << " input_boundary_peak=" << peak << " passed=" << good << '\n';
    g_config.workMemKb = original;
    assert(checked==17);
    std::cout << "SORT_SPILL_MEMORY_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
