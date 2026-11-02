#include "executor/TypedSortSpill.h"

#include <cassert>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

using namespace dbms;
namespace codec = dbms::typed_sort_detail;

static bool sameValue(const ExprValue& first, const ExprValue& second) {
    std::vector<std::pair<const ExprValue*, const ExprValue*>> pending{{&first, &second}};
    while (!pending.empty()) {
        const auto [a, b] = pending.back(); pending.pop_back();
        if (a->typeName != b->typeName || a->value != b->value || a->isNull != b->isNull ||
            a->collation != b->collation || a->objectOid != b->objectOid || a->elementOids != b->elementOids ||
            bool(a->recordFields) != bool(b->recordFields)) return false;
        if (a->recordFields) {
            if (a->recordFields->size() != b->recordFields->size()) return false;
            for (size_t i = 0; i < a->recordFields->size(); ++i)
                pending.emplace_back(&a->recordFields->at(i), &b->recordFields->at(i));
        }
    }
    return true;
}

template <typename Map>
static bool sameMap(const Map& first, const Map& second) {
    if (first.size() != second.size()) return false;
    auto a = first.begin(), b = second.begin();
    for (; a != first.end(); ++a, ++b)
        if (a->first != b->first || !sameValue(a->second, b->second)) return false;
    return true;
}
static bool sameContext(const RowContext& a, const RowContext& b) {
    const auto& first = RowContextSpillAccess::parameters(a);
    const auto& second = RowContextSpillAccess::parameters(b);
    if (first.size() != second.size()) return false;
    for (size_t i = 0; i < first.size(); ++i) if (!sameValue(first[i], second[i])) return false;
    return sameMap(RowContextSpillAccess::sqlValues(a), RowContextSpillAccess::sqlValues(b)) &&
        sameMap(RowContextSpillAccess::values(a), RowContextSpillAccess::values(b)) &&
        sameMap(RowContextSpillAccess::boundColumns(a), RowContextSpillAccess::boundColumns(b));
}

template <typename Produce, typename Consume>
static bool malformed(Produce produce, Consume consume) {
    sort_detail::File file;
    sort_detail::Writer writer(file.fd(), 17); produce(writer); writer.flush();
    sort_detail::Reader reader(file.fd(), 0, writer.position(), 11);
    try { consume(reader); }
    catch (const DbError& error) { return error.sqlState() == "58030"; }
    catch (...) { return false; }
    return false;
}

int main() {
    size_t checked = 0, failures = 0;
    const auto check = [&](const char* label, bool good) {
        ++checked; failures += !good;
        if (!good) std::cout << "TYPED_SORT_SPILL_CODEC_FAILED " << label << '\n';
    };
    std::vector<ExprValue> values;
    values.emplace_back(); // Genuine UNKNOWN sentinel, not a typed NULL.
    values.emplace_back("text", "");
    values.emplace_back("text", "NULL");
    values.emplace_back("text", std::string("a\0\nNULL\xce\xb2", 9)); values.back().collation = "C";
    values.emplace_back("text", "retained NULL bytes", true); values.back().collation = "POSIX";
    for (uint32_t oid : {uint32_t(23), uint32_t(0), std::numeric_limits<uint32_t>::max()}) {
        values.emplace_back("regtype", "rendered type name"); values.back().objectOid = oid;
    }
    values.emplace_back("regtype[]", "[0:3]={integer,NULL,opaque,0}");
    values.back().elementOids = {23, std::nullopt, std::numeric_limits<uint32_t>::max(), 0};
    values.back().collation = "array collation bytes";
    values.emplace_back("record", "opaque record text"); // No structural record.
    values.emplace_back("record", "()"); values.back().recordFields.emplace();
    values.emplace_back("record", std::string("(opaque\0record)", 15));
    values.back().recordFields = std::vector<ExprValue>{values[1], values[4], values[8], values[10]};
    values.back().recordFields->at(1).objectOid = 0;
    for (const auto& value : values) {
        sort_detail::File file;
        sort_detail::Writer writer(file.fd(), 17); codec::writeValue(writer, value); writer.flush();
        sort_detail::Reader reader(file.fd(), 0, writer.position(), 11);
        const auto restored = codec::readValue(reader);
        check("complete scalar/identity/record datum", !reader.remaining() && sameValue(value, restored) &&
            codec::valueBytes(value) >= sizeof(ExprValue) + value.value.size());
    }
    {
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 19);
        for (const auto& value : values) codec::writeValue(writer, value);
        writer.flush(); sort_detail::Reader reader(file.fd(), 0, writer.position(), 7);
        bool good = true;
        for (const auto& value : values) good = sameValue(value, codec::readValue(reader)) && good;
        check("multiple datums retain exact stream boundaries", good && !reader.remaining());
    }

    // The public map constructor admits exact legacy keys without normalizing
    // them. Keep a differently cased key beside the normalized setter key so a
    // codec that accidentally replays set() cannot silently collapse the two.
    RowContext source(std::map<std::string, ExprValue>{{"MIXED.CASE", values[6]}, {"UnFolded.Key", values[7]}});
    for (uint32_t kind = 0; kind <= static_cast<uint32_t>(FunctionCallExpr::SqlValue::LocalTimestamp); ++kind)
        source.setSqlValue(static_cast<FunctionCallExpr::SqlValue>(kind), values[kind % values.size()]);
    source.set("Mixed.Case", values[11]); source.set("", values[2]);
    source.set(std::string("binary\0key", 10), values[3]);
    source.setParameters(values);
    source.setBoundColumn(0, 0, values[4]); source.setBoundColumn(7, 9, values[8]);
    source.setBoundColumn(std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max(), values[11]);
    RowContext restored;
    {
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 13);
        codec::writeContext(writer, source); writer.flush();
        sort_detail::Reader reader(file.fd(), 0, writer.position(), 9);
        restored = codec::readContext(reader);
        check("all four context containers and actual public lookups", sameContext(source, restored) && !reader.remaining() &&
            restored.get("MIXED.CASE") && sameValue(*restored.get("MIXED.CASE"), values[11]) &&
            restored.parameterCount() == values.size() && sameValue(restored.parameter(8), values[8]) &&
            sameValue(restored.boundColumn(7, 9), values[8]) &&
            sameValue(*restored.sqlValue(FunctionCallExpr::SqlValue::CurrentTimestamp),
                *source.sqlValue(FunctionCallExpr::SqlValue::CurrentTimestamp)));
    }
    restored.set("mixed.case", ExprValue("integer", "99"));
    restored.setParameters({ExprValue("integer", "99")}); restored.setBoundColumn(7, 9, ExprValue("text", "different"));
    check("decoded context owns its values independently", sameValue(*source.get("mixed.case"), values[11]) &&
        source.parameterCount() == values.size() && sameValue(source.boundColumn(7, 9), values[8]));
    {
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 23);
        codec::writeContext(writer, source); codec::writeContext(writer, restored); writer.flush();
        sort_detail::Reader reader(file.fd(), 0, writer.position(), 5);
        auto first = codec::readContext(reader), second = codec::readContext(reader);
        check("independent context snapshots retain stream boundaries", sameContext(source, first) &&
            sameContext(restored, second) && !reader.remaining());
    }
    {
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 11); RowContext empty;
        codec::writeContext(writer, empty); writer.flush(); sort_detail::Reader reader(file.fd(), 0, writer.position(), 3);
        check("empty context retains absent containers", sameContext(empty, codec::readContext(reader)) && !reader.remaining());
    }
    {
        ExprValue deep("integer", "42");
        for (size_t i = 0; i < 2048; ++i) {
            ExprValue parent("record", "(nested)"); parent.recordFields.emplace();
            parent.recordFields->push_back(std::move(deep)); deep = std::move(parent);
        }
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 29); codec::writeValue(writer, deep); writer.flush();
        sort_detail::Reader reader(file.fd(), 0, writer.position(), 31);
        const auto copy = codec::readValue(reader);
        check("deep records use iterative traversal without new depth cutoff", sameValue(deep, copy) && !reader.remaining() &&
            codec::valueBytes(deep) > 2048 * sizeof(ExprValue));
    }
    {
        ExprValue cell("text", "x"); const auto before = codec::valueBytes(cell); cell.value.reserve(8192);
        check("account string capacity", codec::valueBytes(cell) >= before + 8000);
        const auto beforeElements = codec::valueBytes(cell); cell.elementOids.reserve(128);
        check("account element identity capacity", codec::valueBytes(cell) >= beforeElements + 128 * sizeof(std::optional<uint32_t>));
        const auto beforeFields = codec::valueBytes(cell); cell.recordFields.emplace(); cell.recordFields->reserve(64);
        check("account record field capacity", codec::valueBytes(cell) >= beforeFields + 64 * sizeof(ExprValue));
    }
    for (size_t container = 0; container < 4; ++container) {
        RowContext row; const auto before = codec::contextBytes(row);
        if (container == 0) row.setSqlValue(FunctionCallExpr::SqlValue::CurrentDate, values[3]);
        if (container == 1) row.set(std::string(128, 'k'), values[3]);
        if (container == 2) row.setParameters({values[3]});
        if (container == 3) row.setBoundColumn(7, 9, values[3]);
        check("account every owned context container", codec::contextBytes(row) >= before + sizeof(ExprValue));
    }
    check("accounting arithmetic saturates", codec::addBytes(std::numeric_limits<size_t>::max(), 1) ==
        std::numeric_limits<size_t>::max() && codec::multiplyBytes(std::numeric_limits<size_t>::max(), 2) ==
        std::numeric_limits<size_t>::max() && codec::multiplyBytes(0, std::numeric_limits<size_t>::max()) == 0);
    for (bool context : {false, true}) {
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 17);
        if (context) codec::writeContext(writer, source); else codec::writeValue(writer, values[11]);
        writer.flush(); bool allRejected = true;
        for (uint64_t prefix = 0; prefix < writer.position(); ++prefix) {
            sort_detail::Reader reader(file.fd(), 0, prefix, 19);
            try { if (context) (void)codec::readContext(reader); else (void)codec::readValue(reader); allRejected = false; }
            catch (const DbError& error) { allRejected = allRejected && error.sqlState() == "58030"; }
            catch (...) { allRejected = false; }
        }
        check("every truncated prefix rejected with typed I/O state", allRejected);
    }
    const auto readCell = [](sort_detail::Reader& reader) { (void)codec::readValue(reader); };
    for (size_t mode = 0; mode < 10; ++mode) {
        codec::ValueHeader header{};
        if (mode == 0) header.isNull = 2;
        if (mode == 1) header.hasOid = 2;
        if (mode == 2) header.hasFields = 2;
        if (mode == 3) header.oid = 23;
        if (mode == 4) header.fields = 1;
        if (mode == 5) header.type = std::numeric_limits<uint64_t>::max();
        if (mode == 6) header.value = std::numeric_limits<uint64_t>::max();
        if (mode == 7) header.collation = std::numeric_limits<uint64_t>::max();
        if (mode == 8) header.elements = std::numeric_limits<uint64_t>::max();
        if (mode == 9) { header.hasFields = 1; header.fields = std::numeric_limits<uint64_t>::max(); }
        check("reject malformed value flags/extents before allocation", malformed([&](auto& writer) { writer.scalar(header); }, readCell));
    }
    for (bool absent : {false, true}) {
        check("reject malformed element identities", malformed([&](auto& writer) {
            codec::ValueHeader header{}; header.elements = 1; writer.scalar(header);
            codec::ElementHeader element{}; element.present = absent ? 0 : 2; element.oid = absent ? 23 : 0;
            writer.scalar(element);
        }, readCell));
    }
    const auto readRow = [](sort_detail::Reader& reader) { (void)codec::readContext(reader); };
    for (size_t container = 0; container < 4; ++container) {
        check("reject oversized context counts before allocation", malformed([&](auto& writer) {
            codec::ContextHeader header{};
            if (container == 0) header.sql = std::numeric_limits<uint64_t>::max();
            if (container == 1) header.values = std::numeric_limits<uint64_t>::max();
            if (container == 2) header.parameters = std::numeric_limits<uint64_t>::max();
            if (container == 3) header.bound = std::numeric_limits<uint64_t>::max();
            writer.scalar(header);
        }, readRow));
    }
    check("reject invalid SQL value enum", malformed([&](auto& writer) {
        writer.scalar(codec::ContextHeader{1, 0, 0, 0}); writer.scalar(std::numeric_limits<uint32_t>::max());
        codec::writeValue(writer, values[0]);
    }, readRow));
    check("reject duplicate SQL value slots", malformed([&](auto& writer) {
        writer.scalar(codec::ContextHeader{2, 0, 0, 0});
        for (size_t i = 0; i < 2; ++i) { writer.scalar(uint32_t(1)); codec::writeValue(writer, values[i]); }
    }, readRow));
    check("reject duplicate exact named slots", malformed([&](auto& writer) {
        writer.scalar(codec::ContextHeader{0, 2, 0, 0});
        for (size_t i = 0; i < 2; ++i) { writer.scalar(uint64_t(3)); writer.append("key", 3); codec::writeValue(writer, values[i]); }
    }, readRow));
    check("reject duplicate bound source/column slots", malformed([&](auto& writer) {
        writer.scalar(codec::ContextHeader{0, 0, 0, 2});
        for (size_t i = 0; i < 2; ++i) { writer.scalar(uint64_t(7)); writer.scalar(uint64_t(9)); codec::writeValue(writer, values[i]); }
    }, readRow));
    check("reject overflowing context name length", malformed([&](auto& writer) {
        writer.scalar(codec::ContextHeader{0, 1, 0, 0}); writer.scalar(std::numeric_limits<uint64_t>::max());
        codec::writeValue(writer, values[0]);
    }, readRow));
    {
        ExprValue capacity("record", "opaque record");
        capacity.elementOids.reserve(1025);
        capacity.recordFields.emplace(); capacity.recordFields->reserve(1025);
        for (uint32_t i = 0; i < 1025; ++i) {
            capacity.elementOids.emplace_back(i % 3 ? std::optional<uint32_t>(i) : std::nullopt);
            capacity.recordFields->emplace_back("integer", std::to_string(i));
        }
        sort_detail::File file; sort_detail::Writer writer(file.fd(), 29);
        codec::writeValue(writer, capacity); writer.flush();
        sort_detail::Reader reader(file.fd(), 0, writer.position(), 31);
        const auto copy = codec::readValue(reader);
        check("validated element count reserves exact 1025 capacity", sameValue(capacity, copy) &&
            copy.elementOids.size() == 1025 && copy.elementOids.capacity() == 1025 && !reader.remaining());
        check("validated record count reserves exact 1025 capacity", copy.recordFields &&
            copy.recordFields->size() == 1025 && copy.recordFields->capacity() == 1025 &&
            codec::valueBytes(copy) <= codec::valueBytes(capacity));
    }
    assert(checked == 51);
    std::cout << "TYPED_SORT_SPILL_CODEC_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
