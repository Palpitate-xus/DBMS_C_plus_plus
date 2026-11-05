#pragma once

#include "catalog/AnalyzedScalarStatistics.h"
#include "catalog/AnalyzedArrayStatistics.h"
#include "catalog/AnalyzedStatisticsStore.h"
#include "catalog/CatalogService.h"
#include "catalog/DatumMetadataFactory.h"
#include "commands/TableManage.h"
#include "expression/ArrayDatumInput.h"
#include "utils/Session.h"
#include <cstring>
#include <fstream>
#include <random>
#include <unordered_map>

namespace dbms::analyzed_statistics {
namespace capture_detail {
inline std::pair<uint32_t, uint32_t> scalarOperators(uint32_t base) {
    // Immutable builtin operator identities for the existing scalar semantics.
    // This does not register pg_operator or resolve user-defined operators.
    switch (base) {
        case 16: return {91, 58};
        case 20: return {410, 412};
        case 21: return {94, 95};
        case 23: return {96, 97};
        case 25: case 1043: return {98, 664};
        case 700: return {620, 622};
        case 701: return {670, 672};
        case 790: return {900, 902};
        case 1042: return {1054, 1058};
        case 1082: return {1093, 1095};
        case 1083: return {1108, 1110};
        case 1114: return {2060, 2062};
        case 1184: return {1320, 1322};
        case 1700: return {1752, 1754};
        case 2950: return {2972, 2974};
        default: return {};
    }
}
inline uint64_t schemaImage(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw DbError("58030", "could not read analyzed relation schema");
    std::string bytes{std::istreambuf_iterator<char>(input), {}};
    if (input.bad()) throw DbError("58030", "could not read complete analyzed relation schema");
    return codec::checksum(bytes);
}
inline std::string attributeImage(const CatalogManager::MetadataSnapshot& snapshot,
                                  uint32_t relation) {
    std::vector<const PgAttributeRow*> rows;
    for (const auto& a : snapshot.attributes) if (a.attrelid == relation) rows.push_back(&a);
    std::sort(rows.begin(), rows.end(), [](const auto* a, const auto* b) { return a->attnum < b->attnum; });
    codec::Writer image;
    for (const auto* a : rows) {
        image.u(static_cast<uint16_t>(a->attnum), 2); image.text(a->attname);
        image.u(a->atttypid, 4); image.u(static_cast<uint32_t>(a->atttypmod), 4);
        image.u(a->attcollation, 4); image.u(static_cast<uint32_t>(a->attstattarget), 4);
        image.u(static_cast<uint32_t>(a->attndims), 4); image.flag(a->attisdropped);
        image.u(static_cast<unsigned char>(a->attgenerated), 1);
    }
    return image.finish();
}
} // namespace capture_detail

// One ANALYZE invocation owns the catalog, sample and comparator inputs.
// Existing native statistics continue to use their original full scan/format.
class ScalarStatisticsCapture {
    struct Attribute {
        size_t physicalColumn;
        Key key;
        Column statistics;
        dbms::Column storage;
        uint32_t equality, ordering;
        std::vector<ScalarSample> sample;
        std::vector<ArraySample> arrays;
        dbms::Column elementStorage;
        uint32_t elementEquality = 0;
    };
    std::vector<Attribute> attributes_;
    std::filesystem::path schemaPath_;
    uint64_t schemaImage_ = 0, seen_ = 0;
    uint32_t relation_ = 0;
    std::string schema_, name_, attributeImage_;
    size_t capacity_ = 0;
    std::optional<size_t> selected_;
    std::optional<std::mt19937_64> random_;
public:
    using IndexedComparison = std::function<int(size_t, size_t)>;
    using ComparisonFactory = std::function<IndexedComparison(
        const dbms::Column&, const std::vector<ScalarSample>&)>;
    ScalarStatisticsCapture(const TableSchema& table, const std::string& physical,
        const std::filesystem::path& schemaPath,
        const CatalogManager::MetadataSnapshot& snapshot) : schemaPath_(schemaPath) {
        auto logical = CatalogService::logicalName(physical);
        if (const auto* session = currentSession()) {
            const auto prefix = tempTablePrefix(*session, "");
            if (physical.rfind(prefix, 0) == 0) {
                logical.schema = sessionTempSchemaName(*session);
                logical.name = physical.substr(prefix.size());
            }
        }
        schema_ = logical.schema.empty() ? "public" : logical.schema; name_ = logical.name;
        uint32_t ns = 0;
        for (const auto& n : snapshot.namespaces) if (n.nspname == schema_) ns = n.oid;
        for (const auto& r : snapshot.relations)
            if (ns && r.relnamespace == ns && r.relname == name_ &&
                (r.relkind == 'r' || r.relkind == 'p' || r.relkind == 'm')) relation_ = r.oid;
        if (!relation_) return; // No invented identity for a legacy/native orphan.
        schemaImage_ = capture_detail::schemaImage(schemaPath_);
        attributeImage_ = capture_detail::attributeImage(snapshot, relation_);
        DatumMetadataFactory factory(snapshot);
        for (size_t i = 0; i < table.len; ++i) {
            const auto& storage = table.cols[i];
            if (storage.generatedKind == 'v') continue;
            for (const auto& a : snapshot.attributes) {
                if (a.attrelid != relation_ || a.attname != storage.dataName ||
                    a.attnum <= 0 || a.attisdropped || a.attstattarget == 0) continue;
                const auto type = factory.attribute(a);
                if (!type || type->enumType || type->kind == DatumKind::Record) continue;
                const bool array = type->kind == DatumKind::Array;
                const auto leaf = array ? type->arrayElement : type;
                if (!leaf || leaf->kind != DatumKind::Scalar || leaf->enumType ||
                    array != storage.isArray) continue;
                const auto ops = capture_detail::scalarOperators(leaf->codecOid);
                if (!ops.first) continue; // Range/custom analysis needs its own owner.
                const auto genuine = std::find_if(snapshot.types.begin(), snapshot.types.end(),
                    [&](const auto& t) { return t.oid == leaf->codecOid && t.typnamespace == 11; });
                const auto& builtin = CatalogManager::builtinTypeRows();
                if (genuine == snapshot.types.end() || std::none_of(builtin.begin(), builtin.end(),
                    [&](const auto& t) { return t.oid == genuine->oid && t.typnamespace == 11 &&
                        t.typname == genuine->typname && t.typtype == genuine->typtype; })) continue;
                Column stats; stats.physicalRelation = table.physicalRelationId;
                stats.schemaGeneration = schemaImage_; stats.attributeType = type;
                stats.collation = a.attcollation; stats.target = a.attstattarget < 0 ? 100 : a.attstattarget;
                if (stats.target > 10000) throw DbError("22023", "statistics target exceeds 10000");
                if (!storage.isVariableLength) stats.averageWidth = storage.dsize;
                capacity_ = std::max(capacity_, size_t(stats.target) * 300);
                attributes_.push_back({i, {relation_, a.attnum,
                    table.partitionType != TableSchema::PartitionType::None}, std::move(stats), storage,
                    array ? 1070U : ops.first, array ? 1072U : ops.second, {}, {}, {}, 0});
                if (array) {
                    auto& attribute = attributes_.back();
                    // Comparison consumes the physical element's existing
                    // spelling/codec, not a newly normalized SQL declaration
                    // (e.g. double versus double precision).
                    attribute.elementStorage = storage;
                    attribute.elementStorage.isArray = false;
                    attribute.elementStorage.isVariableLength = leaf->codecSize < 0;
                    if (leaf->codecSize > 0) attribute.elementStorage.dsize = leaf->codecSize;
                    attribute.elementEquality = ops.first;
                }
                break;
            }
        }
    }
    void beginRow() {
        ++seen_; selected_.reset();
        if (!capacity_) return;
        if (seen_ <= capacity_) selected_ = seen_ - 1;
        else {
            if (!random_) { std::random_device seed; random_.emplace((uint64_t(seed()) << 32) ^ seed()); }
            const auto slot = std::uniform_int_distribution<uint64_t>(0, seen_ - 1)(*random_);
            if (slot < capacity_) selected_ = slot;
        }
        if (selected_ && seen_ <= capacity_)
            for (auto& a : attributes_) {
                if (a.elementEquality) a.arrays.emplace_back();
                else a.sample.emplace_back();
            }
    }
    void cell(size_t physicalColumn, const std::string& raw, bool isNull,
              const std::string& row, const TableSchema& table) {
        if (!selected_) return;
        for (auto& a : attributes_) {
            if (a.physicalColumn != physicalColumn) continue;
            ScalarSample cell; cell.ordinal = seen_ - 1;
            cell.value = ExprValue(a.storage.dataType + (a.elementEquality ? "[]" : ""), {}, isNull);
            if (!isNull) {
                size_t width = a.storage.dsize;
                if (a.storage.isVariableLength) {
                    const size_t position = table.fixedDataSize() + table.getVarColIndex(physicalColumn) * 4;
                    if (position + 4 > row.size()) throw DbError("XX001", "truncated analyzed variable datum");
                    uint16_t offset = 0, bytes = 0;
                    std::memcpy(&offset, row.data() + position, 2);
                    std::memcpy(&bytes, row.data() + position + 2, 2);
                    if (size_t(offset) + bytes > row.size()) throw DbError("XX001", "invalid analyzed variable datum span");
                    width = bytes; // Actual stored payload, including a TOAST reference when present.
                }
                if (width > INT32_MAX) throw DbError("54000", "analyzed datum width exceeds int32");
                cell.storedWidth = width;
                cell.tooWide = a.statistics.attributeType->codecSize < 0 && raw.size() + 4 > 1024;
                if (!cell.tooWide || (a.elementEquality && raw.size() + 4 <= 65536))
                    cell.value.value = raw;
                cell.value.collation = a.storage.collation;
                if (a.elementEquality && raw.size() + 4 <= 65536 &&
                    !compound_value_detail::decodeArrayStorageInput(cell.value, a.statistics.attributeType))
                    throw DbError("XX001", "could not decode actual analyzed array storage input");
            }
            if (a.elementEquality) a.arrays[*selected_] = {std::move(cell), isNull ? 0 : raw.size() + 4};
            else a.sample[*selected_] = std::move(cell);
        }
    }
    std::map<Key, Column> finish(const ComparisonFactory& prepareComparison = {}) {
        std::map<Key, Column> result;
        for (auto& a : attributes_) {
            a.statistics.totalRows = seen_;
            if (a.elementEquality) {
                auto sample = std::move(a.arrays);
                // Reuse the existing local parsed-key/locale owner for the
                // element comparison too. Raw spelling only locates a key;
                // it never defines equality. Bound this optional cache by
                // the column's sample target; exceeding it keeps the entire
                // column on its original comparator.
                std::vector<ScalarSample> keys;
                std::unordered_map<std::string, size_t> keyIndex;
                bool complete = bool(prepareComparison);
                if (complete) for (const auto& input : sample) {
                    if (input.scalar.value.isNull || input.logicalBytes > 65536) continue;
                    checkForQueryInterrupt();
                    for (const auto& value : *analyzedArray(input.scalar.value, a.statistics.attributeType).arrayElements) {
                        if (value.isNull) continue;
                        const auto inserted = keyIndex.emplace(value.value, keys.size());
                        if (!inserted.second) continue;
                        if (keys.size() >= size_t(a.statistics.target) * 300) { complete = false; break; }
                        ScalarSample key; key.value = value; keys.push_back(std::move(key));
                    }
                    if (!complete) break;
                }
                IndexedComparison cached;
                if (complete) {
                    for (const auto& key : keys)
                        if (StorageEngine::compareValues(a.elementStorage, key.value.value, false,
                                key.value.value, false, "=") != StorageEngine::PredicateTruth::True)
                            throw DbError("0A000", "analyzed array element has no applicable equality");
                    cached = prepareComparison(a.elementStorage, keys);
                }
                if (!cached) { keyIndex.clear(); keys.clear(); }
                const auto compare = [&a, &keyIndex, &cached](const ExprValue& x, const ExprValue& y) {
                    if (x.value == y.value) return 0;
                    if (cached) {
                        const auto left = keyIndex.find(x.value), right = keyIndex.find(y.value);
                        if (left == keyIndex.end() || right == keyIndex.end())
                            throw DbError("XX001", "analyzed array comparison key is absent");
                        return cached(left->second, right->second);
                    }
                    const auto equal = StorageEngine::compareValues(a.elementStorage,
                        x.value, false, y.value, false, "=");
                    const auto less = StorageEngine::compareValues(a.elementStorage,
                        x.value, false, y.value, false, "<");
                    if (equal == StorageEngine::PredicateTruth::Unknown || less == StorageEngine::PredicateTruth::Unknown)
                        throw DbError("0A000", "analyzed array element has no applicable comparison");
                    return equal == StorageEngine::PredicateTruth::True ? 0 :
                        less == StorageEngine::PredicateTruth::True ? -1 : 1;
                };
                if (auto stats = collectArrayStatistics(a.statistics, sample, a.equality, a.ordering,
                        a.elementEquality, compare, [] { checkForQueryInterrupt(); }))
                    result.emplace(a.key, std::move(*stats));
                continue;
            }
            // The produced slots own their selected values. Release each
            // sample before the original full-scan statistics sort begins.
            auto sample = std::move(a.sample);
            bool supported = true;
            for (const auto& s : sample) if (!s.value.isNull && !s.tooWide &&
                StorageEngine::compareValues(a.storage, s.value.value, false, s.value.value, false, "=")
                    != StorageEngine::PredicateTruth::True) { supported = false; break; }
            if (!supported) continue;
            const auto compare = [&a](const ExprValue& x, const ExprValue& y) {
                if (x.value == y.value) return 0;
                const auto equal = StorageEngine::compareValues(a.storage, x.value, false, y.value, false, "=");
                if (equal == StorageEngine::PredicateTruth::True) return 0;
                const auto less = StorageEngine::compareValues(a.storage, x.value, false, y.value, false, "<");
                if (equal == StorageEngine::PredicateTruth::Unknown || less == StorageEngine::PredicateTruth::Unknown)
                    throw DbError("0A000", "analyzed scalar has no applicable comparison");
                return less == StorageEngine::PredicateTruth::True ? -1 : 1;
            };
            const auto indexed = prepareComparison ? prepareComparison(a.storage, sample) : IndexedComparison{};
            if (auto stats = collectScalarStatistics(a.statistics, sample, a.equality, a.ordering,
                    compare, [] { checkForQueryInterrupt(); }, indexed)) result.emplace(a.key, std::move(*stats));
        }
        return result;
    }
    bool hasIdentity() const { return relation_ != 0; }
    uint32_t relation() const { return relation_; }
    void verify(const CatalogManager::MetadataSnapshot& current) const {
        if (!relation_) return;
        uint32_t ns = 0;
        for (const auto& n : current.namespaces) if (n.nspname == schema_) ns = n.oid;
        const bool same = std::any_of(current.relations.begin(), current.relations.end(), [&](const auto& r) {
            return r.oid == relation_ && r.relnamespace == ns && r.relname == name_;
        });
        if (!same || capture_detail::schemaImage(schemaPath_) != schemaImage_ ||
            capture_detail::attributeImage(current, relation_) != attributeImage_)
            throw DbError("55000", "relation definition changed during ANALYZE");
    }
};
} // namespace dbms::analyzed_statistics
