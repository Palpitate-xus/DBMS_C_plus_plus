#include "commands/RoutineStore.h"
#include "access/IndexFileUtil.h"
#include "common/DbError.h"
#include "common/sha256.h"
#include <algorithm>
#include <array>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <string_view>

namespace dbms {
namespace {
constexpr std::string_view magic = "DBMS_ROUTINE_V3\n";
constexpr size_t maxArguments = 100;

[[noreturn]] void corrupt(const std::string& message) {
    throw DbError("58030", "invalid routine metadata: " + message);
}
[[noreturn]] void io(const std::string& message) {
    throw DbError("58030", "routine metadata I/O: " + message);
}
void identifier(const std::string& value) {
    if (value.empty() || value.size() > 63 || value.find('\0') != std::string::npos)
        throw DbError("42602", "invalid routine or namespace identifier");
}
void validate(const StoredRoutine& routine) {
    identifier(routine.schema);
    identifier(routine.name);
    if (routine.inputTypeOids.size() > maxArguments)
        throw DbError("54023", "functions cannot have more than 100 arguments");
    if (routine.parameterNames.size() != routine.inputTypeOids.size() ||
        routine.parameterTypes.size() != routine.inputTypeOids.size())
        corrupt("parameter descriptor lengths differ");
    if (routine.returnType.empty() || routine.language.empty() || routine.body.empty() ||
        (routine.volatility != 'i' && routine.volatility != 's' && routine.volatility != 'v'))
        corrupt("incomplete routine declaration");
}
std::string hex(const std::string& value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(value.size()*2);
    for (unsigned char byte : value) {
        result += digits[byte >> 4];
        result += digits[byte & 15];
    }
    return result;
}
std::string unhex(const std::string& value) {
    if (value.empty() || value.size()%2 || value.size() > 126)
        corrupt("invalid routine directory identity");
    const auto nibble = [](char c) -> unsigned {
        if (c >= '0' && c <= '9') return static_cast<unsigned>(c-'0');
        if (c >= 'a' && c <= 'f') return static_cast<unsigned>(c-'a'+10);
        corrupt("noncanonical routine directory identity");
    };
    std::string result;
    for (size_t i = 0; i < value.size(); i += 2)
        result += static_cast<char>((nibble(value[i]) << 4) | nibble(value[i+1]));
    identifier(result);
    return result;
}
void appendNumber(std::string& bytes, uint32_t value) {
    for (unsigned shift : {24U, 16U, 8U, 0U})
        bytes += static_cast<char>((value >> shift) & 255U);
}
void appendString(std::string& bytes, const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max())
        throw DbError("54000", "routine declaration is too large");
    appendNumber(bytes, static_cast<uint32_t>(value.size()));
    bytes += value;
}
std::string signature(const std::vector<uint32_t>& inputs) {
    if (inputs.size() > maxArguments)
        throw DbError("54023", "functions cannot have more than 100 arguments");
    std::string result;
    appendNumber(result, static_cast<uint32_t>(inputs.size()));
    for (uint32_t oid : inputs) appendNumber(result, oid);
    return result;
}
struct Reader {
    const std::string& bytes;
    size_t offset;
    size_t end;
    uint32_t number() {
        if (end-offset < 4) corrupt("truncated integer");
        uint32_t value = 0;
        for (unsigned i = 0; i < 4; ++i)
            value = (value << 8) | static_cast<unsigned char>(bytes[offset++]);
        return value;
    }
    std::string string() {
        const auto length = number();
        if (length > end-offset) corrupt("truncated field");
        auto result = bytes.substr(offset, length);
        offset += length;
        return result;
    }
    unsigned char byte() {
        if (offset == end) corrupt("truncated flag");
        return static_cast<unsigned char>(bytes[offset++]);
    }
};
bool pathExists(const std::filesystem::path& path) {
    std::error_code error;
    const bool found = std::filesystem::exists(path, error);
    if (error) io("cannot inspect " + path.string());
    return found;
}
void directory(const std::filesystem::path& path) {
    std::error_code error;
    if (!std::filesystem::is_directory(path, error) || error)
        io("not a directory: " + path.string());
}
void ensureDirectory(const std::filesystem::path& path) {
    std::error_code error;
    const bool made = std::filesystem::create_directory(path, error);
    if (error) io("cannot create directory: " + path.string());
    directory(path);
    if (made && !index_file::syncDirectory(path.parent_path()))
        io("cannot sync new directory: " + path.string());
}
std::string read(const std::filesystem::path& path) {
    std::error_code error;
    if (!std::filesystem::is_regular_file(path, error) || error)
        io("not a regular file: " + path.string());
    std::ifstream stream(path, std::ios::binary);
    if (!stream) io("cannot open " + path.string());
    std::string bytes{std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
    if (stream.bad()) io("cannot read " + path.string());
    return bytes;
}
std::vector<std::filesystem::directory_entry> entries(const std::filesystem::path& folder) {
    std::error_code error;
    std::filesystem::directory_iterator iterator(folder, error), end;
    if (error) io("cannot enumerate " + folder.string());
    std::vector<std::filesystem::directory_entry> result;
    while (iterator != end) {
        result.push_back(*iterator);
        iterator.increment(error);
        if (error) io("cannot continue enumerating " + folder.string());
    }
    return result;
}
bool sameIdentity(const StoredRoutine& a, const StoredRoutine& b) {
    return a.schema == b.schema && a.name == b.name && a.inputTypeOids == b.inputTypeOids;
}
} // namespace

RoutineStore::RoutineStore(std::filesystem::path database, LegacyReader legacy)
    : database_(std::move(database)), legacy_(std::move(legacy)) {}

std::string RoutineStore::encode(const StoredRoutine& routine) {
    validate(routine);
    std::string bytes(magic);
    appendString(bytes, routine.schema);
    appendString(bytes, routine.name);
    appendNumber(bytes, routine.oid);
    appendNumber(bytes, static_cast<uint32_t>(routine.inputTypeOids.size()));
    for (size_t i = 0; i < routine.inputTypeOids.size(); ++i) {
        appendString(bytes, routine.parameterNames[i]);
        appendString(bytes, routine.parameterTypes[i]);
        appendNumber(bytes, routine.inputTypeOids[i]);
    }
    appendString(bytes, routine.returnType);
    appendNumber(bytes, routine.returnTypeOid);
    appendString(bytes, routine.language);
    appendString(bytes, routine.body);
    bytes += routine.volatility;
    bytes += static_cast<char>(routine.strict);
    const auto digest = SHA256::digestBytes(bytes);
    bytes.append(reinterpret_cast<const char*>(digest.data()), digest.size());
    return bytes;
}

StoredRoutine RoutineStore::decode(const std::string& bytes) {
    if (bytes.size() < magic.size()+32 || bytes.compare(0, magic.size(), magic) != 0)
        corrupt("unsupported version or truncated header");
    const size_t payload = bytes.size()-32;
    const auto digest = SHA256::digestBytes(bytes.substr(0, payload));
    if (!std::equal(digest.begin(), digest.end(),
                    reinterpret_cast<const unsigned char*>(bytes.data()+payload)))
        corrupt("checksum mismatch");
    Reader reader{bytes, magic.size(), payload};
    StoredRoutine result;
    result.schema = reader.string();
    result.name = reader.string();
    result.oid = reader.number();
    const auto count = reader.number();
    if (count > maxArguments) corrupt("invalid argument count");
    for (uint32_t i = 0; i < count; ++i) {
        result.parameterNames.push_back(reader.string());
        result.parameterTypes.push_back(reader.string());
        result.inputTypeOids.push_back(reader.number());
    }
    result.returnType = reader.string();
    result.returnTypeOid = reader.number();
    result.language = reader.string();
    result.body = reader.string();
    result.volatility = static_cast<char>(reader.byte());
    const auto strict = reader.byte();
    if (strict > 1 || reader.offset != reader.end) corrupt("invalid flags or trailing fields");
    result.strict = strict != 0;
    try { validate(result); }
    catch (const DbError& error) {
        if (error.sqlState() == "58030") throw;
        corrupt(error.what());
    }
    return result;
}

std::filesystem::path RoutineStore::path(const std::string& schema,
    const std::string& name, const std::vector<uint32_t>& inputs) const {
    identifier(schema);
    identifier(name);
    return database_/".funcs"/".v3"/hex(schema)/hex(name)/(SHA256::hash(signature(inputs))+".func");
}

std::optional<std::filesystem::path> RoutineStore::legacyPath(
    const std::string& schema, const std::string& name) const {
    const auto representable = [](const std::string& value) {
        return value != "." && value != ".." && value.find('/') == std::string::npos &&
               value.find('\\') == std::string::npos;
    };
    // Old producers rejected these path components. Do not invent a legacy
    // lookup for a newly representable, encoded V3 SQL identifier.
    if (!representable(schema) || !representable(name)) return std::nullopt;
    if (schema == "public") return database_/".funcs"/(name+".func");
    return database_/".funcs"/".namespaces"/schema/(name+".func");
}

std::optional<StoredRoutine> RoutineStore::legacy(const std::string& schema,
    const std::string& name) const {
    const auto file = legacyPath(schema, name);
    if (!file || !pathExists(*file)) return std::nullopt;
    if (!legacy_) corrupt("legacy declaration decoder is unavailable");
    auto value = legacy_(*file, schema, name);
    if (!value) corrupt("legacy declaration cannot be decoded");
    validate(*value);
    if (value->schema != schema || value->name != name)
        corrupt("legacy declaration identity mismatch");
    return value;
}

std::vector<StoredRoutine> RoutineStore::candidates(const std::string& schema,
    const std::string& name) const {
    identifier(schema);
    identifier(name);
    std::map<std::vector<uint32_t>, StoredRoutine> values;
    const auto folder = path(schema, name, {}).parent_path();
    if (pathExists(folder)) {
        directory(folder);
        for (const auto& entry : entries(folder)) {
            if (entry.path().extension() != ".func") continue;
            auto value = decode(read(entry.path()));
            if (value.schema != schema || value.name != name || path(schema, name, value.inputTypeOids) != entry.path())
                corrupt("stored identity and path disagree");
            if (!values.emplace(value.inputTypeOids, std::move(value)).second)
                corrupt("duplicate persisted signature");
        }
    }
    if (auto old = legacy(schema, name); old && !values.count(old->inputTypeOids))
        values.emplace(old->inputTypeOids, std::move(*old));
    std::vector<StoredRoutine> result;
    for (auto& pair : values) result.push_back(std::move(pair.second));
    return result;
}

std::optional<StoredRoutine> RoutineStore::find(const std::string& schema,
    const std::string& name, const std::vector<uint32_t>& inputs) const {
    // Validate the requested vector too, before any filesystem lookup.
    (void)signature(inputs);
    for (auto& candidate : candidates(schema, name))
        if (candidate.inputTypeOids == inputs) return std::move(candidate);
    return std::nullopt;
}

std::vector<std::string> RoutineStore::names(const std::string& schema) const {
    identifier(schema);
    std::set<std::string> result;
    const auto current = database_/".funcs"/".v3"/hex(schema);
    if (pathExists(current)) {
        directory(current);
        for (const auto& entry : entries(current)) {
            directory(entry.path());
            const auto name = unhex(entry.path().filename().string());
            if (!candidates(schema, name).empty()) result.insert(name);
        }
    }
    const auto example = legacyPath(schema, "placeholder");
    if (example && pathExists(example->parent_path())) {
        directory(example->parent_path());
        for (const auto& entry : entries(example->parent_path()))
            if (entry.path().extension() == ".func") {
                auto name = entry.path().stem().string();
                if (!candidates(schema, name).empty()) result.insert(std::move(name));
            }
    }
    return {result.begin(), result.end()};
}

StoredRoutine RoutineStore::put(StoredRoutine routine, bool replace) const {
    validate(routine);
    const auto prior = find(routine.schema, routine.name, routine.inputTypeOids);
    if (prior) {
        if (!replace) throw DbError("42723", "function with the same input types already exists");
        if (prior->returnTypeOid != routine.returnTypeOid)
            throw DbError("42P13", "cannot change return type of existing function");
        for (size_t i = 0; i < prior->parameterNames.size(); ++i)
            if (!prior->parameterNames[i].empty() && prior->parameterNames[i] != routine.parameterNames[i])
                throw DbError("42P13", "cannot change name of input parameter");
        if (routine.oid && prior->oid && routine.oid != prior->oid)
            corrupt("replacement changed routine OID");
        if (prior->oid) routine.oid = prior->oid;
    }
    directory(database_);
    const auto target = path(routine.schema, routine.name, routine.inputTypeOids);
    auto folder = database_;
    for (const auto& component : target.parent_path().lexically_relative(database_)) {
        folder /= component;
        ensureDirectory(folder);
    }
    const auto bytes = encode(routine);
    if (prior && pathExists(target)) {
        if (!sameIdentity(decode(read(target)), routine)) corrupt("signature path collision");
        if (!index_file::writeAtomically(target, bytes)) io("cannot replace routine");
    } else {
        const auto status = index_file::writeAtomicallyNoReplace(target, bytes);
        if (status == index_file::WriteResult::ALREADY_EXISTS)
            throw DbError("42723", "function with the same input types already exists");
        if (status != index_file::WriteResult::OK) io("cannot publish routine");
    }
    // An old flat declaration shadows neither a newly added different
    // overload nor the authoritative same-signature V3 generation. Retire it
    // only after the replacement is completely published.
    const auto old = legacy(routine.schema, routine.name);
    if (old && sameIdentity(*old, routine)) {
        const auto file = legacyPath(routine.schema, routine.name);
        if (!index_file::removeDurably(*file)) io("cannot retire legacy routine");
    }
    return routine;
}

bool RoutineStore::erase(const std::string& schema, const std::string& name,
    const std::vector<uint32_t>& inputs) const {
    const auto candidate = find(schema, name, inputs);
    if (!candidate) return false;
    // Retire the historical fallback first: a failed V3 unlink can leave a
    // complete still-live generation, but must not resurrect an older body.
    const auto old = legacy(schema, name);
    if (old && sameIdentity(*old, *candidate)) {
        if (!index_file::removeDurably(*legacyPath(schema, name)))
            io("cannot remove legacy routine");
    }
    const auto target = path(schema, name, inputs);
    if (pathExists(target) && !index_file::removeDurably(target)) io("cannot remove routine");
    return true;
}

StoredRoutine RoutineStore::move(const std::string& schema, const std::string& name,
    const std::vector<uint32_t>& inputs, const std::string& newSchema,
    const std::string& newName) const {
    auto value = find(schema, name, inputs);
    if (!value) throw DbError("42883", "function does not exist");
    identifier(newSchema);
    identifier(newName);
    if (schema == newSchema && name == newName) return *value;
    value->schema = newSchema;
    value->name = newName;
    auto published = put(std::move(*value));
    if (!erase(schema, name, inputs)) io("source routine disappeared during move");
    return published;
}

} // namespace dbms
