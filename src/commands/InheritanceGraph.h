#pragma once

#include "common/DbError.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace dbms::inheritance_graph {

struct Link {
    std::string_view parent;
    std::string_view child;
    int32_t sequence = 0;
};

inline Link parse(std::string_view line) {
    const auto separator = line.find('|');
    if (separator == line.npos) throw DbError("XX001", "invalid inheritance link");
    Link link{line.substr(0, separator), line.substr(separator + 1)};
    const auto sequence = link.child.find('|');
    if (sequence != line.npos) {
        const auto input = link.child.substr(sequence + 1);
        link.child = link.child.substr(0, sequence);
        const auto parsed = std::from_chars(input.data(), input.data() + input.size(), link.sequence);
        if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() || link.sequence <= 0)
            throw DbError("XX001", "invalid inheritance parent sequence");
    }
    if (link.parent.empty() || link.child.empty() || link.parent == link.child)
        throw DbError("XX001", "invalid inheritance endpoints");
    return link;
}

struct Edge {
    std::string parent;
    std::string child;
    int32_t sequence;
};

struct Image {
    std::string bytes;
    std::vector<Edge> edges;
};

inline int32_t nextSequence(int32_t current) {
    if (current == std::numeric_limits<int32_t>::max())
        throw DbError("54000", "inheritance parent sequence exceeds integer range");
    return current + 1;
}

inline Image read(const std::filesystem::path& path) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory) return {};
    if (error || !std::filesystem::is_regular_file(status))
        throw DbError("58030", "could not inspect inheritance metadata");
    std::ifstream input(path, std::ios::binary);
    if (!input) throw DbError("58030", "could not read inheritance metadata");
    Image image;
    image.bytes.assign(std::istreambuf_iterator<char>(input), {});
    if (input.bad()) throw DbError("58030", "could not finish reading inheritance metadata");
    std::map<std::string_view, int32_t> maximum;
    std::set<std::pair<std::string_view, std::string_view>> pairs;
    std::set<std::pair<std::string_view, int32_t>> positions;
    std::string_view lines(image.bytes);
    while (!lines.empty()) {
        const auto end = lines.find('\n');
        const auto line = lines.substr(0, end);
        lines.remove_prefix(end == lines.npos ? lines.size() : end + 1);
        if (line.empty()) continue;
        const auto [parent, child, storedSequence] = parse(line);
        auto& highest = maximum[child];
        const auto sequence = storedSequence ? storedSequence : nextSequence(highest);
        if (!pairs.emplace(parent, child).second || !positions.emplace(child, sequence).second)
            throw DbError("XX001", "duplicate inheritance metadata");
        highest = std::max(highest, sequence);
        image.edges.push_back({std::string(parent), std::string(child), sequence});
    }
    return image;
}

inline std::string serialize(const std::vector<Edge>& edges) {
    std::string bytes;
    std::map<std::string_view, int32_t> maximum;
    for (const auto& edge : edges) {
        bytes += edge.parent + '|' + edge.child;
        auto& highest = maximum[edge.child];
        // Keep the two-field format when the ordinal can be recovered from
        // file order. Gaps left by NO INHERIT require a stored ordinal.
        if (edge.sequence != static_cast<int64_t>(highest) + 1)
            bytes += '|' + std::to_string(edge.sequence);
        highest = std::max(highest, edge.sequence);
        bytes += '\n';
    }
    return bytes;
}

} // namespace dbms::inheritance_graph
