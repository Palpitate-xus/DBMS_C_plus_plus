#pragma once

#include "commands/StoredReferenceOutputRole.h"
#include "expression/StoredRegtypeValue.h"
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace dbms {

// Final text output for fields whose producer retains genuine stored OIDs.
// The caller must keep the snapshot and session alive and unchanged for this
// renderer's query lifetime. A new query/context needs a new renderer/cache.
// This owns only names; it never changes raw source rows, result declarations,
// object identities or output-format policy.
class StoredReferenceOutputRenderer {
public:
    StoredReferenceOutputRenderer(const CatalogManager::MetadataSnapshot& catalog,
                                  const Session* session)
        : catalog_(catalog), session_(session) {}

    // None means the producer has already presented this field, or that it is
    // not a stored reference. Neither its spelling nor its result type can
    // establish a raw reference role. SQL NULL bypasses even malformed bytes.
    // Stage the entire row so a later decoding/catalog error publishes nothing.
    void render(std::vector<std::string>& cells, const std::vector<bool>& nulls,
                const std::vector<StoredReferenceOutputRole>& roles) {
        if (cells.size() != nulls.size() || cells.size() != roles.size())
            throw DbError("XX000", "stored reference output width mismatch");
        bool hasReference = false;
        for (size_t i = 0; i < cells.size(); ++i)
            if (!nulls[i] && roles[i].kind != StoredReferenceOutputRole::Kind::None) {
                hasReference = true;
                break;
            }
        if (!hasReference) return;

        auto output = cells;
        for (size_t i = 0; i < output.size(); ++i) {
            if (nulls[i]) continue;
            using Kind = StoredReferenceOutputRole::Kind;
            switch (roles[i].kind) {
            case Kind::None:
                break;
            case Kind::Regtype:
                output[i] = renderOid(cells[i], Codec::Regtype);
                break;
            case Kind::Regclass:
                output[i] = renderOid(cells[i], Codec::Regclass);
                break;
            case Kind::RegtypeArray:
                output[i] = renderArray(cells[i], Codec::Regtype);
                break;
            case Kind::RegclassArray:
                output[i] = renderArray(cells[i], Codec::Regclass);
                break;
            default:
                throw DbError("XX000", "invalid stored reference output role");
            }
        }
        cells.swap(output);
    }

private:
    enum class Codec { Regtype, Regclass };
    const CatalogManager::MetadataSnapshot& catalog_;
    const Session* session_;
    std::map<std::pair<Codec, Oid>, std::string> names_;

    const std::string& renderOid(const std::string& raw, Codec codec) {
        const Oid oid = stored_regtype_detail::rawOid(raw);
        const auto key = std::make_pair(codec, oid);
        if (const auto found = names_.find(key); found != names_.end()) return found->second;
        auto name = stored_regtype_detail::outputOid(
            oid, catalog_, session_, codec == Codec::Regclass ? "regclass" : "regtype");
        return names_.emplace(key, std::move(name)).first->second;
    }

    std::string renderArray(const std::string& raw, Codec codec) {
        return stored_regtype_detail::mapArray(raw, [&](const std::string& token) -> std::string {
            return renderOid(token, codec);
        });
    }
};

} // namespace dbms
