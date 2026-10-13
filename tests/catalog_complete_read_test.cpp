#include "catalog/catalog.h"
#include "common/DbError.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <unistd.h>

using namespace dbms;
namespace fs = std::filesystem;

static int failures = 0;
static int checks = 0;
static void check(bool ok, const std::string& label) {
    ++checks;
    if (!ok) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}

static std::string bytes(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

static void replace(const fs::path& path, const std::string& value) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(value.data(), value.size());
    out.close();
    check(out.good(), "write local fixture " + path.filename().string());
}

// Split at a real CSV delimiter, retaining quoted commas and escaped quotes.
static size_t delimiter(const std::string& line, unsigned wanted) {
    bool quoted = false;
    unsigned seen = 0;
    for (size_t i = 0; i < line.size(); ++i) {
        if (quoted && line[i] == '\\') { ++i; continue; }
        if (line[i] == '"') quoted = !quoted;
        if (!quoted && line[i] == ',' && ++seen == wanted) return i;
    }
    return std::string::npos;
}

static Oid seed(CatalogManager& catalog) {
    const Oid ns = catalog.createNamespace("public", 1);
    PgClassRow relation;
    relation.relname = "rows,with\\quote\"";
    relation.relnamespace = ns;
    relation.relnatts = 1;
    relation.relpages = 7;
    const Oid oid = catalog.createClass(relation);
    PgAttributeRow attribute;
    attribute.attrelid = oid;
    attribute.attname = "id,with\\quote\"";
    attribute.atttypid = 23;
    attribute.attnum = 1;
    attribute.attlen = 4;
    attribute.attbyval = true;
    catalog.addAttribute(attribute);
    check(catalog.persistAll(), "seed persist");
    return oid;
}

static void incomplete(const fs::path& directory, const std::string& image,
                       unsigned cutField) {
    CatalogManager catalog(directory.string());
    const Oid relation = seed(catalog);
    const fs::path path = directory / ("pg_" + image + ".cat");
    const std::string original = bytes(path);
    const size_t cut = delimiter(original, cutField);
    check(cut != std::string::npos, image + " source delimiter");
    const std::string torn = original.substr(0, cut + 1);
    replace(path, torn);
    bool rejected = false;
    try { catalog.loadAll(); }
    catch (const DbError& error) { rejected = error.sqlState() == "XX001"; }
    check(rejected, image + " incomplete record rejected");
    const auto classes = catalog.listClasses();
    const auto attributes = catalog.findAttributes(relation);
    check(classes.size() == 1 && classes[0].oid == relation &&
          classes[0].relpages == 7 && classes[0].relname == "rows,with\\quote\"",
          image + " previous class cache preserved");
    check(attributes.size() == 1 && attributes[0].attname == "id,with\\quote\"" &&
          attributes[0].attbyval, image + " previous attribute cache preserved");
    check(bytes(path) == torn, image + " load does not change disk");
    bool freshRejected = false;
    try { (void)CatalogManager::readMetadataSnapshot(directory.string()); }
    catch (const DbError& error) { freshRejected = error.sqlState() == "XX001"; }
    check(freshRejected, image + " fresh read-only snapshot rejects torn image");
    check(bytes(path) == torn, image + " snapshot does not change disk");
    check(!catalog.persistAll(), image + " cannot publish through rejected read");
    check(bytes(path) == torn, image + " failed persist preserves disk");
    replace(path, original);
    catalog.loadAll();
    check(catalog.persistAll(), image + " repaired file permits healthy retry");
    check(catalog.listClasses().size() == 1 &&
          catalog.findAttributes(relation).size() == 1,
          image + " repaired complete metadata");
}

static void directoryRead(const fs::path& directory, const std::string& image) {
    CatalogManager catalog(directory.string());
    const Oid relation = seed(catalog);
    const fs::path path = directory / ("pg_" + image + ".cat");
    const fs::path saved = directory / (image + ".saved");
    fs::rename(path, saved);
    fs::create_directory(path);
    bool rejected = false;
    try { catalog.loadAll(); }
    catch (const DbError& error) { rejected = error.sqlState() == "58030"; }
    check(rejected, image + " directory is not empty metadata");
    bool freshRejected = false;
    try { (void)CatalogManager::readMetadataSnapshot(directory.string()); }
    catch (const DbError& error) { freshRejected = error.sqlState() == "58030"; }
    check(freshRejected, image + " fresh snapshot rejects directory image");
    check(catalog.listClasses().size() == 1 &&
          catalog.findAttributes(relation).size() == 1,
          image + " directory failure preserves previous cache");
    check(!catalog.persistAll() && fs::is_directory(path),
          image + " failed persist preserves directory");
    fs::remove(path);
    fs::rename(saved, path);
    catalog.loadAll();
    check(catalog.persistAll(), image + " directory recovery retry");
}

static void legacy(const fs::path& directory) {
    CatalogManager catalog(directory.string());
    const Oid relation = seed(catalog);
    const fs::path classPath = directory / "pg_class.cat";
    const fs::path attributePath = directory / "pg_attribute.cat";
    const std::string currentClass = bytes(classPath);
    const std::string currentAttribute = bytes(attributePath);
    replace(classPath, currentClass.substr(0, delimiter(currentClass, 12)) + "\n");
    replace(attributePath, currentAttribute.substr(0, delimiter(currentAttribute, 10)) + "\n");
    catalog.loadAll();
    const auto classes = catalog.listClasses();
    const auto attributes = catalog.findAttributes(relation);
    check(classes.size() == 1 && classes[0].relname == "rows,with\\quote\"" &&
          classes[0].relpages == 0, "legacy class prefix and quoted name");
    check(attributes.size() == 1 && attributes[0].attname == "id,with\\quote\"" &&
          !attributes[0].attbyval, "legacy attribute prefix and quoted name");
    check(catalog.persistAll(), "legacy normalized publication");
    catalog.loadAll();
    check(catalog.listClasses().size() == 1 &&
          catalog.findAttributes(relation).size() == 1,
          "normalized complete reload");
}

int main() {
    char pattern[] = "/tmp/dbms-catalog-complete-XXXXXX";
    char* created = ::mkdtemp(pattern);
    if (!created) return 2;
    const fs::path root(created);
    // Every delimiter in the actual persisted formats is a possible torn
    // publication boundary. Exercise all prefix and extension boundaries.
    for (unsigned field = 1; field <= 29; ++field)
        incomplete(root / ("class-field-" + std::to_string(field)), "class", field);
    for (unsigned field = 1; field <= 19; ++field)
        incomplete(root / ("attribute-field-" + std::to_string(field)), "attribute", field);
    directoryRead(root / "class-directory", "class");
    directoryRead(root / "attribute-directory", "attribute");
    legacy(root / "legacy");
    fs::remove_all(root);
    std::cout << "catalog complete read checks=" << checks
              << " failures=" << failures << '\n';
    return failures == 0 ? 0 : 1;
}
