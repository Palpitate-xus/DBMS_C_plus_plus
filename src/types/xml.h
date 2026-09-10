#pragma once

#include <string>

namespace dbms {

enum class XmlParseMode {
    Content,
    Document,
};

struct XmlValidationResult {
    bool ok = false;
    std::string message;
};

// Validate an XML value without loading a DTD or resolving external entities.
// PostgreSQL's xml type defaults to CONTENT, while the document mode is used
// by xml_is_well_formed_document/xml_is_document.
XmlValidationResult validateXml(const std::string& input, XmlParseMode mode);

// Remove a leading XML declaration from an already validated value. This is
// used by XMLCONCAT, whose result cannot retain per-argument declarations.
std::string stripXmlDeclaration(const std::string& input);

}  // namespace dbms
