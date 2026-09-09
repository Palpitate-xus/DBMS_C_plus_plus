#!/usr/bin/env python3
"""Lock the complete generic object fallback registry to fail-closed handlers."""

from pathlib import Path
import re


REPO = Path(__file__).resolve().parents[1]
SOURCE = (REPO / "src/main.cpp").read_text(encoding="utf-8")

EXPECTED = {
    "compatCreatePrefixes": (
        "text search configuration", "text search dictionary",
        "text search template", "text search parser", "foreign data wrapper",
        "operator family", "operator class", "event trigger", "access method",
        "foreign table", "user mapping", "publication", "subscription",
        "extension", "assertion", "aggregate", "transform", "operator",
        "language", "server", "rule",
    ),
    "compatAlterDropPrefixes": (
        "text search configuration", "text search dictionary",
        "text search template", "text search parser", "foreign data wrapper",
        "materialized view", "operator family", "operator class",
        "event trigger", "access method", "foreign table", "large object",
        "user mapping", "publication", "subscription", "conversion",
        "collation", "extension", "assertion", "aggregate", "transform",
        "operator", "language", "function", "procedure", "routine",
        "database", "sequence", "trigger", "domain", "policy", "server",
        "index", "group", "type", "rule",
    ),
    "compatDropPrefixes": (
        "text search configuration", "text search dictionary",
        "text search template", "text search parser", "foreign data wrapper",
        "operator family", "operator class", "event trigger", "access method",
        "foreign table", "large object", "user mapping", "publication",
        "subscription", "extension", "assertion", "aggregate", "transform",
        "operator", "language", "routine", "server", "group", "rule",
    ),
}


def initializer(name):
    start = SOURCE.index(
        "static const vector<CompatObjectPrefix>& %s()" % name)
    end = SOURCE.index("return prefixes;", start)
    return SOURCE[start:end]


def function_body(name):
    start = SOURCE.index("static bool %s(" % name)
    brace = SOURCE.index("{", start)
    depth = 0
    for position in range(brace, len(SOURCE)):
        if SOURCE[position] == "{":
            depth += 1
        elif SOURCE[position] == "}":
            depth -= 1
            if depth == 0:
                return SOURCE[brace:position + 1]
    raise AssertionError("unterminated function %s" % name)


def main():
    for name, expected_phrases in EXPECTED.items():
        pairs = re.findall(r'\{"([^"]+)", "([^"]+)"\}', initializer(name))
        phrases = tuple(phrase for phrase, _kind in pairs)
        assert phrases == expected_phrases, "%s registry changed: %r" % (name, phrases)
        assert len({kind for _phrase, kind in pairs}) == len(pairs), \
            "%s has duplicate kinds" % name
        # Prefix matching must prefer a longer phrase whenever two phrases
        # can overlap; otherwise a newly added specific object becomes dead.
        for earlier, later in zip(phrases, phrases[1:]):
            assert not later.startswith(earlier + " "), \
                "%s must precede %s" % (later, earlier)

    for name, verb in (
            ("handleCreateCompatObject", "CREATE"),
            ("handleAlterCompatObject", "ALTER"),
            ("handleDropCompatObject", "DROP")):
        body = function_body(name)
        assert body.count("featureNotSupportedError") == 1, name
        assert 'string("%s ") + phrase' % verb in body, name
        for forbidden in ("saveCompat", "loadCompat", " created", " altered",
                          " dropped", "checkAdmin", "checkDB"):
            assert forbidden not in body, "%s contains %s" % (name, forbidden)

    print("[COMPAT FALLBACK] complete registry routes only to 0A000")


if __name__ == "__main__":
    main()
