#!/usr/bin/env python3
"""Verify benchmark archive parts and extract one artifact with bounded memory."""
import argparse
import base64
import gzip
import hashlib
import json
import lzma
from pathlib import Path
import sys
import tempfile

from extract_benchmark_artifact import extract, safe_name


def extract_parts(index_path, artifact):
    with gzip.open(index_path, "rb") as source:
        index = json.load(source)
    if index.get("schema") != "benchmark-gzip-parts-v1":
        raise ValueError("unsupported multipart benchmark index")
    parts = index["parts"]
    if not parts or len(parts) > 4096:
        raise ValueError("invalid archive part count")
    digest = hashlib.sha256()
    total = 0
    names = set()
    with tempfile.TemporaryFile() as joined:
        for ordinal, part in enumerate(parts):
            name = part["file"]
            if not safe_name(name) or len(Path(name).parts) != 1 or name in names:
                raise ValueError("invalid archive part filename")
            names.add(name)
            path = index_path.parent / name
            if path.is_symlink() or not path.is_file():
                raise ValueError("archive part must be a regular file")
            if path.stat().st_size != part["bytes"] or path.stat().st_size >= 49 * 1048576:
                raise ValueError("archive part size mismatch")
            compressed = path.read_bytes()
            if hashlib.sha256(compressed).hexdigest() != part["sha256"]:
                raise ValueError("archive part checksum mismatch")
            segment = json.loads(gzip.decompress(compressed))
            if (segment.get("schema") != "benchmark-gzip-segment-v1" or
                    segment["ordinal"] != ordinal or segment["offset"] != total):
                raise ValueError("archive part order mismatch")
            data = base64.b64decode(segment["segment_base64"], validate=True)
            if (len(data) != segment["segment_bytes"] or
                    len(data) != part["segment_bytes"] or
                    len(data) > 32 * 1048576 or
                    hashlib.sha256(data).hexdigest() != segment["segment_sha256"] or
                    segment["segment_sha256"] != part["segment_sha256"]):
                raise ValueError("archive segment size or checksum mismatch")
            joined.write(data)
            digest.update(data)
            total += len(data)
        if total != index["archive_bytes"] or digest.hexdigest() != index["archive_sha256"]:
            raise ValueError("complete reassembled archive checksum mismatch")
        joined.seek(0)
        return extract(joined, artifact)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("index", type=Path)
    parser.add_argument("artifact")
    parser.add_argument("--output", type=Path, help="new output file; existing files are refused")
    args = parser.parse_args()
    try:
        if args.output is not None and (args.output.exists() or args.output.is_symlink()):
            raise FileExistsError("output already exists")
        value = extract_parts(args.index, args.artifact)
        if args.output is None:
            sys.stdout.buffer.write(value)
        else:
            with args.output.open("xb") as destination:
                destination.write(value)
    except (ValueError, KeyError, TypeError, OSError, EOFError, lzma.LZMAError) as error:
        parser.exit(1, "extraction failed: " + str(error) + "\n")


if __name__ == "__main__":
    main()
