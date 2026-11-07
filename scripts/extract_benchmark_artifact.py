#!/usr/bin/env python3
"""Extract one embedded artifact without loading the complete evidence JSON.

The outer gzip envelope and one artifact are held in memory; the potentially
multi-gigabyte XZ JSON is decoded incrementally. Local ELF references are never
followed. Output is published only after the complete payload hash is checked.
"""
import argparse
import base64
import gzip
import hashlib
import io
import json
import lzma
from pathlib import Path, PurePosixPath
import re
import sys


class JsonStream:
    def __init__(self, source):
        self.source = source
        self.buffer = ""
        self.position = 0
        self.eof = False
        self.decoder = json.JSONDecoder()
        self.digest = hashlib.sha256()
        self.bytes = 0

    def fill(self):
        chunk = self.source.read(65536)
        if not chunk:
            self.eof = True
            return False
        self.digest.update(chunk)
        self.bytes += len(chunk)
        # Producers escape non-ASCII JSON text, including artifact paths.
        self.buffer = self.buffer[self.position:] + chunk.decode("ascii")
        self.position = 0
        return True

    def whitespace(self):
        while True:
            while self.position < len(self.buffer) and self.buffer[self.position].isspace():
                self.position += 1
            if self.position < len(self.buffer) or self.eof:
                return
            self.fill()

    def token(self, expected):
        self.whitespace()
        if self.buffer[self.position:self.position + 1] != expected:
            raise ValueError("incomplete or malformed evidence JSON")
        self.position += 1

    def value(self):
        self.whitespace()
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.position)
                # An integer can end at the current read boundary.
                if end == len(self.buffer) and not self.eof:
                    self.fill()
                    continue
                self.position = end
                return value
            except json.JSONDecodeError:
                if self.eof or not self.fill():
                    raise ValueError("incomplete or malformed evidence JSON") from None

    def entries(self):
        self.token("{")
        first = True
        seen = set()
        while True:
            self.whitespace()
            if self.buffer[self.position:self.position + 1] == "}":
                self.position += 1
                return
            if not first:
                self.token(",")
            key = self.value()
            if not isinstance(key, str) or key in seen:
                raise ValueError("invalid or duplicate evidence key")
            seen.add(key)
            self.token(":")
            yield key
            first = False

    def base64_value(self, collect):
        self.token('"')
        chunks = []
        while True:
            end = self.buffer.find('"', self.position)
            stop = len(self.buffer) if end < 0 else end
            piece = self.buffer[self.position:stop]
            if re.fullmatch(r"[A-Za-z0-9+/=]*", piece) is None:
                raise ValueError("invalid base64 JSON string")
            if collect:
                chunks.append(piece)
            self.position = stop
            if end >= 0:
                self.position += 1
                return "".join(chunks) if collect else None
            if self.eof or not self.fill():
                raise ValueError("incomplete base64 JSON string")

    def finish(self):
        self.whitespace()
        if not self.eof or self.position != len(self.buffer):
            raise ValueError("trailing evidence content")


def safe_name(name):
    path = PurePosixPath(name)
    return bool(name) and not path.is_absolute() and ".." not in path.parts and str(path) == name


def extract(archive, name):
    if not safe_name(name):
        raise ValueError("artifact must have a canonical relative path")
    with gzip.open(archive, "rb") as source:
        envelope = json.load(source)
    packed = base64.b64decode(envelope["payload_xz_base64"], validate=True)
    if hashlib.sha256(packed).hexdigest() != envelope["payload_xz_sha256"]:
        raise ValueError("XZ payload checksum mismatch")
    if len(packed) != envelope["payload_xz_bytes"]:
        raise ValueError("XZ payload size mismatch")
    artifact = result = None
    with lzma.LZMAFile(io.BytesIO(packed), "rb") as source:
        stream = JsonStream(source)
        for key in stream.entries():
            if key == "artifacts":
                for entry in stream.entries():
                    metadata = stream.value()
                    if entry == name:
                        artifact = metadata
            elif key == "blobs":
                for digest in stream.entries():
                    wanted = artifact is not None and digest == artifact.get("blob")
                    encoded = stream.base64_value(wanted)
                    if wanted:
                        result = base64.b64decode(encoded, validate=True)
            else:
                stream.value()
        stream.finish()
    if stream.bytes != envelope["payload_raw_bytes"]:
        raise ValueError("complete JSON payload size mismatch")
    if stream.digest.hexdigest() != envelope["payload_raw_sha256"]:
        raise ValueError("complete JSON payload checksum mismatch")
    if artifact is None or artifact.get("type") != "F" or result is None:
        raise ValueError("artifact is not an embedded regular file")
    if len(result) != artifact["bytes"] or hashlib.sha256(result).hexdigest() != artifact["sha256"]:
        raise ValueError("artifact size or checksum mismatch")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("artifact")
    parser.add_argument("--output", type=Path, help="new output file; existing files are refused")
    args = parser.parse_args()
    try:
        value = extract(args.archive, args.artifact)
        if args.output is None:
            sys.stdout.buffer.write(value)
        else:
            with args.output.open("xb") as out:
                out.write(value)
    except (ValueError, KeyError, OSError, EOFError, lzma.LZMAError) as error:
        parser.exit(1, "extraction failed: " + str(error) + "\n")


if __name__ == "__main__":
    main()
