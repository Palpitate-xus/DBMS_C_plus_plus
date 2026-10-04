#!/usr/bin/env python3
"""A stalled partial TLS record during COPY FROM obeys statement_timeout."""

import importlib.util
import os
from pathlib import Path
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import time


class MemoryBioSocket:
    """Small socket facade for the existing PostgreSQL protocol test client."""

    def __init__(self, raw):
        self.raw = raw
        self.incoming = ssl.MemoryBIO()
        self.outgoing = ssl.MemoryBIO()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self.tls = context.wrap_bio(
            self.incoming, self.outgoing, server_hostname="localhost")
        self._handshake()

    def _flush(self):
        while self.outgoing.pending:
            self.raw.sendall(self.outgoing.read())

    def _receive_ciphertext(self):
        data = self.raw.recv(65536)
        if not data:
            self.incoming.write_eof()
            return
        self.incoming.write(data)

    def _handshake(self):
        while True:
            try:
                self.tls.do_handshake()
                self._flush()
                return
            except ssl.SSLWantReadError:
                self._flush()
                self._receive_ciphertext()
            except ssl.SSLWantWriteError:
                self._flush()

    def sendall(self, data):
        view = memoryview(data)
        while view:
            try:
                count = self.tls.write(view)
                view = view[count:]
                self._flush()
            except ssl.SSLWantReadError:
                self._flush()
                self._receive_ciphertext()
            except ssl.SSLWantWriteError:
                self._flush()

    def recv(self, size):
        while True:
            try:
                return self.tls.read(size)
            except ssl.SSLWantReadError:
                self._flush()
                self._receive_ciphertext()
            except ssl.SSLWantWriteError:
                self._flush()
            except (ssl.SSLZeroReturnError, ssl.SSLEOFError):
                return b""

    def send_partial_tls_record(self, application_data, prefix_bytes):
        # Leave no unrelated ciphertext in the BIO, then encrypt a complete
        # PostgreSQL message but send only the TLS record header/prefix.
        self._flush()
        view = memoryview(application_data)
        while view:
            count = self.tls.write(view)
            view = view[count:]
        encrypted = self.outgoing.read()
        assert len(encrypted) > prefix_bytes, len(encrypted)
        self.raw.sendall(encrypted[:prefix_bytes])


def load_protocol_client(root):
    spec = importlib.util.spec_from_file_location(
        "tls_timeout_protocol_client", root / "tests" / "postgres_protocol_test.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    root = Path(__file__).resolve().parent.parent
    client = load_protocol_client(root)
    ldd = subprocess.run(
        ["ldd", client.DBMS_MAIN], capture_output=True, text=True)
    if ldd.returncode != 0 or "libssl" not in ldd.stdout:
        print("[STATEMENT TIMEOUT TLS E2E] skipped (binary has no OpenSSL support)")
        return

    openssl = shutil.which("openssl")
    if openssl is None:
        raise RuntimeError("TLS-enabled binary requires openssl for test certificates")

    work_dir = tempfile.mkdtemp(prefix="dbms-tls-timeout-")
    process = None
    raw = None
    try:
        info_dir = os.path.join(work_dir, "info")
        os.mkdir(info_dir)
        Path(info_dir, "tlist.lst").touch()
        client.write_auth_catalog(work_dir, "alice", "secret")
        Path(work_dir, "pg_hba.conf").write_text(
            "host all alice 127.0.0.1/32 scram-sha-256\n", encoding="utf-8")

        certificate = os.path.join(work_dir, "server.crt")
        private_key = os.path.join(work_dir, "server.key")
        subprocess.run(
            [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", private_key, "-out", certificate, "-days", "1",
             "-subj", "/CN=localhost"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        environment = os.environ.copy()
        environment["DBMS_TLS_CERT"] = certificate
        environment["DBMS_TLS_KEY"] = private_key
        openssl_lib_dir = os.path.realpath(
            os.path.join(os.path.dirname(openssl), "..", "lib"))
        inherited_library_path = environment.get("LD_LIBRARY_PATH", "")
        environment["LD_LIBRARY_PATH"] = os.pathsep.join(
            path for path in (openssl_lib_dir, inherited_library_path) if path)
        process = subprocess.Popen(
            [client.DBMS_MAIN, "--data-dir", work_dir,
             "--server", str(port)],
            cwd=work_dir, env=environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        startup_deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                diagnostic = process.stderr.read().decode(errors="replace")
                raise RuntimeError(
                    "TLS server exited before accepting clients: " + diagnostic)
            try:
                raw = socket.create_connection(
                    ("127.0.0.1", port), timeout=1)
                break
            except OSError:
                if time.monotonic() >= startup_deadline:
                    raise RuntimeError("TLS server did not start")
                time.sleep(0.05)

        raw.settimeout(5)
        raw.sendall(struct.pack("!II", 8, 80877103))  # PostgreSQL SSLRequest
        assert raw.recv(1) == b"S"
        sock = MemoryBioSocket(raw)
        client.startup(sock, "alice", "info")

        def query(sql):
            sock.sendall(client.typed(b"Q", sql.encode() + b"\0"))
            return client.read_until_ready(sock)

        query("CREATE TABLE tls_copy_timeout (id INTEGER, value TEXT)")
        query("SET statement_timeout = 250")

        idle_started = time.monotonic()
        sock.sendall(client.typed(
            b"Q", b"COPY tls_copy_timeout FROM STDIN\0"))
        kind, _ = client.read_message(sock)
        assert kind == b"G", kind
        kind, body = client.read_message(sock)
        idle_elapsed = time.monotonic() - idle_started
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        assert client.read_message(sock) == (b"Z", b"I")
        assert idle_elapsed < 3, f"idle TLS COPY timeout took {idle_elapsed:.3f}s"

        started = time.monotonic()
        sock.sendall(client.typed(
            b"Q", b"COPY tls_copy_timeout FROM STDIN\0"))
        kind, _ = client.read_message(sock)
        assert kind == b"G", kind
        sock.send_partial_tls_record(
            client.typed(b"d", b"1\tpartial\n"), prefix_bytes=5)

        kind, body = client.read_message(sock)
        elapsed = time.monotonic() - started
        assert kind == b"E" and b"C57014\0" in body, (kind, body)
        assert elapsed < 3, f"partial TLS record timeout took {elapsed:.3f}s"
        assert sock.recv(1) == b"", "partial TLS record must close the connection"
        print("[STATEMENT TIMEOUT TLS E2E] partial TLS record timed out and closed")
    finally:
        if raw is not None:
            raw.close()
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            if process.stderr is not None:
                process.stderr.close()
        resolved = os.path.realpath(work_dir)
        if (os.path.dirname(resolved) == os.path.realpath(tempfile.gettempdir()) and
                os.path.basename(resolved).startswith("dbms-tls-timeout-")):
            shutil.rmtree(resolved, ignore_errors=True)


if __name__ == "__main__":
    main()
