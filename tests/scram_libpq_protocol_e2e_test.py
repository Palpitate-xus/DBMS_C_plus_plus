#!/usr/bin/env python3
"""Verify startup identity and SCRAM proof checks with libpq's empty name."""

import socket
import struct

from compat.numeric_type_review import Review


def main():
    review = Review("SCRAM_LIBPQ")
    client = review.client
    if review.server is None:
        host, port, user, database, password = review.runner._reference_connection_settings()
    else:
        host, port, user, database, password = "127.0.0.1", review.server["port"], "alice", "info", "secret"

    def connect():
        return socket.create_connection((host, port), timeout=review.runner.wire_timeout())

    try:
        for name in ("", user):
            with connect() as sock:
                try:
                    client.startup(sock, user, database, password, scram_user=name,
                                   validate_dbms_status=review.server is not None)
                    reply = client.simple_query(sock, "SELECT current_user, current_database()")
                    rows = client.data_row_values(reply)
                    expected = [[user.encode(), database.encode()]]
                    review.record(("login and query", name), rows == expected and reply[-1] == (b"Z", b"I"),
                                  rows, expected)
                except AssertionError as failure:
                    review.record(("login and query", name), False, failure.args, "authenticated query")
            with connect() as sock:
                client.send_startup_packet(sock, user, database, "scram-login-test", 196608,
                                           {}, {}, False)
                try:
                    client.complete_startup_authentication(sock, user, password + "-incorrect",
                                                           scram_user=name, expect_bad_password=True)
                    closed = client.wait_for_disconnect(sock)
                    review.record(("wrong password", name), closed, closed, True)
                except AssertionError as failure:
                    review.record(("wrong password", name), False, failure.args, "28P01")

        if review.server is not None:
            # The empty-name allowance still requires the username attribute
            # and preserves the nonce and duplicate-attribute checks.
            for initial, state in (
                (b"n,,r=clientnonce", b"08P01"),
                (b"n,,n=,n=,r=clientnonce", b"08P01"),
                (b"n,,n=,r=", b"08P01"),
                (b"n,,n=another_user,r=clientnonce", b"28P01"),
            ):
                with connect() as sock:
                    client.send_startup_packet(sock, user, database, "scram-login-test", 196608,
                                               {}, {}, False)
                    kind, body = client.read_message(sock)
                    assert kind == b"R" and struct.unpack("!I", body[:4])[0] == 10, (kind, body)
                    sock.sendall(client.typed(b"p", b"SCRAM-SHA-256\0" +
                                              struct.pack("!i", len(initial)) + initial))
                    kind, body = client.read_message(sock)
                    valid = kind == b"E" and client.diagnostic_fields(body).get(b"C") == state
                    review.record(initial, valid and client.wait_for_disconnect(sock), (kind, body), state)
        review.complete(8 if review.server is not None else 4)
    finally:
        review.close()


if __name__ == "__main__":
    main()
