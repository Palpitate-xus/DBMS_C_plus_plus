"""Shared wire assertions for the focused numeric type review regressions."""
import importlib.util
from pathlib import Path
import socket
import struct
import sys


class Review:
    def __init__(self, label):
        spec = importlib.util.spec_from_file_location(label+'_runner', Path(__file__).with_name('pg_diff_runner.py'))
        self.runner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.runner)
        self.client = self.runner.load_protocol_client()
        self.server = None
        reference = sys.argv[1:] == ['--reference18']
        assert not sys.argv[1:] or reference
        if reference:
            host, port, user, database, password = self.runner._reference_connection_settings()
            self.sock = socket.create_connection((host, port), timeout=self.runner.wire_timeout())
            self.client.startup_reference(self.sock, user, database, password)
            self.runner.verify_reference_version(self.client, self.sock)
        else:
            self.server = self.runner.start_ours(self.client)
            self.sock = self.server['sock']
        self.label, self.controls, self.failures = label, 0, []

    def record(self, label, valid, actual, expected):
        self.controls += 1
        print(self.label, label, actual, 'expected', expected, 'match', valid, flush=True)
        if not valid:
            self.failures.append((label, actual, expected))

    def query(self, sql, rows=None, oids=None, state=None, tag=None):
        result = self.runner.decode_wire_result(self.client.simple_query(self.sock, sql), include_types=True)
        valid = result[1] == state
        if rows is not None:
            valid = valid and result[0] == rows and result[4] == (tag or 'SELECT '+str(len(rows)))
        if oids is not None:
            valid = valid and result[5] == oids
        if state:
            valid = valid and result[0] == [] and result[4] is None
        self.record(sql, valid, result, (rows, oids, state, tag))

    def describe(self, sql, oids=None, state=None, parameter_oids=()):
        name = ('numeric_review_'+str(self.controls)).encode()
        parse = name+b'\0'+sql.encode()+b'\0'+struct.pack('!H', len(parameter_oids))
        parse += b''.join(struct.pack('!I', oid) for oid in parameter_oids)
        self.sock.sendall(self.client.typed(b'P', parse)+self.client.typed(b'D', b'S'+name+b'\0')+self.client.typed(b'S'))
        messages = self.client.read_until_ready(self.sock)
        states = [self.client.diagnostic_fields(body).get(b'C', b'').decode() for kind, body in messages if kind == b'E']
        fields = self.client.row_description_fields(messages) if any(kind == b'T' for kind, _ in messages) else []
        valid = states == ([state] if state else []) and not any(kind in (b'D', b'C') for kind, _ in messages)
        if state:
            valid = valid and not any(kind == b'1' for kind, _ in messages)
        else:
            valid = valid and [field[3] for field in fields] == oids
            valid = valid and sum(kind == b'1' for kind, _ in messages) == 1
        kinds = [kind.decode('ascii') for kind, _ in messages]
        self.record('DESCRIBE '+sql, valid, ([field[3] for field in fields], states, kinds), (oids, state))

    def parameter(self, sql, inputs, input_oids, rows, output_oids):
        parse = b'\0'+sql.encode()+b'\0'+struct.pack('!H', len(input_oids))
        parse += b''.join(struct.pack('!I', oid) for oid in input_oids)
        bind = b'\0\0'+struct.pack('!HH', 0, len(inputs))
        for value in inputs:
            raw = None if value is None else value.encode()
            bind += struct.pack('!i', -1 if raw is None else len(raw))+(raw or b'')
        bind += struct.pack('!H', 0)
        self.sock.sendall(self.client.typed(b'P', parse)+self.client.typed(b'D', b'S\0')+
                          self.client.typed(b'B', bind)+self.client.typed(b'D', b'P\0')+
                          self.client.typed(b'E', b'\0'+struct.pack('!I', 0))+self.client.typed(b'S'))
        messages = self.client.read_until_ready(self.sock)
        result = self.runner.decode_wire_result(messages, include_types=True)
        descriptions = [self.client.row_description_fields([message]) for message in messages if message[0] == b'T']
        valid = result[1] is None and result[0] == rows and result[4] == 'SELECT '+str(len(rows))
        valid = valid and len(descriptions) == 2 and all([field[3] for field in fields] == output_oids for fields in descriptions)
        self.record(('PARAMETER', sql, inputs, input_oids), valid, result, (rows, output_oids))

    def complete(self, expected_controls):
        assert self.controls == expected_controls, self.controls
        print(self.label+'_COMPLETE', self.controls, 'FAILURES', len(self.failures), flush=True)
        assert not self.failures, self.failures

    def close(self):
        if self.server:
            self.runner.stop_ours(self.server)
        else:
            self.sock.close()
