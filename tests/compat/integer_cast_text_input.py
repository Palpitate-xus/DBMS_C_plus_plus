"""Shared integer CAST text grammar and target-width error precedence."""
import struct


def cases():
    # Last member overrides SQLSTATE by width where PG detects an early
    # accumulation overflow before it can inspect trailing invalid syntax.
    values = [
        (None, None, None), ('0', 0, None), ('010', 10, None), ('-0', 0, None),
        (' +0x_Ff \t', 255, None), ('-0b_1000_0000', -128, None), ('0O_10', 8, None),
        ('1_000', 1000, None), ('+0X7fff', 32767, None), ('-0x8000', -32768, None),
        ('32768', 32768, None), ('-32769', -32769, None),
        ('2147483647', 2147483647, None), ('-2147483648', -2147483648, None),
        ('2147483648', 2147483648, None), ('-2147483649', -2147483649, None),
        ('9223372036854775807', 9223372036854775807, None),
        ('-9223372036854775808', -9223372036854775808, None),
        ('0x7fff_ffff_ffff_ffff', 9223372036854775807, None),
        ('-0x8000_0000_0000_0000', -9223372036854775808, None),
        ('0x8000_0000_0000_0000', 9223372036854775808, None),
        ('', None, '22P02'), ('bad', None, '22P02'), ('1.2', None, '22P02'),
        ('+ 1', None, '22P02'), ('1__2', None, '22P02'), ('0x__f', None, '22P02'),
        ('0b2', None, '22P02'), ('0o8', None, '22P02'), ('0x', None, '22P02'),
        ('_1', None, '22P02'), ('1_', None, '22P02'), ('0xF_', None, '22P02'),
        ('32768bad', None, '22P02'),
        ('2147483648bad', None, {16:'22003',32:'22P02',64:'22P02'}),
        ('999999999999999999999bad', None, '22003'),
    ]
    assert len(values) == 36
    return values


def expected(item, bits):
    text, number, state = item
    if isinstance(state, dict): state = state[bits]
    if state: return None, state
    if number is None: return None, None
    if not -(1 << (bits-1)) <= number < (1 << (bits-1)): return None, '22003'
    return str(number), None


def run(client, runner, sock):
    controls = failures = 0
    for transport in ('simple', 'extended', 'parameter'):
        for target, bits, oid in [('smallint',16,21),('integer',32,23),('bigint',64,20)]:
            for source in ('unknown', 'text', 'varchar(64)'):
                for item in cases():
                    text = item[0]; value, state = expected(item, bits)
                    literal = 'NULL' if text is None else "'" + text.replace("'", "''") + "'"
                    if source != 'unknown': literal = 'CAST(' + literal + ' AS ' + source + ')'
                    sql = 'SELECT CAST(' + literal + ' AS ' + target + ') AS v'
                    if transport == 'simple': messages = client.simple_query(sock, sql)
                    else:
                        parameter = transport == 'parameter'
                        if parameter: sql = 'SELECT CAST($1 AS '+target+') AS v'
                        input_oid = 0 if source == 'unknown' else 25 if source == 'text' else 1043
                        parse = b'\0' + sql.encode() + b'\0' + struct.pack('!H', 1 if parameter else 0)
                        if parameter: parse += struct.pack('!I', input_oid)
                        bind = b'\0\0' + struct.pack('!HH', 0, 1 if parameter else 0)
                        if parameter:
                            raw = None if text is None else text.encode()
                            bind += struct.pack('!i', -1 if raw is None else len(raw)) + (raw or b'')
                        bind += struct.pack('!H', 0)
                        sock.sendall(client.typed(b'P', parse) + client.typed(b'B', bind) +
                            client.typed(b'D', b'P\0') + client.typed(b'E', b'\0'+struct.pack('!I',0)) + client.typed(b'S'))
                        messages = client.read_until_ready(sock)
                    rows, actual_state, message, _, tag, oids = runner.decode_wire_result(messages, include_types=True)
                    controls += 1
                    good = rows == ([] if state else [[value]]) and actual_state == state and messages[-1] == (b'Z', b'I')
                    good = good and (tag is None if state else tag == 'SELECT 1' and oids == [oid])
                    if not good:
                        failures += 1
                        print('INTEGER_CAST_TEXT_FAIL', transport, target, source, repr(text),
                              'expected', value, state, oid, 'actual', rows, actual_state, tag, oids, message, flush=True)
    print('INTEGER_CAST_TEXT_COMPLETE', controls, 'FAILURES', failures, flush=True)
    assert controls == 972 and failures == 0, (controls, failures)
    return controls
