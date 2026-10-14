#!/usr/bin/env python3
"""The restore-only polling exception must preserve every other failure."""
import importlib.util
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    'restore_poll_driver', Path(__file__).with_name('div14_feature_gate_test.py'))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


def error(state='55P03', message='cold catalog read conflicts with an active DDL owner'):
    return [(b'E', b'SERROR\0C' + state.encode() + b'\0M' + message.encode() + b'\0\0'),
            (b'Z', b'I')]


def success(oids=None):
    oids = oids or [20, 25, 25, 25, 25, 25, 20, 20, 20, 20, 25, 16]
    description = struct.pack('!h', len(oids))
    for index, oid in enumerate(oids):
        description += ('field_' + str(index)).encode() + b'\0'
        description += struct.pack('!IhIhih', 0, 0, oid, -1, -1, 0)
    values = [b'7', b'restore', b'info', b'image', b'succeeded', b'alice',
              b'1', b'2', b'3', b'4', None, b'f']
    row = struct.pack('!h', len(values))
    for value in values:
        row += struct.pack('!i', -1 if value is None else len(value))
        if value is not None:
            row += value
    return [(b'T', description), (b'D', row), (b'C', b'SHOW 1\0'), (b'Z', b'I')]


class RestorePollTest(unittest.TestCase):
    def test_only_restore_retries_then_checks_real_wire_result(self):
        with patch.object(driver, 'simple_query', side_effect=[error(), success()]) as query, \
                patch.object(driver.time, 'sleep'):
            row = driver.wait_for_maintenance(None, 7, retry_restore_publication=True)
        self.assertEqual(row[0], b'7')
        self.assertEqual(row[4], b'succeeded')
        self.assertEqual(query.call_count, 2)
        self.assertTrue(all(call.args == (None, 'SHOW MAINTENANCE JOB 7')
                            for call in query.call_args_list))

    def test_other_errors_and_non_restore_calls_fail(self):
        for messages, restore in [(error(), False), (error('58030'), True),
                                  (error(message='different lock error'), True),
                                  (error('3D000'), True)]:
            with self.subTest(messages=messages, restore=restore), \
                    patch.object(driver, 'simple_query', return_value=messages):
                with self.assertRaises(AssertionError):
                    driver.wait_for_maintenance(None, 7, retry_restore_publication=restore)

    def test_conflict_needs_idle_and_no_false_success(self):
        for messages in [error()[:-1], error()[:-1] + [(b'Z', b'E')],
                         error()[:-1] + [(b'C', b'SHOW 1\0'), (b'Z', b'I')]]:
            with self.subTest(messages=messages), \
                    patch.object(driver, 'simple_query', return_value=messages):
                with self.assertRaises(AssertionError):
                    driver.wait_for_maintenance(None, 7, retry_restore_publication=True)

    def test_conflict_does_not_restart_original_deadline(self):
        with patch.object(driver, 'simple_query', return_value=error()) as query, \
                patch.object(driver.time, 'time', side_effect=[0, 0, 31]), \
                patch.object(driver.time, 'sleep'):
            with self.assertRaisesRegex(AssertionError, 'did not reach'):
                driver.wait_for_maintenance(None, 7, timeout=30, retry_restore_publication=True)
        self.assertEqual(query.call_count, 1)

    def test_success_metadata_is_not_relaxed(self):
        with patch.object(driver, 'simple_query', return_value=success([25] * 12)):
            with self.assertRaises(AssertionError):
                driver.wait_for_maintenance(None, 7, retry_restore_publication=True)


if __name__ == '__main__':
    unittest.main()
