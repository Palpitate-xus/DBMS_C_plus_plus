#!/usr/bin/env python3
"""All 40 strict checks, including writing view results into stored columns.

This diagnostic must fail while that support is absent. It has no allowlist.
"""
from compat.regtype_projection_protocol import run
from regtype_materialization_protocol_e2e_test import steps as materialization_steps


def steps(prefix):
    return materialization_steps(prefix, include_view_write=True)


def main():
    run(steps, entry_file=__file__, cohort='regtype_materialization_with_view_write40', expected_checks=40)


if __name__ == '__main__':
    main()
