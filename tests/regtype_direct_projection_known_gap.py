#!/usr/bin/env python3
"""All 108 strict checks, including unsupported domains over REGTYPE arrays.

This diagnostic must fail while that support is absent. It has no allowlist.
"""
from compat.regtype_projection_protocol import run, steps_for


def steps(prefix):
    return steps_for(prefix, include_inheritance=True)


def main():
    run(steps, entry_file=__file__, cohort='projection_and_inheritance108', expected_checks=108)


if __name__ == '__main__':
    main()
