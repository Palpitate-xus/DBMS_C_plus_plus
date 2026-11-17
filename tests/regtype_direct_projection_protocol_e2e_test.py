#!/usr/bin/env python3
"""Strict ordinary REGTYPE projection regression: the original 86 checks."""
from compat.regtype_projection_protocol import run, steps_for


def steps(prefix):
    return steps_for(prefix, include_inheritance=False)


def main():
    run(steps, entry_file=__file__, cohort='projection86', expected_checks=86)


if __name__ == '__main__':
    main()
