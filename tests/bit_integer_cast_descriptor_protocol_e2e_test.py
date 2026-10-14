#!/usr/bin/env python3
"""Retain all 172 original BIT cast/descriptor known-gap assertions."""
import runpy
from pathlib import Path


if __name__ == '__main__':
    runpy.run_path(str(Path(__file__).resolve().parent / 'compat' /
                       'bit_integer_cast_descriptor_known_gap.py'), run_name='__main__')
