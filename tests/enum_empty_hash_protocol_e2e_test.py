#!/usr/bin/env python3
"""Run every empty-label SQL/NULL/rollback/cold check with a real HASH index."""
import sys
import enum_empty_label_protocol_e2e_test as regression

if __name__ == "__main__":
    sys.argv.append("--hash")
    regression.main()
