#!/usr/bin/env python

import sys
import unittest
import sol_test


if __name__ == '__main__':
    broker = sol_test.start_broker()
    try:
        tests = unittest.TestLoader().discover(
            'tests/integration/',
            pattern='test*.py'
        )
        result = unittest.TextTestRunner(verbosity=2).run(tests)
    finally:
        sol_test.kill_broker(broker)
    # CTest and CI read the exit code: a failing test must not look green
    sys.exit(0 if result.wasSuccessful() else 1)
