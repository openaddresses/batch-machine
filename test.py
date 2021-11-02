# coding=utf8
"""
Run Python test suite via the standard unittest mechanism.
Usage:
  python test.py
  python test.py --logall
  python test.py TestConformTransforms
  python test.py -l TestOA.test_process
All logging is suppressed unless --logall or -l specified
~/.openaddr-logging-test.json can also be used to configure log behavior
"""
import unittest
import sys, os
import logging

from openaddr.tests import TestOA, TestState, TestPackage
from openaddr.tests.cache import TestCacheExtensionGuessing, TestCacheEsriDownload
from openaddr.tests.conform import TestConformCli, TestConformTransforms, TestConformMisc, TestConformCsv, TestConformLicense, TestConformTests
from openaddr.tests.preview import TestPreview
from openaddr.tests.slippymap import TestSlippyMap
from openaddr.tests.util import TestUtilities

if __name__ == '__main__':
    # Allow the user to turn on logging with -l or --logall
    # unittest.main() has its own command line so we slide this in first
    level = logging.CRITICAL
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "-l" or arg == "--logall":
            level = logging.DEBUG
            del sys.argv[i]

    unittest.main()
