#!/usr/bin/env python
""" A basic test to check that VMDIRAC is import-able. """

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

class ImportTestCase(unittest.TestCase):
    def test(self):
      import VMDIRAC

if __name__ == '__main__':
  unittest.main()
