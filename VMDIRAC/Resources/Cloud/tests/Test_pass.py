import unittest
import importlib
from mock import MagicMock
from DIRAC import gLogger


class TestBasic(unittest.TestCase):

  def setUp(self):
    pass

  def test_true(self):
    self.assertEqual(True, True)


if __name__ == '__main__':
  unittest.main()
