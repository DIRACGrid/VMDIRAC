import unittest

class TestUM(unittest.TestCase):
 
    def setUp(self):
        pass
 
    def test_numbers_3_4(self):
        self.assertEqual( 12, 12)
 
    def test_strings_a_3(self):
        self.assertEqual( 'dfsd', 'aaa')
 
if __name__ == '__main__':
    unittest.main()
