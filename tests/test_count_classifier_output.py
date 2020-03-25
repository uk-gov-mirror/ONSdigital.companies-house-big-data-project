import unittest
import cv2

from src.classifier.cst_classifier import Classifier

class TestClassifierMethods(unittest.TestCase):
    '''
    '''
    def test_values(self):
        '''
        '''
        self.assertRaises(ValueError, Classifier.count_classifier_output, None)