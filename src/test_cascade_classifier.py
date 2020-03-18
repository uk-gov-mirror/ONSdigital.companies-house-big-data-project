import unittest
import cv2
from cascade_classifier import imgs_to_grey, classifier_input, classifier, classifier_output_count, classifier_output, classifier_process

class TestClassifierMethods(unittest.TestCase):
    '''
    '''
    def test_classifier_input(self):
        '''
        '''
        #path = "/Users/spot/repos/companies_house_project/data/test_img_01.tif"
        #self.assertAlmostEqual(classifier_input1(1), 6)
        pass

    def test_values(self):
        '''
        
        '''
        self.assertRaises(ValueError, classifier_input, None)
    
    def test_types(self,
                   methods = [imgs_to_grey,
                             classifier_input]):
        '''
        Check to see if type errors are raised
        '''
        for method in methods:
            self.assertRaises(TypeError, method, 1)
            self.assertRaises(TypeError, method, 1.0)
            self.assertRaises(TypeError, method, 1+5j)
            self.assertRaises(TypeError, method, True)
            self.assertRaises(TypeError, method, [])