import unittest

# Custom import
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics


class TestAccuracy(unittest.TestCase):
    """

    """
    def test_accuracy_pos(self):
        """
        positive test case for the accuracy function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertAlmostEqual(metrics.accuracy(tp = 1, tn = 1, fp = 1, fn = 1), 0.5)

    def test_accuracy_neg(self):
        """
        negative test case for the accuracy function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertNotEqual(metrics.accuracy(tp = 1, tn = 1, fp = 1, fn = 1), 0.8)

    def test_types(self):
        """
        """
        self.assertRaises()

    def test_values(self):
        """
        """
        pass


        

