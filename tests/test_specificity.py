import unittest

# Custom import
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics


class TestSpecificity(unittest.TestCase):
    """

    """
    def test_specificity(self):
        """
        Test case for the specificity function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertAlmostEqual(metrics.specificity(tn=1, fn=1), 0.5)

    def test_types(self):
        """
        Test case for the specificity function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.specificity(1.0, 2)

        with self.assertRaises(TypeError):
            metrics.specificity(1, 2.0)

    def test_values(self):
        """
        Test case for the specificity function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(ValueError):
            metrics.specificity(None, 2)

        with self.assertRaises(ValueError):
            metrics.specificity(1, None)

