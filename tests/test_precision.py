import unittest

# Custom import
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics


class TestPrecision(unittest.TestCase):
    """

    """
    def test_precision_pos(self):
        """
        Positive test case for the precision function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertAlmostEqual(metrics.precision(tp=1, tn=1, fp=1, fn=1), 0.5)

    def test_precision_neg(self):
        """
        Negative test case for the precision function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertNotEqual(metrics.precision(tp=1, tn=1, fp=1, fn=1), 0.8)

    def test_types(self):
        """
        Positive test case for the precision function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.precision(1.0, 2, 3, 4)

        with self.assertRaises(TypeError):
            metrics.precision(1, 2.0, 3, 4)

        with self.assertRaises(TypeError):
            metrics.precision(1, 2, 3.0, 4)

        with self.assertRaises(TypeError):
            metrics.precision(1, 2, 3, 4.0)

    def test_values(self):
        """
        Positive test case for the precision function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(ValueError):
            metrics.precision(None, 2, 3, 4)

        with self.assertRaises(ValueError):
            metrics.precision(1, None, 3, 4)

        with self.assertRaises(ValueError):
            metrics.precision(1, 2, None, 4)

        with self.assertRaises(ValueError):
            metrics.precision(1, 2, 3, None)
