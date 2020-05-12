import unittest

# Custom import
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics


class TestAccuracy(unittest.TestCase):
    """

    """
    def test_accuracy_pos(self):
        """
        Positive test case for the accuracy function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertAlmostEqual(metrics.accuracy(tp=1, tn=1, fp=1, fn=1), 0.5)

    def test_accuracy_neg(self):
        """
        Negative test case for the accuracy function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertNotEqual(metrics.accuracy(tp=1, tn=1, fp=1, fn=1), 0.8)

    def test_types(self):
        """
        Positive test case for the accuracy function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.accuracy(1.0, 2, 3, 4)

        with self.assertRaises(TypeError):
            metrics.accuracy(1, 2.0, 3, 4)

        with self.assertRaises(TypeError):
            metrics.accuracy(1, 2, 3.0, 4)

        with self.assertRaises(TypeError):
            metrics.accuracy(1, 2, 3, 4.0)

    def test_values(self):
        """
        Positive test case for the accuracy function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(ValueError):
            metrics.accuracy(None, 2, 3, 4)

        with self.assertRaises(ValueError):
            metrics.accuracy(1, None, 3, 4)

        with self.assertRaises(ValueError):
            metrics.accuracy(1, 2, None, 4)

        with self.assertRaises(ValueError):
            metrics.accuracy(1, 2, 3, None)
