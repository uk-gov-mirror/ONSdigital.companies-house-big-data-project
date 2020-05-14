import unittest

# Custom import
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics


class TestRecall(unittest.TestCase):
    """

    """
    def test_recall(self):
        """
        Test case for the recall function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        self.assertAlmostEqual(metrics.recall(tp=1, fn=1), 0.5)

    def test_types(self):
        """
        Test case for the recall function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.recall(1.0, 2)

        with self.assertRaises(TypeError):
            metrics.recall(1, 2.0)

    def test_values(self):
        """
        Test case for the recall function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(ValueError):
            metrics.recall(None, 2)

        with self.assertRaises(ValueError):
            metrics.recall(1, None)
