import unittest

# Custom import
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics

class TestMetricReport(unittest.TestCase):
    """
    Tests for metric report.
    """

    def test_precision_positive(self):
        """
        Positive test case for the metric_report function: all arguments
        passed are of type float less than or equal to 1.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.metrics_report(0.1, 0.2, 0.3, 0.4)

        with self.assertRaises(TypeError):
            metrics.metrics_report(0.1, 0.2, 0.3, 0.4)

        with self.assertRaises(TypeError):
            metrics.metrics_report(0.1, 0.2, 0.3, 0.4)

        with self.assertRaises(TypeError):
            metrics.metrics_report(0.1, 0.2, 0.3, 0.4)

    def test_precision_negative(self):
        """
        Negative test case for the metric_report function.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.metrics_report(10, 0.2, 0.3, 0.4)

        with self.assertRaises(TypeError):
            metrics.metrics_report(0.1, 20, 0.3, 0.4)

        with self.assertRaises(TypeError):
            metrics.metrics_reports(0.1, 0.2, 30, 0.4)

        with self.assertRaises(TypeError):
            metrics.metrics_reports(0.1, 0.2, 0.3, 40)

    def test_types(self):
        """
        Test correct argument types passed.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(TypeError):
            metrics.metrics_report(1, 1.0, 1.0, 1.0)

        with self.assertRaises(TypeError):
            metrics.metrics_report(1.0, 1, 1.0, 1.0)

        with self.assertRaises(TypeError):
            metrics.metrics_report(1.0, 1.0, 1, 1.0)

        with self.assertRaises(TypeError):
            metrics.metrics_report(1.0, 1.0, 1.0, 1)

    def test_values(self):
        """
        Test values are provided as arguments.
        """
        # Assume
        metrics = BinaryClassifierMetrics()

        # Assert
        with self.assertRaises(ValueError):
            metrics.metrics_report(None, 0.2, 0.3, 0.4)

        with self.assertRaises(ValueError):
            metrics.metrics_report(0.1, None, 0.3, 0.4)

        with self.assertRaises(ValueError):
            metrics.metrics_report(0.1, 0.2, None, 0.4)

        with self.assertRaises(ValueError):
            metrics.metrics_report(0.1, 0.2, 0.3, None)
