from unittest.mock import MagicMock, patch

from abstract_block_dumper._internal.services import metrics


def test_record_task_recovered_is_a_no_op_without_prometheus():
    with patch.object(metrics, "PROMETHEUS_AVAILABLE", False), patch.object(metrics, "TASKS_RECOVERED", None):
        metrics.record_task_recovered("some_task", "pending")


def test_record_task_recovered_increments_the_counter_when_prometheus_is_available():
    counter = MagicMock()

    with patch.object(metrics, "PROMETHEUS_AVAILABLE", True), patch.object(metrics, "TASKS_RECOVERED", counter):
        metrics.record_task_recovered("some_task", "failed")

    counter.labels.assert_called_once_with(task_name="some_task", previous_status="failed")
    counter.labels.return_value.inc.assert_called_once_with()
