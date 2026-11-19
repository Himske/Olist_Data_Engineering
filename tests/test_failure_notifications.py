import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


def load_notifications():
    path = Path(__file__).resolve().parents[1] / "dags" / "notifications.py"
    spec = importlib.util.spec_from_file_location("olist_notifications", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_failure_notification_skips_when_no_recipient(monkeypatch, caplog):
    module = load_notifications()
    monkeypatch.delenv("AIRFLOW_FAILURE_EMAIL", raising=False)

    module.notify_failure({})

    assert "skipping failure notification" in caplog.text


def test_failure_notification_sends_to_configured_recipients(monkeypatch):
    module = load_notifications()
    send_email = MagicMock()
    airflow = types.ModuleType("airflow")
    utils = types.ModuleType("airflow.utils")
    email = types.ModuleType("airflow.utils.email")
    email.send_email = send_email
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.utils", utils)
    monkeypatch.setitem(sys.modules, "airflow.utils.email", email)
    monkeypatch.setenv("AIRFLOW_FAILURE_EMAIL", "one@example.com, two@example.com")
    context = {
        "task_instance": SimpleNamespace(dag_id="olist<dag>", task_id="silver"),
        "run_id": "scheduled__2026-11-19",
    }

    module.notify_failure(context)

    send_email.assert_called_once_with(
        to=["one@example.com", "two@example.com"],
        subject="[Airflow] Task failed: olist<dag>.silver",
        html_content="<p>Task <strong>olist&lt;dag&gt;.silver</strong> failed in run scheduled__2026-11-19.</p>",
    )
