"""Airflow task failure notifications."""

import html
import logging
import os

logger = logging.getLogger(__name__)


def notify_failure(context: dict) -> None:
    recipients = [value.strip() for value in os.getenv("AIRFLOW_FAILURE_EMAIL", "").split(",") if value.strip()]
    if not recipients:
        logger.warning("AIRFLOW_FAILURE_EMAIL is unset; skipping failure notification")
        return

    task_instance = context["task_instance"]
    dag_id = html.escape(task_instance.dag_id)
    task_id = html.escape(task_instance.task_id)
    run_id = html.escape(context["run_id"])
    from airflow.utils.email import send_email

    send_email(
        to=recipients,
        subject=f"[Airflow] Task failed: {task_instance.dag_id}.{task_instance.task_id}",
        html_content=f"<p>Task <strong>{dag_id}.{task_id}</strong> failed in run {run_id}.</p>",
    )
