from __future__ import annotations

import os
from typing import Any, Dict

import requests


class PipelineService:
    def __init__(self) -> None:
        self.base_url = os.getenv("AIRFLOW_BASE_URL", "http://airflow:8080").rstrip("/")
        self.username = os.getenv("AIRFLOW_USERNAME", "admin")
        self.password = os.getenv("AIRFLOW_PASSWORD", "admin")
        self.dag_id = os.getenv("AIRFLOW_DAG_ID", "master_earthquake_pipeline")

    def trigger_dag_run(self) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v1/dags/{self.dag_id}/dagRuns"

        try:
            response = requests.post(
                url,
                json={},
                auth=(self.username, self.password),
                timeout=15,
            )
        except requests.exceptions.RequestException as exc:
            raise ConnectionError(f"Failed to reach Airflow at {self.base_url}: {exc}") from exc

        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Airflow rejected the trigger request ({response.status_code}): {response.text}"
            )

        payload = response.json()

        return {
            "dag_id": self.dag_id,
            "dag_run_id": payload.get("dag_run_id"),
            "state": payload.get("state"),
            "logical_date": payload.get("logical_date") or payload.get("execution_date"),
        }
