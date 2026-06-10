"""
Locust load test for the churn prediction serving endpoint.

Usage (headless, against a running container):
    locust -f airflow-docker/tests/locustfile.py \
        --headless --users 20 --spawn-rate 5 --run-time 60s \
        --host http://localhost:8000 --csv /tmp/locust_results --only-summary

Or via Makefile:
    make load-test
"""
import random

from locust import HttpUser, between, task

_BASE = {
    "M_Out_Call_Count": 5.0,
    "M_Out_Call_Time": 120.0,
    "M_Data_Sum": 50_000_000.0,
    "M_Data_Count": 3,
    "M_In_Call_Count": 8,
    "M_In_Call_Time": 200.0,
}


def _jitter(base: dict) -> dict:
    return {
        "M_Out_Call_Count": max(0.0, base["M_Out_Call_Count"] + random.uniform(-3, 10)),
        "M_Out_Call_Time": max(0.0, base["M_Out_Call_Time"] + random.uniform(-60, 200)),
        "M_Data_Sum": max(0.0, base["M_Data_Sum"] + random.uniform(-10_000_000, 20_000_000)),
        "M_Data_Count": max(0, int(base["M_Data_Count"] + random.randint(-2, 5))),
        "M_In_Call_Count": max(0, int(base["M_In_Call_Count"] + random.randint(-4, 8))),
        "M_In_Call_Time": max(0.0, base["M_In_Call_Time"] + random.uniform(-100, 300)),
    }


class ChurnUser(HttpUser):
    wait_time = between(0.05, 0.2)

    @task(8)
    def predict_single(self):
        self.client.post("/predict", json=_jitter(_BASE), name="/predict")

    @task(2)
    def predict_batch_10(self):
        self.client.post(
            "/predict/batch",
            json={"records": [_jitter(_BASE) for _ in range(10)]},
            name="/predict/batch[10]",
        )

    @task(1)
    def health_check(self):
        self.client.get("/health", name="/health")
