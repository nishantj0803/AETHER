import random
import uuid
from locust import HttpUser, task, between

PAYMENT_AMOUNTS = [9.99, 19.99, 49.50, 89.00, 120.00, 250.75, 499.99]
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "INR"]

class CustomerCheckoutUser(HttpUser):
    """
    Simulates real-world customer checkout behavior against Aether payment-service.
    Paces requests with short think-times (10ms - 50ms) to test high-concurrency streaming.
    """
    wait_time = between(0.01, 0.05)

    @task(70)
    def charge_payment(self):
        payload = {
            "account_id": f"acc_{random.randint(100000, 999999)}",
            "amount": random.choice(PAYMENT_AMOUNTS),
            "currency": random.choice(CURRENCIES),
            "idempotency_key": f"idemp_{uuid.uuid4().hex[:16]}"
        }
        with self.client.post(
            "/api/v1/payments/charge",
            json=payload,
            name="/api/v1/payments/charge",
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 500:
                # Flag failure but don't crash the load test worker
                response.failure(f"Payment failed with 500: {response.text}")

    @task(20)
    def health_check(self):
        self.client.get("/health", name="/health")

    @task(10)
    def metrics_scrape(self):
        self.client.get("/metrics", name="/metrics")
