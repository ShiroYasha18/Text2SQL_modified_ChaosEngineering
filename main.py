import os
import time
import random
import re
import psutil
import statistics
from datetime import datetime
import google.generativeai as genai
import psycopg2
from dotenv import load_dotenv
from testcontainers.postgres import PostgresContainer
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any


# Metrics tracking
@dataclass
class QueryMetrics:
    prompt: str
    sql_query: str
    execution_time: float
    retry_count: int
    success: bool
    error_type: Optional[str]
    rows_returned: int
    chaos_type: Optional[str]
    timestamp: datetime


class DatabaseChaosRunner:
    def __init__(self):
        load_dotenv()
        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
        self.metrics: List[QueryMetrics] = []
        self.chaos_active = True

    def generate_sql_query(self, prompt: str) -> str:
        structured_prompt = f"""You are a SQL query generator. Respond ONLY with the exact PostgreSQL query, nothing else - no explanations, no markdown, no commentary. The query should end with a semicolon.

Schema:
- customers table: customer_id (PRIMARY KEY), customer_name
- orders table: order_id (PRIMARY KEY), customer_id (FOREIGN KEY), order_date, total_amount

Task: {prompt}"""

        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(
            structured_prompt,
            generation_config={
                "temperature": 0.7,
                "max_output_tokens": 200,
            }
        )
        return response.text.strip()

    def simulate_resource_constraints(self):
        """Simulate high CPU/memory usage"""
        if random.random() < 0.2:  # 20% chance
            print("🔥 Simulating high CPU usage...")
            start_time = time.time()
            while time.time() - start_time < random.uniform(1, 2):
                _ = [i * i for i in range(10000)]
            print("Resource constraint simulation ended")

    def simulate_db_specific_chaos(self, conn):
        """Simulate database-specific issues"""
        if not self.chaos_active or random.random() > 0.3:
            return None

        chaos_types = {
            "long_query": "SELECT pg_sleep(2);",
            "timeout": "SET statement_timeout = '100ms';",
            "kill_connection": "SELECT pg_terminate_backend(pg_backend_pid());",
            "temp_table_stress": """
                CREATE TEMP TABLE stress_test AS 
                SELECT generate_series(1,10000) AS id;
                DROP TABLE stress_test;
            """
        }

        chaos_type = random.choice(list(chaos_types.keys()))
        print(f"🌪️ Simulating database chaos: {chaos_type}")

        try:
            with conn.cursor() as cur:
                cur.execute(chaos_types[chaos_type])
            conn.commit()
        except Exception as e:
            print(f"Chaos error (expected): {e}")

        return chaos_type

    def simulate_network_issues(self):
        """Simulate network-related issues"""
        if not self.chaos_active or random.random() > 0.2:
            return None

        issues = {
            "latency": (0.5, 1.5),
            "timeout": (2.0, 3.0),
            "partition": (1.0, 2.0)
        }

        issue_type = random.choice(list(issues.keys()))
        min_time, max_time = issues[issue_type]

        print(f"🌐 Simulating network {issue_type}...")
        time.sleep(random.uniform(min_time, max_time))
        print(f"Network {issue_type} resolved")

        return issue_type

    def execute_query_with_retry(self, conn, sql_query: str, max_retries: int = 3) -> QueryMetrics:
        metrics = QueryMetrics(
            prompt="",
            sql_query=sql_query,
            execution_time=0,
            retry_count=0,
            success=False,
            error_type=None,
            rows_returned=0,
            chaos_type=None,
            timestamp=datetime.now()
        )

        start_time = time.time()

        for attempt in range(max_retries):
            try:
                # Simulate various chaos scenarios
                self.simulate_resource_constraints()
                network_issue = self.simulate_network_issues()
                db_chaos = self.simulate_db_specific_chaos(conn)

                if network_issue or db_chaos:
                    metrics.chaos_type = network_issue or db_chaos

                with conn.cursor() as cursor:
                    cursor.execute(sql_query)
                    results = cursor.fetchall()
                    conn.commit()

                    metrics.success = True
                    metrics.rows_returned = len(results)
                    metrics.retry_count = attempt
                    return metrics, results

            except psycopg2.Error as e:
                conn.rollback()
                metrics.error_type = str(e)
                metrics.retry_count = attempt + 1
                print(f"🔄 Retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(random.uniform(0.1, 0.5) * (attempt + 1))  # Exponential backoff

            finally:
                metrics.execution_time = time.time() - start_time

        return metrics, None

    def analyze_metrics(self):
        """Analyze collected metrics"""
        if not self.metrics:
            return "No metrics collected yet."

        analysis = {
            "total_queries": len(self.metrics),
            "successful_queries": sum(1 for m in self.metrics if m.success),
            "avg_execution_time": statistics.mean(m.execution_time for m in self.metrics),
            "max_execution_time": max(m.execution_time for m in self.metrics),
            "total_retries": sum(m.retry_count for m in self.metrics),
            "chaos_incidents": sum(1 for m in self.metrics if m.chaos_type is not None)
        }

        return analysis

    def run_resilience_test(self):
        with PostgresContainer("postgres:15") as postgres:
            db_params = {
                "host": postgres.get_container_host_ip(),
                "port": postgres.get_exposed_port(5432),
                "dbname": "test",
                "user": "test",
                "password": "test"
            }

            print("🚀 Starting database resilience test...")

            # Initialize database
            conn = psycopg2.connect(**db_params)
            self.setup_database(conn)

            test_prompts = [
                "Find customers with purchases over $500 last month",
                "Get the customer with the highest total purchase amount",
                "Find customers with above-average purchase amounts",
                "Find customers with purchases over $1000 in the last 30 days",
                "Find top 5 customers by purchase frequency",
                "Calculate the average order value by customer",
                "Find customers who haven't made a purchase in the last 15 days"
            ]

            for prompt in test_prompts:
                print(f"\n{'=' * 50}")
                print(f"📝 Testing prompt: {prompt}")

                sql_query = self.generate_sql_query(prompt)
                print(f"🔍 Generated SQL: {sql_query}")

                metrics, results = self.execute_query_with_retry(conn, sql_query)
                metrics.prompt = prompt
                self.metrics.append(metrics)

                if results is not None:
                    print(f"✅ Query successful ({len(results)} rows)")
                    print("Sample results:")
                    for row in results[:3]:
                        print(row)
                else:
                    print("❌ Query failed after all retries")

            print("\n📊 Test Results Summary:")
            analysis = self.analyze_metrics()
            for key, value in analysis.items():
                print(f"{key}: {value}")

            conn.close()

    def setup_database(self, conn):
        """Initialize database with test data"""
        with conn.cursor() as cursor:
            # Create tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(50)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INT REFERENCES customers(customer_id),
                    order_date DATE,
                    total_amount DECIMAL
                )
            """)

            # Insert test data
            for i in range(1, 21):
                cursor.execute(
                    "INSERT INTO customers (customer_name) VALUES (%s)",
                    (f"Customer {i}",)
                )

            for _ in range(50):
                cursor.execute(
                    """INSERT INTO orders (customer_id, order_date, total_amount)
                       VALUES (%s, CURRENT_DATE - INTERVAL '1 month' + (random() * INTERVAL '28 days'), %s)""",
                    (
                        random.randint(1, 20),
                        random.uniform(100, 1500)
                    )
                )

            conn.commit()


if __name__ == "__main__":
    runner = DatabaseChaosRunner()
    runner.run_resilience_test()
