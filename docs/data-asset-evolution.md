# Data Asset Evolution Roadmap

This document identifies the four key areas needed to evolve this project from a **data transportation pipeline** into a **data asset production system**. Each area includes the current gap, a concrete implementation plan, and acceptance criteria.

## Current State

The pipeline successfully moves data end-to-end:

```
PostgreSQL -> Debezium -> Kafka -> Vector -> MinIO (Bronze)
                                                ↓
                                          Spark -> Iceberg
```

This proves **data can flow from A to B**. The next step is to prove **the data arriving at B is trustworthy, observable, and the pipeline is easy to extend**.

---

## 1. Data Quality Layer

### Gap

The pipeline currently performs zero validation. Every record passes through regardless of correctness. There is no mechanism to answer: "Did this record arrive correctly?" or "Where did the bad records go?"

### Implementation Plan

#### 1.1 Schema Validation at Ingestion

Add a validation step in `iceberg-ingestion.py` between Kafka read and Iceberg write:

```
Kafka → Parse JSON → Validate Schema → Route
                                         ├─ Valid   → Iceberg (Silver)
                                         └─ Invalid → Dead Letter Table
```

- Validate required fields are non-null (`id`, `user_id`, `product_id`)
- Validate data types (e.g., `quantity` is a positive integer, `price` is non-negative)
- Validate referential integrity where possible (e.g., `user_id` exists in `users` table)

#### 1.2 Dead Letter Queue (DLQ)

Create a dedicated Iceberg table `nessie.db.dead_letters`:

| Column | Type | Description |
| :--- | :--- | :--- |
| source_topic | STRING | Kafka topic the record came from |
| raw_payload | STRING | Original JSON that failed validation |
| failure_reason | STRING | Why it was rejected (e.g., "null id", "negative quantity") |
| event_date | DATE | Partition column |

Records that fail validation are routed here instead of being silently dropped.

#### 1.3 End-to-End Reconciliation

Add a periodic reconciliation job (can be a simple SQL query via Trino):

```sql
-- Source count (PostgreSQL)
SELECT count(*) FROM orders WHERE updated_at >= current_date - interval '1' day;

-- Destination count (Iceberg)
SELECT count(*) FROM nessie.db.orders WHERE event_date = current_date;

-- Dead letter count
SELECT count(*) FROM nessie.db.dead_letters WHERE event_date = current_date;

-- Assertion: source_count == destination_count + dead_letter_count
```

### Acceptance Criteria

- [ ] Records with null `id` are routed to DLQ, not to the orders table
- [ ] Daily reconciliation query shows source count == destination count + DLQ count
- [ ] Validation metrics (pass/fail/fix counts) are queryable via Trino

---

## 2. Medallion Architecture (Bronze / Silver / Gold)

### Gap

All data lands in a single Iceberg table as raw CDC events. There is no cleaning, deduplication, or business-level aggregation. The Trino user queries raw Debezium output, not trustworthy business data.

### Implementation Plan

#### 2.1 Bronze Layer (exists)

Current `nessie.db.orders` table. Raw CDC events, append-only.
No changes needed. This is already working.

#### 2.2 Silver Layer (new)

A Spark batch job (scheduled hourly or daily) that reads Bronze and writes to Silver:

```python
# silver_orders_job.py

# 1. Deduplicate: CDC can replay events. Keep only the latest version per order id.
deduped = bronze_df \
    .withWatermark("created_at", "1 hour") \
    .dropDuplicates(["id"])

# 2. Enrich: JOIN with dimension tables
enriched = deduped \
    .join(users_df, "user_id") \
    .join(products_df, "product_id")

# 3. Standardize: Normalize field names, unify date formats, cast types
standardized = enriched \
    .withColumn("order_amount", col("quantity") * col("price")) \
    .withColumn("order_date", to_date(col("created_at")))

# 4. Write to Silver
standardized.writeTo("nessie.db.silver_orders").overwritePartitions()
```

Silver table schema:

| Column | Type | Description |
| :--- | :--- | :--- |
| order_id | INT | Deduplicated order ID |
| username | STRING | From users table |
| product_name | STRING | From products table |
| quantity | INT | Validated positive integer |
| unit_price | DECIMAL | From products.price |
| order_amount | DECIMAL | quantity * unit_price |
| order_status | STRING | Standardized status code |
| order_date | DATE | Partition column |

#### 2.3 Gold Layer (new)

Aggregated tables for business consumption:

```sql
-- gold_daily_summary
CREATE TABLE nessie.db.gold_daily_summary AS
SELECT
    order_date,
    count(*) AS order_count,
    sum(order_amount) AS total_revenue,
    avg(order_amount) AS avg_order_value,
    count(DISTINCT user_id) AS unique_customers
FROM nessie.db.silver_orders
GROUP BY order_date;
```

#### 2.4 Layer Ownership

| Layer | Update Frequency | Consumer | Data Guarantee |
| :--- | :--- | :--- | :--- |
| Bronze | Real-time (streaming) | Data Engineers only | Raw, may contain duplicates |
| Silver | Hourly / Daily (batch) | Analysts, Backend Services | Deduplicated, validated, enriched |
| Gold | Daily (batch) | BI Dashboards, Management | Aggregated, business-ready |

### Acceptance Criteria

- [ ] Silver table contains zero duplicate `order_id` values
- [ ] Silver `order_amount` matches `quantity * price` for every row
- [ ] Gold daily summary is queryable via Trino and matches Silver aggregation
- [ ] Bronze record count >= Silver record count (difference = duplicates + DLQ)

---

## 3. Data Observability

### Gap

`vector top` shows throughput (events/sec) but cannot answer:
- **Freshness**: Is the latest data 5 seconds old or 5 hours old?
- **Volume anomaly**: Today's order count dropped 80% compared to yesterday. Pipeline issue or business reality?
- **Schema drift**: Someone added a column to `orders`. Will Spark break?

### Implementation Plan

#### 3.1 Prometheus + Grafana Stack

Add to `docker-compose.yml`:

```yaml
prometheus:
  image: prom/prometheus
  ports: ["9090:9090"]
  volumes:
    - ./config/prometheus.yml:/etc/prometheus/prometheus.yml

grafana:
  image: grafana/grafana
  ports: ["3000:3000"]
  depends_on: [prometheus]
```

Collect metrics from:
- **Kafka**: JMX exporter (consumer lag, partition count, bytes in/out)
- **Debezium**: JMX exporter (WAL lag, events captured, snapshot status)
- **Spark**: Spark metrics sink to Prometheus (batch duration, records processed)
- **Vector**: Built-in Prometheus endpoint (already has `[api]` enabled)

#### 3.2 Key Dashboards

| Dashboard | Metrics | Alert Threshold |
| :--- | :--- | :--- |
| End-to-End Latency | PG commit timestamp vs Iceberg write timestamp | > 60 seconds |
| Hourly Volume Trend | Records per hour, compared to 7-day moving average | Deviation > 50% |
| Consumer Lag | Kafka consumer group lag per topic | > 100,000 events |
| Pipeline Health | Debezium connector status, Spark job state, Vector sink errors | Any component DOWN |
| Data Quality | Validation pass rate, DLQ volume, reconciliation delta | Pass rate < 99% |

#### 3.3 Schema Registry

Replace raw JSON with schema-enforced serialization:

1. Add Confluent Schema Registry (or Apicurio) as a new service
2. Configure Debezium to output Avro: `value.converter=io.confluent.connect.avro.AvroConverter`
3. Schema changes are versioned and backward-compatible by default
4. Breaking changes are blocked at the registry level before reaching downstream

### Acceptance Criteria

- [ ] Grafana dashboard shows real-time end-to-end latency
- [ ] Alert fires when consumer lag exceeds threshold
- [ ] Schema Registry rejects a breaking schema change (e.g., removing a required field)
- [ ] Volume anomaly alert fires when injecting 0 records for 1 hour after steady traffic

---

## 4. Pipeline Framework (Configuration-Driven Extension)

### Gap

Adding a new table to the pipeline requires changes in multiple places:
1. Debezium connector config (`table.include.list`)
2. Vector config (`topics`)
3. Spark ingestion script (hardcoded schema)
4. Iceberg table DDL

This means every new table needs a developer. The goal is: **a new table needs only a config file**.

### Implementation Plan

#### 4.1 Table Registry (Configuration File)

Create a central table definition file (`config/table-registry.yaml`):

```yaml
tables:
  - name: orders
    source_db: crm
    source_schema: public
    primary_key: id
    timestamp_column: updated_at
    partition_column: event_date
    columns:
      - { name: id, type: int, nullable: false }
      - { name: user_id, type: int, nullable: false }
      - { name: product_id, type: int, nullable: false }
      - { name: quantity, type: int, nullable: false }
      - { name: order_status, type: string, nullable: true }
      - { name: updated_at, type: timestamp, nullable: false }

  - name: inventory
    source_db: erp
    source_schema: public
    primary_key: product_id
    timestamp_column: last_updated
    partition_column: event_date
    columns:
      - { name: product_id, type: int, nullable: false }
      - { name: warehouse_id, type: int, nullable: true }
      - { name: quantity, type: int, nullable: false }
      - { name: last_updated, type: timestamp, nullable: false }
```

#### 4.2 Config-Driven Ingestion Script

Refactor `iceberg-ingestion.py` to be generic:

```python
# generic_ingestion.py
import yaml

registry = yaml.safe_load(open("/config/table-registry.yaml"))

for table_config in registry["tables"]:
    topic = f"prod.{table_config['source_db']}.{table_config['source_schema']}.{table_config['name']}"
    schema = build_spark_schema(table_config["columns"])
    validators = build_validators(table_config["columns"])

    kafka_df = spark.readStream.format("kafka") \
        .option("subscribe", topic).load()

    parsed = parse_debezium(kafka_df, schema)
    valid, invalid = apply_validation(parsed, validators)

    valid.writeStream.format("iceberg") \
        .toTable(f"nessie.db.{table_config['name']}")

    invalid.writeStream.format("iceberg") \
        .toTable("nessie.db.dead_letters")
```

#### 4.3 Auto-Generated Debezium Connectors

Create a script that reads `table-registry.yaml` and generates Debezium connector JSON:

```bash
# scripts/generate-connectors.sh
# Reads table-registry.yaml, groups tables by source_db,
# outputs one connector JSON per database.
```

#### 4.4 Extension Workflow

After framework is built, adding a new table becomes:

```
1. Add entry to config/table-registry.yaml        (5 min)
2. Run: make generate-connectors && make prod-setup (1 min)
3. Restart Spark ingestion                          (1 min)
```

No code changes. No PR review for pipeline logic.

### Acceptance Criteria

- [ ] Adding the `inventory` table requires only a YAML config change (zero Python/SQL changes)
- [ ] Validation rules are derived from the YAML config (nullable, type constraints)
- [ ] Debezium connector configs are auto-generated from the registry
- [ ] Iceberg table DDL is auto-generated from the registry

---

## Priority Order

| Priority | Area | Effort | Impact |
| :--- | :--- | :--- | :--- |
| P0 | Data Quality Layer | Medium | Without this, all downstream data is untrustworthy |
| P1 | Medallion Architecture | Medium | Enables business users to consume clean data |
| P2 | Data Observability | Medium | Enables proactive issue detection instead of reactive firefighting |
| P3 | Pipeline Framework | Low-Medium | Reduces marginal cost of adding new data sources to near-zero |

P0 and P1 can be developed in parallel. P2 can start anytime. P3 depends on having at least 2 tables in the pipeline to validate the abstraction.
