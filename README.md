# Mini Kafka PySpark Docker Pipeline

A Dockerized streaming data quality pipeline with Kafka, PySpark Structured Streaming, and a synthetic transaction producer.

This version avoids Windows-specific Spark setup such as `winutils.exe` and `hadoop.dll` by running Spark inside a Linux container.

## Architecture

```text
producer container
  -> Kafka topic: transactions
  -> Spark streaming container
  -> output/clean_data
  -> output/bad_records
  -> output/health_report
```

The `kafka-init` container creates the `transactions` topic before Spark starts, which avoids startup timing issues.

## Project Structure

```text
mini-kafka-pyspark-docker-pipeline/
|-- docker-compose.yml
|-- producer/
|   |-- Dockerfile
|   |-- producer.py
|   `-- producer_kafka.py
|-- spark_jobs/
|   |-- Dockerfile
|   `-- streaming_cleaning_job.py
|-- output/
|-- requirements.txt
|-- README.md
`-- .gitignore
```

## Run

Build and start Kafka plus Spark:

```powershell
docker compose up --build -d kafka spark
```

Monitor Spark:

```powershell
docker compose logs -f spark
```

Open another terminal and publish records:

```powershell
docker compose run --rm producer
```

Check generated output:

```powershell
Get-ChildItem output\clean_data
Get-ChildItem output\bad_records
Get-ChildItem output\health_report\latest
```

Stop containers:

```powershell
docker compose down
```

Clean generated runtime files before committing:

```powershell
Remove-Item -Recurse -Force output -ErrorAction SilentlyContinue
```

## Data Quality Rules

A record is marked bad when:

- `transaction_id` is missing
- `user_id` is missing
- `email` is missing or does not contain `@`
- `amount` is missing or less than or equal to zero
- `event_time` is missing

## Why Docker

Running Spark locally on Windows can require Hadoop native helper files such as `winutils.exe` and `hadoop.dll`. This project keeps the runtime inside Docker so contributors can run the pipeline with Docker commands instead of configuring Spark and Hadoop on their host machine.
