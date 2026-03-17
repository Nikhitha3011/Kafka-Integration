# Kafka → S3 (Kaggle) Integration

This repository contains two small example scripts that demonstrate a pipeline which
1) downloads Kaggle CSV datasets and produces rows to Kafka, and
2) consumes Kafka records, batches them, writes Parquet files and uploads them to S3.

What you'll find here
- `kaggle_to_kafka.py` — downloads a Kaggle dataset into `DOWNLOAD_DIR` and sends rows from a few CSVs to a Kafka topic.
- `kafka_to_s3.py` — consumes the Kafka topic, partitions/ batches records, writes Parquet using `pyarrow`, and uploads them to S3.
- `sample.dev.env.txt` — sample environment variables used by the scripts.
- `kaggle_download/` — example CSVs (already included) for quick local testing.

Quick overview of what was implemented
- Kaggle download using the `kaggle` CLI and `subprocess`.
- Kafka producer with `kafka-python` that serializes rows as JSON.
- Kafka consumer that groups records by partition keys (e.g. `customer_state` / `order_date`), writes Parquet using `pyarrow`, and uploads to S3 using `boto3`.

Prerequisites
- Python 3.8+
- Local or remote Kafka broker (topic created before running producer)
- AWS credentials with write access to the target S3 bucket (if using S3)
- Kaggle CLI configured (`kaggle` command available and `kaggle.json` credentials placed)

Recommended Python dependencies
Install into a virtualenv and then:

```bash
pip install kafka-python pandas python-dotenv kaggle boto3 pyarrow
```

Environment variables
Copy `sample.dev.env.txt` to `dev.env` and fill values:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- `BOOTSTRAP` — Kafka bootstrap servers (e.g. `localhost:9092`)
- `TOPIC` — Kafka topic name to use
- `KAGGLE_DATASET` — Kaggle dataset identifier (example: `olistbr/brazilian-ecommerce`)
- `DOWNLOAD_DIR` — local folder where the dataset will be downloaded/unzipped
- `S3_BUCKET`, `S3_PREFIX` — S3 target path prefix

See [sample.dev.env.txt](sample.dev.env.txt) for a starter template.

Notes about dotenv paths
- `kaggle_to_kafka.py` loads `dev.env` from the repository root (`load_dotenv("dev.env")`).
- `kafka_to_s3.py` in the current code loads an absolute dotenv path (`C:/saiNikhitha/AI-Fabric/dev.env`). Update that line or ensure your env file is available at that path.

Running the pipeline (local testing)

1. Start Kafka (and Zookeeper) and create a topic that matches `TOPIC` in your `dev.env`.

2. Download & produce Kaggle CSV rows to Kafka

```bash
# from repository root
python kaggle_to_kafka.py
```

This script will:
- download the Kaggle dataset specified in `KAGGLE_DATASET` into `DOWNLOAD_DIR` (requires `kaggle` CLI)
- select a small number of CSVs (`NUM_TEST_FILES = 3`) and stream each row to the Kafka topic as JSON

3. Consume from Kafka and upload Parquet files to S3

```bash
python kafka_to_s3.py
```

Behavior and limits
- `kaggle_to_kafka.py` will send only the first 3 CSV files and all rows from each selected CSV (see `NUM_TEST_FILES` in the script).
- `kafka_to_s3.py` will only process datasets listed in `DATASETS_TO_UPLOAD` and will upload up to `UPLOAD_LIMIT` parquet files per dataset (defaults are shown in the script).
- The consumer batches records by partition keys (example: `customer_state` or `order_date`) and uploads a parquet file every 100 records.


