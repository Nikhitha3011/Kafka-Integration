import os
import json
import uuid
import pandas as pd
from kafka import KafkaConsumer
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from repository-local `dev.env` by default.
load_dotenv("dev.env", override=True)

# ---------------------- AWS CONFIG ----------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

# ---------------------- KAFKA CONFIG ----------------------
TOPIC = "kaggle_raw"
BOOTSTRAP = "localhost:9092"

# Datasets allowed
DATASETS_TO_UPLOAD = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv"
]

UPLOAD_LIMIT = 10  # parquet files per dataset
upload_counter = {f: 0 for f in DATASETS_TO_UPLOAD}

# Partition-based batches
batches = {}  # { "dataset/partition_path": [records] }

print("\nKafka → S3 Parquet uploader started...")
print("Uploading up to 10 parquet files per dataset...\n")

# ---------------------- KAFKA CONSUMER ----------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ---------------------- S3 CLIENT ----------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# ---------------------- PARQUET UPLOADER ----------------------
def upload_parquet_to_s3(records, dataset, partition_path):
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)

    local_path = f"/tmp/{uuid.uuid4()}.parquet"
    pq.write_table(table, local_path)

    s3_key = (
        f"{S3_PREFIX}/kaggle_raw/{dataset}/{partition_path}/"
        f"{dataset}_{uuid.uuid4()}.parquet"
    )

    with open(local_path, "rb") as f:
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f)

    os.remove(local_path)

    print(f"Uploaded parquet → {s3_key}  (rows={len(records)})")


# ---------------------- MAIN LOOP ----------------------
for msg in consumer:

    record = msg.value
    source_file = record.get("source_file")

    # Only allowed datasets
    if source_file not in DATASETS_TO_UPLOAD:
        continue

    # STOP uploading but DO NOT stop batching → prevents locking
    if upload_counter[source_file] >= UPLOAD_LIMIT:
        continue

    # ---------------------- Partition Logic ----------------------
    if source_file == "olist_customers_dataset.csv":
        state = record.get("customer_state", "unknown")
        partition_path = f"customer_state={state}"

    elif source_file == "olist_orders_dataset.csv":
        ts = record.get("order_purchase_timestamp")
        try:
            dt = datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except:
            dt = "unknown"
        partition_path = f"order_date={dt}"

    dataset = source_file.replace(".csv", "")
    batch_key = f"{dataset}/{partition_path}"

    # Init batch if missing
    if batch_key not in batches:
        batches[batch_key] = []

    # Add record
    batches[batch_key].append(record)

    # ---------------------- Upload every 100 records ----------------------
    if len(batches[batch_key]) >= 100:

        upload_parquet_to_s3(
            batches[batch_key],
            dataset,
            partition_path
        )

        batches[batch_key] = []  # reset batch
        upload_counter[source_file] += 1

        print(f"Uploaded {upload_counter[source_file]}/{UPLOAD_LIMIT} for {source_file}")

        # Stop completely when all datasets completed
        if all(upload_counter[f] >= UPLOAD_LIMIT for f in DATASETS_TO_UPLOAD):
            print("\nReached upload limit for ALL datasets.\n")
            break


