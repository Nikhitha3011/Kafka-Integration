import os
import subprocess
import pandas as pd
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

# -----------------------------------------------
# 1. LOAD ENVIRONMENT VARIABLES
# -----------------------------------------------
load_dotenv("dev.env")

KAFKA_TOPIC = os.getenv("TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP")
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")

NUM_TEST_FILES = 3   # send only 3 CSVs for testing


# -----------------------------------------------
# 2. DOWNLOAD KAGGLE DATASET
# -----------------------------------------------
print("\n Downloading Kaggle dataset...")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

subprocess.run([
    "kaggle", "datasets", "download", "-d", KAGGLE_DATASET,
    "-p", DOWNLOAD_DIR, "--unzip"
], check=True)

print("Kaggle dataset downloaded and extracted at:", DOWNLOAD_DIR)


# -----------------------------------------------
# 3. CONFIGURE KAFKA PRODUCER
# -----------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("\n Kafka Producer Connected")


# -----------------------------------------------
# 4. PROCESS CSV FILES & SEND LIMITED FILES
# -----------------------------------------------
csv_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
csv_files = csv_files[:NUM_TEST_FILES]   # pick only a few files

print("\n Selected CSV files for testing:", csv_files)


for csv_file in csv_files:
    file_path = os.path.join(DOWNLOAD_DIR, csv_file)
    print(f"\n Processing file: {csv_file}")

    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        data = row.to_dict()
        data["source_file"] = csv_file       # add source file name

        producer.send(KAFKA_TOPIC, data)

    print(f" Completed sending {len(df)} records from {csv_file}")


# -----------------------------------------------
# 5. FLUSH & COMPLETE
# -----------------------------------------------
producer.flush()
print("\n FINISHED — All selected CSVs sent to Kafka successfully!")
print(" Topic:", KAFKA_TOPIC)

