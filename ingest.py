import os
import json
import uuid
from datetime import datetime
from jsonschema import validate, ValidationError
import pandas as pd

# JSON Schema definition from saved schema_validation file
with open("event_schema.json", "r") as schema_file:
    SCHEMA = json.load(schema_file)

INPUT_FILE = "events.jsonl"
OUTPUT_DIR = "./output"
ERROR_LOG = "./errors/errors.log"

os.makedirs("./errors", exist_ok=True)

def parse_timestamp(ts):
    try:
        return datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        return None

def write_to_parquet(df, tenant_id, dt):
    output_path = os.path.join(
        OUTPUT_DIR,
        f"tenant_id={tenant_id}",
        f"year={dt.year}",
        f"month={dt.month:02d}",
        f"day={dt.day:02d}"
    )
    os.makedirs(output_path, exist_ok=True)
    file_path = os.path.join(output_path, "data.parquet")
    df.to_parquet(file_path, index=False)

def log_error(record, error_msg):
    with open(ERROR_LOG, "a") as f:
        f.write(f"{json.dumps(record)} | Error: {error_msg}\n")

def main():
    valid_records = {}

    with open(INPUT_FILE, "r") as f:
        for line in f:
            try:
                record = json.loads(line)
                validate(instance=record, schema=SCHEMA)
                dt = parse_timestamp(record["timestamp"])
                if not dt:
                    raise ValueError("Invalid timestamp format")

                tenant = record["tenant_id"]
                key = (tenant, dt.date())
                valid_records.setdefault(key, []).append(record)

            except (ValidationError, ValueError) as e:
                log_error(record if 'record' in locals() else line.strip(), str(e))

    # Create partitioned Parquet files
    for (tenant, date), records in valid_records.items():
        df = pd.json_normalize(records)
        write_to_parquet(df, tenant, datetime.combine(date, datetime.min.time()))

if __name__ == "__main__":
    main()