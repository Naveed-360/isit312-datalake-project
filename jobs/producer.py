import pandas as pd
import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime, timezone

# Connect to Kafka (running on localhost from outside Docker)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load existing patient IDs and metadata from historical visits to ensure realism
print("Loading patient IDs and metadata from data/m1/visits.csv...")
sample_df = pd.read_csv('./data/m1/visits.csv', nrows=5000)
existing_patient_ids = sample_df['patient_id'].dropna().unique().tolist()
doctor_ids = sample_df['doctor_id'].dropna().unique().tolist()
diagnoses = sample_df['diagnosis'].dropna().unique().tolist()
branches = [1, 2, 3]

# State file to persist the next visit number across runs
STATE_FILE = './jobs/.producer_state.json'
DEFAULT_START_VISIT_NUM = 324807

def get_next_visit_num():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                return state.get('next_visit_num', DEFAULT_START_VISIT_NUM)
        except Exception:
            pass
    return DEFAULT_START_VISIT_NUM

def save_next_visit_num(next_num):
    with open(STATE_FILE, 'w') as f:
        json.dump({'next_visit_num': next_num}, f, indent=2)

start_visit_num = get_next_visit_num()
NUM_VISITS_TO_SEND = 100  # Default to 100 for the delay test (set to None or large number for continuous)
INTERVAL_SECONDS = 0.2    # Speed (5 visits/sec) so 100 visits take ~20 seconds

print(f"Starting live new-visits stream (Starting from V{start_visit_num})...")
last_id_num = start_visit_num - 1
try:
    for i in range(NUM_VISITS_TO_SEND):
        curr_id_num = start_visit_num + i
        visit_record = {
            "visit_id": f"V{curr_id_num}",
            "patient_id": random.choice(existing_patient_ids),
            "branch_id": int(random.choice(branches)),
            "visit_datetime": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "doctor_id": random.choice(doctor_ids),
            "diagnosis": random.choice(diagnoses),
            "sent_timestamp": time.time()
        }

        producer.send('new-visits', visit_record)
        print(f"[{i+1}/{NUM_VISITS_TO_SEND}] Sent new visit {visit_record['visit_id']} for patient {visit_record['patient_id']} to Kafka")
        last_id_num = curr_id_num
        save_next_visit_num(last_id_num + 1)
        time.sleep(INTERVAL_SECONDS)
finally:
    save_next_visit_num(last_id_num + 1)

producer.flush()
print("\nFinished sending live visits!")
