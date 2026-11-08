import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timezone

# Connect to Kafka (running on localhost from outside Docker)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read the data from your local data/m1 folder
visits_df = pd.read_csv('./data/m1/visits.csv')

print("Starting live visit stream...")
for index, row in visits_df.iterrows():
    visit_record = row.to_dict()
    visit_record['sent_timestamp'] = time.time()  # float epoch seconds

    producer.send('new-visits', visit_record)
    print(f"Sent visit {visit_record['visit_id']} to Kafka")
    time.sleep(1) # Sends one visit per second