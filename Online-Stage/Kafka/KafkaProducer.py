from confluent_kafka import Producer
import json
import csv
import time
import datetime
import os

def send_data_to_kafka(bootstrap_servers, topic, data_path):
    # Load the data
    print(f'Loading data from {data_path}')
    with open(data_path, 'r') as csv_file:
        data = csv.DictReader(csv_file)
        
        # Initialize producer
        producer = Producer({'bootstrap.servers': bootstrap_servers})
        
        # Time interval
        startTime = time.time()
        waitSeconds = 0.1

        for row in data:
            #print(row)
            # Convert to a JSON format
            current_time = time.localtime()
            #print(current_time)
            old_time = datetime.datetime.fromisoformat(row['Timestamp'])
            #print(old_time)
            new_time = old_time + datetime.timedelta(seconds=107209960)
            #print(new_time)
            row['Timestamp'] = new_time.isoformat(' ')
            #print(row['Timestamp'])
            payload = json.dumps(row)
            
            # Produce
            producer.produce(topic=topic, value=payload.encode('utf-8'))
            
            # Wait a number of seconds until the next message
            time.sleep(.5)

        producer.flush()
        print(f"Emptied {data_path}")

if __name__ == "__main__":
    BOOTSTRAP_SERVER = 'localhost:9092'
    TOPIC = 'Epsymolo'
    DATA_DIR = 'Data'  # Update this to the directory containing your CSV files

    # Iterate through all CSV files in the specified directory
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            file_path = os.path.join(DATA_DIR, filename)
            send_data_to_kafka(BOOTSTRAP_SERVER, TOPIC, file_path)