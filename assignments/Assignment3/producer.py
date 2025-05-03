import time
import random
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_ids = [101, 202, 303, 404, 505]
congestion_levels = ["LOW", "MEDIUM", "HIGH"]
max_vehicle_count = 50
max_speed = 120  

while True:
    event = {
        "sensor_id": random.choice(sensor_ids),  # Randomly choose a sensor ID
        "timestamp": time.time(),
        "vehicle_count": random.randint(0, max_vehicle_count),  # Random vehicle count
        "average_speed": round(random.uniform(5, max_speed), 2),  # Random average speed (km/h)
        "congestion_level": random.choice(congestion_levels)  # Random congestion level
    }
    
    producer.send('traffic_data', event)
    print(f"Sent event: {event}")
    
    time.sleep(random.uniform(0.5, 2.0))

