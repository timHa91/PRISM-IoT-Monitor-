import datetime
import json
import random
import time
from datetime import datetime, timezone
import sys
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError



# Configuration
BOOTSTRAP_SERVERS ='localhost:9092'
TOPIC_NAME = 'raw-sensor-data'
DEVICE_COUNTER = 3
INTERVAL = 5 # seconds

def check_kafka_connection():
    """Prüft, ob eine Verbindung zum Kafka-Broker möglich ist."""
    try:
        test_producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            request_timeout_ms=3000
        )
        test_producer.metrics()
        test_producer.close()
        return True
    except NoBrokersAvailable:
        print("\n❌ Error: Kafka-Broker ", BOOTSTRAP_SERVERS, "not reachable.")
        return False
    except KafkaTimeoutError:
        print("\n❌ Error: Timeout while connecting to Kafka.")
        return False
    except Exception as e:
        print("\n❌ Unexpected Error while connecting to Kafka:", str(e))
        return False

def generate_device_id(counter):
    return [f"DEVICE_{str(i+1).zfill(3)}" for i in range(counter)]

def simulate_sensor_data(device_id):
   return {
        "device_id": device_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(-10, 40), 1),  # °C
        "humidity": random.randint(0, 100),            # %
        "battery": random.randint(10, 100),               # %
        "position": {
            "lat": round(random.uniform(47.3, 54.1)),  # Example: DE-latitude
            "lon": round(random.uniform(5.9, 15.0))    # Example: DE-longitude
        }
   }

def main():
    if not check_kafka_connection():
        sys.exit(1)

    try: 
        producer = KafkaProducer(bootstrap_servers = BOOTSTRAP_SERVERS)
    except Exception as e:
        print("\n❌ Error while Creating a Producer:", str(e))
        sys.exit(1)

    device_ids = generate_device_id(DEVICE_COUNTER)

    try:
        print("\n🚀 IoT-Data-Simulation started (Ctrl+C to stop).")
        while True:
          for device_id in device_ids:
             data = simulate_sensor_data(device_id)
             producer.send(
                TOPIC_NAME,
                value = json.dumps(data).encode('utf-8')
                )
             print(f"Sent: {data['device_id']} | Temp: {data['temperature']}°C")

          time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped.")
    except Exception as e:
        print("\n⚠️ Error while running the Simulation:", str(e))
    finally:
        producer.close()
        
if __name__ == "__main__":
    main()
