#!/bin/bash
# init-kafka-topics-dev.sh

#Dev-Configuration
BOOTSTRAP_SERVER="localhost:9092"
TOPICS=("raw-sensor-data" "processed-data" "alerts")

echo "🛠️ Starte Topic-Erstellung..."
for topic in "${TOPICS[@]}"; do
  docker exec broker \
  kafka-topics --create \
    --bootstrap-server broker:29092 \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
done
echo "✅ Alle Topics erstellt!"