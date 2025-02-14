import json
import random
from kafka import KafkaProducer

bootstrap_server='localhost:9092'

temperature = random.randint(-20, 50)

producer = KafkaProducer(bootstrap_server)

