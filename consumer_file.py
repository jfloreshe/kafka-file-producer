from kafka import KafkaConsumer
from datetime import datetime
from subprocess import Popen
import json
import time

if __name__ == "__main__":
    consumer = KafkaConsumer("files", bootstrap_servers='25.12.204.26:9092')    
    print("Waiting for new files")
    for kafka_message in consumer:
        print("test")
        message = kafka_message.value.decode()
        process = Popen(['python3','consumer_file_content.py',message])
        print(f"A new consumer has been created with topic: {message}")
