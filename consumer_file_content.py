from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
import json
import time
import sys

consumer = KafkaConsumer(bootstrap_servers='25.12.204.26:9092', auto_offset_reset='latest', enable_auto_commit=True, auto_commit_interval_ms=1000)    

def check_status(state, size, output_txt):
    if done == True and size == len(output_txt):
        with open(f"new_{topic}", 'w') as file:
            file.writelines(output_txt)
            file.close()
            print(f"{topic} created succesfully")
            consumer.close()
            quit()

if __name__ == "__main__":
    topic = sys.argv[1]
    print(f"Waiting for content for {topic}")
    output_txt = []
    done = False
    size = -1
    topic_partition = TopicPartition(topic,0)
    assigned_topic = [topic_partition]
    consumer.assign(assigned_topic)
    consumer.seek_to_beginning(topic_partition)

    for kafka_message in consumer:
        json_str = kafka_message.value.decode()
        #es posible que se necesite sincronizacion de numero de lineas
        if json_str.split()[0] == "done" and size == -1 and done == False:
            done = True
            size = int(json_str.split()[-1])
            print("-----------------------------------")
            print(json_str.split(), size , end=' ')
            print(f"{topic} {len(output_txt)} {size}")
            check_status(done, size, output_txt)
            continue

        obj = json.loads(json_str)
        output_txt.append(f"{obj['content']}\n")
        if len(output_txt) % 1000 == 0:
            print(f"{topic} has received 1000 more, current size is {len(output_txt)} expecting {size}")
        check_status(done, size, output_txt)

