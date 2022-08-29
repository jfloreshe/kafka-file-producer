from kafka import KafkaProducer
from faker import Faker
import json
import time
import glob
import time
import sys

KAFKA_PRODUCER =  KafkaProducer(bootstrap_servers='25.12.204.26:9092', max_request_size=10485760)

class KafkaMessageDTO:
    def __init__(self, fn, con):
        self.filename = fn
        self.content = con

def get_files(dir_path, file_pattern):
    files = []
    for file_name in  glob.glob(f"{dir_path}/{file_pattern}"):
        files.append(file_name)
    return files

#def send_message(files):
#    for file_path in files:
#        with open(file_path, mode="rb") as file:
#            file_content = file.read()
#            file_name = file_path.split('/')[-1]
#            obj = KafkaMessageDTO(file_name, file_content.decode())
#            json_str = json.dumps(obj.__dict__)
#            kafka_message = json_str.encode()
#            KAFKA_PRODUCER.send("files", kafka_message) 
#            print(f"{file_name} HAS BEEN SENT")
#            time.sleep(2)
def send_files(files_paths):
    for file_path in files_paths:
        file_name = file_path.split('/')[-1]
        kafka_message = file_name.encode()
        KAFKA_PRODUCER.send("files", kafka_message)
        print(f"{file_name} FILE SUCCESFULLY SENT")
    return True

def send_file_content(file_path):
    topic = file_path.split('/')[-1]
    with open(file_path, mode="rb") as file:
        lines = file.readlines()
        for line in lines:
            content = line.strip().decode()
            obj = KafkaMessageDTO(topic,content)
            json_str = json.dumps(obj.__dict__)
            kafka_message = json_str.encode()
            KAFKA_PRODUCER.send(topic, kafka_message)
        final_message = f"done {len(lines)}"
        KAFKA_PRODUCER.send(topic,final_message.encode())
        print(f"{final_message} send for {topic}")
    print(f"{topic} CONTENT SUCCESFULLY SENT")
            
def main():
    dir_path = ''
    file_pattern = ''
    if len(sys.argv) >= 3 and sys.argv[1] == '-d':
        dir_path = sys.argv[2]
    else:
        print('flag 1 must be -d and its used for directory')
    if len(sys.argv) >= 5 and sys.argv[3] == '-r':
        file_pattern = sys.argv[4]
    else:
        print('flag 2 must be -r and its used for file name')
    files_paths = get_files(dir_path, file_pattern)
    sent = send_files(files_paths)
    if sent == True:
        for file_path in files_paths:
            send_file_content(file_path)
    KAFKA_PRODUCER.flush()

main()
