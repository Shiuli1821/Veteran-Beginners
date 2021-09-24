from kafka import KafkaProducer
import sys
import numpy as np
import pandas as pd
import json
import time
import random
import pickle

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'houseprice'

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')


#read test data
df = pd.read_csv('test.csv')



class EncodeData(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(EncodeData, self).default(obj)

#send one record at a time
for i in range(df.shape[0]):
    record_dict = df.iloc[i].to_dict()
    print("sending...", str(record_dict))
    producer.send(KAFKA_TOPIC, json.dumps(record_dict,cls=EncodeData).encode("utf-8"))
    print('New Record sent to: ',KAFKA_TOPIC)
    time.sleep(random.randint(3,5))