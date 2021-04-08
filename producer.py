from time import sleep
from json import dumps
from kafka import KafkaProducer

size = 1000000
producer = KafkaProducer(security_protocol="SSL", bootstrap_servers="b-4.demo-cluster-yh.kpkjyg.c4.kafka.ap-northeast-2.amazonaws.com:9094,b-3.demo-cluster-yh.kpkjyg.c4.kafka.ap-northeast-2.amazonaws.com:9094,b-2.demo-cluster-yh.kpkjyg.c4.kafka.ap-northeast-2.amazonaws.com:9094")
    
def success(metadata):
    print(metadata)

def error(exception):
    print(exception)

def kafka_python_producer_async(producer, size):
    for i in range(size):
        data = {'id' : i ,
            'message': f'My Number is {i}.'}
        producer.send('test_topic', msg).add_callback(success).add_errback(error)
    producer.flush()

kafka_python_producer_async(producer, size)