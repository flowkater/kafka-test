from kafka import KafkaConsumer
from json import loads
import mysql.connector

mydb = mysql.connector.connect(
  host="database-1.clyq2yvs4ymv.ap-northeast-2.rds.amazonaws.com",
  user="admin",
  passwd="kafkatest1234",
  database="testdb"
)

consumer = KafkaConsumer(
    'test_topic',
     security_protocol="SSL",
     bootstrap_servers='b-4.demo-cluster-yh.kpkjyg.c4.kafka.ap-northeast-2.amazonaws.com:9094,b-3.demo-cluster-yh.kpkjyg.c4.kafka.ap-northeast-2.amazonaws.com:9094,b-2.demo-cluster-yh.kpkjyg.c4.kafka.ap-northeast-2.amazonaws.com:9094',
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='db-test-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for data in consumer:
    data = data.value
    
    mycursor = mydb.cursor()
    sql = "INSERT INTO test_message (id, message) VALUES (%s, %s)"
    val = (data['id'], data['message'])
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")