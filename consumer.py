from kafka import KafkaConsumer
from json import loads
import mysql.connector

mydb = mysql.connector.connect(
  host="",
  user="",
  passwd="",
  database=""
)

consumer = KafkaConsumer(
    'test_topic',
     security_protocol="SSL",
     bootstrap_servers='',
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