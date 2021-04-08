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
    'mildang_topic',
    security_protocol="SSL",
     bootstrap_servers='',
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='db-test-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for i, message in enumerate(consumer):
    data = message.value

    try:
        mycursor = mydb.cursor()

        if i % 100 == 99:
            sql = "INSE INTO mildang_message (id, message) VALUES (%s, %s)"
        else:
            sql = "INSERT INTO mildang_message (id, message) VALUES (%s, %s)"
        
        val = (data['id'], data['message'])
        mycursor.execute(sql, val)
        mydb.commit()

        print(data['id'], "record inserted.")
    except Exception as e: 
        print(data['id'])
        print('Exception', e)
        pass

