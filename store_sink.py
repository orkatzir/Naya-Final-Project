from datetime import datetime
from pymongo import MongoClient
from kafka import KafkaConsumer
from json import dumps,loads
from google.cloud import storage

date_today=datetime.now().strftime("%Y-%m-%d")


TOPIC_NAME='store_topic'
KAFKA_SERVER='cnt7-naya-cdh63:9092'
cluster=MongoClient("mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/")
stock_db=cluster["Real-Time-Inventory"]["stock"]
history_db=cluster["Real-Time-Inventory"]["transactions"]
consumer = KafkaConsumer(TOPIC_NAME,bootstrap_servers=KAFKA_SERVER,auto_offset_reset='latest',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))
while True:
    try:
         records = consumer.poll(3000) # timeout in millis , here set to 1 min
         record_list = []
         for tp, consumer_records in records.items():
          for consumer_record in consumer_records:
            record_list.append(consumer_record.value)
            history_db.insert_one(record_list[0])
            print(record_list[0])
            purchase_pn=record_list[0]['trans_pn']
            item_qty=record_list[0]['trans_qty']
            mongo_record=stock_db.find_one( { "pn":purchase_pn } )
            prev_stock=int(mongo_record['stock'])
            new_stock=prev_stock-item_qty
            query={
              "pn":purchase_pn
              }
            new_val={
             "$set":{"stock":new_stock}
             }
            new_rec=stock_db.update_one(query,new_val)
            print("New Stock For "+mongo_record["product_line_item"]+" Is:"+str(new_stock))
               
    except:
        print("failed")
        
        
        
