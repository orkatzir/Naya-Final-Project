from datetime import datetime
import  random
from time import sleep
from kafka import KafkaProducer
from json import loads,dumps
from pymongo import MongoClient
import pandas as pd
cluster=MongoClient("mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/")
stock_db=cluster["Real-Time-Inventory"]["stock"]
history_db=cluster["Real-Time-Inventory"]["transactions"]
    
def send_transactions(topic_name):    
    producer = KafkaProducer(bootstrap_servers=['cnt7-naya-cdh63:9092'], #change ip here
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))
    df_stock= pd.DataFrame(list(stock_db.find()))
    for i in range(30,31):
     df_to_send=df_stock.sample(n=215, weights=df_stock['odds'], axis=0)
     for index, row in df_to_send.iterrows():
       if row['stock']==1:
           item_qty=1
       else:
          item_qty=random.randint(1,3)
       date=datetime.now().strftime("%Y-06-"+str(i)+" %H:%M:%S")    
       val={"trans_pn":row['pn'],"trans_product_name":row['product_line_item'],"trans_datetime":date,"trans_qty":item_qty}    
       print(val)  
       producer.send(topic_name,value=val)       
       sleep(1)
send_transactions("store_topic")


