from pymongo import MongoClient
from datetime import datetime
import math

cluster=MongoClient("mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/")
stock_db=cluster["Real-Time-Inventory"]["stock"]
supply_db=cluster["Real-Time-Inventory"]["supply"]
cursor = supply_db.find({})
date_today=datetime.now().strftime('%Y-%m-%d')
for document in cursor:
    if document['date_of_supply']==date_today and document["is_done"]==False:
        item_s=stock_db.find_one({"pn":document['pn']})
        prev_stock=int(item_s['stock'])
        new_stock=math.ceil(prev_stock+document['qty_of_supply'])
        query_stock={"pn":document['pn']} 
        print(item_s['product_line_item'])
        new_val={
         "$set":{"stock":new_stock}
         }
        stock_db.update_one(query_stock,new_val)
        query_supply={"pn":document['pn']}
        new_val={
         "$set":{"is_done":True}
         }
        supply_db.update_one(query_supply,new_val)