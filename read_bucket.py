import json
from google.cloud import storage
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient
import math
json_list=[]
storage_client = storage.Client.from_service_account_json('/home/naya/feisty-oxide-385407-e33af95c3f86.json')
cluster=MongoClient("mongodb+srv://ork:****@cluster0.e0w4mmx.mongodb.net/")
supply_db=cluster["Real-Time-Inventory"]["supply"]
stock_db=cluster["Real-Time-Inventory"]["stock"]
bucket = storage_client.bucket('restockitems')

def check_supply_exists(pn):
    """Check if supply already scheduled"""
    supply_db.find_one({'pn':pn,'is_done':False})
    
if __name__ == '__main__':
    blobs = list(bucket.list_blobs(prefix='restock_data__'))
    for file in blobs:
     list_reupload=[]
     current_file=json.loads(file.download_as_string())
     item_date=datetime.strptime(current_file['end_of_stock_date'], '%Y-%m-%d').date()
     item_avg_qty=current_file['avg_qty']
     item_pn=current_file['pn']
     item_name=current_file['product_line_item']
     if  not check_supply_exists(item_pn):
      supply_date=(datetime.now().date()+timedelta(days=3)).strftime('%Y-%m-%d')
      qty_of_supply=math.ceil(item_avg_qty*5)
      new_json={"pn":item_pn,"product_line_item":item_name,"date_of_supply":supply_date,"qty_of_supply":qty_of_supply,"is_done":False}
      supply_db.insert_one(new_json)
      file.delete()   

