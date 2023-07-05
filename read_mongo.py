
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from textblob import TextBlob
from pyspark.sql.types import  IntegerType
from pyspark.sql import functions as F
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from datetime import timedelta
import json,os
from google.cloud import storage
from pyspark.sql.functions import  to_date,col
import jsonlines
from google.cloud import bigquery
import sqlalchemy as sa
from sqlalchemy import create_engine, Table, MetaData,select

def build_spark_session():
    """Create Spark Session"""
    spark = SparkSession\
        .builder\
        .appName("naya")\
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.3")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")        
    data_trans = spark.read.format("mongo").option("uri","mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/Real-Time-Inventory.transactions").load()
    return spark
def read_stock_db(spark):
    """Read Stock DB"""
    data_stock = spark.read.format("mongo").option("uri","mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/Real-Time-Inventory.stock").load()    
    return data_stock

def read_trans_db(spark):
    """Read Transactions DB"""
    data_trans = spark.read.format("mongo").option("uri","mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/Real-Time-Inventory.transactions").load()
    return data_trans

   
def stock_prediction(df_stock,df_trans):
    """Predict Stock End Date by average qty per day"""
    """returns products which stock may end in the upcoming week"""
    """returns stock prediction dataframe"""
    df_by_day=df_trans.groupBy(to_date("trans_datetime"),"trans_pn","trans_product_name")\
     .sum("trans_qty").orderBy(col("trans_pn").asc()).withColumnRenamed("sum(trans_qty)","Sum_Qty").withColumnRenamed("to_date(`trans_datetime`)","Date")
    count_dates=df_by_day.select(countDistinct("Date")) 
    count_dates=count_dates.collect()[0][0]  
    df_avg_by_day=df_by_day.groupBy('trans_pn','trans_product_name').sum('Sum_Qty').withColumn('avg_qty_per_day',col("sum(Sum_Qty)") /count_dates).drop("sum(Sum_Qty)")  
    df_joined=df_stock.join(F.broadcast(df_avg_by_day),((df_stock.pn ==  df_avg_by_day.trans_pn)),"inner")\
    .drop(df_avg_by_day.trans_pn).drop(df_avg_by_day.trans_product_name)
    df_res = df_joined.withColumn('Expected_qty_end_of_stock', (df_joined.stock/df_joined.avg_qty_per_day).cast(IntegerType()))    
    df_res=df_res.select(df_res['*'],F.current_date(),df_res.Expected_qty_end_of_stock,
     expr("date_add(current_date(),Expected_qty_end_of_stock)")
  .alias("end_of_stock_date"))
    df_res=df_res.drop(df_res['current_date()']).drop(df_res['Expected_qty_end_of_stock'])
    refill_stock=df_res.filter(F.col("end_of_stock_date").between(pd.to_datetime(datetime.now().date()),pd.to_datetime(datetime.now().date()+timedelta(days=4))))  
    restock_collection = refill_stock.collect()
    return restock_collection,df_res

def write_to_bucket(restock_collection):
    """Write restock products in json format to GCS bucket"""
    List_restock=[]
    for row in restock_collection:
     current={"pn":row['pn'],
         "product_line_item":row['product_line_item'],
         "avg_qty":row['avg_qty_per_day'],
         "end_of_stock_date":row['end_of_stock_date'].strftime('%Y-%m-%d')
         } 
     blob = bucket.blob('restock_data__'+row['pn']+'.json')
     blob.upload_from_string(data=json.dumps(current),content_type='application/json')
     List_restock.append(current)    
    print(List_restock) 

def write_to_bigquery(df_prediction):
  """Write Stock Prediction table to bigquery"""   
  pandas_df = df_prediction.toPandas()
  pandas_df['end_of_stock_date']=pandas_df['end_of_stock_date'].astype(str)
  pandas_df=pandas_df[['pn','avg_qty_per_day','end_of_stock_date']]
  client = bigquery.Client()
  table_id = 'feisty-oxide-385407.Stock_Project.prediction'
  job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE
    )
  write_disposition = 'WRITE_TRUNCATE'
  job = client.load_table_from_dataframe(
    pandas_df, table_id,
    job_config=job_config
   )  
  
def update_my_sql(stock_df):
    """Update out of stock products"""
    engine= sa.create_engine("mysql+pymysql://naya:NayaPass1!@localhost/inventory")
    conn = engine.connect()
    metadata = MetaData()
    out_of_stock_tbl = Table('out_of_stock', metadata, autoload_with=engine)
    df_out_of_stock=stock_df.filter(F.col('stock')==0).collect()
    for row in df_out_of_stock:
        query = select([out_of_stock_tbl]).where(out_of_stock_tbl.columns.pn.like(row['pn']))
        result = conn.execute(query)
        if result.rowcount==0:
         print("adding out of stock item "+row['pn'])
         ins=out_of_stock_tbl.insert().values(pn=row['pn'], product_line_item=row['product_line_item'])   
         result = conn.execute(ins)
         print(result)
    
  
     
if __name__ == '__main__':
 os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/naya/Final_Project/feisty-oxide-385407-e33af95c3f86.json"
 storage_client = storage.Client.from_service_account_json('/home/naya/Final_Project/feisty-oxide-385407-e33af95c3f86.json')
 bucket = storage_client.bucket('restockitems')
 cluster=MongoClient("mongodb+srv://ork:12345@cluster0.e0w4mmx.mongodb.net/")
 stock_db=cluster["Real-Time-Inventory"]["stock"]
 session=build_spark_session()
 stock_df=read_stock_db(session)
 trans_df=read_trans_db(session)
 update_my_sql(stock_df)
 restock_collection,df_prediction=stock_prediction(stock_df,trans_df)
 write_to_bucket(restock_collection)
 write_to_bigquery(df_prediction)
