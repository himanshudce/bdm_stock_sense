# import libraries
import pandas as pd
import os
import re
from pyspark.sql import SparkSession
import numpy as np
import time


#creating sparksession
appName = "stock parquet files"
spark = SparkSession.builder.appName(appName).getOrCreate()


# function to store files in pyspark parquet 
def store_in_parquet(parquet_location, c_data):
        sparkDF=spark.createDataFrame(c_data) 
        sparkDF.write.parquet(parquet_location)
        
        

# create dir and saving location file name according to IST(Indian Standard time) 
base_location = "/home/bdm/data_sources/stock_data"
output_location = "/home/bdm/data_sources/stock_data_parquet"
# create dir if not exist
try:
    os.makedirs(output_location)
except:
    pass


# extracting time stamp of current time in Asia-Kolkata 
date = pd.Timestamp.now(tz="Asia/Kolkata")
date_str = str(date.date()).replace("-","_")
print("extracting data for date ", date_str)

#base location date from where data needs to be read
base_location_date = os.path.join(base_location,date_str)
#location where parquet file will be stored
output_location_date = os.path.join(output_location,date_str)

# if location doesn't exist already 
# create dir if not exist
try:
    os.makedirs(output_location_date)
except:
    pass
hour = str(date.hour)
min = str(date.minute)

csv_name = 'stock_data_'+hour+'_'+min+'.csv'


file_name = os.path.join(base_location_date,csv_name)
m = re.findall(r"\d+",file_name)
print(m)
time.sleep(15)
# read the csv file
df = pd.read_csv(file_name)

#append timestamp in dataframe at index 0
df.insert(0, "Timestamp", "_".join(m[0:3]))
   
df.insert(1, "hour", "".join(m[3]))
df.insert(2, "minute", "".join(m[4]))
df.head()

df = df.apply(lambda x: x.fillna(0) if x.dtype.kind in 'biufc' else x.fillna('-'))

parquet_file = 'stock_data' +'_'+hour+'_'+min +'.parquet'
parquet_location = os.path.join(output_location_date,parquet_file)
df1 = df[['symbol','Timestamp','hour', 'minute','companyName','lastPrice','dayHigh','dayLow','closePrice','open']]
store_in_parquet(parquet_location,df1)
print("\n saved")





   








