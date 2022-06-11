from glob import glob

from pyspark.sql import SparkSession
import pandas as pd
import os 


#path from where we will read files for one hour 
path = '/home/bdm/data_sources/stock_data_parquet/'

#path where aggregations will be stored for one hour
outputpath = '/home/bdm/data_sources/stock_data_parquet_agg/'

# extracting current date and time
date = pd.Timestamp.now(tz="Asia/Kolkata")
date_str = str(date.date()).replace("-","_")
print("extracting data for date ", date_str)
hour = str(date.hour-1)
# min = str(date.minute)

#base location date from where data of today's date needs to be read
base_location_date = os.path.join(path,date_str)



# if location doesn't exist already 
# create dir if not exist
try:
    os.makedirs(outputpath)
except:
    pass

# name of files with Previous hour stored in parquet 
name = 'stock_data'+'_'+hour
print(name)
location_hr = os.path.join(base_location_date,name)

out_hr = os.path.join(outputpath,name)

FilenamesList = glob(location_hr+'_*.parquet')

#creating sparksession
appName = "stock parquet files"
spark = SparkSession.builder.appName(appName).getOrCreate()

#read all data of one hour
def read_allFiles(FilenamesList,out_hr):
    path = out_hr+'.parquet'
    for f in FilenamesList:
        df1 = spark.read.parquet(f)
        #appending files of one hour
        df1.write.mode('append').parquet(path)
        
    #returning path of file contains data of one hour
    return path
# taking average of price of stocks and max of day high and minimum of day low
def hour_agg(path):
  
    df1 = spark.read.parquet(path)
    df2 = df1.select('symbol','lastPrice','dayHigh','dayLow')
    df2.show()
    print(df2.dtypes)
    # min, max 
    rdd = df2.rdd
    #rdd.show()
    rdd2 = rdd.map(lambda x:(x[0],(x[2],x[3])))
    
    rdd3 = rdd2.reduceByKey(lambda a,b : (max(a[0],b[0]),min(a[1],b[1])))
    # average
    rdd4 = rdd.map(lambda x:(x[0],(x[1],1)))
    rdd5 = rdd4.reduceByKey(lambda x1,x2:(x1[0]+x2[0],x1[1]+x2[1])).mapValues(lambda x:x[0]/x[1])
    rdd6 = rdd3.join(rdd5)
    df = rdd6.toDF()
    return df


opath = read_allFiles(FilenamesList,out_hr)
# aggregation 
df = hour_agg(opath)
# storing aggregation
df.write.parquet(out_hr+"_agg.parquet")         

