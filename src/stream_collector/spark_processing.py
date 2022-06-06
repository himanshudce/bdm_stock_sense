 # set sense_stock in python path to consider as module
# import pyspark libarries
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField,StringType, FloatType, ArrayType,IntegerType,TimestampType, LongType, BinaryType, MapType
import datetime


# python libraries
import pandas as pd
import json
import re
from pymongo import MongoClient
from pytz import timezone
from datetime import datetime


# NLP library
from textblob import TextBlob





########## ==================== Supporting Functions ==============================================

# remove null rows from data
def remove_null_rows(df):
    df = df.filter(F.col("text").isNotNull())
    df = df.filter(F.col("favorite_count").isNotNull())
    df = df.filter(F.col("retweet_count").isNotNull())
    return df


# The UDF function to clean the tweets
def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # users removal from the data 
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove all the puntuations
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove the number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove the hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet


# udf to get sentiments
def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'


# udf to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


# udf to match the companies, by passing companies global list
def get_comp(tweet):
    comp = []
    for company in companies_list:
        if company.lower() in tweet.lower():
            comp.append(company)
    return comp


def get_date(time):
    str_time_org = str(time).split('.')[0]
    dt = datetime.strptime(str_time_org, f"%Y-%m-%d %H:%M:%S")
    dt_ist = dt.astimezone(timezone('Asia/Kolkata'))
    str_time = str(dt_ist).split()
    date = str_time[0].replace('-','_')
    return date


def get_hour(time):
    str_time_org = str(time).split('.')[0]
    dt = datetime.strptime(str_time_org, f"%Y-%m-%d %H:%M:%S")
    dt_ist = dt.astimezone(timezone('Asia/Kolkata'))
    str_time = str(dt_ist).split()
    hour = str_time[1].split(":")[0]
    return int(hour)


def get_minute(time):
    str_time_org = str(time).split('.')[0]
    dt = datetime.strptime(str_time_org, f"%Y-%m-%d %H:%M:%S")
    dt_ist = dt.astimezone(timezone('Asia/Kolkata'))
    str_time = str(dt_ist).split()
    minute = str_time[1].split(":")[1]
    return int(minute)



def sort_and_save(grouped_tweets,batchID):
    grouped_tweets_filtered = grouped_tweets.withColumn('top_N_tweets',sort_group(F.col('top_tweets')))
    query = grouped_tweets_filtered \
        .write \
        .outputMode("complete") \
        .format("console") \
        .start().awaitTermination()
    grouped_tweets.show()



def sort_gr(tweet_dict):
    return sorted(tweet_dict, key=lambda d: d['polarity'], reverse=True)[:10]


def write_row(batch_df , batch_id):
    batch_df.write.format("mongo").mode("append").save()
    pass


def write_mongo_row(df, epoch_id):
    grouped_tweets_filtered = df.withColumn('top_N_tweets',sort_group(F.col('top_tweets')))
    grouped_tweets_filtered_df = grouped_tweets_filtered.select(F.col('companies'),F.col('top_N_tweets'),F.col('date'),F.col('hour'),F.col('minute'))
    grouped_tweets_filtered_df.write.format("mongo").mode("append").option("database", "sense_stock_db") \
        .option("collection", "tweet_data").save()





#====================================================================================



if __name__=='__main__':
    # load companies
    companies = pd.read_csv("top_25NSE.csv")
    companies_list = list(companies["Company Name"].values)
    # companies_list.lower()


    # create spark session
    spark = SparkSession \
    .builder \
    .appName("TweetPipeline") \
    .config("spark.mongodb.input.uri","mongodb://localhost:27017") \
    .config("spark.mongodb.output.uri","mongodb://localhost:27017") \
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0') \
    .getOrCreate()


    # schema that we need
    tweetSchema = StructType([StructField("text", StringType(), True),
    StructField("favorite_count", IntegerType(), True),
    StructField("retweet_count", IntegerType(), True),
    ])


    # # use json files as stream
    #df = spark.readStream.schema(tweetSchema).json('/Users/himanshu/sense_stock/stream_example/')

    # #read simple json file without stream 
    # df = spark.read.option("multiline","true").json('/Users/himanshu/sense_stock/oht.json')


    #read from kafka stream
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("failOnDataLoss","false") \
        .load()


    #select columns based on schema
    df = df.select(F.from_json(df.value.cast("string"),tweetSchema).alias("raw_tweets"),"timestamp")
    df = df.select("raw_tweets.*","timestamp")
    
    # show the schema
    df.printSchema()




    # ###### ====================  UDF definations ====================================

    # to clean tweets
    clean_tweets = F.udf(cleanTweet, StringType())

    # to get sentiment and polarity
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    # function to connect with companies
    get_companies = F.udf(lambda x:get_comp(x),ArrayType(StringType()))

    get_date_F = F.udf(lambda x: get_date(x),StringType())
    get_hour_F = F.udf(lambda x: get_hour(x),IntegerType())
    get_minute_F = F.udf(lambda x: get_minute(x),IntegerType())

    # sort each group and take top 2
    sort_group = F.udf(lambda x:sort_gr(x),ArrayType(MapType(StringType(), StringType())))

    # ========================= spark processing pipeline ==============================
   
    # adding cleaned tweets 
    processed_tw_df = df.withColumn('processed_text', clean_tweets(F.col('text')))

    # adding tweet sentiments
    polarity_tweets = processed_tw_df.withColumn('polarity', polarity(F.col('processed_text')))
    sentiment_tweets = polarity_tweets.withColumn('sentiment', sentiment(F.col('polarity')))



    # adding matched companies
    company_tweets = sentiment_tweets.withColumn('company_list',get_companies(F.col('text')))
    company_tweets = company_tweets.filter(F.size('company_list')>0) # filtiring rows if no company found
    company_tweets = company_tweets.withColumn('companies', F.explode('company_list')) # exploding rows to get multiple companies from the list 


    # adding total support count (favorite count + share count) and sorting based on that
    support_tweet = company_tweets.withColumn('support_count', F.expr("favorite_count + retweet_count"))

    # convert timestamp to date, hour, minute and to save in mongo
    support_tweet_date = support_tweet.withColumn('date',get_date_F(F.col('timestamp')))
    support_tweet_hour = support_tweet_date.withColumn('hour',get_hour_F(F.col('timestamp')))
    support_tweet_minute = support_tweet_hour.withColumn('minute',get_minute_F(F.col('timestamp')))



    # map tweets to make dict like structure and keep text, count and rank
    mapped_tweets = support_tweet_minute.withColumn("prop_collection",F.create_map(
            F.lit("text"),F.col("text"),
            F.lit("support_count"),F.col("support_count"),
            F.lit("timestamp"),F.col("timestamp"),
            F.lit("sentiment"),F.col("sentiment"),
            F.lit("polarity"),F.col("polarity"),
            ))


    # group the tweets by specific companies
    # grouped_tweets = mapped_tweets.groupBy('companies').agg(F.collect_list('prop_collection').alias('top_tweets'))
    grouped_tweets = mapped_tweets.withWatermark("timestamp", "60 seconds").groupBy(F.window("timestamp", windowDuration = "60 seconds", slideDuration=" 60 seconds"),'companies').agg(F.collect_list('prop_collection').alias('top_tweets'), F.first('date').alias('date'),F.first('hour').alias('hour'),F.first('minute').alias('minute'))
     


    # =============================  Visualization and saving Part ======================== #


    grouped_tweets.writeStream\
        .foreachBatch(write_mongo_row)\
        .start().awaitTermination()


    # to visualize the tweets
    # grouped_tweets.foreachBatch(sort_and_save).start()

    # to visualize data on terminal
    # query = grouped_tweets \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start().awaitTermination() 