# script to save raw tweet data to mongodb database
# import libraries
import pymongo
import pandas as pd
import os
import json

# to connect from local system
# myclient = pymongo.MongoClient("mongodb://10.4.41.42:27017/")

# to connect from within VM
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")


# define Database
tweetdb = mongo_client["tweetdb"]



# loading data from json
def load_json(json_location):
    with open(json_location) as json_file:
        data = json.load(json_file)
    print("data loaded from the disk \n")
    return data



# get the date of the day
date = pd.Timestamp.now('Asia/Kolkata')


# defining collection name in tweetdb for the date
col = 'date'+'_'+str(date.date()).replace("-","_")
tw_date_col = tweetdb[col]


# json saved path to load from local
base_location = 'data_sources/tweet_data'
json_path = str(date.date()).replace("-","_") +'.json'
json_save_location = os.path.join(base_location,json_path)


# loading Data in MongoDB
data = load_json(json_save_location)
for key,val in data.items():
    ins = tw_date_col.insert_many(val)
print('object ids loaded are - ',ins.inserted_ids)
print("================================================================================================")




#=========================
"""
basic mongo queries id needed

1. to find
temp = tweetdb.tw_date_col.find()

#2. to print
tw_date_col = tweetdb['date_2022_04_21']
for x in tw_date_col.find():
  print(x)


3.to drop collection
tw_date_col.drop()
"""


