# script if we explicity want to create a collection of companies

import pymongo
import pandas as pd
import os
import json


# to connect from local system
# myclient = pymongo.MongoClient("mongodb://10.4.41.42:27017/")

# to connect from within VM
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")


# define Database
sense_stock_db = mongo_client["sense_stock_db"]


# if we want to create a collection of each company name
# read company files to create collections
# laod top 25 companies 
companies = pd.read_csv('top_25NSE.csv')
companies_list = list(companies['Company Name'].values)
# companies_list_lw = [comp.lower() for comp in companies_list]


# create collections if does not exist
for cmp in companies_list:
    pt = sense_stock_db[cmp].insert_one({"company_name":cmp})
    

# print collections
print("created collections for - ")
for coll in sense_stock_db.list_collection_names():
    print(coll)