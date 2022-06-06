# script to save raw tweet data to local system
# import libraries
import tweepy
import json
import pandas as pd
import os


# savnig in json
def save_json(json_location,tw_dict):
    with open(json_location, 'w') as f:
        json.dump(tw_dict, f, indent=2)
    print("data saved to disk at location",json_location)


# loading from json
def load_json(json_location):
    with open(json_location) as json_file:
        data = json.load(json_file)
    print("data loaded from the disk \n")
    return data




# API Keys (user specific)
api_key = 'YnexwDPzFJqX61H9BBOQvEUBx'
api_key_secret = 'lUE47pTi98oadCkfoiednY2B9P1QwH2YsZamwJ7jMiywNfl6tu'
access_token = '1181621632768462848-Q3Q37PBA4ohatLGSJwX0BpMs8cQjII'
access_token_secret = 'TGYmhkg1u7YMgLUPat0CpwqYKVLgpJLs0YKsiCiw8gcRq'



# API authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


# laod top 25 companies 
companies = pd.read_csv('top_25NSE.csv')
companies_list = list(companies['Company Name'].values)


# important business keywords along with companies
keywords_stock = ['finance','india','business','nse','sensex','bank','investment','stock','money','funds']


# Search list for tweets
query_list = companies_list[:25] + keywords_stock


# extracting tweet data using the above list for timezone INDIA
print('extracting tweet data')
hour_ls = []
for query in query_list:
    try:
        tweets = tweepy.Cursor(api.search_tweets, q=query,result_type='popular',tweet_mode = "extended",lang="en").items(100)
        print(tweets)
    except:
        break
    
    for tweet in tweets:
        json_d = tweet._json
        date = pd.Timestamp.now('Asia/Kolkata')
        json_d['timestamp'] = date.timestamp()
        json_d['hour'] = date.hour
        hour_ls.append(json_d)

print('extracted tweet data for date {}'.format(date))


# save data for that hour in dict
hour_tweet_dict = dict()
date = pd.Timestamp.now('Asia/Kolkata')
hour = date.hour
hour_tweet_dict[str(date.hour)] = hour_ls



# saving local paths
base_location = 'data_sources/tweet_data'
json_path = str(date.date()).replace("-","_") +'.json'
json_save_location = os.path.join(base_location,json_path)



# create base dir if not present
try:
    os.makedirs(base_location)
except:
    pass



# load and save hour data to same dict or else create the new dict and save
try:
    data = load_json(json_save_location)
    data[str(hour)] = hour_tweet_dict
    save_json(json_save_location,data)
except:
    save_json(json_save_location,hour_tweet_dict)
