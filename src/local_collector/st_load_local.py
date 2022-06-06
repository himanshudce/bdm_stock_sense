# import libraries
import pandas as pd
from nsetools import Nse
import os


# defining base location
base_location = '/home/bdm/data_sources/stock_data'

# create dir if not exist
try:
    os.makedirs(base_location)
except:
    pass

# define file name according to indian time zone (NSE stock)
date = pd.Timestamp.now(tz='Asia/Kolkata')
date_str = str(date.date()).replace("-","_")
print("extracting data for date ", date_str)
base_location_date= os.path.join(base_location,date_str)
print(base_location_date)

hour = str(date.hour)
min = str(date.minute)

base_location_hour= os.path.join(base_location_date,hour)
# defing NSE api object
nse = Nse() #stock API


# create today's date dir
try:
    os.makedirs(base_location_date)
except:
    pass

#create hour's directory

# try:
#     os.makedirs(base_location_hour)
# except:
#     pass

# load top 25 companies
companies = pd.read_csv('/home/bdm/sense_stock/top_25NSE.csv')
companies_list = list(companies['Company Name'].values)



# function to pull data from nse API
def pull_stock_data(nse,companies_list):
    stock =pd.DataFrame()
    
    for i in companies_list:

        q= nse.get_quote(i)
        if q is None:
            continue
        else:
            stock = stock.append(pd.DataFrame(q,index=[0]),ignore_index=True)
    return stock




    
# csv file location and timestamp 
#date = pd.Timestamp.now(tz='Asia/Kolkata')

csv_name = 'stock_data_'+hour+'_'+min+'.csv'
#csv_name = 'stock_data_'+'_'+min+'.csv'
csv_location = os.path.join(base_location_date,csv_name)
print("\n saving data location is ",csv_location)

# converting data to csv and savnig to local
stock_data = pull_stock_data(nse,companies_list)
stock_data_csv = stock_data.to_csv()
stock_data.to_csv(csv_location)

print("data saved to disk \n")
    
