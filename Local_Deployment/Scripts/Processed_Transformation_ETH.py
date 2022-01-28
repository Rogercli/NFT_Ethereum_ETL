'''
Processed Transformation (ETH)

Uses EtherScan reference data for ETH data enrichment

Input:

- Data Content:EtherScan Reference Data
- Data Type: Parquet
- Data Source: Preprocessed Layer

Output:

- Data Content:EtherScan Enriched ETH Data 
- Data Type: Parquet
- Data Destination: Processed Layer
'''


from API_configs import etherscan_url,eth_api_key
from Env_configs import processed_data_path, preprocessed_data_path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import time
import sys
import requests

import logging
spark = SparkSession.builder.master('local[*]').appName('Second_transformation').getOrCreate()

# adding logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#variable for today's date for retriveing and writing file name
today=datetime.date.today().strftime('%m-%d-%y')

nft_name=sys.argv[1]

def get_eth_balance(eth_address):
    url=etherscan_url
    api_key=eth_api_key
    param={'ETH_balance':{'module':'account',\
                        'action':'balance',\
                        'address':eth_address,\
                        'tag':'latest',\
                        'apikey':api_key}}
    limit_exceeded=True
    while limit_exceeded==True:
        response=requests.get(url,params=param['ETH_balance'])
        message=response.json()
        if message['result']=='Max rate limit reached':
            time.sleep(0.5)
            continue
        else:
            limit_exceeded=False
            return message['result']
    return


def process_eth_data():
    EScan_parquet_path=f'{preprocessed_data_path}/{today}/EScan/NFT={nft_name}/'

    logging.info(f"READING ETHERSCAN REFERENCE DATA")
    EScan_reference_DF=spark.read.parquet(EScan_parquet_path)
    
    logging.info(f"CALLING ETH BALANCE UDF")
    eth_udf=udf(lambda x : get_eth_balance(x))
    eth_balance_df=EScan_reference_DF.withColumn('ETH_Balance',eth_udf(EScan_reference_DF['owner_address']))
    logging.info(f"CACHEING ETH UDF DATAFRAME TO PROCESSED LAYER")
    eth_balance_df.cache()
    eth_balance_df.show(10,truncate=False)
    logging.info(f"WRITING ETH UDF DATAFRAME TO PROCESSED LAYER")
    eth_balance_df.write.mode('overwrite').parquet(f'{processed_data_path}{today}/ETH_Balance/NFT={nft_name}/')
    return

if __name__=="__main__":
    process_eth_data()
