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

from pyspark.sql.functions import udf
from Azure_configs import preprocessed_data_path, processed_data_path
from API_configs import etherscan_url, eth_api_key
import datetime
import time
import requests
import sys

nft_name=sys.argv[1]

def get_eth_balance(eth_address):
    param={'ETH_balance':{'module':'account',\
                        'action':'balance',\
                        'address':eth_address,\
                        'tag':'latest',\
                        'apikey':eth_api_key}}
    limit_exceeded=True
    while limit_exceeded==True:
        response=requests.get(etherscan_url,params=param['ETH_balance'])
        message=response.json()
        if message['result']=='Max rate limit reached':
            time.sleep(0.5)
            continue
        else:
            limit_exceeded=False
            return message['result']
    return


def process_eth_data():

    today=datetime.date.today().strftime('%m-%d-%y')

    EScan_parquet_path=f'{preprocessed_data_path}/{today}/EScan/NFT={nft_name}/'

    EScan_reference_DF=spark.read.parquet(EScan_parquet_path)
    
    eth_udf=udf(lambda x : get_eth_balance(x))

    eth_balance_df=EScan_reference_DF.withColumn('ETH_Balance',eth_udf(EScan_reference_DF['owner_address']))
    eth_balance_df.cache()
    eth_balance_df.show(10,truncate=False)
    eth_balance_df.write.mode('overwrite').parquet(f'{processed_data_path}{today}/ETH_Balance/NFT={nft_name}/')

    return

if __name__=="__main__":
    process_eth_data()


