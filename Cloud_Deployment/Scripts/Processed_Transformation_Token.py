'''
Processed Transformation (Token)

Uses EtherScan reference data for Token data enrichment

Input:

- Data Content:EtherScan Reference Data
- Data Type: Parquet
- Data Source: Preprocessed Layer

Output:

- Data Content:EtherScan Enriched Token Data 
- Data Type: Parquet
- Data Destination: Processed Layer
'''

from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import udf
from Azure_configs import preprocessed_data_path, processed_data_path
from API_configs import etherscan_url, token_api_key
import datetime
import time
import requests
import sys


nft_name=sys.argv[1]
token_addresses={
    'Wrapped_eth':'0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    'Tether':'0xdac17f958d2ee523a2206206994597c13d831ec7',
    'Usdc':'0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
}


def get_token_balance(eth_address):
    new_dict={}
    
    for i in token_addresses.keys():
        param={'token_balance':{'module':'account',
                                'action':'tokenbalance',\
                                'contractaddress':token_addresses[i],\
                                'address':eth_address,\
                                'tag':'latest','apikey':token_api_key}}
        limit_exceeded=True
        while limit_exceeded==True:
            response=requests.get(etherscan_url,params=param['token_balance'])
            message=response.json()
            if message['result']=='Max rate limit reached':
                time.sleep(0.5)
                continue
            else:
                limit_exceeded=False
                new_dict[i]=message['result']
        
    return new_dict



def process_token_data():

    today=datetime.date.today().strftime('%m-%d-%y')
    
    EScan_parquet_path=f'{preprocessed_data_path}/{today}/EScan/NFT={nft_name}/'

    EScan_reference_DF=spark.read.parquet(EScan_parquet_path)

    token_udf=udf(lambda x : get_token_balance(x),MapType(StringType(),StringType()))
    
    token_balance_df=EScan_reference_DF.withColumn('Token_Balance',token_udf(EScan_reference_DF['owner_address']))
    token_balance_df.cache()
    token_balance_df.show(10,truncate=False)
    token_balance_df.write.mode('overwrite').parquet(f'{processed_data_path}{today}/Token_Balance/NFT={nft_name}/')
    
    return

if __name__=="__main__":
    process_token_data()
