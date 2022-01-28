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



from API_configs import cryptocompare_api_key,cryptocompare_url
from Env_configs import processed_data_path, preprocessed_data_path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import requests
import sys
import time
import logging


# adding logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#variable for today's date for retriveing and writing file name
today=datetime.date.today().strftime('%m-%d-%y')
spark = SparkSession.builder.master('local[*]').appName('Second_transformation').getOrCreate()
nft_name=sys.argv[1]


def get_usd_price(unix):
    url=f'{cryptocompare_url}{unix}'
    api_key=cryptocompare_api_key
    limit_exceeded=True
    while limit_exceeded==True:
        response=requests.get(url,params={'api_key':api_key})
        message=response.json()
        if message['ETH']:
            limit_exceeded=False
            return message['ETH']['USD']
        else:
            time.sleep(0.5)
            continue
    return



def process_usd_data():

    CCompare_parquet_path=f'{preprocessed_data_path}/{today}/CCompare/NFT={nft_name}/'

    logging.info(f"READING CRYPTOCOMPARE REFERENCE DATA")
    CCompare_reference_DF=spark.read.parquet(CCompare_parquet_path)

    logging.info(f"CALLING DOLLAR PRICE UDF")
    dollar_udf=udf(lambda x : get_usd_price(x))
    usd_price_df=CCompare_reference_DF.withColumn('USD_Rate',dollar_udf(CCompare_reference_DF['unix'])).drop(CCompare_reference_DF['unix'])
    logging.info(f"CACHEING DOLLAR UDF DATAFRAME TO PROCESSED LAYER")
    usd_price_df.cache()
    usd_price_df.show(10,truncate=False)
    logging.info(f"WRITING DOLLAR UDF DATAFRAME TO PROCESSED LAYER")
    usd_price_df.write.mode('overwrite').parquet(f'{processed_data_path}{today}/USD_Price/NFT={nft_name}/')

    return


if __name__=="__main__":
    process_usd_data()
