'''
Processed Transformation (USD)

Uses CryptoCompare reference data for USD data enrichment

Input:

- Data Content:CryptoCompare Reference Data
- Data Type: Parquet
- Data Source: Preprocessed Layer

Output:

- Data Content:CryptoCompare Enriched USD Data 
- Data Type: Parquet
- Data Destination: Processed Layer
'''

from pyspark.sql.functions import udf
from Azure_configs import preprocessed_data_path, processed_data_path
from API_configs import cryptocompare_api_key, cryptocompare_url
import datetime
import time
import requests
import sys



nft_name=sys.argv[1]


def get_usd_price(unix):
    url=f'{cryptocompare_url}{unix}'
    limit_exceeded=True

    while limit_exceeded==True:
        response=requests.get(url,params={'api_key':cryptocompare_api_key})
        message=response.json()
        if message['ETH']:
            limit_exceeded=False
            return message['ETH']['USD']
        else:
            time.sleep(0.5)
            continue
    return



def process_usd_data():

    today=datetime.date.today().strftime('%m-%d-%y')

    CCompare_parquet_path=f'{preprocessed_data_path}/{today}/CCompare/NFT={nft_name}/'

    CCompare_reference_DF=spark.read.parquet(CCompare_parquet_path)

    dollar_udf=udf(lambda x : get_usd_price(x))

    usd_price_df=CCompare_reference_DF.withColumn('USD_Rate',dollar_udf(CCompare_reference_DF['unix'])).drop(CCompare_reference_DF['unix'])
    usd_price_df.cache()
    usd_price_df.show(10,truncate=False)
    usd_price_df.write.mode('overwrite').parquet(f'{processed_data_path}{today}/USD_Price/NFT={nft_name}/')

    return



if __name__=="__main__":
    process_usd_data()
