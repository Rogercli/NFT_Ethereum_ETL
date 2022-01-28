'''
Structured Transformation (NFT)

Joins NFT data with enriched USD data 

Input:
Data Content:
        1) NFT Data 
        2) Enriched USD Data
- Data Type: Parquet
- Data Destination: Processed Layer

Output:

- Data Content:NFT Data 
- Data Type: Parquet
- Data Destination: Structured Layer
'''

from Env_configs import processed_data_path, structured_data_path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import logging


# adding logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.master('local[*]').appName('Structured_transformation').getOrCreate()

def Structured_transformation_nft():
    today=datetime.date.today().strftime('%m-%d-%y')

    logging.info(f"CREATING NFT DATAFRAME FROM PROCESSED")
    processed_nft_df= spark.read.parquet(f'{processed_data_path}{today}/NFT_Collection/')
    
    logging.info(f"CREATING CRYPTOCOMPARE USD DATAFRAME FROM PROCESSED")
    processed_usd_df=spark.read.parquet(f'{processed_data_path}{today}/USD_Price/NFT=**/')

    logging.info(f"JOINING NFT AND USD DATAFRAME")
    structured_nft_DF=processed_nft_df.join(broadcast(processed_usd_df),processed_nft_df['txn_date']==processed_usd_df['txn_date'],'left').drop(processed_usd_df['txn_date'])

    logging.info(f"WRITING NFT DATAFRAME TO STRUCTURED")
    structured_nft_DF.show(10,truncate=False)
    structured_nft_DF.write.mode('overwrite').parquet(f'{structured_data_path}{today}/NFT_Collection/')
    
    return



if __name__=="__main__":
    Structured_transformation_nft()