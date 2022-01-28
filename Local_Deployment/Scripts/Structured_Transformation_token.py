'''
Structured Transformation (Token)

Transforms EtherScan Token Enriched data 

Input:

- Data Content:EtherScan Enriched Token Data 
- Data Type: Parquet
- Data Destination: Processed Layer

Output:

- Data Content:Token Balance Data 
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

def Structured_transformation_token():
    today=datetime.date.today().strftime('%m-%d-%y')

    logging.info(f"CREATING ETHERSCAN TOKEN DATAFRAME FROM PROCESSED")
    
    EScan_token_DF=spark.read.parquet(f'{processed_data_path}{today}/Token_Balance/NFT=**/')
    structured_token_DF=EScan_token_DF.select(\
    EScan_token_DF['owner_address'],\
    format_number(EScan_token_DF['token_balance']['Wrapped_eth'].cast(FloatType())/10**18,2).alias('WETH'),\
    format_number(EScan_token_DF['token_balance']['Usdc'].cast(FloatType())/10**6,2).alias('USDC'),\
    format_number(EScan_token_DF['token_balance']['Tether'].cast(FloatType())/10**6,2).alias('Tether')
    )
    logging.info(f"WRITING ETHERSCAN TOKEN DATAFRAME TO STRUCTURED")
    structured_token_DF.show(10,truncate=False)
    structured_token_DF.write.mode('overwrite').parquet(f'{structured_data_path}{today}/Token_Balance/')


    return


if __name__=="__main__":
    Structured_transformation_token()