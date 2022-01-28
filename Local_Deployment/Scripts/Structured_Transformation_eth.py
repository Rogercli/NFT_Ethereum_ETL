'''
Structured Transformation (ETH)

Transforms EtherScan ETH Enriched data 

Input:

- Data Content:EtherScan Enriched ETH Data 
- Data Type: Parquet
- Data Destination: Processed Layer

Output:

- Data Content:ETH Balance Data 
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


def Structured_transformation_eth():
    today=datetime.date.today().strftime('%m-%d-%y')


    logging.info(f"CREATING ETHERSCAN ETH DATAFRAME FROM PROCESSED")
    EScan_ETH_DF=spark.read.parquet(f'{processed_data_path}{today}/ETH_Balance/NFT=**/')
    structured_ETH_DF=EScan_ETH_DF.select(
    EScan_ETH_DF['owner_address'],\
    format_number((EScan_ETH_DF['ETH_Balance'].cast(FloatType())/10**18),2).alias('ETH'))

    logging.info(f"WRITING ETHERSCAN TOKEN DATAFRAME TO STRUCTURED")
    structured_ETH_DF.show(10,truncate=False)
    structured_ETH_DF.write.mode('overwrite').parquet(f'{structured_data_path}{today}/ETH_Balance/')

    return




if __name__=="__main__":
    Structured_transformation_eth()