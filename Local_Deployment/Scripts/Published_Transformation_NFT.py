'''
Published Transformation (NFT)

Adds in data aggregations

Input:

- Data Content: NFT Data
- Data Type: Parquet
- Data Destination: Structured Layer

Output:

- Data Content: Final NFT Data 
- Data Type: Parquet
- Data Destination: Published Layer
'''

from Env_configs import structured_data_path,published_data_path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import logging




# adding logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.master('local[*]').appName('Structured_transformation').getOrCreate()

def Published_transformation_NFT():
    today=datetime.date.today().strftime('%m-%d-%y')

    logging.info(f"READING NFT DATAFRAME FROM STRUCTURED")
    
    struct_NFT_DF=spark.read.parquet(f'{structured_data_path}{today}/NFT_Collection/')
    Final_NFT_DF=struct_NFT_DF.withColumn('USD_Value', (struct_NFT_DF['payment_amt']*struct_NFT_DF['USD_Rate'])).orderBy(col('USD_Value').desc())

    logging.info(f"ADDING USD_TOTAL COLUMN TO NFT DATAFRAME")
    logging.info(f"WRITING NFT DATAFRAME TO PUBLISHED")
    Final_NFT_DF.cache()
    Final_NFT_DF.show(10,truncate=False)
    Final_NFT_DF.write.mode('overwrite').parquet(f'{published_data_path}{today}/Final_NFT/')

    return


if __name__=="__main__":
    Published_transformation_NFT()