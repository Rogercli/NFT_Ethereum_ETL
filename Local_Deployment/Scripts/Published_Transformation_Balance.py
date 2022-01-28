'''
Published Transformation (Balance)

Joins ETH and Token Balance Data

Input:

- Data Content: 
        1) ETH Balance Data 
        2) Token Balance Data
- Data Type: Parquet
- Data Destination: Structured Layer

Output:

- Data Content: Final Balance Data 
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



def Published_transformation_balance():
    today=datetime.date.today().strftime('%m-%d-%y')

    logging.info(f"READING TOKEN AND ETH DATAFRAME FROM STRUCTURED")
    
    struct_token_DF=spark.read.parquet(f'{structured_data_path}{today}/Token_Balance/')
    struct_ETH_DF=spark.read.parquet(f'{structured_data_path}{today}/ETH_Balance/')

    logging.info(f"JOINING TOKEN AND ETH DATAFRAME")
    Final_Balance_DF=struct_ETH_DF.join(struct_token_DF,struct_ETH_DF['owner_address']==struct_token_DF['owner_address'],'left').drop(struct_token_DF['owner_address'])
    Final_Balance_DF.cache()
    Final_Balance_DF.show(10,truncate=False)
    Final_Balance_DF.write.mode('overwrite').parquet(f'{published_data_path}{today}/Final_Balance/')


    return

if __name__=="__main__":
    Published_transformation_balance()