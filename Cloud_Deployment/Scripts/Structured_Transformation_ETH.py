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

from pyspark.sql.types import FloatType
from pyspark.sql.functions import format_number
from Azure_configs import processed_data_path, structured_data_path
import datetime



def Structured_transformation_eth():
    
    today=datetime.date.today().strftime('%m-%d-%y')

    EScan_ETH_DF=spark.read.parquet(f'{processed_data_path}{today}/ETH_Balance/NFT=**/')

    structured_ETH_DF=EScan_ETH_DF.select(
                    EScan_ETH_DF['owner_address'],\
                    format_number((EScan_ETH_DF['ETH_Balance'].cast(FloatType())/10**18),2).alias('ETH'))

    structured_ETH_DF.show(10,truncate=False)
    structured_ETH_DF.write.mode('overwrite').parquet(f'{structured_data_path}{today}/ETH_Balance/')

    return


if __name__=="__main__":
    Structured_transformation_eth()