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

from pyspark.sql.types import FloatType
from pyspark.sql.functions import format_number
from Azure_configs import processed_data_path, structured_data_path
import datetime



def Structured_transformation_token():

    today=datetime.date.today().strftime('%m-%d-%y')
    
    EScan_token_DF=spark.read.parquet(f'{processed_data_path}{today}/Token_Balance/NFT=**/')

    structured_token_DF=EScan_token_DF.select(\
                        EScan_token_DF['owner_address'],\
                        format_number(EScan_token_DF['token_balance']['Wrapped_eth'].cast(FloatType())/10**18,2).alias('WETH'),\
                        format_number(EScan_token_DF['token_balance']['Usdc'].cast(FloatType())/10**6,2).alias('USDC'),\
                        format_number(EScan_token_DF['token_balance']['Tether'].cast(FloatType())/10**6,2).alias('Tether')
                        )

    structured_token_DF.show(10,truncate=False)
    structured_token_DF.write.mode('overwrite').parquet(f'{structured_data_path}{today}/Token_Balance/')

    return


if __name__=="__main__":
    Structured_transformation_token()