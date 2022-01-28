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

from pyspark.sql.functions import col, format_number
from Azure_configs import structured_data_path, published_data_path
import datetime


def Published_transformation_NFT():

    today=datetime.date.today().strftime('%m-%d-%y')
    
    struct_nft_df=spark.read.parquet(f'{structured_data_path}{today}/NFT_Collection/')
    
    usd_val_df=struct_nft_df.withColumn('USD_Value_1', (struct_nft_df['payment_amt']*struct_nft_df['USD_Rate'])).orderBy(col('USD_Value_1').desc())
    Final_NFT_DF=usd_val_df.select('*',format_number(usd_val_df['USD_Value_1'].cast('float'),2).alias('USD_Value')).drop(col('USD_Value_1'))

    Final_NFT_DF.show(10,truncate=False)
    Final_NFT_DF.write.mode('overwrite').parquet(f'{published_data_path}{today}/Final_NFT/')

    return 


if __name__=="__main__":
    Published_transformation_NFT()
