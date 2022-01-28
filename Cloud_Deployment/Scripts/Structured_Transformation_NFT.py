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

from pyspark.sql.functions import broadcast
from Azure_configs import processed_data_path, structured_data_path
import datetime

def Structured_transformation_nft():
    today=datetime.date.today().strftime('%m-%d-%y')

    processed_nft_df= spark.read.parquet(f'{processed_data_path}{today}/NFT_Collection/')
    
    processed_usd_df=spark.read.parquet(f'{processed_data_path}{today}/USD_Price/NFT=**/')


    structured_nft_DF=processed_nft_df.join(broadcast(processed_usd_df),processed_nft_df['txn_date']==processed_usd_df['txn_date'],'left').drop(processed_usd_df['txn_date'])
    structured_nft_DF.show(10,truncate=False)
    structured_nft_DF.write.mode('overwrite').parquet(f'{structured_data_path}{today}/NFT_Collection/')
    
    return


if __name__=="__main__":
    Structured_transformation_nft()