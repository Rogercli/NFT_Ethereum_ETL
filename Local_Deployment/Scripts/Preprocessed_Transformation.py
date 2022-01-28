'''
Preprocessed Transformation 

Creates OpenSea NFT dataframe from raw Json data and then creates reference dataframes
for CryptoCompare and EtherScan in order to perform data enrichment in the next stage

Input:

- Data Content: OpenSea NFT Data 
- Data Type: JSON
- Data Source: Raw Layer

Output:

- Data Content:
        1) NFT Data 
        2) CryptoCompare Reference Data
        3) EtherScan Reference Data
- Data Type: Parquet
- Data Destination: Preprocessed Layer
'''




from Env_configs import raw_data_file_path, preprocessed_data_path,processed_data_path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import logging



# adding logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

today=datetime.date.today().strftime('%m-%d-%y')
spark = SparkSession.builder.master('local[*]').appName('Preprocessed_transformation').getOrCreate()
    
def nft_raw_tranformation():
    
    Opensea_df1= spark.read.json(raw_data_file_path)
    
    logging.info(f"CREATING OPENSEA NFT DATAFRAME FROM OPENSEA JSON IN RAW LAYER")
    Opensea_df2=Opensea_df1.select(
        Opensea_df1['asset_contract']['name'].alias('NFT'),\
        Opensea_df1['token_id'],\
        Opensea_df1['num_sales'],\
        Opensea_df1['owner']['user']['username'].alias('username'),\
        Opensea_df1['owner']['address'].alias('owner_address'),\
        to_timestamp(Opensea_df1['last_sale']['event_timestamp']).alias('txn_date'),\
        (Opensea_df1['last_sale']['total_price']/10**18).alias('payment_amt'),\
        Opensea_df1['last_sale']['payment_token']['symbol'].alias('payment_type'))
    
    logging.info(f"CREATING REFERENCE DATAFRAME")
    Reference_df=Opensea_df2.select(
        Opensea_df2['NFT'],\
        Opensea_df2['owner_address'],\
        Opensea_df2['txn_date']).cache()

    logging.info(f"WRITING OPENSEA NFT DATAFRAME TO PROCESSED LAYER")
    # writing OpenSea DF to pre-processed
    Opensea_df2.show(10,truncate=False)
    Opensea_df2.write.mode('overwrite').parquet(f'{processed_data_path}{today}/NFT_Collection/')
    return Reference_df

def create_ccompare_reference(reference):
    logging.info(f"CREATING CRYPTOCOMPARE REFERENCE DATAFRAME")
    unix_df=reference.select(
        reference['NFT'],\
        reference['txn_date'])\
        .withColumn('unix',unix_timestamp(reference['txn_date']))

    logging.info(f"WRITING CRYPTOCOMPARE DATAFRAME TO PRE-PROCESSED")
    unix_df.show(10,truncate=False)
    unix_df.write.partitionBy('NFT').mode('overwrite').parquet(f'{preprocessed_data_path}{today}/CCompare/')
    
    return


def create_escan_reference(reference):
    logging.info(f"CREATING ETHERSCAN REFERENCE DATAFRAME")
    eth_addr_df=reference.select(
        reference['NFT'],\
        reference['owner_address'])\
        .dropDuplicates(['owner_address'])

    logging.info(f"WRITING ETHERSCAN DATAFRAME TO PRE-PROCESSED")
    # writing Etherscan reference DF to pre-processed
    eth_addr_df.show(10,truncate=False)
    eth_addr_df.write.partitionBy('NFT').mode('overwrite').parquet(f'{preprocessed_data_path}{today}/EScan/')

    return

def main():
    
    reference_df=nft_raw_tranformation()
    create_escan_reference(reference_df)
    create_ccompare_reference(reference_df)
    return

if __name__=="__main__":
    main()
