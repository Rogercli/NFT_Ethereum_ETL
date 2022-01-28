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

import datetime
from pyspark.sql.functions import *
today=datetime.date.today().strftime('%m-%d-%y')


# mount azure blob container to make files accessible to Databricks cluster
def mount_blob(): 
    if not any(mount.mountPoint == '/mnt/capstoneblob/' for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(ContainerName, storageAccountName),
            mount_point = "/mnt/capstoneblob",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey})
        except Exception as e:
            print("already mounted. Try to unmount first")
    return 




# add dbfs file path of modules to sparkcontext in order to import to a notebook or python script for access
spark.sparkContext.addPyFile("dbfs:/mnt/capstoneblob/Cloud_Deployment/Azure_configs.py")
from Azure_configs import *


def nft_raw_tranformation():
    
    Opensea_df1= spark.read.json(raw_data_file_path)
    
    Opensea_df2=Opensea_df1.select(
        Opensea_df1['asset_contract']['name'].alias('NFT'),\
        Opensea_df1['token_id'],\
        Opensea_df1['num_sales'],\
        Opensea_df1['owner']['user']['username'].alias('username'),\
        Opensea_df1['owner']['address'].alias('owner_address'),\
        to_timestamp(Opensea_df1['last_sale']['event_timestamp']).alias('txn_date'),\
        (Opensea_df1['last_sale']['total_price']/10**18).alias('payment_amt'),\
        Opensea_df1['last_sale']['payment_token']['symbol'].alias('payment_type'))

    Reference_df=Opensea_df2.select(
        Opensea_df2['NFT'],\
        Opensea_df2['owner_address'],\
        Opensea_df2['txn_date'])
        
    Opensea_df2.show(10,truncate=False)
    Opensea_df2.write.mode('overwrite').parquet(f'{processed_data_path}{today}/NFT_Collection/')

    return Reference_df


def create_ccompare_reference(reference):

    unix_df=reference.select(
        reference['NFT'],\
        reference['txn_date'])\
        .withColumn('unix',unix_timestamp(reference['txn_date']))
    unix_df.show(10,truncate=False)
    unix_df.write.partitionBy('NFT').mode('overwrite').parquet(f'{preprocessed_data_path}{today}/CCompare/')
    
    return

def create_escan_reference(reference):

    eth_addr_df=reference.select(
        reference['NFT'],\
        reference['owner_address'])\
        .dropDuplicates(['owner_address'])
    eth_addr_df.show(10,truncate=False)
    eth_addr_df.write.partitionBy('NFT').mode('overwrite').parquet(f'{preprocessed_data_path}{today}/EScan/')

    return

def main():
    mount_blob()
    reference_df=nft_raw_tranformation()
    reference_df.cache()
    reference_df.show()
    create_escan_reference(reference_df)
    create_ccompare_reference(reference_df)
    return

if __name__=="__main__":
    main()
