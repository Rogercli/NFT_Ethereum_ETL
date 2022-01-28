from pyspark.sql import SparkSession
import datetime

basepath='/home/roger/SB/Capstone/NFT_ETH_pipeline/Local_Deployment/Data/'
today=datetime.date.today().strftime('%m-%d-%y')
spark = SparkSession.builder.master('local[*]').appName('Test_Suite').getOrCreate()

def test_raw():
    raw_df=spark.read.json(f'{basepath}Raw/')
    assert raw_df.count()==250

def test_escan_preprocessed():
    EScan_df=spark.read.parquet(f'{basepath}Preprocessed/01-24-22/EScan/NFT=**/')
    assert EScan_df.where('owner_address is Null').count()==0

def test_ccompare_preprocessed():
    CCompare_df=spark.read.parquet(f'{basepath}Preprocessed/01-24-22/CCompare/NFT=**/')
    assert CCompare_df.count()==250
    assert CCompare_df.where('unix is Null').count()==0

def test_usd_processed():
    usd_df=spark.read.parquet(f'{basepath}Processed/01-24-22/USD_Price/NFT=**/')
    assert usd_df.where('USD_Rate is Null').count()==0

def test_nft_processed():
    nft_df=spark.read.parquet(f'{basepath}Processed/01-24-22/NFT_Collection/')
    assert nft_df.count()==250
    assert nft_df.dtypes[5][1]=='timestamp'


def test_token_processed():
    token_df=spark.read.parquet(f'{basepath}Processed/01-24-22/Token_Balance/NFT=**/')
    assert token_df.where('Token_balance is Null').count()==0

def test_eth_processed():
    ETH_df=spark.read.parquet(f'{basepath}Processed/01-24-22/ETH_Balance/NFT=**/')
    assert ETH_df.where('ETH_Balance is Null').count()==0


