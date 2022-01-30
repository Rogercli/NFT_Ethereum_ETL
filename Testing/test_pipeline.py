
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
    eth_df=spark.read.parquet(f'{basepath}Processed/01-24-22/ETH_Balance/NFT=**/')
    assert eth_df.where('ETH_Balance is Null').count()==0




def test_nft_structured():
    nft_df=spark.read.parquet(f'{basepath}Structured/01-24-22/NFT_Collection/')
    schema=[('NFT', 'string'),
            ('token_id', 'string'),
            ('num_sales', 'bigint'),
            ('username', 'string'),
            ('owner_address', 'string'),
            ('txn_date', 'timestamp'),
            ('payment_amt', 'double'),
            ('payment_type', 'string'),
            ('USD_Rate', 'string')]
    assert nft_df.dtypes==schema
    assert nft_df.count()==256
    assert nft_df.where('USD_Rate is Null').count()==0
    
def test_token_structured():
    token_df=spark.read.parquet(f'{basepath}Structured/01-24-22/Token_Balance/')
    schema=[('owner_address', 'string'),
            ('WETH', 'string'),
            ('USDC', 'string'),
            ('Tether', 'string')]
    assert token_df.dtypes==schema
    assert token_df.where('WETH is Null').count()==0
    assert token_df.where('USDC is Null').count()==0
    assert token_df.where('Tether is Null').count()==0

def test_eth_structured():
    eth_df=spark.read.parquet(f'{basepath}Structured/01-24-22/ETH_Balance/')
    schema=[('owner_address', 'string'),
            ('ETH', 'string')]
    assert eth_df.dtypes==schema
    assert eth_df.where('ETH is Null').count()==0
    



def test_nft_published():
    nft_df=spark.read.parquet(f'{basepath}Published/01-24-22/Final_NFT/')
    schema=[('NFT', 'string'),
            ('token_id', 'string'),
            ('num_sales', 'bigint'),
            ('username', 'string'),
            ('owner_address', 'string'),
            ('txn_date', 'timestamp'),
            ('payment_amt', 'double'),
            ('payment_type', 'string'),
            ('USD_Rate', 'string'),
            ('USD_Value', 'double')]
    assert nft_df.dtypes==schema
    assert nft_df.count()==256
    assert nft_df.where('USD_Value is Null').count()==0
    
def test_balance_published():
    balance_df=spark.read.parquet(f'{basepath}Published/01-24-22/Final_Balance/')
    schema=[('owner_address', 'string'),
            ('ETH','string'),
            ('WETH', 'string'),
            ('USDC', 'string'),
            ('Tether', 'string')]
    assert balance_df.dtypes==schema
    assert balance_df.where('ETH is Null').count()==0
    assert balance_df.where('WETH is Null').count()==0
    assert balance_df.where('USDC is Null').count()==0
    assert balance_df.where('Tether is Null').count()==0

