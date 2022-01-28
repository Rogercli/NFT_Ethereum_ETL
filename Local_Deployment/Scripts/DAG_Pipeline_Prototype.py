
import os

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta, date





basepath='/home/roger/SB/Capstone/NFT_ETH_pipeline/Local_v2/'
Preprocessed_script_path = f'{basepath}Preprocessed_Transformation.py'
Processed_ETH_script_path=f'{basepath}Processed_Transformation_ETH.py'
Processed_Token_script_path=f'{basepath}Processed_Transformation_Token.py'
Processed_USD_script_path=f'{basepath}Processed_Transformation_USD.py'
Structured_transform_token = f'{basepath}Structured_transformation_token.py'
Structured_transform_nft = f'{basepath}Structured_transformation_nft.py'
Structured_transform_eth = f'{basepath}Structured_transformation_eth.py'
Published_transform_balance=f'{basepath}Published_transformation_Balance.py'
Published_transform_nft=f'{basepath}Published_transformation_NFT.py'




default_args = {
        'start_date': datetime(2022, 1, 17), 
        'execution_timeout':timedelta(seconds=200)}

dag=DAG(
        'Capstone_Pipeline_Main',
        default_args=default_args,
        description='Transforms data from raw layer and writes to preprocessed layer',
        schedule_interval='@daily',
        catchup=True
)



Preprocessed_transform= SparkSubmitOperator(
        application=Preprocessed_script_path, 
        task_id='Preprocessed_transformation',
        conn_id='spark_local',
        dag=dag)


ETH_Enrich_1= SparkSubmitOperator(
        application=Processed_ETH_script_path, 
        task_id='Process_ETH_BoredApes',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['BoredApeYachtClub'],
        dag=dag)
ETH_Enrich_2= SparkSubmitOperator(
        application=Processed_ETH_script_path, 
        task_id='Process_ETH_CoolCats',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Cool Cats'],
        dag=dag)
ETH_Enrich_3= SparkSubmitOperator(
        application=Processed_ETH_script_path, 
        task_id='Process_ETH_CryptoPunks',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['CryptoPunks'],
        dag=dag)
ETH_Enrich_4= SparkSubmitOperator(
        application=Processed_ETH_script_path, 
        task_id='Process_ETH_Doodles',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Doodles'],
        dag=dag)
ETH_Enrich_5= SparkSubmitOperator(
        application=Processed_ETH_script_path, 
        task_id='Process_ETH_Meebits',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Meebits'],
        dag=dag)

Token_Enrich_1= SparkSubmitOperator(
        application=Processed_Token_script_path, 
        task_id='Process_Token_BoredApes',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['BoredApeYachtClub'],
        dag=dag)
Token_Enrich_2= SparkSubmitOperator(
        application=Processed_Token_script_path, 
        task_id='Process_Token_CoolCats',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Cool Cats'],
        dag=dag)
Token_Enrich_3= SparkSubmitOperator(
        application=Processed_Token_script_path, 
        task_id='Process_Token_CryptoPunks',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['CryptoPunks'],
        dag=dag)
Token_Enrich_4= SparkSubmitOperator(
        application=Processed_Token_script_path, 
        task_id='Process_Token_Doodles',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Doodles'],
        dag=dag)
Token_Enrich_5= SparkSubmitOperator(
        application=Processed_Token_script_path, 
        task_id='Process_Token_Meebits',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Meebits'],
        dag=dag)

USD_Enrich_1= SparkSubmitOperator(
        application=Processed_USD_script_path, 
        task_id='Process_USD_BoredApes',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['BoredApeYachtClub'],
        dag=dag)
USD_Enrich_2= SparkSubmitOperator(
        application=Processed_USD_script_path, 
        task_id='Process_USD_CoolCats',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Cool Cats'],
        dag=dag)
USD_Enrich_3= SparkSubmitOperator(
        application=Processed_USD_script_path, 
        task_id='Process_USD_CryptoPunks',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['CryptoPunks'],
        dag=dag)
USD_Enrich_4= SparkSubmitOperator(
        application=Processed_USD_script_path, 
        task_id='Process_USD_Doodles',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Doodles'],
        dag=dag)
USD_Enrich_5= SparkSubmitOperator(
        application=Processed_USD_script_path, 
        task_id='Process_USD_Meebits',
        conn_id='spark_local',
        execution_timeout=timedelta(seconds=200),
        application_args=['Meebits'],
        dag=dag)



Struc_transform_token= SparkSubmitOperator(
        application=Structured_transform_token, 
        task_id='Token_structured_transformation',
        conn_id='spark_local',
        dag=dag)

Struc_transform_eth= SparkSubmitOperator(
        application=Structured_transform_eth, 
        task_id='ETH_structured_transformation',
        conn_id='spark_local',
        dag=dag)

Struc_transform_nft= SparkSubmitOperator(
        application=Structured_transform_nft, 
        task_id='NFT_structured_transformation',
        conn_id='spark_local',
        dag=dag)

Publish_transform_nft= SparkSubmitOperator(
        application=Published_transform_nft, 
        task_id='NFT_Published_transformation',
        conn_id='spark_local',
        dag=dag)

Publish_transform_balance= SparkSubmitOperator(
        application=Published_transform_balance, 
        task_id='Balance_Published_transformation',
        conn_id='spark_local',
        dag=dag)


Preprocessed_transform>>ETH_Enrich_1>>ETH_Enrich_2>>ETH_Enrich_3>>ETH_Enrich_4>>ETH_Enrich_5>>Struc_transform_eth
Preprocessed_transform>>Token_Enrich_1>>Token_Enrich_2>>Token_Enrich_3>>Token_Enrich_4>>Token_Enrich_5>>Struc_transform_token
Preprocessed_transform>>USD_Enrich_1>>USD_Enrich_2>>USD_Enrich_3>>USD_Enrich_4>>USD_Enrich_5>>Struc_transform_nft>>Publish_transform_nft
[Struc_transform_eth,Struc_transform_token]>>Publish_transform_balance
