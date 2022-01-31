# NFT_Ethereum_ETL

## Overview
The goal for this was project was to develop and deploy a scalable pipeline on the cloud using data collected from 3 seperate API's.
A primary data set from which this pipeline extracts from is OpenSea's NFT marketplace, which holds data regarding NFT collections such as transaction price, transaction date, owner_address, etc. The data for highest valued NFT collections will first be extracted and then enriched with data from 
CrytoCompare and EtherScan to ultimately enable analysis of NFT price/transactions statistics as well as analysis of each owner's Ethereum wallet token balances. 
  The projects utilizes solutions such as:
  1) Azure Blob Storage
  2) Azure DataBricks Cluster
  3) Spark 
  4) Airflow and DataBricks Orchestrator

**Step 1**: Data Acquisition
* Python script ingesting sample data from OpenSea NFT market place and storing the raw data

**Step 2**: Data Exploration
* Notebook for exploring raw OpenSea data, as well as for understanding extraction/enrichment methods and data from EtherScan and CryptoCompare 

**Step 3**: Pipeline Prototype
* Pipeline using Airflow to schedule and orchestrate the complete end-to-end steps of extracting and enriching data from all API sources, performing necessary transformations, and loading to persistence layers  

**Step 4**: Pipeline Deployment
* Upload data to Azure Blob and deploy scripts to DataBricks cluster using DataBricks built-in orchestration tool to schedule jobs

**Step 5**. Pipeline Monitoring:
* Uses Ganglia dashboard, a distributed monitoring system for high-performance computing systems to check metrics related to CPU, memory usage, network

## Diagram
![alt text](https://github.com/Rogercli/NFT_Ethereum_ETL/blob/main/Capstone_Architecture.png?raw=true)

## Directory Details
- **Cloud_Deployment**: Scripts, reference rotebooks, and images relating to pipeline cloud deployment using DataBricks Cluster
- **Local_Deployment**: Scripts, reference rotebooks, and images relating to pipeline local deployment 
- **Exploratory_Data_Analysis**: Notebooks used to understand source data from API's and determine methods of extraction and transformation
- **Sample_Data_Acquisition**: Script for acquiring sample data from API
- **Testing**: Script for testing and validating each stage of pipeline
## Local
- **Dependencies**: Spark, Airflow, and source data available on local environment
- **Run**: Run local python and DAG scripts with the correct configurations and paths

## Production
- **Dependencies**: Microsoft Azure Account, with Databricks cluster and Blob container resources set up. Spark configurations updated with Microsoft Account and Blob information. 
- **Set-up**: Using either Azcopy or Azure Storage-Explorer, populate the Blob container with source data. In addition, in order to make our data accessible by DataBricks, we need to mount our Blob container onto the DataBricks cluster. After mounting data, if there are certain modules that need to be accessed by Pyspark scripts (e.g. API configs etc),then we need to add the DFBS filepath of modules to sparkcontext using spark.sparkContext.addPyFile('dbfs:/mnt/path/to/module/
)
- **Run**: Use DataBricks orchestration tool to schedule jobs, add dependencies, and pass in parameters. In this version, we are using the orchestration tool to schedule Python scripts rather than notebooks. 
