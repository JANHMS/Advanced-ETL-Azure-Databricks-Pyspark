# Advanced ETL using Azure + Databricks + Pyspark

## Introduction
This project aims to perform data transformation using Databricks `Pyspark` and `SparkSQL`. The data was mounted from an `Azure Data Lake Storage Gen2` and transformed within Databricks. The transformed data was then loaded back to the Datalake. This notebooks were then combined using `Azure Data Fractory`
#### Data Flow project overview
![Data_Factory_overview](https://user-images.githubusercontent.com/45521680/215343684-0259be55-e9d3-4e19-8f09-f5de9f1fd20e.png)
#### Lakehouse project overview
![Data_Lakehouse](https://user-images.githubusercontent.com/45521680/215344008-2da6da03-76bd-420b-9bb4-8a8dced25bb5.png)

## Tools & Libraries
* Databricks Pyspark
* Python
* SparkSQL
* Azure Data Lake Storage Gen2
* Azure Storage Account
* Azure resource group
* Azure Key Vault
* Azure Data Factory
* PowerBI
* Azure Storage Explorer

## Steps
1. Mount the data from the Azure Data Lake Storage Gen2 to Databricks.
2. Use Pyspark within Databricks to perform data transformations using `DELTA TABLES`.
3. Load the transformed data back to the Azure Data Lake Storage Gen2.

## Data
The data can be found in the data folder. There is either the `raw` data or the `raw_incremental_load` data. 
This is basically the same data, but the in `raw_incremental_load` the data is ordered in a way to mimic data, which would normally generated over time and hence use incremental load. 

## Prerequisites
* An active Azure subscription with access to Azure Data Lake Storage Gen2
* Databricks account set up
* Python
* Pyspark
* SQL
* Azure Storage Explorer installed

## Conclusion
This project demonstrates how to perform data transformation using Databricks Pyspark and Azure Data Lake Storage Gen2. This setup can be used for larger scale data processing and storage needs.

## Mount data from ADLS Gen2 to Databricks
- Storing data in the FileStore of Databricks, loading into Workspace notebook and perfroming data science.
- Storing Data in Azure Blob and mounting to Databricks. This includes the following steps:
1. Create Resource Group in Azure.
2. Create Storage account and assign to Resouce group.
3. App registration (create a managed itenditiy), which we will use to connect Databricks to storage account.
3.1 Create a client secret and copy.
4. Create Key vault (assign to same resource group)
4.1. Add the cleint secret here.
5. Create secret scope within Databricks.
5.1 Use the keyvault DNS (url) and the ResourceID to allow Databricks to access the key valuts secrets within a specific scope.
6. Use this scope to retreive secrets and connect to storage acount container, where data is stored in Azure:

```
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "<appId>",
       "fs.azure.account.oauth2.client.secret": "<clientSecret>",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant>/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
```

7. Finally we can mount the data:
```
dbutils.fs.mount(
source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/folder1",
mount_point = "/mnt/flightdata",
extra_configs = configs)
```
8. Now we can load the data from the MountPoint into a Dataframe and perform actions.


## How to mount data from Azure Data Lake Storage Gen2 to Databricks.  

### Used Azure Services
<img width="739" alt="Screenshot 2023-01-29 at 11 14 32" src="https://user-images.githubusercontent.com/45521680/215344570-bf7415cb-0940-4848-a983-9d12bd687d00.png">

### Azure Data Factory 
<img width="347" alt="Screenshot 2023-01-29 at 18 28 28" src="https://user-images.githubusercontent.com/45521680/215344532-2a6c4bb4-cf04-445c-b514-b70a369b243c.png">
<img width="632" alt="Screenshot 2023-01-29 at 11 45 39" src="https://user-images.githubusercontent.com/45521680/215344561-c2461509-1648-491f-a417-76b8355c0543.png">
![Azure Data Factory](https://user-images.githubusercontent.com/45521680/215344336-9e1580c7-43d0-4e77-b8ce-a96b4d9921ea.png)

### Potential project architecture (Big picture)
<img width="508" alt="Screenshot 2023-01-25 at 14 14 10" src="https://user-images.githubusercontent.com/45521680/214684174-7fb3e321-588a-4808-8f46-af497fff6ebc.png">

### Incremetal load architecture
![incremen<img width="604" alt="Screenshot 2023-01-29 at 17 04 17" src="https://user-images.githubusercontent.com/45521680/215344558-b368ee72-0734-4fdb-9fc8-80f3f9d76582.png">
tal_load_pipeline](https://user-images.githubusercontent.com/45521680/215343729-7da58562-4b5f-47d9-a9ff-c44e17dbc46e.png)

### Databricks Architecture
<img width="1390" alt="Screenshot 2023-01-25 at 13 28 10" src="https://user-images.githubusercontent.com/45521680/214684129-b83f23c8-ae6d-4cf7-942e-fea5fb152ef7.png">
