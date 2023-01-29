# Advanced ETL using Azure + Databricks + Pyspark

## Introduction
This project aims to perform data transformation using Databricks Pyspark and SparkSQL. The data was mounted from an Azure Data Lake Storage Gen2 and transformed within Databricks. The transformed data was then loaded back to the Datalake.

## Prerequisites
* An active Azure subscription with access to Azure Data Lake Storage Gen2
* Databricks account set up

## Steps
1. Mount the data from the Azure Data Lake Storage Gen2 to Databricks.
2. Use Pyspark within Databricks to perform data transformations using `DELTA TABLES`.
3. Load the transformed data back to the Azure Data Lake Storage Gen2.

##Tools & Libraries
* Databricks Pyspark
* Azure Data Lake Storage Gen2
* Azure Storage Account
* Azure resource group
* Azure Key Vault
* Azzure Data Factory
* PowerBI

## Conclusion
This project demonstrates how to perform data transformation using Databricks Pyspark and Azure Data Lake Storage Gen2. This setup can be used for larger scale data processing and storage needs.



## How to mount data from Azure Data Lake Storage Gen2 to Databricks.  

## Project architecture
<img width="508" alt="Screenshot 2023-01-25 at 14 14 10" src="https://user-images.githubusercontent.com/45521680/214684174-7fb3e321-588a-4808-8f46-af497fff6ebc.png">

## Databricks Architecture
<img width="1390" alt="Screenshot 2023-01-25 at 13 28 10" src="https://user-images.githubusercontent.com/45521680/214684129-b83f23c8-ae6d-4cf7-942e-fea5fb152ef7.png">
