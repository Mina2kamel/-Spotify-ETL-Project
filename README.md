# Spotify ETL-AWS Project
Building End to End ETL pipeline on AWS cloud using spotify API.

### Architecture
![architecture-diagram](https://github.com/Mina2kamel/-Spotify-ETL-Project/blob/main/Project%20Archicture.JPG)

### Project Describtion

**1. Extraction:** A daily AWS Lambda function, triggered by AWS CloudWatch, extracts Spotify albums, songs, and artists using the Spotify API. The raw data is stored in JSON format in an S3 bucket's raw container.

**2. Transformation:** Another AWS Lambda function processes the raw data by extracting relevant information, removing duplicates, and changing data types. The transformed data is saved in CSV format in transformed container.

**3. Loading:** The raw data is initially stored in the "raw_folder" container. After processing, the files are copied to a "processed" folder and deleted from the "to_processed" folder to avoid duplication transformation.

**4. Analytics:** AWS Glue Crawler defines the schema for the transformed data, with metadata stored in the Data Catalog. Data analysis is performed using SQL queries on AWS Athena.

### Services Used:

The project utilizes the following AWS services:

1. AWS Lambda function
3. AWS S3 (Simple Storage Service)
5. AWS Glue
6. Amazon Athena
