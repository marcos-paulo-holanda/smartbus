# Smartbus
## A full pipeline of public bus locations in the Sao Paulo city.

This pipeline involves capturing information from the SPTrans API, which shows the bus lines and locations in the city of São Paulo. The ingestion is performed using NiFi with batch captures every 1 minute and a simple python batch script for capturing Excel files from the internet. The MinIO  was used as the data lake and Apache Spark, in stream mode, was used for instant data capture from the bronze layer and data processing for the silver layer. For the semantic layer of the pipeline, Hive was used with updates every 2 minutes through a cron job installed on the virtual machine of the container where it is hosted. Finally, for data visualization, Power BI was used locally. For the local connection of PBI to the Hive container, the Cloudera driver was used as the ODBC configuration in Windows.

![image](https://github.com/user-attachments/assets/2f9a7e7a-54a0-454b-aedd-fb4a9254a00c)


https://github.com/user-attachments/assets/a4c55d1f-cdc6-40e1-8826-23d52b2503f3

