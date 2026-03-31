# Databricks notebook source
import pandas as pd

files = [
{"file":"map_cancellation_reasons"},
{"file":"map_cities"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]


for file in files:
    #to access data from adls we need to get the public URL of the adls container and Saas Token and enter in this format- "(URL)?(SaasToken)"
    URL= f"https://uberprojectdata.blob.core.windows.net/raw/ingestion/{file['file']}.json?(SaasToken)"

    df = pd.read_json(URL)
    #convert into Spark dataframe
    df_spark = spark.createDataFrame(df)

    #write data to bronze layer
    df_spark.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"uber.bronze.{file['file']}")


# COMMAND ----------

url = "https://uberprojectdata.blob.core.windows.net/raw/ingestion/bulk_rides.json?(SaasToken)"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")
