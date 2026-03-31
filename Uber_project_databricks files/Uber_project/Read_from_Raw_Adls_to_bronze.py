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
    URL= f"https://uberprojectdata.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-03-30T11:29:23Z&se=2026-04-02T19:44:23Z&spr=https&sv=2024-11-04&sr=c&sig=Rp2UcsC%2Bp0LyHfKnDY0RfVgKfPIiVVMMxIQ6jrofnJM%3D"

    df = pd.read_json(URL)
    #convert into Spark dataframe
    df_spark = spark.createDataFrame(df)

    #write data to bronze layer
    df_spark.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"uber.bronze.{file['file']}")


# COMMAND ----------

url = "https://uberprojectdata.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=r&st=2026-03-30T11:29:23Z&se=2026-04-02T19:44:23Z&spr=https&sv=2024-11-04&sr=c&sig=Rp2UcsC%2Bp0LyHfKnDY0RfVgKfPIiVVMMxIQ6jrofnJM%3D"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")