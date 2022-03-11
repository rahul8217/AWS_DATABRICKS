// Databricks notebook source
val access_key = "xxxxxxxxxxxxxxxxxxxx"
val secret_key = "xxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxxx"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "Rahul_kumar_mount_1"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

import org.apache.spark.sql.functions._
val read_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .csv("dbfs:/mnt/Rahul_kumar_mount_1/Rahul_Data/Processed_Data/Processed_stream_data1/part-00000-tid-1019175943866677000-76b2bafd-5a07-44a3-85f5-3471f2264691-27-1-c000.csv")
display(read_data)

// COMMAND ----------

val transform_Stream = read_data.select("dt","device_name","product_type","user_id","program_title","country_code")
                                .groupBy("dt","device_name","product_type","user_id","program_title","country_code")
                                .agg(countDistinct("user_id") as "unique_users",count("program_title") as "content_count")
display(transform_Stream)

// COMMAND ----------

val transfrom_Stream1 = read_data.select("dt","device_name","product_type","user_id","program_title","country_code")
                                .groupBy("dt","device_name","product_type","program_title","country_code")
                                .agg(countDistinct("user_id") as "unique_users",count("program_title") as "content_count")
                                .withColumn("load_date", current_date())
                                .withColumn("year", year(col("load_date")))
                                .withColumn("month", month(col("load_date")))
                                .withColumn("day", dayofmonth(col("load_date")))
                                .sort(col("unique_users").asc)
display(transfrom_Stream1)

// COMMAND ----------

val transfrom_Stream2 = read_data.select("dt","device_name","product_type","user_id","program_title","country_code")
                                .groupBy("dt","device_name","product_type","program_title","country_code")
                                .agg(countDistinct("user_id") as "unique_users",count("program_title") as "content_count")
                                .withColumn("load_date", current_date())
                                .withColumn("year", year(col("load_date")))
                                .withColumn("month", month(col("load_date")))
                                .withColumn("day", dayofmonth(col("load_date")))
                                .sort(col("unique_users").desc)
display(transfrom_Stream2)

// COMMAND ----------



// COMMAND ----------

transfrom_Stream2.coalesce(1).write.option("header","true")
  .partitionBy("Year","Month","Day")
  .csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Structured_Data/Structured_stream_data")
