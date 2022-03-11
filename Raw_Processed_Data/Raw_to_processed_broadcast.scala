// Databricks notebook source
val access_key = "xxxxxxxxxxxxxxxxxxxx"
val secret_key = "xxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxxx"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "Rahul_kumar_mount"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

val read_data = spark.read.csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Raw_data/broadcast_right.csv")
display(read_data)

// COMMAND ----------

val read_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Raw_data/broadcast_right.csv")
display(read_data)

// COMMAND ----------

import org.apache.spark.sql.functions._
val read_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Raw_data/broadcast_right.csv")
  .withColumn("Load_date", current_date().as("Load_date"))
display(read_data)

// COMMAND ----------

 val trans_data =  read_data.withColumn("broadcast_right_vod_type", 
         lower(col("broadcast_right_vod_type")))
display(trans_data)

// COMMAND ----------

val trans_data1 =  read_data.withColumn("broadcast_right_vod_type", 
         lower(col("broadcast_right_vod_type")))
         .withColumn("load_date",current_date().as("load_date"))
         .withColumn("broadcast_right_region",
         when (col("broadcast_right_region") === "Denmark", "dk")
         .when (col("broadcast_right_region") === "Baltics", "ba")
         .when (col("broadcast_right_region") === "Bulgaria", "bg")
         .when (col("broadcast_right_region") === "Estonia", "ee")
         .when (col("broadcast_right_region") === "Finland", "fl")
         .when (col("broadcast_right_region") === "Latvia", "lv")
         .when (col("broadcast_right_region") === "Lithuania", "lt")
         .when (col("broadcast_right_region") === "Nordic", "nd")
         .when (col("broadcast_right_region") === "Norway", "no")
         .when (col("broadcast_right_region") === "Russia", "ru")
         .when (col("broadcast_right_region") === "Serbia", "rs")
         .when (col("broadcast_right_region") === "Slovenia", "si")
         .when (col("broadcast_right_region") === "Sweden", "se")
         .when (col("broadcast_right_region") === "VNH Region Group","vnh")
         .when (col("broadcast_right_region") === "Viasat History Region Group","vh")
         .otherwise("nocode"))
display(trans_data1)

// COMMAND ----------

trans_data1.coalesce(1).write.option("header","true").csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Processed_Data/Processed_broadcast_right_data1/")
