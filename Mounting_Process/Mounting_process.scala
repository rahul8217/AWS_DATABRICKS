// Databricks notebook source
val access_key = "xxxxxxxxxxxxxxxxxxxx"
val secret_key = "xxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxxx"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "Rahul_kumar_mount"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

display(dbutils.fs.ls("/mnt/Rahul_kumar_mount"))

// COMMAND ----------

val readdf = spark.read.csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Raw_data/broadcast_right.csv")
display(readdf)

// COMMAND ----------

val readdf = spark.read.csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Raw_data/started_streams.csv")
display(readdf)
