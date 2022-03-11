// Databricks notebook source
val access_key = "xxxxxxxxxxxxxxxxxxxx"
val secret_key = "xxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxxx"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "Rahul_kumar_mount_2"



dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

val read_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .option("delimiter", ";")
  .csv("dbfs:/mnt/Rahul_kumar_mount_2/Rahul_Data/Raw_data/started_streams.csv")
display(read_data)

// COMMAND ----------

val filtered_data = read_data.filter($"device_name"==="pcdash")
display(filtered_data)

// COMMAND ----------

import org.apache.spark.sql.functions._
val adding_column = spark.read
  .option("header", true)
  .option("inferschema", true)
  .option("delimiter", ";")
  .csv("dbfs:/mnt/Rahul_kumar_mount_2/Rahul_Data/Raw_data/started_streams.csv")
  .withColumn("Load_date",current_date().as("Load_date"))
  //.withColumnRenamed("product_type,,,","product_type")
display(adding_column)

// COMMAND ----------

val adding_column = spark.read
  .option("header", true)
  .option("inferschema", true)
  .option("delimiter", ";")
  .csv("dbfs:/mnt/Rahul_kumar_mount_2/Rahul_Data/Raw_data/started_streams.csv")
  .withColumn("Load_date",current_date().as("Load_date"))
display(adding_column)

// COMMAND ----------

val add = read_data.withColumn("load_date", current_date().as("load_date"))
display(add)

// COMMAND ----------

read_data.write.option("header", true).save("/mnt/Rahul_kumar_mount/Rahul_Data/Processed_Data/".format("csv"))

// COMMAND ----------

adding_column.coalesce(1).write.option("header","true").csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Processed_Data/Processed_stream_data1/")

// COMMAND ----------


