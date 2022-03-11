// Databricks notebook source
val access_key = "xxxxxxxxxxxxxxxxxxxx"
val secret_key = "xxxxxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxxxxx"
val encoded_secret_key = secret_key.replace("/", "%2F")
val aws_bucket_name = "databrickprac"
val mount_name = "Rahul_kumar_mount"

dbutils.fs.mount(s"s3a://$access_key:$encoded_secret_key@$aws_bucket_name", s"/mnt/$mount_name")

// COMMAND ----------

val read_data = spark.read
  .csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Processed_Data/Processed_broadcast_right_data1/part-00000-tid-7296460115071866252-1e30feaa-cbd9-47d7-93b0-7ac95906840d-210-1-c000.csv")
display(read_data)

// COMMAND ----------

val broadcast_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Processed_Data/Processed_broadcast_right_data1/part-00000-tid-7296460115071866252-1e30feaa-cbd9-47d7-93b0-7ac95906840d-210-1-c000.csv")
display(broadcast_data)

// COMMAND ----------

val stream_data = spark.read
  .option("header", true)
  .option("inferschema", true)
  .csv("dbfs:/mnt/Rahul_kumar_mount_1/Rahul_Data/Processed_Data/Processed_stream_data1/part-00000-tid-1019175943866677000-76b2bafd-5a07-44a3-85f5-3471f2264691-27-1-c000.csv")
display(stream_data)

// COMMAND ----------

val joined_data = broadcast_data.join(stream_data,
    broadcast_data.col("house_number") === stream_data.col("house_number") &&
    broadcast_data.col("broadcast_right_region") === stream_data.col("country_code"),"inner").distinct
    .drop(broadcast_data.col("house_number"))
    .drop(stream_data.col("dt"))
    .drop(broadcast_data.col("load_date"))
    .drop(broadcast_data.col("broadcast_right_region"))
display(joined_data)

// COMMAND ----------

val selecting_required_data = joined_data.select("dt","time","device_name","house_number",
                                                 "user_id","country_code","program_title",
                                                 "season","season_episode","genre","product_type",
                                                 "broadcast_right_start_date","broadcast_right_end_date")
display(selecting_required_data)

// COMMAND ----------

val filtering_data = selecting_required_data.where(stream_data("product_type") === "tvod" || stream_data("product_type") === "est")
                                .withColumn("load_date",current_date())
                                .withColumn("year",year(col("load_date")))
                                .withColumn("month",month(col("load_date")))
                                .withColumn("day",dayofmonth(col("load_date")))
display(filtering_data)

// COMMAND ----------

filtering_data.coalesce(1).write.option("header","true")
  .partitionBy("Year","Month","Day")
  .csv("dbfs:/mnt/Rahul_kumar_mount/Rahul_Data/Structured_Data/Structured_broadcast_data")
