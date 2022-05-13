# Databricks notebook source
# MAGIC %md
# MAGIC #Basic Delta Tables actions
# MAGIC - Read and write to Delta using streaming
# MAGIC - Demonstrate streaming job with Delta merge
# MAGIC - Demonstrate streaming job with CDF
# MAGIC - Test GIT integration

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../00-Delta-Lake/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Create Delta table using python / Scala API
data_parquet = spark.read.parquet("/mnt/field-demos/delta/lending_club_parquet")

data_parquet.write.format("delta").mode("overwrite").save(cloud_storage_path+"/lending_club_delta")

spark.read.format("delta").load(cloud_storage_path+"/lending_club_delta").display()

# COMMAND ----------

# DBTITLE 1,Delta table creation using SQL
# MAGIC %sql
# MAGIC create table if not exists lending_club_delta using delta
# MAGIC    tblproperties (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true) as 
# MAGIC   (select monotonically_increasing_id() as id, loan_amnt, funded_amnt, term, int_rate, addr_state 
# MAGIC           from parquet.`/mnt/field-demos/delta/lending_club_parquet`);
# MAGIC      
# MAGIC select * from lending_club_delta;

# COMMAND ----------

# MAGIC %md ### ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Exploring delta structure

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL lending_club_delta

# COMMAND ----------

# DBTITLE 1,Delta is composed of parquet files and a transactional log
# MAGIC %fs ls /mnt/field-demos/delta/lending_club_delta

# COMMAND ----------

# MAGIC %fs ls /mnt/field-demos/delta/lending_club_delta/_delta_log/

# COMMAND ----------

# DBTITLE 1,Each log contains parquet files stats for efficient data skipping
# MAGIC %fs head /mnt/field-demos/delta/lending_club_delta/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-lake-perf-bench.png" width="500" style="float: right; margin-left: 50px"/>
# MAGIC 
# MAGIC ### Blazing fast query at scale
# MAGIC 
# MAGIC Log files are compacted in a parquet checkpoint every 10 commits. The checkpoint file contains the entire table structure.
# MAGIC 
# MAGIC Table is self suficient, the metastore doesn't store additional information removing bottleneck and scaling metadata
# MAGIC 
# MAGIC This result in **fast read query**, even with a growing number of files/partitions!

# COMMAND ----------

# MAGIC %md ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Unified Batch and Streaming Source and Sink
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * We will run a streaming query on this data
# MAGIC * This notebook will run an `INSERT` against our `lending_club_delta` table

# COMMAND ----------

# Read the insertion of data
spark.readStream.table("lending_club_delta").createOrReplaceTempView("loan_by_state_readStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- observe that Iowa doesn't have any loan (in grey in the center)
# MAGIC select addr_state, sum(loan_amnt) as loan_amnt_total from loan_by_state_readStream group by addr_state

# COMMAND ----------

# MAGIC %md **Wait** until the stream is up and running before executing the code below

# COMMAND ----------

# DBTITLE 1,Let's add a new loan in Iowa, it'll appear in our map as blue as the stream picks up the update
# MAGIC %sql 
# MAGIC insert into lending_club_delta (id, loan_amnt, funded_amnt, term, int_rate, addr_state) 
# MAGIC     values (99999, 10000000000, 1000000, '6 moth', '6%', 'IA') 

# COMMAND ----------

# MAGIC %md
# MAGIC ##![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Full DML Support
# MAGIC 
# MAGIC **Note**: Full DML Support is a feature that will be coming soon to Delta Lake; the preview is currently available in Databricks.
# MAGIC 
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM lending_club_delta WHERE addr_state = 'IA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running `UPDATE` on the Delta Lake table
# MAGIC UPDATE lending_club_delta SET loan_amnt = 0 WHERE id = 99999

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists load_updates (id int, loan_amnt int, funded_amnt int, term string, int_rate string, addr_state string) ;
# MAGIC delete from load_updates;
# MAGIC insert into load_updates values (1, 1000, 1000, '6 month', '6%', 'IA'); 
# MAGIC insert into load_updates values (2, 1000, 1000, '3 month', '3%', 'IA');
# MAGIC insert into load_updates values (3, 500, 500, '12 month', '8%', 'IA');
# MAGIC select * from load_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO lending_club_delta as d
# MAGIC USING load_updates as m
# MAGIC on d.id = m.id
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT * ;
# MAGIC   
# MAGIC select * from lending_club_delta where id in (1,2,3)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake CDF (Change Data Feed) to support data sharing and Datamesh organization (DBR)
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
# MAGIC Enable Change Data Capture on your Delta table (with Databricks Runtime). With CDF, you can track all the changes (INSERT/UPDATE/DELETE) from your table.
# MAGIC 
# MAGIC It's then easy to subscribe to modifications stream on one of your table to propagage GDPR DELETE downstream

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from table_changes("lending_club_delta", 0);

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Generated columns for dynamic partitions
# MAGIC Partitions can now be generated based on expression, and push-down applying the same expression even if the request is on the original field

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- drop table lending_club_delta_cdc;
# MAGIC CREATE TABLE lending_club_delta_cdc (
# MAGIC   id bigint,
# MAGIC   loan_amnt bigint,
# MAGIC   addr_state string,
# MAGIC   modification_time timestamp,
# MAGIC   modification_date date GENERATED ALWAYS AS ( CAST(modification_time AS DATE) ) )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (modification_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into lending_club_delta_cdc (id, loan_amnt, addr_state, modification_time) select
# MAGIC   id,
# MAGIC   loan_amnt,
# MAGIC   addr_state,
# MAGIC   _commit_timestamp as modification_time
# MAGIC from table_changes("lending_club_delta", 0);
# MAGIC 
# MAGIC select * from lending_club_delta_cdc where modification_time >= '2021-09-28T22:09:41.000+0000';
