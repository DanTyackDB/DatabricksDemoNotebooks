-- Databricks notebook source
-- MAGIC %py
-- MAGIC event_log_path = "/user/dan.tyack@databricks.com/DLT_Example/system/events/"
-- MAGIC event_log = spark.read.format('delta').load(event_log_path)
-- MAGIC event_log.createOrReplaceTempView("event_log_raw")
-- MAGIC  

-- COMMAND ----------

SELECT * FROM event_log_raw LIMIT 1000

-- COMMAND ----------

-- Expectations query aggregate
SELECT
  
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  row_expectations.passed_records,
  row_expectations.failed_records,
  row_expectations.passed_records/(row_expectations.passed_records+row_expectations.failed_records) AS Pass_Percentage
  
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw
    WHERE
      event_type = 'flow_progress'
  )

-- COMMAND ----------

-- Expectations query aggregate
SELECT
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw
    WHERE
      event_type = 'flow_progress'
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name


-- COMMAND ----------


