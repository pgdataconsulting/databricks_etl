# Databricks notebook source
# MAGIC %md
# MAGIC # Generic ETL pipeline runner
# MAGIC
# MAGIC This notebook is the single, reusable entry point for every YAML-defined
# MAGIC pipeline. It contains no pipeline-specific business logic: Azure Data
# MAGIC Factory (or a Databricks Job) passes the configuration path and runtime
# MAGIC parameters, and the `etl_framework` library installed on the cluster does
# MAGIC the rest. The execution summary is returned to the orchestrator through
# MAGIC `dbutils.notebook.exit`.

# COMMAND ----------

import json

from etl_framework.runner import run_pipeline

# COMMAND ----------

dbutils.widgets.text("config_path", "", "Path to the YAML pipeline configuration")
dbutils.widgets.text("business_date", "", "Business date (yyyy-MM-dd)")
dbutils.widgets.text("environment", "", "Target environment (dev/test/prod)")
dbutils.widgets.text("run_id", "", "Orchestrator run id (optional)")
dbutils.widgets.text("extra_parameters", "{}", "Additional parameters as JSON (optional)")

# COMMAND ----------

config_path = dbutils.widgets.get("config_path")
business_date = dbutils.widgets.get("business_date")
environment = dbutils.widgets.get("environment")
run_id = dbutils.widgets.get("run_id") or None
extra_parameters = json.loads(dbutils.widgets.get("extra_parameters") or "{}")

parameters = dict(extra_parameters)
if business_date:
    parameters["business_date"] = business_date
if environment:
    parameters["environment"] = environment

# COMMAND ----------

result = run_pipeline(
    config_path=config_path,
    parameters=parameters,
    run_id=run_id,
)

# COMMAND ----------

dbutils.notebook.exit(json.dumps(result))
