# Databricks notebook source
# MAGIC %md
# MAGIC # 🕐 Schedule Job Creator
# MAGIC 
# MAGIC Dit notebook maakt een Databricks Job aan om de pipeline automatisch te runnen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Configuratie

# COMMAND ----------

JOB_NAME = "Football Data Pipeline"
NOTEBOOK_PATH = "/Shared/football_data/00_run_all"
SCHEDULE_CRON = "0 0 6 * * ?"  # Elke dag om 6:00 UTC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maak Job via API

# COMMAND ----------

import requests
import json

# Haal token en host op
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = context.apiUrl().get()
token = context.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Job configuratie
job_config = {
    "name": JOB_NAME,
    "tasks": [
        {
            "task_key": "run_pipeline",
            "notebook_task": {
                "notebook_path": NOTEBOOK_PATH
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 0,
                "spark_conf": {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": "local[*]"
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                }
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": SCHEDULE_CRON,
        "timezone_id": "Europe/Amsterdam"
    },
    "email_notifications": {
        "on_failure": []  # Voeg email toe indien gewenst
    },
    "max_concurrent_runs": 1
}

# Check of job al bestaat
response = requests.get(f"{host}/api/2.1/jobs/list", headers=headers)
existing_jobs = response.json().get("jobs", [])

job_exists = False
for job in existing_jobs:
    if job["settings"]["name"] == JOB_NAME:
        job_exists = True
        job_id = job["job_id"]
        print(f"⚠️ Job '{JOB_NAME}' bestaat al (ID: {job_id})")
        
        # Update bestaande job
        update_config = {"job_id": job_id, "new_settings": job_config}
        response = requests.post(f"{host}/api/2.1/jobs/update", 
                                headers=headers, json=update_config)
        if response.status_code == 200:
            print(f"✅ Job bijgewerkt")
        else:
            print(f"❌ Update failed: {response.text}")
        break

if not job_exists:
    # Maak nieuwe job
    response = requests.post(f"{host}/api/2.1/jobs/create", 
                            headers=headers, json=job_config)
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        print(f"✅ Job aangemaakt: {JOB_NAME} (ID: {job_id})")
    else:
        print(f"❌ Create failed: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job Details

# COMMAND ----------

print(f"""
📋 JOB DETAILS
==============
Naam:     {JOB_NAME}
Notebook: {NOTEBOOK_PATH}
Schedule: Dagelijks om 06:00 (Amsterdam tijd)

📍 Ga naar: Workflows → Jobs → {JOB_NAME}
""")
