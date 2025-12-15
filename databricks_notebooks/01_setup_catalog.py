# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Catalog en Schemas
# MAGIC Dit notebook maakt de benodigde catalog en schemas aan voor het voetbal data project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuratie

# COMMAND ----------

# Catalog en schema namen
CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aanmaken Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
print(f"✅ Catalog '{CATALOG_NAME}' aangemaakt/geselecteerd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aanmaken Schemas

# COMMAND ----------

# Bronze schema - ruwe data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
print(f"✅ Schema '{BRONZE_SCHEMA}' aangemaakt")

# Silver schema - gecleande data
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
print(f"✅ Schema '{SILVER_SCHEMA}' aangemaakt")

# Gold schema - business aggregaties
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
print(f"✅ Schema '{GOLD_SCHEMA}' aangemaakt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificatie

# COMMAND ----------

# Toon alle schemas
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grants (optioneel)
# MAGIC Uncomment onderstaande regels om toegang te geven aan andere gebruikers.

# COMMAND ----------

# # Geef toegang aan alle gebruikers in de workspace
# spark.sql(f"GRANT USAGE ON CATALOG {CATALOG_NAME} TO `account users`")
# spark.sql(f"GRANT USAGE ON SCHEMA {CATALOG_NAME}.{BRONZE_SCHEMA} TO `account users`")
# spark.sql(f"GRANT USAGE ON SCHEMA {CATALOG_NAME}.{SILVER_SCHEMA} TO `account users`")
# spark.sql(f"GRANT USAGE ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `account users`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.{GOLD_SCHEMA} TO `account users`")

# COMMAND ----------

print("🎉 Setup compleet!")
