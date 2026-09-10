# Databricks notebook source
# Import all libraries needed for SFTP ingestion
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType

# COMMAND ----------

# Simulate SFTP file data
# In production this would be a real CSV file
# downloaded from Optum's SFTP server

def fetch_claims_from_sftp():
    
    # This simulates what Optum drops on SFTP server
    simulated_sftp_data = [
        {
            "claim_id":     "CLM006",
            "patient_id":   "PAT106",
            "provider_id":  "PRV03",
            "claim_status": "APPROVED",
            "claim_amount": 4500.00,
            "updated_at":   "2024-01-16 09:00:00"
        },
        {
            "claim_id":     "CLM007",
            "patient_id":   "PAT107",
            "provider_id":  "PRV01",
            "claim_status": "PENDING",
            "claim_amount": 1200.00,
            "updated_at":   "2024-01-16 10:00:00"
        },
        {
            "claim_id":     "CLM008",
            "patient_id":   "PAT108",
            "provider_id":  "PRV02",
            "claim_status": "DENIED",
            "claim_amount": 3300.00,
            "updated_at":   "2024-01-16 11:00:00"
        },
        {
            "claim_id":     "CLM001",
            "patient_id":   "PAT101",
            "provider_id":  "PRV01",
            "claim_status": "PAID",
            "claim_amount": 1500.00,
            "updated_at":   "2024-01-16 12:00:00"
        }
    ]
    
    return simulated_sftp_data

print("SFTP function defined successfully")

# COMMAND ----------

# Land SFTP data into Bronze claims table
def land_sftp_claims_in_bronze(sftp_response):
    
    # Step 1 - Convert SFTP response to Spark DataFrame
    claims_df = spark.createDataFrame(sftp_response)
    
    # Step 2 - Cast updated_at to timestamp
    from pyspark.sql.functions import col
    claims_df = claims_df.withColumn(
        "updated_at",
        col("updated_at").cast("timestamp")
    )
    
    # Step 3 - Add metadata columns
    claims_df = claims_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("SFTP"))
    
    # Step 4 - Write to Bronze
    claims_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("bronze.claims")
    
    print(f"Landed {claims_df.count()} claims from SFTP in Bronze successfully")
    
    return claims_df

# COMMAND ----------

# Call the function
sftp_response = fetch_claims_from_sftp()
sftp_df = land_sftp_claims_in_bronze(sftp_response)

# COMMAND ----------

spark.sql("SELECT * FROM bronze.claims ORDER BY updated_at").show()

# COMMAND ----------

