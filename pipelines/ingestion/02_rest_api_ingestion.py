# Databricks notebook source
import requests
import json
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,TimestampType

# COMMAND ----------

# Define the schema for claims data coming from API
claims_schema = StructType([
    StructField("claim_id",     StringType(),    False),
    StructField("patient_id",   StringType(),    False),
    StructField("provider_id",  StringType(),    False),
    StructField("claim_status", StringType(),    True),
    StructField("claim_amount", DoubleType(),    True),
    StructField("updated_at",   TimestampType(), True)
])

print("Schema defined successfully")

# COMMAND ----------

# Simulate API response from Optum
# In production this would be a real API call
def fetch_claims_from_api():
    
    # This simulates what Optum's API sends back
    simulated_api_response = [
        {
            "claim_id":     "CLM001",
            "patient_id":   "PAT101",
            "provider_id":  "PRV01",
            "claim_status": "APPROVED",
            "claim_amount": 1500.00,
            "updated_at":   "2024-01-15 10:30:00"
        },
        {
            "claim_id":     "CLM002",
            "patient_id":   "PAT102",
            "provider_id":  "PRV02",
            "claim_status": "PENDING",
            "claim_amount": 800.00,
            "updated_at":   "2024-01-15 11:00:00"
        },
        {
            "claim_id":     "CLM003",
            "patient_id":   "PAT103",
            "provider_id":  "PRV03",
            "claim_status": "DENIED",
            "claim_amount": 2200.00,
            "updated_at":   "2024-01-15 12:00:00"
        },
        {
            "claim_id":     "CLM004",
            "patient_id":   "PAT104",
            "provider_id":  "PRV01",
            "claim_status": "APPROVED",
            "claim_amount": 3100.00,
            "updated_at":   "2024-01-15 13:00:00"
        },
        {
            "claim_id":     "CLM005",
            "patient_id":   "PAT105",
            "provider_id":  "PRV02",
            "claim_status": "PENDING",
            "claim_amount": 950.00,
            "updated_at":   "2024-01-15 14:00:00"
        }
    ]
    
    return simulated_api_response

print("API function defined successfully")

# COMMAND ----------

# Define the function
def land_claims_in_bronze(api_response):
    
    # Convert API response to Spark DataFrame
    claims_df = spark.createDataFrame(api_response)
    
    # Cast updated_at to timestamp
    from pyspark.sql.functions import col
    claims_df = claims_df.withColumn(
        "updated_at",
        col("updated_at").cast("timestamp")
    )
    
    # Add metadata columns
    claims_df = claims_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("REST_API"))
    
    # Write to Bronze
    claims_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("bronze.claims")
    
    print(f"Landed {claims_df.count()} claims in Bronze successfully")
    
    return claims_df

# COMMAND ----------

# Call the function
api_response = fetch_claims_from_api()
claims_df = land_claims_in_bronze(api_response)

# COMMAND ----------

spark.sql("SELECT * FROM bronze.claims").show()

# COMMAND ----------

