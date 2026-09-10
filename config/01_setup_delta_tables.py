# Databricks notebook source
# Test that everything is working
print("Optum Claims Pipeline - Setup Starting")
spark.version

# COMMAND ----------

# Create all databases
spark.sql("CREATE DATABASE IF NOT EXISTS pipeline_control")
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

print("All databases created successfully")

# COMMAND ----------

spark.sql('SHOW DATABASES').show()

# COMMAND ----------

spark.sql(""" 
    CREATE TABLE IF NOT EXISTS pipeline_control.watermarks (
        pipeline_name STRING,
        last_watermark TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING DELTA
""")
print ("watermark table created successfully")

# COMMAND ----------

spark.sql("""
          INSERT INTO pipeline_control.watermarks VALUES
          ('claims_silver', '2020-01-01' , current_timestamp()),
          ('members_silver', '2020-01-01', current_timestamp()),
          ('providers_silver', '2020-01-01', current_timestamp()),
          ('payments_silver', '2020-01-01', current_timestamp()),
          ('eligibility_silver', '2020-01-01', current_timestamp())
          """)
print("Starting watermarks inserted successfully")

# COMMAND ----------

spark.sql("SELECT * FROM pipeline_control.watermarks").show()

# COMMAND ----------

# Delete all existing watermarks
spark.sql("DELETE FROM pipeline_control.watermarks")

print("Watermarks cleared successfully")

# COMMAND ----------

# Reinsert watermarks once
spark.sql("""
    INSERT INTO pipeline_control.watermarks VALUES
    ('claims_silver',      '2020-01-01', current_timestamp()),
    ('members_silver',     '2020-01-01', current_timestamp()),
    ('providers_silver',   '2020-01-01', current_timestamp()),
    ('payments_silver',    '2020-01-01', current_timestamp()),
    ('eligibility_silver', '2020-01-01', current_timestamp())
""")

print("Watermarks reinserted successfully")

# COMMAND ----------

spark.sql("SELECT * FROM pipeline_control.watermarks").show()

# COMMAND ----------

spark.sql(""" 
  CREATE TABLE IF NOT EXISTS pipeline_control.ingestion_config (
    source_name STRING,
    source_path STRING,
    target_table STRING,
    merge_key STRING,
    watermark_col STRING,
    is_active BOOLEAN 
    )
    USING DELTA
    """)
print("config table created successfully")

# COMMAND ----------

# Insert config for all five tables
spark.sql("""
    INSERT INTO pipeline_control.ingestion_config VALUES
    ('claims',      'bronze.claims',      'silver.claims',      'claim_id',    'updated_at',     true),
    ('members',     'bronze.members',     'silver.members',     'member_id',   'updated_at',     true),
    ('providers',   'bronze.providers',   'silver.providers',   'provider_id', 'updated_at',     true),
    ('payments',    'bronze.payments',    'silver.payments',    'payment_id',  'created_at',     true),
    ('eligibility', 'bronze.eligibility', 'silver.eligibility', 'member_id',  'effective_date', true)
""")

print("Config inserted successfully")

# COMMAND ----------

spark.sql("SELECT * FROM pipeline_control.ingestion_config").show()

# COMMAND ----------

spark.sql(""" CREATE TABLE IF NOT EXISTS bronze.claims(
    claim_id STRING,
    patient_id STRING,
    provider_id STRING,
    claim_status STRING,
    claim_amaount STRING,
    updated_at TIMESTAMP,
    _ingested_at TIMESTAMP,
    _source     STRING
)
USING DELTA
""")
print("Bronze claims table created successfully")

# COMMAND ----------

# Drop the incorrect table
spark.sql("DROP TABLE IF EXISTS bronze.claims")

print("Bronze claims table dropped successfully")

# COMMAND ----------

# Create Bronze claims table
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.claims (
        claim_id        STRING,
        patient_id      STRING,
        provider_id     STRING,
        claim_status    STRING,
        claim_amount    DOUBLE,
        updated_at      TIMESTAMP,
        _ingested_at    TIMESTAMP,
        _source         STRING
    )
    USING DELTA
""")

print("Bronze claims table created successfully")

# COMMAND ----------

# Create Bronze members table
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.members (
        member_id       STRING,
        first_name      STRING,
        last_name       STRING,
        date_of_birth   DATE,
        address         STRING,
        updated_at      TIMESTAMP,
        _ingested_at    TIMESTAMP,
        _source         STRING
    )
    USING DELTA
""")

print("Bronze members table created successfully")

# COMMAND ----------

# Create Bronze providers table
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.providers (
        provider_id     STRING,
        provider_name   STRING,
        specialty       STRING,
        location        STRING,
        updated_at      TIMESTAMP,
        _ingested_at    TIMESTAMP,
        _source         STRING
    )
    USING DELTA
""")

print("Bronze providers table created successfully")

# COMMAND ----------

# Create Bronze payments table
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.payments (
        payment_id      STRING,
        claim_id        STRING,
        payment_amount  DOUBLE,
        payment_date    DATE,
        payment_status  STRING,
        created_at      TIMESTAMP,
        _ingested_at    TIMESTAMP,
        _source         STRING
    )
    USING DELTA
""")

print("Bronze payments table created successfully")

# COMMAND ----------

# Create Bronze eligibility table
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.eligibility (
        eligibility_id   STRING,
        member_id        STRING,
        plan_id          STRING,
        effective_date   DATE,
        termination_date DATE,
        coverage_type    STRING,
        updated_at       TIMESTAMP,
        _ingested_at     TIMESTAMP,
        _source          STRING
    )
    USING DELTA
""")

print("Bronze eligibility table created successfully")

# COMMAND ----------

spark.sql("SHOW TABLES IN bronze").show()

# COMMAND ----------

# Create Silver claims table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.claims (
        claim_id        STRING,
        patient_id      STRING,
        provider_id     STRING,
        claim_status    STRING,
        claim_amount    DOUBLE,
        updated_at      TIMESTAMP,
        is_active       BOOLEAN,
        _ingested_at    TIMESTAMP
    )
    USING DELTA
""")

print("Silver claims table created successfully")

# COMMAND ----------

# Create Silver members table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.members (
        member_id       STRING,
        date_of_birth   DATE,
        updated_at      TIMESTAMP,
        is_active       BOOLEAN,
        _ingested_at    TIMESTAMP
    )
    USING DELTA
""")

print("Silver members table created successfully")

# COMMAND ----------

# Create Silver providers table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.providers (
        provider_id     STRING,
        provider_name   STRING,
        specialty       STRING,
        location        STRING,
        updated_at      TIMESTAMP,
        is_active       BOOLEAN,
        _ingested_at    TIMESTAMP
    )
    USING DELTA
""")

print("Silver providers table created successfully")

# COMMAND ----------

# Create Silver payments table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.payments (
        payment_id      STRING,
        claim_id        STRING,
        payment_amount  DOUBLE,
        payment_date    DATE,
        payment_status  STRING,
        created_at      TIMESTAMP,
        is_active       BOOLEAN,
        _ingested_at    TIMESTAMP
    )
    USING DELTA
""")

print("Silver payments table created successfully")

# COMMAND ----------

# Create Silver eligibility table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.eligibility (
        eligibility_id   STRING,
        member_id        STRING,
        plan_id          STRING,
        effective_date   DATE,
        termination_date DATE,
        coverage_type    STRING,
        updated_at       TIMESTAMP,
        is_active        BOOLEAN,
        _ingested_at     TIMESTAMP
    )
    USING DELTA
""")

print("Silver eligibility table created successfully")

# COMMAND ----------

spark.sql("SHOW TABLES IN silver").show()

# COMMAND ----------

# Create Gold claims summary table
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.claims_summary (
        provider_id         STRING,
        claim_month         STRING,
        total_claims        LONG,
        total_amount        DOUBLE,
        approved_claims     LONG,
        denied_claims       LONG,
        avg_claim_amount    DOUBLE,
        _updated_at         TIMESTAMP
    )
    USING DELTA
""")

print("Gold claims summary table created successfully")

# COMMAND ----------

spark.sql("SHOW TABLES IN gold").show()

# COMMAND ----------

# Final verification of all tables
print("=== PIPELINE CONTROL ===")
spark.sql("SHOW TABLES IN pipeline_control").show()

print("=== BRONZE LAYER ===")
spark.sql("SHOW TABLES IN bronze").show()

print("=== SILVER LAYER ===")
spark.sql("SHOW TABLES IN silver").show()

print("=== GOLD LAYER ===")
spark.sql("SHOW TABLES IN gold").show()

# COMMAND ----------

