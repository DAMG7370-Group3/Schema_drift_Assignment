# AddNewColumns Mode Implementation - For Comparison with Rescue Mode
# Uses SAME logic but with addNewColumns schema evolution mode

from pyspark import pipelines as pl
from pyspark.sql.functions import *
from pyspark.sql.types import *

volume_path = "/Volumes/workspace/damg7370/datastore/schema_drift/demo_smm/customer_*.json"

# ============================================================================
# BRONZE LAYER - addNewColumns Mode
# ============================================================================

pl.create_streaming_table("demo_cust_bronze_addnew")

@pl.append_flow(
    target="demo_cust_bronze_addnew",
    name="demo_cust_bronze_addnew_ingest_flow"
)
def demo_cust_bronze_addnew_ingest_flow():
    """
    Bronze layer with addNewColumns mode.
    KEY DIFFERENCE: New columns are automatically added to table schema
    instead of going to _rescued_data
    """
    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # 🔑 KEY CHANGE
            .load(f"{volume_path}")
    )
    return df.withColumn("ingestion_datetime", current_timestamp()) \
             .withColumn("source_filename", col("_metadata.file_path"))


# ============================================================================
# SILVER LAYER - addNewColumns Mode
# ============================================================================

# Same functions (but they won't do anything since no _rescued_data)
def process__rescue_data_datatype_change(df, target_schema: StructType):
    """
    Function to handle DATATYPE changes.
    NOTE: With addNewColumns mode, this won't find anything in _rescued_data
    """
    df = df.withColumn(
        "_rescued_data_modified",
        from_json(col("_rescued_data"), MapType(StringType(), StringType()))
    )
    
    for field in target_schema.fields:
        data_type = field.dataType
        column_name = field.name

        key_condition = expr(
            f"_rescued_data_modified IS NOT NULL AND map_contains_key(_rescued_data_modified, '{column_name}')"
        )
        
        rescued_value = when(
            key_condition,
            col("_rescued_data_modified").getItem(column_name).cast(data_type)
        ).otherwise(col(column_name).cast(data_type))
        
        df = df.withColumn(column_name, rescued_value)
        df = df.withColumn(column_name, col(column_name).cast(data_type))
        
    df = df.drop('_rescued_data_modified')
    df = df.withColumn('_rescued_data', lit(None).cast(StringType()))
    
    return df


def process__rescue_data_new_fields(df):
    """
    Function to handle adding NEW FIELDS.
    NOTE: With addNewColumns mode, this function won't be needed
    because columns are already added automatically!
    """
    df = df.withColumn(
        "_rescued_data_json_to_map", 
        from_json(
            col("_rescued_data"), 
            MapType(StringType(), StringType())
        )
    )

    df = df.withColumn("_rescued_data_map_keys", map_keys(col("_rescued_data_json_to_map")))

    df_keys = df.select(
        explode(
            map_keys(col("_rescued_data_json_to_map"))
        ).alias("rescued_key")
    ).distinct()

    # This will be empty list for streaming, but that's okay
    # because addNewColumns already added the columns!
    new_keys = [row["rescued_key"] for row in df_keys.collect()] if not df.isStreaming else []

    for key in new_keys:
        if key != "_file_path":
            df = df.withColumn(
                key,
                col("_rescued_data_json_to_map").getItem(key).cast(StringType())
            )

    return df


# Define schema with updated datatypes
updated_datatypes = StructType([
    StructField("signupDate", DateType(), True)
])

# Silver Layer - addNewColumns Mode
pl.create_streaming_table(
    name="demo_cust_silver_addnew",
    expect_all_or_drop={
        "valid_id": "CustomerID IS NOT NULL"
        # NOTE: Removed "no_rescued_data" expectation since _rescued_data will always be NULL
    }
)

@pl.append_flow(
    target="demo_cust_silver_addnew",
    name="demo_cust_silver_addnew_clean_flow"
)
def demo_cust_silver_addnew_clean_flow():
    """
    Silver layer with addNewColumns mode.
    KEY DIFFERENCE: No need to process _rescued_data because new columns
    are already in the DataFrame!
    """
    df = spark.readStream.table("demo_cust_bronze_addnew")
    
    # These functions will do nothing since _rescued_data is always NULL
    # but keeping them for consistency with rescue mode
    df = process__rescue_data_new_fields(df)
    df = process__rescue_data_datatype_change(df, updated_datatypes)
    
    return df


