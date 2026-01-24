# Bronze → Silver (Delta Live Tables Pipeline)
#
# This file defines managed streaming tables using
# Delta Live Tables (DLT).
#
# It must be attached to a DLT pipeline.
# It will NOT run as a normal Spark job.


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#############################################
# BOOKINGS - FACT Table(No future changes)
#############################################
# Bronze → Staging (Streaming table)
@dlt.table(
    name="staging_bookings",
    comment="Raw streaming data from Bronze Delta table"
)
def staging_bookings():
    return spark.readStream.format("delta") \
        .load("/Volumes/flightsproj/bronze/bronzevolume/bookings/data")

# Transform View (business logic layer)
@dlt.view(name="transform_bookings")
def transform_bookings():

    # Read streaming data from the staging table
    df = spark.readStream.table("staging_bookings")

    # Apply transformations
    df = df.withColumn("amount", col("amount").cast(DoubleType())) \
           .withColumn("booking_date", to_date(col("booking_date"))) \
           .drop("_rescued_data") \
           .withColumn("modified_date", current_timestamp())

    return df

# Data Quality Rules
rules = {
    "rule1": "BOOKING_ID IS NOT NULL",
    "rule2": "AIRPORT_ID IS NOT NULL",
    "rule3": "FLIGHT_ID IS NOT NULL",
    "rule4": "PASSENGER_ID IS NOT NULL"
}

# Silver Table with Data Quality Enforcement
@dlt.table(
    name="silver_bookings",
    comment="Cleaned and transformed data for downstream processing"
)
@dlt.expect_all_or_drop(rules)   # Drop rows that violate rules
def silver_bookings():
    df = dlt.readStream("transform_bookings")
    return df



###############################################################################
# FLIGHTS - DIMENSION Table (Future changes can be occur so we need to upsert)
###############################################################################


@dlt.view(name="staging_flights")
def staging_flights():

    df = spark.readStream.format("delta") \
        .load("/Volumes/flightsproj/bronze/bronzevolume/flights/data")

    # Transformations 
    df = df.withColumn("flight_date", to_date(col("flight_date"))) \
           .drop("_rescued_data") \
           .withColumn("modified_date", current_timestamp())

    return df

# Using DLT AUTO CDC
# CDC = Change Data Capture
# It refers to techniques used to identify and capture changes (INSERT, UPDATE, DELETE) in a database and stream or replicate those changes to another system in real time or near-real time.
# DLT will automatically detect changes in the source table and apply them to the target table.
# This is a convenient way to keep the target table up-to-date with the latest changes in the source table.


# Create managed Silver table
dlt.create_streaming_table("silver_flights")


dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="staging_flights",
    keys=["flight_id"],
    sequence_by=col("modified_date"),   # must be time/order column
    stored_as_scd_type=1,
)



##################################################################################
# PASSENGERS - DIMENSION Table (Future changes can be occur so we need to upsert)
##################################################################################

@dlt.view(name="staging_passengers")
def staging_passengers():

    df = spark.readStream.format("delta") \
        .load("/Volumes/flightsproj/bronze/bronzevolume/passengers/data")

    df = df.drop('_rescued_data')\
       .withColumn('modified_date', current_timestamp())

    return df

# Create managed Silver table
dlt.create_streaming_table("silver_passengers")


dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="staging_passengers",
    keys=["passenger_id"],
    sequence_by=col("modified_date"),   # must be time/order column
    stored_as_scd_type=1,
)


################################################################################
# AIRPORTS - DIMENSION Table (Future changes can be occur so we need to upsert)
################################################################################

@dlt.view(name="staging_airports")
def staging_passengers():

    df = spark.readStream.format("delta") \
        .load("/Volumes/flightsproj/bronze/bronzevolume/airports/data")

    df = df.drop('_rescued_data')\
       .withColumn('modified_date', current_timestamp())

    return df

# Create managed Silver table
dlt.create_streaming_table("silver_airports")


dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="staging_airports",
    keys=["airport_id"],
    sequence_by=col("modified_date"),   # must be time/order column
    stored_as_scd_type=1,
)


#############################
# SILVER BUSINESS JOIN TABLE
#############################

@dlt.table(
    name="silver_business",
    comment="Business logic layer for downstream processing"
)
def silver_business():
    df = dlt.readStream("silver_bookings") \
            .join(dlt.read("silver_flights"), on="flight_id", how="inner") \
            .join(dlt.read("silver_passengers"), on="passenger_id", how="inner") \
            .join(dlt.read("silver_airports"), on="airport_id", how="inner") \
            .drop('modified_date')\
            .withColumn("modified_date", current_timestamp())

    return df   






